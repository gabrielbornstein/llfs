//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_slot.hpp>
//

#include <llfs/logging.hpp>

#include <batteries/env.hpp>

#include <random>

namespace llfs {

#if 0  // TODO [tastolfi 2025-07-16] move this somewhere reusable
    
namespace {

template <typename T>
T getenv_log(const char* var_name, T default_value)
{
  auto opt_value = batt::getenv_as<T>(var_name);
  if (!opt_value) {
    LLFS_LOG_INFO() << var_name << " not defined; using" << BATT_INSPECT(default_value);
    return default_value;
  }

  LLFS_LOG_INFO() << var_name << " == " << opt_value;
  return *opt_value;
}

}  //namespace

#endif

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto PageCacheSlot::Pool::Metrics::instance() -> Metrics&
{
  static Metrics metrics_;

  [[maybe_unused]] static bool registered_ = [] {
    const auto metric_name = [](std::string_view property) {
      return batt::to_string("PageCacheSlot_Pool_", property);
    };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), metrics_.n)

    ADD_METRIC_(indexed_slots);
    ADD_METRIC_(query_count);
    ADD_METRIC_(hit_count);
    ADD_METRIC_(stale_count);
    ADD_METRIC_(allocate_count);
    ADD_METRIC_(construct_count);
    ADD_METRIC_(free_queue_insert_count);
    ADD_METRIC_(free_queue_remove_count);
    ADD_METRIC_(evict_count);
    ADD_METRIC_(evict_prior_generation_count);
    ADD_METRIC_(insert_count);
    ADD_METRIC_(erase_count);
    ADD_METRIC_(full_count);
    ADD_METRIC_(admit_byte_count);
    ADD_METRIC_(evict_byte_count);

#undef ADD_METRIC_

    return true;
  }();

  return metrics_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCacheSlot::Pool::Pool(SlotCount n_slots, MaxCacheSizeBytes max_byte_size,
                                       std::string&& name) noexcept
    : n_slots_{n_slots}
    , max_byte_size_{max_byte_size}
    , name_{std::move(name)}
    , slot_storage_{new SlotStorage[n_slots]}
{
  this->metrics_.total_capacity_allocated.add(this->max_byte_size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot::Pool::~Pool() noexcept
{
  this->halt();
  this->join();

  if (this->slot_storage_) {
    const usize n_to_delete = this->n_constructed_.load();
    BATT_CHECK_EQ(n_to_delete, this->n_allocated_.load());

    for (usize i = 0; i < n_to_delete; ++i) {
      BATT_DEBUG_INFO("Destructing slot " << i << BATT_INSPECT(n_to_delete)
                                          << BATT_INSPECT(this->n_allocated_.load())
                                          << BATT_INSPECT(this->n_slots_));
      this->get_slot(i)->~PageCacheSlot();
    }
  }

  this->metrics_.total_capacity_freed.add(this->max_byte_size_);

  LLFS_VLOG(1) << "PageCacheSlot::Pool::~Pool()";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::Pool::halt()
{
  this->halt_requested_.store(true);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::Pool::join()
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::get_slot(usize i)
{
  BATT_CHECK_LT(i, this->n_slots_);

  return std::addressof(*this->slots()[i]);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::allocate(PageSize page_size)
{
  this->metrics_.allocate_count.add(1);

  const i64 max_resident_size = static_cast<i64>(this->max_byte_size_);
  i64 observed_resident_size = this->resident_size_.fetch_add(page_size) + page_size;
  PageCacheSlot* free_slot = nullptr;

  // Before constructing _new_ slots, try popping one from the free queue.
  //
  while (this->free_queue_.pop(free_slot)) {
    BATT_CHECK_NOT_NULLPTR(free_slot);
    this->metrics_.free_queue_remove_count.add(1);
    if (free_slot->evict()) {
      BATT_CHECK(!free_slot->is_valid());
      this->metrics_.allocate_free_queue_count.add(1);
      break;
    }
    free_slot = nullptr;
  }

  // Free queue is empty; if we can construct a new one, do it.
  //
  if (!free_slot && this->n_allocated_.load() < this->n_slots_ &&
      observed_resident_size < max_resident_size) {
    free_slot = this->construct_new_slot();
    if (free_slot) {
      BATT_CHECK(!free_slot->is_valid());
      this->metrics_.allocate_construct_count.add(1);
    }
  }

  // If both of the previous methods failed to allocate a free_slot, or if the cache has grown too
  // large, then evict expired pages until we fix both problems.
  //
  if (!free_slot || observed_resident_size > max_resident_size) {
    usize n_slots_constructed = this->n_constructed_.load();
    batt::CpuCacheLineIsolated<PageCacheSlot>* const p_slots = this->slots();

    // Loop over all constructed slots, evicting until we have a slot to return *and* we are under
    // the limit.
    //
    for (;;) {
      PageCacheSlot* candidate = p_slots[this->advance_clock_hand(n_slots_constructed)].get();
      if (!candidate->expire()) {
        continue;
      }
      if (!candidate->evict()) {
        continue;
      }
      const i64 bytes_evicted = candidate->get_page_size_while_invalid();

      // If we don't have a slot to return yet, take this one; else add the just-evicted slot to
      // the free list so other threads can pick it up immediately.
      //
      if (!free_slot) {
        free_slot = candidate;
        this->metrics_.allocate_evict_count.add(1);
      } else {
        this->push_free_slot(candidate);
      }

      observed_resident_size = this->resident_size_.fetch_sub(bytes_evicted) - bytes_evicted;
      if (observed_resident_size <= max_resident_size) {
        break;
      }
    }  // for (;;) - loop through slots until resident set <= max
  }

  BATT_CHECK_NOT_NULLPTR(free_slot);

  return free_slot;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::Pool::advance_clock_hand(usize& n_slots_constructed)
{
  usize slot_i;
  for (;;) {
    slot_i = this->clock_hand_.fetch_add(1);
    if (slot_i < n_slots_constructed) {
      break;
    }
    usize expected_clock_hand = slot_i + 1;
    for (;;) {
      n_slots_constructed = this->n_constructed_.load();
      if (expected_clock_hand < n_slots_constructed) {
        break;
      }
      if (this->clock_hand_.compare_exchange_weak(expected_clock_hand, 0)) {
        break;
      }
    }
  }
  return slot_i;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::construct_new_slot()
{
  const usize allocated_i = this->n_allocated_.fetch_add(1);
  if (allocated_i >= this->n_slots_) {
    const usize reverted = this->n_allocated_.fetch_sub(1);
    BATT_CHECK_GE(reverted, this->n_slots_);
    return nullptr;
  }

  this->metrics_.construct_count.add(1);

  void* storage_addr = this->slots() + allocated_i;
  PageCacheSlot* const new_slot = new (storage_addr) PageCacheSlot{*this};

  this->n_constructed_.fetch_add(1);

  return new_slot;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::Pool::index_of(const PageCacheSlot* slot)
{
  BATT_CHECK_NOT_NULLPTR(slot);
  BATT_CHECK_EQ(std::addressof(slot->pool()), this);

  const usize index = batt::CpuCacheLineIsolated<PageCacheSlot>::pointer_from(slot) - this->slots();

  BATT_CHECK_LT(index, this->n_slots_);

  return index;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::CpuCacheLineIsolated<PageCacheSlot>* PageCacheSlot::Pool::slots()
{
  return reinterpret_cast<batt::CpuCacheLineIsolated<PageCacheSlot>*>(this->slot_storage_.get());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::Pool::clear_all()
{
  usize success_count = 0;
  usize n_slots = this->n_constructed_.load();

  usize slot_i = 0;
  for (usize n_tries = 0; n_tries < 4; ++n_tries) {
    for (; slot_i < n_slots; ++slot_i) {
      PageCacheSlot* slot = this->get_slot(slot_i);
      if (slot->evict()) {
        ++success_count;
        slot->clear();
      }
    }
    if (n_slots == this->n_constructed_.load()) {
      break;
    }
    n_slots = this->n_constructed_.load();
    if (n_tries == 2) {
      slot_i = 0;
    }
  }

  return success_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool PageCacheSlot::Pool::push_free_slot(PageCacheSlot* slot)
{
  this->metrics_.free_queue_insert_count.add(1);
  return this->free_queue_.push(slot);
}

}  //namespace llfs
