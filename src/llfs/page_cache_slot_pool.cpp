//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_slot.hpp>
//

#include <batteries/env.hpp>

#include <random>

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize default_background_thread_count()
{
  static const usize value_ =
      batt::getenv_as<usize>("LLFS_CACHE_BACKGROUND_EVICTION_THREADS").value_or(1);

  return value_;
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ usize PageCacheSlot::Pool::default_eviction_candidate_count()
{
  static const usize value_ = batt::getenv_as<usize>("LLFS_CACHE_EVICTION_CANDIDATES")
                                  .value_or(Self::kDefaultEvictionCandidates);

  return value_;
}

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
    ADD_METRIC_(alloc_count);
    ADD_METRIC_(evict_count);
    ADD_METRIC_(evict_prior_generation_count);
    ADD_METRIC_(insert_count);
    ADD_METRIC_(erase_count);
    ADD_METRIC_(full_count);
    ADD_METRIC_(admit_byte_count);
    ADD_METRIC_(evict_byte_count);
    ADD_METRIC_(background_evict_count);
    ADD_METRIC_(background_evict_byte_count);

#undef ADD_METRIC_

    return true;
  }();

#if 0  // TODO [tastolfi 2025-04-21]
  global_metric_registry()
      .remove(this->metrics_.indexed_slots)
      .remove(this->metrics_.query_count)
      .remove(this->metrics_.hit_count)
      .remove(this->metrics_.stale_count)
      .remove(this->metrics_.alloc_count)
      .remove(this->metrics_.evict_count)
      .remove(this->metrics_.evict_prior_generation_count)
      .remove(this->metrics_.insert_count)
      .remove(this->metrics_.erase_count)
      .remove(this->metrics_.full_count)
      .remove(this->metrics_.admit_byte_count)
      .remove(this->metrics_.evict_byte_count)
      .remove(this->metrics_.background_evict_count)
      .remove(this->metrics_.background_evict_byte_count);
#endif

  return metrics_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCacheSlot::Pool::Pool(SlotCount n_slots, MaxCacheSizeBytes max_byte_size,
                                       std::string&& name,
                                       Optional<SlotCount> eviction_candidates) noexcept
    : n_slots_{n_slots}
    , max_byte_size_{max_byte_size}
    , eviction_candidates_{std::min<usize>(
          n_slots, std::max<usize>(
                       2, eviction_candidates.value_or(Self::default_eviction_candidate_count())))}
    , name_{std::move(name)}
    , slot_storage_{new SlotStorage[n_slots]}
{
  this->metrics_.total_capacity_allocated.add(this->max_byte_size_);

  const usize n_threads = default_background_thread_count();
  for (usize i = 0; i < n_threads; ++i) {
    this->background_eviction_threads_.emplace_back([this] {
      this->background_eviction_thread_main();
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot::Pool::~Pool() noexcept
{
  this->halt();
  this->join();

  if (this->slot_storage_) {
    const usize n_to_delete = this->n_constructed_.get_value();
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
  std::unique_lock<std::mutex> lock{this->background_threads_mutex_};
  for (std::thread& t : this->background_eviction_threads_) {
    t.join();
  }
  this->background_eviction_threads_.clear();
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
PageCacheSlot* PageCacheSlot::Pool::allocate()
{
  if (this->n_allocated_.load() < this->n_slots_) {
    const usize allocated_i = this->n_allocated_.fetch_add(1);
    if (allocated_i < this->n_slots_) {
      this->metrics_.alloc_count.add(1);
      void* storage_addr = this->slots() + allocated_i;
      PageCacheSlot* const new_slot = new (storage_addr) PageCacheSlot{*this};
      this->n_constructed_.fetch_add(1);
      return new_slot;
    }
    const usize reverted = this->n_allocated_.fetch_sub(1);
    BATT_CHECK_GE(reverted, this->n_slots_);
    //
    // continue...
  }

  BATT_CHECK_OK(this->n_constructed_.await_equal(this->n_slots_));

  return this->evict_lru();
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
PageCacheSlot* PageCacheSlot::Pool::evict_lru()
{
  thread_local std::default_random_engine rng{/*seed=*/std::random_device{}()};

  const usize n_slots = this->n_constructed_.get_value();

  if (n_slots == 0) {
    return nullptr;
  }

  if (n_slots == 1) {
    PageCacheSlot* only_slot = this->get_slot(0);
    if (only_slot->evict()) {
      return only_slot;
    }
    return nullptr;
  }

  // Pick k slots at random and try to evict whichever one has the least (earliest) latest use
  // logical time stamp.
  //
  std::uniform_int_distribution<usize> pick_first_slot{0, n_slots - 1};
  std::uniform_int_distribution<usize> pick_second_slot{0, n_slots - 2};

  for (usize attempts = 0; attempts < n_slots; ++attempts) {
    usize first_slot_i = pick_first_slot(rng);
    usize second_slot_i = pick_second_slot(rng);

    if (second_slot_i >= first_slot_i) {
      ++second_slot_i;
    }
    BATT_CHECK_NE(first_slot_i, second_slot_i);

    PageCacheSlot* first_slot = this->get_slot(first_slot_i);
    PageCacheSlot* second_slot = this->get_slot(second_slot_i);
    PageCacheSlot* lru_slot = [&] {
      if (first_slot->get_latest_use() - second_slot->get_latest_use() < 0) {
        return first_slot;
      }
      return second_slot;
    }();

    // Pick more random slots (with replacement, since we already have >= 2) to try to get a
    // better (older) last-usage LTS.
    //
    for (usize k = 2; k < this->eviction_candidates_; ++k) {
      usize nth_slot_i = pick_first_slot(rng);
      PageCacheSlot* nth_slot = this->get_slot(nth_slot_i);
      lru_slot = [&] {
        if (nth_slot->get_latest_use() - lru_slot->get_latest_use() < 0) {
          return nth_slot;
        }
        return lru_slot;
      }();
    }

    // Fingers crossed!
    //
    if (lru_slot->evict()) {
      return lru_slot;
    }
  }

  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::Pool::clear_all()
{
  usize success_count = 0;
  usize n_slots = this->n_constructed_.get_value();

  usize slot_i = 0;
  for (usize n_tries = 0; n_tries < 4; ++n_tries) {
    for (; slot_i < n_slots; ++slot_i) {
      PageCacheSlot* slot = this->get_slot(slot_i);
      if (slot->evict()) {
        ++success_count;
        slot->clear();
      }
    }
    if (n_slots == this->n_constructed_.get_value()) {
      break;
    }
    n_slots = this->n_constructed_.get_value();
    if (n_tries == 2) {
      slot_i = 0;
    }
  }

  return success_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::Pool::background_eviction_thread_main()
{
  Status status = [this]() -> Status {
    std::default_random_engine rng{std::random_device{}()};

    const i64 n_threads = std::max<i64>(1, default_background_thread_count());

    constexpr i64 kMinDelayUsec = 500;
    constexpr i64 kMaxDelayUsec = 750;

    std::uniform_int_distribution<i64> pick_delay_usec{kMinDelayUsec, kMaxDelayUsec * n_threads};

    while (!this->halt_requested_) {
      {
        const i64 delay_usec = pick_delay_usec(rng);
        std::this_thread::sleep_for(std::chrono::microseconds(delay_usec));

        if (this->halt_requested_) {
          break;
        }
      }

      i64 evict_count = 0;
      i64 evict_byte_count = 0;
      i64 estimated_cache_bytes = this->metrics_.estimate_cache_bytes();
      i64 global_target = this->metrics_.estimate_total_limit();

      auto update_eviction_metrics = [&] {
        this->metrics_.background_evict_count.add(evict_count);
        this->metrics_.background_evict_byte_count.add(evict_byte_count);
        evict_count = 0;
        evict_byte_count = 0;
        estimated_cache_bytes = this->metrics_.estimate_cache_bytes();
        global_target = this->metrics_.estimate_total_limit();
      };

      while (global_target > 0 && estimated_cache_bytes > global_target) {
        PageCacheSlot* slot = this->evict_lru();
        if (!slot) {
          if (this->halt_requested_) {
            break;
          }
          update_eviction_metrics();
          std::this_thread::yield();
          continue;
        }
        const i64 slot_page_size = slot->get_page_size_while_invalid();
        estimated_cache_bytes -= slot_page_size;
        evict_count += 1;
        evict_byte_count += slot_page_size;
        slot->clear();

        if ((evict_count & 0x7f) == 0) {
          update_eviction_metrics();
          if (this->halt_requested_) {
            break;
          }
        }
      }

      update_eviction_metrics();
    }

    return OkStatus();
  }();

  if (!status.ok() && !this->halt_requested_) {
    LLFS_LOG_INFO() << "PageCacheSlot::Pool::background_eviction_thread exited unexpectedly: "
                    << status;
  } else {
    LLFS_VLOG(1) << "PageCacheSlot::Pool::background_eviction_thread exited normally: " << status;
  }
}

}  //namespace llfs
