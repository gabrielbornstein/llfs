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
    this->background_eviction_threads_.emplace_back([this, thread_i = i, n_threads] {
      this->background_eviction_thread_main(thread_i, n_threads);
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
template <typename Fn /*=void(PageCacheSlot*)*/>
void PageCacheSlot::Pool::pick_k_random_slots(usize k, Fn&& fn, usize shard_i, usize n_shards)
{
  if (k == 0) {
    return;
  }

  const usize n_slots = this->n_constructed_.get_value() / n_shards;

  if (k >= n_slots) {
    for (usize i = 0; i < n_slots; ++i) {
      fn(this->get_slot(i * n_shards + shard_i));
    }
    return;
  }

  //----- --- -- -  -  -   -
  thread_local std::default_random_engine rng{/*seed=*/std::random_device{}()};
  //----- --- -- -  -  -   -

  std::uniform_int_distribution<usize> pick_first_slot{0, n_slots - 1};
  const usize first_slot_i = pick_first_slot(rng) * n_shards + shard_i;
  fn(this->get_slot(first_slot_i));
  if (k == 1) {
    return;
  }

  std::uniform_int_distribution<usize> pick_second_slot{0, n_slots - 2};
  usize second_slot_i = pick_second_slot(rng) * n_shards + shard_i;
  if (second_slot_i >= first_slot_i) {
    second_slot_i += n_shards;
  }
  BATT_CHECK_NE(first_slot_i, second_slot_i);
  fn(this->get_slot(second_slot_i));

  // Once we have at least two unique slots, switch to selection-with-replacement
  // for speed.
  //
  for (usize i = 2; i < k; ++i) {
    fn(this->get_slot(pick_first_slot(rng) * n_shards + shard_i));
  }
}

namespace {
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct SlotWithLatestUse {
  PageCacheSlot* slot;
  i64 latest_use;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
struct LruSlotOrder {
  bool operator()(const SlotWithLatestUse& left, const SlotWithLatestUse& right) const
  {
    return (left.latest_use - right.latest_use) < 0;
  }
};

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot* PageCacheSlot::Pool::evict_lru()
{
  this->metrics_.evict_lru_count.add(1);

  batt::SmallVec<SlotWithLatestUse, 8> candidates;

  for (usize attempts = 0; attempts < this->n_constructed_.get_value(); ++attempts) {
    // Select `k` random slots, then try to evict the least recently used among these.
    //
    candidates.clear();
    this->pick_k_random_slots(this->eviction_candidates_, [&](PageCacheSlot* slot) {
      candidates.emplace_back(SlotWithLatestUse{
          .slot = slot,
          .latest_use = slot->get_latest_use(),
      });
    });

    // Sort from least recently used to most recently used.
    //
    std::sort(candidates.begin(), candidates.end(), LruSlotOrder{});

    // Only consider the least recently used half of the collected candidates.
    //
    const usize half_size = (candidates.size() + 1) / 2;
    candidates.resize(half_size);

    // Try to find an evictable candidate; stop as soon as we succeed.
    //
    for (const SlotWithLatestUse& slu : candidates) {
      if (slu.slot->evict()) {
        return slu.slot;
      }
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
void PageCacheSlot::Pool::background_eviction_thread_main(usize thread_i, usize n_threads)
{
  Status status = [this, thread_i, n_threads]() -> Status {
    std::default_random_engine rng{std::random_device{}()};

    constexpr usize kMaxCandidates = 65536;
    constexpr i64 kMinDelayUsec = 500;
    constexpr i64 kMaxDelayUsec = 750;

    std::uniform_int_distribution<i64> pick_delay_usec{kMinDelayUsec, kMaxDelayUsec};

    std::vector<SlotWithLatestUse> candidates;

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
      auto start_time = std::chrono::steady_clock::now();

      auto update_eviction_metrics = [&] {
        this->metrics_.background_evict_count.add(evict_count);
        this->metrics_.background_evict_byte_count.add(evict_byte_count);
        this->metrics_.background_evict_latency.update(start_time, evict_count);
        this->metrics_.background_evict_byte_latency.update(start_time, evict_byte_count);
        evict_count = 0;
        evict_byte_count = 0;
        start_time = std::chrono::steady_clock::now();
        estimated_cache_bytes = this->metrics_.estimate_cache_bytes();
        global_target = this->metrics_.estimate_total_limit();
      };

      while (global_target > 0 && estimated_cache_bytes > global_target && !this->halt_requested_) {
        // Pick a number of candidates large enough to cover the gap, assuming 4kb pages; don't
        // exceed our max.
        //
        const usize n_candidates = std::min<usize>(
            kMaxCandidates, ((estimated_cache_bytes - global_target + 4095) / 4096) * 2);

        candidates.clear();
        this->pick_k_random_slots(
            n_candidates,
            [&candidates](PageCacheSlot* slot) {
              candidates.emplace_back(SlotWithLatestUse{
                  .slot = slot,
                  .latest_use = slot->get_latest_use(),
              });
            },
            thread_i, n_threads);

        // Sort from least recently used to most recently used.
        //
        std::sort(candidates.begin(), candidates.end(), LruSlotOrder{});

        // Only consider the least recently used half of the collected candidates.
        //
        const usize half_size = (candidates.size() + 1) / 2;
        candidates.resize(half_size);

        // Try to find evictable candidates; stop as soon as the target is reached.
        //
        for (const SlotWithLatestUse& slu : candidates) {
          // If we fail to evict, then check for halt.
          //
          if (!slu.slot->evict()) {
            this->metrics_.background_evict_fail_count.add(1);
            if (this->halt_requested_) {
              break;
            }
            continue;
          }

          const i64 slot_page_size = slu.slot->get_page_size_while_invalid();
          estimated_cache_bytes -= slot_page_size;
          evict_count += 1;
          evict_byte_count += slot_page_size;
          slu.slot->clear();

          if (estimated_cache_bytes <= global_target) {
            update_eviction_metrics();
            if (estimated_cache_bytes <= global_target) {
              break;
            }
          }
        }

        update_eviction_metrics();
      }
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
