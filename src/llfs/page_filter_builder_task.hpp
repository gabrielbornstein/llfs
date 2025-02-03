//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_FILTER_BUILDER_TASK_HPP
#define LLFS_PAGE_FILTER_BUILDER_TASK_HPP

#include <llfs/config.hpp>
//
#include <llfs/metrics.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_filter_builder.hpp>
#include <llfs/pinned_page.hpp>
#include <llfs/status.hpp>

#include <batteries/async/queue.hpp>
#include <batteries/async/read_write_lock.hpp>
#include <batteries/async/task.hpp>
#include <batteries/math.hpp>
#include <batteries/type_traits.hpp>

#include <atomic>
#include <memory>

namespace llfs {

class PageFilterBuilderTask
{
 public:
  using Self = PageFilterBuilderTask;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static constexpr usize kNumLocks = 32;

  static_assert(__builtin_popcountll(Self::kNumLocks) == 1, "kNumLocks must be a power of 2");

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  struct Metrics {
    RateMetric<i64, 15 /*seconds*/> push_rate_15s;

    RateMetric<i64, 15 /*seconds*/> pop_rate_15s;

    RateMetric<i64, 15 /*seconds*/> build_rate_15s;

    CountMetric<i64> pop_count{0};

    CountMetric<i64> push_count{0};

    CountMetric<i64> build_ok_count{0};

    CountMetric<i64> build_error_count{0};
  };

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageFilterBuilderTask(batt::TaskScheduler& task_scheduler,
                                 std::unique_ptr<PageFilterBuilder>&& filter_builder,
                                 PageDevice* src_page_device, PageDevice* filter_device) noexcept;

  PageFilterBuilderTask(const PageFilterBuilderTask&) = delete;
  PageFilterBuilderTask& operator=(const PageFilterBuilderTask&) = delete;

  ~PageFilterBuilderTask() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const Metrics& metrics() const noexcept
  {
    return this->metrics_;
  }

  void halt() noexcept;

  void join() noexcept;

  PageId filter_page_id_for(PageId src_page_id) const noexcept;

  Status push(llfs::PinnedPage&& src_page) noexcept;

  /** \brief LockT must be batt::ReadWriteLock::Reader or batt::ReadWriteLock::Writer.
   */
  template <typename LockT>
  std::shared_ptr<LockT> lock_page(PageId filter_page_id,
                                   batt::StaticType<LockT> type_of_lock = {}) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  void process_queue() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::unique_ptr<PageFilterBuilder> filter_builder_;

  PageDevice* src_page_device_;

  PageIdFactory src_page_ids_;

  PageDevice* filter_device_;

  PageIdFactory filter_page_ids_;

  Metrics metrics_;

  batt::Queue<llfs::PinnedPage> queue_;

  std::atomic<bool> halt_requested_{false};

  std::array<batt::ReadWriteLock, kNumLocks> locks_;

  batt::Task task_;
};

}  //namespace llfs

#endif  // LLFS_PAGE_FILTER_BUILDER_TASK_HPP
