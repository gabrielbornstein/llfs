//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_filter_builder_task.hpp>
//

#include <xxhash.h>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageFilterBuilderTask::PageFilterBuilderTask(
    batt::TaskScheduler& task_scheduler, std::unique_ptr<PageFilterBuilder>&& filter_builder,
    PageDevice* src_page_device, PageDevice* filter_device) noexcept
    : filter_builder_{std::move(filter_builder)}
    , src_page_device_{src_page_device}
    , src_page_ids_{src_page_device->page_ids()}
    , filter_device_{filter_device}
    , filter_page_ids_{filter_device->page_ids()}
    , task_{task_scheduler.schedule_task(), [this] {
              this->process_queue();
            }}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageFilterBuilderTask::~PageFilterBuilderTask() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageFilterBuilderTask::halt() noexcept
{
  if (!this->halt_requested_.exchange(true)) {
    this->queue_.close();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageFilterBuilderTask::join() noexcept
{
  this->task_.join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageFilterBuilderTask::push(llfs::PinnedPage&& src_page) noexcept
{
  if (this->filter_builder_->accepts_page(src_page)) {
    BATT_REQUIRE_OK(this->queue_.push(std::move(src_page)));

    this->metrics_.push_count.add(1);
    this->metrics_.push_rate_15s.update(this->metrics_.push_count.load());
  }
  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename LockT>
std::shared_ptr<LockT> PageFilterBuilderTask::lock_page(PageId filter_page_id,
                                                        batt::StaticType<LockT>) noexcept
{
  // Hash the physical page number.
  //
  const u64 physical_page = this->filter_page_ids_.get_physical_page(filter_page_id);
  const u64 hash_val = XXH64(&physical_page, sizeof(physical_page), (u64)this);

  // This page belongs to the shard defined by hash mod num_locks.
  //
  const usize lock_i = hash_val & (Self::kNumLocks - 1);

  // Acquire and return the lock.
  //
  return std::make_shared<LockT>(this->locks_[lock_i]);
}

//----- --- -- -  -  -   -
// Explicitly instantiate the Reader lock and Writer lock cases.
//
template std::shared_ptr<batt::ReadWriteLock::Reader> PageFilterBuilderTask::lock_page(
    PageId filter_page_id, batt::StaticType<batt::ReadWriteLock::Reader>) noexcept;

template std::shared_ptr<batt::ReadWriteLock::Writer> PageFilterBuilderTask::lock_page(
    PageId filter_page_id, batt::StaticType<batt::ReadWriteLock::Writer>) noexcept;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageFilterBuilderTask::process_queue() noexcept
{
  // Builder thread main loop.
  //
  for (;;) {
    // Grab the next new leaf page from the queue.
    //
    StatusOr<llfs::PinnedPage> pinned_src_page = this->queue_.await_next();
    if (!pinned_src_page.ok()) {
      break;
    }
    this->metrics_.pop_count.add(1);

    const llfs::PageId src_page_id = pinned_src_page->page_id();
    const llfs::PageId filter_page_id = this->filter_page_id_for(src_page_id);

    BATT_CHECK_NE(src_page_id, filter_page_id);

    batt::StatusOr<std::shared_ptr<llfs::PageBuffer>> filter_buffer =
        this->filter_device_->prepare(filter_page_id);

    if (!filter_buffer.ok()) {
      LOG(ERROR) << "Failed to prepare filter page!" << BATT_INSPECT(filter_page_id)
                 << BATT_INSPECT(filter_buffer.status());
      continue;
    }

    BATT_DEBUG_INFO("PageFilterBuilderTask::process_queue()"
                    << BATT_INSPECT(batt::Task::current_id()) << BATT_INSPECT(src_page_id)
                    << BATT_INSPECT(filter_page_id));

    Status build_status = this->filter_builder_->build_filter(*pinned_src_page, *filter_buffer);
    if (!build_status.ok()) {
      LOG(ERROR) << "Failed to build filter page!" << BATT_INSPECT(filter_page_id)
                 << BATT_INSPECT(build_status);
      this->metrics_.build_error_count.add(1);
      continue;
    }

    this->metrics_.build_ok_count.add(1);
    this->metrics_.build_rate_15s.update(this->metrics_.build_ok_count.load());

    if (false) {
      LOG_EVERY_N(INFO, 10000) << BATT_INSPECT(this->metrics_.push_count)
                               << BATT_INSPECT(this->metrics_.pop_count)
                               << BATT_INSPECT(this->metrics_.build_ok_count)
                               << BATT_INSPECT(this->metrics_.build_error_count)
                               << BATT_INSPECT(this->metrics_.push_rate_15s.get())
                               << BATT_INSPECT(this->metrics_.build_rate_15s.get());
    }

    // Start writing the page asynchronously.
    //
    this->filter_device_->write(
        std::move(*filter_buffer),
        [writer_lock =
             this->lock_page(filter_page_id, batt::StaticType<batt::ReadWriteLock::Writer>{})](
            batt::Status /*ignored_for_now*/) mutable {
          writer_lock = nullptr;
          // TODO [tastolfi 2025-01-06] add to cache?
        });
  }
}

}  //namespace llfs
