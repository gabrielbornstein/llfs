//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_filter_builder_task.hpp>
//

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
PageId PageFilterBuilderTask::filter_page_id_for(PageId src_page_id) const noexcept
{
  return this->filter_page_ids_.make_page_id(  //
      this->src_page_ids_.get_physical_page(src_page_id),
      this->src_page_ids_.get_generation(src_page_id));
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
void PageFilterBuilderTask::process_queue() noexcept
{
  // Builder thread main loop.
  //
  for (;;) {
    // Grab the next new leaf page from the queue.
    //
    StatusOr<llfs::PinnedPage> pinned_leaf = this->queue_.await_next();
    if (!pinned_leaf.ok()) {
      break;
    }
    this->metrics_.pop_count.add(1);

    const llfs::PageId src_page_id = pinned_leaf->page_id();
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

    Status build_status = this->filter_builder_->build_filter(*pinned_leaf, *filter_buffer);
    if (!build_status.ok()) {
      LOG(ERROR) << "Failed to build filter page!" << BATT_INSPECT(filter_page_id)
                 << BATT_INSPECT(build_status);
      this->metrics_.build_error_count.add(1);
      continue;
    }

    this->metrics_.build_ok_count.add(1);
    this->metrics_.build_rate_15s.update(this->metrics_.build_ok_count.load());

    LOG_EVERY_N(INFO, 10000) << BATT_INSPECT(this->metrics_.push_count)
                             << BATT_INSPECT(this->metrics_.pop_count)
                             << BATT_INSPECT(this->metrics_.build_ok_count)
                             << BATT_INSPECT(this->metrics_.build_error_count)
                             << BATT_INSPECT(this->metrics_.push_rate_15s.get())
                             << BATT_INSPECT(this->metrics_.build_rate_15s.get());

    // Start writing the page asynchronously.
    //
    this->filter_device_->write(std::move(*filter_buffer), [](batt::Status /*ignored_for_now*/) {
      // TODO [tastolfi 2025-01-06] add to cache?
    });
  }
}

}  //namespace llfs
