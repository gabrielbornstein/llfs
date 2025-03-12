//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_LOADER_HPP
#define LLFS_PAGE_LOADER_HPP

#include <llfs/api_types.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_id_slot.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_loader_decl.hpp>
#include <llfs/status.hpp>

namespace llfs {

// Interface for an entity which can resolve `PageId`s into `PinnedPage`s.
//
template <typename PinnedPageParamT>
class BasicPageLoader
{
 public:
  using PinnedPageT = PinnedPageParamT;

  BasicPageLoader(const BasicPageLoader&) = delete;
  BasicPageLoader& operator=(const BasicPageLoader&) = delete;

  virtual ~BasicPageLoader() = default;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual void prefetch_hint(PageId page_id) = 0;

  virtual StatusOr<PinnedPageT> get_page_with_layout_in_job(
      PageId page_id, const Optional<PageLayoutId>& required_layout, PinPageToJob pin_page_to_job,
      OkIfNotFound ok_if_not_found) = 0;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_slot_with_layout_in_job(
      const PageIdSlot& page_id_slot, const Optional<PageLayoutId>& required_layout,
      PinPageToJob pin_page_to_job, OkIfNotFound ok_if_not_found)
  {
    return page_id_slot.load_through(*this, required_layout, pin_page_to_job, ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_slot_with_layout(
      const PageIdSlot& page_id_slot, const Optional<PageLayoutId>& required_layout,
      OkIfNotFound ok_if_not_found)
  {
    return this->get_page_slot_with_layout_in_job(page_id_slot, required_layout,
                                                  PinPageToJob::kDefault, ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_slot_in_job(const PageIdSlot& page_id_slot,
                                                     PinPageToJob pin_page_to_job,
                                                     OkIfNotFound ok_if_not_found)
  {
    return this->get_page_slot_with_layout_in_job(page_id_slot, /*required_layout=*/None,
                                                  pin_page_to_job, ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_slot(const PageIdSlot& page_id_slot,
                                              OkIfNotFound ok_if_not_found)
  {
    return this->get_page_slot_with_layout_in_job(page_id_slot, /*required_layout=*/None,
                                                  PinPageToJob::kDefault, ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_with_layout(PageId page_id,
                                                     const Optional<PageLayoutId>& required_layout,
                                                     OkIfNotFound ok_if_not_found)
  {
    return this->get_page_with_layout_in_job(page_id, required_layout, PinPageToJob::kDefault,
                                             ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_in_job(PageId page_id, PinPageToJob pin_page_to_job,
                                                OkIfNotFound ok_if_not_found)
  {
    return this->get_page_with_layout_in_job(page_id, /*required_layout=*/None, pin_page_to_job,
                                             ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page(PageId page_id, OkIfNotFound ok_if_not_found)
  {
    return this->get_page_with_layout(page_id, /*required_layout=*/None, ok_if_not_found);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  virtual StatusOr<PinnedPageT> get_page_slot_ref_with_layout_in_job(
      PageId page_id, PageCacheSlot::AtomicRef& slot_ref,
      const Optional<PageLayoutId>& required_layout, PinPageToJob pin_page_to_job,
      OkIfNotFound ok_if_not_found)
  {
    return PageIdSlot::load_through_impl(slot_ref, *this, required_layout, pin_page_to_job,
                                         ok_if_not_found, page_id);
  }

 protected:
  BasicPageLoader() = default;
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace llfs

#endif  // LLFS_PAGE_LOADER_HPP
