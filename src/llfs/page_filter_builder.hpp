//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_FILTER_BUILDER_HPP
#define LLFS_PAGE_FILTER_BUILDER_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_buffer.hpp>
#include <llfs/pinned_page.hpp>

#include <memory>

namespace llfs {

class PageFilterBuilder
{
 public:
  PageFilterBuilder(const PageFilterBuilder&) = delete;
  PageFilterBuilder& operator=(const PageFilterBuilder&) = delete;

  virtual ~PageFilterBuilder() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual bool accepts_page(const PinnedPage& pinned_page) const noexcept = 0;

  virtual Status build_filter(                                   //
      const PinnedPage& src_pinned_page,                         //
      const std::shared_ptr<PageBuffer>& dst_filter_page_buffer  //
      ) noexcept = 0;

 protected:
  PageFilterBuilder() = default;
};

}  //namespace llfs

#endif  // LLFS_PAGE_FILTER_BUILDER_HPP
