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

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_loader_decl.hpp>
#include <llfs/pin_page_to_job.hpp>
#include <llfs/status.hpp>

#include <batteries/type_traits.hpp>
#include <batteries/typed_args.hpp>

namespace llfs {

class PageCache;

struct PageLoadOptions {
  using Self = PageLoadOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<PageLayoutId> required_layout_ = None;
  PinPageToJob pin_page_to_job_ = PinPageToJob::kDefault;
  OkIfNotFound ok_if_not_found_ = OkIfNotFound{false};
  LruPriority lru_priority_ = LruPriority{1};

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageLoadOptions() = default;
  PageLoadOptions(const PageLoadOptions&) = default;
  PageLoadOptions& operator=(const PageLoadOptions&) = default;

  template <typename... Args, typename = batt::EnableIfNoShadow<Self, Args...>>
  explicit PageLoadOptions(Args&&... args) noexcept
      : required_layout_{batt::get_typed_arg<Optional<PageLayoutId>>(None, args...)}
      , pin_page_to_job_{batt::get_typed_arg<PinPageToJob>(PinPageToJob::kDefault, args...)}
      , ok_if_not_found_{batt::get_typed_arg<OkIfNotFound>(OkIfNotFound{false}, args...)}
      , lru_priority_{batt::get_typed_arg<LruPriority>(LruPriority{1}, args...)}
  {
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Self clone() const
  {
    return *this;
  }

  //----- --- -- -  -  -   -

  Self& required_layout(const Optional<PageLayoutId>& value)
  {
    this->required_layout_ = value;
    return *this;
  }

  const Optional<PageLayoutId>& required_layout() const
  {
    return this->required_layout_;
  }

  //----- --- -- -  -  -   -

  Self& pin_page_to_job(PinPageToJob value)
  {
    this->pin_page_to_job_ = value;
    return *this;
  }

  Self& pin_page_to_job(bool value)
  {
    this->pin_page_to_job_ = value ? PinPageToJob::kTrue : PinPageToJob::kFalse;
    return *this;
  }

  PinPageToJob pin_page_to_job() const
  {
    return this->pin_page_to_job_;
  }

  //----- --- -- -  -  -   -

  Self& ok_if_not_found(bool value)
  {
    this->ok_if_not_found_ = OkIfNotFound{value};
    return *this;
  }

  OkIfNotFound ok_if_not_found() const
  {
    return this->ok_if_not_found_;
  }

  //----- --- -- -  -  -   -

  Self& lru_priority(i64 value)
  {
    this->lru_priority_ = LruPriority{value};
    return *this;
  }

  LruPriority lru_priority() const
  {
    return this->lru_priority_;
  }
};

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

  virtual PageCache* page_cache() const = 0;

  virtual void prefetch_hint(PageId page_id) = 0;

  virtual StatusOr<PinnedPageT> try_pin_cached_page(PageId page_id,
                                                    const PageLoadOptions& options) = 0;

  virtual StatusOr<PinnedPageT> load_page(PageId page_id, const PageLoadOptions& options) = 0;

 protected:
  BasicPageLoader() = default;
};

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace llfs

#endif  // LLFS_PAGE_LOADER_HPP
