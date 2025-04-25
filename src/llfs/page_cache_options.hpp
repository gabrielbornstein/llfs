//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_OPTIONS_HPP
#define LLFS_PAGE_CACHE_OPTIONS_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/constants.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_size.hpp>

#include <batteries/assert.hpp>
#include <batteries/bit_ops.hpp>
#include <batteries/math.hpp>

#include <array>
#include <set>

namespace llfs {

class PageCacheOptions
{
 public:
  static PageCacheOptions with_default_values();

  u64 default_log_size() const
  {
    return this->default_log_size_;
  }

#if 0
  PageCacheOptions& set_max_cached_pages_per_size(PageSize page_size, usize n)
  {
    const int page_size_log2 = batt::log2_ceil(page_size);
    BATT_CHECK_EQ(page_size_log2, batt::log2_floor(page_size));
    BATT_CHECK_LT(page_size_log2, kMaxPageSizeLog2);
    this->max_cached_pages_per_size_log2[page_size_log2] = n;
    return *this;
  }
#endif

  PageCacheOptions& add_sharded_view(PageSize page_size, PageSize shard_size)
  {
    BATT_CHECK_LT(shard_size, page_size);
    BATT_CHECK_EQ(batt::bit_count(page_size.value()), 1);
    BATT_CHECK_EQ(batt::bit_count(shard_size.value()), 1);

    this->sharded_views.emplace(page_size, shard_size);
    return *this;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheOptions& set_byte_size(usize max_size,
                                  Optional<PageSize> default_page_size = PageSize{4096})
  {
    this->max_cache_size_bytes = MaxCacheSizeBytes{max_size};
    if (default_page_size) {
      this->cache_slot_count = SlotCount{this->max_cache_size_bytes / *default_page_size};
    }

    return *this;
  }

  SlotCount cache_slot_count;

  MaxCacheSizeBytes max_cache_size_bytes;

#if 0
  std::array<usize, kMaxPageSizeLog2> max_cached_pages_per_size_log2;
#endif

  std::set<std::pair<PageSize, PageSize>> sharded_views;

 private:
  u64 default_log_size_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_OPTIONS_HPP
