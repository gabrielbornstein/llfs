//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_BLOOM_FILTER_PAGE_HPP
#define LLFS_PACKED_BLOOM_FILTER_PAGE_HPP

#include <llfs/bloom_filter.hpp>
#include <llfs/packed_page_id.hpp>

namespace llfs {

struct PackedBloomFilterPage {
  PackedPageId src_page_id;
  PackedBloomFilter bloom_filter;
};

}  //namespace llfs

#endif  // LLFS_PACKED_BLOOM_FILTER_PAGE_HPP
