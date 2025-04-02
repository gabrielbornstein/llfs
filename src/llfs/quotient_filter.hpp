//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_QUOTIENT_FILTER_HPP
#define LLFS_QUOTIENT_FILTER_HPP

#include <llfs/config.hpp>
//

#include <llfs/buffer.hpp>
#include <llfs/filter_hash.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/slice.hpp>
#include <llfs/status.hpp>

#include <batteries/bit_ops.hpp>
#include <batteries/math.hpp>
#include <batteries/small_vec.hpp>
#include <batteries/suppress.hpp>

#include <array>
#include <vector>

namespace llfs {

struct PackedQuotientFilterBucket {
  little_u64 group_ends;
  std::array<u8, 32> fingerprints;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i32 start_of_group(i32 i) const
  {
    const u64 b = this->group_ends.value();
    return batt::bit_rank(~b, batt::bit_select(b, i - 1));
  }

  auto group(i32 i) const
  {
    return Slice<const u8>{
        this->fingerprints.data() + this->start_of_group(i),
        this->fingerprints.data() + this->start_of_group(i + 1),
    };
  }

  i32 size() const
  {
    return this->start_of_group(32);
  }
};

struct PackedQuotientFilter {
  little_u8 hash_fn_i;
  little_u8 pad_[7];
  PackedArray<PackedQuotientFilterBucket> buckets;
};

struct Bucket {
  i32 size = 0;
  std::array<batt::SmallVec<u8, 32>, 32> groups;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const
  {
    return this->size == 0;
  }

  void compact()
  {
    usize new_size = 0;
    for (auto& group : this->groups) {
      std::sort(group.begin(), group.end());
      group.erase(std::unique(group.begin(), group.end()), group.end());
      new_size += group.size();
    }
    this->size = new_size;
  }
};

BATT_SUPPRESS_IF_GCC("-Warray-bounds")

template <typename ItemsRangeT, typename GetKeyFn>
inline PackedQuotientFilter* build_quotient_filter(const ItemsRangeT& items,
                                                   const GetKeyFn& get_key,
                                                   MutableBuffer dst_buffer)
{
  auto* const packed_filter = static_cast<PackedQuotientFilter*>(dst_buffer.data());

  dst_buffer += sizeof(PackedQuotientFilter);

  const auto items_begin = std::begin(items);
  const auto items_end = std::end(items);
  const usize item_count = std::distance(items_begin, items_end);

  const isize bucket_overhead = sizeof(PackedQuotientFilterBucket);
  const isize fingerprints_size = item_count;
  const isize size_for_buckets = (isize)dst_buffer.size() - fingerprints_size;

  if (size_for_buckets < bucket_overhead) {
    return nullptr;
  }

  const usize bucket_count_upper_bound = size_for_buckets / bucket_overhead;
  const i32 bucket_bits = batt::log2_floor(bucket_count_upper_bound);
  const u64 bucket_count = u64{1} << bucket_bits;
  const u64 bucket_mask = bucket_count - 1;

  packed_filter->buckets.initialize(bucket_count);

  const i32 fingerprint_bits = 8;
  const u64 fingerprint_mask = (u64{1} << fingerprint_bits) - 1;

  const i32 group_index_bits = 5;
  const u64 group_index_mask = (u64{1} << group_index_bits) - 1;

  const i32 remainder_bits = group_index_bits + fingerprint_bits;

  std::vector<Bucket> buckets(bucket_count);

  for (usize hash_fn_i = 0; hash_fn_i < 64; ++hash_fn_i) {
    packed_filter->hash_fn_i = hash_fn_i;

    bool failed = false;
    for (const auto& item : items) {
      u64 hash_val = get_nth_hash_for_filter(get_key(item), 0);
      u64 bucket_i = (hash_val >> remainder_bits) & bucket_mask;

      Bucket& bucket = buckets[bucket_i];

      if (bucket.size == 32) {
        bucket.compact();
        if (bucket.size == 32) {
          buckets.clear();
          buckets.resize(bucket_count);
          failed = true;
          break;
        }
      }

      u64 group_i = (hash_val >> fingerprint_bits) & group_index_mask;
      u8 fingerprint = hash_val & fingerprint_mask;

      buckets[bucket_i].size += 1;
      buckets[bucket_i].groups[group_i].push_back(fingerprint);
    }
    if (failed) {
      if (hash_fn_i == 63) {
        return nullptr;
      }
      continue;
    }
    break;
  }

  for (usize bucket_i = 0; bucket_i < bucket_count; ++bucket_i) {
    Bucket& bucket = buckets[bucket_i];
    PackedQuotientFilterBucket& packed_bucket = packed_filter->buckets[bucket_i];

    if (bucket.empty()) {
      packed_bucket.group_ends = (u64{1} << 32) - 1;
      std::memset(&packed_bucket.fingerprints, 0, 32);

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      for (i32 group_i = 0; group_i <= 32; ++group_i) {
        BATT_CHECK_EQ(packed_bucket.start_of_group(group_i), -1) << BATT_INSPECT(group_i);
        BATT_CHECK_EQ(packed_bucket.group(group_i).size(), 0);
      }

      //+++++++++++-+-+--+----- --- -- -  -  -   -
      continue;
    }

    bucket.compact();

    //----- --- -- -  -  -   -
    batt::SmallVec<i32, 32> expected_group_start;
    expected_group_start.push_back(0);
    //----- --- -- -  -  -   -

    u64 group_ends = 0;
    i32 pos = 0;
    i32 fingerprint_i = 0;

    for (i32 group_i = 0; group_i < 32; ++group_i) {
      batt::SmallVec<u8, 32>& group = bucket.groups[group_i];

      pos += 1 + group.size();
      group_ends = batt::set_bit(group_ends, pos, true);

      for (u8 fingerprint : group) {
        BATT_CHECK_LT(fingerprint_i, 32);
        packed_bucket.fingerprints[fingerprint_i] = fingerprint;
        ++fingerprint_i;
      }

      //----- --- -- -  -  -   -
      expected_group_start.push_back(fingerprint_i);
      //----- --- -- -  -  -   -
    }

    packed_bucket.group_ends = group_ends;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Sanity check
    //
    if (false) {
      std::cerr << BATT_INSPECT(std::bitset<64>{group_ends}) << std::endl
                << BATT_INSPECT_RANGE(expected_group_start) << std::endl;
    }

    BATT_CHECK_EQ(packed_bucket.size(), bucket.size);

    for (i32 group_i = 0; group_i < 32; ++group_i) {
      BATT_CHECK_EQ(packed_bucket.start_of_group(group_i), expected_group_start[group_i]);
      BATT_CHECK_EQ(packed_bucket.group(group_i).size(), bucket.groups[group_i].size());
    }
    //
    //+++++++++++-+-+--+----- --- -- -  -  -   -
  }

  return packed_filter;
}

BATT_UNSUPPRESS_IF_GCC()

}  //namespace llfs

#endif  // LLFS_QUOTIENT_FILTER_HPP
