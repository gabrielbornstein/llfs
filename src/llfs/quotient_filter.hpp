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
#include <llfs/int_types.hpp>
#include <llfs/key.hpp>
#include <llfs/packed_array.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/slice.hpp>
#include <llfs/status.hpp>

#include <batteries/bit_ops.hpp>
#include <batteries/math.hpp>
#include <batteries/small_vec.hpp>
#include <batteries/suppress.hpp>

#include <xxhash.h>

#include <algorithm>
#include <array>
#include <random>
#include <vector>

namespace llfs {

BATT_SUPPRESS_IF_GCC("-Warray-bounds")

struct PackedQuotientFilterBucket {
  static constexpr usize kNumGroups = 32;
  static constexpr usize kFingerprintsSize = kNumGroups;

  static constexpr i32 kFingerprintBits = 8;
  static constexpr u64 kFingerprintMask = (u64{1} << kFingerprintBits) - 1;

  static constexpr i32 kGroupIndexBits = 5;
  static constexpr u64 kGroupIndexMask = (u64{1} << kGroupIndexBits) - 1;

  static constexpr i32 kRemainderBits = kGroupIndexBits + kFingerprintBits;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static u8 fingerprint_from_hash(u64 hash_val)
  {
    return hash_val & kFingerprintMask;
  }

  static i32 group_index_from_hash(u64 hash_val)
  {
    return fingerprint_from_hash(hash_val) >> 3;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  little_u64 group_ends;
  std::array<u8, kFingerprintsSize> fingerprints;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BATT_ALWAYS_INLINE i32 start_of_group(i32 i) const
  {
    const u64 b = this->group_ends.value();
    return batt::bit_rank(~b << 1, batt::bit_select(b, i - 1)) + 1;
  }

  BATT_ALWAYS_INLINE auto group(i32 i) const
  {
    return Slice<const u8>{
        this->fingerprints.data() + this->start_of_group(i),
        this->fingerprints.data() + this->start_of_group(i + 1),
    };
  }

  void clear()
  {
    this->group_ends = (u64{1} << kFingerprintsSize) - 1;
    this->fingerprints.fill(0);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Sanity check
    //
    static std::atomic<bool> first_time{true};
    if (first_time.exchange(false)) {
      for (i32 group_i = 0; group_i <= 32; ++group_i) {
        BATT_CHECK_EQ(this->start_of_group(group_i), 0) << BATT_INSPECT(group_i);
        BATT_CHECK_EQ(this->group(group_i).size(), 0);
      }
    }
    //+++++++++++-+-+--+----- --- -- -  -  -   -
  }

  i32 size() const
  {
    return this->start_of_group(this->fingerprints.size());
  }

  BATT_ALWAYS_INLINE bool might_contain_hash(u64 hash_val) const
  {
    const i32 group_i = PackedQuotientFilterBucket::group_index_from_hash(hash_val);
    const u8 fingerprint = PackedQuotientFilterBucket::fingerprint_from_hash(hash_val);

    auto group_slice = this->group(group_i);

    if (true) {
      auto it = std::lower_bound(group_slice.begin(), group_slice.end(), fingerprint);
      return it != group_slice.end() && *it == fingerprint;
    } else {
      for (const u8 stored : group_slice) {
        if (stored == fingerprint) {
          return true;
        }
      }
      return false;
    }
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedQuotientFilterBucket), 40);

struct PackedQuotientFilter {
  little_u64 hash_seed;
  batt::fixed_point::LinearProjection<u64, little_u32> hash_to_bucket;
  u8 pad_[6];
  PackedArray<PackedQuotientFilterBucket> buckets;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BATT_ALWAYS_INLINE u64 hash_for_key(const KeyView& key) const
  {
    return XXH64(key.data(), key.size(), this->hash_seed);
  }

  BATT_ALWAYS_INLINE bool might_contain_hash(u64 hash_val) const
  {
    const u32 bucket_i = this->hash_to_bucket(hash_val);
    BATT_CHECK_LT(bucket_i, this->buckets.size());

    return this->buckets[bucket_i].might_contain_hash(hash_val);
  }

  BATT_ALWAYS_INLINE bool might_contain_key(const KeyView& key) const
  {
    return this->might_contain_hash(this->hash_for_key(key));
  }
};

struct QuotientFilterBucket;

template <typename ItemsRangeT, typename GetKeyFn>
class QuotientFilterBuilder
{
 public:
  using Iter = std::decay_t<decltype(std::begin(std::declval<const ItemsRangeT&>()))>;

  static constexpr bool kExtraChecks = true;

  static constexpr i32 kFingerprintBits = PackedQuotientFilterBucket::kFingerprintBits;
  static constexpr u64 kFingerprintMask = PackedQuotientFilterBucket::kFingerprintMask;
  static constexpr i32 kGroupIndexBits = PackedQuotientFilterBucket::kGroupIndexBits;
  static constexpr u64 kGroupIndexMask = PackedQuotientFilterBucket::kGroupIndexMask;
  static constexpr i32 kRemainderBits = PackedQuotientFilterBucket::kRemainderBits;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  using Bucket = QuotientFilterBucket;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit QuotientFilterBuilder(const ItemsRangeT& items, const GetKeyFn& get_key_fn,
                                 const MutableBuffer& dst_buffer) noexcept
      : items_{items}
      , get_key_fn_{get_key_fn}
      , dst_buffer_{dst_buffer}
  {
  }

  PackedQuotientFilter* build();

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  [[nodiscard]] bool try_pack_buckets(u64 hash_seed);

  [[nodiscard]] bool try_build_buckets(u64 hash_seed, Slice<Bucket> buckets);

  void pack_buckets(Slice<Bucket> buckets);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  /** \brief The input item range.
   */
  const ItemsRangeT& items_;

  /** \brief Function that takes an element of `items_` and returns a KeyView.
   */
  const GetKeyFn& get_key_fn_;

  /** \brief The output buffer.
   */
  const MutableBuffer& dst_buffer_;

  /** \brief The header at the front of the output buffer.
   */
  PackedQuotientFilter* const packed_filter_ = [&] {
    BATT_CHECK_GT(this->dst_buffer_.size(), sizeof(PackedQuotientFilter));
    return static_cast<PackedQuotientFilter*>(this->dst_buffer_.data());
  }();

  /** \brief The portion of the output buffer that comes after the header.
   */
  const MutableBuffer buckets_buffer_ = this->dst_buffer_ + sizeof(PackedQuotientFilter);

  /** \brief Iterator pointing at the start of the input range.
   */
  const Iter items_begin_ = std::begin(this->items_);

  /** \brief Iterator pointing at (one past) the end of the input range.
   */
  const Iter items_end_ = std::end(this->items_);

  /** \brief The number of elements in the input range.
   */
  const usize item_count_ = std::distance(this->items_begin_, this->items_end_);

  /** \brief The number of buckets that will fit in the output buffer.
   */
  const usize bucket_count_ = this->buckets_buffer_.size() / sizeof(PackedQuotientFilterBucket);
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsRangeT, typename GetKeyFn>
inline PackedQuotientFilter* build_quotient_filter(const ItemsRangeT& items,
                                                   const GetKeyFn& get_key,
                                                   const MutableBuffer& dst_buffer)
{
  QuotientFilterBuilder<ItemsRangeT, GetKeyFn> builder{items, get_key, dst_buffer};
  return builder.build();
}

BATT_UNSUPPRESS_IF_GCC()

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct QuotientFilterBucket {
  i32 size = 0;
  std::array<batt::SmallVec<u8, 32>, 32> groups;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const
  {
    return this->size == 0;
  }

  void insert(u64 hash_val)
  {
    const u8 fingerprint = PackedQuotientFilterBucket::fingerprint_from_hash(hash_val);
    const i32 group_i = PackedQuotientFilterBucket::group_index_from_hash(hash_val);

    this->size += 1;
    this->groups[group_i].push_back(fingerprint);
  }

  /** \brief Makes more space in this Bucket, possibly by truncating fingerprints.
   *
   * This function tries two things to free up space in the bucket:
   *  1. sort and de-duplicate the fingerprint groups
   *  2. (if 1 fails) truncate one bit from the fingerprints and retry 1
   */
  void compact()
  {
    [[maybe_unused]] bool only_singleton_groups = true;
    i32 new_size = 0;
    for (auto& group : this->groups) {
      std::sort(group.begin(), group.end());
      group.erase(std::unique(group.begin(), group.end()), group.end());
      new_size += group.size();
      if (group.size() > 1) {
        only_singleton_groups = false;
      }
    }

#if 0
    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
    // Try strategy 2.
    //
    if (new_size == this->size) {
      if (only_singleton_groups) {
        return;
      }

      for (auto& group : this->groups) {
        for (u8& fingerprint : group) {
          fingerprint >>= 1;
        }
      }
      return this->compact();

    } else {
    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
#endif
    this->size = new_size;
  }
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsRangeT, typename GetKeyFn>
inline PackedQuotientFilter* QuotientFilterBuilder<ItemsRangeT, GetKeyFn>::build()
{
  this->packed_filter_->hash_to_bucket.reset_output_range(this->bucket_count_);
  this->packed_filter_->buckets.initialize(this->bucket_count_);

  std::default_random_engine rng{std::random_device{}()};
  std::uniform_int_distribution pick_hash_seed{u64{0}, ~u64{0}};

  //  for (i32 max_truncate_bits = 0; max_truncate_bits < 2; ++max_truncate_bits) {
  for (usize hash_fn_i = 0; hash_fn_i < 16; ++hash_fn_i) {
    const u64 hash_seed = pick_hash_seed(rng);
    if (this->try_pack_buckets(hash_seed)) {
      this->packed_filter_->hash_seed = hash_seed;
      return this->packed_filter_;
    }
  }
  // }
  return nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsRangeT, typename GetKeyFn>
inline bool QuotientFilterBuilder<ItemsRangeT, GetKeyFn>::try_pack_buckets(u64 hash_seed)
{
  std::vector<Bucket> buckets(this->bucket_count_);

  if (!this->try_build_buckets(hash_seed, as_slice(buckets))) {
    return false;
  }

  this->pack_buckets(as_slice(buckets));

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsRangeT, typename GetKeyFn>
bool QuotientFilterBuilder<ItemsRangeT, GetKeyFn>::try_build_buckets(u64 hash_seed,
                                                                     Slice<Bucket> buckets)
{
  for (const auto& item : this->items_) {
    const KeyView& key = this->get_key_fn_(item);
    const u64 hash_val = XXH64(key.data(), key.size(), hash_seed);
    const u64 bucket_i = this->packed_filter_->hash_to_bucket(hash_val);
    Bucket& bucket = buckets[bucket_i];

    bucket.insert(hash_val);

    if (bucket.size > 32) {
      bucket.compact();
      if (bucket.size > 32) {
        return false;
      }
      /*
      if (bucket.truncate_bits > max_truncate_bits) {
        return false;
      }
      */
    }
  }

  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename ItemsRangeT, typename GetKeyFn>
void QuotientFilterBuilder<ItemsRangeT, GetKeyFn>::pack_buckets(Slice<Bucket> buckets)
{
  BATT_CHECK_EQ(buckets.size(), this->bucket_count_);
  BATT_CHECK_EQ(buckets.size(), this->packed_filter_->buckets.size());

  for (usize bucket_i = 0; bucket_i < this->bucket_count_; ++bucket_i) {
    Bucket& bucket = buckets[bucket_i];
    PackedQuotientFilterBucket& packed_bucket = this->packed_filter_->buckets[bucket_i];

    if (bucket.empty()) {
      packed_bucket.clear();
      continue;
    }

    bucket.compact();

    batt::SmallVec<i32, 32> expected_group_start;
    if (kExtraChecks) {
      expected_group_start.push_back(0);
    }

    auto debug_info = [&](std::ostream& out) {
      for (usize i = 0; i < bucket.groups.size(); ++i) {
        out << "group[" << std::setw(2) << std::setfill(' ') << i
            << "]: " << BATT_INSPECT(packed_bucket.start_of_group(i))
            << BATT_INSPECT(expected_group_start[i]) << std::endl
            << "    bucket=" << batt::dump_range(bucket.groups[i]) << std::endl
            << "    packed=" << batt::dump_range(packed_bucket.group(i)) << std::endl;
      }
    };

    u64 group_ends = 0;
    i32 pos = 0;
    i32 fingerprint_i = 0;

    for (i32 group_i = 0; group_i < (i32)PackedQuotientFilterBucket::kNumGroups; ++group_i) {
      batt::SmallVecBase<u8>& group = bucket.groups[group_i];

      pos += group.size();
      group_ends = batt::set_bit(group_ends, pos, true);
      pos += 1;

      for (u8 fingerprint : group) {
        BATT_CHECK_LT(fingerprint_i, 32) << BATT_INSPECT(bucket.size) << std::endl << debug_info;
        packed_bucket.fingerprints[fingerprint_i] = fingerprint;
        ++fingerprint_i;
      }

      if (kExtraChecks) {
        expected_group_start.push_back(fingerprint_i);
      }
    }

    packed_bucket.group_ends = group_ends;
    //packed_bucket.truncate_bits = BATT_CHECKED_CAST(i8, bucket.truncate_bits);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // Sanity check
    //
    BATT_CHECK_EQ(packed_bucket.size(), bucket.size)
        << std::endl
        << debug_info << BATT_INSPECT(std::bitset<64>{packed_bucket.group_ends});

    if (kExtraChecks) {
      for (i32 group_i = 0; group_i < (i32)PackedQuotientFilterBucket::kNumGroups; ++group_i) {
        BATT_CHECK_EQ(packed_bucket.start_of_group(group_i), expected_group_start[group_i])
            << std::endl
            << debug_info;

        BATT_CHECK_LE(expected_group_start[group_i], expected_group_start[group_i + 1]);

        BATT_CHECK_EQ(packed_bucket.group(group_i).size(), bucket.groups[group_i].size());

        const usize group_size = bucket.groups[group_i].size();
        for (usize elem_i = 0; elem_i < group_size; ++elem_i) {
          BATT_CHECK_EQ((int)bucket.groups[group_i][elem_i],
                        (int)packed_bucket.group(group_i)[elem_i]);
        }
      }
    }
    //
    //+++++++++++-+-+--+----- --- -- -  -  -   -
  }
}

}  //namespace llfs

#endif  // LLFS_QUOTIENT_FILTER_HPP
