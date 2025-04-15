//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/quotient_filter.hpp>
//
#include <llfs/quotient_filter.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/stable_string_store.hpp>

#include <batteries/metrics/metric_collectors.hpp>

#include <random>
#include <unordered_set>
#include <vector>

namespace {

using namespace llfs;

using batt::StatsMetric;

template <typename Rng>
std::string pick_key(Rng& rng)
{
  std::uniform_int_distribution<char> pick_char{'a', 'z'};
  std::array<char, 20> chars;

  for (char& ch : chars) {
    ch = pick_char(rng);
  }

  return std::string(chars.data(), chars.size());
}

struct Filter {
  std::vector<u8> remainders;
};

TEST(QuotientFilterTest, DISABLED_Test)
{
  std::default_random_engine rng{std::random_device{}()};

#if 0
  {
  if (false) {
    StableStringStore store;
    std::vector<std::string_view> all_keys;
    {
      std::array<char, 7> buffer;
      for (buffer[0] = 'a'; buffer[0] <= 'z'; ++buffer[0]) {
        for (buffer[1] = 'a'; buffer[1] <= 'z'; ++buffer[1]) {
          for (buffer[2] = 'a'; buffer[2] <= 'z'; ++buffer[2]) {
            for (buffer[3] = 'a'; buffer[3] <= 'z'; ++buffer[3]) {
              for (buffer[4] = 'a'; buffer[4] <= 'z'; ++buffer[4]) {
                for (buffer[5] = 'a'; buffer[5] <= 'z'; ++buffer[5]) {
                  for (buffer[6] = 'a'; buffer[6] <= 'z'; ++buffer[6]) {
                    all_keys.emplace_back(
                        store.store(std::string_view{buffer.data(), buffer.size()}));
                  }
                }
              }
            }
          }
        }
      }
    }
    std::cerr << BATT_INSPECT(all_keys.size()) << std::endl;
  }

  batt::fixed_point::LinearProjection<u64, u32> to_bucket(100);

  std::unordered_set<std::string> seen;

  std::array<StatsMetric<u64>, 100> bucket_stats;

  std::array<std::unordered_set<u16>, 100> fingers;
  std::array<double, 10> bucket_hist;
  bucket_hist.fill(0);

  double collisions = 0;
  double key_count = 0;

  for (usize i = 0; i < 1e8; ++i) {
    std::string key = pick_key(rng);
    if (seen.count(key)) {
      continue;
    }
    seen.emplace(key);
    key_count += 1;

    const u64 hash_val = get_nth_hash_for_filter(key, 0);

    auto bucket_i = to_bucket(hash_val);
    BATT_CHECK_LT(bucket_i, 100);

    bucket_hist[bucket_i / 10] += 1;

    u16 finger = hash_val & ((u64{1} << 13) - 1);

    auto [iter, inserted] = fingers[bucket_i].emplace(finger);
    if (!inserted) {
      collisions += 1;
    } else {
      bucket_stats[bucket_i].update(1);
    }
    if ((i % (usize)1e6) == 0) {
      std::cerr << i << ": collisions=" << collisions << " rate=" << collisions / key_count
                << " keys=" << seen.size() << std::endl
                << BATT_INSPECT_RANGE(bucket_hist) << std::endl
                << batt::dump_range(bucket_stats) << std::endl
                << std::endl;
    }
  }

  return;
  }
#endif

  for (double bit_rate = 6; bit_rate <= 64; bit_rate += 1) {
    double positive_queries = 0;
    double negative_queries = 0;
    double false_positives = 0;

    std::array<u8, 4096> buffer;
    usize available_size = buffer.size() - sizeof(PackedQuotientFilter);
    double buffer_bit_size = available_size * 8 * 40 / 41;
    usize key_count = (buffer_bit_size / bit_rate) / 2;

    for (usize n = 0; n < 100; ++n) {
      buffer.fill(0);

      std::vector<std::string> keys;

      for (usize i = 0; i < key_count; ++i) {
        keys.emplace_back(pick_key(rng));
      }

      std::unordered_set<std::string> key_set(keys.begin(), keys.end());

      PackedQuotientFilter* filter = build_quotient_filter(
          keys,
          [](const std::string& key) -> KeyView {
            return key;
          },
          MutableBuffer{buffer.data(), buffer.size()});

      if (filter == nullptr) {
        continue;
      }

      ASSERT_NE(filter, nullptr) << BATT_INSPECT(key_count) << BATT_INSPECT(buffer_bit_size)
                                 << BATT_INSPECT(bit_rate) << BATT_INSPECT(n);

      // Verify that all the keys we know are in the set return a positive filter result.
      //
      for (const std::string& key : keys) {
        EXPECT_TRUE(filter->might_contain_key(key));
      }

      for (usize j = 0; j < keys.size() * 1000 || false_positives < 10; ++j) {
        std::string query_key = pick_key(rng);
        const bool key_in_set = key_set.count(query_key);

        if (key_in_set) {
          positive_queries += 1;
        } else {
          negative_queries += 1;
        }

        const bool filter_result = filter->might_contain_key(query_key);

        if (filter_result) {
          if (!key_in_set) {
            false_positives += 1;
          }
        } else {
          ASSERT_FALSE(key_in_set);
        }
      }
    }

    std::cerr << "bits/key=" << bit_rate << " α=" << (false_positives / negative_queries)
              << " keys=" << key_count << std::endl;
  }
}

}  // namespace
