//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

//#include <llfs/binary_fuse_filter.hpp>
//
//#include <llfs/binary_fuse_filter.hpp>

#include <FastFilter/binaryfusefilter.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/stable_string_store.hpp>

#include <batteries/metrics/metric_collectors.hpp>

#include <xxhash.h>

#include <random>
#include <unordered_set>
#include <vector>

namespace {

using namespace llfs;

using batt::Every2ToTheConst;
using batt::LatencyMetric;
using batt::LatencyTimer;
using batt::StatsMetric;

template <typename Rng>
std::string pick_key(Rng& rng)
{
  std::uniform_int_distribution<char> pick_char{'a', 'z'};
  std::array<char, 9> chars;

  for (char& ch : chars) {
    ch = pick_char(rng);
  }

  return std::string(chars.data(), chars.size());
}

TEST(BinaryFuseFilterTest, Test)
{
  std::default_random_engine rng{std::random_device{}()};
  std::uniform_int_distribution pick_hash_seed{u64{0}, ~u64{0}};

  double positive_queries = 0;
  double negative_queries = 0;
  double false_positives = 0;

  LatencyMetric build_latency;
  LatencyMetric query_latency;

  const usize key_count = 13786;

  for (usize n = 0; n < 10; ++n) {
    const u64 hash_seed = pick_hash_seed(rng);
    std::vector<std::string> keys;
    std::vector<u64> hash_vals;

    for (usize key_i = 0; key_i < key_count; ++key_i) {
      keys.emplace_back(pick_key(rng));
      const std::string& key = keys.back();
      hash_vals.emplace_back(XXH64(key.data(), key.size(), hash_seed));
    }

    std::unordered_set<std::string> key_set(keys.begin(), keys.end());

    binary_fuse8_t filter;
    {
      LatencyTimer timer{build_latency};

      bool is_ok = binary_fuse8_allocate(hash_vals.size(), &filter);
      ASSERT_TRUE(is_ok);

      is_ok = binary_fuse8_populate(hash_vals.data(), hash_vals.size(), &filter);
      ASSERT_TRUE(is_ok);
    }

    for (u64 val : hash_vals) {
      EXPECT_TRUE(binary_fuse8_contain(val, &filter));
    }

    {
      std::vector<std::string> query_keys;
      for (usize i = 0; i < 1000; ++i) {
        query_keys.emplace_back(pick_key(rng));
      }

      usize count = 0;

      LatencyTimer timer{query_latency, query_keys.size()};

      for (const std::string& key : query_keys) {
        const u64 val = XXH64(key.data(), key.size(), hash_seed);
        const bool filter_result = binary_fuse8_contain(val, &filter);
        count += filter_result;
      }
      timer.stop();

      std::cerr << BATT_INSPECT(count) << std::endl;
    }

    for (usize j = 0; j < keys.size() * 1000 || false_positives < 10; ++j) {
      // LatencyTimer timer{Every2ToTheConst<10>{}, query_latency};
      std::string query_key = pick_key(rng);
      const u64 val = XXH64(query_key.data(), query_key.size(), hash_seed);
      const bool filter_result = binary_fuse8_contain(val, &filter);
      //timer.stop();

      const bool key_in_set = key_set.count(query_key);

      if (key_in_set) {
        positive_queries += 1;
      } else {
        negative_queries += 1;
      }

      if (filter_result) {
        if (!key_in_set) {
          false_positives += 1;
        }
      } else {
        ASSERT_FALSE(key_in_set);
      }
    }

    std::cerr << " α=" << (false_positives / negative_queries) << " keys=" << key_count
              << BATT_INSPECT(build_latency) << BATT_INSPECT(query_latency) << std::endl;

    binary_fuse8_free(&filter);
  }
}

}  // namespace
