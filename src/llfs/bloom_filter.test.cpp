//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter.hpp>
//
#include <llfs/bloom_filter.hpp>

#include <llfs/metrics.hpp>
#include <llfs/slice.hpp>

#include <batteries/segv.hpp>

#include <random>
#include <sstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace {

using llfs::as_slice;
//using llfs::BloomFilterParams;
using llfs::LatencyMetric;
using llfs::LatencyTimer;
//using llfs::packed_sizeof_bloom_filter;
using llfs::PackedBloomFilter;
using llfs::parallel_build_bloom_filter;

using namespace llfs::int_types;

using batt::WorkerPool;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

template <typename Rng>
std::string make_random_word(Rng& rng)
{
  static std::exponential_distribution<> pick_negative_len;
  static std::uniform_int_distribution<char> pick_letter('a', 'z');

  static constexpr double kMaxLen = 12;

  double nl = std::min(kMaxLen - 1, pick_negative_len(rng));
  usize len = kMaxLen - nl + 1;
  std::ostringstream oss;
  for (usize i = 0; i < len; ++i) {
    oss << pick_letter(rng);
  }
  return std::move(oss).str();
}

struct QueryStats {
  usize total = 0;
  usize false_positive = 0;
  double expected_rate = 0;

  double actual_rate() const
  {
    return double(this->false_positive) / double(this->total);
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

TEST(BloomFilterTest, RandomItems)
{
  std::vector<std::vector<std::string>> input_sets;
  std::vector<std::string> query_keys;

  // Generate random data for the tests.
  //
  for (usize r = 1; r <= 5; ++r) {
    std::default_random_engine rng{15485863 /*(1M-th prime)*/ * r};

    {
      std::vector<std::string> items;

      for (usize i = 0; i < 10 * 1000; ++i) {
        items.emplace_back(make_random_word(rng));
      }
      for (const auto& s : as_slice(items.data(), 40)) {
        LLFS_VLOG(1) << "sample random word: " << s;
      }
      std::sort(items.begin(), items.end());

      input_sets.emplace_back(std::move(items));
    }

    for (usize j = 0; j < 2000 * 1000; ++j) {
      std::string query_str = make_random_word(rng);
      query_keys.emplace_back(std::move(query_str));
    }
  }

  using AlignedUnit = std::aligned_storage_t<64, 64>;

  // Construct and verify properties of filters with various values of N, M, and layout
  //
  //----- --- -- -  -  -   -
  for (llfs::BloomFilterLayout layout : {
           llfs::BloomFilterLayout::kFlat,
           llfs::BloomFilterLayout::kBlocked64,
           llfs::BloomFilterLayout::kBlocked512,
       }) {
    //----- --- -- -  -  -   -
    for (const usize n_items : {
             1,
             2,
             100,
             500,
             10 * 1000,
         }) {
      constexpr usize kMaxBitsPerItem = 17;
      //----- --- -- -  -  -   -
      for (double bits_per_item = 1; bits_per_item < kMaxBitsPerItem; bits_per_item += 0.37) {
        LLFS_LOG_INFO() << "n=" << n_items << " m/n=" << bits_per_item << " layout=" << layout;

        batt::Optional<double> expected_fpr;
        bool bits_per_item_done = false;

        for (const std::vector<std::string>& all_items : input_sets) {
          BATT_CHECK_LE(n_items, all_items.size());
          batt::Slice<const std::string> items = batt::as_slice(all_items.data(), n_items);

          auto config = llfs::BloomFilterConfig::from(layout, llfs::ItemCount{n_items},
                                                      llfs::RealBitCount{bits_per_item});

          if (!expected_fpr) {
            expected_fpr.emplace(config.false_positive_rate);
          } else {
            EXPECT_EQ(*expected_fpr, config.false_positive_rate);
          }

          if (config.bits_per_key >= kMaxBitsPerItem) {
            bits_per_item_done = true;
          }

          std::unique_ptr<AlignedUnit[]> memory{new AlignedUnit[config.word_count() + 1]};

          auto* filter = (PackedBloomFilter*)memory.get();

          filter->initialize(config);

          EXPECT_EQ(filter->word_index_from_hash(u64{0}), 0u);
          EXPECT_EQ(filter->word_index_from_hash(~u64{0}), filter->word_count() - 1);

          for (usize divisor = 2; divisor < 23; ++divisor) {
            EXPECT_THAT((double)filter->word_index_from_hash(~u64{0} / divisor),
                        ::testing::DoubleNear(filter->word_count() / divisor, 1));
          }

          parallel_build_bloom_filter(
              WorkerPool::default_pool(), items.begin(), items.end(),
              /*get_key_fn=*/
              [](const std::string& s) -> std::string_view {
                return std::string_view{s};
              },
              filter);

          //----- --- -- -  -  -   -
          // Helper function; lookup the passed string in items, returning true if present.
          //
          const auto items_contains = [&items](const std::string_view& s) {
            auto iter = std::lower_bound(items.begin(), items.end(), s);
            return iter != items.end() && *iter == s;
          };
          //----- --- -- -  -  -   -

          for (const std::string& s : items) {
            llfs::BloomFilterQuery<std::string_view> query{s};

            EXPECT_TRUE(items_contains(s));
            EXPECT_TRUE(filter->might_contain(s));
            EXPECT_TRUE(filter->query(query));
          }

          double false_positive_count = 0, query_count = 0;

          const auto run_query = [&](std::string_view s) {
            llfs::BloomFilterQuery<std::string_view> query{s};
            bool filter_ans = filter->might_contain(s);
            EXPECT_EQ(filter_ans, filter->query(query));

            bool true_ans = items_contains(s);
            if (true_ans) {
              EXPECT_TRUE(filter_ans);
            }
            query_count += 1;
            LOG_EVERY_N(INFO, 1000 * 1000)
                << "expected=" << *expected_fpr << " actual=" << false_positive_count << "/"
                << query_count;
            if (filter_ans && !true_ans) {
              false_positive_count += 1;
            }
          };

          for (const std::string& s : query_keys) {
            run_query(s);
            if (false_positive_count > 100) {
              break;
            }
          }

          double actual_fpr = false_positive_count / query_count;

          if (1.0 / (*expected_fpr) > query_count) {
            ASSERT_LE(actual_fpr, *expected_fpr)
                << BATT_INSPECT(false_positive_count) << BATT_INSPECT(query_count)
                << BATT_INSPECT(config) << filter->dump();
          } else {
            ASSERT_THAT(actual_fpr, ::testing::DoubleNear(*expected_fpr, 1e-2))
                << BATT_INSPECT(false_positive_count) << BATT_INSPECT(query_count)
                << BATT_INSPECT(config) << filter->dump();
          }
        }

        if (bits_per_item_done) {
          break;
        }

#if 0
        std::map<std::pair<u64, u16>, QueryStats> stats;

        LatencyMetric build_latency;
        LatencyMetric query_latency;

        double false_positive_rate_total = 0.0;
        double false_positive_rate_count = 0.0;


        const BloomFilterParams params{
            .bits_per_item = bits_per_item,
        };

        for (usize r = 0; r < 25; ++r) {
          *filter = PackedBloomFilter::from_params(params, items.size(), layout);

          const double actual_bit_rate = double(filter->word_count() * 64) / double(items.size());

          LLFS_VLOG(1) << BATT_INSPECT(n_items) << " (target)" << BATT_INSPECT(bits_per_item)
                       << BATT_INSPECT(filter->word_count()) << BATT_INSPECT(filter->hash_count)
                       << " bit_rate == " << actual_bit_rate;

          {
            LatencyTimer build_timer{build_latency, items.size()};

            BATT_DEBUG_INFO(BATT_INSPECT(n_items)
                            << BATT_INSPECT(bits_per_item) << BATT_INSPECT(filter->word_count())
                            << BATT_INSPECT(filter->block_count()) << BATT_INSPECT((i32)layout));

            parallel_build_bloom_filter(
                WorkerPool::default_pool(), items.begin(), items.end(),
                [](const auto& v) -> decltype(auto) {
                  return v;
                },
                filter);
          }

          for (const std::string& s : items) {
            EXPECT_TRUE(filter->might_contain(s));

            llfs::BloomFilterQuery<std::string_view> query{s};
            EXPECT_TRUE(filter->query(query));
          }

          const auto items_contains = [&items](const std::string& s) {
            auto iter = std::lower_bound(items.begin(), items.end(), s);
            return iter != items.end() && *iter == s;
          };

          std::pair<u64, u16> config_key{filter->word_count(), filter->hash_count};
          QueryStats& c_stats = stats[config_key];
          {
            const double k = filter->hash_count;
            const double n = items.size();
            const double m = filter->word_count() * 64 * [&] {
              switch (layout) {
                case PackedBloomFilter::kLayoutBlocked64:
                  return 0.5;
                case PackedBloomFilter::kLayoutBlocked512:
                  return 0.8;
                default:
                  return 1.0;
              }
            }();

            c_stats.expected_rate = std::pow(1 - std::exp(-((k * (n + 0.5)) / (m - 1))), k);
          }

          for (usize j = 0; j < n_items * 10; ++j) {
            std::string query_str = make_random_word(rng);
            c_stats.total += 1;
            const bool ans = LLFS_COLLECT_LATENCY_N(
                query_latency, filter->might_contain(std::string_view{query_str}), 1);

            LLFS_LOG_INFO() << BATT_INSPECT(actual_fpr / expected_fpr) << BATT_INSPECT(word_count)
                            << BATT_INSPECT(hash_count) << BATT_INSPECT(actual_fpr)
                            << BATT_INSPECT(expected_fpr);
          }

          EXPECT_LT(false_positive_rate_total / false_positive_rate_count, 1.05)
              << BATT_INSPECT(n_items);

          if (false) {
            LLFS_LOG_INFO() << BATT_INSPECT(false_positive_rate_total / false_positive_rate_count);

            LLFS_LOG_INFO() << "build latency (per key) == " << build_latency
                            << " build rate (keys/sec) == " << build_latency.rate_per_second();

            LLFS_LOG_INFO() << "normalized query latency (per key*bit) == " << query_latency
                            << " query rate (key*bits/sec) == " << query_latency.rate_per_second();
          }
#endif

      }  // bits_per_item
    }  // n_items
  }  // layout
}

}  // namespace
