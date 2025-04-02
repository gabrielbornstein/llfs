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

#include <random>

namespace {

using namespace llfs;

template <typename Rng>
std::string pick_key(Rng& rng)
{
  std::uniform_int_distribution<char> pick_char{'a', 'z'};
  std::array<char, 6> chars;

  for (char& ch : chars) {
    ch = pick_char(rng);
  }

  return std::string(chars.data(), chars.size());
}

TEST(QuotientFilterTest, Test)
{
  std::default_random_engine rng{1};

  for (usize n = 0; n < 100000000; ++n) {
    std::vector<std::string> keys;
    for (usize i = 0; i < 20; ++i) {
      keys.emplace_back(pick_key(rng));
    }

    std::array<u8, (usize{1} << 8)> buffer;

    PackedQuotientFilter* filter = build_quotient_filter(
        keys,
        [](const std::string& key) -> KeyView {
          return key;
        },
        MutableBuffer{buffer.data(), buffer.size()});

    ASSERT_NE(filter, nullptr);
  }
}

}  // namespace
