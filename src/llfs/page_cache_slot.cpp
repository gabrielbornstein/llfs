//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_slot.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ PageCacheSlot::PageCacheSlot(Pool& pool) noexcept : pool_{pool}
{
  BATT_CHECK(!this->is_pinned());
  BATT_CHECK(!this->is_valid());
  BATT_CHECK_EQ(this->ref_count(), 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheSlot::~PageCacheSlot() noexcept
{
  BATT_CHECK(!this->is_pinned()) << BATT_INSPECT(this->pin_count())
                                 << BATT_INSPECT(this->ref_count()) << BATT_INSPECT((void*)this);
  BATT_CHECK_EQ(this->ref_count(), 0);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize PageCacheSlot::index() const
{
  return this->pool_.index_of(this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageCacheSlot::fill(PageId key) -> PinnedRef
{
  BATT_CHECK(!this->is_valid());
  BATT_CHECK(key.is_valid());

  this->key_ = key;
  this->value_.emplace();
  this->p_value_ = std::addressof(*this->value_);
  this->obsolete_.store(0);
  this->update_latest_use();

  auto observed_state = this->state_.fetch_add(kPinCountDelta) + kPinCountDelta;
  BATT_CHECK_EQ(observed_state & Self::kOverflowMask, 0);
  BATT_CHECK(Self::is_pinned(observed_state));

  this->add_ref();
  this->set_valid();

  return PinnedRef{this, CallerPromisesTheyAcquiredPinCount{}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::clear()
{
  BATT_CHECK(!this->is_valid());

  this->key_ = PageId{};
  this->value_ = None;
  this->p_value_ = nullptr;
  this->obsolete_.store(0);
  this->set_valid();
}

#if LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::notify_first_ref_acquired()
{
  intrusive_ptr_add_ref(std::addressof(this->pool_));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageCacheSlot::notify_last_ref_released()
{
  intrusive_ptr_release(std::addressof(this->pool_));
}

#endif  // LLFS_PAGE_CACHE_SLOT_UPDATE_POOL_REF_COUNT

}  //namespace llfs
