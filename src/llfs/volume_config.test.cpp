//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_config.hpp>
//
#include <llfs/volume_config.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/testing/mock_slot_visitor_fn.hpp>

#include <llfs/constants.hpp>
#include <llfs/ioring.hpp>
#include <llfs/packed_bytes.hpp>
#include <llfs/storage_context.hpp>

#include <filesystem>

namespace {

using namespace llfs::constants;

const std::filesystem::path kStorageFilePath{"/tmp/llfs_volume_config_test_file"};
const std::string kTestVolumeName = "MyTestVolume";

// SlotVisitorFn implementation that does nothing and returns OkStatus.
//
llfs::Status null_slot_visitor(const llfs::SlotParse&, std::string_view)
{
  return llfs::OkStatus();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class VolumeConfigTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    this->remove_test_files();
  }

  void TearDown() override
  {
    this->remove_test_files();
  }

  // Remove all temporary test files used/generated by this test.
  //
  void remove_test_files()
  {
    llfs::delete_file(kStorageFilePath.string()).IgnoreError();
    EXPECT_FALSE(std::filesystem::exists(kStorageFilePath));
  }

  // Initializes a new IoRing for file I/O.
  //
  void create_ioring()
  {
    llfs::StatusOr<llfs::ScopedIoRing> io =
        llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{1024}, llfs::ThreadPoolSize{1});
    ASSERT_TRUE(io.ok()) << BATT_INSPECT(io.status());

    this->ioring_ = std::move(*io);
  }

  // Creeate a new StorageContext; this has the side-effect of resetting all caches.
  //
  void create_storage_context()
  {
    this->storage_context_ = batt::make_shared<llfs::StorageContext>(
        batt::Runtime::instance().default_scheduler(), this->ioring_.get_io_ring());
  }

  // Returns default VolumeRuntimeOptions for tests.
  //
  auto get_volume_runtime_options(
      llfs::VolumeReader::SlotVisitorFn&& slot_visitor_fn = &null_slot_visitor) const
  {
    return llfs::VolumeRuntimeOptions{
        .slot_visitor_fn = std::move(slot_visitor_fn),
        .root_log_options = llfs::IoRingLogDriverOptions{},
        .recycler_log_options = llfs::IoRingLogDriverOptions{},
        .trim_control = nullptr,
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // io_uring context used for file I/O.
  //
  llfs::ScopedIoRing ioring_;

  // StorageContext used to read/write storage files.
  //
  batt::SharedPtr<llfs::StorageContext> storage_context_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(VolumeConfigTest, ConfigRestore)
{
  ASSERT_NO_FATAL_FAILURE(this->create_ioring());

  this->create_storage_context();

  // We want to save the uuid generated for the volume so we can reload it later.
  //
  boost::uuids::uuid volume_uuid;
  boost::uuids::uuid root_log_uuid;

  // Create a storage file with two page arenas, one for small pages (4kb), one for large (2mb).
  //
  llfs::StatusOr<std::vector<boost::uuids::uuid>> file_create_status =
      this->storage_context_->add_new_file(
          kStorageFilePath.string(), [&](llfs::StorageFileBuilder& builder) -> llfs::Status {
            llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedVolumeConfig&>> p_volume_config =
                builder.add_object(llfs::VolumeConfigOptions{
                    .base =
                        llfs::VolumeOptions{
                            .name = kTestVolumeName,
                            .uuid = llfs::None,
                            .max_refs_per_page = llfs::MaxRefsPerPage{1},
                            .trim_lock_update_interval = llfs::TrimLockUpdateInterval{4 * kKiB},
                            .trim_delay_byte_count = llfs::TrimDelayByteCount{0},
                        },
                    .root_log =
                        llfs::LogDeviceConfigOptions{
                            .uuid = llfs::None,
                            .pages_per_block_log2 = llfs::None,
                            .log_size = 8 * kMiB,
                        },
                    .recycler_max_buffered_page_count = llfs::None,
                });

            BATT_REQUIRE_OK(p_volume_config);

            volume_uuid = (*p_volume_config)->uuid;
            root_log_uuid = (*p_volume_config)->root_log_uuid;

            return llfs::OkStatus();
          });

  ASSERT_TRUE(file_create_status.ok()) << BATT_INSPECT(file_create_status);

  std::cerr << BATT_INSPECT(volume_uuid) << BATT_INSPECT(root_log_uuid) << std::endl;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // These will be used for verification below, so keep them out of this block scope.
  //
  llfs::StatusOr<llfs::SlotRange> alpha_slot;
  llfs::StatusOr<llfs::SlotRange> bravo_slot;
  llfs::StatusOr<llfs::SlotRange> charlie_slot;
  llfs::StatusOr<llfs::SlotRange> delta_slot;
  {
    // Verify that we get the correct error status if we try to recover a Volume from the wrong type
    // of config slot.
    {
      llfs::StatusOr<std::unique_ptr<llfs::Volume>> expect_fail =
          this->storage_context_->recover_object(batt::StaticType<llfs::PackedVolumeConfig>{},
                                                 root_log_uuid, this->get_volume_runtime_options());

      EXPECT_FALSE(expect_fail.ok());
      EXPECT_EQ(expect_fail.status(),
                ::llfs::make_status(llfs::StatusCode::kStorageObjectTypeError));
    }

    // Now restore the volume, write some events.
    //
    llfs::StatusOr<std::unique_ptr<llfs::Volume>> maybe_volume =
        this->storage_context_->recover_object(batt::StaticType<llfs::PackedVolumeConfig>{},
                                               volume_uuid, this->get_volume_runtime_options());

    ASSERT_TRUE(maybe_volume.ok()) << BATT_INSPECT(maybe_volume.status());

    llfs::Volume& volume = **maybe_volume;
    llfs::StatusOr<batt::Grant> grant =
        volume.reserve(volume.calculate_grant_size(std::string_view("alpha")) +        //
                           volume.calculate_grant_size(std::string_view("bravo")) +    //
                           volume.calculate_grant_size(std::string_view("charlie")) +  //
                           volume.calculate_grant_size(std::string_view("delta")),
                       batt::WaitForResource::kTrue);

    ASSERT_TRUE(grant.ok()) << BATT_INSPECT(grant.status());

    alpha_slot = volume.append(std::string_view("alpha"), *grant);
    ASSERT_TRUE(alpha_slot.ok()) << BATT_INSPECT(alpha_slot.status());

    bravo_slot = volume.append(std::string_view("bravo"), *grant);
    ASSERT_TRUE(bravo_slot.ok()) << BATT_INSPECT(bravo_slot.status());

    charlie_slot = volume.append(std::string_view("charlie"), *grant);
    ASSERT_TRUE(charlie_slot.ok()) << BATT_INSPECT(charlie_slot.status());

    delta_slot = volume.append(std::string_view("delta"), *grant);
    ASSERT_TRUE(delta_slot.ok()) << BATT_INSPECT(delta_slot.status());

    llfs::Status sync_status = volume
                                   .sync(llfs::LogReadMode::kDurable,
                                         llfs::SlotUpperBoundAt{.offset = delta_slot->upper_bound})
                                   .status();
    ASSERT_TRUE(sync_status.ok()) << BATT_INSPECT(sync_status);
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Create a new storage context and recover the Volume to verify the events we appended.
  {
    this->create_storage_context();

    llfs::StatusOr<std::vector<boost::uuids::uuid>> file_add_status =
        this->storage_context_->add_existing_named_file(kStorageFilePath);
    ASSERT_TRUE(file_add_status.ok()) << BATT_INSPECT(file_add_status);

    ::testing::StrictMock<llfs::MockSlotVisitorFn> mock_slot_visitor;

    ::testing::Expectation read_alpha =
        EXPECT_CALL(mock_slot_visitor,
                    visit_slot(::testing::Truly([&](const llfs::SlotParse& slot) {
                                 return slot.offset == *alpha_slot;
                               }),
                               ::testing::StrEq("alpha")))
            .WillOnce(::testing::Return(llfs::OkStatus()));

    ::testing::Expectation read_bravo =
        EXPECT_CALL(mock_slot_visitor,
                    visit_slot(::testing::Truly([&](const llfs::SlotParse& slot) {
                                 return slot.offset == *bravo_slot;
                               }),
                               ::testing::StrEq("bravo")))
            .After(read_alpha)
            .WillOnce(::testing::Return(llfs::OkStatus()));

    ::testing::Expectation read_charlie =
        EXPECT_CALL(mock_slot_visitor,
                    visit_slot(::testing::Truly([&](const llfs::SlotParse& slot) {
                                 return slot.offset == *charlie_slot;
                               }),
                               ::testing::StrEq("charlie")))
            .After(read_bravo)
            .WillOnce(::testing::Return(llfs::OkStatus()));

    EXPECT_CALL(mock_slot_visitor, visit_slot(::testing::Truly([&](const llfs::SlotParse& slot) {
                                                return slot.offset == *delta_slot;
                                              }),
                                              ::testing::StrEq("delta")))
        .After(read_charlie)
        .WillOnce(::testing::Return(llfs::OkStatus()));

    llfs::StatusOr<std::unique_ptr<llfs::Volume>> maybe_volume =
        this->storage_context_->recover_object(
            batt::StaticType<llfs::PackedVolumeConfig>{}, volume_uuid,
            this->get_volume_runtime_options(std::ref(mock_slot_visitor)));

    ASSERT_TRUE(maybe_volume.ok()) << BATT_INSPECT(maybe_volume);
  }
}

}  // namespace
