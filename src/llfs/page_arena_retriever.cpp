//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

// #include <llfs/page_arena_retriever.hpp>
//

#include <llfs/page_arena.hpp>
#include <llfs/storage_context.hpp>

#include <boost/uuid/random_generator.hpp>

namespace llfs {

void increase_storage_capacity(StorageContext& storage_context,
                               const std::filesystem::path& dir_path, u64 increase_capacity)
{
  // TODO: [Gabe Bornstein 6/3/24] A lot of this code is copy-pasted and could be de-duped.
  //

  constexpr unsigned kMaxTreeHeight = 10;

  const Optional<TreeOptions>& tree_options = TreeOptions::with_default_values();

  // Calculate the page counts from the total capacity and TreeOptions.
  //
  const auto max_in_refs_size_per_leaf = 64 * kMaxTreeHeight;

  const auto leaf_page_count =
      llfs::PageCount{increase_capacity / (tree_options.leaf_size() + max_in_refs_size_per_leaf)};

  const auto total_leaf_pages_size = leaf_page_count * tree_options.leaf_size();
  const auto total_node_pages_size = increase_capacity - total_leaf_pages_size;

  const auto node_page_count = llfs::PageCount{total_node_pages_size / tree_options.node_size()};

  VLOG(1) << BATT_INSPECT(increase_capacity) << BATT_INSPECT(node_page_count)
          << BATT_INSPECT(leaf_page_count);

  // Create the page file.
  //
  const char* const kPageFileName = "pages.llfs";
  StatusOr<std::vector<boost::uuids::uuid>> page_file_status = storage_context.add_new_file(
      (dir_path / kPageFileName).string(),
      [&](llfs::StorageFileBuilder& builder) -> Status  //
      {
        // Add an arena for node pages.
        //
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> node_pool_config =
            builder.add_object(
                llfs::PageArenaConfigOptions{
                    .uuid = None,
                    .page_allocator =
                        llfs::CreateNewPageAllocator{
                            .options =
                                llfs::PageAllocatorConfigOptions{
                                    .uuid = llfs::None,
                                    .max_attachments =
                                        32 /* TODO [tastolfi 2022-07-25] add config option */,
                                    .page_count = node_page_count,
                                    .log_device =
                                        llfs::CreateNewLogDeviceWithDefaultSize{
                                            .uuid = llfs::None,
                                            .pages_per_block_log2 = 1,
                                        },
                                    // TODO: [Gabe Bornstein 6/3/24] Use TreeOptions w/o default
                                    // values here.
                                    //
                                    .page_size_log2 =
                                        TreeOptions::with_default_values().node_size_log2(),
                                    .page_device = llfs::LinkToNewPageDevice{},
                                },
                        },
                    .page_device =
                        llfs::CreateNewPageDevice{
                            .options =
                                llfs::PageDeviceConfigOptions{
                                    .uuid = llfs::None,
                                    .device_id = llfs::None,
                                    .page_count = node_page_count,
                                    // TODO: [Gabe Bornstein 6/3/24] Use TreeOptions w/o default
                                    // values here.
                                    //
                                    .page_size_log2 =
                                        TreeOptions::with_default_values().node_size_log2(),
                                },
                        },
                });

        BATT_REQUIRE_OK(node_pool_config);

        // Add an arena for leaf pages.
        //
        llfs::StatusOr<llfs::FileOffsetPtr<const llfs::PackedPageArenaConfig&>> leaf_pool_config =
            builder.add_object(
                llfs::PageArenaConfigOptions{
                    .uuid = None,
                    .page_allocator =
                        llfs::CreateNewPageAllocator{
                            .options =
                                llfs::PageAllocatorConfigOptions{
                                    .uuid = llfs::None,
                                    .max_attachments =
                                        32 /* TODO [tastolfi 2022-07-25] add config option */,
                                    .page_count = leaf_page_count,
                                    .log_device =
                                        llfs::CreateNewLogDeviceWithDefaultSize{
                                            .uuid = llfs::None,
                                            .pages_per_block_log2 = 1,
                                        },
                                    // TODO: [Gabe Bornstein 6/3/24] Use TreeOptions w/o default
                                    // values here.
                                    //
                                    .page_size_log2 =
                                        TreeOptions::with_default_values().leaf_size_log2(),
                                    .page_device = llfs::LinkToNewPageDevice{},
                                },
                        },
                    .page_device =
                        llfs::CreateNewPageDevice{
                            .options =
                                llfs::PageDeviceConfigOptions{
                                    .uuid = llfs::None,
                                    .device_id = llfs::None,
                                    .page_count = leaf_page_count,
                                    // TODO: [Gabe Bornstein 6/3/24] Use TreeOptions w/o default
                                    // values here.
                                    //
                                    .page_size_log2 =
                                        TreeOptions::with_default_values().leaf_size_log2(),
                                },
                        },
                });

        BATT_REQUIRE_OK(leaf_pool_config);

        return OkStatus();
      });
}

std::vector<PageArena>& retrieve_arenas(StorageContext& storage_context,
                                        std::vector<boost::uuids::uuid> uuids)
{
  // TODO: [Gabe Bornstein 6/3/24] A lot of this code is copy-pasted and could be de-duped.
  //

  // Add Arenas to PageCache.
  //
  std::vector<PageArena> arenas;
  for (boost::uuids::uuid uuid : uuids) {
    batt::SharedPtr<StorageObjectInfo> p_object_info = storage_context.find_object_by_uuid(uuid);

    if (p_object_info->p_config_slot->tag == PackedConfigSlotBase::Tag::kPageArena) {
      const auto& packed_arena_config =
          config_slot_cast<PackedPageArenaConfig>(p_object_info->p_config_slot.object);

      const std::string base_name =
          batt::to_string("PageDevice_", packed_arena_config.page_device_uuid);

      StatusOr<PageArena> arena = storage_context.recover_object(
          batt::StaticType<PackedPageArenaConfig>{}, uuid,
          PageAllocatorRuntimeOptions{
              .scheduler = storage_context.get_scheduler(),
              .name = batt::to_string(base_name, "_Allocator"),
          },
          [&] {
            IoRingLogDriverOptions options;
            options.name = batt::to_string(base_name, "_AllocatorLog");
            return options;
          }(),
          IoRingFileRuntimeOptions{
              .io_ring = storage_context.get_io_ring(),
              .use_raw_io = true,
              .allow_read = true,
              .allow_write = true,
          });

      BATT_REQUIRE_OK(arena);
      arenas.push_back(std::move(arena));
    }
  }
}
}  //namespace llfs