/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <filesystem>

#include "../catch.hpp"

#include "sync.hpp"

#include "volstore/image.hpp"
#include "dircopy/backup.hpp"
#include "dircopy/restore.hpp"
#include "dircopy/validate.hpp"
#include "volrng/volume.hpp"
#include "volrng/compat.hpp"
#include "d8u/util.hpp"


TEST_CASE("Basic Migrate to Image Sync", "[blocksync::]")
{
	constexpr auto folder_size = d8u::util::_mb(100);

	volrng::DISK::Dismount("tempdisk\\disk.img");
	volrng::DISK::Dismount("resdisk.img");

	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("store1");
	std::filesystem::remove_all("store2");
	std::filesystem::remove_all("sync");

	std::filesystem::create_directories("tempdisk");

	{
		volrng::volume::Test<volrng::DISK> handle("tempdisk");

		blocksync::Sync<volstore::Image, volstore::Image> sync_handle("sync", "store1", "store2");


		handle.Run(folder_size, volrng::MOUNT);

		handle.Mount(volrng::MOUNT);

		dircopy::backup::KeyResult result1 = dircopy::backup::recursive_folder("", "testsnap", volrng::MOUNT, sync_handle.Source(),
			[](auto&, auto, auto) { return true; },
			d8u::util::default_domain, 64, d8u::util::_mb(1), 64, 5, 64, d8u::util::_mb(64));

		handle.Dismount();


		handle.Run(folder_size, volrng::MOUNT);

		handle.Mount(volrng::MOUNT);

		dircopy::backup::KeyResult result2 = dircopy::backup::recursive_folder("", "testsnap", volrng::MOUNT, sync_handle.Source(),
			[](auto&, auto, auto) { return true; },
			d8u::util::default_domain, 64, d8u::util::_mb(1), 64, 5, 64, d8u::util::_mb(64));

		handle.Dismount();


		auto merge = sync_handle.MigrateFolder(result2.key, d8u::util::default_domain, true, 1, 1);


		CHECK(true == dircopy::validate::folder(result2.key, sync_handle.Destination(), d8u::util::default_domain, d8u::util::_mb(1), d8u::util::_mb(64), 64, 64).first);

		CHECK(true == dircopy::validate::deep_folder(result2.key, sync_handle.Destination(), d8u::util::default_domain, d8u::util::_mb(1), d8u::util::_mb(64), 64, 64).first);

		{
			volrng::DISK res_disk("resdisk.img", d8u::util::_gb(100), volrng::MOUNT2);


			dircopy::restore::folder(volrng::MOUNT2, result2.key, sync_handle.Destination(), d8u::util::default_domain, true, true, d8u::util::_mb(1), d8u::util::_mb(64), 64, 64);


			CHECK(handle.Validate(volrng::MOUNT2));
		}

		std::filesystem::remove_all("resdisk.img");
	}

	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("store1");
	std::filesystem::remove_all("store2");
	std::filesystem::remove_all("sync");
}

TEST_CASE("Basic Image to Image Sync", "[blocksync::]")
{
	constexpr auto folder_size = d8u::util::_mb(100);

	volrng::DISK::Dismount("tempdisk\\disk.img");
	volrng::DISK::Dismount("resdisk.img");

	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("store1");
	std::filesystem::remove_all("store2");
	std::filesystem::remove_all("sync");

	std::filesystem::create_directories("tempdisk");

	{
		volrng::volume::Test<volrng::DISK> handle("tempdisk");

		blocksync::Sync<volstore::Image, volstore::Image> sync_handle("sync", "store1", "store2");

		handle.Run(folder_size, volrng::MOUNT);

		handle.Mount(volrng::MOUNT);

		dircopy::backup::KeyResult result = dircopy::backup::recursive_folder("", "testsnap", volrng::MOUNT, sync_handle.Source(),
			[](auto&, auto, auto) { return true; },
			d8u::util::default_domain, 64, d8u::util::_mb(1), 64, 5, 64, d8u::util::_mb(64));

		handle.Dismount();

		sync_handle.Push(true,8);


		CHECK(true == dircopy::validate::folder(result.key, sync_handle.Destination(), d8u::util::default_domain, d8u::util::_mb(1), d8u::util::_mb(64), 64, 64).first);
	
		CHECK(true == dircopy::validate::deep_folder(result.key, sync_handle.Destination(), d8u::util::default_domain, d8u::util::_mb(1), d8u::util::_mb(64), 64, 64).first);

		{
			volrng::DISK res_disk("resdisk.img", d8u::util::_gb(100), volrng::MOUNT2);


			dircopy::restore::folder(volrng::MOUNT2, result.key, sync_handle.Destination(), d8u::util::default_domain, true, true, d8u::util::_mb(1), d8u::util::_mb(64), 64, 64);
			

			CHECK(handle.Validate(volrng::MOUNT2));
		}

		std::filesystem::remove_all("resdisk.img");
	}

	std::filesystem::remove_all("resdisk.img");
	std::filesystem::remove_all("testsnap");
	std::filesystem::remove_all("tempdisk");
	std::filesystem::remove_all("store1");
	std::filesystem::remove_all("store2");
	std::filesystem::remove_all("sync");
}