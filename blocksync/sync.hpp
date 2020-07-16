/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>
#include <filesystem>
#include <fstream>

#include "../gsl-lite.hpp"

#include "d8u/transform.hpp"
#include "d8u/util.hpp"

#include "dircopy/restore.hpp"
#include "dircopy/delta.hpp"
#include "tdb/legacy.hpp"

namespace blocksync
{
	template <typename TH,typename S, typename D> uint64_t sync(d8u::util::Statistics &s, uint64_t start, S& store, D& dest,bool validate, size_t T = 1)
	{
		auto move = [&](auto block)
		{
			d8u::util::dec_scope lock(s.atomic.threads);

			s.atomic.blocks++;
			s.atomic.read += block.size();

			if (validate && !d8u::transform::validate_block<TH>(block))
				throw std::runtime_error("Block Validation Failed!");

			auto id = d8u::transform::id_block<TH>(block);

			if (!dest.Is(id))
			{
				dest.Write(id, std::move(block));
				s.atomic.write += block.size();
			}
			else
			{
				s.atomic.dblocks++;
				s.atomic.duplicate += block.size();
			}

			return true;
		};

		auto result = store.EnumerateMap(start, [&](auto &&block)
		{
			if(T == 1)
				return move(std::move(block));

			d8u::util::fast_wait_inc(s.atomic.threads, T);

			std::thread(move, std::move(block)).detach();

			return true;
		});

		if(T!=1) d8u::util::fast_wait(s.atomic.threads);

		return result;
	}

	template <typename TH, typename S, typename D> void sync_key(d8u::util::Statistics &s, const TH & key, S& store, D& dest, bool validate)
	{
		auto id = key.GetNext();
		auto block = store.Map(id);

		s.atomic.blocks++;
		s.atomic.read += block.size();
		
		if (!block.size())
			throw std::runtime_error("Block not found");

		if (validate)
		{
			auto actual = d8u::transform::id_block<TH>(block);

			if(!std::equal(id.begin(),id.end(),actual.begin()))
				throw std::runtime_error("Mismatched block");
		}

		if (!dest.Is(id))
		{
			dest.Write(id, std::move(block));
			s.atomic.write += block.size();
		}
		else
		{
			s.atomic.dblocks++;
			s.atomic.duplicate += block.size();
		}
	}

	template <typename TH, typename S, typename D> void sync_keys(d8u::util::Statistics& s, const gsl::span<TH> keys, S& store, D& dest, bool validate, size_t T = 1)
	{
		if (T == 1)
			for (size_t i = 0; i <keys.size()-1; i++)
				sync_key(s, keys[i], store, dest, validate);
		else
		{
			std::atomic<size_t> local = 0;

			for (size_t i = 0; i < keys.size() - 1; i++)
			{
				d8u::util::fast_wait_inc(s.atomic.threads, T);
				local++;

				std::thread([&](TH key)
				{
					d8u::util::dec_scope lock1(s.atomic.threads);
					d8u::util::dec_scope lock2(local);

					sync_key(s, key, store, dest, validate);
				}, keys[i]).detach();
			}

			d8u::util::fast_wait(local);
		}
	}

	template<typename TH, typename S, typename D> class Sync
	{
		std::filesystem::path root;
		std::filesystem::path state;

		S source;
		D dest;
	public:

		S& Source() { return source; }
		D& Destination() { return dest; }

		Sync(std::string_view _root, std::string_view source_params, std::string_view dest_params)
			: source (source_params)
			, dest(dest_params)
			, root(_root)
		{
			state = root.append(std::string(source_params) + ".to." + std::string(dest_params));
			std::filesystem::create_directories(root);
		}

		void Push(d8u::util::Statistics& s,bool validate=true, size_t T = 1)
		{
			set_start( sync<TH>( s, get_start() , source, dest,validate,T) );
		}

		auto Push( bool validate = true, size_t T = 1)
		{
			d8u::util::Statistics s;

			set_start(sync<TH>(s,get_start(), source, dest, validate,T));

			return s.direct;
		}

		template < typename DO> void MigrateFolder(d8u::util::Statistics& s,const TH& folder_key, const DO& domain, bool validate=true,size_t F = 1, size_t T = 1)
		{
			sync_key(s,folder_key,source,dest,validate);

			auto folder_record = dircopy::restore::block(s, folder_key, source, domain, validate);
			auto folder_keys = gsl::span<TH>((TH*)folder_record.data(), folder_record.size() / sizeof(TH));

			sync_keys(s, folder_keys, source, dest, validate, T);

			auto database = dircopy::restore::file_memory(s, folder_keys, source, domain, validate, validate);

			tdb::MemoryHashmap db(database);

			{
				auto [size, time, name, data] = dircopy::delta::Path<TH>::DecodeRaw(db.FindObject(domain));

				auto stats = (typename dircopy::delta::Path<TH>::FolderStatistics *)data.data();

				s.direct.target = stats->size;
			}

			auto file = [&](uint64_t p)
			{
				d8u::util::dec_scope lock(s.atomic.files);

				auto [size, time, name, keys] = dircopy::delta::Path<TH>::Decode(db.GetObject(p));

				if (!size)
					return true; //Empty File & Stats block

				if (keys.size() == 1)
				{
					auto block = dircopy::restore::file_memory(s, keys, source, domain, validate, validate);

					keys = gsl::span<TH>((TH*)block.data(),block.size()/sizeof(TH));
					sync_keys(s, keys, source, dest, validate, T);
				}
				else
					sync_keys(s, keys, source, dest, validate, T);

				return true;
			};

			if (F == 1)
				db.Iterate([&](uint64_t p)
				{
					return file(p);
				});
			else
			{
				db.Iterate([&](uint64_t p)
				{
					d8u::util::fast_wait_inc(s.atomic.files, F);

					std::thread(file, p).detach();

					return true;
				});

				d8u::util::fast_wait(s.atomic.files);
			}
		}

		template <typename DO> auto MigrateFolder(const TH& folder_key, const DO& domain, bool validate = true, size_t F = 1, size_t T = 1)
		{
			d8u::util::Statistics s;

			MigrateFolder(s, folder_key, domain, validate, F, T);

			return s.direct;
		}

	private:

		void set_start(uint64_t start)
		{
			std::ofstream o(state.c_str());

			o << start;
		}

		uint64_t get_start()
		{
			uint64_t result;

			std::ifstream i(state.c_str());

			if (!i.is_open())
				return 0;

			i >> result;

			return result;
		}
	};
}