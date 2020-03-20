/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#pragma once

#include <string_view>
#include <filesystem>
#include <fstream>

#include "d8u/transform.hpp"

namespace blocksync
{
	template <typename S, typename D> uint64_t sync(uint64_t start, S& s, D& d,bool validate)
	{
		return s.EnumerateMap(start, [&](auto block) 
		{
			if (validate && !d8u::transform::validate_block(block))
				throw std::runtime_error("Block Validation Failed!");

			d.Write(d8u::transform::id_block(block),block);

			return true;
		});
	}

	template<typename S, typename D> class Sync
	{
		std::filesystem::path root;
		std::filesystem::path state;

		S source;
		D dest;
	public:

		S& Source() { return source; }
		S& Destination() { return dest; }

		Sync(std::string_view _root, std::string_view source_params, std::string_view dest_params)
			: source (source_params)
			, dest(dest_params)
			, root(_root)
		{
			state = root.append(std::string(source_params) + ".to." + std::string(dest_params));
			std::filesystem::create_directories(root);
		}

		void Push(bool validate=true)
		{
			set_start( sync( get_start() , source, dest,validate) );
		}

		template <typename T> void Migrate(const T & key, bool validate=true)
		{
			//Todo move all blocks for a point in time.
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