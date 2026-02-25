cmake_minimum_required(VERSION 4.1)
project(Replicated_Hash_Table)

set(CMAKE_CXX_STANDARD 20)

include_directories(libs)
include_directories(libs/data_structures)
include_directories(libs/data_structures/hashmap)

add_executable(Replicated_Hash_Table
        libs/data_structures/hashmap/bits.hpp
        libs/data_structures/hashmap/gtl_base.hpp
        libs/data_structures/hashmap/gtl_config.hpp
        libs/data_structures/hashmap/phmap.hpp
        libs/data_structures/hashmap/phmap_fwd_decl.hpp
        libs/data_structures/hashmap/phmap_utils.hpp
        libs/data_structures/concurrentqueue.h
        src/node_map.h
        src/Node.cpp
        src/Node.h)
