cmake_minimum_required(VERSION 3.16)
project(Replicated_Hash_Table)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -pthread")

include_directories(libs)
include_directories(libs/data_structures)
include_directories(libs/data_structures/hashmap)
include_directories(libs/networking)


add_executable(Replicated_Hash_Table
        libs/data_structures/hashmap/bits.hpp
        libs/data_structures/hashmap/gtl_base.hpp
        libs/data_structures/hashmap/gtl_config.hpp
        libs/data_structures/hashmap/phmap.hpp
        libs/data_structures/hashmap/phmap_fwd_decl.hpp
        libs/data_structures/hashmap/phmap_utils.hpp
        libs/data_structures/concurrentqueue.h
        libs/networking/asio.hpp
        src/Node.cpp
        src/Node.h
        src/Pub_API.cpp
        src/Server.cpp
        src/Client.cpp
        src/main.cpp
)
