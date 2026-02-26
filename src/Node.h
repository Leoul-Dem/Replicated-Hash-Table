#pragma once

#include "phmap.hpp"
#include "asio.hpp"
#include "utils.hpp"

#include <array>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <thread>
#include <memory>
#include <optional>
#include <string>
#include <vector>

class Node{
    struct Peers{
        std::unique_ptr<asio::ip::tcp::socket> left_conn;
        std::unique_ptr<asio::ip::tcp::socket> right_conn;
        std::unique_ptr<asio::ip::tcp::socket> farthest_conn;

        std::mutex left_mtx;
        std::mutex right_mtx;
        std::mutex farthest_mtx;

        int8_t left_idx;
        int8_t right_idx;
        int8_t farthest_idx;
    };
    
    gtl::parallel_flat_hash_map<int32_t, std::array<char, VALUE_SIZE>> todo_1;
    gtl::parallel_flat_hash_map<int32_t, std::array<char, VALUE_SIZE>> todo_2;

    gtl::parallel_flat_hash_map_m<int32_t, std::array<char, VALUE_SIZE>> table_1;
    gtl::parallel_flat_hash_map_m<int32_t, std::array<char, VALUE_SIZE>> table_2;

    static constexpr size_t NUM_STRIPES = 256;
    std::array<std::shared_timed_mutex, NUM_STRIPES> t1_locks;
    std::array<std::shared_timed_mutex, NUM_STRIPES> t2_locks;

    static size_t stripe(int32_t key) { return static_cast<uint32_t>(key) % NUM_STRIPES; }

    std::vector<asio::ip::tcp::endpoint> all_nodes;

    int PORT = 6000;

    asio::io_context io_ctx;

    Peers peers;

    std::unique_ptr<asio::ip::tcp::acceptor> acceptor;

    int8_t my_idx;

    std::atomic_uint32_t req_id = 0;

    void parse_node_addrs(int argc, char** argv);
    void establish_connections(int myIdx);

    void create_server(int port);

    size_t send_request(const Request_Full &req, Response_Full &resp);

    

    void handle_request(const Request_Full &req, Response_Full &resp);


public:
   std::atomic<bool> running{true};

   Node(int port, const int argc, char** argv);

   bool put(KV_Pair input, bool original);

   bool put(std::array<KV_Pair, 3> input, bool original);

   std::array<char, VALUE_SIZE> get(int32_t input);
   
   void recv_request();

   void stop();

};
