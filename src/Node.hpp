#pragma once

#include "../libs/ds/hashmap/phmap.hpp"
#include "../libs/networking/asio.hpp"
#include "../libs/utils.hpp"

#include <array>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <thread>
#include <memory>
#include <string>
#include <vector>
#include <shared_mutex>
#include <unordered_map>

struct PendingTx {
    std::vector<std::pair<int32_t, std::string>> kvs;
};

class Node{
    static constexpr int CONNS_PER_PEER = 4;

    gtl::parallel_flat_hash_map_m<int32_t, std::string> table;

    static constexpr size_t NUM_STRIPES = 256;
    std::array<std::shared_timed_mutex, NUM_STRIPES> locks;

    static size_t stripe(int32_t key) { return static_cast<uint32_t>(key) % NUM_STRIPES; }

    std::vector<asio::ip::tcp::endpoint> all_nodes;

    // Per-peer connections: conns[peer_idx][0..CONNS_PER_PEER-1]
    std::vector<std::array<std::unique_ptr<asio::ip::tcp::socket>, CONNS_PER_PEER>> conns;
    std::vector<std::array<std::mutex, CONNS_PER_PEER>> conn_mtx;
    std::vector<std::atomic<uint32_t>> conn_rr;

    int PORT;

    asio::io_context io_ctx;

    std::unique_ptr<asio::ip::tcp::acceptor> acceptor;

    int8_t my_idx;

    // 2PC transaction state
    std::atomic<uint32_t> tx_counter{0};
    std::mutex pending_mtx;
    std::unordered_map<uint64_t, PendingTx> pending_txs;

    uint64_t next_tx_id() {
        return (static_cast<uint64_t>(my_idx) << 32) | tx_counter.fetch_add(1);
    }

    int8_t replica_idx() const {
        return (my_idx + 1) % static_cast<int8_t>(all_nodes.size());
    }

    void parse_node_addrs(const int argc, const char** argv);
    void establish_conns(int myIdx);
    void create_server(const int port);

    size_t send_request(int8_t dest, const Request &req, Response &resp);
    void handle_request(const Request &req, Response &resp);

    // 2PC participant handlers (called from handle_request, no outbound RPCs)
    bool handle_prepare(const Request &req);
    void handle_commit(uint64_t tx_id);
    void handle_abort(uint64_t tx_id);

public:
    std::atomic<bool> running{true};

    Node(int port, const int argc, const char** argv);

    bool put(const int32_t &key, const std::string &val);
    bool put3(const std::array<KV_Pair, 3> &kvs);
    std::string get(const int32_t &key);

    void recv_request();
    void stop();
};
