#include "Node.hpp"
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <thread>
#include <algorithm>
#include <sys/socket.h>
#include <set>

static void set_sock_timeout(asio::ip::tcp::socket &sock, int seconds) {
    struct timeval tv;
    tv.tv_sec = seconds;
    tv.tv_usec = 0;
    int fd = sock.native_handle();
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

Node::Node(int port, const int argc, const char** argv)
    : PORT(port) {
    parse_node_addrs(argc, argv);
    create_server(port);
    establish_conns(my_idx);
}

void Node::parse_node_addrs(const int argc, const char** argv){
    my_idx = std::atoi(argv[1]);

    for (int i = 3; i < argc; i++) {
        std::string arg(argv[i]);
        auto colon = arg.rfind(':');
        if (colon == std::string::npos) {
            std::fprintf(stderr, "Bad address (expected ip:port): %s\n", argv[i]);
            std::exit(1);
        }
        std::string ip = arg.substr(0, colon);
        int port = std::atoi(arg.substr(colon + 1).c_str());
        asio::ip::tcp::endpoint ep(asio::ip::make_address(ip), port);
        all_nodes.push_back(ep);
    }
}

void Node::create_server(const int port){
    acceptor = std::make_unique<asio::ip::tcp::acceptor>(io_ctx);
    acceptor->open(asio::ip::tcp::v4());
    acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
    acceptor->bind(asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port));
}

void Node::establish_conns(int myIdx){
    const int N = static_cast<int>(all_nodes.size());

    conns.resize(N);
    conn_mtx = std::vector<std::array<std::mutex, CONNS_PER_PEER>>(N);
    conn_rr = std::vector<std::atomic<uint32_t>>(N);

    int accept_count = 0;
    for (int i = 0; i < N; i++) {
        if (i < myIdx)
            accept_count += CONNS_PER_PEER;
    }

    acceptor->listen(accept_count + 16);

    std::vector<int> accepted_per_peer(N, 0);

    // Handshake: connector sends its 1-byte index, acceptor reads it to identify peer
    std::thread accept_thread([&]() {
        for (int a = 0; a < accept_count; a++) {
            auto sock = std::make_unique<asio::ip::tcp::socket>(acceptor->accept());
            uint8_t peer_idx = 0;
            std::error_code ec;
            asio::read(*sock, asio::buffer(&peer_idx, 1), ec);
            if (ec || peer_idx >= N || peer_idx >= myIdx) continue;
            int pi = static_cast<int>(peer_idx);
            if (accepted_per_peer[pi] < CONNS_PER_PEER) {
                conns[pi][accepted_per_peer[pi]] = std::move(sock);
                accepted_per_peer[pi]++;
            }
        }
    });

    for (int i = 0; i < N; i++) {
        if (i == myIdx) continue;
        if (i > myIdx) {
            for (int c = 0; c < CONNS_PER_PEER; c++) {
                std::error_code ec;
                for (int attempt = 0; attempt < 120; attempt++) {
                    auto sock = std::make_unique<asio::ip::tcp::socket>(io_ctx);
                    sock->connect(all_nodes[i], ec);
                    if (!ec) {
                        // Send our index as handshake
                        uint8_t idx_byte = static_cast<uint8_t>(myIdx);
                        asio::write(*sock, asio::buffer(&idx_byte, 1), ec);
                        if (!ec) {
                            conns[i][c] = std::move(sock);
                            break;
                        }
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                }
                if (ec) {
                    std::fprintf(stderr, "Failed to connect to node %d: %s\n",
                                 i, ec.message().c_str());
                    std::exit(1);
                }
            }
        }
    }

    accept_thread.join();

    // Set timeouts and TCP_NODELAY on all sockets
    for (int i = 0; i < N; i++) {
        if (i == myIdx) continue;
        for (int c = 0; c < CONNS_PER_PEER; c++) {
            if (conns[i][c]) {
                set_sock_timeout(*conns[i][c], 1);
                conns[i][c]->set_option(asio::ip::tcp::no_delay(true));
            }
        }
    }
}

size_t Node::send_request(int8_t dest, const Request &req, Response &resp){
    std::array<uint8_t, BUF_SIZE> send_buf{};
    std::array<uint8_t, BUF_SIZE> recv_buf{};
    std::error_code ec;

    serialize(req, send_buf);

    int c = conn_rr[dest].fetch_add(1, std::memory_order_relaxed) % CONNS_PER_PEER;
    std::lock_guard<std::mutex> lock(conn_mtx[dest][c]);

    asio::write(*conns[dest][c], asio::buffer(send_buf), ec);
    if (ec) return 0;
    asio::read(*conns[dest][c], asio::buffer(recv_buf), ec);
    if (ec) return 0;

    deserialize(recv_buf, resp);
    return recv_buf.size();
}

void Node::recv_request(){
    auto run = [this](asio::ip::tcp::socket& conn){
        while (running.load()) {
            std::array<uint8_t, BUF_SIZE> recv_buf{};
            std::array<uint8_t, BUF_SIZE> send_buf{};

            std::error_code ec;
            asio::read(conn, asio::buffer(recv_buf), ec);
            if (ec) {
                if (ec == asio::error::would_block || ec == asio::error::try_again)
                    continue;
                break;
            }

            Request req{};
            Response resp{};
            deserialize(recv_buf, req);

            handle_request(req, resp);

            serialize(resp, send_buf);
            asio::write(conn, asio::buffer(send_buf), ec);
            if (ec) break;
        }
    };

    const int N = static_cast<int>(all_nodes.size());
    std::vector<std::thread> threads;
    for (int i = 0; i < N; i++) {
        if (i == my_idx) continue;
        for (int c = 0; c < CONNS_PER_PEER; c++) {
            threads.emplace_back(run, std::ref(*conns[i][c]));
        }
    }

    for (auto& t : threads)
        t.join();
}

void Node::stop(){
    running.store(false);

    std::error_code ec;
    if (acceptor && acceptor->is_open())
        acceptor->close(ec);

    const int N = static_cast<int>(all_nodes.size());
    for (int i = 0; i < N; i++) {
        if (i == my_idx) continue;
        for (int c = 0; c < CONNS_PER_PEER; c++) {
            if (conns[i][c] && conns[i][c]->is_open()) {
                conns[i][c]->shutdown(asio::ip::tcp::socket::shutdown_both, ec);
                conns[i][c]->close(ec);
            }
        }
    }
}

// ─── handle_request: dispatches incoming RPCs ───────────────────────────────

void Node::handle_request(const Request &req, Response &resp){
    resp.id = req.id;
    resp.tx_id = req.tx_id;
    resp.src = req.src;
    resp.dest = req.dest;

    switch (req.op) {
        case GET: {
            // Read locally — no 2PC needed
            std::shared_lock<std::shared_timed_mutex> lk(locks[stripe(req.inputs[0].key)]);
            std::string result;
            table.if_contains(req.inputs[0].key, [&result](const auto& item) {
                result = item.second;
            });
            std::memset(resp.output, 0, MAX_VAL_SIZE);
            std::memcpy(resp.output, result.data(), std::min(result.size(), MAX_VAL_SIZE));
            resp.success = !result.empty();
            break;
        }
        case PREPARE_PUT:
        case PREPARE_PUT3:
            resp.success = handle_prepare(req);
            break;
        case COMMIT:
            handle_commit(req.tx_id);
            resp.success = true;
            break;
        case ABORT:
            handle_abort(req.tx_id);
            resp.success = true;
            break;
        default:
            resp.success = false;
            break;
    }
}

// ─── 2PC participant handlers (NO outbound RPCs) ────────────────────────────

bool Node::handle_prepare(const Request &req) {
    // PREPARE: just stage the KVs, no locks held across phases
    PendingTx tx;
    for (int i = 0; i < req.input_count; i++) {
        std::string val(req.inputs[i].value, strnlen(req.inputs[i].value, MAX_VAL_SIZE));
        tx.kvs.emplace_back(req.inputs[i].key, std::move(val));
    }

    std::lock_guard<std::mutex> lk(pending_mtx);
    pending_txs[req.tx_id] = std::move(tx);
    return true;
}

void Node::handle_commit(uint64_t tx_id) {
    PendingTx tx;
    {
        std::lock_guard<std::mutex> lk(pending_mtx);
        auto it = pending_txs.find(tx_id);
        if (it == pending_txs.end()) return;
        tx = std::move(it->second);
        pending_txs.erase(it);
    }

    // Lock stripes (sorted to avoid deadlock), apply, unlock
    std::set<size_t> stripes;
    for (auto& [key, val] : tx.kvs)
        stripes.insert(stripe(key));

    std::vector<std::unique_lock<std::shared_timed_mutex>> held;
    for (size_t s : stripes)
        held.emplace_back(locks[s]);

    for (auto& [key, val] : tx.kvs)
        table.insert_or_assign(key, val);
    // locks released when `held` goes out of scope
}

void Node::handle_abort(uint64_t tx_id) {
    std::lock_guard<std::mutex> lk(pending_mtx);
    pending_txs.erase(tx_id);
}

// ─── helpers ────────────────────────────────────────────────────────────────

static void fill_wire_kv(WireKV &wkv, int32_t key, const std::string &val) {
    wkv.key = key;
    std::memset(wkv.value, 0, MAX_VAL_SIZE);
    std::memcpy(wkv.value, val.data(), std::min(val.size(), MAX_VAL_SIZE));
}

// ─── 2PC coordinator: put() ─────────────────────────────────────────────────

bool Node::put(const int32_t &key, const std::string &val){
    const auto N = static_cast<int8_t>(all_nodes.size());
    int8_t owner = key % N;
    int8_t replica = (owner + 1) % N;
    uint64_t txid = next_tx_id();

    // Determine which remote nodes to contact
    // Participants: owner and replica (may include us)
    std::set<int8_t> remote_participants;
    if (owner != my_idx) remote_participants.insert(owner);
    if (replica != my_idx) remote_participants.insert(replica);

    // Phase 1: PREPARE
    bool all_ok = true;
    std::set<int8_t> voted_yes;  // remote nodes that said yes

    // Prepare locally if we're a participant
    bool local_participant = (owner == my_idx || replica == my_idx);
    bool local_prepared = false;
    if (local_participant) {
        PendingTx tx;
        tx.kvs.emplace_back(key, val);
        {
            std::lock_guard<std::mutex> lk(pending_mtx);
            pending_txs[txid] = std::move(tx);
        }
        local_prepared = true;
    }

    // Prepare remote participants
    if (all_ok) {
        for (int8_t dest : remote_participants) {
            Request req{};
            req.tx_id = txid;
            req.src = my_idx;
            req.dest = dest;
            req.op = PREPARE_PUT;
            req.input_count = 1;
            fill_wire_kv(req.inputs[0], key, val);

            Response resp{};
            send_request(dest, req, resp);
            if (resp.success) {
                voted_yes.insert(dest);
            } else {
                all_ok = false;
                break;
            }
        }
    }

    // Phase 2: COMMIT or ABORT
    if (all_ok) {
        // Commit remote
        for (int8_t dest : voted_yes) {
            Request req{};
            req.tx_id = txid;
            req.src = my_idx;
            req.dest = dest;
            req.op = COMMIT;
            Response resp{};
            send_request(dest, req, resp);
        }
        // Commit local
        if (local_prepared) handle_commit(txid);
        return true;
    } else {
        // Abort remote
        for (int8_t dest : voted_yes) {
            Request req{};
            req.tx_id = txid;
            req.src = my_idx;
            req.dest = dest;
            req.op = ABORT;
            Response resp{};
            send_request(dest, req, resp);
        }
        // Abort local
        if (local_prepared) handle_abort(txid);
        return false;
    }
}

// ─── 2PC coordinator: put3() ────────────────────────────────────────────────

bool Node::put3(const std::array<KV_Pair, 3> &kvs){
    const auto N = static_cast<int8_t>(all_nodes.size());
    uint64_t txid = next_tx_id();

    // For each key, determine owner and replica
    // Group KVs by participant node (each node gets the KVs it holds as owner OR replica)
    struct ParticipantData {
        std::vector<std::pair<int32_t, std::string>> kvs;
    };
    std::unordered_map<int8_t, ParticipantData> participants;

    for (int i = 0; i < 3; i++) {
        int8_t owner = kvs[i].key % N;
        int8_t replica = (owner + 1) % N;
        participants[owner].kvs.emplace_back(kvs[i].key, kvs[i].value);
        if (replica != owner)
            participants[replica].kvs.emplace_back(kvs[i].key, kvs[i].value);
    }

    bool all_ok = true;
    std::set<int8_t> voted_yes;
    bool local_prepared = false;

    // Prepare local if we're a participant
    auto local_it = participants.find(my_idx);
    if (local_it != participants.end()) {
        PendingTx tx;
        tx.kvs = local_it->second.kvs;
        {
            std::lock_guard<std::mutex> lk(pending_mtx);
            pending_txs[txid] = std::move(tx);
        }
        local_prepared = true;
    }

    // Prepare remote participants
    if (all_ok) {
        for (auto& [node_id, pdata] : participants) {
            if (node_id == my_idx) continue;

            Request req{};
            req.tx_id = txid;
            req.src = my_idx;
            req.dest = node_id;
            req.op = PREPARE_PUT3;
            req.input_count = static_cast<uint8_t>(pdata.kvs.size());
            for (size_t j = 0; j < pdata.kvs.size() && j < 3; j++) {
                fill_wire_kv(req.inputs[j], pdata.kvs[j].first, pdata.kvs[j].second);
            }

            Response resp{};
            send_request(node_id, req, resp);
            if (resp.success) {
                voted_yes.insert(node_id);
            } else {
                all_ok = false;
                break;
            }
        }
    }

    // Phase 2
    if (all_ok) {
        for (int8_t dest : voted_yes) {
            Request req{};
            req.tx_id = txid;
            req.src = my_idx;
            req.dest = dest;
            req.op = COMMIT;
            Response resp{};
            send_request(dest, req, resp);
        }
        if (local_prepared) handle_commit(txid);
        return true;
    } else {
        for (int8_t dest : voted_yes) {
            Request req{};
            req.tx_id = txid;
            req.src = my_idx;
            req.dest = dest;
            req.op = ABORT;
            Response resp{};
            send_request(dest, req, resp);
        }
        if (local_prepared) handle_abort(txid);
        return false;
    }
}

// ─── get() — unchanged, no 2PC needed ───────────────────────────────────────

std::string Node::get(const int32_t &key){
    const auto N = static_cast<int8_t>(all_nodes.size());
    int8_t owner = key % N;

    int8_t owner_replica = (owner + 1) % N;
    if (owner == my_idx || owner_replica == my_idx) {
        std::shared_lock<std::shared_timed_mutex> lk(locks[stripe(key)]);
        std::string result;
        table.if_contains(key, [&result](const auto& item) {
            result = item.second;
        });
        return result;
    } else {
        int8_t dest = (std::rand() % 2) ? owner : owner_replica;

        Request req{};
        req.src = my_idx;
        req.dest = dest;
        req.op = GET;
        req.input_count = 1;
        req.inputs[0].key = key;

        Response resp{};
        send_request(dest, req, resp);

        return std::string(resp.output, strnlen(resp.output, MAX_VAL_SIZE));
    }
}
