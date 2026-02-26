#include "Node.hpp"
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <thread>
#include <algorithm>

Node::Node(int port, const int argc, const char** argv){
    parse_node_addrs(argc, argv);
    create_server(port);
    establish_conns(my_idx);
}

void Node::parse_node_addrs(const int argc, const char** argv){
    my_idx = std::atoi(argv[1]);

    for (int i = 2; i < argc; i++) {
        asio::ip::tcp::endpoint ep(
            asio::ip::make_address(argv[i]),
            PORT
        );
        all_nodes.push_back(ep);
    }
}

void Node::create_server(const int port){
    acceptor = std::make_unique<asio::ip::tcp::acceptor>(
        io_ctx,
        asio::ip::tcp::endpoint(asio::ip::tcp::v4(), port)
    );
    acceptor->set_option(asio::ip::tcp::acceptor::reuse_address(true));
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

    std::thread accept_thread([&]() {
        for (int a = 0; a < accept_count; a++) {
            auto sock = std::make_unique<asio::ip::tcp::socket>(acceptor->accept());
            auto remote_addr = sock->remote_endpoint().address();
            for (int i = 0; i < N; i++) {
                if (i < myIdx &&
                    remote_addr == all_nodes[i].address() &&
                    accepted_per_peer[i] < CONNS_PER_PEER) {
                    conns[i][accepted_per_peer[i]] = std::move(sock);
                    accepted_per_peer[i]++;
                    break;
                }
            }
        }
    });

    for (int i = 0; i < N; i++) {
        if (i == myIdx) continue;
        if (i > myIdx) {
            for (int c = 0; c < CONNS_PER_PEER; c++) {
                boost::system::error_code ec;
                for (int attempt = 0; attempt < 120; attempt++) {
                    auto sock = std::make_unique<asio::ip::tcp::socket>(io_ctx);
                    sock->connect(all_nodes[i], ec);
                    if (!ec) {
                        conns[i][c] = std::move(sock);
                        break;
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
}

size_t Node::send_request(int8_t dest, const Request &req, Response &resp){
    std::array<uint8_t, BUF_SIZE> send_buf{};
    std::array<uint8_t, BUF_SIZE> recv_buf{};
    boost::system::error_code ec;

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

            boost::system::error_code ec;
            asio::read(conn, asio::buffer(recv_buf), ec);
            if (ec) break;

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

    boost::system::error_code ec;
    const int N = static_cast<int>(all_nodes.size());
    for (int i = 0; i < N; i++) {
        if (i == my_idx) continue;
        for (int c = 0; c < CONNS_PER_PEER; c++) {
            if (conns[i][c]) conns[i][c]->close(ec);
        }
    }
}

void Node::handle_request(const Request &req, Response &resp){
    resp.id = req.id;
    resp.src = req.src;
    resp.dest = req.dest;

    switch (req.op) {
        case GET: {
            std::string result = get(req.inputs[0].key);
            std::memset(resp.output, 0, MAX_VAL_SIZE);
            std::memcpy(resp.output, result.data(),
                        std::min(result.size(), MAX_VAL_SIZE));
            resp.success = !result.empty();
            break;
        }
        case PUT: {
            std::string val(req.inputs[0].value, strnlen(req.inputs[0].value, MAX_VAL_SIZE));
            resp.success = put(req.inputs[0].key, val);
            break;
        }
        case PUT_NOT_OG: {
            std::string val(req.inputs[0].value, strnlen(req.inputs[0].value, MAX_VAL_SIZE));
            resp.success = put_not_og(req.inputs[0].key, val);
            break;
        }
        case PUT3: {
            std::array<KV_Pair, 3> kvs;
            for (int i = 0; i < 3; i++) {
                kvs[i].key = req.inputs[i].key;
                kvs[i].value = std::string(req.inputs[i].value, strnlen(req.inputs[i].value, MAX_VAL_SIZE));
            }
            resp.success = put3(kvs);
            break;
        }
        case PUT3_NOT_OG: {
            std::array<KV_Pair, 3> kvs;
            for (int i = 0; i < 3; i++) {
                kvs[i].key = req.inputs[i].key;
                kvs[i].value = std::string(req.inputs[i].value, strnlen(req.inputs[i].value, MAX_VAL_SIZE));
            }
            resp.success = put3_not_og(kvs);
            break;
        }
    }
}

// --- helpers ---

static void fill_wire_kv(WireKV &wkv, int32_t key, const std::string &val) {
    wkv.key = key;
    std::memset(wkv.value, 0, MAX_VAL_SIZE);
    std::memcpy(wkv.value, val.data(), std::min(val.size(), MAX_VAL_SIZE));
}

// --- Public API ---

bool Node::put(const int32_t &key, const std::string &val){
    const auto N = static_cast<int8_t>(all_nodes.size());
    int8_t owner = key % N;

    if (owner == my_idx) {
        std::unique_lock<std::shared_timed_mutex> lk(locks[stripe(key)]);

        // Synchronous replica write
        int8_t rep = replica_idx();
        Request req{};
        req.src = my_idx;
        req.dest = rep;
        req.op = PUT_NOT_OG;
        req.input_count = 1;
        fill_wire_kv(req.inputs[0], key, val);

        Response resp{};
        send_request(rep, req, resp);

        if (resp.success) {
            table.insert_or_assign(key, val);
            return true;
        }
        return false;
    } else {
        // Forward to owner
        Request req{};
        req.src = my_idx;
        req.dest = owner;
        req.op = PUT;
        req.input_count = 1;
        fill_wire_kv(req.inputs[0], key, val);

        Response resp{};
        send_request(owner, req, resp);
        return resp.success;
    }
}

bool Node::put_not_og(const int32_t &key, const std::string &val){
    std::unique_lock<std::shared_timed_mutex> lk(locks[stripe(key)]);
    table.insert_or_assign(key, val);
    return true;
}

bool Node::put3(const std::array<KV_Pair, 3> &kvs){
    const auto N = static_cast<int8_t>(all_nodes.size());

    // Group keys by owner
    // For each key: if we own it, write locally + fire-and-forget replica
    // If remote owns it, send PUT3_NOT_OG to that owner (they write locally)
    // Fire-and-forget: we don't wait for replica ACKs

    // Lock local stripes first (sorted to prevent deadlock)
    std::array<int, 3> order = {0, 1, 2};
    std::sort(order.begin(), order.end(), [&](int a, int b){
        return stripe(kvs[a].key) < stripe(kvs[b].key);
    });

    std::array<std::unique_lock<std::shared_timed_mutex>, 3> local_locks;
    for (int i = 0; i < 3; i++) {
        int idx = order[i];
        int8_t owner = kvs[idx].key % N;
        if (owner == my_idx || (owner + 1) % N == my_idx) {
            // We hold this key locally (as primary or replica)
            if (i > 0 && stripe(kvs[order[i]].key) == stripe(kvs[order[i-1]].key))
                continue;
            local_locks[idx] = std::unique_lock<std::shared_timed_mutex>(
                locks[stripe(kvs[idx].key)], std::chrono::milliseconds(5));
            if (!local_locks[idx].owns_lock()) return false;
        }
    }

    // Send fire-and-forget replica writes for keys we own
    for (int i = 0; i < 3; i++) {
        int8_t owner = kvs[i].key % N;
        if (owner == my_idx) {
            int8_t rep = replica_idx();
            Request req{};
            req.src = my_idx;
            req.dest = rep;
            req.op = PUT_NOT_OG;
            req.input_count = 1;
            fill_wire_kv(req.inputs[0], kvs[i].key, kvs[i].value);

            Response resp{};
            send_request(rep, req, resp);
            // Fire and forget — don't check resp.success
        }
    }

    // Send PUT3_NOT_OG to remote owners for keys we don't own
    // Group by owner to batch
    struct RemoteGroup { int8_t dest; std::vector<int> indices; };
    std::vector<RemoteGroup> remote_groups;

    for (int i = 0; i < 3; i++) {
        int8_t owner = kvs[i].key % N;
        if (owner == my_idx) continue;

        bool found = false;
        for (auto& g : remote_groups) {
            if (g.dest == owner) {
                g.indices.push_back(i);
                found = true;
                break;
            }
        }
        if (!found)
            remote_groups.push_back({owner, {i}});
    }

    for (auto& g : remote_groups) {
        Request req{};
        req.src = my_idx;
        req.dest = g.dest;
        req.op = PUT3_NOT_OG;
        req.input_count = static_cast<uint8_t>(g.indices.size());
        for (size_t j = 0; j < g.indices.size(); j++) {
            fill_wire_kv(req.inputs[j], kvs[g.indices[j]].key, kvs[g.indices[j]].value);
        }

        Response resp{};
        send_request(g.dest, req, resp);
        // Fire and forget
    }

    // Write all locally-held keys
    for (int i = 0; i < 3; i++) {
        int8_t owner = kvs[i].key % N;
        if (owner == my_idx || (owner + 1) % N == my_idx) {
            table.insert_or_assign(kvs[i].key, kvs[i].value);
        }
    }

    return true;
}

bool Node::put3_not_og(const std::array<KV_Pair, 3> &kvs){
    // Called on a remote primary that received keys from the coordinator.
    // Write locally + fire-and-forget replica for keys we own.
    const auto N = static_cast<int8_t>(all_nodes.size());

    // Lock stripes
    std::array<int, 3> order = {0, 1, 2};
    std::sort(order.begin(), order.end(), [&](int a, int b){
        return stripe(kvs[a].key) < stripe(kvs[b].key);
    });

    std::array<std::unique_lock<std::shared_timed_mutex>, 3> local_locks;
    for (int i = 0; i < 3; i++) {
        int idx = order[i];
        int8_t owner = kvs[idx].key % N;
        if (owner != my_idx) continue;
        if (i > 0 && stripe(kvs[order[i]].key) == stripe(kvs[order[i-1]].key))
            continue;
        local_locks[idx] = std::unique_lock<std::shared_timed_mutex>(
            locks[stripe(kvs[idx].key)], std::chrono::milliseconds(5));
        if (!local_locks[idx].owns_lock()) return false;
    }

    // Fire-and-forget replica writes
    for (int i = 0; i < 3; i++) {
        int8_t owner = kvs[i].key % N;
        if (owner != my_idx) continue;

        int8_t rep = replica_idx();
        Request req{};
        req.src = my_idx;
        req.dest = rep;
        req.op = PUT_NOT_OG;
        req.input_count = 1;
        fill_wire_kv(req.inputs[0], kvs[i].key, kvs[i].value);

        Response resp{};
        send_request(rep, req, resp);
    }

    // Write locally
    for (int i = 0; i < 3; i++) {
        int8_t owner = kvs[i].key % N;
        if (owner != my_idx) continue;
        table.insert_or_assign(kvs[i].key, kvs[i].value);
    }

    return true;
}

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
