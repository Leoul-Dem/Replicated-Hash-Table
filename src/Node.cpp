//
// Created by leoul on 2/18/26.
//
//

#include "Node.h"
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <thread>
#include <signal.h>

Node::Node(const int port, const int argc, char** argv){
    this->parse_node_addrs(argc, argv);
    this->create_server(port);
    this->establish_connections(my_idx);
}

void Node::parse_node_addrs(const int argc, char** argv){
    my_idx = atoi(argv[1]);

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

void Node::establish_connections(const int myIdx){
    // In a circular topology each node connects to its left, right, and farthest peers.
    // Each node accepts from lower-indexed peers and connects to higher-indexed peers.
    // A background thread handles accepts concurrently with connects to avoid deadlock
    // when farthest peers create out-of-order index dependencies.

    const int N = static_cast<int>(all_nodes.size());
    peers.left_idx  = myIdx == 0 ? N - 1 : myIdx - 1;
    peers.right_idx = myIdx == N - 1 ? 0 : myIdx + 1;
    peers.farthest_idx = (myIdx + N / 2) % N;

    const int peer_indices[3] = { peers.left_idx, peers.right_idx, peers.farthest_idx };
    std::unique_ptr<asio::ip::tcp::socket>* peer_conns[3] = {
        &peers.left_conn, &peers.right_conn, &peers.farthest_conn
    };

    // Count how many peers we need to accept connections from (those with lower index)
    int accept_count = 0;
    for (int i = 0; i < 3; i++) {
        if (peer_indices[i] < myIdx)
            accept_count++;
    }

    acceptor->listen(accept_count);

    // Accept from lower-indexed peers in a background thread so connects can proceed concurrently
    std::thread accept_thread([&]() {
        for (int a = 0; a < accept_count; a++) {
            auto sock = std::make_unique<asio::ip::tcp::socket>(acceptor->accept());

            // Match the accepted connection to the correct peer by address
            auto remote_addr = sock->remote_endpoint().address();
            for (int i = 0; i < 3; i++) {
                if (peer_indices[i] < myIdx &&
                    remote_addr == all_nodes[peer_indices[i]].address()) {
                    *peer_conns[i] = std::move(sock);
                    break;
                }
            }
        }
    });

    // Initiate connections to higher-indexed peers concurrently with the accept thread.
    // Retry with backoff since peers may not be listening yet.
    for (int i = 0; i < 3; i++) {
        if (peer_indices[i] > myIdx) {
            asio::error_code ec;
            for (int attempt = 0; attempt < 120; attempt++) {
                auto sock = std::make_unique<asio::ip::tcp::socket>(io_ctx);
                sock->connect(all_nodes[peer_indices[i]], ec);
                if (!ec) {
                    *peer_conns[i] = std::move(sock);
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
            }
            if (ec) {
                std::fprintf(stderr, "Failed to connect to node %d after retries: %s\n",
                             peer_indices[i], ec.message().c_str());
                std::exit(1);
            }
        }
    }

    accept_thread.join();
}

size_t Node::send_request(const Request_Full &req, Response_Full &resp){
    std::array<uint8_t, BUF_SIZE> send_buf{};
    std::array<uint8_t, BUF_SIZE> recv_buf{};
    asio::error_code ec;

    serialize(req, send_buf);
    const auto N = static_cast<int8_t>(all_nodes.size());
    const auto ring_dist = [N](int8_t a, int8_t b) -> uint8_t {
        int8_t d = (b - a + N) % N;
        return std::min(static_cast<uint8_t>(d), static_cast<uint8_t>(N - d));
    };
    const uint8_t my_diff = ring_dist(req.src, req.dest);

    if(const uint8_t farthest_diff = ring_dist(peers.farthest_idx, req.dest); farthest_diff < my_diff + 1){
        std::lock_guard<std::mutex> lock(peers.farthest_mtx);
        asio::write(*peers.farthest_conn, asio::buffer(send_buf), ec);
        if(ec) return 0;
        asio::read(*peers.farthest_conn, asio::buffer(recv_buf), ec);
        if(ec) return 0;
        deserialize(recv_buf, resp);
        return recv_buf.size();
    }
    const int8_t dest = req.dest;

    const int8_t left_dist  = (dest - my_idx + N) % N;
    const int8_t right_dist = (my_idx - dest + N) % N;

    if(left_dist >= right_dist){
        std::lock_guard<std::mutex> lock(peers.right_mtx);
        asio::write(*peers.right_conn, asio::buffer(send_buf), ec);
        if(ec) return 0;
        asio::read(*peers.right_conn, asio::buffer(recv_buf), ec);
        if(ec) return 0;
    } else {
        std::lock_guard<std::mutex> lock(peers.left_mtx);
        asio::write(*peers.left_conn, asio::buffer(send_buf), ec);
        if(ec) return 0;
        asio::read(*peers.left_conn, asio::buffer(recv_buf), ec);
        if(ec) return 0;
    }
    deserialize(recv_buf, resp);

    return recv_buf.size();
}


void Node::recv_request(){
    auto run = [this](asio::ip::tcp::socket& conn){
        while(running.load()){
            std::array<uint8_t, BUF_SIZE> recv_buf{};
            std::array<uint8_t, BUF_SIZE> send_buf{};

            asio::error_code ec;
            asio::read(conn, asio::buffer(recv_buf), ec);
            if (ec)
                break;

            Request_Full req{};
            Response_Full resp{};
            deserialize(recv_buf, req);

            handle_request(req, resp);

            serialize(resp, send_buf);
            asio::write(conn, asio::buffer(send_buf), ec);
            if (ec)
                break;
        }
    };

    std::thread t1(run, std::ref(*peers.left_conn));
    std::thread t2(run, std::ref(*peers.right_conn));
    std::thread t3(run, std::ref(*peers.farthest_conn));

    t1.join();
    t2.join();
    t3.join();
}

void Node::stop(){
    running.store(false);

    // Closing sockets unblocks any blocked read()/write() calls
    asio::error_code ec;
    if(peers.left_conn)     peers.left_conn->close(ec);
    if(peers.right_conn)    peers.right_conn->close(ec);
    if(peers.farthest_conn) peers.farthest_conn->close(ec);
}

void Node::handle_request(const Request_Full &req, Response_Full &resp){
    switch (req.op) {
        case GET:{
            resp.id = req.id;
            resp.src = req.src;
            resp.dest = req.dest;
            resp.output = this->get(req.inputs[0].key);
            break;
        }

        case PUT:{
            resp.id = req.id;
            resp.src = req.src;
            resp.dest = req.dest;
            if(req.input_count == 1){
                resp.success = this->put(req.inputs[0], false);
            }else{
                resp.success = this->put(req.inputs, false);
            }
            break;
        }
    }
}
