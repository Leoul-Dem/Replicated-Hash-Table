//
// Created by leoul on 2/18/26.
//
//

#include "Node.h"
#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <sys/socket.h>
#include <thread>

void Node::parse_node_addrs(const int argc, char** argv){

    const int8_t myIdx = atoi(argv[1]);
    
    for (int i = 2; i < argc; i++) {
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(PORT);
        inet_pton(AF_INET, argv[i], &addr.sin_addr);
        all_nodes.push_back(addr);
    }
    indentify_peers(myIdx);
}

void Node::indentify_peers(const int myIdx){
    if (myIdx == 0)
        peers.left = all_nodes[all_nodes.size()-1];
    else
        peers.left = all_nodes[myIdx-1];


    if (myIdx == all_nodes.size()-1)
        peers.right = all_nodes[0];
    else
        peers.right = all_nodes[myIdx+1];

    peers.farthest = all_nodes[(myIdx + static_cast<int>(all_nodes.size()) / 2) % static_cast<int>(all_nodes.size())];
}

int Node::create_server(const int port){
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1)
        return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == -1){
        close(server_fd);
        return -1;
    }

    return 0;
}

int Node::accept_conn() const{
    sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    const int conn_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
    if (conn_fd == -1) {
        perror("accept");
    }
    return conn_fd;
}

void Node::establish_connections(const int myIdx){
    // In a circular topology each node connects to its left, right, and farthest peers.
    // To avoid deadlock we use a convention: a node initiates a connection to peers
    // with a higher index and accepts connections from peers with a lower index.

    peers.left_idx  = myIdx == 0 ? static_cast<int>(all_nodes.size()) - 1 : myIdx - 1;
    peers.right_idx = myIdx == static_cast<int>(all_nodes.size()) - 1 ? 0 : myIdx + 1;
    peers.farthest_idx = (myIdx + static_cast<int>(all_nodes.size()) / 2) % static_cast<int>(all_nodes.size());

    const int peer_indices[3]  = { peers.left_idx, peers.right_idx, peers.farthest_idx };
    int* peer_conns[3]   = { &peers.left_conn, &peers.right_conn, &peers.farthest_conn };

    // Count how many peers we need to accept connections from (those with lower index)
    int accept_count = 0;
    for (int i = 0; i < 3; i++) {
        if (peer_indices[i] < myIdx)
            accept_count++;
    }

    // Listen for incoming connections from lower-indexed peers
    if (accept_count > 0) {
        listen(server_fd, accept_count);

        for (int a = 0; a < accept_count; a++) {
            sockaddr_in client_addr{};
            socklen_t addr_len = sizeof(client_addr);
            const int conn_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &addr_len);
            if (conn_fd == -1) {
                perror("accept");
                continue;
            }

            // Match the accepted connection to the correct peer by address
            for (int i = 0; i < 3; i++) {
                if (peer_indices[i] < myIdx &&
                    client_addr.sin_addr.s_addr == all_nodes[peer_indices[i]].sin_addr.s_addr) {
                    *peer_conns[i] = conn_fd;
                    break;
                }
            }
        }
    }

    // Initiate connections to higher-indexed peers
    for (int i = 0; i < 3; i++) {
        if (peer_indices[i] > myIdx) {
            const int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock == -1) {
                perror("socket");
                continue;
            }

            if (connect(sock, reinterpret_cast<sockaddr*>(&all_nodes[peer_indices[i]]),
                        sizeof(sockaddr_in)) == -1) {
                perror("connect");
                close(sock);
                continue;
            }

            *peer_conns[i] = sock;
        }
    }
}

size_t Node::send_request(const Request &req, Response &resp){
    std::array<uint8_t, BUF_SIZE> send_buf;
    std::array<uint8_t, BUF_SIZE> recv_buf;
    
    serialize(req, send_buf);
    const uint8_t my_diff = std::abs(req.dest - req.src);


    if(const uint8_t farthest_diff = std::abs(req.dest - peers.farthest_idx); farthest_diff < my_diff + 1){
        send(peers.farthest_conn, &send_buf, BUF_SIZE, 0);
        
        recv(peers.farthest_conn, &recv_buf, BUF_SIZE, 0);
        deserialize(recv_buf, resp);
        
        return recv_buf.size();
    }
    const int8_t N = static_cast<int8_t>(all_nodes.size());
    const int8_t dest = req.dest;

    const int8_t left_dist  = (dest - my_idx + N) % N;
    const int8_t right_dist = (my_idx - dest + N) % N;

    const int conn = left_dist <= right_dist ? peers.right_conn : peers.left_conn;
    send(conn, &send_buf, BUF_SIZE, 0);
        
    recv(conn, &recv_buf, BUF_SIZE, 0);
    deserialize(recv_buf, resp);
        
    return recv_buf.size();
}


void Node::recv_request(){
    auto run = [this](int conn){
        while(true){
            std::array<uint8_t, BUF_SIZE> recv_buf;
            std::array<uint8_t, BUF_SIZE> send_buf;
            
            recv(conn, &recv_buf, BUF_SIZE, 0);
            Request req{};
            Response resp{};
            deserialize(recv_buf, req);
            
            handle_request(req, resp);
            
            serialize(resp, send_buf);
            send(conn, &send_buf, BUF_SIZE, 0);
        }
    };
    
    std::thread t1(run, peers.left_conn);
    std::thread t2(run, peers.right_conn);
    std::thread t3(run, peers.farthest_conn);
    
    // stay here until sigterm
    
    
    t1.join();
    t2.join();
    t3.join(); 
}

void Node::handle_request(const Request &req, Response &resp){
    switch (req.op) {
        case GET:{
            resp.id = req.id;
            resp.src = req.src;
            resp.dest = req.dest;
            resp.output = this->get(req.inputs[0].key);
        }
        case PUT:{
            resp.id = req.id;
            resp.src = req.src;
            resp.dest = req.dest;
            if(req.inputs.size() < 3){
                resp.output[1] = this->put(req.inputs[0]) ? '1' : '\0';
            }else{
                resp.output[1] = this->put(req.inputs) ? '1' : '\0';
            }
        }
    }
}