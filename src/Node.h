
#include "../libs/data_structures/hashmap/phmap.hpp"
#include "../libs/data_structures/concurrentqueue.h"
#include "../libs/utils.hpp"

#include <array>
#include <cstdint>
#include <mutex>
#include <atomic>
#include <netinet/in.h>
#include <optional>
#include <string>
#include <vector>
#include <arpa/inet.h>

class Node{
    struct Peers{
        sockaddr_in left;
        sockaddr_in right;
        sockaddr_in farthest;

        int left_conn;
        int right_conn;
        int farthest_conn;
        
        int8_t left_idx;
        int8_t right_idx;
        int8_t farthest_idx;
    };
    
    gtl::parallel_flat_hash_map_m<int32_t, std::array<char, VALUE_SIZE>> table_1;
    gtl::parallel_flat_hash_map_m<int32_t, std::array<char, VALUE_SIZE>> table_2;
    
    std::shared_timed_mutex t1_mtx;
    std::shared_timed_mutex t2_mtx;
    
    std::vector<sockaddr_in> all_nodes;
    
    int PORT = 6000;
    
    Peers peers;

    int server_fd;
    
    int8_t my_idx;
    
    std::atomic_uint32_t req_id = 0;
    
    void parse_node_addrs(int argc, char** argv);
    void indentify_peers(int myIdx);
    void establish_connections(int myIdx);

    int create_server(int port);

    int accept_conn() const;
    
    size_t send_request(const Request &req, Response &resp);
    
    
    void recv_request();


    void handle_request(const Request &req, Response &resp);


    
public:
   Node();
   
   bool put(KV_Pair input);
   
   bool put(std::array<KV_Pair, 3> input);
   
   std::array<char, VALUE_SIZE> get(int32_t input);
    

};