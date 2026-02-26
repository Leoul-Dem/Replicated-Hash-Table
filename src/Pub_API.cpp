#include "Node.h"
#include <array>
#include <atomic>
#include <chrono>
#include <mutex>
#include <shared_mutex>




bool Node::put(KV_Pair input, bool original){
    int8_t kv_idx = input.key % all_nodes.size();
    
    if(kv_idx == my_idx && original){
        std::unique_lock<std::shared_timed_mutex>lock(t2_mtx, std::chrono::milliseconds(5));
        if(!lock.owns_lock()){
            return false;
        }
        
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, peers.left_idx, PUT, 1, {input}};
        Response_Full resp{};

        size_t size_recvd = send_request(req, resp);

        if(resp.success){
            table_2.emplace(input.key, input.value);
            return true;
        }

        return false;

    }else if(kv_idx == peers.right_idx && original){
        std::unique_lock<std::shared_timed_mutex>lock(t1_mtx, std::chrono::milliseconds(5));
        if(!lock.owns_lock()){
            return false;
        }

        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, kv_idx, PUT, 1, {input}};
        Response_Full resp{};
        
        size_t size_recvd = send_request(req, resp);
        
        if(resp.success){
            table_1.emplace(input.key, input.value);
            return true;
        }
        
        return false;       
        
    }else if(kv_idx == my_idx){
        std::unique_lock<std::shared_timed_mutex>lock(t2_mtx, std::chrono::milliseconds(5));
        if(!lock.owns_lock()){
            return false;
        }
        
        table_2.emplace(input.key, input.value);;
        return true;
    }else{
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, kv_idx, PUT, 1, {input}};
        Response_Full resp{};

        size_t size_recvd = send_request(req, resp);

        return resp.success; 
    }
}

bool Node::put(std::array<KV_Pair, 3> input, bool original){
    // this will be implemented later
    return false;
}

std::array<char, VALUE_SIZE> Node::get(int32_t input){
    int8_t kv_idx = input % all_nodes.size();
    
    if(kv_idx == my_idx){
        std::shared_lock<std::shared_timed_mutex>lock(t2_mtx, std::chrono::milliseconds(5));
        if(lock.owns_lock()){
            std::array<char, VALUE_SIZE> found_value{};
            table_2.if_contains(input, [&found_value](const auto& item) {
                found_value = item.second;
            });
            return found_value;
        }
    
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, peers.left_idx, GET, 1, {{input, ""}}};
        Response_Full resp{};
        
        send_request(req, resp);
        
        return resp.output;
        
    }else{
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, kv_idx, GET, 1, {{input, ""}}};
        Response_Full resp{};
        
        send_request(req, resp);
        
        return resp.output;  
    }
}

