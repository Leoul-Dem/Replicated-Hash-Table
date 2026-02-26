#include "Node.h"
#include <array>
#include <atomic>
#include <chrono>
#include <climits>
#include <mutex>
#include <shared_mutex>


bool Node::put(KV_Pair input, bool original){
    int8_t kv_idx = input.key % all_nodes.size();
    
    if(kv_idx == my_idx && original){
        std::unique_lock<std::shared_timed_mutex>lock(t2_locks[stripe(input.key)], std::chrono::milliseconds(5));
        if(!lock.owns_lock()){
            return false;
        }
        
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, peers.left_idx, PUT, 1, {input}};
        Response_Full resp{};

        size_t size_recvd = send_request(req, resp);

        if(resp.success){
            todo_2.erase_if(input.key, [](const auto&){return true;});
            table_2.emplace(input.key, input.value);
            return true;
        }else{
            todo_2.erase_if(input.key, [&](const auto& item) {
                table_2.emplace(input.key, item.second);
                return true;
            });
        }

        return false;

    }else if(kv_idx == peers.right_idx && original){
        std::unique_lock<std::shared_timed_mutex>lock(t1_locks[stripe(input.key)], std::chrono::milliseconds(5));
        if(!lock.owns_lock()){
            return false;
        }

        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, kv_idx, PUT, 1, {input}};
        Response_Full resp{};
        
        size_t size_recvd = send_request(req, resp);
        
        if(resp.success){
            todo_1.erase_if(input.key, [](const auto&){return true;});
            table_1.emplace(input.key, input.value);
            return true;
        }else{
            todo_1.erase_if(input.key, [&](const auto& item) {
                table_1.emplace(input.key, item.second);
                return true;
            });
        }
        
        return false;       
        
    }else if(kv_idx == my_idx){
        std::unique_lock<std::shared_timed_mutex>lock(t2_locks[stripe(input.key)], std::chrono::milliseconds(5));
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
    const auto N = static_cast<int8_t>(all_nodes.size());

    if(original){
        // --- Coordinator path ---
        // Sort by stripe index to acquire locks in consistent order (deadlock prevention)
        std::array<int, 3> order = {0, 1, 2};
        std::sort(order.begin(), order.end(), [&](int a, int b){
            return stripe(input[a].key) < stripe(input[b].key);
        });

        // Determine which table each key lives in locally (if any)
        // and acquire the corresponding stripe locks
        std::array<std::unique_lock<std::shared_timed_mutex>, 3> locks;
        for(int i = 0; i < 3; i++){
            int idx = order[i];
            int8_t owner = input[idx].key % N;

            if(owner == my_idx){
                // We are the primary — lock table_2
                // Skip if same stripe already locked by a previous key
                if(i > 0 && stripe(input[order[i]].key) == stripe(input[order[i-1]].key)){
                    continue;
                }
                locks[idx] = std::unique_lock<std::shared_timed_mutex>(
                    t2_locks[stripe(input[idx].key)], std::chrono::milliseconds(5));
                if(!locks[idx].owns_lock()) return false;
            }else if(owner == peers.right_idx){
                // We are the replica — lock table_1
                if(i > 0 && stripe(input[order[i]].key) == stripe(input[order[i-1]].key)){
                    continue;
                }
                locks[idx] = std::unique_lock<std::shared_timed_mutex>(
                    t1_locks[stripe(input[idx].key)], std::chrono::milliseconds(5));
                if(!locks[idx].owns_lock()) return false;
            }
            // else: key is on a remote node, no local lock needed
        }

        // Group keys by destination node
        // Map: dest_idx -> list of input indices
        std::array<std::vector<int>, 1> local_keys;  // keys we are primary for
        std::array<int8_t, 3> owners;
        for(int i = 0; i < 3; i++)
            owners[i] = input[i].key % N;

        // Collect unique remote destinations and their keys
        struct RemoteGroup { int8_t dest; std::vector<int> indices; };
        std::vector<RemoteGroup> remote_groups;
        std::vector<int> my_keys;

        for(int i = 0; i < 3; i++){
            if(owners[i] == my_idx){
                my_keys.push_back(i);
            }else{
                bool found = false;
                for(auto& g : remote_groups){
                    if(g.dest == owners[i]){
                        g.indices.push_back(i);
                        found = true;
                        break;
                    }
                }
                if(!found)
                    remote_groups.push_back({owners[i], {i}});
            }
        }

        bool all_success = true;

        // Fire-and-forget TODO enqueue to our replica for keys we own
        for(int i : my_keys){
            Request_Full todo_req{req_id.fetch_add(1, std::memory_order_relaxed),
                my_idx, peers.left_idx, PUT, 1, {input[i]}};
            Response_Full todo_resp{};
            send_request(todo_req, todo_resp);
            if(!todo_resp.success) all_success = false;
        }

        // Send one batched request per remote destination
        for(auto& g : remote_groups){
            std::array<KV_Pair, 3> batch{};
            for(size_t j = 0; j < g.indices.size(); j++)
                batch[j] = input[g.indices[j]];

            Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed),
                my_idx, g.dest, PUT, static_cast<uint8_t>(g.indices.size()), batch};
            Response_Full resp{};
            send_request(req, resp);
            if(!resp.success) all_success = false;
        }

        // Check if all writes succeeded
        if(all_success){
            // Commit: write to our local tables for keys we own
            for(int i = 0; i < 3; i++){
                int8_t owner = input[i].key % N;
                if(owner == my_idx){
                    todo_2.erase_if(input[i].key, [](const auto&){return true;});
                    table_2.emplace(input[i].key, input[i].value);
                }else if(owner == peers.right_idx){
                    todo_1.erase_if(input[i].key, [](const auto&){return true;});
                    table_1.emplace(input[i].key, input[i].value);
                }
            }
            return true;
        }

        // Abort: restore any TODO entries we may have displaced
        for(int i = 0; i < 3; i++){
            int8_t owner = input[i].key % N;
            if(owner == my_idx){
                todo_2.erase_if(input[i].key, [&](const auto& item){
                    table_2.emplace(input[i].key, item.second);
                    return true;
                });
            }else if(owner == peers.right_idx){
                todo_1.erase_if(input[i].key, [&](const auto& item){
                    table_1.emplace(input[i].key, item.second);
                    return true;
                });
            }
        }
        return false;

    }else{
        // --- Remote primary path (received from coordinator) ---
        // This node is the primary for some of these keys.
        // Lock stripes, enqueue TODO to replica, write locally.
        std::array<int, 3> order = {0, 1, 2};
        std::sort(order.begin(), order.end(), [&](int a, int b){
            return stripe(input[a].key) < stripe(input[b].key);
        });

        std::array<std::unique_lock<std::shared_timed_mutex>, 3> locks;
        for(int i = 0; i < 3; i++){
            int idx = order[i];
            int8_t owner = input[idx].key % N;
            if(owner != my_idx) continue;

            if(i > 0 && stripe(input[order[i]].key) == stripe(input[order[i-1]].key)){
                continue;
            }
            locks[idx] = std::unique_lock<std::shared_timed_mutex>(
                t2_locks[stripe(input[idx].key)], std::chrono::milliseconds(5));
            if(!locks[idx].owns_lock()) return false;
        }

        // Fire-and-forget TODO enqueue to our replica for each key we own
        for(int i = 0; i < 3; i++){
            int8_t owner = input[i].key % N;
            if(owner != my_idx) continue;

            Request_Full todo_req{req_id.fetch_add(1, std::memory_order_relaxed),
                my_idx, peers.left_idx, PUT, 1, {input[i]}};
            Response_Full todo_resp{};
            send_request(todo_req, todo_resp);
        }

        // Write locally
        for(int i = 0; i < 3; i++){
            int8_t owner = input[i].key % N;
            if(owner != my_idx) continue;
            table_2.emplace(input[i].key, input[i].value);
        }
        return true;
    }
}

std::array<char, VALUE_SIZE> Node::get(int32_t input){
    int8_t kv_idx = input % all_nodes.size();
    
    if(kv_idx == my_idx){
        
        std::array<char, VALUE_SIZE> found_value{};
        bool found_in_todo_2 = false;
        todo_2.erase_if(input, [&](const auto& item) {
            std::unique_lock<std::shared_timed_mutex>lock2(t2_locks[stripe(input)]);
            table_2.emplace(input, item.second);
            found_value = item.second;
            found_in_todo_2 = true;
            return true;
        });
        
        if(found_in_todo_2){
            return found_value;
        }
        
        std::shared_lock<std::shared_timed_mutex>lock(t2_locks[stripe(input)], std::chrono::milliseconds(5));
        if(lock.owns_lock()){
            table_2.if_contains(input, [&found_value](const auto& item) {
                found_value = item.second;
            });
            return found_value;
        }
    
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, peers.left_idx, GET, 1, {{input, ""}}};
        Response_Full resp{};
        
        send_request(req, resp);
        
        return resp.output;
        
    }else if (kv_idx == peers.right_idx) {
        std::array<char, VALUE_SIZE> found_value{};
        bool found_in_todo_1 = false;
        todo_1.erase_if(input, [&](const auto& item) {
            std::unique_lock<std::shared_timed_mutex>lock1(t1_locks[stripe(input)]);
            table_1.emplace(input, item.second);
            found_value = item.second;
            found_in_todo_1 = true;
            return true;
        });
        
        if(found_in_todo_1){
            return found_value;
        }
        
        std::shared_lock<std::shared_timed_mutex>lock(t1_locks[stripe(input)]);
        table_1.if_contains(input, [&found_value](const auto& item) {
            found_value = item.second;
        });
        return found_value;
        
    }else{
        Request_Full req{req_id.fetch_add(1, std::memory_order_relaxed), my_idx, kv_idx, GET, 1, {{input, ""}}};
        Response_Full resp{};
        
        send_request(req, resp);
        
        return resp.output;  
    }
}

