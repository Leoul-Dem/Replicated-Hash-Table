#pragma once

#include "../libs/ds/hashmap/phmap.hpp"
#include "../libs/ds/concurrentqueue.h"
#include "../libs/utils.hpp"
#include <atomic>
#include <cstdint>
#include <cstring>
#include <thread>

inline int runClient(moodycamel::ConcurrentQueue<Request_Cut> &req_queue,
              moodycamel::ConcurrentQueue<Response_Cut> &resp_queue,
              std::atomic<bool> &running){

    std::atomic<uint32_t> idx = 0;
    gtl::parallel_flat_hash_map_m<uint32_t, Op> map;

    static constexpr size_t MAX_INFLIGHT = 32;

    auto request = [&](){
        while(running.load()){
            if(req_queue.size_approx() >= MAX_INFLIGHT){
                std::this_thread::sleep_for(std::chrono::microseconds(10));
                continue;
            }

            int roll = std::rand() % 100;
            auto temp_idx = idx.fetch_add(1);

            Request_Cut req{};
            req.id = temp_idx;

            // 60% GET, 20% single PUT, 20% 3 PUT
            if(roll < 60){
                req.op = GET;
                req.input_count = 1;
                req.inputs[0].key = std::rand() % 1000000;
            } else if(roll < 80){
                req.op = PUT;
                req.input_count = 1;
                req.inputs[0].key = std::rand() % 1000000;
                std::memset(req.inputs[0].value, 0, MAX_VAL_SIZE);
                std::memcpy(req.inputs[0].value, "blah", 4);
            } else {
                req.op = PUT3;
                req.input_count = 3;
                for (int i = 0; i < 3; i++) {
                    req.inputs[i].key = std::rand() % 1000000;
                    std::memset(req.inputs[i].value, 0, MAX_VAL_SIZE);
                    std::memcpy(req.inputs[i].value, "blah", 4);
                }
            }

            req_queue.enqueue(req);
            map.emplace(temp_idx, req.op);
        }
    };

    auto read_verify = [&](){
        while(running.load()){
            Response_Cut resp{};
            if(!resp_queue.try_dequeue(resp)){
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }

            map.erase_if(resp.id, [&resp](const auto& item){
                Op op = item.second;
                if(op == GET){
                    return strnlen(resp.output, MAX_VAL_SIZE) > 0;
                } else {
                    return resp.success;
                }
            });
        }
    };

    std::thread t1(request);
    std::thread t2(request);

    std::thread t3(read_verify);
    std::thread t4(read_verify);

    while(running.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    return 0;
}
