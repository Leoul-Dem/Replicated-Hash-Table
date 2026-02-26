//
// Created by leoul on 2/25/26.
//
//


#include "Node.h"
#include "concurrentqueue.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <system_error>
#include <thread>

int runClient(moodycamel::ConcurrentQueue<Request_Cut> &req_queue,
              moodycamel::ConcurrentQueue<Response_Cut> &resp_queue,
              std::atomic<bool> &running){

    std::atomic<uint32_t> idx = 0;
    gtl::parallel_flat_hash_map_m<uint32_t, Op> map;

    auto request = [&](){
        while(running.load()){
            Op op = std::rand() % 2 ? Op::GET : Op::PUT;
            int32_t key = std::rand() % 1000000;
            std::array<char, 128> value = {'b', 'l', 'a', 'h'};
            if(op == Op::PUT){
                auto temp_idx = idx.fetch_add(1);
                Request_Cut req = {temp_idx, op, 1, {{key, value}}};
                req_queue.enqueue(req);
                map.emplace(temp_idx, req.op);
            }else{
                auto temp_idx = idx.fetch_add(1);
                Request_Cut req = {temp_idx, op, 1, {{key, }}};
                req_queue.enqueue(req);
                map.emplace(temp_idx, req.op);
            }
        }
    };

    auto read_verify = [&](){
        while(running.load()){
            Response_Cut resp{};
            if(!resp_queue.try_dequeue(resp)){
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            map.erase_if(resp.id, [&resp](const auto& item){
                Op op = item.second;
                if(op == Op::GET){
                    return std::any_of(resp.output.begin(), resp.output.end(), [](char c){ return c != 0; });
                } else {
                    return resp.success;
                }
            });
        }
    };

    std::thread t1(request);
    std::thread t2(request);
    std::thread t3(request);

    std::thread t4(read_verify);
    std::thread t5(read_verify);
    std::thread t6(read_verify);

    // Wait until main signals shutdown
    while(running.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

    t1.join();
    t2.join();
    t3.join();

    t4.join();
    t5.join();
    t6.join();

    return 0;
}