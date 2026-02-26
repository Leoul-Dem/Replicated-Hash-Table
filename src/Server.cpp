//
// Created by leoul on 2/25/26.
//


#include "Node.h"
#include "concurrentqueue.h"
#include <atomic>
#include <thread>

int runServer(const int port, const int argc, char** argv,
              moodycamel::ConcurrentQueue<Request_Cut> &req_queue,
              moodycamel::ConcurrentQueue<Response_Cut> &resp_queue,
              std::atomic<bool> &running){
    Node node{port, argc, argv};


    auto run = [&](){
        while(running.load()){
            Request_Cut req{};
            Response_Cut resp{};

            if(!req_queue.try_dequeue(req)){
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            switch (req.op) {
                case PUT:{
                    if(req.input_count < 3){
                        resp.id = req.id;
                        resp.success = node.put(req.inputs[0], true);
                    }else{
                        resp.id = req.id;
                        resp.success = node.put(req.inputs, true);
                    }

                    resp_queue.enqueue(resp);
                    break;
                }
                case GET:{
                    resp.id = req.id;
                    resp.output = node.get(req.inputs[0].key);

                    resp_queue.enqueue(resp);
                    break;
                }
                default:
                    break;
            }
        }
    };


    std::thread t1(run);
    std::thread t2(run);
    std::thread t3(run);
    std::thread t4(&Node::recv_request, &node);

    // Wait until main signals shutdown
    while(running.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

    node.stop();

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    return 0;
}
