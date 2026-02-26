//
// Created by leoul on 2/25/26.
//

#include "concurrentqueue.h"
#include "utils.hpp"
#include <atomic>
#include <complex>
#include <iostream>
#include <thread>
#include <signal.h>

// [program] [my_index] [n * ip addresses]
int runServer(int port, int argc, char** argv,
              moodycamel::ConcurrentQueue<Request_Cut> &req_queue,
              moodycamel::ConcurrentQueue<Response_Cut> &resp_queue,
              std::atomic<bool> &running);

int runClient(moodycamel::ConcurrentQueue<Request_Cut> &req_queue,
              moodycamel::ConcurrentQueue<Response_Cut> &resp_queue,
              std::atomic<bool> &running);

int main(const int argc, char** argv){
    if(argc < 6){
        std::cout << "not enough arguments\n";
        return -1;
    }
    
    int port = 6000;

    // Block signals in main thread before spawning children —
    // child threads inherit the mask, so none of them will handle
    // the signal either. Only sigwait() below will receive it.
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    sigprocmask(SIG_BLOCK, &mask, nullptr);

    std::atomic<bool> running{true};

    moodycamel::ConcurrentQueue<Request_Cut> req_queue;
    moodycamel::ConcurrentQueue<Response_Cut> resp_queue;

    std::thread server_thread(runServer, port, argc, argv,
                              std::ref(req_queue), std::ref(resp_queue),
                              std::ref(running));
    std::thread client_thread(runClient,
                              std::ref(req_queue), std::ref(resp_queue),
                              std::ref(running));

    // Single signal waiter for the whole process
    int sig;
    sigwait(&mask, &sig);

    running.store(false);

    server_thread.join();
    client_thread.join();

    return 0;
}