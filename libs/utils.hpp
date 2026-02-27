#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <array>
#include <vector>

// Max value size for wire format
constexpr size_t MAX_VAL_SIZE = 128;
constexpr size_t BUF_SIZE = 1024;

enum Op : uint8_t {
    PUT,
    GET,
    PUT3,
    PREPARE_PUT,
    PREPARE_PUT3,
    COMMIT,
    ABORT,
};

struct WireKV {
    int32_t key;
    char value[MAX_VAL_SIZE];
};

struct Request {
    uint32_t id;
    uint64_t tx_id;            
    int8_t src;
    int8_t dest;
    Op op;
    uint8_t input_count;       
    std::array<WireKV, 3> inputs;
};

struct Response {
    uint32_t id;
    uint64_t tx_id;
    int8_t src;
    int8_t dest;
    bool success;
    char output[MAX_VAL_SIZE]; 
};

static_assert(sizeof(Request) <= BUF_SIZE);
static_assert(sizeof(Response) <= BUF_SIZE);

inline void serialize(const Request &req, std::array<uint8_t, BUF_SIZE> &buf) {
    std::memcpy(buf.data(), &req, sizeof(Request));
}

inline void deserialize(const std::array<uint8_t, BUF_SIZE> &buf, Request &req) {
    std::memcpy(&req, buf.data(), sizeof(Request));
}

inline void serialize(const Response &resp, std::array<uint8_t, BUF_SIZE> &buf) {
    std::memcpy(buf.data(), &resp, sizeof(Response));
}

inline void deserialize(const std::array<uint8_t, BUF_SIZE> &buf, Response &resp) {
    std::memcpy(&resp, buf.data(), sizeof(Response));
}

struct Request_Cut {
    uint32_t id;
    Op op;
    uint8_t input_count;
    std::array<WireKV, 3> inputs;
};

struct Response_Cut {
    uint32_t id;
    bool success;
    char output[MAX_VAL_SIZE];
};

struct KV_Pair {
    int32_t key;
    std::string value;
};
