
#pragma once

#include <array>
#include <cassert>
#include <cstdint>
#include <cstring>

constexpr size_t BUF_SIZE = 512;
constexpr size_t VALUE_SIZE = 128;

enum Op{
    PUT,
    GET,
};

struct KV_Pair{
    int32_t key;
    std::array<char, VALUE_SIZE> value;  
};

// The id between the two fulls and the two cuts is the same, but id is not the same between a full and cut.

struct Request_Full{
    uint32_t id;
    int8_t src;
    int8_t dest;
    Op op;
    uint8_t input_count;
    std::array<KV_Pair, 3> inputs;
};

struct Response_Full{
    uint32_t id;
    int8_t src;
    int8_t dest;
    bool success;
    std::array<char, VALUE_SIZE> output; 
};

struct Request_Cut{
    uint32_t id;
    Op op;
    uint8_t input_count;
    std::array<KV_Pair, 3> inputs;
};

struct Response_Cut{
    uint32_t id;
    bool success;
    std::array<char, VALUE_SIZE> output;
};

inline size_t serialize(const Request_Full &req, std::array<uint8_t, 512> &buf) {
    static_assert(std::is_trivially_copyable_v<Request_Full>);
    static_assert(sizeof(Request_Full) <= BUF_SIZE);

    std::memcpy(buf.data(), &req, sizeof(Request_Full));
    return sizeof(Request_Full);
}

inline size_t deserialize(const std::array<uint8_t, 512> &buf, Request_Full &req) {
    static_assert(std::is_trivially_copyable_v<Request_Full>);

    std::memcpy(&req, buf.data(), sizeof(Request_Full));
    return sizeof(Request_Full);
}

inline size_t serialize(const Response_Full &resp, std::array<uint8_t, 512> &buf) {
    static_assert(std::is_trivially_copyable_v<Response_Full>);
    static_assert(sizeof(Response_Full) <= BUF_SIZE);

    std::memcpy(buf.data(), &resp, sizeof(Response_Full));
    return sizeof(Response_Full);

}

inline size_t deserialize(const std::array<uint8_t, 512> &buf, Response_Full &resp) {
    static_assert(std::is_trivially_copyable_v<Response_Full>);

    std::memcpy(&resp, buf.data(), sizeof(Response_Full));
    return sizeof(Response_Full);
}