
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

struct Request{
    uint32_t id;
    int8_t src;
    int8_t dest;
    Op op;
    std::array<KV_Pair, 3> inputs;
};

struct Response{
    uint32_t id;
    int8_t src;
    int8_t dest;
    std::array<char, VALUE_SIZE> output; 
};

size_t serialize(const Request &req, std::array<uint8_t, 512> &buf) {
    static_assert(std::is_trivially_copyable_v<Request>);
    static_assert(sizeof(Request) <= BUF_SIZE);

    std::memcpy(buf.data(), &req, sizeof(Request));
    return sizeof(Request);
}

size_t deserialize(const std::array<uint8_t, 512> &buf, Request &req) {
    static_assert(std::is_trivially_copyable_v<Request>);

    std::memcpy(&req, buf.data(), sizeof(Request));
    return sizeof(Response);
}

size_t serialize(const Response &resp, std::array<uint8_t, 512> &buf) {
    static_assert(std::is_trivially_copyable_v<Response>);
    static_assert(sizeof(Response) <= BUF_SIZE);

    std::memcpy(buf.data(), &resp, sizeof(Response));
    return sizeof(Response);

}

size_t deserialize(const std::array<uint8_t, 512> &buf, Response &resp) {
    static_assert(std::is_trivially_copyable_v<Response>);

    std::memcpy(&resp, buf.data(), sizeof(Response));
    return sizeof(Response);
}