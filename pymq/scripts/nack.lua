local ready = KEYS[1]
local inflight = KEYS[2]
local dlq = KEYS[3]
local states = KEYS[4]
local attempts = KEYS[5]
local tokens = KEYS[6]
local leases = KEYS[7]
local errors = KEYS[8]

local id = ARGV[1]
local token = ARGV[2]
local max_attempts = tonumber(ARGV[3])
local backoff_ms = tonumber(ARGV[4])
local error_msg = ARGV[5]

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

if redis.call("HGET", tokens, id) ~= token then
	return {"lost_ownership", id}
end

local attempt = tonumber(redis.call("HGET", attempts, id) or "0")
redis.call("ZREM", inflight, id)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
if error_msg ~= "" then
	redis.call("HSET", errors, id, error_msg)
end

if max_attempts == 0 or attempt >= max_attempts then
	redis.call("HSET", states, id, "dlq")
	redis.call("ZADD", dlq, now, id)
	return {"dlq", id}
end

redis.call("HSET", states, id, "ready")
redis.call("ZADD", ready, now + backoff_ms, id)
return {"retry", id}
