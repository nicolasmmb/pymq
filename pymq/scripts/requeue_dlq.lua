local ready = KEYS[1]
local dlq = KEYS[2]
local states = KEYS[3]
local attempts = KEYS[4]
local tokens = KEYS[5]
local leases = KEYS[6]
local errors = KEYS[7]

local id = ARGV[1]
local delay_ms = tonumber(ARGV[2])
local reset_attempts = ARGV[3]

if redis.call("ZSCORE", dlq, id) == false then
	return {"missing", id}
end

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

redis.call("ZREM", dlq, id)
redis.call("HSET", states, id, "ready")
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
redis.call("HDEL", errors, id)
if reset_attempts == "1" then
	redis.call("HSET", attempts, id, 0)
end
redis.call("ZADD", ready, now + delay_ms, id)

return {"ok", id}
