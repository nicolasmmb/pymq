local inflight = KEYS[1]
local done = KEYS[2]
local payloads = KEYS[3]
local headers = KEYS[4]
local states = KEYS[5]
local attempts = KEYS[6]
local tokens = KEYS[7]
local leases = KEYS[8]
local metadata = KEYS[9]

local id = ARGV[1]
local token = ARGV[2]
local dedupe_ttl_ms = tonumber(ARGV[3])

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

if redis.call("ZSCORE", done, id) then
	return {"already_done", id}
end
if redis.call("HGET", tokens, id) ~= token then
	return {"lost_ownership", id}
end

redis.call("ZREM", inflight, id)
redis.call("HDEL", payloads, id)
redis.call("HDEL", headers, id)
redis.call("HDEL", states, id)
redis.call("HDEL", attempts, id)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
redis.call("HDEL", metadata, id)
redis.call("ZADD", done, now, id)

if dedupe_ttl_ms > 0 then
	redis.call("ZREMRANGEBYSCORE", done, "-inf", now - dedupe_ttl_ms)
end

return {"ok", id}
