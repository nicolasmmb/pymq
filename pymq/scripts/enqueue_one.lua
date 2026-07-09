local ready = KEYS[1]
local done = KEYS[2]
local payloads = KEYS[3]
local headers = KEYS[4]
local states = KEYS[5]
local attempts = KEYS[6]
local tokens = KEYS[7]
local leases = KEYS[8]
local metadata = KEYS[9]

local id = ARGV[1]
local payload = ARGV[2]
local header_json = ARGV[3]
local delay_ms = tonumber(ARGV[4])
local dedupe_ttl_ms = tonumber(ARGV[5])

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

if redis.call("ZSCORE", done, id) then
	return {"done", id}
end
if redis.call("HEXISTS", states, id) == 1 then
	return {"duplicate", id}
end

redis.call("HSET", payloads, id, payload)
redis.call("HSET", headers, id, header_json)
redis.call("HSET", states, id, "ready")
redis.call("HSET", attempts, id, 0)
redis.call("HSET", metadata, id, now)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
redis.call("ZADD", ready, now + delay_ms, id)

if dedupe_ttl_ms > 0 then
	redis.call("ZREMRANGEBYSCORE", done, "-inf", now - dedupe_ttl_ms)
end

return {"ok", id}
