local inflight = KEYS[1]
local done = KEYS[2]
local dedupe = KEYS[3]
local payloads = KEYS[4]
local headers = KEYS[5]
local states = KEYS[6]
local attempts = KEYS[7]
local tokens = KEYS[8]
local leases = KEYS[9]
local metadata = KEYS[10]
local errors = KEYS[11]

local id = ARGV[1]
local token = ARGV[2]
local dedupe_ttl_ms = tonumber(ARGV[3])
local retention_policy = ARGV[4]

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

if redis.call("ZSCORE", dedupe, id) then
	return {"already_done", id}
end
if redis.call("HGET", tokens, id) ~= token then
	return {"lost_ownership", id}
end

redis.call("ZREM", inflight, id)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
if retention_policy == "delete_acked" then
	redis.call("HDEL", payloads, id)
	redis.call("HDEL", headers, id)
	redis.call("HDEL", states, id)
	redis.call("HDEL", attempts, id)
	redis.call("HDEL", metadata, id)
	redis.call("HDEL", errors, id)
else
	redis.call("HSET", states, id, "done")
end
redis.call("ZADD", done, now, id)
redis.call("ZADD", dedupe, now, id)

return {"ok", id}
