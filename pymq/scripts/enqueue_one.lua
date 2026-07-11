local ready = KEYS[1]
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
local payload = ARGV[2]
local header_json = ARGV[3]
local delay_ms = tonumber(ARGV[4])
local dedupe_ttl_ms = tonumber(ARGV[5])

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

if dedupe_ttl_ms > 0 then
	redis.call("ZREMRANGEBYSCORE", dedupe, "-inf", now - dedupe_ttl_ms)
end
if redis.call("ZSCORE", dedupe, id) then
	return {"done", id}
end

local state = redis.call("HGET", states, id)
local done_score = redis.call("ZSCORE", done, id)
if state ~= false and state ~= "done" then
	return {"duplicate", id}
end

if done_score ~= false and state == false and dedupe_ttl_ms > 0 and tonumber(done_score) > now - dedupe_ttl_ms then
	return {"done", id}
end

if done_score ~= false or state == "done" then
	redis.call("ZREM", done, id)
	redis.call("HDEL", payloads, id)
	redis.call("HDEL", headers, id)
	redis.call("HDEL", states, id)
	redis.call("HDEL", attempts, id)
	redis.call("HDEL", tokens, id)
	redis.call("HDEL", leases, id)
	redis.call("HDEL", metadata, id)
	redis.call("HDEL", errors, id)
end

redis.call("HSET", payloads, id, payload)
redis.call("HSET", headers, id, header_json)
redis.call("HSET", states, id, "ready")
redis.call("HSET", attempts, id, 0)
redis.call("HSET", metadata, id, now)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
redis.call("HDEL", errors, id)
redis.call("ZADD", ready, now + delay_ms, id)

return {"ok", id}
