local ready = KEYS[1]
local inflight = KEYS[2]
local payloads = KEYS[3]
local headers = KEYS[4]
local states = KEYS[5]
local attempts = KEYS[6]
local tokens = KEYS[7]
local leases = KEYS[8]
local metadata = KEYS[9]

local visibility_ms = tonumber(ARGV[1])
local token = ARGV[2]

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)
local ids = redis.call("ZRANGEBYSCORE", ready, "-inf", now, "LIMIT", 0, 1)
if #ids == 0 then
	return {"none"}
end

local id = ids[1]
redis.call("ZREM", ready, id)
local deadline = now + visibility_ms
local attempt = redis.call("HINCRBY", attempts, id, 1)
redis.call("ZADD", inflight, deadline, id)
redis.call("HSET", states, id, "processing")
redis.call("HSET", tokens, id, token)
redis.call("HSET", leases, id, deadline)

return {
	"ok",
	id,
	redis.call("HGET", payloads, id) or "",
	redis.call("HGET", headers, id) or "",
	attempt,
	deadline,
	token,
	redis.call("HGET", metadata, id) or "0"
}
