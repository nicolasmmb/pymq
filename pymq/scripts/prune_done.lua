local done = KEYS[1]
local payloads = KEYS[2]
local headers = KEYS[3]
local states = KEYS[4]
local attempts = KEYS[5]
local tokens = KEYS[6]
local leases = KEYS[7]
local errors = KEYS[8]
local metadata = KEYS[9]

local older_than_ms = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)
local ids = redis.call("ZRANGEBYSCORE", done, "-inf", now - older_than_ms, "LIMIT", 0, limit)

for _, id in ipairs(ids) do
	redis.call("ZREM", done, id)
	redis.call("HDEL", payloads, id)
	redis.call("HDEL", headers, id)
	redis.call("HDEL", states, id)
	redis.call("HDEL", attempts, id)
	redis.call("HDEL", tokens, id)
	redis.call("HDEL", leases, id)
	redis.call("HDEL", errors, id)
	redis.call("HDEL", metadata, id)
end

return #ids
