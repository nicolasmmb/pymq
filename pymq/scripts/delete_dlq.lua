local dlq = KEYS[1]
local payloads = KEYS[2]
local headers = KEYS[3]
local states = KEYS[4]
local attempts = KEYS[5]
local tokens = KEYS[6]
local leases = KEYS[7]
local errors = KEYS[8]
local metadata = KEYS[9]

local id = ARGV[1]

if redis.call("ZSCORE", dlq, id) == false then
	return {"missing", id}
end

redis.call("ZREM", dlq, id)
redis.call("HDEL", payloads, id)
redis.call("HDEL", headers, id)
redis.call("HDEL", states, id)
redis.call("HDEL", attempts, id)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
redis.call("HDEL", errors, id)
redis.call("HDEL", metadata, id)

return {"ok", id}
