local done = KEYS[1]
local dedupe = KEYS[2]
local payloads = KEYS[3]
local headers = KEYS[4]
local states = KEYS[5]
local attempts = KEYS[6]
local tokens = KEYS[7]
local leases = KEYS[8]
local errors = KEYS[9]
local metadata = KEYS[10]

local id = ARGV[1]

if redis.call("ZSCORE", done, id) == false then
	return {"missing", id}
end

redis.call("ZREM", done, id)
redis.call("ZREM", dedupe, id)
redis.call("HDEL", payloads, id)
redis.call("HDEL", headers, id)
redis.call("HDEL", states, id)
redis.call("HDEL", attempts, id)
redis.call("HDEL", tokens, id)
redis.call("HDEL", leases, id)
redis.call("HDEL", errors, id)
redis.call("HDEL", metadata, id)

return {"ok", id}
