local inflight = KEYS[1]
local tokens = KEYS[2]
local leases = KEYS[3]
local states = KEYS[4]

local id = ARGV[1]
local token = ARGV[2]
local visibility_ms = tonumber(ARGV[3])

if redis.call("HGET", tokens, id) ~= token then
	return {"lost_ownership", id}
end
if redis.call("HGET", states, id) ~= "processing" then
	return {"lost_ownership", id}
end

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)
local deadline = now + visibility_ms
redis.call("ZADD", inflight, deadline, id)
redis.call("HSET", leases, id, deadline)

return {"ok", id, deadline}
