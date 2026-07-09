local ready = KEYS[1]
local inflight = KEYS[2]
local dlq = KEYS[3]
local states = KEYS[4]
local attempts = KEYS[5]
local tokens = KEYS[6]
local leases = KEYS[7]
local errors = KEYS[8]

local max_attempts = tonumber(ARGV[1])
local backoff_ms = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)
local ids = redis.call("ZRANGEBYSCORE", inflight, "-inf", now, "LIMIT", 0, limit)
local moved = 0
local retried = 0
local dead = 0

for _, id in ipairs(ids) do
	local attempt = tonumber(redis.call("HGET", attempts, id) or "0")
	redis.call("ZREM", inflight, id)
	redis.call("HDEL", tokens, id)
	redis.call("HDEL", leases, id)
	if max_attempts > 0 and attempt >= max_attempts then
		redis.call("HSET", states, id, "dlq")
		redis.call("HSET", errors, id, "visibility timeout expired")
		redis.call("ZADD", dlq, now, id)
		dead = dead + 1
	else
		redis.call("HSET", states, id, "ready")
		redis.call("ZADD", ready, now + backoff_ms, id)
		retried = retried + 1
	end
	moved = moved + 1
end

return {moved, retried, dead}
