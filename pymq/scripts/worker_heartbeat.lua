local workers = KEYS[1]
local worker_info = KEYS[2]

local worker_id = ARGV[1]
local info_json = ARGV[2]
local ttl_ms = tonumber(ARGV[3])

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

local expired = redis.call("ZRANGEBYSCORE", workers, "-inf", now)
if #expired > 0 then
	redis.call("ZREM", workers, unpack(expired))
	redis.call("HDEL", worker_info, unpack(expired))
end

local expires_at = now + ttl_ms
redis.call("ZADD", workers, expires_at, worker_id)
redis.call("HSET", worker_info, worker_id, info_json)

return {"ok", worker_id, expires_at}
