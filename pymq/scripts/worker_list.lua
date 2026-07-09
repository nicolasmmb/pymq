local workers = KEYS[1]
local worker_info = KEYS[2]

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

local expired = redis.call("ZRANGEBYSCORE", workers, "-inf", now)
if #expired > 0 then
	redis.call("ZREM", workers, unpack(expired))
	redis.call("HDEL", worker_info, unpack(expired))
end

local page = redis.call("ZRANGE", workers, 0, -1, "WITHSCORES")
local out = {}

for i = 1, #page, 2 do
	local id = page[i]
	local expires_at = page[i + 1]
	table.insert(out, {
		id,
		expires_at,
		redis.call("HGET", worker_info, id) or ""
	})
end

return out
