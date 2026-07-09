local ready = KEYS[1]
local inflight = KEYS[2]
local dlq = KEYS[3]
local done = KEYS[4]

local now_parts = redis.call("TIME")
local now = now_parts[1] * 1000 + math.floor(now_parts[2] / 1000)

local function oldest_age_ms(key)
	local item = redis.call("ZRANGE", key, 0, 0, "WITHSCORES")
	if #item == 0 then
		return 0
	end
	local age = now - tonumber(item[2])
	if age < 0 then
		return 0
	end
	return age
end

return {
	redis.call("ZCARD", ready),
	redis.call("ZCARD", inflight),
	redis.call("ZCARD", dlq),
	redis.call("ZCARD", done),
	oldest_age_ms(ready),
	oldest_age_ms(dlq)
}
