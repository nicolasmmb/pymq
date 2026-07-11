local done = KEYS[1]
local payloads = KEYS[2]
local headers = KEYS[3]
local states = KEYS[4]
local attempts = KEYS[5]
local leases = KEYS[6]
local errors = KEYS[7]

local offset = tonumber(ARGV[1])
local count = tonumber(ARGV[2])
local page = redis.call("ZRANGE", done, offset, offset + count - 1, "WITHSCORES")
local out = {}

for i = 1, #page, 2 do
	local id = page[i]
	table.insert(out, {
		id,
		redis.call("HGET", payloads, id) or "",
		redis.call("HGET", headers, id) or "",
		redis.call("HGET", states, id) or "done",
		redis.call("HGET", attempts, id) or "0",
		redis.call("HGET", leases, id) or "0",
		redis.call("HGET", errors, id) or "",
		"",
		page[i + 1],
	})
end

return out
