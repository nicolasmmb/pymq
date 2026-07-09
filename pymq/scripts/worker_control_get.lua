local worker_controls = KEYS[1]

local worker_id = ARGV[1]
local all_target = ARGV[2]

return {
	redis.call("HGET", worker_controls, all_target) or "",
	redis.call("HGET", worker_controls, worker_id) or ""
}
