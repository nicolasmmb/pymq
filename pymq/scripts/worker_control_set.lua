local worker_controls = KEYS[1]

local target = ARGV[1]
local control_json = ARGV[2]

redis.call("HSET", worker_controls, target, control_json)

return {"ok", target}
