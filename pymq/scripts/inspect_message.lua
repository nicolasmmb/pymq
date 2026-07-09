local payloads = KEYS[1]
local headers = KEYS[2]
local states = KEYS[3]
local attempts = KEYS[4]
local leases = KEYS[5]
local errors = KEYS[6]
local dlq = KEYS[7]
local done = KEYS[8]

local id = ARGV[1]

local payload = redis.call("HGET", payloads, id)
local header_json = redis.call("HGET", headers, id)
local state = redis.call("HGET", states, id)
local attempt = redis.call("HGET", attempts, id)
local lease = redis.call("HGET", leases, id)
local last_error = redis.call("HGET", errors, id)
local dlq_score = redis.call("ZSCORE", dlq, id)
local done_score = redis.call("ZSCORE", done, id)

if state == false and done_score ~= false then
	state = "done"
end

if state == false and payload == false and dlq_score == false and done_score == false then
	return {"missing", id}
end

return {
	"ok",
	id,
	payload or "",
	header_json or "",
	state or "",
	attempt or "0",
	lease or "0",
	last_error or "",
	dlq_score or "",
	done_score or ""
}
