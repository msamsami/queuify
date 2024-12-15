-- Operation: get_nowait

-- KEYS[1]: queue_key
-- KEYS[2]: semaphore_key
-- ARGV[1]: maxsize
-- ARGV[2]: semaphore_token
-- ARGV[3]: queue_empty_msg

local item = redis.call("RPOP", KEYS[1])
if not item then
    return redis.error_reply(ARGV[3])
end

if tonumber(ARGV[1]) > 0 then
    redis.call("LPUSH", KEYS[2], ARGV[2])
end

return item
