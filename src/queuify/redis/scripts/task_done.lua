-- Operation: task_done

-- KEYS[1]: unfinished_tasks_key
-- KEYS[2]: join_channel_key
-- ARGV[1]: no_remaining_tasks_msg
-- ARGV[2]: task_done_too_many_times_msg

local remaining = redis.call('GET', KEYS[1])

-- Counter should not be non-positive
if tonumber(remaining) <= 0 then
    return redis.error_reply(ARGV[2])
end

-- Decrement the unfinished tasks counter
local remaining = redis.call('DECR', KEYS[1])

if remaining == 0 then
    -- All tasks are done, publish a message to unblock join()
    redis.call('PUBLISH', KEYS[2], ARGV[1])
end

return remaining