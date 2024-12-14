-- Operation: put_nowait

-- KEYS[1]: queue_key
-- KEYS[2]: unfinished_tasks_key
-- ARGV[1]: item
-- ARGV[2]: maxsize

local maxsize = tonumber(ARGV[2])

if maxsize <= 0 then
    -- Unlimited queue, proceed to push the item
    redis.call('LPUSH', KEYS[1], ARGV[1])
    -- Increment the unfinished tasks counter
    redis.call('INCR', KEYS[2])
    return 1  -- Success
else
    local queue_length = redis.call('LLEN', KEYS[1])
    if queue_length >= maxsize then
        return 0  -- Queue is full
    else
        -- Space is available, proceed to push the item
        redis.call('LPUSH', KEYS[1], ARGV[1])
        -- Increment the unfinished tasks counter
        redis.call('INCR', KEYS[2])
        return 1  -- Success
    end
end