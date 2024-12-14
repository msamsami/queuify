-- Operation: put

-- KEYS[1]: queue_key
-- KEYS[2]: unfinished_tasks_key
-- ARGV[1]: item

-- Atomically push the item onto the queue
redis.call('LPUSH', KEYS[1], ARGV[1])

-- Increment the unfinished tasks counter
redis.call('INCR', KEYS[2])

return 1  -- Success