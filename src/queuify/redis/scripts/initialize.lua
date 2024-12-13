-- Operation: initialize

-- KEYS[1]: queue_key
-- KEYS[2]: semaphore_key
-- ARGV[1]: maxsize
-- ARGV[2]: semaphore_token

-- Define the keys and arguments
local key = KEYS[1]
local semaphore_key = KEYS[2]
local maxsize = tonumber(ARGV[1])
local semaphore_token = ARGV[2]

-- Get the current sizes of the queues
local queue_size = redis.call('LLEN', key)
local semaphore_queue_size = redis.call('LLEN', semaphore_key)

-- Calculate the number of missing tokens
local count_missing_tokens = math.max(0, maxsize - queue_size) - semaphore_queue_size

-- Add or remove tokens as necessary
if count_missing_tokens > 0 then
    for i = 1, count_missing_tokens do
        redis.call('RPUSH', semaphore_key, semaphore_token)
    end
elseif count_missing_tokens < 0 then
    for i = 1, math.abs(count_missing_tokens) do
        redis.call('LPOP', semaphore_key)
    end
end
