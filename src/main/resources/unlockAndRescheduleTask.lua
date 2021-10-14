local tenantId = KEYS[1]
local taskType = KEYS[2]
local taskName = KEYS[3]

local nextExecution = ARGV[1]

local taskKey = 'jiveTask:' .. tenantId .. ':' .. taskType .. ':' .. taskName
local lockedTaskKey = 'jiveLockedTaskQueue:' .. tenantId
local unlockedTaskKey = 'jiveUnlockedTaskQueue:' .. tenantId

redis.call('zrem', lockedTaskKey, taskKey)
redis.call('zadd', unlockedTaskKey, nextExecution, taskKey)
local time = redis.call('time')
redis.call('hset', taskKey, 'lockTime', time[1] .. math.floor(time[2] / 1000))

return redis.call('hgetall', taskKey)
