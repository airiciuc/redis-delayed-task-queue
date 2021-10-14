local tenantId = KEYS[1]
local taskType = KEYS[2]
local taskName = KEYS[3]

local taskKey = 'jiveTask:' .. tenantId .. ':' .. taskType .. ':' .. taskName
local lockedTaskKey = 'jiveLockedTaskQueue:' .. tenantId
local unlockedTaskKey = 'jiveUnlockedTaskQueue:' .. tenantId

local score = redis.call('zscore', lockedTaskKey, taskKey)
redis.call('zrem', lockedTaskKey, taskKey)
redis.call('zadd', unlockedTaskKey, score, taskKey)
redis.call('hdel', taskKey, 'lockNode', 'lockTime')

return redis.call('hgetall', taskKey)