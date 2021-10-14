local tenantId = KEYS[1]
local taskType = KEYS[2]
local taskName = KEYS[3]

local taskDetails = ARGV[1]
local taskDigest = ARGV[2]
local score = ARGV[3]

local taskKey = 'jiveTask:' .. tenantId .. ':' .. taskType .. ':' .. taskName
local existingTask = redis.call('hgetall', taskKey)
if next(existingTask) ~= nil then
    return existingTask
else
    local unlockedTaskKey = 'jiveUnlockedTaskQueue:' .. tenantId
    redis.call('hset', taskKey, 'taskDetails', taskDetails, 'taskDigest', taskDigest)
    redis.call('zadd', unlockedTaskKey, score, taskKey)
    return {'taskDetails', taskDetails, 'taskDigest', taskDigest}
end