local tenantId = KEYS[1]
local taskType = KEYS[2]
local taskName = KEYS[3]

local taskKey = 'jiveTask:' .. tenantId .. ':' .. taskType .. ':' .. taskName
local lockedTaskKey = 'jiveLockedTaskQueue:' .. tenantId

return redis.call('zscore', lockedTaskKey, taskKey)
