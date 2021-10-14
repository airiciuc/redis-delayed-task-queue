local lockKey = KEYS[1]

local random = math.random
local template ='xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'
local identifier = string.gsub(template, '[xy]', function (c)
    local v = (c == 'x') and random(0, 0xf) or random(8, 0xb)
    return string.format('%x', v)
end)

while 1 do
    if redis.call('setnx', lockKey, identifier) then
        return identifier
    end
end