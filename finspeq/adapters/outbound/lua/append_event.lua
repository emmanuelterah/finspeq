-- Store one event in its card's window without touching any partition position.
-- Semantics match MemoryWindowStore.append (WindowAppender contract).
--
-- KEYS[1]  card events sorted set
-- KEYS[2]  card transactions hash
-- KEYS[3]  card newest event time
-- ARGV[1]  seconds kept behind the card's newest event time (window + allowed lateness)
-- ARGV[2..5]  tx_id, time, cents, merchant
--
-- Returns 1 if the entry was stored, 0 if the card already held the tx_id.
-- Errors: "INVALID ..." for a malformed argument list. Every check runs before the first write.

local keep = tonumber(ARGV[1])
local tx = ARGV[2]
local time = tonumber(ARGV[3])
if keep == nil or time == nil or tonumber(ARGV[4]) == nil or #ARGV ~= 5 then
  return redis.error_reply('INVALID argument layout')
end

local newest = tonumber(redis.call('GET', KEYS[3])) or time
if time > newest then
  newest = time
end
local value = ARGV[3] .. '|' .. ARGV[4] .. '|' .. ARGV[5]
local stored = redis.call('HSETNX', KEYS[2], tx, value)
if stored == 1 then
  redis.call('ZADD', KEYS[1], ARGV[3], tx)
end
redis.call('SET', KEYS[3], string.format('%d', newest))
local boundary = '(' .. string.format('%d', newest - keep)
local old = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', boundary)
if #old > 0 then
  redis.call('HDEL', KEYS[2], unpack(old))
  redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', boundary)
end
return stored
