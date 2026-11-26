-- Read one card's window entries before an order key, as one consistent snapshot.
--
-- KEYS[1]  card events sorted set (member tx_id, score event time)
-- KEYS[2]  card transactions hash (field tx_id, value "time|cents|merchant")
-- ARGV[1]  lowest event time included
-- ARGV[2]  event time of the upper bound
-- ARGV[3]  tx_id of the upper bound; entries at that time are included only below it
--
-- Returns a flat list {tx_id, value, tx_id, value, ...} in (time, tx_id) order.

local until_time = tonumber(ARGV[2])
local rows = redis.call('ZRANGEBYSCORE', KEYS[1], ARGV[1], ARGV[2], 'WITHSCORES')
local ids = {}
for index = 1, #rows, 2 do
  local tx = rows[index]
  local time = tonumber(rows[index + 1])
  if time < until_time or tx < ARGV[3] then
    ids[#ids + 1] = tx
  end
end
if #ids == 0 then
  return {}
end
local values = redis.call('HMGET', KEYS[2], unpack(ids))
local result = {}
for index, tx in ipairs(ids) do
  result[#result + 1] = tx
  result[#result + 1] = values[index]
end
return result
