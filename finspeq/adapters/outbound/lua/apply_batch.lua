-- Apply one batch of events to card windows and store the partition position, atomically.
-- Semantics match MemoryWindowStore.apply (WindowWriter contract).
--
-- KEYS[1]  position key of the partition
-- ARGV[1]  key prefix
-- ARGV[2]  seconds kept behind each card's newest event time (window + allowed lateness)
-- ARGV[3]  number of events n
-- ARGV[4..] n groups of 6 values: offset, card, tx_id, time, cents, merchant
--
-- Returns {applied, skipped, last offset or -1, behind}; behind counts stored events earlier
-- than their card's newest event time before them.
-- Errors: "INVALID ..." for a malformed argument list, "GAP ..." for an offset gap.
-- Every check runs before the first write.

local FIELDS = 6
local prefix = ARGV[1]
local keep = tonumber(ARGV[2])
local count = tonumber(ARGV[3])
if keep == nil or count == nil or #ARGV ~= 3 + count * FIELDS then
  return redis.error_reply('INVALID argument layout')
end

local events = {}
for index = 0, count - 1 do
  local base = 3 + index * FIELDS
  local event = {
    offset = tonumber(ARGV[base + 1]),
    card = ARGV[base + 2],
    tx = ARGV[base + 3],
    time = tonumber(ARGV[base + 4]),
    time_text = ARGV[base + 4],
    cents_text = ARGV[base + 5],
    merchant = ARGV[base + 6],
  }
  if event.offset == nil or event.time == nil or tonumber(event.cents_text) == nil then
    return redis.error_reply('INVALID event ' .. index)
  end
  events[#events + 1] = event
end

-- GET returns false, not nil, for a missing key.
local stored = nil
local stored_raw = redis.call('GET', KEYS[1])
if stored_raw then
  stored = tonumber(stored_raw)
end

local pending = {}
local expected = nil
if stored ~= nil then
  expected = stored + 1
end
for _, event in ipairs(events) do
  if stored == nil or event.offset > stored then
    if expected ~= nil and event.offset ~= expected then
      return redis.error_reply('GAP expected offset ' .. expected .. ', got ' .. event.offset)
    end
    expected = event.offset + 1
    pending[#pending + 1] = event
  end
end
if #pending == 0 then
  return {0, count, stored or -1, 0}
end

local function card_key(card, suffix)
  return prefix .. 'card:{' .. card .. '}:' .. suffix
end

local applied = 0
local behind = 0
local newest = {}
local touched = {}
for _, event in ipairs(pending) do
  local current = newest[event.card]
  if current == nil then
    touched[#touched + 1] = event.card
    current = tonumber(redis.call('GET', card_key(event.card, 'newest'))) or event.time
  end
  local is_behind = event.time < current
  if event.time > current then
    current = event.time
  end
  newest[event.card] = current
  local value = event.time_text .. '|' .. event.cents_text .. '|' .. event.merchant
  if redis.call('HSETNX', card_key(event.card, 'tx'), event.tx, value) == 1 then
    redis.call('ZADD', card_key(event.card, 'ev'), event.time_text, event.tx)
    applied = applied + 1
    if is_behind then
      behind = behind + 1
    end
  end
end

for _, card in ipairs(touched) do
  local latest = newest[card]
  redis.call('SET', card_key(card, 'newest'), string.format('%d', latest))
  local boundary = '(' .. string.format('%d', latest - keep)
  local old = redis.call('ZRANGEBYSCORE', card_key(card, 'ev'), '-inf', boundary)
  if #old > 0 then
    redis.call('HDEL', card_key(card, 'tx'), unpack(old))
    redis.call('ZREMRANGEBYSCORE', card_key(card, 'ev'), '-inf', boundary)
  end
end

local last = pending[#pending].offset
redis.call('SET', KEYS[1], string.format('%d', last))
return {applied, count - applied, last, behind}
