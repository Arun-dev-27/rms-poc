/**
 * Ticket booking (movie-ticket style) per city zone, all in Redis Lua scripts.
 * Redis runs one script at a time, start to finish, so every decision below ("duplicate? enough
 * tickets? whose turn in the waiting line?") and the change it leads to happen in ONE atomic step,
 * on any number of API instances.
 *
 * Lifecycle of a request:
 *   BOOK  -> FROZEN   tickets free and nobody waiting: N tickets frozen (held) for this request
 *         -> WAITING  not enough free tickets (or people already waiting): joins the FIFO waiting line
 *   FROZEN -> BOOKED    payment done (confirm)
 *          -> RELEASED  user cancelled / admin unfroze      } tickets go back, then the waiting line
 *          -> EXPIRED   freeze time ran out before payment  } is served strictly in request order
 *   WAITING -> FROZEN   moved up from the line when freed tickets cover its quantity
 *           -> CANCELLED left the line
 *   BOOKED -> BOOKING_CANCELLED  booked tickets returned (also served to the line)
 *
 * Duplicates: a request id is processed once (sent again = DUPLICATE, original answer returned), and a
 * user can have only one open (FROZEN / WAITING) request per zone.
 *
 * KEYS (every script) = [zone, reqs, wait, holds, active, events, seq, zoneReqs]
 *   zone     HASH  tb:zone:<id>     city, name, show, total, available, frozen, booked, holdMs, requests, duplicates, n_<STATUS>
 *   reqs     HASH  tb:reqs          request id -> JSON (all zones)
 *   wait     LIST  tb:wait:<id>     waiting line, head = next to be served
 *   holds    ZSET  tb:holds:<id>    request id -> freeze expiry (ms), user freezes only
 *   active   HASH  tb:active:<id>   user (or admin:<request id>) -> open request id
 *   events   STREAM tb:ev:<id>      report timeline
 *   seq      STRING tb:seq          global request order
 *   zoneReqs ZSET  tb:zreqs:<id>    seq -> request id
 * ARGV[1] = zone id (always)
 */
const PRELUDE = `
local ZONE, REQS, WAIT, HOLDS, ACTIVE, EV, SEQ, ZREQS = KEYS[1], KEYS[2], KEYS[3], KEYS[4], KEYS[5], KEYS[6], KEYS[7], KEYS[8]
local ZID = ARGV[1]
local T = redis.call('TIME')
local NOW = tonumber(T[1]) * 1000 + math.floor(tonumber(T[2]) / 1000)
local function zget(f) return tonumber(redis.call('HGET', ZONE, f) or '0') end
local function getReq(rid) local s = redis.call('HGET', REQS, rid); if s then return cjson.decode(s) end return nil end
local function putReq(r) redis.call('HSET', REQS, r.rid, cjson.encode(r)) end
local function setStatus(r, s)
  if r.status then redis.call('HINCRBY', ZONE, 'n_' .. r.status, -1) end
  r.status = s
  redis.call('HINCRBY', ZONE, 'n_' .. s, 1)
end
local function ev(kind, r, note)
  redis.call('XADD', EV, 'MAXLEN', '~', '200000', '*',
    'ts', tostring(NOW), 'type', kind, 'rid', r.rid or '', 'seq', tostring(r.seq or ''), 'user', r.user or '',
    'qty', tostring(r.qty or ''), 'available', tostring(zget('available')), 'frozen', tostring(zget('frozen')),
    'booked', tostring(zget('booked')), 'waiting', tostring(redis.call('LLEN', WAIT)), 'note', note or '')
end
local function markBooked(r)
  redis.call('HSET', ZONE, 'lastBookedRid', r.rid, 'lastBookedIts', r.user or '', 'lastBookedSeq', tostring(r.seq or ''), 'lastBookedAt', tostring(NOW))
end
local function freeze(r, cause, by)
  redis.call('HINCRBY', ZONE, 'available', -r.qty)
  -- confirm mode "direct" (registration): no hold, the seat is CONFIRMED at once, in request order.
  -- The ITS id stays registered (its open-request marker is kept), so registering again is a duplicate.
  if r.kind == 'USER' and redis.call('HGET', ZONE, 'direct') == '1' then
    redis.call('HINCRBY', ZONE, 'booked', r.qty)
    setStatus(r, 'BOOKED')
    r.frozenAt = NOW
    r.bookedAt = NOW
    r.cause = cause
    r.freedBy = by
    if cause == 'WAITLIST' then r.leftLineAt = NOW end
    putReq(r)
    markBooked(r)
    ev('BOOKED', r, cause == 'WAITLIST' and ('confirmed from the waiting line (joined at #' .. tostring(r.joinPos) .. '); seat freed by ' .. by) or 'seat free, confirmed directly')
    if zget('available') == 0 then ev('NO_TICKETS_LEFT', r, 'all seats confirmed; new requests get NO SEATS AVAILABLE') end
    return
  end
  redis.call('HINCRBY', ZONE, 'frozen', r.qty)
  setStatus(r, 'FROZEN')
  r.frozenAt = NOW
  r.cause = cause
  r.freedBy = by
  if cause == 'WAITLIST' then r.leftLineAt = NOW end
  if r.kind == 'USER' then
    r.expiresAt = NOW + zget('holdMs')
    redis.call('ZADD', HOLDS, r.expiresAt, r.rid)
  end
  putReq(r)
  local note
  if cause == 'WAITLIST' then note = 'served from the waiting line (joined at #' .. tostring(r.joinPos) .. '); tickets freed by ' .. by
  elseif r.kind == 'ADMIN' then note = 'admin freeze: ' .. (r.note or '')
  else note = 'tickets free, frozen immediately' end
  ev('FROZEN', r, note)
  if zget('available') == 0 then ev('NO_TICKETS_LEFT', r, 'all tickets frozen or booked; new requests go to the waiting line') end
end
-- Zone freeze ("everything except booked"): nobody gets tickets, held timers are paused, new requests queue.
local function zoneFrozen() return redis.call('HGET', ZONE, 'paused') == '1' end
-- Serve the waiting line strictly in request order: stop at the first request the free tickets do not cover.
-- While the zone is frozen nobody is served; the line waits for the unfreeze.
local function promote(by)
  local moved = 0
  if zoneFrozen() then return 0 end
  while true do
    local head = redis.call('LINDEX', WAIT, 0)
    if not head then break end
    local h = getReq(head)
    if not h or h.status ~= 'WAITING' then
      redis.call('LPOP', WAIT)
    else
      if zget('available') < h.qty then break end
      redis.call('LPOP', WAIT)
      freeze(h, 'WAITLIST', by)
      moved = moved + 1
    end
  end
  return moved
end
local function activeKey(r) if r.kind == 'USER' then return r.user end return 'admin:' .. r.rid end
local function closeActive(r)
  if redis.call('HGET', ACTIVE, activeKey(r)) == r.rid then redis.call('HDEL', ACTIVE, activeKey(r)) end
end
local function unfreeze(r, status, note)
  redis.call('HINCRBY', ZONE, 'frozen', -r.qty)
  redis.call('HINCRBY', ZONE, 'available', r.qty)
  redis.call('ZREM', HOLDS, r.rid)
  setStatus(r, status)
  r.endedAt = NOW
  closeActive(r)
  putReq(r)
  ev(status, r, note)
  return promote(r.rid)
end
`;

/**
 * User = Mumin_ID (ITS id). Many requests for the same ITS id can arrive at the same moment on any
 * instance; this script runs them one at a time, so exactly one becomes the ITS id's open request and
 * every other one is answered DUPLICATE.
 * Extra KEYS: [9] mumin:rec:<ITS> (must exist)  [10] mumin:status (CANCELLED ITS cannot book)
 *             [11] tb:dups:<zone> (ITS -> duplicates blocked)  [12] tb:its:<ITS> (seq -> request id, all zones)
 * ARGV = [zone, requestId, user, qty, kind USER|ADMIN, note]
 */
export const BOOK_LUA = PRELUDE + `
local MREC, MSTATUS, DUPS, BYITS = KEYS[9], KEYS[10], KEYS[11], KEYS[12]
local rid, user, qty, kind, note = ARGV[2], ARGV[3], tonumber(ARGV[4]), ARGV[5], ARGV[6]
if redis.call('EXISTS', ZONE) == 0 then return {'ERR', 'ZONE_NOT_FOUND', ''} end
local function dup(why, orig, code)
  redis.call('HINCRBY', ZONE, 'duplicates', 1)
  redis.call('HINCRBY', DUPS, user, 1)
  ev('DUPLICATE_BLOCKED', {rid = rid, user = user, qty = qty}, why)
  return {'DUPLICATE', orig and cjson.encode(orig) or '{}', code}
end
local prev = getReq(rid)
if prev then
  return dup('request id already received (#' .. tostring(prev.seq) .. ', ITS ' .. prev.user .. ', ' .. prev.status .. '); not processed again', prev, 'REQUEST_ID_ALREADY_USED')
end
local name, jamaat = '', ''
if kind == 'USER' then
  local rec = redis.call('GET', MREC)
  if not rec then return {'ERR', 'ITS_NOT_FOUND', ''} end
  if redis.call('HGET', MSTATUS, user) == 'CANCELLED' then return {'ERR', 'ITS_CANCELLED', ''} end
  local open = redis.call('HGET', ACTIVE, user)
  if open then
    local o = getReq(open)
    return dup('ITS ' .. user .. ' already has an open request ' .. open .. ' (' .. (o and o.status or '?') .. ')', o, 'ITS_HAS_OPEN_REQUEST')
  end
  local m = cjson.decode(rec)
  if type(m['Short_Name']) == 'string' then name = m['Short_Name'] end
  if type(m['Jamaat']) == 'string' then jamaat = m['Jamaat'] end
end
if qty > zget('total') then return {'ERR', 'MORE_THAN_TOTAL', ''} end
if kind == 'ADMIN' and zget('available') < qty then return {'ERR', 'NOT_ENOUGH_FREE_TICKETS', ''} end
local seq = redis.call('INCR', SEQ)
redis.call('HINCRBY', ZONE, 'requests', 1)
local r = {rid = rid, seq = seq, zone = ZID, user = user, name = name, jamaat = jamaat, qty = qty, kind = kind, note = note, requestedAt = NOW}
redis.call('ZADD', ZREQS, seq, rid)
if kind == 'USER' then redis.call('ZADD', BYITS, seq, rid) end
redis.call('HSET', ACTIVE, activeKey(r), rid)
-- admin freezes take free tickets directly; users never overtake the waiting line
-- direct mode (registration) has no waiting line: a free seat is confirmed in request order, else NO SEATS
local zf = zoneFrozen()
local direct = redis.call('HGET', ZONE, 'direct') == '1'
if kind == 'ADMIN' or (not zf and zget('available') >= qty and (direct or redis.call('LLEN', WAIT) == 0)) then
  freeze(r, 'DIRECT', '')
  return {r.status == 'BOOKED' and 'BOOKED' or 'FROZEN', cjson.encode(r), ''}
end
if direct and zf then
  -- zone frozen: answered FROZEN at once. Nothing is kept for later (no line, no order, ITS id not held),
  -- so the unfreeze has nothing to process; after the unfreeze a new request is booked if a seat is free.
  setStatus(r, 'ZONE_FROZEN')
  r.endedAt = NOW
  r.duringZoneFreeze = redis.call('HGET', ZONE, 'pauseId')
  closeActive(r)
  putReq(r)
  ev('REFUSED_ZONE_FROZEN', r, 'zone is frozen: not booked, not stored for later')
  return {'ZONE_FROZEN', cjson.encode(r), ''}
end
if direct and not zf then
  -- all seats confirmed: answered NO SEATS, not registered (the ITS id may try again if a seat is cancelled)
  setStatus(r, 'NO_SEATS')
  r.endedAt = NOW
  closeActive(r)
  putReq(r)
  ev('NO_SEATS', r, 'no seats available (' .. tostring(zget('available')) .. ' free, ' .. tostring(qty) .. ' asked); not registered')
  return {'NO_SEATS', cjson.encode(r), ''}
end
setStatus(r, zf and 'PAUSED' or 'WAITING')
r.joinPos = redis.call('RPUSH', WAIT, rid)
if zf then r.duringZoneFreeze = redis.call('HGET', ZONE, 'pauseId') end
putReq(r)
if zf then
  ev('PAUSED', r, 'zone is frozen: request FROZEN at #' .. tostring(r.joinPos) .. ' in request order; processed when the zone is unfrozen')
  return {'PAUSED', cjson.encode(r), ''}
end
ev('WAITING', r, 'not enough free tickets now (' .. tostring(zget('available')) .. ' free); joined the waiting line at #' .. tostring(r.joinPos))
return {'WAITING', cjson.encode(r), ''}`;

/** ARGV = [zone, requestId] */
export const CONFIRM_LUA = PRELUDE + `
local r = getReq(ARGV[2])
if not r or r.zone ~= ZID then return {'ERR', 'REQUEST_NOT_FOUND', ''} end
if r.status == 'BOOKED' then return {'DUPLICATE', cjson.encode(r), 'ALREADY_BOOKED'} end
if r.status ~= 'FROZEN' then return {'ERR', 'NOT_FROZEN', cjson.encode(r)} end
local zfc = zoneFrozen()
if zfc and redis.call('HGET', ZONE, 'pauseAllowPay') ~= '1' then return {'ERR', 'ZONE_FROZEN', cjson.encode(r)} end
if not zfc and r.expiresAt and NOW > r.expiresAt then
  unfreeze(r, 'EXPIRED', 'freeze time ran out before payment; tickets returned')
  return {'ERR', 'FREEZE_EXPIRED', cjson.encode(r)}
end
redis.call('HINCRBY', ZONE, 'frozen', -r.qty)
redis.call('HINCRBY', ZONE, 'booked', r.qty)
redis.call('ZREM', HOLDS, r.rid)
setStatus(r, 'BOOKED')
r.bookedAt = NOW
closeActive(r)
putReq(r)
markBooked(r)
ev('BOOKED', r, 'payment done; frozen tickets booked')
return {'BOOKED', cjson.encode(r), ''}`;

/** ARGV = [zone, requestId, reason] -> {'OK', request, movedFromLine} */
export const RELEASE_LUA = PRELUDE + `
local r = getReq(ARGV[2])
local reason = ARGV[3]
if not r or r.zone ~= ZID then return {'ERR', 'REQUEST_NOT_FOUND', ''} end
local moved = 0
if r.status == 'FROZEN' then
  moved = unfreeze(r, 'RELEASED', reason)
elseif r.status == 'WAITING' or r.status == 'PAUSED' then
  redis.call('LREM', WAIT, 1, r.rid)
  setStatus(r, 'CANCELLED')
  r.endedAt = NOW
  r.leftLineAt = NOW
  closeActive(r)
  putReq(r)
  ev('CANCELLED', r, reason)
  moved = promote(r.rid) -- the next one may fit now
elseif r.status == 'BOOKED' then
  redis.call('HINCRBY', ZONE, 'booked', -r.qty)
  redis.call('HINCRBY', ZONE, 'available', r.qty)
  setStatus(r, 'BOOKING_CANCELLED')
  r.endedAt = NOW
  closeActive(r) -- direct mode keeps the ITS registered until it cancels
  putReq(r)
  ev('BOOKING_CANCELLED', r, reason)
  moved = promote(r.rid)
else
  return {'DUPLICATE', cjson.encode(r), 'ALREADY_' .. r.status}
end
return {'OK', cjson.encode(r), tostring(moved)}`;

/** ARGV = [zone, limit] -> number of freezes expired */
export const EXPIRE_LUA = PRELUDE + `
if zoneFrozen() then return 0 end -- held timers are paused while the zone is frozen
local due = redis.call('ZRANGEBYSCORE', HOLDS, '-inf', NOW, 'LIMIT', 0, tonumber(ARGV[2]))
local n = 0
for _, rid in ipairs(due) do
  local r = getReq(rid)
  if r and r.status == 'FROZEN' then
    unfreeze(r, 'EXPIRED', 'freeze time ran out before payment; tickets returned')
    n = n + 1
  else
    redis.call('ZREM', HOLDS, rid)
  end
end
return n`;

/** ARGV = [zone, city, name, show, total, holdMs] */
export const UPSERT_ZONE_LUA = PRELUDE + `
local city, name, show, total, holdMs = ARGV[2], ARGV[3], ARGV[4], tonumber(ARGV[5]), tonumber(ARGV[6])
if redis.call('EXISTS', ZONE) == 1 then
  local used = zget('frozen') + zget('booked')
  if total < used then return {'ERR', 'TOTAL_BELOW_USED', tostring(used)} end
  redis.call('HSET', ZONE, 'city', city, 'name', name, 'show', show, 'total', total, 'holdMs', holdMs, 'available', total - used)
  ev('ZONE_UPDATED', {}, 'total ' .. tostring(total) .. ', freeze time ' .. tostring(holdMs / 1000) .. ' s')
  return {'UPDATED', tostring(promote('ZONE_UPDATED')), ''}
end
redis.call('HSET', ZONE, 'id', ZID, 'city', city, 'name', name, 'show', show, 'total', total, 'holdMs', holdMs,
  'available', total, 'frozen', 0, 'booked', 0, 'requests', 0, 'duplicates', 0, 'createdAt', NOW)
ev('ZONE_CREATED', {}, tostring(total) .. ' tickets, freeze time ' .. tostring(holdMs / 1000) .. ' s')
return {'CREATED', '0', ''}`;

/**
 * Edit a zone in place (id never changes). Any field may change; '' / 0 = keep.
 * Total can go up (new tickets go to the waiting line first, in order) or down (only free tickets can be removed).
 * ARGV = [zone, city, name, show, total, holdMs]
 */
export const UPDATE_ZONE_LUA = PRELUDE + `
if redis.call('EXISTS', ZONE) == 0 then return {'ERR', 'ZONE_NOT_FOUND', ''} end
local changes = {}
local function setText(f, v, lbl)
  local old = redis.call('HGET', ZONE, f) or ''
  if v ~= '' and v ~= old then redis.call('HSET', ZONE, f, v); table.insert(changes, lbl .. ' "' .. old .. '" -> "' .. v .. '"') end
end
setText('city', ARGV[2], 'city'); setText('name', ARGV[3], 'zone'); setText('show', ARGV[4], 'show')
local total, holdMs = tonumber(ARGV[5]), tonumber(ARGV[6])
local oldTotal = zget('total')
if total > 0 and total ~= oldTotal then
  local used = zget('frozen') + zget('booked')
  if total < used then return {'ERR', 'TOTAL_BELOW_USED', tostring(used)} end
  local oldAvail = zget('available')
  redis.call('HSET', ZONE, 'total', total, 'available', total - used)
  local seatTxt = 'seats ' .. tostring(oldTotal) .. ' -> ' .. tostring(total) .. ', available ' .. tostring(oldAvail) .. ' -> ' .. tostring(total - used) .. ', booked ' .. tostring(zget('booked'))
  if total > oldTotal then table.insert(changes, 'seats increased +' .. tostring(total - oldTotal) .. ' (' .. seatTxt .. ')')
  else table.insert(changes, 'seats reduced -' .. tostring(oldTotal - total) .. ' (' .. seatTxt .. ')') end
end
if holdMs > 0 and holdMs ~= zget('holdMs') then
  table.insert(changes, 'freeze time ' .. tostring(zget('holdMs') / 1000) .. ' s -> ' .. tostring(holdMs / 1000) .. ' s (new freezes)')
  redis.call('HSET', ZONE, 'holdMs', holdMs)
end
if #changes == 0 then return {'UNCHANGED', '0', ''} end
ev('ZONE_UPDATED', {}, table.concat(changes, '; '))
return {'UPDATED', tostring(promote('ZONE_UPDATED')), table.concat(changes, '; ')}`;

/**
 * Edit one request (atomic, FIFO kept):
 *   FROZEN   tickets less -> freed tickets go to the waiting line; more -> only from free tickets and only
 *            when nobody is waiting (a user may not overtake the line; an admin freeze may). extendMs: later expiry.
 *   WAITING  tickets any 1..total, keeps its place; if it now fits it is served.
 *   BOOKED   tickets less only (partial cancel) -> freed tickets go to the waiting line.
 *   ADMIN    note = new reason.
 * ARGV = [zone, requestId, newQty (0 = keep), extendMs (0 = none), note ('' = keep)]
 */
export const EDIT_REQUEST_LUA = PRELUDE + `
local r = getReq(ARGV[2])
if not r or r.zone ~= ZID then return {'ERR', 'REQUEST_NOT_FOUND', ''} end
local newQty, extendMs, note = tonumber(ARGV[3]), tonumber(ARGV[4]), ARGV[5]
local changes = {}
local oldQty = r.qty
if newQty > 0 and newQty ~= oldQty then
  local d = newQty - oldQty
  if r.status == 'FROZEN' then
    if d > 0 then
      if r.kind == 'USER' and zoneFrozen() then return {'ERR', 'ZONE_FROZEN', ''} end
      if r.kind == 'USER' and redis.call('LLEN', WAIT) > 0 then return {'ERR', 'OTHERS_WAITING', ''} end
      if zget('available') < d then return {'ERR', 'NOT_ENOUGH_FREE_TICKETS', ''} end
    end
    redis.call('HINCRBY', ZONE, 'available', -d)
    redis.call('HINCRBY', ZONE, 'frozen', d)
  elseif r.status == 'WAITING' or r.status == 'PAUSED' then
    if newQty > zget('total') then return {'ERR', 'MORE_THAN_TOTAL', ''} end
  elseif r.status == 'BOOKED' then
    if d > 0 then return {'ERR', 'BOOKED_ONLY_LESS', ''} end
    redis.call('HINCRBY', ZONE, 'available', -d)
    redis.call('HINCRBY', ZONE, 'booked', d)
  else
    return {'ERR', 'NOT_EDITABLE', r.status}
  end
  r.qty = newQty
  table.insert(changes, 'tickets ' .. tostring(oldQty) .. ' -> ' .. tostring(newQty))
end
if extendMs > 0 then
  if r.status ~= 'FROZEN' or not r.expiresAt then return {'ERR', 'NOT_EXTENDABLE', ''} end
  if NOW > r.expiresAt then
    unfreeze(r, 'EXPIRED', 'freeze time ran out before payment; tickets returned')
    return {'ERR', 'FREEZE_EXPIRED', ''}
  end
  r.expiresAt = r.expiresAt + extendMs
  redis.call('ZADD', HOLDS, r.expiresAt, r.rid)
  table.insert(changes, 'freeze extended by ' .. tostring(extendMs / 1000) .. ' s')
end
if note ~= '' and r.kind == 'ADMIN' and note ~= r.note then
  table.insert(changes, 'reason "' .. (r.note or '') .. '" -> "' .. note .. '"')
  r.note = note
end
if #changes == 0 then return {'UNCHANGED', cjson.encode(r), '0'} end
r.edits = (r.edits or 0) + 1
r.editedAt = NOW
r.lastEdit = table.concat(changes, '; ')
putReq(r)
ev('EDITED', r, r.lastEdit)
local moved = promote(r.rid)
if moved > 0 then r = getReq(r.rid) end
return {'OK', cjson.encode(r), tostring(moved)}`;

/**
 * Redis key log: every redis.call(...) in a script is replaced by R(...), which runs the command and records
 * {command, key, args, result}. The script's answer becomes { answer, json(log) }. Still one atomic script.
 */
const TRACE_HEAD = `
local __LOG = {}
local __N = 0 -- ops run; only the first 300 are listed (a big unfreeze can run tens of thousands)
local __rc = redis.call
local function __short(v)
  local t = type(v)
  if v == nil or v == false then return 'nil' end
  if t == 'number' then return tostring(v) end
  if t == 'string' then
    if #v > 80 then return '"' .. string.sub(v, 1, 77) .. '..." (' .. tostring(#v) .. ' bytes)' end
    return '"' .. v .. '"'
  end
  if t == 'table' then
    if v.ok then return v.ok end
    if v.err then return 'ERR ' .. v.err end
    return '[' .. tostring(#v) .. ' items]'
  end
  return t
end
local function R(cmd, ...)
  local res = __rc(cmd, ...)
  __N = __N + 1
  if __N > 300 then return res end
  local a = {...}
  local args = {}
  for i = 2, math.min(#a, 8) do args[#args + 1] = __short(a[i]) end
  if #a > 8 then args[#args + 1] = '+' .. tostring(#a - 8) .. ' more' end
  local key = a[1] and tostring(a[1]) or ''
  local out = __short(res)
  -- personal data stays out of the log: a Mumin_Mast record is logged by key and size only
  if string.sub(key, 1, 10) == 'mumin:rec:' and type(res) == 'string' then out = '(Mumin_Mast record found, ' .. tostring(#res) .. ' bytes)' end
  __LOG[#__LOG + 1] = {cmd, key, table.concat(args, ' '), out}
  return res
end
local function __main()
`;

export function withRedisLog(src: string): string {
  return `${TRACE_HEAD}${src.split('redis.call(').join('R(')}
end
local __res = __main()
if __N > 300 then __LOG[#__LOG + 1] = {'...', '', tostring(__N - 300) .. ' more Redis ops not listed', ''} end
return {__res, cjson.encode(__LOG)}`;
}


/**
 * ZONE FREEZE: freeze everything except booked tickets, while booking is going on.
 *   booked stays booked · free tickets: nobody can take them · held (unpaid) requests keep their tickets but
 *   cannot pay (unless allowPay) and their timer is paused · the waiting line is not served · every new
 *   request joins the line in request order (duplicates still blocked).
 * KEYS = 8 common + [9] tb:pauses:<zone> (history, JSON per freeze)   ARGV = [zone, pauseId, reason, allowPay '1'/'0']
 */
export const ZONE_FREEZE_LUA = PRELUDE + `
local PAUSES = KEYS[9]
if redis.call('EXISTS', ZONE) == 0 then return {'ERR', 'ZONE_NOT_FOUND', ''} end
if zoneFrozen() then return {'ERR', 'ALREADY_FROZEN', redis.call('HGET', ZONE, 'pauseId') or ''} end
local id, reason, allowPay = ARGV[2], ARGV[3], ARGV[4]
local snap = { available = zget('available'), frozen = zget('frozen'), booked = zget('booked'), waiting = redis.call('LLEN', WAIT),
  heldRequests = redis.call('ZCARD', HOLDS) }
local lb = redis.call('HMGET', ZONE, 'lastBookedRid', 'lastBookedIts', 'lastBookedSeq', 'lastBookedAt')
if lb[1] then snap.lastBooked = { rid = lb[1], its = lb[2], seq = tonumber(lb[3]), at = tonumber(lb[4]) } end
redis.call('HSET', ZONE, 'paused', '1', 'pausedAt', NOW, 'pauseId', id, 'pauseReason', reason, 'pauseAllowPay', allowPay)
local p = { id = id, at = NOW, reason = reason, allowPay = allowPay == '1', atFreeze = snap }
redis.call('RPUSH', PAUSES, cjson.encode(p))
ev('ZONE_FROZEN', {}, 'last booked before the freeze: ' .. (snap.lastBooked and ('ITS ' .. snap.lastBooked.its .. ', request ' .. snap.lastBooked.rid .. ' (order #' .. tostring(snap.lastBooked.seq) .. ')') or 'none') .. '; '
  .. tostring(snap.booked) .. ' booked; everything except booked frozen: ' .. tostring(snap.available) .. ' free tickets blocked, ' .. tostring(snap.heldRequests)
  .. ' held requests paused (' .. (allowPay == '1' and 'may still pay' or 'cannot pay, timer paused') .. '), ' .. tostring(snap.waiting) .. ' waiting; new requests join the line. Reason: ' .. reason)
return {'OK', cjson.encode(p), ''}`;

/**
 * ZONE UNFREEZE: held timers resume with the time they had left, then the waiting line is served in request order.
 * KEYS = 8 common + [9] tb:pauses:<zone>   ARGV = [zone]
 */
export const ZONE_UNFREEZE_LUA = PRELUDE + `
local PAUSES = KEYS[9]
if not zoneFrozen() then return {'ERR', 'ZONE_NOT_FROZEN', ''} end
local id = redis.call('HGET', ZONE, 'pauseId')
local dur = NOW - tonumber(redis.call('HGET', ZONE, 'pausedAt'))
-- resume every held timer: it gets back exactly the time the freeze took away
local holds = redis.call('ZRANGE', HOLDS, 0, -1, 'WITHSCORES')
for i = 1, #holds, 2 do
  local r = getReq(holds[i])
  local exp = tonumber(holds[i + 1]) + dur
  redis.call('ZADD', HOLDS, exp, holds[i])
  if r then r.expiresAt = exp; putReq(r) end
end
redis.call('HSET', ZONE, 'paused', '0')
redis.call('HDEL', ZONE, 'pausedAt', 'pauseId', 'pauseReason', 'pauseAllowPay')
local paused, dropped = 0, 0
if redis.call('HGET', ZONE, 'direct') == '1' then
  -- direct mode: requests that came while frozen are DROPPED (not processed) and their ITS ids freed;
  -- only requests that arrive after the unfreeze are processed, in request order
  local keep = {}
  for _, rid in ipairs(redis.call('LRANGE', WAIT, 0, -1)) do
    local r = getReq(rid)
    if r and r.status == 'PAUSED' then
      setStatus(r, 'DROPPED')
      r.endedAt = NOW
      r.leftLineAt = NOW
      closeActive(r)
      putReq(r)
      dropped = dropped + 1
    elseif r and r.status == 'WAITING' then keep[#keep + 1] = rid end
  end
  redis.call('DEL', WAIT)
  for i = 1, #keep, 5000 do redis.call('RPUSH', WAIT, unpack(keep, i, math.min(i + 4999, #keep))) end
else
  -- hold mode: requests that came while frozen (PAUSED) are processed now, in request order: they already
  -- hold their place in the list, so they become WAITING and the line is served from the top
  for _, rid in ipairs(redis.call('LRANGE', WAIT, 0, -1)) do
    local r = getReq(rid)
    if r and r.status == 'PAUSED' then setStatus(r, 'WAITING'); putReq(r); paused = paused + 1 end
  end
end
local moved = promote('zone-unfreeze:' .. id)
local last = redis.call('LINDEX', PAUSES, -1)
local p = last and cjson.decode(last) or { id = id }
p.endedAt = NOW
p.durationMs = dur
p.movedFromLine = moved
p.timersResumed = #holds / 2
p.frozenRequests = paused
p.droppedRequests = dropped
p.atUnfreeze = { available = zget('available'), frozen = zget('frozen'), booked = zget('booked'), waiting = redis.call('LLEN', WAIT) }
if last then redis.call('LSET', PAUSES, -1, cjson.encode(p)) end
ev('ZONE_UNFROZEN', {}, 'zone unfrozen after ' .. tostring(math.floor(dur / 1000)) .. ' s: ' .. (dropped > 0 and (tostring(dropped) .. ' frozen requests dropped (not processed), ') or '') .. tostring(paused) .. ' frozen requests processed in request order, ' .. tostring(#holds / 2) .. ' held timers resumed, '
  .. tostring(moved) .. ' waiting requests got tickets in request order')
return {'OK', cjson.encode(p), tostring(moved)}`;
