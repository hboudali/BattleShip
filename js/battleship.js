/**
 * Battleship – online two-player game
 *
 * Players connect through a Supabase Realtime channel named after the game
 * code. No database table is used: presence tells each side who is online and
 * broadcast messages carry shots and results.
 *
 * Each device only knows its own fleet. The attacker sends a shot, the
 * defender resolves it and replies with the result, so ship positions are
 * never sent until the game is over.
 *
 * Messages may be missed (phone locked, flaky network), so every device keeps
 * its full game state in localStorage and the two sides exchange a "sync"
 * message whenever one of them is behind.
 */

import { SUPABASE_CONFIG } from './config.js';
import {
  SIZE, FLEET, generateCode, normalizeCode, shipCells, canPlace, randomFleet, shipAt, resolveShot
} from './battleship-logic.js';

const supabase = window.supabase.createClient(SUPABASE_CONFIG.URL, SUPABASE_CONFIG.ANON_KEY);

const STORAGE_PREFIX = 'battleship:';
const STORAGE_TTL_MS = 24 * 60 * 60 * 1000;
const JOIN_TIMEOUT_MS = 5000;
const HEARTBEAT_MS = 4000;
const COLUMNS = 'ABCDEFGHIJ';

const $ = id => document.getElementById(id);

let state = null;        // Persisted game state, null on the home screen
let channel = null;
let connected = false;
let lastError = '';     // Shown to players so connection problems can be diagnosed
let oppOnline = false;
let heartbeat = null;
let selectedShipId = null;
let vertical = false;
let toastTimer = null;

const boards = {};

/* ---------- Storage ---------- */

function save() {
  if (!state) return;
  state.updatedAt = Date.now();
  try {
    localStorage.setItem(STORAGE_PREFIX + state.code, JSON.stringify(state));
  } catch { /* storage unavailable: game still works, just can't resume */ }
}

function load(code) {
  try {
    const raw = localStorage.getItem(STORAGE_PREFIX + code);
    return raw ? JSON.parse(raw) : null;
  } catch {
    return null;
  }
}

function forget(code) {
  try { localStorage.removeItem(STORAGE_PREFIX + code); } catch { /* ignore */ }
}

function pruneStorage() {
  try {
    Object.keys(localStorage)
      .filter(key => key.startsWith(STORAGE_PREFIX))
      .forEach(key => {
        const saved = JSON.parse(localStorage.getItem(key) || '{}');
        if (!saved.updatedAt || Date.now() - saved.updatedAt > STORAGE_TTL_MS) localStorage.removeItem(key);
      });
  } catch { /* ignore */ }
}

function newState(code, role) {
  return {
    code,
    role,
    clientId: crypto.randomUUID ? crypto.randomUUID() : generateCode(24),
    oppKey: null,
    round: 0,
    phase: 'lobby',
    ships: [],
    ready: false,
    oppReady: false,
    fired: [],       // My shots: { row, col, result? }
    received: [],    // Their shots with my resolution: { row, col, hit, sunk, gameOver }
    oppShips: null,  // Revealed at game over
    rematch: false,
    oppRematch: false,
    oppLeft: false
  };
}

function setUrl(code) {
  const url = new URL(window.location.href);
  if (code) url.searchParams.set('game', code);
  else url.searchParams.delete('game');
  history.replaceState(null, '', url);
}

/* ---------- Derived state ---------- */

const sameCell = (a, b) => a.row === b.row && a.col === b.col;

function pendingShot() {
  return state.fired.find(f => !f.result);
}

function iShootFirst() {
  // Host opens the first round, then players alternate
  return (state.round % 2 === 0) === (state.role === 'host');
}

function isMyTurn() {
  if (state.phase !== 'battle' || pendingShot()) return false;
  return iShootFirst()
    ? state.fired.length === state.received.length
    : state.fired.length < state.received.length;
}

function winner() {
  if (state.fired.some(f => f.result?.gameOver)) return 'me';
  if (state.received.some(r => r.gameOver)) return 'opp';
  return null;
}

/* ---------- Networking ---------- */

function presenceOthers() {
  if (!channel) return [];
  return Object.entries(channel.presenceState())
    .filter(([key]) => key !== state?.clientId)
    .map(([key, metas]) => ({ key, ...metas[0] }));
}

function connect() {
  channel = supabase.channel(`battleship:${state.code}`, {
    config: { broadcast: { self: false }, presence: { key: state.clientId } }
  });
  channel.on('broadcast', { event: 'msg' }, ({ payload }) => handleMessage(payload));
  channel.on('presence', { event: 'sync' }, onPresenceSync);

  clearInterval(heartbeat);
  heartbeat = setInterval(() => { if (needsSync()) sendSync(); }, HEARTBEAT_MS);

  return new Promise(resolve => {
    channel.subscribe(async (status, err) => {
      if (status === 'SUBSCRIBED') {
        connected = true;
        lastError = '';
        // Runs again after every automatic reconnect, so presence is restored
        await channel.track({ role: state.role });
        sendSync();
        resolve(true);
      } else if (['CHANNEL_ERROR', 'TIMED_OUT', 'CLOSED'].includes(status)) {
        connected = false;
        lastError = err?.message ? `${status}: ${err.message}` : status;
        console.error('Realtime connection problem:', status, err);
        resolve(false);
      }
      renderBanner();
    });
  });
}

async function disconnect() {
  clearInterval(heartbeat);
  heartbeat = null;
  if (channel) {
    const old = channel;
    channel = null;
    await supabase.removeChannel(old);
  }
  connected = false;
  oppOnline = false;
}

function waitForPresence(predicate, timeoutMs) {
  return new Promise(resolve => {
    const started = Date.now();
    const check = () => {
      const others = presenceOthers();
      if (others.some(predicate) || Date.now() - started > timeoutMs) resolve(others);
      else setTimeout(check, 200);
    };
    check();
  });
}

function send(type, data = {}) {
  if (!channel || !state?.oppKey) return Promise.resolve();
  return channel.send({
    type: 'broadcast',
    event: 'msg',
    payload: { type, from: state.clientId, to: state.oppKey, round: state.round, ...data }
  });
}

function sendSync() {
  if (!state?.oppKey || state.phase === 'lobby') return;
  send('sync', {
    ready: state.ready,
    rematch: state.rematch,
    fired: state.fired.map(({ row, col }) => ({ row, col })),
    received: state.received,
    ships: state.phase === 'over' ? state.ships : null,
    // What I know about the opponent, so they can tell if I'm behind
    knowsReady: state.oppReady,
    knowsRematch: state.oppRematch,
    knowsShips: Boolean(state.oppShips),
    resolved: state.fired.filter(f => f.result).length
  });
}

function needsSync() {
  if (!state || !oppOnline) return false;
  return (state.phase === 'placing' && state.ready && !state.oppReady)
    || Boolean(state.phase === 'battle' && pendingShot())
    || (state.phase === 'over' && state.rematch && !state.oppRematch);
}

function onPresenceSync() {
  if (!state) return;
  const others = presenceOthers();

  if (!state.oppKey && state.role === 'host') {
    const guest = others.find(o => o.role === 'guest');
    if (guest) {
      state.oppKey = guest.key;
      state.phase = 'placing';
      selectedShipId = FLEET[0].id;
      save();
      toast('Opponent joined!');
      vibrate(80);
    }
  }

  const wasOnline = oppOnline;
  oppOnline = Boolean(state.oppKey) && others.some(o => o.key === state.oppKey);
  if (oppOnline && !wasOnline) {
    state.oppLeft = false;
    sendSync();
  }
  render();
}

function handleMessage(msg) {
  if (!state || msg.from !== state.oppKey || msg.to !== state.clientId) return;

  // Opponent already started the rematch we both asked for
  if (msg.round === state.round + 1 && state.rematch) startNewRound();
  // Opponent missed our rematch request and is still on the old round
  if (msg.round === state.round - 1 && msg.type === 'sync' && msg.rematch && !msg.knowsRematch) {
    send('rematch', { round: msg.round });
    return;
  }
  if (msg.round !== state.round) return;

  switch (msg.type) {
    case 'sync': return onSync(msg);
    case 'ready': return onOppReady();
    case 'shot': return onShot(msg.row, msg.col);
    case 'result': return onResult(msg);
    case 'reveal':
      state.oppShips = msg.ships;
      save();
      return render();
    case 'rematch':
      if (!state.oppRematch) toast('Opponent wants a rematch');
      state.oppRematch = true;
      return checkRematch();
    case 'leave':
      state.oppLeft = true;
      oppOnline = false;
      save();
      toast('Your opponent left the game');
      return render();
  }
}

function onSync(msg) {
  if (msg.ready) onOppReady();
  msg.fired
    .filter(shot => !state.received.some(r => sameCell(r, shot)))
    .forEach(shot => onShot(shot.row, shot.col));
  msg.received.forEach(result => onResult(result));
  if (msg.rematch && state.phase === 'over') {
    state.oppRematch = true;
    checkRematch();
  }
  if (msg.ships) state.oppShips = msg.ships;

  // Reply if the opponent is missing something I know (including results
  // for shots this very sync just delivered)
  const theyAreBehind = (state.ready && !msg.knowsReady)
    || (state.rematch && !msg.knowsRematch)
    || (state.phase === 'over' && !msg.knowsShips)
    || msg.received.length < state.fired.length
    || msg.resolved < state.received.length;
  if (theyAreBehind) sendSync();
  save();
  render();
}

function onOppReady() {
  if (state.oppReady) return;
  state.oppReady = true;
  maybeStartBattle();
  save();
  render();
}

function maybeStartBattle() {
  if (state.phase === 'placing' && state.ready && state.oppReady) {
    state.phase = 'battle';
    toast(iShootFirst() ? 'Battle stations! You fire first.' : 'Battle stations! Opponent fires first.');
  }
}

/* ---------- Game actions ---------- */

function onShot(row, col) {
  // A shot proves the opponent saw us both ready
  if (state.phase === 'placing' && state.ready) onOppReady();
  if (state.phase !== 'battle' && state.phase !== 'over') return;

  const known = state.received.find(r => sameCell(r, { row, col }));
  if (known) {
    send('result', known);
    return;
  }
  if (state.phase === 'over') return;

  const result = resolveShot(state.ships, state.received, row, col);
  state.received.push(result);
  send('result', result);

  if (result.hit) vibrate(result.sunk ? [60, 40, 120] : 60);
  if (result.gameOver) {
    endGame();
  } else if (result.sunk) {
    toast(`They sank your ${shipName(result.sunk.id)}`);
  }
  save();
  render();
}

function onResult(result) {
  const shot = state.fired.find(f => sameCell(f, result));
  if (!shot || shot.result) return;
  shot.result = { hit: result.hit, sunk: result.sunk, gameOver: result.gameOver };

  if (result.gameOver) endGame();
  else if (result.sunk) toast(`You sank their ${shipName(result.sunk.id)}!`);
  else toast(result.hit ? 'Hit!' : 'Miss');
  save();
  render();
}

function endGame() {
  state.phase = 'over';
  send('reveal', { ships: state.ships });
  const won = winner() === 'me';
  toast(won ? 'Victory! Enemy fleet destroyed.' : 'Your fleet has been sunk.');
  vibrate(won ? [80, 60, 80, 60, 200] : 300);
}

function fire(row, col) {
  if (!isMyTurn() || state.fired.some(f => sameCell(f, { row, col }))) return;
  state.fired.push({ row, col });
  save();
  send('shot', { row, col });
  render();
}

function checkRematch() {
  if (state.rematch && state.oppRematch) startNewRound();
  else {
    save();
    render();
  }
}

function startNewRound() {
  Object.assign(state, {
    round: state.round + 1,
    phase: 'placing',
    ready: false,
    oppReady: false,
    fired: [],
    received: [],
    oppShips: null,
    rematch: false,
    oppRematch: false
  });
  // Keep the previous layout so players can just hit Ready again
  selectedShipId = null;
  save();
  render();
  toast('New round, place your fleet');
}

async function createGame() {
  state = newState(generateCode(), 'host');
  save();
  setUrl(state.code);
  render();
  await connect();
}

async function joinGame(rawCode) {
  const code = normalizeCode(rawCode);
  const error = $('homeError');
  if (code.length < 4) {
    error.textContent = 'Enter the code your opponent shared.';
    return;
  }

  const saved = load(code);
  if (saved) {
    resumeGame(saved);
    return;
  }

  const btn = $('joinForm').querySelector('button');
  btn.disabled = true;
  btn.textContent = 'Joining…';
  error.textContent = '';

  state = newState(code, 'guest');
  const ok = await connect();
  const others = ok ? await waitForPresence(o => o.role === 'host', JOIN_TIMEOUT_MS) : [];
  const host = others.find(o => o.role === 'host');

  btn.disabled = false;
  btn.textContent = 'Join';

  let problem = null;
  if (!ok) problem = `Could not connect to the game server (${lastError}).`;
  else if (!host) problem = 'No game found with that code.';
  else if (others.some(o => o.role === 'guest')) problem = 'That game already has two players.';

  if (problem) {
    await disconnect();
    state = null;
    error.textContent = problem;
    return;
  }

  state.oppKey = host.key;
  state.phase = 'placing';
  selectedShipId = FLEET[0].id;
  oppOnline = true;
  save();
  setUrl(code);
  sendSync();
  render();
}

async function resumeGame(saved) {
  state = saved;
  setUrl(state.code);
  render();
  await connect();
}

async function leaveGame() {
  if (state) {
    await send('leave');
    forget(state.code);
  }
  await disconnect();
  state = null;
  selectedShipId = null;
  setUrl(null);
  render();
}

/* ---------- Placement ---------- */

function shipName(id) {
  return FLEET.find(s => s.id === id)?.name || 'ship';
}

function nextUnplaced() {
  return FLEET.find(s => !state.ships.some(p => p.id === s.id))?.id || null;
}

function candidateAt(row, col) {
  const { id, length } = FLEET.find(s => s.id === selectedShipId);
  // Nudge the ship back inside the grid instead of rejecting edge taps
  return {
    id,
    length,
    vertical,
    row: vertical ? Math.min(row, SIZE - length) : row,
    col: vertical ? col : Math.min(col, SIZE - length)
  };
}

function onPlaceCell(row, col) {
  if (state.ready) return;
  const existing = shipAt(state.ships, row, col);

  if (selectedShipId) {
    const candidate = candidateAt(row, col);
    if (canPlace(state.ships, candidate)) {
      state.ships = state.ships.filter(s => s.id !== candidate.id).concat(candidate);
      selectedShipId = nextUnplaced();
      save();
      render();
      return;
    }
    if (!existing) {
      toast("That ship doesn't fit there");
      return;
    }
  }

  if (existing) {
    // Pick the ship back up to move or rotate it
    state.ships = state.ships.filter(s => s.id !== existing.id);
    selectedShipId = existing.id;
    vertical = existing.vertical;
    save();
    render();
  }
}

function previewPlacement(row, col) {
  const board = boards.place;
  board.cells.flat().forEach(cell => cell.classList.remove('is-preview', 'is-invalid'));
  if (!selectedShipId || state.ready || row == null) return;
  const candidate = candidateAt(row, col);
  const cls = canPlace(state.ships, candidate) ? 'is-preview' : 'is-invalid';
  shipCells(candidate).forEach(([r, c]) => board.cells[r]?.[c]?.classList.add(cls));
}

/* ---------- Rendering ---------- */

function buildBoard(container, onCell) {
  const cells = [];
  container.appendChild(document.createElement('span'));
  for (let c = 0; c < SIZE; c++) {
    const label = document.createElement('span');
    label.className = 'bs-label';
    label.textContent = COLUMNS[c];
    container.appendChild(label);
  }
  for (let r = 0; r < SIZE; r++) {
    const rowLabel = document.createElement('span');
    rowLabel.className = 'bs-label';
    rowLabel.textContent = r + 1;
    container.appendChild(rowLabel);
    cells.push([]);
    for (let c = 0; c < SIZE; c++) {
      const cell = document.createElement('button');
      cell.type = 'button';
      cell.className = 'bs-cell';
      cell.dataset.row = r;
      cell.dataset.col = c;
      cell.setAttribute('aria-label', `${COLUMNS[c]}${r + 1}`);
      if (onCell) cell.addEventListener('click', () => onCell(r, c));
      container.appendChild(cell);
      cells[r].push(cell);
    }
  }
  return { container, cells };
}

function resetBoard(board) {
  board.cells.flat().forEach(cell => { cell.className = 'bs-cell'; });
}

function mark(board, row, col, ...classes) {
  board.cells[row][col].classList.add(...classes);
}

function markShips(board, ships, ...classes) {
  ships.forEach(ship => shipCells(ship).forEach(([r, c]) => mark(board, r, c, ...classes)));
}

function show(screenId) {
  ['screenHome', 'screenLobby', 'screenPlace', 'screenBattle'].forEach(id => { $(id).hidden = id !== screenId; });
}

function render() {
  const chip = $('codeChip');
  chip.textContent = state?.code || '';

  const joining = state && state.role === 'guest' && !state.oppKey;
  chip.hidden = !state || joining;

  if (!state || joining) show('screenHome');
  else if (state.phase === 'lobby') {
    show('screenLobby');
    $('lobbyCode').textContent = state.code;
  } else if (state.phase === 'placing') {
    show('screenPlace');
    renderPlacement();
  } else {
    show('screenBattle');
    renderBattle();
  }
  renderBanner();
}

function renderBanner() {
  const banner = $('connBanner');
  let text = '';
  if (state && channel && !connected) text = `Connection lost. Reconnecting… (${lastError || 'connecting'})`;
  else if (state?.oppLeft) text = 'Your opponent left the game.';
  else if (state?.oppKey && !oppOnline && channel) text = 'Opponent is offline. Waiting for them to come back…';
  banner.textContent = text;
  banner.hidden = !text;
}

function renderPlacement() {
  const board = boards.place;
  resetBoard(board);
  markShips(board, state.ships, 'is-ship');

  const dock = $('dock');
  dock.replaceChildren(...FLEET.map(ship => {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'bs-dock-ship';
    btn.classList.toggle('is-selected', ship.id === selectedShipId);
    btn.classList.toggle('is-placed', state.ships.some(s => s.id === ship.id));
    btn.disabled = state.ready;
    const blocks = document.createElement('span');
    blocks.className = 'bs-dock-blocks';
    blocks.append(...Array.from({ length: ship.length }, () => document.createElement('i')));
    const name = document.createElement('span');
    name.textContent = ship.name;
    btn.append(blocks, name);
    btn.addEventListener('click', () => {
      state.ships = state.ships.filter(s => s.id !== ship.id);
      selectedShipId = ship.id;
      save();
      render();
    });
    return btn;
  }));

  $('rotateLabel').textContent = vertical ? 'Vertical' : 'Horizontal';
  ['rotateBtn', 'randomBtn', 'clearBtn'].forEach(id => { $(id).disabled = state.ready; });

  const readyBtn = $('readyBtn');
  readyBtn.disabled = state.ready || state.ships.length !== FLEET.length;
  readyBtn.textContent = state.ready ? 'Waiting for opponent…' : 'Ready';

  const selected = FLEET.find(s => s.id === selectedShipId);
  $('placeStatus').textContent = state.ready
    ? 'Fleet locked in. Waiting for your opponent…'
    : selected
      ? `Tap a cell to place your ${selected.name} (${selected.length})`
      : state.ships.length === FLEET.length
        ? 'All set! Tap a ship to move it, or press Ready'
        : 'Pick a ship below';
}

function renderBattle() {
  const over = state.phase === 'over';
  const myTurn = isMyTurn();
  const result = winner();

  // Enemy board: my shots
  const enemy = boards.enemy;
  resetBoard(enemy);
  enemy.container.classList.toggle('is-target', myTurn);
  if (over && state.oppShips) markShips(enemy, state.oppShips, 'is-revealed');
  state.fired.forEach(({ row, col, result: res }) => {
    if (res) mark(enemy, row, col, res.hit ? 'is-hit' : 'is-miss');
  });
  const enemySunk = state.fired.filter(f => f.result?.sunk).map(f => f.result.sunk);
  markShips(enemy, enemySunk, 'is-sunk');
  const lastFired = state.fired[state.fired.length - 1];
  if (lastFired) mark(enemy, lastFired.row, lastFired.col, 'is-last');

  // My board: their shots
  const mine = boards.mine;
  resetBoard(mine);
  markShips(mine, state.ships, 'is-ship');
  state.received.forEach(({ row, col, hit }) => mark(mine, row, col, hit ? 'is-hit' : 'is-miss'));
  const mySunk = state.received.filter(r => r.sunk).map(r => r.sunk);
  markShips(mine, mySunk, 'is-sunk');
  const lastReceived = state.received[state.received.length - 1];
  if (lastReceived) mark(mine, lastReceived.row, lastReceived.col, 'is-last');

  renderFleet($('enemyFleet'), enemySunk);
  renderFleet($('myFleet'), mySunk);

  const banner = $('turnBanner');
  banner.className = 'bs-turn';
  if (over) {
    banner.textContent = result === 'me' ? 'You win! 🎉' : 'You lost. Better luck next time!';
    banner.classList.add(result === 'me' ? 'is-win' : 'is-loss');
  } else if (myTurn) {
    banner.textContent = 'Your turn: tap enemy waters to fire';
    banner.classList.add('is-mine');
  } else if (pendingShot()) {
    banner.textContent = 'Firing…';
  } else {
    banner.textContent = "Opponent's turn…";
  }

  $('overActions').hidden = !over;
  $('quitBtn').hidden = over;
  const rematchBtn = $('rematchBtn');
  rematchBtn.disabled = state.rematch || state.oppLeft;
  rematchBtn.innerHTML = state.rematch
    ? '<span class="bs-spinner"></span> Waiting…'
    : `<i class="fa-solid fa-rotate-right"></i> ${state.oppRematch ? 'Accept rematch' : 'Rematch'}`;
}

function renderFleet(container, sunkShips) {
  container.replaceChildren(...FLEET.map(ship => {
    const tag = document.createElement('span');
    tag.textContent = ship.length;
    tag.title = ship.name;
    tag.classList.toggle('is-sunk', sunkShips.some(s => s.id === ship.id));
    return tag;
  }));
}

function toast(text) {
  const el = $('toast');
  el.textContent = text;
  el.classList.add('is-visible');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove('is-visible'), 2200);
}

function vibrate(pattern) {
  try { navigator.vibrate?.(pattern); } catch { /* unsupported */ }
}

/* ---------- Wiring ---------- */

function init() {
  pruneStorage();

  boards.place = buildBoard($('placeBoard'), onPlaceCell);
  boards.enemy = buildBoard($('enemyBoard'), fire);
  boards.mine = buildBoard($('myBoard'));

  $('placeBoard').addEventListener('pointerover', e => {
    const { row, col } = e.target.dataset;
    previewPlacement(row == null ? null : Number(row), Number(col));
  });
  $('placeBoard').addEventListener('pointerleave', () => previewPlacement(null));

  $('createBtn').addEventListener('click', createGame);
  $('joinForm').addEventListener('submit', e => {
    e.preventDefault();
    joinGame($('joinInput').value);
  });
  $('joinInput').addEventListener('input', e => {
    e.target.value = normalizeCode(e.target.value);
    $('homeError').textContent = '';
  });

  $('shareBtn').addEventListener('click', async () => {
    const text = `Join my Battleship game with code ${state.code}`;
    try {
      if (navigator.share) {
        await navigator.share({ title: 'Battleship', text });
        return;
      }
      await navigator.clipboard.writeText(state.code);
      toast('Code copied');
    } catch { /* share dismissed */ }
  });

  $('rotateBtn').addEventListener('click', () => {
    vertical = !vertical;
    render();
  });
  $('randomBtn').addEventListener('click', () => {
    state.ships = randomFleet();
    selectedShipId = null;
    save();
    render();
  });
  $('clearBtn').addEventListener('click', () => {
    state.ships = [];
    selectedShipId = FLEET[0].id;
    save();
    render();
  });
  $('readyBtn').addEventListener('click', () => {
    if (state.ships.length !== FLEET.length) return;
    state.ready = true;
    selectedShipId = null;
    send('ready');
    maybeStartBattle();
    save();
    render();
  });

  $('rematchBtn').addEventListener('click', () => {
    state.rematch = true;
    send('rematch');
    checkRematch();
  });

  document.querySelectorAll('.js-leave').forEach(btn => btn.addEventListener('click', () => {
    const inGame = state && ['placing', 'battle'].includes(state.phase) && !state.oppLeft;
    if (inGame && !confirm('Leave this game? Your opponent will be notified.')) return;
    leaveGame();
  }));

  // Phones drop sockets when locked; catch up as soon as we're visible again
  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible' && state) sendSync();
  });

  const code = normalizeCode(new URLSearchParams(window.location.search).get('game'));
  const saved = code && load(code);
  if (saved) resumeGame(saved);
  else {
    if (code) $('joinInput').value = code;
    render();
  }
}

init();
