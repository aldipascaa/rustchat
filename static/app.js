(() => {
  'use strict';
  const $ = (id) => document.getElementById(id);
  const store = {
    get(k) { try { return localStorage.getItem('rustchat.' + k); } catch (e) { return null; } },
    set(k, v) { try { localStorage.setItem('rustchat.' + k, v); } catch (e) { /* ignore */ } },
    del(k) { try { localStorage.removeItem('rustchat.' + k); } catch (e) { /* ignore */ } },
  };

  const el = {
    login: $('login'), loginForm: $('login-form'), usernameInput: $('username-input'), loginError: $('login-error'),
    app: $('app'), sidebar: $('sidebar'), people: $('people'), scrim: $('scrim'),
    roomList: $('room-list'), newRoomForm: $('new-room-form'), newRoomInput: $('new-room-input'),
    meName: $('me-name'), meAvatar: $('me-avatar'), status: $('conn-status'),
    roomTitle: $('room-title'), roomSub: $('room-sub'),
    messages: $('messages'), jump: $('jump'), empty: $('empty'), typing: $('typing'),
    composer: $('composer'), input: $('composer-input'), send: $('send'),
    peopleList: $('people-list'), peopleCount: $('people-count'),
    toasts: $('toasts'),
  };

  const state = {
    username: null,
    ws: null,
    retry: 0,
    reconnectTimer: null,
    rooms: [],
    current: null,        // room we want to be in
    joined: null,         // room the server confirmed
    messages: [],         // rendered list for the joined room
    seen: new Set(),      // message ids already displayed
    users: [],
    typers: new Map(),    // user -> timeout handle
    lastTypingSent: 0,
    typingStop: null,
    stick: true,
    closing: false,
    everOnline: false,    // received a welcome at least once this session
    rejected: null,       // username_taken / username_reserved error from the server
    hasMore: false,       // older messages exist on the server
    loadingEarlier: false,
  };

  /* ---------- helpers ---------- */
  function hue(name) {
    let h = 0;
    for (const c of name) h = (h * 31 + c.charCodeAt(0)) % 360;
    return h;
  }
  function initials(name) {
    const parts = name.split(/[\s._-]+/).filter(Boolean);
    const s = parts.length > 1 ? parts[0][0] + parts[1][0] : name.slice(0, 2);
    return s.toUpperCase();
  }
  function avatar(name) {
    const a = document.createElement('span');
    a.className = 'avatar';
    a.style.setProperty('--h', hue(name));
    a.textContent = initials(name);
    a.setAttribute('aria-hidden', 'true');
    return a;
  }
  const timeFmt = new Intl.DateTimeFormat([], { hour: '2-digit', minute: '2-digit' });
  const dayFmt = new Intl.DateTimeFormat([], { weekday: 'long', month: 'short', day: 'numeric' });
  const fmtTime = (ts) => timeFmt.format(new Date(ts));
  const dayKey = (ts) => new Date(ts).toDateString();

  function toast(text, kind) {
    const t = document.createElement('div');
    t.className = 'toast' + (kind ? ' ' + kind : '');
    t.textContent = text;
    el.toasts.appendChild(t);
    setTimeout(() => t.remove(), 4000);
  }

  function setStatus(kind, text) {
    el.status.className = 'status ' + kind;
    el.status.textContent = text;
  }

  /* ---------- theme ---------- */
  $('theme-toggle').addEventListener('click', () => {
    const root = document.documentElement;
    const cur = root.dataset.theme || (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const next = cur === 'dark' ? 'light' : 'dark';
    root.dataset.theme = next;
    store.set('theme', next);
  });

  /* ---------- drawers ---------- */
  function closeDrawers() {
    el.sidebar.classList.remove('open');
    el.people.classList.remove('open');
    el.scrim.hidden = true;
    $('open-people').setAttribute('aria-expanded', 'false');
  }
  $('open-sidebar').addEventListener('click', () => { el.sidebar.classList.add('open'); el.scrim.hidden = false; $('close-sidebar').focus(); });
  $('close-sidebar').addEventListener('click', () => { closeDrawers(); $('open-sidebar').focus(); });
  $('open-people').addEventListener('click', () => {
    const open = !el.people.classList.contains('open');
    closeDrawers();
    if (open) { el.people.classList.add('open'); el.scrim.hidden = false; $('open-people').setAttribute('aria-expanded', 'true'); }
  });
  el.scrim.addEventListener('click', closeDrawers);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeDrawers(); });

  /* ---------- login ---------- */
  function showLogin(msg) {
    el.app.hidden = true;
    el.login.hidden = false;
    el.loginError.textContent = msg || '';
    el.usernameInput.value = state.username || store.get('username') || '';
    el.usernameInput.focus();
  }
  el.loginForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const name = el.usernameInput.value.trim();
    if (!/^[A-Za-z0-9_.-]{1,24}$/.test(name)) {
      el.loginError.textContent = 'Use 1-24 letters, digits, "_", "-" or ".".';
      return;
    }
    store.set('username', name);
    start(name);
  });
  $('logout').addEventListener('click', () => {
    state.closing = true;
    if (state.ws) state.ws.close();
    clearTimeout(state.reconnectTimer);
    store.del('username');
    state.username = null;
    showLogin();
  });

  function start(name) {
    state.username = name;
    state.closing = false;
    state.everOnline = false;
    state.rejected = null;
    state.retry = 0;
    el.login.hidden = true;
    el.app.hidden = false;
    el.meName.textContent = name;
    el.meAvatar.textContent = initials(name);
    el.meAvatar.style.setProperty('--h', hue(name));
    state.current = store.get('room') || 'general';
    renderRoomHeader();
    connect();
  }

  /* ---------- websocket ---------- */
  function connect() {
    clearTimeout(state.reconnectTimer);
    setStatus('', state.retry ? 'Reconnecting…' : 'Connecting…');
    const proto = location.protocol === 'https:' ? 'wss' : 'ws';
    const ws = new WebSocket(`${proto}://${location.host}/ws?username=${encodeURIComponent(state.username)}`);
    state.ws = ws;
    let opened = false;
    ws.onopen = () => { opened = true; state.retry = 0; setStatus('online', 'Online'); };
    ws.onmessage = (ev) => {
      let m;
      try { m = JSON.parse(ev.data); } catch (e) { return; }
      handle(m);
    };
    ws.onclose = (ev) => {
      if (state.ws !== ws || state.closing) return;
      const rej = state.rejected;
      state.rejected = null;
      if (ev.code === 4009 && rej && !state.everOnline) {
        // The name is taken or reserved: back to the login form instead of retrying forever.
        // (After a network blip the old session can linger briefly, so once we have been
        // online we keep retrying with backoff instead.)
        state.closing = true;
        store.del('username');
        showLogin(rej.message);
        return;
      }
      state.joined = null;
      setComposerEnabled(false);
      clearTypers();
      state.retry += 1;
      const delay = Math.min(15000, 500 * 2 ** Math.min(state.retry, 5));
      setStatus('offline', `Disconnected, retrying in ${Math.ceil(delay / 1000)}s`);
      state.reconnectTimer = setTimeout(connect, delay);
    };
    ws.onerror = () => { /* onclose follows */ };
  }
  function send(obj) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) { state.ws.send(JSON.stringify(obj)); return true; }
    return false;
  }

  function handle(m) {
    switch (m.type) {
      case 'welcome':
        state.everOnline = true;
        state.rooms = m.rooms;
        renderRooms();
        if (state.current) joinRoom(state.current, true);
        break;
      case 'rooms':
        state.rooms = m.rooms;
        renderRooms();
        break;
      case 'joined':
        if (m.room !== state.current) { send({ type: 'leave', room: m.room }); break; }
        state.joined = m.room;
        state.messages = [];
        state.seen = new Set();
        state.users = m.users;
        state.hasMore = !!m.has_more;
        state.loadingEarlier = false;
        for (const h of m.history) addMessage({ kind: 'msg', ...h }, true);
        renderMessages();
        renderPeople();
        renderRoomHeader();
        setComposerEnabled(true);
        clearTypers();
        break;
      case 'left':
        break;
      case 'message':
        if (m.room !== state.joined || state.seen.has(m.id)) break;
        addMessage({ kind: 'msg', ...m });
        removeTyper(m.user);
        break;
      case 'system':
        if (m.room !== state.joined) break;
        addMessage({ kind: 'sys', text: m.text, ts: m.ts });
        break;
      case 'presence':
        if (m.room !== state.joined) break;
        state.users = m.users;
        renderPeople();
        renderRoomHeader();
        break;
      case 'typing':
        if (m.room !== state.joined || m.user === state.username) break;
        if (m.typing) addTyper(m.user); else removeTyper(m.user);
        break;
      case 'error':
        if (m.code === 'username_taken' || m.code === 'username_reserved') { state.rejected = m; if (state.everOnline) toast(m.message, 'error'); break; }
        toast(m.message, 'error');
        break;
      case 'lagged':
        // This connection fell behind and missed messages: ask for a fresh snapshot.
        if (m.room === state.joined) { toast('Connection was slow, resyncing…'); send({ type: 'join', room: m.room }); }
        break;
    }
  }

  /* ---------- rooms ---------- */
  function joinRoom(name, force) {
    if (!force && name === state.joined) return;
    if (state.joined && state.joined !== name) send({ type: 'leave', room: state.joined });
    state.current = name;
    store.set('room', name);
    setComposerEnabled(false);
    renderRoomHeader();
    renderRooms();
    send({ type: 'join', room: name });
  }
  function renderRooms() {
    el.roomList.replaceChildren();
    for (const r of state.rooms) {
      const li = document.createElement('li');
      const b = document.createElement('button');
      b.className = 'room-btn';
      b.type = 'button';
      if (r.name === state.current) b.setAttribute('aria-current', 'true');
      const hash = document.createElement('span'); hash.className = 'hash'; hash.textContent = '#';
      const nm = document.createElement('span'); nm.className = 'name'; nm.textContent = r.name;
      const bd = document.createElement('span'); bd.className = 'badge'; bd.textContent = r.users;
      bd.setAttribute('aria-label', `${r.users} online`);
      b.append(hash, nm, bd);
      b.addEventListener('click', () => { joinRoom(r.name); closeDrawers(); el.input.focus(); });
      li.appendChild(b);
      el.roomList.appendChild(li);
    }
  }
  function renderRoomHeader() {
    el.roomTitle.textContent = state.current ? '# ' + state.current : '# —';
    el.roomSub.textContent = state.joined ? `${state.users.length} online` : '';
    el.empty.hidden = !!state.current;
  }
  el.newRoomForm.addEventListener('submit', (e) => {
    e.preventDefault();
    const name = el.newRoomInput.value.trim().toLowerCase().replace(/\s+/g, '-');
    if (!name) return;
    if (!/^[a-z0-9_-]{1,32}$/.test(name)) { toast('Room names: a-z, 0-9, "-" and "_" (max 32).', 'error'); return; }
    send({ type: 'create_room', room: name });
    el.newRoomInput.value = '';
    joinRoom(name);
    closeDrawers();
  });

  /* ---------- people ---------- */
  function renderPeople() {
    el.peopleList.replaceChildren();
    el.peopleCount.textContent = state.users.length;
    if (!state.users.length) {
      const li = document.createElement('li'); li.className = 'people-empty'; li.textContent = 'Nobody here yet.';
      el.peopleList.appendChild(li);
      return;
    }
    for (const u of state.users) {
      const li = document.createElement('li');
      const n = document.createElement('span'); n.textContent = u;
      li.append(avatar(u), n);
      if (u === state.username) { const y = document.createElement('span'); y.className = 'you'; y.textContent = '(you)'; li.appendChild(y); }
      el.peopleList.appendChild(li);
    }
  }

  /* ---------- messages ---------- */
  const GROUP_MS = 5 * 60 * 1000;
  function addMessage(item, bulk) {
    if (item.kind === 'msg') state.seen.add(item.id);
    state.messages.push(item);
    if (!bulk) appendOne(state.messages.length - 1);
  }
  function buildNode(i) {
    const item = state.messages[i];
    const prev = state.messages[i - 1];
    const frag = document.createDocumentFragment();
    if (!prev || dayKey(prev.ts) !== dayKey(item.ts)) {
      const d = document.createElement('div'); d.className = 'day'; d.textContent = dayFmt.format(new Date(item.ts));
      frag.appendChild(d);
    }
    if (item.kind === 'sys') {
      const s = document.createElement('div'); s.className = 'sys';
      const sp = document.createElement('span'); sp.textContent = item.text;
      s.appendChild(sp); frag.appendChild(s);
      return frag;
    }
    const grouped = prev && prev.kind === 'msg' && prev.user === item.user && item.ts - prev.ts < GROUP_MS && dayKey(prev.ts) === dayKey(item.ts);
    const row = document.createElement('div');
    row.className = 'msg' + (grouped ? '' : ' first') + (item.user === state.username ? ' own' : '');
    const body = document.createElement('div');
    const text = document.createElement('div'); text.className = 'msg-text'; text.textContent = item.text;
    if (grouped) {
      const sp = document.createElement('span'); sp.className = 'spacer'; sp.textContent = fmtTime(item.ts);
      row.append(sp);
      body.append(text);
    } else {
      row.append(avatar(item.user));
      const head = document.createElement('div'); head.className = 'msg-head';
      const u = document.createElement('span'); u.className = 'msg-user'; u.textContent = item.user;
      const t = document.createElement('time'); t.className = 'msg-time'; t.dateTime = new Date(item.ts).toISOString(); t.textContent = fmtTime(item.ts);
      head.append(u, t);
      body.append(head, text);
    }
    row.append(body);
    frag.appendChild(row);
    return frag;
  }
  function renderMessages(keepPosition) {
    const before = el.messages.scrollHeight - el.messages.scrollTop;
    el.messages.replaceChildren();
    if (state.hasMore) {
      const b = document.createElement('button');
      b.type = 'button'; b.className = 'load-earlier'; b.textContent = state.loadingEarlier ? 'Loading…' : 'Load earlier messages';
      b.disabled = state.loadingEarlier;
      b.addEventListener('click', loadEarlier);
      el.messages.appendChild(b);
    }
    for (let i = 0; i < state.messages.length; i++) el.messages.appendChild(buildNode(i));
    if (keepPosition) { el.messages.scrollTop = el.messages.scrollHeight - before; return; }
    state.stick = true;
    scrollToBottom();
  }
  // Page backwards through history over REST, using the oldest message on screen as the cursor.
  async function loadEarlier() {
    const room = state.joined;
    const oldest = state.messages.find((m) => m.kind === 'msg');
    if (!room || !oldest || state.loadingEarlier) return;
    state.loadingEarlier = true;
    renderMessages(true);
    try {
      const res = await fetch(`/api/rooms/${encodeURIComponent(room)}/messages?before=${oldest.id}&limit=50`);
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const page = await res.json();
      if (room !== state.joined) return; // switched rooms while loading
      const older = page.messages.filter((m) => !state.seen.has(m.id)).map((m) => ({ kind: 'msg', ...m }));
      for (const m of older) state.seen.add(m.id);
      state.messages = older.concat(state.messages);
      state.hasMore = !!page.has_more;
    } catch (e) {
      toast('Could not load earlier messages', 'error');
    } finally {
      state.loadingEarlier = false;
      if (room === state.joined) renderMessages(true);
    }
  }
  function appendOne(i) {
    const own = state.messages[i].user === state.username;
    el.messages.appendChild(buildNode(i));
    if (state.stick || own) scrollToBottom(); else el.jump.hidden = false;
  }
  function scrollToBottom() {
    el.messages.scrollTop = el.messages.scrollHeight;
    el.jump.hidden = true;
    state.stick = true;
  }
  el.messages.addEventListener('scroll', () => {
    const dist = el.messages.scrollHeight - el.messages.scrollTop - el.messages.clientHeight;
    state.stick = dist < 60;
    if (state.stick) el.jump.hidden = true;
  });
  el.jump.addEventListener('click', () => { scrollToBottom(); el.input.focus(); });

  /* ---------- typing ---------- */
  function addTyper(u) {
    clearTimeout(state.typers.get(u));
    state.typers.set(u, setTimeout(() => removeTyper(u), 4000));
    renderTyping();
  }
  function removeTyper(u) {
    if (state.typers.has(u)) { clearTimeout(state.typers.get(u)); state.typers.delete(u); renderTyping(); }
  }
  function clearTypers() { for (const t of state.typers.values()) clearTimeout(t); state.typers.clear(); renderTyping(); }
  function renderTyping() {
    const names = [...state.typers.keys()];
    el.typing.replaceChildren();
    if (!names.length) return;
    const who = names.length === 1 ? `${names[0]} is typing`
      : names.length === 2 ? `${names[0]} and ${names[1]} are typing` : 'Several people are typing';
    el.typing.append(who);
    const d = document.createElement('span'); d.className = 'dots'; d.setAttribute('aria-hidden', 'true');
    d.innerHTML = '<span>.</span><span>.</span><span>.</span>';
    el.typing.appendChild(d);
  }
  function sendTyping(on) {
    if (!state.joined) return;
    send({ type: 'typing', room: state.joined, typing: on });
  }
  el.input.addEventListener('input', () => {
    const now = Date.now();
    if (el.input.value && now - state.lastTypingSent > 2000) { state.lastTypingSent = now; sendTyping(true); }
    clearTimeout(state.typingStop);
    state.typingStop = setTimeout(() => { state.lastTypingSent = 0; sendTyping(false); }, 2500);
  });

  /* ---------- composer ---------- */
  function setComposerEnabled(on) {
    el.input.disabled = !on;
    el.send.disabled = !on;
    el.input.placeholder = on ? `Message #${state.joined}` : 'Connecting…';
  }
  el.composer.addEventListener('submit', (e) => {
    e.preventDefault();
    const text = el.input.value.trim();
    if (!text || !state.joined) return;
    if (send({ type: 'message', room: state.joined, text })) {
      el.input.value = '';
      clearTimeout(state.typingStop);
      state.lastTypingSent = 0;
      el.input.focus();
    } else {
      toast('Not connected. Your message was not sent.', 'error');
    }
  });

  /* ---------- boot ---------- */
  const params = new URLSearchParams(location.search);
  if (params.get('room')) store.set('room', params.get('room'));
  const initial = params.get('u') || store.get('username');
  if (initial && /^[A-Za-z0-9_.-]{1,24}$/.test(initial)) start(initial); else showLogin();
})();
