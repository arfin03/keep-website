// static/js/top.js
const TOP_CONTAINER = document.querySelector('#top-list'); // sesuaikan
const CURRENT_USER_ID = window.CURRENT_USER_ID || null;

async function fetchTop(type='waifu', limit=50) {
  try {
    const res = await fetch(`/api/top?type=${type}&limit=${limit}`);
    const j = await res.json();
    if (!j.ok) return;
    renderTop(j.items);
  } catch (e) {
    console.error('fetchTop error', e);
  }
}

function renderTop(items) {
  if (!TOP_CONTAINER) return;
  if (!items || items.length === 0) {
    TOP_CONTAINER.innerHTML = '<div class="empty">There is no Top charms data yet.</div>';
    return;
  }
  TOP_CONTAINER.innerHTML = items.map(it => {
    const avatar = it.avatar || '/static/default.png';
    const name = it.name || 'Traveler';
    const username = it.username ? `@${it.username}` : '';
    return `
      <div class="leader-row" data-user-id="${it.user_id}">
        <img class="avatar" src="${avatar}" alt="${name}" />
        <div class="meta">
          <div class="name">${name} ${username ? `<small>${username}</small>` : ''}</div>
          <div class="score">#${it.rank} • ${Number(it.charms).toLocaleString()}</div>
        </div>
      </div>
    `;
  }).join('');
}

// SSE subscription to update single rows / current user balance
function subscribeSSE() {
  try {
    const es = new EventSource('/stream/charms');
    es.onmessage = (e) => {
      try {
        const d = JSON.parse(e.data);
        const uid = String(d.user_id);
        const charms = Number(d.charms||0);
        // update row if present
        const row = document.querySelector(`.leader-row[data-user-id="${uid}"]`);
        if (row) {
          const scoreEl = row.querySelector('.score');
          if (scoreEl) scoreEl.textContent = `# - • ${charms.toLocaleString()}`;
        }
        // update current user badge
        if (CURRENT_USER_ID && uid === String(CURRENT_USER_ID)) {
          const bal = document.querySelector('#balance');
          if (bal) bal.textContent = charms.toLocaleString();
        }
      } catch (err) { console.error(err); }
    };
  } catch (err) {
    console.warn('SSE not available', err);
  }
}

// init
document.addEventListener('DOMContentLoaded', () => {
  fetchTop('waifu', 100);
  subscribeSSE();
  // optionally allow switching type by buttons...
});
