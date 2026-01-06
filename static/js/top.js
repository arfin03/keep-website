// static/js/top.js
const DEFAULT_AVATAR = "/static/default.png"; // pastikan file ini ada di static/

async function renderTop(type = 'waifu', ev = null) {
  const list = document.getElementById('top-list');
  if (!list) return;
  list.innerHTML = '<div style="text-align:center; color:rgba(255,255,255,0.5);">Loading Leaderboard...</div>';

  const tabs = document.querySelectorAll('#view-top .tab');
  tabs.forEach(el => el.classList.remove('active'));
  if (ev && ev.target) {
    ev.target.classList.add('active');
  } else {
    const activeTab = Array.from(tabs).find(t => (t.dataset && t.dataset.type ? t.dataset.type.toLowerCase() : t.innerText.toLowerCase()) === String(type).toLowerCase());
    if (activeTab) activeTab.classList.add('active');
  }

  let data;
  try {
    const res = await fetch(`/api/top?type=${encodeURIComponent(type)}&limit=100`);
    data = await res.json();
  } catch (err) {
    console.error('Error fetching /api/top', err);
    list.innerHTML = '<div style="text-align:center; color:red;">Error loading top list.</div>';
    return;
  }

  if (!data || !data.ok || !Array.isArray(data.items) || data.items.length === 0) {
    list.innerHTML = `
      <div style="text-align:center; padding:20px; color:rgba(255,255,255,0.5);">
        <i class="fa-solid fa-trophy" style="font-size:40px; margin-bottom:10px; display:block;"></i>
        <p>Belum ada data Top ${escapeHtml(String(type))}.</p>
      </div>`;
    return;
  }

  let html = '';
  data.items.forEach((item, index) => {
    const scoreValue = Number(item.charms ?? item.score ?? item.count ?? 0);
    const scoreLabel = ('charms' in item) ? '💎 Charms' : '🃏 Collection';

    let avatar = item.avatar;
    if (!avatar || avatar === 'null' || String(avatar).trim() === '') {
      avatar = DEFAULT_AVATAR;
    }

    const name = item.name || item.firstname || item.first_name || 'Traveler';
    const username = item.username ? String(item.username).replace(/^@/, '') : null;

    html += `
      <div class="glass-card" style="display:flex; align-items:center; gap:12px; padding:12px;">
        <div style="font-weight:bold; font-family:var(--font-accent); width:30px; font-size:18px; color:var(--accent); text-align:center;">
          ${index + 1}
        </div>
        <img src="${escapeAttr(avatar)}" onerror="this.onerror=null;this.src='${DEFAULT_AVATAR}';" style="width:40px; height:40px; border-radius:50%; object-fit:cover; border:2px solid rgba(255,255,255,0.2);">
        <div style="flex:1;">
          <div style="font-weight:700; font-size:15px; color:#fff;">
            ${escapeHtml(name)}
            ${username ? `<span style="font-weight:400; font-size:12px; opacity:0.75; margin-left:6px;">@${escapeHtml(username)}</span>` : ''}
          </div>
          <div style="font-size:12px; opacity:0.7; margin-top:2px; display:flex; justify-content:space-between; align-items:center;">
            <span>${scoreLabel}</span>
            <span style="color:#ffd700; font-weight:bold; font-size:14px;">${Number(scoreValue).toLocaleString()}</span>
          </div>
        </div>
      </div>
    `;
  });

  list.innerHTML = html;
}

function initTopTabs() {
  const tabs = document.querySelectorAll('#view-top .tab');
  tabs.forEach(tab => {
    const type = tab.dataset && tab.dataset.type ? tab.dataset.type : tab.innerText;
    tab.addEventListener('click', (ev) => {
      renderTop(type, ev);
    });
  });
  const firstTab = Array.from(tabs)[0];
  const initialType = firstTab ? (firstTab.dataset && firstTab.dataset.type ? firstTab.dataset.type : firstTab.innerText) : 'waifu';
  renderTop(initialType, null);
}

function escapeHtml(str) {
  return String(str)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
function escapeAttr(str) {
  return escapeHtml(str).replaceAll('"', '&quot;');
}

document.addEventListener('DOMContentLoaded', () => {
  if (document.getElementById('top-list')) {
    initTopTabs();
  }
});
