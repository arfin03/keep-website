// static/js/top.js (perbarui fungsi renderTop dan tambahkan initTopTabs)
const DEFAULT_AVATAR = "/static/default.png"; // ubah sesuai path project-mu

/**
 * Render leaderboard.
 * @param {string} type - 'waifu' | 'husband' | 'charms' | etc.
 * @param {Event|null} ev - optional event dari klik tab (dipakai untuk toggling active class)
 */
async function renderTop(type = 'waifu', ev = null) {
  // 1. Tampilkan Loading
  const list = document.getElementById('top-list');
  if (!list) return;
  list.innerHTML = '<div style="text-align:center; color:rgba(255,255,255,0.5);">Loading Leaderboard...</div>';

  // 2. Update Visual Tab Active State (safe: gunakan ev bila ada, atau cari tab by type)
  const tabs = document.querySelectorAll('#view-top .tab');
  tabs.forEach(el => el.classList.remove('active'));
  if (ev && ev.target) {
    ev.target.classList.add('active');
  } else {
    const activeTab = Array.from(tabs).find(t => (t.dataset && t.dataset.type ? t.dataset.type.toLowerCase() : t.innerText.toLowerCase()) === String(type).toLowerCase());
    if (activeTab) activeTab.classList.add('active');
  }

  // 3. Fetch Data dari Backend (encode type)
  let data;
  try {
    const res = await fetch(`/api/top?type=${encodeURIComponent(type)}&limit=100`);
    data = await res.json();
  } catch (err) {
    console.error('Error fetching /api/top', err);
    list.innerHTML = '<div style="text-align:center; color:red;">Error loading top list.</div>';
    return;
  }

  // 4. Render List (build satu string lalu set sekali)
  if (!data || !data.ok || !Array.isArray(data.items) || data.items.length === 0) {
    list.innerHTML = `
      <div style="text-align:center; padding:20px; color:rgba(255,255,255,0.5);">
        <i class="fa-solid fa-trophy" style="font-size:40px; margin-bottom:10px; display:block;"></i>
        <p>Belum ada data Top ${escapeHtml(String(type))}.</p>
      </div>`;
    return;
  }

  // build HTML safely
  let html = '';
  data.items.forEach((item, index) => {
    // Score label heuristik: jika item memiliki 'charms' gunakan Charms, else Collection
    const scoreValue = Number(item.charms ?? item.score ?? item.count ?? 0);
    const scoreLabel = ('charms' in item) ? '💎 Charms' : '🃏 Collection';

    // Avatar fallback and sanitation: treat 'null' (string) as missing
    let avatar = item.avatar;
    if (!avatar || avatar === 'null' || String(avatar).trim() === '') {
      avatar = DEFAULT_AVATAR;
    }

    // Nama & username fallback: check several possible fields
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

/**
 * Attach click listeners to tabs (expects .tab elements under #view-top,
 * and each tab to have data-type attribute with the type string)
 */
function initTopTabs() {
  const tabs = document.querySelectorAll('#view-top .tab');
  tabs.forEach(tab => {
    const type = tab.dataset && tab.dataset.type ? tab.dataset.type : tab.innerText;
    tab.addEventListener('click', (ev) => {
      renderTop(type, ev);
    });
  });
  // initial render: try to find first tab or default to 'waifu'
  const firstTab = Array.from(tabs)[0];
  const initialType = firstTab ? (firstTab.dataset && firstTab.dataset.type ? firstTab.dataset.type : firstTab.innerText) : 'waifu';
  renderTop(initialType, null);
}

/** small helpers to avoid XSS in generated HTML */
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

// initialize when DOM ready
document.addEventListener('DOMContentLoaded', () => {
  // ensure #top-list exists before init
  if (document.getElementById('top-list')) {
    initTopTabs();
  }
});
