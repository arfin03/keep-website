
// static/js/profile.js
// Handles profile avatar/name loading + collection rendering + history rendering
// Put this file after core.js so `state.user` exists (or adapt to your app state)

async function safeJson(res) {
  try {
    return await res.json();
  } catch (e) {
    return null;
  }
}

function isValidAvatarUrl(url) {
  if (!url || typeof url !== 'string') return false;
  const s = url.trim();
  if (!s.startsWith('http')) return false;
  if (s.includes('picsum.photos')) return false;
  if (s.includes('/static/default') || s.includes('/static/')) {
    // local static images are considered placeholders; prefer real remote avatar
    return false;
  }
  return true;
}

// Extract first http(s) url from a string (handles concatenated URLs)
function extractFirstHttpUrl(s) {
  if (!s || typeof s !== 'string') return null;
  const m = s.match(/https?:\/\/[^\s,;'"]+/);
  if (!m) return null;
  return m[0];
}

// Scan an object/document for avatar-like urls (deep shallow scan)
function findAvatarInDoc(doc) {
  if (!doc || typeof doc !== 'object') return null;
  const keys = ['avatar', 'photo_url', 'photo', 'picture', 'image', 'img_url', 'img', 'image_url', 'userpic', 'telegram_photo', 'tg_photo'];
  for (const k of keys) {
    const v = doc[k];
    if (!v) continue;
    if (Array.isArray(v)) {
      for (const el of v) {
        if (typeof el === 'string') {
          const url = extractFirstHttpUrl(el);
          if (isValidAvatarUrl(url)) return url;
        }
      }
    } else if (typeof v === 'string') {
      // if full http url or maybe telegram id -> convert if bare token
      const urlFound = extractFirstHttpUrl(v);
      if (isValidAvatarUrl(urlFound)) return urlFound;
      // telegram "id" style (some DBs store bare file id)
      if (/^[A-Za-z0-9_\-]+\.svg$/.test(v) || /^[A-Za-z0-9_\-]{20,}$/.test(v)) {
        // construct userpic url guess
        const tg = `https://t.me/i/userpic/320/${v}`;
        if (isValidAvatarUrl(tg)) return tg;
      }
    }
  }
  // check nested profile object
  if (doc.profile && typeof doc.profile === 'object') {
    const nested = findAvatarInDoc(doc.profile);
    if (nested) return nested;
  }
  // last resort: scan all string fields for an http url
  for (const [k, v] of Object.entries(doc)) {
    if (typeof v === 'string') {
      const url = extractFirstHttpUrl(v);
      if (isValidAvatarUrl(url)) return url;
    }
  }
  return null;
}

async function fetchAndEnsureAvatar(uid, doPersistToServer = true) {
  // returns avatar url or null
  if (!uid) return null;
  try {
    const res = await fetch(`/api/user_info?user_id=${encodeURIComponent(uid)}`);
    const data = await safeJson(res);
    if (data && data.ok && data.avatar && isValidAvatarUrl(data.avatar)) {
      return data.avatar;
    }
  } catch (e) {
    console.warn("fetch /api/user_info failed", e);
  }

  // try inspect_user to hunt for avatars in other collections
  try {
    const res2 = await fetch(`/api/inspect_user?user_id=${encodeURIComponent(uid)}`);
    const info = await safeJson(res2);
    if (info && info.ok && info.sources) {
      // search sources for avatar (priority order: registered_users, global_user_profiles_coll, waifu_users_coll, husband_users_coll, top_global_coll)
      const order = ['registered_users', 'global_user_profiles_coll', 'waifu_users_coll', 'husband_users_coll', 'top_global_coll'];
      for (const key of order) {
        const src = info.sources[key];
        if (!src) continue;
        // src may be object or array
        if (Array.isArray(src)) {
          for (const s of src) {
            const found = findAvatarInDoc(s);
            if (found) {
              // persist to server via POST /api/user_info (non-destructive)
              if (doPersistToServer) {
                try {
                  await fetch('/api/user_info', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({user_id: uid, avatar: found})
                  });
                } catch(e) {
                  console.warn("persist avatar to /api/user_info failed", e);
                }
              }
              return found;
            }
          }
        } else {
          const found = findAvatarInDoc(src);
          if (found) {
            if (doPersistToServer) {
              try {
                await fetch('/api/user_info', {
                  method: 'POST',
                  headers: {'Content-Type': 'application/json'},
                  body: JSON.stringify({user_id: uid, avatar: found})
                });
              } catch(e) {
                console.warn("persist avatar to /api/user_info failed", e);
              }
            }
            return found;
          }
        }
      }
    }
  } catch (e) {
    console.warn("fetch /api/inspect_user failed", e);
  }
  return null;
}

function setProfileDom(avatarUrl, name, uid, balance) {
  try {
    const small = document.getElementById('profile-avatar');
    const big = document.getElementById('profile-avatar-lg');
    const nameEl = document.getElementById('profile-name');
    const nameLg = document.getElementById('profile-name-lg');
    const idLg = document.getElementById('profile-id-lg');
    const balanceEl = document.getElementById('balance-display');

    if (small) {
      if (avatarUrl && isValidAvatarUrl(avatarUrl)) {
        small.src = avatarUrl;
      } else {
        // keep as empty src so CSS can show fallback or let app decide
        // but avoid forcing local static default (server side controls default)
        small.src = '/static/default.png';
      }
      small.onerror = function(){ this.onerror=null; this.src='/static/default.png'; }
    }
    if (big) {
      if (avatarUrl && isValidAvatarUrl(avatarUrl)) {
        big.src = avatarUrl;
      } else {
        big.src = '/static/default.png';
      }
      big.onerror = function(){ this.onerror=null; this.src='/static/default.png'; }
    }
    if (nameEl) nameEl.innerText = name || 'Traveler';
    if (nameLg) nameLg.innerText = name || 'Traveler';
    if (idLg) idLg.innerText = uid || '';
    if (balanceEl) balanceEl.innerText = Number(balance || 0).toLocaleString();
  } catch (e) {
    console.warn("setProfileDom error", e);
  }
}

// public initializer: loads profile info and ensures avatar saved on server/redis
async function initProfile(user) {
  // user: object {id, name, avatar, ...} or if null, try to use global state.user
  const uid = (user && user.id) ? user.id : (window.state && window.state.user && window.state.user.id ? window.state.user.id : null);
  if (!uid) return;

  // first, if state.user already has a good avatar, use it
  let avatar = (user && user.avatar) || (window.state && window.state.user && window.state.user.avatar) || null;
  let name = (user && user.name) || (window.state && window.state.user && window.state.user.name) || null;
  let balance = (window.state && window.state.user && window.state.user.balance) || 0;

  if (!isValidAvatarUrl(avatar)) {
    const found = await fetchAndEnsureAvatar(uid, true);
    if (found) {
      avatar = found;
    }
  }

  // refresh name & balance from server if possible
  try {
    const res = await fetch(`/api/user_info?user_id=${encodeURIComponent(uid)}`);
    const info = await safeJson(res);
    if (info && info.ok) {
      name = info.name || name;
      balance = info.balance || balance;
      if (info.avatar && isValidAvatarUrl(info.avatar)) {
        avatar = info.avatar;
      }
    }
  } catch (e) {
    console.warn("initProfile: /api/user_info fetch failed", e);
  }

  // set DOM
  setProfileDom(avatar, name, uid, balance);

  // update global state if available
  try {
    if (window.state && window.state.user) {
      window.state.user.avatar = avatar || null;
      window.state.user.name = name || window.state.user.name;
      window.state.user.balance = balance || window.state.user.balance;
    }
  } catch (e) {}

  return { avatar, name, uid, balance };
}

// ================= HISTORY (unchanged) =================
async function renderHistory() {
    try {
        const res = await fetch(`/api/history?user_id=${state.user.id}`);
        const data = await res.json();
        const list = document.getElementById('history-list');
        if (!list) return;
        list.innerHTML = '';
        if (data.items && data.items.length > 0) {
            data.items.forEach(tx => {
                list.innerHTML += `
                    <div class="glass-card">
                        <div>${tx.title || '-'}</div>
                        <small>${tx.amount || 0} Charms</small>
                    </div>
                `;
            });
        } else {
            list.innerHTML = `
                <div style="text-align:center; opacity:0.6;">
                    No history.
                </div>
            `;
        }
    } catch (err) {
        console.error("renderHistory error", err);
    }
}

// ================= COLLECTION (fixed: no picsum fallback, skip missing images) =================
async function renderCollection(type, ev = null) {
    const grid = document.getElementById('collection-grid');
    if (!grid) {
        console.error("collection-grid not found");
        return;
    }

    // Handle tab active
    const tabs = document.querySelectorAll('#view-collection .tab');
    tabs.forEach(el => el.classList.remove('active'));
    if (ev && ev.target) {
        ev.target.classList.add('active');
    } else {
        // find tab by type
        const tab = Array.from(tabs).find(t => (t.dataset && t.dataset.type ? t.dataset.type : t.innerText).toLowerCase() === String(type).toLowerCase());
        if (tab) tab.classList.add('active');
    }

    grid.innerHTML = `
        <p style="text-align:center; opacity:0.6;">
            Loading Collection...
        </p>
    `;

    try {
        const res = await fetch(
            `/api/my_collection?user_id=${state.user.id}&type=${type}`
        );
        const data = await res.json();

        grid.innerHTML = '';

        if (!data.ok || !data.items || data.items.length === 0) {
            grid.innerHTML = `
                <div style="text-align:center; grid-column:1/-1; opacity:0.6;">
                    No characters yet.
                </div>
            `;
            return;
        }

        data.items.forEach(char => {
            // Require server to provide img_url (my_collection does this). If missing, skip.
            const imgUrl = char.img_url;
            if (!imgUrl || typeof imgUrl !== 'string') return;

            grid.innerHTML += `
                <div class="char-card-small">
                    <img 
                        src="${escapeAttr(imgUrl)}" 
                        class="char-img" 
                        loading="lazy"
                        onerror="this.onerror=null;this.src='/static/default.png';"
                    >
                    <div class="char-name">
                        ${escapeHtml(char.name || 'Unknown')}
                    </div>
                </div>
            `;
        });

    } catch (err) {
        console.error("Error loading collection:", err);
        grid.innerHTML = `
            <div style="text-align:center;">
                Error loading collection.
            </div>
        `;
    }
}

/* Small helpers used above (XSS safe insertion) */
function escapeHtml(str) {
  return String(str || '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
}
function escapeAttr(str) {
  return escapeHtml(str).replaceAll('"', '&quot;');
}

// auto-init when DOM loaded (if state.user exists)
document.addEventListener('DOMContentLoaded', async () => {
  try {
    if (window.state && window.state.user && window.state.user.id) {
      await initProfile(window.state.user);
    }
  } catch (e) {
    console.warn("profile init failed", e);
  }
});
