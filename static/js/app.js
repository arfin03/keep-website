// --- CONSTANTS ---
const CATEGORY_MAP = {
    '🏖': '🏖𝒐𝒖𝒎𝒎𝒆𝒓 🏖', '👘': '👘𝑲𝒊𝒎𝒐𝒏𝒐👘', '🤝': '🤝𝐆𝐫𝐨𝐮𝐩🤝', '🎮': '🎮𝑮𝒂𝒎𝒆🎮'
    // ... etc
};
const RARITIES = ["⚪ Common", "🟠 Rare", "🟢 Medium", "🟡 Legendary", "💮 Special Edition", "🔮 Mythical", "🎐 Celestial", "❄️ Premium Edition", "🫧 X Verse"];

// --- STATE ---
const state = { user: { id: null, name: 'Traveler', avatar: '', balance: 0 }, lang: 'en' };

// --- INIT ---
window.addEventListener('load', async () => {
    const tg = window.Telegram.WebApp;
    tg.ready(); tg.expand();

    state.user.id = tg.initDataUnsafe.user?.id || "123456";
    state.user.name = tg.initDataUnsafe.user?.first_name || "Traveler";
    state.user.avatar = tg.initDataUnsafe.user?.photo_url || "https://picsum.photos/seed/user/200/200";

    // Update Profile UI
    document.getElementById('profile-name').innerText = state.user.name;
    document.getElementById('profile-avatar').src = state.user.avatar;
    
    // Fetch User Info & Balance
    const res = await fetch(`/api/user_info?user_id=${state.user.id}`);
    if(res.ok) {
        const data = await res.json();
        state.user.balance = data.balance;
        document.getElementById('balance-display').innerText = state.user.balance;
    }
    
    navigateTo('home');
});

// --- NAVIGATION ---
function navigateTo(viewId) {
    document.querySelectorAll('.view-section').forEach(el => el.classList.remove('active'));
    const target = document.getElementById(`view-${viewId}`);
    if(target) target.classList.add('active');
    
    // Load Data
    if(viewId === 'market') renderMarket();
    if(viewId === 'p2p') renderP2P();
    if(viewId === 'top') renderTop();
    if(viewId === 'history') renderHistory();
    if(viewId === 'collection') renderCollection();
    if(viewId === 'friends') renderFriends();
    
    // Nav Active State
    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    const btn = document.querySelector(`.nav-item[onclick="navigateTo('${viewId}')"]`);
    if(btn) btn.classList.add('active');
}

// --- API HELPER ---
async function apiCall(endpoint, method='GET', body=null) {
    try {
        const opts = { method, headers: {'Content-Type': 'application/json'} };
        if(body) opts.body = JSON.stringify(body);
        const res = await fetch(endpoint, opts);
        return await res.json();
    } catch(e) { console.error(e); return null; }
}

// --- MARKET ---
async function renderMarket() {
    // Render List
    const data = await apiCall(`/api/market?type=${state.marketTab || 'waifu'}`);
    const list = document.getElementById('market-list');
    list.innerHTML = '';
    if(data && data.items) {
        data.items.forEach(item => {
            const card = document.createElement('div');
            card.className = 'glass-card';
            card.innerHTML = `
                <div style="display:flex; gap:12px;">
                    <img src="${item.image || 'https://picsum.photos/200/300'}" style="width:80px; height:100px; border-radius:8px; object-fit:cover;">
                    <div style="flex:1;">
                        <h3>${item.name}</h3>
                        <div style="color:#ffd700; font-size:12px;">${item.rarity}</div>
                        <div style="margin-top:8px; display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-weight:bold; color:var(--accent);">${item.price} <i class="fa-solid fa-gem"></i></span>
                            <button class="glass-btn primary-btn" onclick="buyMarket('${item._id}', ${item.price})">Buy</button>
                        </div>
                        <div style="font-size:10px; opacity:0.6;">Stock: ${item.stock}</div>
                    </div>
                </div>`;
            list.appendChild(card);
        });
    }
}

async function buyMarket(id, price) {
    if(state.user.balance < price) return alert("Not enough Charms!");
    const res = await apiCall('/api/buy_market', 'POST', { user_id: state.user.id, item_id: id });
    if(res && res.ok) {
        alert("Purchase Success!");
        location.reload(); // Reload to update balance UI simple
    } else {
        alert(res.error || "Failed");
    }
}

// --- P2P ---
async function renderP2P() {
    const data = await apiCall('/api/p2p_listings');
    const list = document.getElementById('p2p-list');
    list.innerHTML = '';
    if(data && data.items) {
        data.items.forEach(item => {
            const card = document.createElement('div');
            card.className = 'glass-card';
            card.innerHTML = `
                <div style="display:flex; gap:12px;">
                    <img src="${item.char_data?.image}" style="width:80px; height:100px; border-radius:8px; object-fit:cover;">
                    <div style="flex:1;">
                        <div style="font-size:10px; color:var(--primary);">Seller: ${item.seller_name}</div>
                        <h3>${item.char_data?.name}</h3>
                        <div style="font-weight:bold; color:var(--accent);">${item.price} <i class="fa-solid fa-gem"></i></div>
                    </div>
                </div>`;
            list.appendChild(card);
        });
    }
}

// --- TOP / LEADERBOARD ---
async function renderTop() {
    const type = document.querySelector('#view-top .tab.active')?.innerText.toLowerCase() || 'charms';
    const data = await apiCall(`/api/top?type=${type}`);
    const list = document.getElementById('top-list');
    list.innerHTML = '';
    if(data && data.items) {
        data.items.forEach((item, idx) => {
            const div = document.createElement('div');
            div.style.cssText = "display:flex; justify-content:space-between; padding:10px; border-bottom:1px solid rgba(255,255,255,0.1);";
            div.innerHTML = `
                <div style="display:flex; align-items:center; gap:10px;">
                    <span style="font-weight:bold; width:20px;">${idx+1}</span>
                    <span>${item.name || item.firstname}</span>
                </div>
                <span style="color:#ffd700;">${item.count || item.score}</span>`;
            list.appendChild(div);
        });
    }
}

// --- HISTORY ---
async function renderHistory() {
    const data = await apiCall(`/api/history?user_id=${state.user.id}`);
    const list = document.getElementById('history-list');
    list.innerHTML = '';
    if(data && data.items) {
        data.items.forEach(tx => {
            const div = document.createElement('div');
            div.className = 'glass-card';
            div.innerHTML = `<div>${tx.title}</div><small>${tx.amount} Charms</small>`;
            list.appendChild(div);
        });
    }
}

// --- WALLET (SEND/RECEIVE) ---
function openScanner() {
    document.getElementById('camera-view').style.display = 'flex';
    // Init Camera Logic here using getUserMedia
}
function closeScanner() {
    document.getElementById('camera-view').style.display = 'none';
}

// --- COLLECTION ---
async function renderCollection() {
    const type = document.querySelector('#view-collection .tab.active')?.innerText.toLowerCase() || 'waifu';
    const data = await apiCall(`/api/my_collection?user_id=${state.user.id}&type=${type}`);
    const grid = document.getElementById('collection-grid');
    grid.innerHTML = '';
    if(data && data.items) {
        data.items.forEach(char => {
            const div = document.createElement('div');
            div.className = 'char-card-small';
            div.innerHTML = `<img src="${char.image}" class="char-img">`;
            grid.appendChild(div);
        });
    }
}

// --- FRIENDS ---
async function renderFriends() {
    // Mock/Redis Fetch
    const data = await apiCall(`/api/friends?user_id=${state.user.id}`);
    // Render list...
}

// --- UTILS ---
function showToast(msg) { alert(msg); } // Simple alert for now
