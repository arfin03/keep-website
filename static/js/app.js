// --- CONSTANTS ---
const CATEGORY_MAP = {
    '🏖': '🏖𝒐𝒖𝒎𝒎𝒆𝒓 🏖', '👘': '👘𝑲𝒊𝒎𝒐𝒏𝒐👘', '🧹': '🧹𝑴𝒂𝒊𝒅🧹', '🤝': '🤝𝐆𝐫𝐨𝐮𝐩🤝',
    '🎮': '🎮𝑮𝒂𝒎𝒆🎮', '👙': '👙𝑩𝒊𝒌𝒊𝒏𝒊👙', '👥': '👥𝐃𝐮𝐨👥'
    // ... add rest from prompt
};

const RARITIES = ["⚪ Common", "🟠 Rare", "🟢 Medium", "🟡 Legendary", "💮 Special Edition", "🔮 Mythical", "🎐 Celestial", "❄️ Premium Edition", "🫧 X Verse"];

// --- STATE ---
const state = {
    user: { id: null, name: 'Traveler', avatar: '', balance: 0 },
    token: null,
    currentView: 'home',
    market: { filters: { rarity: 'All', sort: 'price-asc' } },
    p2p: { view: 'list', sellingChar: null },
    lang: 'en'
};

// --- INIT ---
window.addEventListener('load', async () => {
    const tg = window.Telegram.WebApp;
    tg.ready();
    tg.expand();

    // Setup User Data
    state.user.id = tg.initDataUnsafe.user?.id || "123456";
    state.user.name = tg.initDataUnsafe.user?.first_name || "Traveler";
    state.user.avatar = tg.initDataUnsafe.user?.photo_url || "https://picsum.photos/seed/user/200/200";

    // Update UI Profile
    document.getElementById('profile-name').innerText = state.user.name;
    document.getElementById('profile-id').innerText = `ID: ${state.user.id}`;
    document.getElementById('profile-avatar').src = state.user.avatar;

    // Fetch Initial Data
    await fetchBalance();
    
    // Render Initial View
    navigateTo('home');
});

// --- NAVIGATION ---
function navigateTo(viewId) {
    document.querySelectorAll('.view-section').forEach(el => el.classList.remove('active'));
    document.getElementById(`view-${viewId}`).classList.add('active');
    
    // Load specific view data
    if (viewId === 'market') renderMarket();
    if (viewId === 'p2p') renderP2P();
    if (viewId === 'top') renderTop();
    if (viewId === 'history') renderHistory();
    
    // Update Bottom Bar
    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    const navBtn = document.querySelector(`.nav-item[onclick="navigateTo('${viewId}')"]`);
    if(navBtn) navBtn.classList.add('active');
}

// --- API HELPER ---
async function apiCall(endpoint, method = 'GET', body = null) {
    try {
        const options = { method, headers: { 'Content-Type': 'application/json' } };
        if (body) options.body = JSON.stringify(body);
        
        // NOTE: Asumsi base URL adalah root
        const res = await fetch(endpoint, options);
        return await res.json();
    } catch (e) {
        console.error("API Error:", e);
        showToast("Connection Error", "error");
        return null;
    }
}

// --- MARKET LOGIC ---
async function renderMarket() {
    const container = document.getElementById('market-list');
    container.innerHTML = '<div class="glass-card">Loading Market...</div>';
    
    // Fetch Data (Asumsi endpoint /api/market dengan query params)
    // const data = await apiCall(`/api/market?type=${state.currentTab}&sort=${state.market.filters.sort}`);
    
    // MOCK DATA for logic demonstration
    setTimeout(() => {
        container.innerHTML = '';
        const mockChars = [
            { id: 1, name: "👘Sakura Maid", rarity: "🟡 Legendary", price: 1500, stock: 29, img: "https://picsum.photos/seed/maid1/200/300" },
            { id: 2, name: "🤝Cyber Duo", rarity: "⚪ Common", price: 200, stock: 28, img: "https://picsum.photos/seed/duo1/200/300" },
        ];

        mockChars.forEach(char => {
            const card = document.createElement('div');
            card.className = 'glass-card';
            card.innerHTML = `
                <div style="display:flex; gap:12px;">
                    <img src="${char.img}" style="width:80px; height:100px; border-radius:8px; object-fit:cover;">
                    <div style="flex:1;">
                        <h3 style="margin:0;">${char.name}</h3>
                        <div style="color:#ffd700; font-size:12px;">${char.rarity}</div>
                        <div style="margin-top:8px; display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-weight:bold; color:var(--accent);">${char.price} <i class="fa-solid fa-gem"></i></span>
                            <button class="glass-btn primary-btn" onclick="buyMarketChar(${char.id})">${translations[state.lang]['market.buy'] || 'Buy'}</button>
                        </div>
                        <div style="font-size:10px; margin-top:4px; opacity:0.6;">${translations[state.lang]['market.stock_left'] || 'Left'}: ${char.stock}</div>
                    </div>
                </div>
            `;
            container.appendChild(card);
        });
    }, 500);
}

// --- P2P LOGIC ---
async function renderP2P() {
    const container = document.getElementById('p2p-list');
    container.innerHTML = '<div class="glass-card">Loading P2P...</div>';
    
    // Fetch P2P Listings
    // const data = await apiCall('/api/p2p_listings');
    
    // MOCK
    setTimeout(() => {
        container.innerHTML = '';
        const mockListings = [
            { id: 101, name: "👘Special Kimono", seller: "UserX", price: 5000, img: "https://picsum.photos/seed/p2p1/200/300" }
        ];
        
        mockListings.forEach(item => {
            const card = document.createElement('div');
            card.className = 'glass-card';
            card.innerHTML = `
                <div style="display:flex; gap:12px;" onclick="openP2PDetail(${item.id})">
                    <img src="${item.img}" style="width:80px; height:100px; border-radius:8px; object-fit:cover;">
                    <div style="flex:1;">
                        <div style="font-size:10px; color:var(--primary);">Seller: ${item.seller}</div>
                        <h3 style="margin:5px 0;">${item.name}</h3>
                        <div style="font-weight:bold; color:var(--accent);">${item.price} <i class="fa-solid fa-gem"></i></div>
                    </div>
                </div>
            `;
            container.appendChild(card);
        });
    }, 500);
}

// Open Sell Modal
function openSellModal() {
    document.getElementById('modal-sell-step1').classList.add('open');
    loadUserCollectionForSell();
}

function loadUserCollectionForSell() {
    // Fetch user chars from /api/my_collection
    // ...
    // Mock Render
    const grid = document.getElementById('sell-char-grid');
    grid.innerHTML = `
        <div class="char-card-small" onclick="selectCharToSell(1, 'Maiden', 500)">
            <img src="https://picsum.photos/seed/maid1/200/300" class="char-img">
        </div>
    `;
}

// --- WALLET / QR LOGIC ---
// Receive: Generate QR
function generateReceiveQR() {
    const qrContainer = document.getElementById('receive-qr-container');
    const userId = state.user.id;
    
    // Call API to get QR Base64
    // apiCall(`/api/generate_qr?address=${userId}`).then(res => { ... });
    
    // Mock
    qrContainer.innerHTML = `<img src="https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${userId}" style="border-radius:12px;">`;
}

// Send: Open Camera
function openScanner() {
    const view = document.getElementById('camera-view');
    view.style.display = 'flex';
    
    // Logic Camera Access
    if (navigator.mediaDevices && navigator.mediaDevices.getUserMedia) {
        navigator.mediaDevices.getUserMedia({ video: { facingMode: "environment" } })
        .then(stream => {
            const video = document.getElementById('camera-video');
            video.srcObject = stream;
            video.play();
            // Logic QR Scan library implementation here (e.g., html5-qrcode)
        })
        .catch(err => {
            showToast("Camera access denied", "error");
            view.style.display = 'none';
        });
    }
}

function closeScanner() {
    const view = document.getElementById('camera-view');
    view.style.display = 'none';
    // Stop stream logic
}

// --- PROFILE & COLLECTION ---
function renderCollection(type) {
    // Toggle Waifu/Husband logic
    // Fetch from /api/my_collection?type=waifu
}

// --- TOP / LEADERBOARD ---
async function renderTop() {
    // Fetch from /api/top?type=waifu|husband|charms
}

// --- HISTORY ---
async function renderHistory() {
    const list = document.getElementById('history-list');
    list.innerHTML = 'Loading...';
    
    // const data = await apiCall(`/api/history?user_id=${state.user.id}`);
    
    // Mock
    list.innerHTML = `
        <div class="glass-card">
            <div style="display:flex; justify-content:space-between;">
                <span>Bought: Sakura Maid</span>
                <span style="color:#ff4757;">-1500</span>
            </div>
            <small>Today</small>
        </div>
    `;
}

// --- UTILS ---
function showToast(msg, type='info') {
    // Simple toast implementation
    const toast = document.createElement('div');
    toast.style.cssText = `position:fixed; bottom:80px; left:50%; transform:translateX(-50%); background:rgba(0,0,0,0.8); padding:10px 20px; border-radius:20px; z-index:300; animation:slideUp 0.3s;`;
    toast.innerText = msg;
    document.body.appendChild(toast);
    setTimeout(() => toast.remove(), 3000);
}
