const state = { 
    user: { id: null, name: 'Traveler', avatar: '', balance: 0 }, 
    marketTab: 'waifu',
    sellStep: 1,
    selectedSellChar: null
};

window.addEventListener('load', async () => {
    const tg = window.Telegram.WebApp;
    tg.ready(); tg.expand();

    state.user.id = tg.initDataUnsafe.user?.id || "123456";
    state.user.name = tg.initDataUnsafe.user?.first_name || "Traveler";
    state.user.avatar = tg.initDataUnsafe.user?.photo_url || "https://picsum.photos/seed/user/200/200";

    document.getElementById('profile-name').innerText = state.user.name;
    document.getElementById('profile-avatar').src = state.user.avatar;

    // Load Settings & Balance
    const res = await fetch(`/api/user_info?user_id=${state.user.id}`);
    if(res.ok) {
        const data = await res.json();
        state.user.balance = data.balance;
        document.getElementById('balance-display').innerText = state.user.balance;
        
        // Set Theme
        if(data.theme_url) {
            document.getElementById('app-bg').style.backgroundImage = `url(${data.theme_url})`;
        }
        // Set Language
        if(data.lang) setLanguage(data.lang);
        // Play BGM
        if(data.bgm_url) {
            new Audio(data.bgm_url).play().catch(e=>console.log("Auto-play blocked"));
        }
    }
});

function navigateTo(viewId) {
    document.querySelectorAll('.view-section').forEach(el => el.classList.remove('active'));
    const target = document.getElementById(`view-${viewId}`);
    if(target) target.classList.add('active');
    
    if(viewId === 'market') renderMarket();
    if(viewId === 'p2p') renderP2P();
    if(viewId === 'receive') renderQR();
    if(viewId === 'history') renderHistory();
    if(viewId === 'collection') renderCollection('waifu');
    if(viewId === 'top') renderTop('waifu');
    if(viewId === 'profile') {
        document.getElementById('profile-name-lg').innerText = state.user.name;
        document.getElementById('profile-avatar-lg').src = state.user.avatar;
        document.getElementById('profile-id-lg').innerText = state.user.id;
    }

    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    const btn = document.querySelector(`.nav-item[onclick="navigateTo('${viewId}')"]`);
    if(btn) btn.classList.add('active');
}

async function apiCall(endpoint, method='GET', body=null) {
    try {
        const opts = { method, headers: {'Content-Type': 'application/json'} };
        if(body) opts.body = JSON.stringify(body);
        const res = await fetch(endpoint, opts);
        return await res.json();
    } catch(e) { console.error(e); return null; }
}

// Reusable Swipe Logic
function initSwipe(containerId, onConfirm) {
    const container = document.getElementById(containerId);
    if(!container) return;

    const thumb = container.querySelector('.swipe-thumb');
    const fill = container.querySelector('.swipe-fill');
    const text = container.querySelector('.swipe-text');
    let isDragging = false, startX = 0, currentX = 0;
    const maxDrag = container.offsetWidth - thumb.offsetWidth - 8; 
    const threshold = maxDrag * 0.85;

    const startDrag = (e) => {
        isDragging = true;
        startX = (e.type === 'touchstart') ? e.touches[0].clientX : e.clientX;
        thumb.style.transition = 'none'; fill.style.transition = 'none';
    };

    const moveDrag = (e) => {
        if (!isDragging) return;
        e.preventDefault(); 
        const x = (e.type === 'touchmove') ? e.touches[0].clientX : e.clientX;
        let diff = x - startX;
        if (diff < 0) diff = 0;
        if (diff > maxDrag) diff = maxDrag;

        currentX = diff;
        thumb.style.transform = `translateX(${currentX}px)`;
        fill.style.width = `${currentX + 26}px`; 
        if (currentX >= threshold) fill.style.background = '#2ecc71'; 
        else fill.style.background = 'var(--primary-dim)';
    };

    const endDrag = () => {
        if (!isDragging) return;
        isDragging = false;
        if (currentX >= threshold) {
            thumb.style.transform = `translateX(${maxDrag}px)`; fill.style.width = '100%';
            onConfirm();
        } else {
            thumb.style.transition = 'transform 0.3s'; fill.style.transition = 'width 0.3s';
            thumb.style.transform = 'translateX(0)'; fill.style.width = '0';
        }
    };

    thumb.addEventListener('touchstart', startDrag); thumb.addEventListener('mousedown', startDrag);
    document.addEventListener('touchmove', moveDrag, {passive: false});
    document.addEventListener('mousemove', moveDrag);
    document.addEventListener('touchend', endDrag); document.addEventListener('mouseup', endDrag);
}

function showToast(msg) {
    alert(msg); // Simple alert for now
}
