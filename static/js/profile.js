async function renderHistory() {
    const res = await fetch(`/api/history?user_id=${state.user.id}`);
    const data = await res.json();
    const list = document.getElementById('history-list');
    list.innerHTML = '';
    if(data.items) {
        data.items.forEach(tx => {
            list.innerHTML += `
            <div class="glass-card">
                <div>${tx.title}</div>
                <small>${tx.amount} Charms</small>
            </div>`;
        });
    }
}

async function renderCollection(type) {
    const res = await fetch(`/api/my_collection?user_id=${state.user.id}&type=${type}`);
    const data = await res.json();
    const grid = document.getElementById('collection-grid');
    grid.innerHTML = '';
    if(data.items) {
        data.items.forEach(char => {
            grid.innerHTML += `<div class="char-card-small"><img src="${char.image}" class="char-img"></div>`;
        });
    }
}

async function renderTop(type) {
    const res = await fetch(`/api/top?type=${type}`);
    const data = await res.json();
    const list = document.getElementById('top-list');
    list.innerHTML = '';
    if(data.items) {
        data.items.forEach((item, idx) => {
            list.innerHTML += `
            <div class="glass-card" style="display:flex; justify-content:space-between; padding:10px;">
                <div style="display:flex; gap:10px;"><span style="width:20px;">${idx+1}</span> <span>${item.name}</span></div>
                <span style="color:#ffd700;">${item.count || item.score}</span>
            </div>`;
        });
    }
}
