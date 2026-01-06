const CATEGORY_MAP = {
    '🏖': '🏖𝒐𝒖𝒎𝒎𝒆𝒓 🏖', '👘': '👘𝑲𝒊𝒎𝒐𝒏𝒐👘', '🧹': '🧹𝑴𝒂𝒊𝒅🧹',
    '🤝': '🤝𝐆𝐫𝐨𝐮𝐩🤝', '🎮': '🎮𝑮𝒂𝒎𝒆🎮'
};

async function renderMarket() {
    const type = document.querySelector('#view-market .tab.active').innerText.toLowerCase();
    const sort = document.getElementById('filter-sort').value;
    const rarity = document.getElementById('filter-rarity').value;
    
    const res = await fetch(`/api/market?type=${type}&sort=${sort}&rarity=${rarity}`);
    const data = await res.json();
    const list = document.getElementById('market-list');
    list.innerHTML = '';
    
    if(data.items) {
        data.items.forEach(item => {
            // Category Logic
            let categoryDisplay = '';
            for (const [emoji, name] of Object.entries(CATEGORY_MAP)) {
                if (item.name.includes(emoji)) {
                    categoryDisplay = `<div style="font-size:10px; margin-top:4px; background:rgba(0,0,0,0.5); border-radius:4px; display:inline-block; padding:2px 6px;">${name}</div>`;
                    break;
                }
            }

            list.innerHTML += `
            <div class="glass-card">
                <div style="display:flex; gap:12px;">
                    <img src="${item.image}" style="width:80px; height:100px; border-radius:8px; object-fit:cover;">
                    <div style="flex:1;">
                        <h3>${item.name}</h3>
                        <div style="color:#ffd700;">${item.rarity}</div>
                        ${categoryDisplay}
                        <div style="margin-top:8px; display:flex; justify-content:space-between;">
                            <span style="font-weight:bold;">${item.price} <i class="fa-solid fa-gem"></i></span>
                            <button class="glass-btn primary-btn" onclick="buyMarket('${item._id}', ${item.price})">Buy</button>
                        </div>
                        <div style="font-size:10px; opacity:0.6;">Stock: ${item.stock}</div>
                    </div>
                </div>
            </div>`;
        });
    }
}

function switchMarketTab(type) {
    state.marketTab = type;
    document.querySelectorAll('#view-market .tab').forEach(el => el.classList.remove('active'));
    event.target.classList.add('active');
    renderMarket();
}

function applyFilter() {
    document.getElementById('modal-filter').classList.remove('open');
    renderMarket();
}

async function buyMarket(id, price) {
    if(state.user.balance < price) return showToast("No Balance");
    if(confirm("Buy this item?")) {
        const res = await fetch('/api/buy_market', {
            method: 'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ user_id: state.user.id, item_id: id })
        });
        const data = await res.json();
        if(data.ok) {
            showToast("Success!");
            location.reload();
        } else { showToast(data.error); }
    }
}

// Init Swipe for Market Cards (Logic handled in confirm dialog, but if you want swipe):
// initSwipe('market-swipe-id', () => { ... })
