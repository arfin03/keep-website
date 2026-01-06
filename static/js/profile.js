// ================= HISTORY =================

async function renderHistory() {
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
}

// ================= COLLECTION (FIXED) =================

async function renderCollection(type) {
    const grid = document.getElementById('collection-grid');
    if (!grid) {
        console.error("collection-grid not found");
        return;
    }

    // Handle tab active
    const tabs = document.querySelectorAll('#view-collection .tab');
    tabs.forEach(el => el.classList.remove('active'));
    if (event && event.target) {
        event.target.classList.add('active');
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
            // 🔥 FIX UTAMA DI SINI
            const imgUrl = char.img_url || 'https://picsum.photos/300';

            grid.innerHTML += `
                <div class="char-card-small">
                    <img 
                        src="${imgUrl}" 
                        class="char-img" 
                        loading="lazy"
                        onerror="this.src='https://picsum.photos/300'"
                    >
                    <div class="char-name">
                        ${char.name || 'Unknown'}
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
