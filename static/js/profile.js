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
    // 1. Tentukan Grid di HTML
    const grid = document.getElementById('collection-grid');
    if (!grid) {
        console.error("Grid collection-grid tidak ditemukan!");
        return;
    }

    // 2. Ubah state Tab (Visual)
    const tabs = document.querySelectorAll('#view-collection .tab');
    tabs.forEach(el => el.classList.remove('active'));
    if (event && event.target) {
        event.target.classList.add('active');
    } else {
        // Default set active jika manual dipanggil
        if(tabs.length > 0) {
             // Cari tab yang sesuai tipe
             for(let t of tabs) {
                 if(t.innerText.toLowerCase() === type) t.classList.add('active');
             }
        }
    }

    // 3. Tampilkan Loading
    grid.innerHTML = '<p style="text-align:center; opacity:0.6;">Loading Collection...</p>';

    // 4. Fetch Data dari Backend
    const res = await fetch(`/api/my_collection?user_id=${state.user.id}&type=${type}`);
    
    try {
        const data = await res.json();
        
        // 5. Bersihkan Grid
        grid.innerHTML = '';

        if (data.ok && data.items) {
            if (data.items.length === 0) {
                grid.innerHTML = '<div style="text-align:center; grid-column: 1/-1; color:rgba(255,255,255,0.5);">No characters yet.</div>';
                return;
            }

            // 6. Render Kartu (Gambar + Nama)
            data.items.forEach(char => {
                // Buat elemen HTML string
                const cardHTML = `
                    <div class="char-card-small">
                        <img src="${char.image}" class="char-img">
                        <div class="char-name">${char.name}</div>
                    </div>
                `;
                grid.innerHTML += cardHTML;
            });
        } else {
            grid.innerHTML = '<div style="text-align:center;">Failed to load collection.</div>';
        }
    } catch (e) {
        console.error("Error loading collection:", e);
        grid.innerHTML = '<div style="text-align:center;">Error loading collection.</div>';
    }
}
