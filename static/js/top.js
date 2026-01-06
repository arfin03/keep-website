async function renderTop(type) {
    // 1. Tampilkan Loading
    const list = document.getElementById('top-list');
    list.innerHTML = '<div style="text-align:center; color:rgba(255,255,255,0.5);">Loading Leaderboard...</div>';

    // 2. Update Visual Tab Active State
    const tabs = document.querySelectorAll('#view-top .tab');
    tabs.forEach(el => el.classList.remove('active'));
    if (event && event.target) {
        event.target.classList.add('active');
    } else {
        // Set default tab jika dipanggil manual pertama kali
        const activeTab = Array.from(tabs).find(t => t.innerText.toLowerCase() === type);
        if(activeTab) activeTab.classList.add('active');
    }

    // 3. Fetch Data dari Backend
    try {
        const res = await fetch(`/api/top?type=${type}`);
        const data = await res.json();
        
        // 4. Render List
        list.innerHTML = '';
        
        if (data.ok && data.items && data.items.length > 0) {
            data.items.forEach((item, index) => {
                // Tentukan Label Score
                const scoreLabel = type === 'charms' ? '💎 Charms' : '🃏 Collection';
                const scoreValue = item.score || item.count || item.charms || 0;

                // Fallback avatar & name
                const avatar = item.avatar && item.avatar !== 'null' ? item.avatar : '/static/default.png';
                const name = item.name || 'Traveler';
                const username = item.username ? String(item.username).replace(/^@/, '') : null;

                const html = `
                <div class="glass-card" style="display:flex; align-items:center; gap:12px; padding:12px;">
                    <!-- Rank -->
                    <div style="font-weight:bold; font-family:var(--font-accent); width:30px; font-size:18px; color:var(--accent); text-align:center;">
                        ${index + 1}
                    </div>
                    
                    <!-- Avatar -->
                    <img src="${avatar}" onerror="this.src='/static/default.png'" style="width:40px; height:40px; border-radius:50%; object-fit:cover; border:2px solid rgba(255,255,255,0.2);">
                    
                    <!-- Info (Name & Score) -->
                    <div style="flex:1;">
                        <div style="font-weight:700; font-size:15px; color:#fff;">
                            ${name}
                            ${username ? `<span style="font-weight:400; font-size:12px; opacity:0.75; margin-left:6px;">@${username}</span>` : ''}
                        </div>
                        <div style="font-size:12px; opacity:0.7; margin-top:2px; display:flex; justify-content:space-between; align-items:center;">
                            <span>${scoreLabel}</span> 
                            <span style="color:#ffd700; font-weight:bold; font-size:14px;">${Number(scoreValue).toLocaleString()}</span>
                        </div>
                    </div>
                </div>
                `;
                list.innerHTML += html;
            });
        } else {
            list.innerHTML = `
            <div style="text-align:center; padding:20px; color:rgba(255,255,255,0.5);">
                <i class="fa-solid fa-trophy" style="font-size:40px; margin-bottom:10px; display:block;"></i>
                <p>Belum ada data Top ${type}.</p>
            </div>`;
        }
    } catch (e) {
        console.error("Error renderTop:", e);
        list.innerHTML = '<div style="text-align:center; color:red;">Error loading top list.</div>';
    }
}
