// Theme Persistence
document.getElementById('theme-file').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if(file) {
        const reader = new FileReader();
        reader.onload = async (evt) => {
            const b64 = evt.target.result; // Data URI
            // Apply immediately
            document.getElementById('app-bg').style.backgroundImage = `url(${b64})`;
            // Save to DB
            await fetch('/api/update_settings', {
                method: 'POST', headers:{'Content-Type':'application/json'},
                body: JSON.stringify({ user_id: state.user.id, theme_b64: b64 })
            });
            showToast("Theme Saved!");
        };
        reader.readAsDataURL(file);
    }
});

// BGM Persistence
document.getElementById('bgm-file').addEventListener('change', async (e) => {
    const file = e.target.files[0];
    if(file) {
        const b64 = await new Promise(resolve => {
            const reader = new FileReader();
            reader.onload = (evt) => resolve(evt.target.result);
            reader.readAsDataURL(file);
        });
        
        // Save to DB
        await fetch('/api/update_settings', {
            method: 'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({ user_id: state.user.id, bgm_b64: b64 })
        });
        
        // Play
        new Audio(b64).play();
        showToast("BGM Saved!");
    }
});

// Language Change
document.getElementById('lang-select').addEventListener('change', async (e) => {
    const lang = e.target.value;
    setLanguage(lang);
    await fetch('/api/update_settings', {
        method: 'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ user_id: state.user.id, lang: lang })
    });
});
