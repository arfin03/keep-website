const translations = {
    en: {
        "nav.home": "Home", "nav.market": "Market", "nav.p2p": "P2P",
        "nav.shop": "Shop", "nav.friends": "Friends", "nav.top": "Top",
        "nav.settings": "Settings", "nav.games": "Games",
        "market.buy": "Buy", "market.stock_left": "Left",
        "p2p.sell": "Sell Character", "p2p.trading": "P2P Trading",
        "send.title": "Send Charms", "send.scan": "Scan QR", "send.manual": "Manual Input",
        "receive.title": "Receive Charms", "receive.desc": "Scan to send me charms",
        "profile.my_chars": "My Collection", "profile.close": "Close",
        "settings.lang": "Language", "settings.bgm": "Background Music",
        "settings.upload_bgm": "Upload BGM", "settings.theme": "Theme",
        "friends.add": "Add Friend", "friends.accept": "Requests", "friends.chat": "Chat",
        "modal.sell_title": "Sell Your Character",
        "form.price": "Price", "form.qty": "Qty", "form.desc": "Description",
        "rarity.common": "Common", "rarity.rare": "Rare", "rarity.legend": "Legendary"
    },
    idn: {
        "nav.home": "Beranda", "nav.market": "Pasar", "nav.p2p": "P2P",
        "nav.shop": "Toko", "nav.friends": "Teman", "nav.top": "Peringkat",
        "nav.settings": "Pengaturan", "nav.games": "Permainan",
        "market.buy": "Beli", "market.stock_left": "Sisa",
        "p2p.sell": "Jual Karakter", "p2p.trading": "Pasar P2P",
        "send.title": "Kirim Charms", "send.scan": "Scan QR", "send.manual": "Input Manual",
        "receive.title": "Terima Charms", "receive.desc": "Scan untuk kirim saya charms",
        "profile.my_chars": "Koleksi Saya", "profile.close": "Tutup",
        "settings.lang": "Bahasa", "settings.bgm": "Musik Latar",
        "settings.upload_bgm": "Upload Musik", "settings.theme": "Tema",
        "friends.add": "Tambah Teman", "friends.accept": "Permintaan", "friends.chat": "Obrolan",
        "modal.sell_title": "Jual Karakter Anda",
        "form.price": "Harga", "form.qty": "Jumlah", "form.desc": "Deskripsi",
        "rarity.common": "Umum", "rarity.rare": "Langka", "rarity.legend": "Legendaris"
    },
    // Tambahkan logika serupa untuk India, Myanmar, Arabic (AR perlu dir="rtl")
    hi: { /* Hindi placeholders */ "nav.home": "घर", "nav.market": "बाज़ार", "settings.lang": "भाषा" },
    my: { /* Myanmar placeholders */ "nav.home": "ပင်မ", "nav.market": "ဈေး", "settings.lang": "ဘာသာစကား" },
    ar: { /* Arabic placeholders */ "nav.home": "الرئيسية", "nav.market": "السوق", "settings.lang": "اللغة" }
};

let currentLang = 'en';

function setLanguage(lang) {
    currentLang = lang;
    const elements = document.querySelectorAll('[data-i18n]');
    elements.forEach(el => {
        const key = el.getAttribute('data-i18n');
        if (translations[lang] && translations[lang][key]) {
            el.innerText = translations[lang][key];
        } else {
            el.innerText = translations['en'][key] || key; // Fallback
        }
    });

    // Handle RTL
    if (lang === 'ar') {
        document.documentElement.setAttribute('dir', 'rtl');
    } else {
        document.documentElement.setAttribute('dir', 'ltr');
    }
}
