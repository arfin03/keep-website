const translations = {
    en: {
        "nav.home": "Home", "nav.market": "Market", "nav.p2p": "P2P", "nav.friends": "Friends",
        "nav.top": "Top", "nav.settings": "Settings", "nav.games": "Games", "nav.send": "Send", "nav.receive": "Receive",
        "market.buy": "Buy", "market.stock": "Stock", "p2p.sell": "Sell Character",
        "send.title": "Send Charms", "send.scan": "Scan QR", "receive.title": "Receive Charms",
        "profile.my_chars": "My Collection", "settings.lang": "Language", "notifs.title": "Notifications",
        "top.waifu": "Waifu", "top.husband": "Husband", "top.charms": "Charms"
    },
    idn: {
        "nav.home": "Beranda", "nav.market": "Pasar", "nav.p2p": "P2P", "nav.friends": "Teman",
        "nav.top": "Peringkat", "nav.settings": "Pengaturan", "nav.games": "Permainan", "nav.send": "Kirim", "nav.receive": "Terima",
        "market.buy": "Beli", "market.stock": "Stok", "p2p.sell": "Jual Karakter",
        "send.title": "Kirim Charms", "send.scan": "Scan QR", "receive.title": "Terima Charms",
        "profile.my_chars": "Koleksi Saya", "settings.lang": "Bahasa", "notifs.title": "Notifikasi",
        "top.waifu": "Waifu", "top.husband": "Husband", "top.charms": "Charms"
    },
    hi: { /* India */ "nav.home": "घर", "nav.market": "बाज़ार", "settings.lang": "भाषा" },
    my: { /* Myanmar */ "nav.home": "ပင်မ", "nav.market": "ဈေး", "settings.lang": "ဘာသာစကား" },
    ar: { /* Arabic */ "nav.home": "الرئيسية", "nav.market": "السوق", "settings.lang": "اللغة" }
};
let currentLang = 'en';

function setLanguage(lang) {
    currentLang = lang;
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        if (translations[lang] && translations[lang][key]) {
            el.innerText = translations[lang][key];
        } else { el.innerText = translations['en'][key] || key; }
    });
    if (lang === 'ar') document.documentElement.setAttribute('dir', 'rtl');
    else document.documentElement.setAttribute('dir', 'ltr');
}
