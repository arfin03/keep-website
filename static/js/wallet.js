async function renderQR() {
    const res = await fetch(`/api/qr_code?user_id=${state.user.id}`);
    const data = await res.json();
    if(data.ok) {
        document.getElementById('receive-qr').innerHTML = `<img src="data:image/png;base64,${data.image_b64}" style="border-radius:12px;">`;
    }
}

function openScanner() { 
    document.getElementById('camera-view').style.display = 'flex'; 
    // Init Camera here
}
function closeScanner() { 
    document.getElementById('camera-view').style.display = 'none'; 
}

function sendCharms() {
    const uid = document.getElementById('send-uid').value;
    const amt = document.getElementById('send-amt').value;
    if(!uid || !amt) return alert("Fill all fields");
    alert("Sending Logic (Mock): " + amt + " to " + uid);
}

// Init Swipe for Send
initSwipe('send-swipe-container', () => {
    alert("Swiped to send!");
});
