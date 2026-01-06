Di bagian my collection sudah bagus sekarang kita perbaiki dulu di bagian lain untuk sekarang kita perbaiki di bagian top

Untuk app.py yang sudah di perbaiki di bagian my collection nya ini jadi nanti jangan di ubah lagi 

import os
import io
import json
import logging
import base64
from datetime import datetime

from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from bson import ObjectId
import redis
import qrcode

# ================= CONFIG =================

API_ID    = int(os.getenv('API_ID', 123456))
API_HASH  = os.getenv('API_HASH', "")

MONGO_URI = os.getenv(
    'MONGO_URI',
    "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority"
)

MARKET_DB_URL      = os.getenv('MARKET_DB_URL', MONGO_URI)
MONGO_URL_WAIFU    = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
MONGO_URL_HUSBAND  = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 13380))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

# ================= APP =================

app = Flask(__name__, static_folder='static', template_folder='templates')

# ================= DB =================

market_client = MongoClient(MARKET_DB_URL)
market_db = market_client['market_p2p']
user_settings_coll = market_db['user_settings']

waifu_client = MongoClient(MONGO_URL_WAIFU)
waifu_db = waifu_client['Character_catcher']
waifu_users_coll = waifu_db['user_collection_lmaoooo']

husband_client = MongoClient(MONGO_URL_HUSBAND)
husband_db = husband_client['Character_catcher']
husband_users_coll = husband_db['user_collection_lmaoooo']

registered_users = market_client['Character_catcher']['registered_users']

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

# ================= HELPERS =================

def serialize_mongo(obj):
    if isinstance(obj, list):
        return [serialize_mongo(i) for i in obj]
    if isinstance(obj, dict):
        return {
            k: serialize_mongo(str(v) if isinstance(v, ObjectId) else v)
            for k, v in obj.items()
        }
    return obj


def get_charms(uid):
    try:
        return int(r.hget(f"user:{uid}", "charm") or 0)
    except:
        return 0


def update_charms(uid, amt):
    try:
        r.hincrby(f"user:{uid}", "charm", amt)
        r.zadd('leaderboard:charms', {str(uid): get_charms(uid)})
        return True
    except:
        return False


def log_tx(uid, t_type, amt, title, detail=""):
    try:
        tx = {
            "type": t_type,
            "amount": amt,
            "title": title,
            "detail": detail,
            "ts": datetime.utcnow().timestamp()
        }
        r.lpush(f"user:{uid}:txs", json.dumps(tx))
        r.ltrim(f"user:{uid}:txs", 0, 99)
    except:
        pass

# ================= ROUTES =================

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/user_info')
def api_user_info():
    uid = request.args.get('user_id')
    u = registered_users.find_one({'user_id': str(uid)}) or {}
    return jsonify({
        "ok": True,
        "id": uid,
        "name": u.get('firstname', 'Traveler'),
        "avatar": u.get('photo_url', 'https://picsum.photos/200'),
        "balance": get_charms(uid)
    })


# ================= FIXED MY COLLECTION =================

@app.route('/api/my_collection')
def api_my_collection():
    uid = request.args.get('user_id')
    db_type = request.args.get('type', 'waifu')

    users_coll = husband_users_coll if db_type == 'husband' else waifu_users_coll

    try:
        user_doc = (
            users_coll.find_one({'id': str(uid)}) or
            users_coll.find_one({'id': int(uid)})
        )

        if not user_doc:
            return jsonify({"ok": True, "items": []})

        items = (
            user_doc.get('characters') or
            user_doc.get('waifu') or
            user_doc.get('husband') or
            user_doc.get('char') or
            []
        )

        items = serialize_mongo(items)

        return jsonify({
            "ok": True,
            "items": items
        })

    except Exception as e:
        print("[MY_COLLECTION ERROR]", e)
        return jsonify({
            "ok": False,
            "items": [],
            "error": str(e)
        }), 500


@app.route('/api/history')
def api_history():
    uid = request.args.get('user_id')
    try:
        raw = r.lrange(f"user:{uid}:txs", 0, 50)
        return jsonify({"ok": True, "items": [json.loads(x) for x in raw]})
    except:
        return jsonify({"ok": True, "items": []})


@app.route('/api/qr_code')
def api_qr_code():
    uid = request.args.get('user_id')
    qr = qrcode.QRCode(box_size=8, border=3)
    qr.add_data(str(uid))
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buf = io.BytesIO()
    img.save(buf, format="PNG")

    return jsonify({
        "ok": True,
        "image_b64": base64.b64encode(buf.getvalue()).decode()
    })


# ================= RUN =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)


