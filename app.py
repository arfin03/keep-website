import os
import io
import json
import base64
from datetime import datetime

from flask import Flask, request, jsonify, render_template
from pymongo import MongoClient
from bson import ObjectId
import redis
import qrcode

# ================= CONFIG =================

MONGO_URI = os.getenv(
    'MONGO_URI',
    "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority"
)

MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')

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


def save_user_profile(uid, firstname, avatar):
    if not r:
        return
    r.hset(
        f"user:profile:{uid}",
        mapping={
            "uid": str(uid),
            "firstname": firstname,
            "avatar": avatar
        }
    )


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

    name = u.get('firstname', 'Traveler')
    avatar = u.get('photo_url', 'https://picsum.photos/200')

    save_user_profile(uid, name, avatar)

    return jsonify({
        "ok": True,
        "id": uid,
        "name": name,
        "avatar": avatar,
        "balance": get_charms(uid)
    })


@app.route('/api/my_collection')
def api_my_collection():
    uid = request.args.get('user_id')
    db_type = request.args.get('type', 'waifu')

    users_coll = husband_users_coll if db_type == 'husband' else waifu_users_coll

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
        []
    )

    return jsonify({
        "ok": True,
        "items": serialize_mongo(items)
    })


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


# ================= TOP (FIXED) =================

@app.route('/api/top')
def api_top():
    type_ = request.args.get('type', 'charms')

    # ===== TOP CHARMS =====
    if type_ == 'charms' and r is not None:
        tops = r.zrevrange('leaderboard:charms', 0, 99, withscores=True)
        res = []

        for uid, score in tops:
            profile = r.hgetall(f"user:profile:{uid}")
            res.append({
                "uid": uid,
                "name": profile.get("firstname", "Traveler"),
                "avatar": profile.get("avatar", "https://picsum.photos/200"),
                "score": int(score)
            })

        return jsonify({"ok": True, "items": res})

    # ===== TOP WAIFU / HUSBAND =====
    if type_ in ['waifu', 'husband']:
        coll = husband_users_coll if type_ == 'husband' else waifu_users_coll
        if coll is None:
            return jsonify({"ok": True, "items": []})

        pipeline = [
            {
                "$project": {
                    "name": "$firstname",
                    "count": {
                        "$cond": [
                            {"$isArray": "$characters"},
                            {"$size": "$characters"},
                            0
                        ]
                    },
                    "photo_url": "$photo_url"
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 100}
        ]

        tops = list(coll.aggregate(pipeline))
        tops = serialize_mongo(tops)

        return jsonify({
            "ok": True,
            "items": [
                {
                    "name": t.get("name", "Traveler"),
                    "count": t.get("count", 0),
                    "avatar": t.get("photo_url") or "https://picsum.photos/200"
                }
                for t in tops
            ]
        })

    return jsonify({"ok": True, "items": []})


# ================= RUN =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
