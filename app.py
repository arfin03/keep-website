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
    "MONGO_URI",
    "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority"
)

MARKET_DB_URL = os.getenv("MARKET_DB_URL", MONGO_URI)
MONGO_URL_WAIFU = os.getenv("MONGO_URL_WAIFU", MONGO_URI)
MONGO_URL_HUSBAND = os.getenv("MONGO_URL_HUSBAND", MONGO_URI)

REDIS_HOST = os.getenv("redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com")
REDIS_PORT = int(os.getenv("REDIS_PORT", 13380))
REDIS_PASSWORD = os.getenv("NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

DEFAULT_AVATAR = "https://picsum.photos/200"

# ================= APP =================

app = Flask(__name__, static_folder="static", template_folder="templates")

# ================= DB =================

mongo_client = MongoClient(MONGO_URI)

market_db = mongo_client["market_p2p"]
registered_users = mongo_client["Character_catcher"]["registered_users"]

waifu_users_coll = mongo_client["Character_catcher"]["user_collection_lmaoooo"]
husband_users_coll = mongo_client["Character_catcher"]["user_collection_lmaoooo"]

r = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True
)

# ================= UTIL =================

def serialize_mongo(obj):
    if isinstance(obj, list):
        return [serialize_mongo(i) for i in obj]
    if isinstance(obj, dict):
        return {
            k: serialize_mongo(str(v) if isinstance(v, ObjectId) else v)
            for k, v in obj.items()
        }
    return obj

# ================= REGIS / PROFILE =================
# GA NYENTUH CHARM

def ensure_user_profile(uid, firstname, avatar):
    """
    Simpan profile ke Redis TANPA overwrite & TANPA sentuh charm
    """
    if not r:
        return

    key = f"user:profile:{uid}"
    if r.exists(key):
        return

    r.hset(
        key,
        mapping={
            "uid": str(uid),
            "firstname": firstname,
            "avatar": avatar
        }
    )

# ================= CHARMS (AMAN TOTAL) =================

def get_charms(uid):
    """
    RULE:
    1. Kalau charm ada → pakai
    2. Kalau ga ada → coba recover dari leaderboard
    3. Kalau dua-duanya ga ada → 0 (TANPA overwrite)
    """
    if not r:
        return 0

    user_key = f"user:{uid}"

    try:
        charm = r.hget(user_key, "charm")
        if charm is not None:
            return int(charm)

        # recover dari leaderboard
        score = r.zscore("leaderboard:charms", str(uid))
        if score is not None:
            r.hsetnx(user_key, "charm", int(score))
            return int(score)

        return 0
    except:
        return 0


def update_charms(uid, amount):
    """
    Nambah / kurang charm TANPA reset
    """
    if not r:
        return False

    r.hincrby(f"user:{uid}", "charm", amount)
    r.zadd("leaderboard:charms", {str(uid): get_charms(uid)})
    return True


def log_tx(uid, t_type, amount, title):
    if not r:
        return

    tx = {
        "type": t_type,
        "amount": amount,
        "title": title,
        "ts": datetime.utcnow().timestamp()
    }

    r.lpush(f"user:{uid}:txs", json.dumps(tx))
    r.ltrim(f"user:{uid}:txs", 0, 99)

# ================= ROUTES =================

@app.route("/")
def index():
    return render_template("index.html")

# -------- USER INFO (REGIS DI SINI) --------

@app.route("/api/user_info")
def api_user_info():
    uid = request.args.get("user_id")

    u = registered_users.find_one({"user_id": str(uid)}) or {}
    firstname = u.get("firstname", "Traveler")
    avatar = u.get("photo_url", DEFAULT_AVATAR)

    # REGIS PROFILE (AMAN)
    ensure_user_profile(uid, firstname, avatar)

    return jsonify({
        "ok": True,
        "id": uid,
        "name": firstname,
        "avatar": avatar,
        "balance": get_charms(uid)
    })

# -------- MY COLLECTION --------

@app.route("/api/my_collection")
def api_my_collection():
    uid = request.args.get("user_id")
    db_type = request.args.get("type", "waifu")

    coll = husband_users_coll if db_type == "husband" else waifu_users_coll

    user_doc = coll.find_one({"id": str(uid)}) or coll.find_one({"id": int(uid)})
    if not user_doc:
        return jsonify({"ok": True, "items": []})

    items = user_doc.get("characters", [])
    return jsonify({"ok": True, "items": serialize_mongo(items)})

# -------- HISTORY --------

@app.route("/api/history")
def api_history():
    uid = request.args.get("user_id")
    if not r:
        return jsonify({"ok": True, "items": []})

    raw = r.lrange(f"user:{uid}:txs", 0, 50)
    return jsonify({"ok": True, "items": [json.loads(x) for x in raw]})

# -------- QR --------

@app.route("/api/qr_code")
def api_qr_code():
    uid = request.args.get("user_id")

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

# ================= TOP (FINAL FIX) =================

@app.route("/api/top")
def api_top():
    type_ = request.args.get("type", "charms")

    # -------- TOP CHARMS --------
    if type_ == "charms" and r:
        tops = r.zrevrange("leaderboard:charms", 0, 99, withscores=True)
        res = []

        for uid, score in tops:
            profile = r.hgetall(f"user:profile:{uid}")
            res.append({
                "uid": uid,
                "name": profile.get("firstname", "Traveler"),
                "avatar": profile.get("avatar", DEFAULT_AVATAR),
                "score": int(score)
            })

        return jsonify({"ok": True, "items": res})

    # -------- TOP WAIFU / HUSBAND --------
    if type_ in ["waifu", "husband"]:
        coll = husband_users_coll if type_ == "husband" else waifu_users_coll

        pipeline = [
            {
                "$project": {
                    "uid": "$id",
                    "count": {
                        "$cond": [
                            {"$isArray": "$characters"},
                            {"$size": "$characters"},
                            0
                        ]
                    }
                }
            },
            {"$group": {"_id": "$uid", "count": {"$max": "$count"}}},
            {"$sort": {"count": -1}},
            {"$limit": 100}
        ]

        raw = list(coll.aggregate(pipeline))
        res = []

        for item in raw:
            uid = item["_id"]
            profile = r.hgetall(f"user:profile:{uid}")
            res.append({
                "uid": uid,
                "name": profile.get("firstname", "Traveler"),
                "avatar": profile.get("avatar", DEFAULT_AVATAR),
                "count": item.get("count", 0)
            })

        return jsonify({"ok": True, "items": res})

    return jsonify({"ok": True, "items": []})

# ================= RUN =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
