import os
import io
import json
import base64
import traceback
from datetime import datetime

from flask import Flask, request, jsonify, render_template, Response
from bson import ObjectId

# optional imports that might fail in some envs
try:
    from pymongo import MongoClient
except Exception as e:
    MongoClient = None
    print("[WARN] pymongo import failed:", e)

try:
    import redis
except Exception as e:
    redis = None
    print("[WARN] redis import failed:", e)

try:
    import qrcode
except Exception as e:
    qrcode = None
    print("[WARN] qrcode import failed:", e)


# ================= CONFIG =================

API_ID = int(os.getenv('API_ID', 123456))
API_HASH = os.getenv('API_HASH', "")

MONGO_URI = os.getenv(
    'MONGO_URI',
    "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority"
)

MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 13380))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

# ================= APP =================

app = Flask(__name__, static_folder='static', template_folder='templates')

# ================= DB (lazy/defensive init) =================

def safe_mongo_client(uri):
    if MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        try:
            client.admin.command('ping')
        except Exception:
            print("[WARN] mongo ping failed (continuing without live mongo):", uri)
        return client
    except Exception as e:
        print("[ERROR] creating MongoClient failed:", e)
        return None

def safe_redis_client(host, port, password):
    if redis is None:
        return None
    try:
        r = redis.Redis(host=host, port=port, password=password, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
        try:
            r.ping()
        except Exception:
            print("[WARN] redis ping failed (continuing without live redis)")
        return r
    except Exception as e:
        print("[ERROR] creating Redis client failed:", e)
        return None

market_client = safe_mongo_client(MARKET_DB_URL)
waifu_client = safe_mongo_client(MONGO_URL_WAIFU)
husband_client = safe_mongo_client(MONGO_URL_HUSBAND)
r = safe_redis_client(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

def get_collection(client, dbname, collname):
    try:
        if client is None:
            return None
        return client[dbname][collname]
    except Exception as e:
        print("[WARN] get_collection error:", e)
        return None

market_db = market_client['market_p2p'] if market_client is not None else None
user_settings_coll = get_collection(market_client, 'market_p2p', 'user_settings')

waifu_db = waifu_client['Character_catcher'] if waifu_client is not None else None
waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_lmaoooo')

husband_db = husband_client['Character_catcher'] if husband_client is not None else None
husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_lmaoooo')

registered_users = None
try:
    if market_client is not None:
        registered_users = market_client['Character_catcher']['registered_users']
except Exception as e:
    print("[WARN] registered_users collection not available:", e)
    registered_users = None

# ================= HELPERS =================

DEFAULT_AVATAR = 'https://picsum.photos/200'
DEFAULT_NAME = 'Traveler'

def serialize_mongo(obj):
    if isinstance(obj, list):
        return [serialize_mongo(i) for i in obj]
    if isinstance(obj, dict):
        return {
            k: serialize_mongo(str(v) if isinstance(v, ObjectId) else v)
            for k, v in obj.items()
        }
    return obj

def ensure_user_profile(uid, first_name=None, username=None, avatar=None):
    if uid is None:
        return None
    uid_str = str(uid)
    try:
        if registered_users is None:
            return {'user_id': uid_str, 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR}
        update = {}
        if first_name is not None:
            update['firstname'] = first_name
        if username is not None:
            update['username'] = username
        if avatar is not None:
            update['photo_url'] = avatar
        if update:
            update['user_id'] = uid_str
            registered_users.update_one({'user_id': uid_str}, {'$set': update}, upsert=True)
        else:
            registered_users.update_one(
                {'user_id': uid_str},
                {'$setOnInsert': {'user_id': uid_str, 'firstname': DEFAULT_NAME, 'photo_url': DEFAULT_AVATAR}},
                upsert=True
            )
        return registered_users.find_one({'user_id': uid_str})
    except Exception as e:
        print("[ERROR] ensure_user_profile failed:", e)
        return {'user_id': uid_str, 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR}

def get_charms(uid):
    try:
        if r is None:
            return 0
        return int(r.hget(f"user:{uid}", "charm") or 0)
    except Exception:
        return 0

def update_charms(uid, amt, typ=None):
    try:
        if r is None:
            print("[WARN] update_charms called but redis not available")
            return False
        r.hincrby(f"user:{uid}", "charm", amt)
        current = get_charms(uid)
        r.zadd('leaderboard:charms', {str(uid): current})
        if typ and str(typ).lower() in ('waifu', 'husband'):
            r.zadd(f'leaderboard:charms:{str(typ).lower()}', {str(uid): current})
        tx = {
            "type": "charm_change",
            "amount": amt,
            "title": "Charm update",
            "detail": "",
            "ts": datetime.utcnow().timestamp()
        }
        try:
            r.lpush(f"user:{uid}:txs", json.dumps(tx))
            r.ltrim(f"user:{uid}:txs", 0, 99)
        except Exception:
            pass
        try:
            payload = json.dumps({"user_id": str(uid), "charms": current, "type": typ})
            r.publish('charms_updates', payload)
        except Exception:
            pass
        return True
    except Exception as e:
        print("[ERROR] update_charms failed:", e)
        return False

def log_tx(uid, t_type, amt, title, detail=""):
    try:
        if r is None:
            return
        tx = {
            "type": t_type,
            "amount": amt,
            "title": title,
            "detail": detail,
            "ts": datetime.utcnow().timestamp()
        }
        r.lpush(f"user:{uid}:txs", json.dumps(tx))
        r.ltrim(f"user:{uid}:txs", 0, 99)
    except Exception as e:
        print("[WARN] log_tx failed:", e)

# ================= ROUTES =================

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/user_info')
def api_user_info():
    uid = request.args.get('user_id')
    firstname = request.args.get('firstname')
    username = request.args.get('username')
    avatar = request.args.get('avatar')

    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400

    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)
    return jsonify({
        "ok": True,
        "id": str(uid),
        "name": (firstname or (doc.get('firstname') if isinstance(doc, dict) else DEFAULT_NAME) or DEFAULT_NAME),
        "username": (username or (doc.get('username') if isinstance(doc, dict) else None)),
        "avatar": (avatar or (doc.get('photo_url') if isinstance(doc, dict) else DEFAULT_AVATAR) or DEFAULT_AVATAR),
        "balance": get_charms(uid)
    })

@app.route('/api/update_profile', methods=['POST', 'GET'])
def api_update_profile():
    data = request.get_json(silent=True) or request.values
    uid = data.get('user_id')
    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    firstname = data.get('firstname')
    username = data.get('username')
    avatar = data.get('avatar')
    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)
    return jsonify({"ok": True, "user": serialize_mongo(doc) if isinstance(doc, dict) else doc})

@app.route('/api/my_collection')
def api_my_collection():
    uid = request.args.get('user_id')
    db_type = request.args.get('type', 'waifu')
    users_coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    try:
        if users_coll is None:
            return jsonify({"ok": True, "items": []})
        user_doc = (users_coll.find_one({'id': str(uid)}) or users_coll.find_one({'id': int(uid)}))
        if not user_doc:
            return jsonify({"ok": True, "items": []})
        items = (user_doc.get('characters') or user_doc.get('waifu') or user_doc.get('husband') or user_doc.get('char') or [])
        items = serialize_mongo(items)
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        print("[MY_COLLECTION ERROR]", e)
        return jsonify({"ok": False, "items": [], "error": str(e)}), 500

@app.route('/api/history')
def api_history():
    uid = request.args.get('user_id')
    try:
        if r is None:
            return jsonify({"ok": True, "items": []})
        raw = r.lrange(f"user:{uid}:txs", 0, 50)
        return jsonify({"ok": True, "items": [json.loads(x) for x in raw]})
    except Exception as e:
        print("[HISTORY ERROR]", e)
        return jsonify({"ok": True, "items": []})

@app.route('/api/qr_code')
def api_qr_code():
    uid = request.args.get('user_id')
    if qrcode is None:
        return jsonify({"ok": False, "error": "qrcode library not available"}), 500
    qr = qrcode.QRCode(box_size=8, border=3)
    qr.add_data(str(uid))
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return jsonify({"ok": True, "image_b64": base64.b64encode(buf.getvalue()).decode()})

# ================= LEADERBOARD / TOP =================

@app.route('/api/top')
def api_top():
    try:
        limit = int(request.args.get('limit', 100))
        if limit <= 0 or limit > 100:
            limit = 100

        typ = (request.args.get('type') or '').lower()
        if typ in ('waifu', 'husband'):
            key = f'leaderboard:charms:{typ}'
        else:
            key = 'leaderboard:charms'

        raw = []
        try:
            if r is not None:
                raw = r.zrevrange(key, 0, limit - 1, withscores=True)
        except Exception as e:
            print("[WARN] reading redis leaderboard failed:", e)

        if not raw and key != 'leaderboard:charms':
            try:
                if r is not None:
                    raw = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            except Exception as e:
                print("[WARN] reading global leaderboard failed:", e)

        if not raw:
            candidates = []
            try:
                if registered_users is not None:
                    for u in registered_users.find({}, {'user_id': 1, 'firstname': 1, 'username': 1, 'photo_url': 1}):
                        uid = u.get('user_id')
                        if not uid:
                            continue
                        charms = get_charms(uid)
                        if charms > 0:
                            candidates.append((str(uid), int(charms)))
                candidates.sort(key=lambda x: -x[1])
                raw = [(m, s) for m, s in candidates[:limit]]
            except Exception as e:
                print("[WARN] fallback building leaderboard failed:", e)

        items = []
        rank = 1
        for member, score in raw:
            uid = str(member)
            user_doc = {}
            try:
                if registered_users is not None:
                    user_doc = registered_users.find_one({'user_id': uid}) or {}
            except Exception:
                user_doc = {}
            items.append({
                'rank': rank,
                'user_id': uid,
                'name': user_doc.get('firstname', DEFAULT_NAME),
                'username': user_doc.get('username'),
                'avatar': user_doc.get('photo_url', DEFAULT_AVATAR),
                'charms': int(score)
            })
            rank += 1
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        print("[API_TOP_ERROR]", e)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e), "items": []}), 500

@app.route('/api/top_user')
def api_top_user():
    uid = request.args.get('user_id')
    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    try:
        rank = None
        if r is not None:
            try:
                rank = r.zrevrank('leaderboard:charms', str(uid))
            except Exception:
                rank = None
        charms = get_charms(uid)
        return jsonify({"ok": True, "user_id": str(uid), "rank": (int(rank)+1) if rank is not None else None, "charms": charms})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/api/rebuild_leaderboard')
def api_rebuild_leaderboard():
    try:
        typ = (request.args.get('type') or '').lower()
        dry = request.args.get('dry') == '1'
        key = f'leaderboard:charms:{typ}' if typ in ('waifu','husband') else 'leaderboard:charms'

        if r is None:
            return jsonify({"ok": False, "error": "redis not available"})

        pipe = r.pipeline()
        counts = 0
        entries = []
        if registered_users is None:
            return jsonify({"ok": False, "error": "registered_users collection not available"})
        for u in registered_users.find({}, {'user_id': 1}):
            uid = u.get('user_id')
            if not uid:
                continue
            charms = get_charms(uid)
            if charms > 0:
                counts += 1
                entries.append({'user_id': str(uid), 'charms': int(charms)})
                if not dry:
                    pipe.zadd(key, {str(uid): int(charms)})
        if not dry:
            pipe.execute()
        return jsonify({"ok": True, "key": key, "count": counts, "sample": entries[:20]})
    except Exception as e:
        print('[REBUILD_LEADERBOARD_ERROR]', e)
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

# ================= SSE =================

@app.route('/stream/charms')
def stream_charms():
    def event_stream():
        if r is None:
            while True:
                yield "data: {}\n\n"
        pubsub = None
        try:
            pubsub = r.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe('charms_updates')
            for message in pubsub.listen():
                if message is None:
                    continue
                if message.get('type') != 'message':
                    continue
                data = message.get('data')
                yield f"data: {data}\n\n"
        except GeneratorExit:
            try:
                if pubsub:
                    pubsub.close()
            except Exception:
                pass
        except Exception as e:
            print("[SSE ERROR]", e)
            try:
                if pubsub:
                    pubsub.close()
            except Exception:
                pass
    return Response(event_stream(), mimetype='text/event-stream')

# ================= RUN =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
