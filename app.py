import os
import io
import json
import logging
import base64
from datetime import datetime
from time import sleep

from flask import Flask, request, jsonify, render_template, Response
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
    """Upsert minimal profile info into registered_users collection."""
    if uid is None:
        return None
    uid_str = str(uid)
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
        # ensure user exists with default fields
        registered_users.update_one(
            {'user_id': uid_str},
            {'$setOnInsert': {'user_id': uid_str, 'firstname': DEFAULT_NAME, 'photo_url': DEFAULT_AVATAR}},
            upsert=True
        )
    return registered_users.find_one({'user_id': uid_str})


def get_charms(uid):
    try:
        return int(r.hget(f"user:{uid}", "charm") or 0)
    except:
        return 0


def update_charms(uid, amt):
    """Increment charms and update leaderboard + publish real-time update via Redis pubsub."""
    try:
        r.hincrby(f"user:{uid}", "charm", amt)
        current = get_charms(uid)
        # update sorted set for leaderboard (score = charms)
        r.zadd('leaderboard:charms', {str(uid): current})
        # log tx
        tx = {
            "type": "charm_change",
            "amount": amt,
            "title": "Charm update",
            "detail": "",
            "ts": datetime.utcnow().timestamp()
        }
        r.lpush(f"user:{uid}:txs", json.dumps(tx))
        r.ltrim(f"user:{uid}:txs", 0, 99)
        # publish real-time event for any listening clients
        try:
            payload = json.dumps({"user_id": str(uid), "charms": current})
            r.publish('charms_updates', payload)
        except Exception:
            pass
        return True
    except Exception as e:
        print('[UPDATE_CHARMS ERROR]', e)
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
    """Return minimal profile + charm balance. Also ensures user profile exists.

    You can pass optional query params: firstname, username, avatar to update profile on the fly.
    Useful for the miniapp to call on page load so server has the latest info.
    """
    uid = request.args.get('user_id')
    firstname = request.args.get('firstname')
    username = request.args.get('username')
    avatar = request.args.get('avatar')

    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400

    # make sure the user doc exists and store provided profile fields
    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)

    return jsonify({
        "ok": True,
        "id": str(uid),
        "name": (firstname or (doc.get('firstname') if doc else DEFAULT_NAME) or DEFAULT_NAME),
        "username": (username or (doc.get('username') if doc else None)),
        "avatar": (avatar or (doc.get('photo_url') if doc else DEFAULT_AVATAR) or DEFAULT_AVATAR),
        "balance": get_charms(uid)
    })


@app.route('/api/update_profile', methods=['POST', 'GET'])
def api_update_profile():
    """Upsert profile information. Accepts JSON body or query params.

    POST JSON: {user_id, firstname, username, avatar}
    """
    data = request.get_json(silent=True) or request.values
    uid = data.get('user_id')
    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    firstname = data.get('firstname')
    username = data.get('username')
    avatar = data.get('avatar')
    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)
    return jsonify({"ok": True, "user": serialize_mongo(doc)})


@app.route('/api/my_collection')
def api_my_collection():
    # unchanged from your stable version
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


# ================= LEADERBOARD / TOP ENDPOINTS =================

@app.route('/api/top')
def api_top():
    """Return top users by charms. Query params: limit (default 100).

    Response: {ok: True, items: [{rank, user_id, name, username, avatar, charms}, ...]}
    """
    try:
        limit = int(request.args.get('limit', 100))
        if limit <= 0 or limit > 100:
            limit = 100

        raw = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
        items = []
        rank = 1
        for member, score in raw:
            uid = str(member)
            user_doc = registered_users.find_one({'user_id': uid}) or {}
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
        print('[API_TOP_ERROR]', e)
        return jsonify({"ok": False, "error": str(e), "items": []}), 500


@app.route('/api/top_user')
def api_top_user():
    """Return a single user's rank and charms. Query param: user_id"""
    uid = request.args.get('user_id')
    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    try:
        # zrevrank gives rank starting at 0 (highest score). Add 1 to make human-friendly.
        rank = r.zrevrank('leaderboard:charms', str(uid))
        charms = get_charms(uid)
        if rank is None:
            return jsonify({"ok": True, "user_id": str(uid), "rank": None, "charms": charms})
        return jsonify({"ok": True, "user_id": str(uid), "rank": int(rank) + 1, "charms": charms})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ================= SSE / REAL-TIME STREAM =================

@app.route('/stream/charms')
def stream_charms():
    """Server-Sent Events endpoint that streams charm updates in real-time.

    Clients should connect with an EventSource to this URL. The server will forward messages
    published to Redis channel 'charms_updates'. Each message is a JSON payload: {user_id, charms}.
    """
    def event_stream():
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe('charms_updates')
        try:
            for message in pubsub.listen():
                if message is None:
                    continue
                if message['type'] != 'message':
                    continue
                data = message['data']
                # data is a string because decode_responses=True
                yield f"data: {data}\n\n"
        except GeneratorExit:
            try:
                pubsub.close()
            except:
                pass
        except Exception as e:
            print('[SSE ERROR]', e)
            try:
                pubsub.close()
            except:
                pass

    return Response(event_stream(), mimetype='text/event-stream')


# ================= RUN =================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
