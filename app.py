# app.py
import os
import io
import json
import base64
import traceback
from datetime import datetime

from flask import Flask, request, jsonify, render_template, Response
from bson import ObjectId

# optional imports
try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

try:
    import redis
except Exception:
    redis = None

try:
    import qrcode
except Exception:
    qrcode = None

# ================ CONFIG ================
MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 13380))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

DEFAULT_AVATAR = 'https://picsum.photos/200'
DEFAULT_NAME = 'Traveler'

# ================ APP ================
app = Flask(__name__, static_folder='static', template_folder='templates')

# ================ DB (defensive init) ================
def safe_mongo(uri):
    if MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        try:
            client.admin.command('ping')
        except Exception:
            pass
        return client
    except Exception:
        return None

def safe_redis(host, port, password):
    if redis is None:
        return None
    try:
        r = redis.Redis(host=host, port=port, password=password, decode_responses=True, socket_connect_timeout=5, socket_timeout=5)
        try:
            r.ping()
        except Exception:
            pass
        return r
    except Exception:
        return None

market_client = safe_mongo(MARKET_DB_URL)
waifu_client = safe_mongo(MONGO_URL_WAIFU)
husband_client = safe_mongo(MONGO_URL_HUSBAND)
r = safe_redis(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

def get_collection(client, dbname, collname):
    try:
        if client is None:
            return None
        return client[dbname][collname]
    except Exception:
        return None

# collections used
waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_lmaoooo')
husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_lmaoooo')
registered_users = None
if market_client is not None:
    try:
        registered_users = market_client['Character_catcher']['registered_users']
    except Exception:
        registered_users = None

# ================ HELPERS ================
def serialize_mongo(obj):
    if isinstance(obj, list):
        return [serialize_mongo(i) for i in obj]
    if isinstance(obj, dict):
        return {k: serialize_mongo(str(v) if isinstance(v, ObjectId) else v) for k, v in obj.items()}
    return obj

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
            return False
        r.hincrby(f"user:{uid}", "charm", amt)
        current = get_charms(uid)
        r.zadd('leaderboard:charms', {str(uid): current})
        if typ and str(typ).lower() in ('waifu', 'husband'):
            r.zadd(f'leaderboard:charms:{typ}', {str(uid): current})
        try:
            r.publish('charms_updates', json.dumps({"user_id": str(uid), "charms": current, "type": typ}))
        except Exception:
            pass
        return True
    except Exception:
        return False

def ensure_user_profile(uid, first_name=None, username=None, avatar=None):
    if uid is None:
        return None
    uid_str = str(uid)
    if registered_users is None:
        return {'user_id': uid_str, 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR}
    try:
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
            registered_users.update_one({'user_id': uid_str}, {'$setOnInsert': {'user_id': uid_str, 'firstname': DEFAULT_NAME, 'photo_url': DEFAULT_AVATAR}}, upsert=True)
        return registered_users.find_one({'user_id': uid_str})
    except Exception:
        return {'user_id': uid_str, 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR}

# helper: aggregate top from a mongo users collection
def build_top_from_users_coll(users_coll, limit=100):
    if users_coll is None:
        return []
    try:
        pipeline = [
            {"$addFields": {"character_count": {"$cond": {"if": {"$isArray": "$characters"}, "then": {"$size": "$characters"}, "else": 0}}}},
            {"$project": {"user_id": {"$ifNull": ["$id", "$user_id"]}, "first_name": 1, "username": 1, "character_count": 1}},
            {"$sort": {"character_count": -1}},
            {"$limit": limit}
        ]
        cursor = users_coll.aggregate(pipeline)
        return list(cursor)
    except Exception:
        return []

def fetch_profile_fallback(uid, users_coll=None):
    """
    Return a dict with keys: name, username, avatar.
    Priority:
      1) registered_users collection (photo_url, firstname, username)
      2) provided users_coll (waifu/husband user collection) fields (first_name, firstname, username)
      3) defaults
    """
    uid_str = str(uid)
    # try registered_users first
    if registered_users is not None:
        try:
            doc = registered_users.find_one({'user_id': uid_str})
            if doc:
                name = doc.get('firstname') or doc.get('first_name') or doc.get('name') or DEFAULT_NAME
                username = doc.get('username') or None
                avatar = doc.get('photo_url') or None
                return {'name': name, 'username': username, 'avatar': avatar}
        except Exception:
            pass
    # try users_coll if provided
    if users_coll is not None:
        try:
            # user docs in waifu/husband collections often use 'id' for user id
            doc = users_coll.find_one({'id': uid_str}) or users_coll.find_one({'user_id': uid_str}) or users_coll.find_one({'id': int(uid_str)}) if uid_str.isdigit() else None
            if doc:
                name = doc.get('first_name') or doc.get('firstname') or doc.get('name') or DEFAULT_NAME
                username = doc.get('username') or None
                # check some possible avatar fields if stored in that collection
                avatar = doc.get('photo_url') or doc.get('avatar') or doc.get('picture') or None
                return {'name': name, 'username': username, 'avatar': avatar}
        except Exception:
            pass
    # fallback default
    return {'name': DEFAULT_NAME, 'username': None, 'avatar': None}

# ================ ROUTES ================
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

@app.route('/api/my_collection')
def api_my_collection():
    uid = request.args.get('user_id')
    db_type = request.args.get('type', 'waifu')
    users_coll = husband_users_coll if db_type == 'husband' else waifu_users_coll
    try:
        if users_coll is None:
            return jsonify({"ok": True, "items": []})
        user_doc = users_coll.find_one({'id': str(uid)}) or users_coll.find_one({'id': int(uid)})
        if not user_doc:
            return jsonify({"ok": True, "items": []})
        items = user_doc.get('characters') or user_doc.get('waifu') or user_doc.get('husband') or user_doc.get('char') or []
        items = serialize_mongo(items)
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "items": [], "error": str(e)}), 500

@app.route('/api/top')
def api_top():
    """
    GET /api/top?type=waifu|husband&limit=50
    - tries Redis type-specific leaderboard first (leaderboard:charms:waifu)
    - fallback to global charms set
    - final fallback: aggregate from Mongo collection (count characters) like bot's logic
    """
    try:
        limit = int(request.args.get('limit', 100))
        if limit <= 0 or limit > 100:
            limit = 100
        typ = (request.args.get('type') or '').lower()

        # choose keys/collections per type
        if typ == 'waifu':
            redis_key = 'leaderboard:charms:waifu'
            users_coll = waifu_users_coll
        elif typ == 'husband':
            redis_key = 'leaderboard:charms:husband'
            users_coll = husband_users_coll
        else:
            redis_key = 'leaderboard:charms'
            users_coll = None

        raw = []
        try:
            if r is not None:
                raw = r.zrevrange(redis_key, 0, limit - 1, withscores=True)
        except Exception:
            raw = []

        # fallback to global charms if type-specific empty
        if not raw and redis_key != 'leaderboard:charms':
            try:
                if r is not None:
                    raw = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            except Exception:
                raw = []

        # final fallback: aggregate from users collection (count characters)
        agg_docs = None
        if not raw:
            if users_coll is not None:
                agg_docs = build_top_from_users_coll(users_coll, limit=limit)
                raw = []
                for doc in agg_docs:
                    uid = doc.get('user_id') or doc.get('id') or doc.get('user_id')
                    if not uid:
                        continue
                    raw.append((str(uid), int(doc.get('character_count') or 0)))
            else:
                # global fallback: if registered_users exists, try reading charms from Redis per user
                fallback = []
                if registered_users is not None:
                    try:
                        for u in registered_users.find({}, {'user_id': 1}):
                            uid = u.get('user_id')
                            if not uid:
                                continue
                            charms = get_charms(uid)
                            if charms > 0:
                                fallback.append((str(uid), int(charms)))
                        fallback.sort(key=lambda x: -x[1])
                        raw = fallback[:limit]
                    except Exception:
                        raw = []

        # build items with profile lookup in registered_users if present (or fallback to users_coll)
        items = []
        rank = 1
        for idx, (member, score) in enumerate(raw):
            uid = str(member)
            # prefer registered_users, else fallback to users_coll, else defaults
            profile = fetch_profile_fallback(uid, users_coll=users_coll)
            name = profile.get('name') or DEFAULT_NAME
            username = profile.get('username') or None
            avatar = profile.get('avatar') or None
            if not avatar:
                # if user hasn't logged in miniapp, avatar remains default
                avatar = DEFAULT_AVATAR
            # expose fields compatible with frontend
            items.append({
                'rank': rank,
                'user_id': uid,
                'name': name,
                'username': username,
                'avatar': avatar,
                # score & count for compatibility with various frontends
                'charms': int(score),
                'score': int(score),
                'count': int(score)
            })
            rank += 1

        return jsonify({"ok": True, "items": items})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e), "items": []}), 500

@app.route('/api/rebuild_leaderboard')
def api_rebuild_leaderboard():
    try:
        typ = (request.args.get('type') or '').lower()
        dry = request.args.get('dry') == '1'
        if typ == 'waifu':
            key = 'leaderboard:charms:waifu'
            users_coll = waifu_users_coll
        elif typ == 'husband':
            key = 'leaderboard:charms:husband'
            users_coll = husband_users_coll
        else:
            key = 'leaderboard:charms'
            users_coll = None

        if r is None:
            return jsonify({"ok": False, "error": "redis not available"})

        if users_coll is not None:
            agg = build_top_from_users_coll(users_coll, limit=10000)
            pipe = r.pipeline()
            count = 0
            for doc in agg:
                uid = doc.get('user_id')
                if not uid:
                    continue
                score = int(doc.get('character_count', 0))
                if score > 0:
                    count += 1
                    if not dry:
                        pipe.zadd(key, {str(uid): score})
            if not dry:
                pipe.execute()
            return jsonify({"ok": True, "key": key, "count": count, "sample": agg[:20]})
        else:
            # rebuild from registered_users charms values
            if registered_users is None:
                return jsonify({"ok": False, "error": "no source collection available"})
            pipe = r.pipeline()
            count = 0
            for u in registered_users.find({}, {'user_id': 1}):
                uid = u.get('user_id')
                if not uid:
                    continue
                c = get_charms(uid)
                if c > 0:
                    count += 1
                    if not dry:
                        pipe.zadd(key, {str(uid): int(c)})
            if not dry:
                pipe.execute()
            return jsonify({"ok": True, "key": key, "count": count})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

# SSE for charms updates
@app.route('/stream/charms')
def stream_charms():
    def event_stream():
        if r is None:
            while True:
                yield "data: {}\n\n"
        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe('charms_updates')
        try:
            for message in pubsub.listen():
                if message is None:
                    continue
                if message.get('type') != 'message':
                    continue
                data = message.get('data')
                yield f"data: {data}\n\n"
        except GeneratorExit:
            try:
                pubsub.close()
            except Exception:
                pass
    return Response(event_stream(), mimetype='text/event-stream')

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
