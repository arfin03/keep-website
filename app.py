# app.py (defensive boot + detailed init error endpoint)
import os
import io
import json
import base64
import traceback
from datetime import datetime
from typing import Any

from flask import Flask, request, jsonify, render_template, Response

# defensive imports
try:
    from pymongo import MongoClient
except Exception:
    MongoClient = None

try:
    from bson import ObjectId
except Exception:
    ObjectId = None

try:
    import redis
except Exception:
    redis = None

try:
    import qrcode
except Exception:
    qrcode = None

# helper to safely parse ints from env
def safe_int(v, default):
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default

# default values (can be overridden by env)
DEFAULT_AVATAR = os.getenv('DEFAULT_AVATAR', 'https://picsum.photos/200')
DEFAULT_NAME = os.getenv('DEFAULT_NAME', 'Traveler')

# Keep a container for init error (traceback string) if any happens during heavy init
_initialization_error = None

# Create Flask app early so Gunicorn can import module without failing.
app = Flask(__name__, static_folder='static', template_folder='templates')

# We'll attempt full initialization inside try/except so that import-time exceptions are captured
try:
    # ================ CONFIG (safe parsing) ================
    MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
    MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
    MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
    MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
    REDIS_PORT = safe_int(os.getenv('REDIS_PORT'), 13380)
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

    # ================ DB initialization helpers ================
    def safe_mongo(uri: str):
        if MongoClient is None:
            return None
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            # try ping but do not fail if ping errors
            try:
                client.admin.command('ping')
            except Exception:
                pass
            return client
        except Exception:
            return None

    def safe_redis(host: str, port: int, password: str):
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

    # init clients
    market_client = safe_mongo(MARKET_DB_URL)
    waifu_client = safe_mongo(MONGO_URL_WAIFU)
    husband_client = safe_mongo(MONGO_URL_HUSBAND)

    # fallback reuse market_client if the type-specific client is not available
    if waifu_client is None:
        waifu_client = market_client
    if husband_client is None:
        husband_client = market_client

    r = safe_redis(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD)

    def get_collection(client, dbname, collname):
        try:
            if client is None:
                return None
            return client[dbname][collname]
        except Exception:
            return None

    # collections (avoid truth-testing Collection objects)
    waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_lmaoooo')
    if waifu_users_coll is None:
        waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_waifu')

    husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_lmaoooo')
    if husband_users_coll is None:
        husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_husband')

    registered_users = None
    global_user_profiles_coll = None
    if market_client is not None:
        try:
            registered_users = market_client['Character_catcher']['registered_users']
        except Exception:
            registered_users = None
        try:
            global_user_profiles_coll = market_client['Character_catcher']['user_collection_lmoooo']
        except Exception:
            # tolerate slight naming differences
            try:
                global_user_profiles_coll = market_client['Character_catcher']['user_collection_lmaoooo']
            except Exception:
                global_user_profiles_coll = None

    # ================ HELPERS ================
    def serialize_mongo(obj: Any):
        if isinstance(obj, list):
            return [serialize_mongo(i) for i in obj]
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                try:
                    if ObjectId is not None and isinstance(v, ObjectId):
                        out[k] = str(v)
                    else:
                        out[k] = serialize_mongo(v)
                except Exception:
                    try:
                        out[k] = str(v)
                    except Exception:
                        out[k] = None
            return out
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
            # ensure keys only used when r available
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

    def _try_many_fields_for_avatar(doc):
        if not isinstance(doc, dict):
            return None
        avatar = None
        try:
            avatar = doc.get('photo_url') or doc.get('avatar') or doc.get('avatar_url') or doc.get('picture')
        except Exception:
            avatar = None
        profile = None
        try:
            profile = doc.get('profile') or doc.get('profile_info') or None
        except Exception:
            profile = None
        if isinstance(profile, dict):
            avatar = avatar or profile.get('avatar') or profile.get('photo') or profile.get('picture')
        return avatar

    def fetch_profile_fallback(uid, users_coll=None):
        if uid is None:
            return {'name': DEFAULT_NAME, 'username': None, 'avatar': None}
        uid_str = str(uid)
        # 1) registered_users
        if registered_users is not None:
            try:
                doc = registered_users.find_one({'user_id': uid_str}) or registered_users.find_one({'id': uid_str})
                if doc is None and uid_str.isdigit():
                    doc = registered_users.find_one({'id': int(uid_str)})
                if doc:
                    name = doc.get('firstname') or doc.get('first_name') or doc.get('name') or DEFAULT_NAME
                    username = doc.get('username') or doc.get('user_name') or None
                    avatar = _try_many_fields_for_avatar(doc) or None
                    return {'name': name, 'username': username, 'avatar': avatar}
            except Exception:
                pass
        # 2) global miniapps collection
        if global_user_profiles_coll is not None:
            try:
                doc = global_user_profiles_coll.find_one({'user_id': uid_str}) or global_user_profiles_coll.find_one({'id': uid_str})
                if doc is None and uid_str.isdigit():
                    doc = global_user_profiles_coll.find_one({'id': int(uid_str)})
                if doc:
                    name = doc.get('firstname') or doc.get('first_name') or doc.get('name') or doc.get('displayName') or DEFAULT_NAME
                    username = doc.get('username') or doc.get('user_name') or doc.get('handle') or None
                    avatar = _try_many_fields_for_avatar(doc) or None
                    return {'name': name, 'username': username, 'avatar': avatar}
            except Exception:
                pass
        # 3) users collection (waifu/husband)
        if users_coll is not None:
            try:
                doc = users_coll.find_one({'user_id': uid_str}) or users_coll.find_one({'id': uid_str})
                if doc is None and uid_str.isdigit():
                    doc = users_coll.find_one({'id': int(uid_str)})
                if doc:
                    name = doc.get('first_name') or doc.get('firstname') or doc.get('name') or DEFAULT_NAME
                    username = doc.get('username') or doc.get('user_name') or None
                    avatar = _try_many_fields_for_avatar(doc) or None
                    return {'name': name, 'username': username, 'avatar': avatar}
            except Exception:
                pass
        return {'name': DEFAULT_NAME, 'username': None, 'avatar': None}

    # ===== END initialization; everything below relies on the helpers above =====

except Exception:
    # capture full trace for debugging (exposed by /__init_error)
    _initialization_error = traceback.format_exc()
    # set safe fallbacks to avoid NameError in route functions
    market_client = None
    waifu_client = None
    husband_client = None
    waifu_users_coll = None
    husband_users_coll = None
    registered_users = None
    global_user_profiles_coll = None
    r = None

    # define minimal safe helpers so routes still work (return defaults)
    def serialize_mongo(obj):
        return obj

    def get_charms(uid):
        return 0

    def update_charms(uid, amt, typ=None):
        return False

    def ensure_user_profile(uid, first_name=None, username=None, avatar=None):
        if uid is None:
            return None
        return {'user_id': str(uid), 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR}

    def build_top_from_users_coll(users_coll, limit=100):
        return []

    def _try_many_fields_for_avatar(doc):
        return None

    def fetch_profile_fallback(uid, users_coll=None):
        return {'name': DEFAULT_NAME, 'username': None, 'avatar': None}


# ================= ROUTES (safe to call even if init failed) =================
@app.route('/')
def index():
    # If init error exists, show a minimal page indicating the server started but init failed.
    if _initialization_error:
        return (
            "<h3>App started — but initialization failed</h3>"
            "<p>Check <a href='/__init_error'>/__init_error</a> for details (Heroku logs will also show trace).</p>"
        ), 500
    return render_template('index.html')

@app.route('/__init_error')
def init_error():
    # expose init trace only if exists (helpful for debugging in staging)
    if _initialization_error:
        return Response(_initialization_error, mimetype='text/plain'), 500
    return jsonify({"ok": True, "msg": "no initialization error"}), 200

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
        # tolerant find (string or int)
        if uid and uid.isdigit():
            user_doc = users_coll.find_one({'id': str(uid)}) or users_coll.find_one({'id': int(uid)})
        else:
            user_doc = users_coll.find_one({'id': str(uid)})
        if not user_doc:
            return jsonify({"ok": True, "items": []})
        items = user_doc.get('characters') or user_doc.get('waifu') or user_doc.get('husband') or user_doc.get('char') or []
        items = serialize_mongo(items)
        return jsonify({"ok": True, "items": items})
    except Exception as e:
        return jsonify({"ok": False, "items": [], "error": str(e)}), 500

@app.route('/api/top')
def api_top():
    try:
        limit = safe_int(request.args.get('limit'), 100)
        if limit <= 0 or limit > 100:
            limit = 100
        typ = (request.args.get('type') or '').lower()

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

        # fallback to aggregate from users collection
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

        items = []
        rank = 1
        for idx, (member, score) in enumerate(raw):
            uid = str(member)
            profile = {'name': DEFAULT_NAME, 'username': None, 'avatar': None}
            try:
                profile = fetch_profile_fallback(uid, users_coll=users_coll) or profile
            except Exception:
                pass

            avatar = profile.get('avatar') or None
            if not avatar and global_user_profiles_coll is not None:
                try:
                    alt = global_user_profiles_coll.find_one({'user_id': uid}) or global_user_profiles_coll.find_one({'id': uid})
                    if alt is None and uid.isdigit():
                        alt = global_user_profiles_coll.find_one({'id': int(uid)})
                    if alt:
                        avatar = _try_many_fields_for_avatar(alt) or avatar
                        if (not profile.get('name')) or profile.get('name') == DEFAULT_NAME:
                            profile['name'] = alt.get('firstname') or alt.get('first_name') or alt.get('displayName') or profile.get('name')
                        if not profile.get('username'):
                            profile['username'] = alt.get('username') or alt.get('handle') or profile.get('username')
                except Exception:
                    pass

            if not avatar:
                avatar = DEFAULT_AVATAR

            name = profile.get('name') or DEFAULT_NAME
            username = profile.get('username') or None

            items.append({
                'rank': rank,
                'user_id': uid,
                'name': name,
                'username': username,
                'avatar': avatar,
                'charms': int(score),
                'score': int(score),
                'count': int(score)
            })
            rank += 1

        # if initialization had error, include a flag for easier debugging (non-sensitive)
        meta = {}
        if _initialization_error:
            meta['init_error'] = True

        return jsonify({"ok": True, "items": items, "meta": meta})
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
            # keep connection alive but no events
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

# local run helper
if __name__ == "__main__":
    port = safe_int(os.getenv("PORT"), 5000)
    app.run(host="0.0.0.0", port=port, threaded=True)
