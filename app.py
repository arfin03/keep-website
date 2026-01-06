# app.py
import os
import json
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

# safe int parser
def safe_int(v, default):
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default

# defaults (env override)
DEFAULT_AVATAR = os.getenv('DEFAULT_AVATAR', 'https://picsum.photos/200')
DEFAULT_NAME = os.getenv('DEFAULT_NAME', 'Traveler')

# flask app early
app = Flask(__name__, static_folder='static', template_folder='templates')

# capture init error
_init_error = None

try:
    # ---------- config ----------
    MONGO_URI = os.getenv('MONGO_URI', "mongodb+srv://Keepwaifu:Keepwaifu@cluster0.i8aca.mongodb.net/?retryWrites=true&w=majority")
    MARKET_DB_URL = os.getenv('MARKET_DB_URL', MONGO_URI)
    MONGO_URL_WAIFU = os.getenv('MONGO_URL_WAIFU', MONGO_URI)
    MONGO_URL_HUSBAND = os.getenv('MONGO_URL_HUSBAND', MONGO_URI)

    REDIS_HOST = os.getenv('REDIS_HOST', 'redis-13380.c81.us-east-1-2.ec2.cloud.redislabs.com')
    REDIS_PORT = safe_int(os.getenv('REDIS_PORT'), 13380)
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', "NRwYNwxwAjbyFxHDod1esj2hwsxugTiw")

    # ---------- helpers to init clients ----------
    def safe_mongo(uri: str):
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

    def safe_redis(host: str, port: int, password: str):
        if redis is None:
            return None
        try:
            r = redis.Redis(host=host, port=port, password=password, decode_responses=True,
                            socket_connect_timeout=5, socket_timeout=5)
            try:
                r.ping()
            except Exception:
                pass
            return r
        except Exception:
            return None

    # ---------- init clients ----------
    market_client = safe_mongo(MARKET_DB_URL)
    waifu_client = safe_mongo(MONGO_URL_WAIFU)
    husband_client = safe_mongo(MONGO_URL_HUSBAND)

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

    # ---------- collections ----------
    waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_lmaoooo')
    if waifu_users_coll is None:
        waifu_users_coll = get_collection(waifu_client, 'Character_catcher', 'user_collection_waifu')

    husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_lmaoooo')
    if husband_users_coll is None:
        husband_users_coll = get_collection(husband_client, 'Character_catcher', 'user_collection_husband')

    registered_users = None
    global_user_profiles_coll = None
    top_global_coll = None  # new collection to store global top/profile cache

    if market_client is not None:
        try:
            registered_users = market_client['Character_catcher']['registered_users']
        except Exception:
            registered_users = None
        try:
            global_user_profiles_coll = market_client['Character_catcher']['user_collection_lmoooo']
        except Exception:
            try:
                global_user_profiles_coll = market_client['Character_catcher']['user_collection_lmaoooo']
            except Exception:
                global_user_profiles_coll = None
        try:
            top_global_coll = market_client['Character_catcher']['top_global_db']
        except Exception:
            top_global_coll = None

    # ---------- helpers ----------
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

    def _try_many_fields_for_avatar(doc: Any):
        """
        Try multiple common fields where avatar/image might be stored.
        Returns first non-empty string or None.
        """
        if not isinstance(doc, dict):
            return None
        avatar = None
        try:
            avatar = (doc.get('photo_url') or doc.get('photo') or doc.get('avatar') or
                      doc.get('avatar_url') or doc.get('picture') or doc.get('image') or doc.get('photo_url_thumb'))
        except Exception:
            avatar = None
        try:
            profile = doc.get('profile') or doc.get('profile_info') or None
        except Exception:
            profile = None
        if isinstance(profile, dict):
            avatar = avatar or profile.get('avatar') or profile.get('photo') or profile.get('picture') or profile.get('image')
        # final sanity: ensure avatar is a non-empty string
        if isinstance(avatar, str) and avatar.strip():
            return avatar
        return None

    def get_charms(uid: str) -> int:
        try:
            if r is None:
                return 0
            return int(r.hget(f"user:{uid}", "charm") or 0)
        except Exception:
            return 0

    # ---------- main helpers (improved) ----------
    def upsert_top_global(uid: str, firstname: str = None, username: str = None, avatar: str = None):
        uid_s = str(uid)
        charms = 0
        try:
            charms = get_charms(uid_s)
        except Exception:
            charms = 0
        now = datetime.utcnow()

        # write to redis hash if available
        if r is not None:
            try:
                mapping = {}
                if avatar is not None:
                    mapping['avatar'] = avatar
                if username is not None:
                    mapping['username'] = username
                if firstname is not None:
                    mapping['firstname'] = firstname
                mapping['charm'] = str(charms)
                if mapping:
                    try:
                        r.hset(f"user:{uid_s}", mapping=mapping)
                    except Exception:
                        for k, v in mapping.items():
                            try:
                                r.hset(f"user:{uid_s}", k, v)
                            except Exception:
                                pass
                try:
                    r.zadd('leaderboard:charms', {str(uid_s): charms})
                except Exception:
                    pass
            except Exception as ex:
                print(f"[upsert_top_global][redis_error] uid={uid_s} err={ex}", flush=True)

        # write to mongo top_global_db
        if top_global_coll is not None:
            try:
                doc = {
                    'user_id': uid_s,
                    'username': username,
                    'firstname': firstname,
                    'avatar': avatar,
                    'charms': int(charms),
                    'updated_at': now
                }
                top_global_coll.update_one({'user_id': uid_s}, {'$set': doc}, upsert=True)
                print(f"[upsert_top_global][mongo_upsert] uid={uid_s} username={username} firstname={firstname} charms={charms} avatar={avatar}", flush=True)
            except Exception as ex:
                print(f"[upsert_top_global][mongo_error] uid={uid_s} err={ex}", flush=True)
        else:
            print(f"[upsert_top_global][mongo_missing] uid={uid_s} charms={charms}", flush=True)

    def ensure_user_profile(uid: str, first_name: str = None, username: str = None, avatar: str = None):
        """
        Upsert into registered_users if available, otherwise try to synthesize user info
        from global_user_profiles_coll, waifu_users_coll or husband_users_coll.
        Return a dict with at least user_id and firstname and photo_url/avatar.
        """
        if uid is None:
            return None
        uid_s = str(uid)

        doc_from_db = None
        # 1) try registered_users upsert (if available)
        if registered_users is not None:
            try:
                update = {}
                if first_name is not None:
                    update['firstname'] = first_name
                if username is not None:
                    update['username'] = username
                if avatar is not None:
                    update['photo_url'] = avatar
                    update['avatar'] = avatar
                if update:
                    update['user_id'] = uid_s
                    registered_users.update_one({'user_id': uid_s}, {'$set': update}, upsert=True)
                else:
                    registered_users.update_one({'user_id': uid_s}, {'$setOnInsert': {'user_id': uid_s, 'firstname': DEFAULT_NAME, 'photo_url': DEFAULT_AVATAR, 'avatar': DEFAULT_AVATAR}}, upsert=True)
                doc_from_db = registered_users.find_one({'user_id': uid_s})
            except Exception:
                doc_from_db = None

        # 2) if missing fields, try global profiles
        if (not username or not first_name or avatar is None) and global_user_profiles_coll is not None:
            try:
                alt = global_user_profiles_coll.find_one({'user_id': uid_s}) or global_user_profiles_coll.find_one({'id': uid_s})
                if alt is None and uid_s.isdigit():
                    alt = global_user_profiles_coll.find_one({'id': int(uid_s)})
                if alt:
                    if not first_name:
                        first_name = alt.get('firstname') or alt.get('first_name') or alt.get('displayName') or first_name
                    if not username:
                        username = alt.get('username') or alt.get('user_name') or alt.get('handle') or username
                    if avatar is None:
                        avatar = _try_many_fields_for_avatar(alt) or avatar
            except Exception:
                pass

        # 3) try waifu collection
        if (not username or not first_name or avatar is None) and waifu_users_coll is not None:
            try:
                alt = waifu_users_coll.find_one({'id': uid_s}) or waifu_users_coll.find_one({'user_id': uid_s})
                if alt is None and uid_s.isdigit():
                    alt = waifu_users_coll.find_one({'id': int(uid_s)})
                if alt:
                    if not first_name:
                        first_name = alt.get('first_name') or alt.get('firstname') or first_name
                    if not username:
                        username = alt.get('username') or username
                    if avatar is None:
                        avatar = _try_many_fields_for_avatar(alt) or avatar
            except Exception:
                pass

        # 4) try husband collection
        if (not username or not first_name or avatar is None) and husband_users_coll is not None:
            try:
                alt = husband_users_coll.find_one({'id': uid_s}) or husband_users_coll.find_one({'user_id': uid_s})
                if alt is None and uid_s.isdigit():
                    alt = husband_users_coll.find_one({'id': int(uid_s)})
                if alt:
                    if not first_name:
                        first_name = alt.get('first_name') or alt.get('firstname') or first_name
                    if not username:
                        username = alt.get('username') or username
                    if avatar is None:
                        avatar = _try_many_fields_for_avatar(alt) or avatar
            except Exception:
                pass

        # final defaults
        if not first_name:
            first_name = DEFAULT_NAME
        if avatar is None:
            avatar = DEFAULT_AVATAR

        # ensure redis & top_global updated (best-effort)
        try:
            upsert_top_global(uid_s, firstname=first_name, username=username, avatar=avatar)
        except Exception:
            pass

        if isinstance(doc_from_db, dict):
            # ensure doc has photo_url/avatar fields
            if 'photo_url' not in doc_from_db:
                doc_from_db['photo_url'] = avatar
            if 'avatar' not in doc_from_db:
                doc_from_db['avatar'] = avatar
            return doc_from_db

        return {'user_id': uid_s, 'firstname': first_name, 'username': username, 'photo_url': avatar, 'avatar': avatar}

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

except Exception:
    _init_error = traceback.format_exc()
    market_client = waifu_client = husband_client = None
    registered_users = global_user_profiles_coll = top_global_coll = None
    r = None

    def serialize_mongo(x): return x
    def get_charms(uid): return 0
    def update_charms(uid, amt, typ=None): return False
    def ensure_user_profile(uid, first_name=None, username=None, avatar=None):
        return {'user_id': str(uid), 'firstname': first_name or DEFAULT_NAME, 'username': username, 'photo_url': avatar or DEFAULT_AVATAR, 'avatar': avatar or DEFAULT_AVATAR}
    def upsert_top_global(uid, firstname=None, username=None, avatar=None): return None
    def build_top_from_users_coll(users_coll, limit=100): return []

# ================= ROUTES =================
@app.route('/')
def index():
    if _init_error:
        return ("<h3>App started but init failed</h3>"
                "<p>Check <a href='/__init_error'>/__init_error</a> for details.</p>"), 500
    return render_template('index.html')

@app.route('/__init_error')
def show_init_error():
    if _init_error:
        return Response(_init_error, mimetype='text/plain'), 500
    return jsonify({"ok": True, "msg": "no init error"}), 200

@app.route('/api/user_info', methods=['GET', 'POST'])
def api_user_info():
    # Accept both GET query params and POST json
    if request.method == 'POST':
        data = request.get_json(silent=True) or request.form.to_dict()
    else:
        data = request.args.to_dict()

    uid = data.get('user_id') or data.get('id') or data.get('uid')
    firstname = data.get('firstname') or data.get('first_name') or None
    username = data.get('username') or data.get('user_name') or None
    avatar = data.get('avatar') or data.get('photo_url') or data.get('avatar_url') or None

    if uid is None:
        return jsonify({"ok": False, "error": "missing user_id"}), 400

    # ensure user profile saved and update top_global_db + redis
    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)

    uid_s = str(uid)
    charms = get_charms(uid_s)

    # normalize avatar: prefer provided avatar param, else doc fields
    avatar_final = avatar or (doc.get('avatar') if isinstance(doc, dict) else None) or (doc.get('photo_url') if isinstance(doc, dict) else None) or DEFAULT_AVATAR

    return jsonify({
        "ok": True,
        "id": uid_s,
        "name": (firstname or (doc.get('firstname') if isinstance(doc, dict) else DEFAULT_NAME) or DEFAULT_NAME),
        "username": (username or (doc.get('username') if isinstance(doc, dict) else None)),
        "avatar": avatar_final,
        "balance": charms
    })

@app.route('/api/top')
def api_top():
    try:
        limit = safe_int(request.args.get('limit'), 100)
        if limit <= 0 or limit > 100:
            limit = 100
        typ = (request.args.get('type') or '').lower().strip()

        # If no specific type and top_global available: prefer it
        if top_global_coll is not None and not typ:
            try:
                cursor = top_global_coll.find({}, {"_id": 0}).sort("charms", -1).limit(limit)
                items = []
                rank = 1
                for doc in cursor:
                    # basic values
                    uid = str(doc.get('user_id') or doc.get('_id') or '')
                    name = doc.get('firstname') or DEFAULT_NAME
                    username = doc.get('username') or None
                    avatar = doc.get('avatar') or None
                    charms = int(doc.get('charms') or 0)

                    # if avatar is default-ish or missing, attempt to find better avatar from other sources
                    if not avatar or avatar == DEFAULT_AVATAR:
                        # try registered_users
                        try:
                            if registered_users is not None:
                                d = registered_users.find_one({'user_id': uid})
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('firstname') or name
                                    username = username or d.get('username')
                        except Exception:
                            pass
                        # try global profiles
                        if (not avatar) and global_user_profiles_coll is not None:
                            try:
                                d = global_user_profiles_coll.find_one({'user_id': uid}) or global_user_profiles_coll.find_one({'id': uid})
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('firstname') or name
                                    username = username or d.get('username')
                            except Exception:
                                pass
                        # try waifu/husband
                        if (not avatar) and waifu_users_coll is not None:
                            try:
                                d = waifu_users_coll.find_one({'id': uid}) or waifu_users_coll.find_one({'user_id': uid})
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('first_name') or d.get('firstname') or name
                                    username = username or d.get('username')
                            except Exception:
                                pass
                        if (not avatar) and husband_users_coll is not None:
                            try:
                                d = husband_users_coll.find_one({'id': uid}) or husband_users_coll.find_one({'user_id': uid})
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('first_name') or d.get('firstname') or name
                                    username = username or d.get('username')
                            except Exception:
                                pass

                    if not avatar:
                        avatar = DEFAULT_AVATAR

                    items.append({
                        "rank": rank,
                        "user_id": uid,
                        "name": name,
                        "username": username,
                        "avatar": avatar,
                        "charms": charms,
                        "score": charms,
                        "count": charms
                    })
                    rank += 1
                return jsonify({"ok": True, "items": items})
            except Exception:
                pass

        # redis key selection
        redis_key = 'leaderboard:charms'
        users_coll = None
        if typ == 'waifu':
            redis_key = 'leaderboard:charms:waifu'
            users_coll = waifu_users_coll
        elif typ == 'husband':
            redis_key = 'leaderboard:charms:husband'
            users_coll = husband_users_coll

        raw = []
        try:
            if r is not None:
                raw = r.zrevrange(redis_key, 0, limit - 1, withscores=True)
        except Exception:
            raw = []

        # If type-specific and no per-type redis, aggregate from collection instead of falling back to global
        if not raw and typ in ('waifu', 'husband'):
            if users_coll is not None:
                try:
                    agg = build_top_from_users_coll(users_coll, limit=limit)
                    raw = []
                    for d in agg:
                        uid = d.get('user_id') or d.get('id') or d.get('_id')
                        if not uid:
                            continue
                        raw.append((str(uid), int(d.get('character_count') or 0)))
                except Exception:
                    raw = []

        # If still empty and no type specified, fallback to global redis
        if not raw and typ == '':
            try:
                if r is not None:
                    raw = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            except Exception:
                raw = []

        # final fallback: build from registered_users
        if not raw:
            raw = []
            if registered_users is not None:
                try:
                    for u in registered_users.find({}, {'user_id': 1}):
                        uid = u.get('user_id')
                        if not uid:
                            continue
                        c = get_charms(uid)
                        if c > 0:
                            raw.append((str(uid), int(c)))
                    raw.sort(key=lambda x: -x[1])
                    raw = raw[:limit]
                except Exception:
                    raw = []

        # build final items and enrich avatars/names from multiple sources
        items = []
        rank = 1
        for member, score in raw:
            uid = str(member)
            name = DEFAULT_NAME
            username = None
            avatar = None

            # prefer top_global_coll doc for name/avatar if exists
            if top_global_coll is not None:
                try:
                    doc = top_global_coll.find_one({'user_id': uid})
                    if doc:
                        name = doc.get('firstname') or name
                        username = doc.get('username') or username
                        avatar = doc.get('avatar') or avatar
                except Exception:
                    pass

            # try registered_users
            if (not avatar or avatar == DEFAULT_AVATAR):
                try:
                    if registered_users is not None:
                        d = registered_users.find_one({'user_id': uid})
                        if d:
                            avatar = _try_many_fields_for_avatar(d) or avatar
                            name = d.get('firstname') or name
                            username = username or d.get('username')
                except Exception:
                    pass

            # try global profile
            if (not avatar or avatar == DEFAULT_AVATAR) and global_user_profiles_coll is not None:
                try:
                    d = global_user_profiles_coll.find_one({'user_id': uid}) or global_user_profiles_coll.find_one({'id': uid})
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            # waifu/husband fallbacks
            if (not avatar or avatar == DEFAULT_AVATAR) and waifu_users_coll is not None:
                try:
                    d = waifu_users_coll.find_one({'id': uid}) or waifu_users_coll.find_one({'user_id': uid})
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            if (not avatar or avatar == DEFAULT_AVATAR) and husband_users_coll is not None:
                try:
                    d = husband_users_coll.find_one({'id': uid}) or husband_users_coll.find_one({'user_id': uid})
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            if not avatar:
                avatar = DEFAULT_AVATAR

            items.append({
                "rank": rank,
                "user_id": uid,
                "name": name,
                "username": username,
                "avatar": avatar,
                "charms": int(score),
                "score": int(score),
                "count": int(score)
            })
            rank += 1

        return jsonify({"ok": True, "items": items})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e), "items": []}), 500

@app.route('/api/rebuild_top_global')
def api_rebuild_top_global():
    if top_global_coll is None:
        return jsonify({"ok": False, "error": "top_global_db collection not available"}), 400
    try:
        limit = safe_int(request.args.get('limit'), 10000)
        count = 0
        if registered_users is not None:
            for u in registered_users.find({}, {'user_id': 1, 'firstname': 1, 'photo_url': 1, 'avatar': 1, 'username': 1}):
                uid = u.get('user_id')
                if not uid:
                    continue
                firstname = u.get('firstname') or DEFAULT_NAME
                avatar = _try_many_fields_for_avatar(u) or u.get('photo_url') or u.get('avatar') or DEFAULT_AVATAR
                username = u.get('username') or None
                charms = get_charms(uid)
                top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'user_id': str(uid), 'firstname': firstname, 'avatar': avatar, 'username': username, 'charms': int(charms), 'updated_at': datetime.utcnow()}}, upsert=True)
                count += 1
                if count >= limit:
                    break
            return jsonify({"ok": True, "count": count})
        if r is not None:
            pairs = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            for uid, score in pairs:
                firstname = DEFAULT_NAME
                avatar = DEFAULT_AVATAR
                username = None
                try:
                    if global_user_profiles_coll is not None:
                        d = global_user_profiles_coll.find_one({'user_id': str(uid)}) or global_user_profiles_coll.find_one({'id': str(uid)})
                        if d:
                            firstname = d.get('firstname') or d.get('first_name') or firstname
                            username = d.get('username') or username
                            avatar = _try_many_fields_for_avatar(d) or avatar
                    if firstname == DEFAULT_NAME and waifu_users_coll is not None:
                        d2 = waifu_users_coll.find_one({'id': str(uid)}) or waifu_users_coll.find_one({'user_id': str(uid)})
                        if d2:
                            firstname = d2.get('first_name') or d2.get('firstname') or firstname
                            username = username or d2.get('username')
                            avatar = avatar or _try_many_fields_for_avatar(d2)
                    if (avatar == DEFAULT_AVATAR or not avatar) and husband_users_coll is not None:
                        d3 = husband_users_coll.find_one({'id': str(uid)}) or husband_users_coll.find_one({'user_id': str(uid)})
                        if d3:
                            firstname = firstname or d3.get('first_name') or d3.get('firstname')
                            username = username or d3.get('username')
                            avatar = avatar or _try_many_fields_for_avatar(d3)
                except Exception:
                    pass
                top_global_coll.update_one({'user_id': str(uid)}, {'$set': {'user_id': str(uid), 'firstname': firstname, 'username': username, 'avatar': avatar or DEFAULT_AVATAR, 'charms': int(score), 'updated_at': datetime.utcnow()}}, upsert=True)
            return jsonify({"ok": True, "count": len(pairs)})
        return jsonify({"ok": False, "error": "no source to rebuild from"}), 400
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

# SSE stream
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

# local runner
if __name__ == "__main__":
    port = safe_int(os.getenv('PORT'), 5000)
    app.run(host="0.0.0.0", port=port, threaded=True)
