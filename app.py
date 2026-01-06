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

    REDIS_HOST = os.getenv('REDIS_HOST', None)
    REDIS_PORT = safe_int(os.getenv('REDIS_PORT'), 6379)
    REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)

    # ---------- helpers to init clients ----------
    def safe_mongo(uri: str):
        if MongoClient is None or not uri:
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
        if redis is None or not host:
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
    top_global_coll = None  # collection to store cache

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

    # utilities to find doc variants and normalize
    def _find_doc_in_coll_variants(coll, uid_s):
        if coll is None:
            return None
        try:
            # try string user_id or id
            doc = coll.find_one({'user_id': uid_s})
            if doc:
                return doc
            doc = coll.find_one({'id': uid_s})
            if doc:
                return doc
            # try numeric id
            if uid_s.isdigit():
                try:
                    doc = coll.find_one({'id': int(uid_s)})
                    if doc:
                        return doc
                except Exception:
                    pass
                try:
                    doc = coll.find_one({'user_id': int(uid_s)})
                    if doc:
                        return doc
                except Exception:
                    pass
            # try _id (string)
            try:
                doc = coll.find_one({'_id': uid_s})
                if doc:
                    return doc
            except Exception:
                pass
        except Exception:
            return None
        return None

    def _normalize_profile_doc(raw):
        if not isinstance(raw, dict):
            return None
        out = {}
        out['user_id'] = str(raw.get('user_id') or raw.get('id') or raw.get('_id') or '')
        out['firstname'] = raw.get('firstname') or raw.get('first_name') or raw.get('name') or raw.get('displayName') or None
        out['username'] = raw.get('username') or raw.get('user_name') or raw.get('handle') or None
        avatar = None
        try:
            avatar = raw.get('avatar') or raw.get('photo_url') or raw.get('photo') or raw.get('picture') or raw.get('image') or None
        except Exception:
            avatar = None
        try:
            profile = raw.get('profile') or raw.get('profile_info') or None
            if isinstance(profile, dict) and not avatar:
                avatar = profile.get('avatar') or profile.get('photo') or profile.get('picture') or None
        except Exception:
            pass
        out['photo_url'] = avatar
        out['avatar'] = avatar
        return out

    # robust upsert_top_global (do not overwrite good avatar with default)
    def upsert_top_global(uid: str, firstname: str = None, username: str = None, avatar: str = None):
        uid_s = str(uid)
        charms = 0
        try:
            charms = get_charms(uid_s)
        except Exception:
            charms = 0
        now = datetime.utcnow()

        chosen_avatar = avatar
        # prefer keeping an existing non-default avatar
        try:
            if top_global_coll is not None:
                existing = top_global_coll.find_one({'user_id': uid_s})
                if (not chosen_avatar or chosen_avatar == DEFAULT_AVATAR) and existing:
                    existing_av = existing.get('avatar')
                    if existing_av and existing_av != DEFAULT_AVATAR:
                        chosen_avatar = existing_av
        except Exception:
            pass

        # write to redis
        if r is not None:
            try:
                mapping = {}
                if chosen_avatar is not None:
                    mapping['avatar'] = chosen_avatar
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
            except Exception:
                pass

        # write to mongo top_global_db
        if top_global_coll is not None:
            try:
                doc = {
                    'user_id': uid_s,
                    'username': username,
                    'firstname': firstname,
                    'avatar': chosen_avatar,
                    'charms': int(charms),
                    'updated_at': now
                }
                top_global_coll.update_one({'user_id': uid_s}, {'$set': doc}, upsert=True)
            except Exception:
                pass

    # ensure_user_profile: search many sources, normalize, persist to registered_users
    def ensure_user_profile(uid: str, first_name: str = None, username: str = None, avatar: str = None):
        if uid is None:
            return None
        uid_s = str(uid)

        # 1) try registered_users first
        doc = None
        try:
            if registered_users is not None:
                doc = _find_doc_in_coll_variants(registered_users, uid_s)
        except Exception:
            doc = None

        if doc:
            norm = _normalize_profile_doc(doc) or {}
            if first_name:
                norm['firstname'] = first_name
            if username:
                norm['username'] = username
            if avatar:
                norm['avatar'] = avatar
                norm['photo_url'] = avatar
            if not norm.get('firstname'):
                norm['firstname'] = DEFAULT_NAME
            if not norm.get('avatar'):
                norm['avatar'] = DEFAULT_AVATAR
                norm['photo_url'] = DEFAULT_AVATAR
            # persist normalized back
            try:
                update_doc = {'user_id': uid_s, 'firstname': norm['firstname'], 'photo_url': norm['photo_url'], 'avatar': norm['avatar']}
                if norm.get('username'):
                    update_doc['username'] = norm['username']
                registered_users.update_one({'user_id': uid_s}, {'$set': update_doc}, upsert=True)
            except Exception:
                pass
            try:
                upsert_top_global(uid_s, firstname=norm['firstname'], username=norm.get('username'), avatar=norm.get('avatar'))
            except Exception:
                pass
            return norm

        # 2) try other sources
        candidates = []
        try:
            if global_user_profiles_coll is not None:
                d = _find_doc_in_coll_variants(global_user_profiles_coll, uid_s)
                if d:
                    candidates.append(d)
        except Exception:
            pass
        try:
            if waifu_users_coll is not None:
                d = _find_doc_in_coll_variants(waifu_users_coll, uid_s)
                if d:
                    candidates.append(d)
        except Exception:
            pass
        try:
            if husband_users_coll is not None:
                d = _find_doc_in_coll_variants(husband_users_coll, uid_s)
                if d:
                    candidates.append(d)
        except Exception:
            pass

        # pick best candidate
        chosen = None
        for c in candidates:
            n = _normalize_profile_doc(c)
            if not n:
                continue
            if not chosen:
                chosen = n
                continue
            # prefer one with non-default avatar
            if (n.get('avatar') and n.get('avatar') != DEFAULT_AVATAR) and (not chosen.get('avatar') or chosen.get('avatar') == DEFAULT_AVATAR):
                chosen = n
            if n.get('firstname') and not chosen.get('firstname'):
                chosen = n

        if not chosen:
            chosen = {'user_id': uid_s, 'firstname': first_name or DEFAULT_NAME, 'username': username or None, 'photo_url': avatar or DEFAULT_AVATAR, 'avatar': avatar or DEFAULT_AVATAR}
        else:
            if first_name:
                chosen['firstname'] = first_name
            if username:
                chosen['username'] = username
            if avatar:
                chosen['avatar'] = avatar
                chosen['photo_url'] = avatar

        # persist to registered_users for quick future lookup
        try:
            to_save = {'user_id': uid_s, 'firstname': chosen.get('firstname') or DEFAULT_NAME, 'photo_url': chosen.get('photo_url') or chosen.get('avatar') or DEFAULT_AVATAR}
            if chosen.get('username'):
                to_save['username'] = chosen.get('username')
            if chosen.get('avatar'):
                to_save['avatar'] = chosen.get('avatar')
            if registered_users is not None:
                registered_users.update_one({'user_id': uid_s}, {'$set': to_save}, upsert=True)
        except Exception:
            pass

        try:
            upsert_top_global(uid_s, firstname=chosen.get('firstname'), username=chosen.get('username'), avatar=chosen.get('avatar'))
        except Exception:
            pass

        return chosen

    # helper: aggregate top from users coll
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

@app.route('/api/debug_top_status')
def api_debug_top_status():
    info = {
        "top_global_coll_exists": top_global_coll is not None,
        "registered_users_exists": registered_users is not None,
        "global_user_profiles_coll_exists": global_user_profiles_coll is not None,
        "waifu_users_coll_exists": waifu_users_coll is not None,
        "husband_users_coll_exists": husband_users_coll is not None,
        "redis_available": r is not None,
    }
    try:
        if top_global_coll is not None:
            info['top_global_count'] = int(top_global_coll.count_documents({}))
            sample = []
            for d in list(top_global_coll.find({}, {"_id": 0}).limit(5)):
                dd = dict(d)
                if 'updated_at' in dd and isinstance(dd['updated_at'], datetime):
                    dd['updated_at'] = dd['updated_at'].strftime("%a, %d %b %Y %H:%M:%S GMT")
                sample.append(dd)
            info['top_global_sample'] = sample
        else:
            info['top_global_count'] = 0
            info['top_global_sample'] = []
    except Exception as ex:
        info['top_global_error'] = str(ex)

    try:
        if registered_users is not None:
            info['registered_users_count'] = int(registered_users.count_documents({}))
        else:
            info['registered_users_count'] = 0
    except Exception as ex:
        info['registered_users_error'] = str(ex)

    try:
        if r is not None:
            info['redis_zcard_leaderboard'] = r.zcard('leaderboard:charms')
        else:
            info['redis_zcard_leaderboard'] = 0
    except Exception as ex:
        info['redis_error'] = str(ex)

    return jsonify({"ok": True, "info": info})

@app.route('/api/inspect_user')
def api_inspect_user():
    uid = request.args.get('user_id') or request.args.get('uid')
    if not uid:
        return jsonify({"ok": False, "error": "missing user_id"}), 400
    uid_s = str(uid)
    out = {'ok': True, 'user_id': uid_s, 'sources': {}}
    try:
        if registered_users is not None:
            out['sources']['registered_users'] = _find_doc_in_coll_variants(registered_users, uid_s)
    except Exception as ex:
        out['sources']['registered_users_error'] = str(ex)
    try:
        if global_user_profiles_coll is not None:
            out['sources']['global_user_profiles_coll'] = _find_doc_in_coll_variants(global_user_profiles_coll, uid_s)
    except Exception as ex:
        out['sources']['global_user_profiles_coll_error'] = str(ex)
    try:
        if waifu_users_coll is not None:
            out['sources']['waifu_users_coll'] = _find_doc_in_coll_variants(waifu_users_coll, uid_s)
    except Exception as ex:
        out['sources']['waifu_users_coll_error'] = str(ex)
    try:
        if husband_users_coll is not None:
            out['sources']['husband_users_coll'] = _find_doc_in_coll_variants(husband_users_coll, uid_s)
    except Exception as ex:
        out['sources']['husband_users_coll_error'] = str(ex)
    try:
        if top_global_coll is not None:
            out['sources']['top_global_coll'] = top_global_coll.find_one({'user_id': uid_s})
    except Exception as ex:
        out['sources']['top_global_coll_error'] = str(ex)
    try:
        if r is not None:
            out['redis_hash'] = r.hgetall(f"user:{uid_s}") or {}
            try:
                out['redis_score'] = r.zscore('leaderboard:charms', uid_s)
            except Exception:
                out['redis_score'] = None
    except Exception as ex:
        out['redis_error'] = str(ex)
    return jsonify(out)

@app.route('/api/user_info', methods=['GET', 'POST'])
def api_user_info():
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

    doc = ensure_user_profile(uid, first_name=firstname, username=username, avatar=avatar)
    uid_s = str(uid)
    charms = get_charms(uid_s)

    avatar_final = avatar or (doc.get('avatar') if isinstance(doc, dict) else None) or (doc.get('photo_url') if isinstance(doc, dict) else None) or DEFAULT_AVATAR

    return jsonify({
        "ok": True,
        "id": uid_s,
        "name": (firstname or (doc.get('firstname') if isinstance(doc, dict) else DEFAULT_NAME) or DEFAULT_NAME),
        "username": (username or (doc.get('username') if isinstance(doc, dict) else None)),
        "avatar": avatar_final,
        "balance": charms
    })

# Minimal defensive route for my collection
@app.route('/api/my_collection', methods=['GET'])
def api_my_collection():
    try:
        uid = request.args.get('user_id') or request.args.get('uid') or request.args.get('id')
        if not uid:
            return jsonify({"ok": False, "error": "missing user_id"}), 400
        uid_s = str(uid)

        def _find_user_doc(coll):
            if coll is None:
                return None
            try:
                doc = coll.find_one({'user_id': uid_s}) or coll.find_one({'id': uid_s})
                if doc:
                    return doc
                if uid_s.isdigit():
                    doc = coll.find_one({'id': int(uid_s)}) or coll.find_one({'user_id': int(uid_s)})
                    if doc:
                        return doc
            except Exception:
                return None
            return None

        user_doc = _find_user_doc(waifu_users_coll) or _find_user_doc(husband_users_coll)
        if user_doc is None:
            try:
                user_doc = registered_users.find_one({'user_id': uid_s}) if registered_users is not None else None
            except Exception:
                user_doc = None

        profile = {'user_id': uid_s, 'firstname': None, 'username': None, 'avatar': None}
        if isinstance(user_doc, dict):
            profile['firstname'] = user_doc.get('first_name') or user_doc.get('firstname') or user_doc.get('name') or profile['firstname']
            profile['username'] = user_doc.get('username') or user_doc.get('user_name') or profile['username']
            profile['avatar'] = (user_doc.get('photo_url') or user_doc.get('avatar') or user_doc.get('avatar_url') or user_doc.get('picture') or user_doc.get('image') or None)
        if not profile['firstname']:
            profile['firstname'] = DEFAULT_NAME
        if not profile['avatar']:
            try:
                if registered_users is not None:
                    ru = registered_users.find_one({'user_id': uid_s})
                    if isinstance(ru, dict):
                        profile['avatar'] = ru.get('photo_url') or ru.get('avatar') or profile['avatar']
            except Exception:
                pass
        if not profile['avatar']:
            profile['avatar'] = DEFAULT_AVATAR

        items = []
        try:
            if isinstance(user_doc, dict):
                chars = user_doc.get('characters') or user_doc.get('collection') or user_doc.get('my_chars') or []
                if isinstance(chars, list):
                    for c in chars:
                        try:
                            item = {
                                "id": str(c.get('id') or c.get('_id') or c.get('char_id') or ''),
                                "name": c.get('name') or c.get('title') or c.get('character_name') or 'Unknown',
                                "rarity": c.get('rarity') or c.get('rank') or None,
                                "avatar": c.get('avatar') or c.get('image') or c.get('photo') or DEFAULT_AVATAR
                            }
                            items.append(item)
                        except Exception:
                            continue
            if not items and waifu_users_coll is not None:
                try:
                    cursor = waifu_users_coll.find({'user_id': uid_s}, {'_id': 0, 'id': 1, 'name': 1, 'title': 1, 'avatar': 1, 'image': 1})
                    for d in cursor:
                        items.append({
                            "id": str(d.get('id') or ''),
                            "name": d.get('name') or d.get('title') or 'Unknown',
                            "rarity": d.get('rarity') or None,
                            "avatar": _try_many_fields_for_avatar(d) or DEFAULT_AVATAR
                        })
                except Exception:
                    pass
            if not items and husband_users_coll is not None:
                try:
                    cursor = husband_users_coll.find({'user_id': uid_s}, {'_id': 0, 'id': 1, 'name': 1, 'title': 1, 'avatar': 1, 'image': 1})
                    for d in cursor:
                        items.append({
                            "id": str(d.get('id') or ''),
                            "name": d.get('name') or d.get('title') or 'Unknown',
                            "rarity": d.get('rarity') or None,
                            "avatar": _try_many_fields_for_avatar(d) or DEFAULT_AVATAR
                        })
                except Exception:
                    pass
        except Exception:
            items = []

        return jsonify({"ok": True, "profile": profile, "items": items})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route('/api/top')
def api_top():
    try:
        limit = safe_int(request.args.get('limit'), 100)
        if limit <= 0 or limit > 100:
            limit = 100
        typ = (request.args.get('type') or '').lower().strip()

        # use top_global if no type requested
        if top_global_coll is not None and not typ:
            try:
                cursor = top_global_coll.find({}, {"_id": 0}).sort("charms", -1).limit(limit)
                items = []
                rank = 1
                for doc in cursor:
                    uid = str(doc.get('user_id') or doc.get('_id') or '')
                    name = doc.get('firstname') or DEFAULT_NAME
                    username = doc.get('username') or None
                    avatar = doc.get('avatar') or None
                    charms = int(doc.get('charms') or 0)

                    # try to improve avatar if default
                    if not avatar or avatar == DEFAULT_AVATAR:
                        try:
                            if registered_users is not None:
                                d = registered_users.find_one({'user_id': uid})
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('firstname') or name
                                    username = username or d.get('username')
                        except Exception:
                            pass
                        if (not avatar) and global_user_profiles_coll is not None:
                            try:
                                d = _find_doc_in_coll_variants(global_user_profiles_coll, uid)
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('firstname') or name
                                    username = username or d.get('username')
                            except Exception:
                                pass
                        if (not avatar) and waifu_users_coll is not None:
                            try:
                                d = _find_doc_in_coll_variants(waifu_users_coll, uid)
                                if d:
                                    avatar = _try_many_fields_for_avatar(d) or avatar
                                    name = d.get('first_name') or d.get('firstname') or name
                                    username = username or d.get('username')
                            except Exception:
                                pass
                        if (not avatar) and husband_users_coll is not None:
                            try:
                                d = _find_doc_in_coll_variants(husband_users_coll, uid)
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

        # else use redis or collection aggregation
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

        # if type requested and per-type empty, aggregate from collection
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

        if not raw and typ == '':
            try:
                if r is not None:
                    raw = r.zrevrange('leaderboard:charms', 0, limit - 1, withscores=True)
            except Exception:
                raw = []

        # fallback to registered_users
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

        items = []
        rank = 1
        for member, score in raw:
            uid = str(member)
            name = DEFAULT_NAME
            username = None
            avatar = None

            if top_global_coll is not None:
                try:
                    doc = top_global_coll.find_one({'user_id': uid})
                    if doc:
                        name = doc.get('firstname') or name
                        username = doc.get('username') or username
                        avatar = doc.get('avatar') or avatar
                except Exception:
                    pass

            if (not avatar or avatar == DEFAULT_AVATAR) and registered_users is not None:
                try:
                    d = registered_users.find_one({'user_id': uid})
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            if (not avatar or avatar == DEFAULT_AVATAR) and global_user_profiles_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(global_user_profiles_coll, uid)
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            if (not avatar or avatar == DEFAULT_AVATAR) and waifu_users_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(waifu_users_coll, uid)
                    if d:
                        avatar = _try_many_fields_for_avatar(d) or avatar
                        name = d.get('first_name') or d.get('firstname') or name
                        username = username or d.get('username')
                except Exception:
                    pass

            if (not avatar or avatar == DEFAULT_AVATAR) and husband_users_coll is not None:
                try:
                    d = _find_doc_in_coll_variants(husband_users_coll, uid)
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
                        d = _find_doc_in_coll_variants(global_user_profiles_coll, str(uid))
                        if d:
                            firstname = d.get('firstname') or d.get('first_name') or firstname
                            username = d.get('username') or username
                            avatar = _try_many_fields_for_avatar(d) or avatar
                    if firstname == DEFAULT_NAME and waifu_users_coll is not None:
                        d2 = _find_doc_in_coll_variants(waifu_users_coll, str(uid))
                        if d2:
                            firstname = d2.get('first_name') or d2.get('firstname') or firstname
                            username = username or d2.get('username')
                            avatar = avatar or _try_many_fields_for_avatar(d2)
                    if (avatar == DEFAULT_AVATAR or not avatar) and husband_users_coll is not None:
                        d3 = _find_doc_in_coll_variants(husband_users_coll, str(uid))
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
