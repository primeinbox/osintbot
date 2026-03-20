  # ======================================================
# ADVANCE MULTI-BOT SYSTEM WITH POINTS & REFERRAL
# ======================================================
# CODED BY: DEMON_KILLER 😈🔥
# FOR: @Aerivue
# ======================================================

import os
import json
import asyncio
import aiohttp
import random
import string
import time
import html
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    CallbackQueryHandler,
    filters,
    ConversationHandler,
    CallbackContext
)
from telegram.error import InvalidToken, TelegramError
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
import sys

# ======================================================
# LOAD ENVIRONMENT VARIABLES
# ======================================================
load_dotenv()

# Bot Configuration
MASTER_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# ── Multiple Admins ────────────────────────────────────
# .env mein: OWNER_ID=123456789
# Extra admins: ADMIN_IDS=111111111,222222222,333333333
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
_extra_admins = os.getenv("ADMIN_IDS", "")
ADMIN_IDS: set = {OWNER_ID}
if _extra_admins:
    for _aid in _extra_admins.split(","):
        _aid = _aid.strip()
        if _aid.isdigit():
            ADMIN_IDS.add(int(_aid))

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ── Multiple Force-Join Channels/Groups ───────────────
# .env mein: FORCE_JOIN_CHANNELS=@channel1,@channel2,-100123456789
# (comma separated, @ username ya numeric chat id dono chalega)
_fjc_raw = os.getenv("FORCE_JOIN_CHANNELS", os.getenv("FORCE_JOIN_CHANNEL", ""))
FORCE_JOIN_CHANNELS: list = []
if _fjc_raw:
    for _ch in _fjc_raw.split(","):
        _ch = _ch.strip()
        if _ch:
            FORCE_JOIN_CHANNELS.append(_ch)

# API Keys for different services
API_KEYS = {
    "number": os.getenv("NUMBER_API_KEY"),
    "aadhar": os.getenv("AADHAR_API_KEY"),
    "imei": os.getenv("IMEI_API_KEY"),
    "rto": os.getenv("RTO_API_KEY"),
    "tg": os.getenv("TG_API_KEY")
}

BASE_URL = "https://aerivue.onrender.com"

# Point deduction rules
POINTS = {
    "number": 3,
    "aadhar": 8,
    "imei": 4,
    "rto": 5,
    "tg": 10
}

# Referral bonus
REFERRAL_BONUS = {
    "referrer": 80,
    "referee": 120  # 100 start + 20 extra
}

START_BONUS = 100

# ======================================================
# MONGODB CONNECTION
# ======================================================
mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client.get_default_database()

# Collections
users_collection = db["users"]
bots_collection = db["bots"]
promo_codes_collection = db["promo_codes"]
transactions_collection = db["transactions"]
referrals_collection = db["referrals"]
search_stats_collection = db["search_stats"]

# ======================================================
# RATE LIMITER
# ======================================================
class RateLimiter:
    def __init__(self):
        self.user_requests = defaultdict(list)
        
    def is_allowed(self, user_id: str, max_requests: int = 5, time_window: int = 60) -> bool:
        now = time.time()
        user_history = self.user_requests[user_id]
        user_history = [t for t in user_history if now - t < time_window]
        self.user_requests[user_id] = user_history
        
        if len(user_history) >= max_requests:
            return False
        
        self.user_requests[user_id].append(now)
        return True

rate_limiter = RateLimiter()

# ======================================================
# DATABASE INITIALIZATION
# ======================================================
async def init_db():
    """Create indexes on startup."""
    await users_collection.create_index("telegram_id", unique=True)
    await users_collection.create_index("referral_code", unique=True)
    await bots_collection.create_index("token", unique=True)
    await promo_codes_collection.create_index("code", unique=True)
    await transactions_collection.create_index([("user_id", 1), ("timestamp", -1)])

# ======================================================
# FORCE JOIN CHECKER  (multiple channels/groups support)
# ======================================================
def _channel_invite_url(chat_id: str) -> str:
    """Build a t.me link from @username or numeric id."""
    if chat_id.startswith("@"):
        return f"https://t.me/{chat_id[1:]}"
    # numeric id — we can't build a public link, return empty
    return ""

async def check_force_join(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Check if user has joined ALL required channels/groups.
    Returns True only when the user is a member of every entry in
    FORCE_JOIN_CHANNELS.  Sends a join-prompt on the first failure.
    """
    if not FORCE_JOIN_CHANNELS:
        return True

    user_id = update.effective_user.id
    not_joined = []

    for chat_id in FORCE_JOIN_CHANNELS:
        try:
            member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status not in ("member", "administrator", "creator"):
                not_joined.append(chat_id)
        except Exception as e:
            print(f"Force join check error ({chat_id}): {e}")
            not_joined.append(chat_id)

    if not not_joined:
        return True

    # Build join buttons for every un-joined channel
    keyboard = []
    for idx, chat_id in enumerate(not_joined, 1):
        url = _channel_invite_url(chat_id)
        label = f"📢 Join {chat_id if chat_id.startswith('@') else f'Chat {idx}'}"
        if url:
            keyboard.append([InlineKeyboardButton(label, url=url)])

    keyboard.append([InlineKeyboardButton("✅ I Joined — Check Again", callback_data="check_force_join")])

    msg_text = (
        "❌ <b>Access Denied!</b>\n\n"
        "Please join the following to use this bot:\n\n"
        + "\n".join(
            f"• {c}" for c in not_joined
        )
    )

    if update.callback_query:
        await update.callback_query.answer("❌ Please join all channels first!", show_alert=True)
        try:
            await update.callback_query.edit_message_text(
                msg_text, parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
        except Exception:
            pass
    elif update.message:
        await update.message.reply_text(
            msg_text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    return False

# ======================================================
# USER MANAGEMENT
# ======================================================
async def get_or_create_user(user_id: int, username: str = None, first_name: str = None, referrer_id: int = None) -> dict:
    """Get user from DB or create new one"""
    user = await users_collection.find_one({"telegram_id": str(user_id)})
    
    if not user:
        referral_code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        
        user = {
            "telegram_id": str(user_id),
            "username": username,
            "first_name": first_name,
            "points": START_BONUS,
            "total_points_earned": START_BONUS,
            "referral_code": referral_code,
            "referred_by": str(referrer_id) if referrer_id else None,
            "referral_count": 0,
            "total_searches": 0,
            "created_at": datetime.utcnow(),
            "last_active": datetime.utcnow(),
            "is_banned": False
        }
        
        await users_collection.insert_one(user)
        
        if referrer_id:
            referrer = await users_collection.find_one({"telegram_id": str(referrer_id)})
            if referrer:
                await users_collection.update_one(
                    {"telegram_id": str(referrer_id)},
                    {
                        "$inc": {
                            "points": REFERRAL_BONUS["referrer"],
                            "total_points_earned": REFERRAL_BONUS["referrer"],
                            "referral_count": 1
                        }
                    }
                )
                
                await referrals_collection.insert_one({
                    "referrer_id": str(referrer_id),
                    "referee_id": str(user_id),
                    "bonus_given": REFERRAL_BONUS["referrer"],
                    "timestamp": datetime.utcnow()
                })
                
                try:
                    await context.bot.send_message(
                        chat_id=referrer_id,
                        text=f"🎉 <b>New Referral!</b>\n\n"
                             f"User: {first_name or username}\n"
                             f"You earned: <b>{REFERRAL_BONUS['referrer']} points</b>",
                        parse_mode="HTML"
                    )
                except:
                    pass
    
    return user

async def deduct_points(user_id: int, points: int, service: str) -> bool:
    """Deduct points from user account"""
    user = await users_collection.find_one({"telegram_id": str(user_id)})
    
    if not user or user.get("points", 0) < points:
        return False
    
    await users_collection.update_one(
        {"telegram_id": str(user_id)},
        {
            "$inc": {"points": -points},
            "$push": {
                "transactions": {
                    "type": "deduction",
                    "amount": points,
                    "service": service,
                    "timestamp": datetime.utcnow()
                }
            }
        }
    )
    
    return True

# ======================================================
# CHECK API SUCCESS — per service response structure
# ======================================================
def is_api_success(data: dict, service: str) -> bool:
    """
    Returns True if the API response contains valid data.

    Response structures:
      number  → {"result": {"status": "success", "results": [...]}}
      aadhar  → {"status": true, "results": {"success": true, "records": [...]}}
      imei    → {"success": true, "imei": "...", "brand": "..."}
      rto     → {"details": {...}, "rc": "..."}
      tg      → {"result": {"status": "success", "results": [...]}}
    """
    if service == "number" or service == "tg":
        result = data.get("result", {})
        return (
            result.get("status") == "success"
            and bool(result.get("results"))
        )

    elif service == "aadhar":
        return (
            data.get("status") is True
            and data.get("results", {}).get("success") is True
            and bool(data.get("results", {}).get("records"))
        )

    elif service == "imei":
        return data.get("success") is True and bool(data.get("imei"))

    elif service == "rto":
        return bool(data.get("details")) and bool(data.get("rc"))

    return False

# ======================================================
# API HANDLER WITH POINT DEDUCTION
# ======================================================
async def call_api(
    endpoint: str,
    params: dict,
    api_key: str,
    points_to_deduct: int,
    user_id: int,
    service: str,
    bot_token: str = None
) -> Tuple[dict, bool]:
    """
    Call API and handle point deduction.
    Returns: (response_data, success)
    """
    user = await users_collection.find_one({"telegram_id": str(user_id)})
    if not user or user.get("points", 0) < points_to_deduct:
        return {"error": "Insufficient points"}, False

    params["apikey"] = api_key
    url = f"{BASE_URL}{endpoint}"

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if is_api_success(data, service):
                    await deduct_points(user_id, points_to_deduct, service)

                    await users_collection.update_one(
                        {"telegram_id": str(user_id)},
                        {"$inc": {"total_searches": 1}}
                    )

                    if bot_token:
                        await bots_collection.update_one(
                            {"token": bot_token},
                            {"$inc": {"search_count": 1}}
                        )

                    query_value = (
                        params.get("number")
                        or params.get("aadhar")
                        or params.get("imei")
                        or params.get("rc")
                        or params.get("userid")
                    )
                    await search_stats_collection.insert_one({
                        "user_id": str(user_id),
                        "service": service,
                        "query": query_value,
                        "points_deducted": points_to_deduct,
                        "timestamp": datetime.utcnow(),
                        "bot_token": bot_token
                    })

                    return data, True
                else:
                    return data, False

    except Exception as e:
        print(f"API Error: {e}")
        return {"error": str(e)}, False

# ======================================================
# FORMAT API RESPONSE  (updated for real response shapes)
# ======================================================
def format_api_response(data: dict, service: str) -> str:
    """Format API response for Telegram display."""

    header = (
        f"<b>━━━━━━━━━━━━━━━━━━━━</b>\n"
        f"<b>        🔎 {service.upper()} SEARCH</b>\n"
        f"<b>━━━━━━━━━━━━━━━━━━━━</b>\n\n"
    )
    footer = "\n<b>Bot: @qxvoltbot</b>\n<b>━━━━━━━━━━━━━━━━━━━━</b>"

    # ── NUMBER lookup ────────────────────────────────────
    if service == "number":
        result = data.get("result", {})
        if result.get("status") != "success" or not result.get("results"):
            return f"{header}<b>❌ No records found.</b>{footer}"

        records = result["results"]
        output = header

        # de-duplicate by (mobile, id)
        seen = set()
        unique = []
        for r in records:
            key = (r.get("mobile"), r.get("id"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        for idx, item in enumerate(unique[:5], 1):
            output += f"<b>📌 RECORD {idx}</b>\n────────────────────\n"
            output += f"<b>👤 Name   :</b> <code>{item.get('name', 'N/A')}</code>\n"
            output += f"<b>📱 Mobile :</b> <code>{item.get('mobile', 'N/A')}</code>\n"
            output += f"<b>👨 Father :</b> <code>{item.get('fname', 'N/A')}</code>\n"
            output += f"<b>🌐 Circle :</b> <code>{item.get('circle', 'N/A')}</code>\n"
            if item.get("address"):
                output += f"<b>🏠 Address:</b> <code>{item['address']}</code>\n"
            if item.get("alt"):
                output += f"<b>📞 Alt No :</b> <code>{item['alt']}</code>\n"
            if item.get("email"):
                output += f"<b>📧 Email  :</b> <code>{item['email']}</code>\n"
            if item.get("id"):
                output += f"<b>🆔 ID     :</b> <code>{item['id']}</code>\n"
            output += "\n"

        if len(unique) > 5:
            output += f"<i>… and {len(unique) - 5} more records</i>\n"

        return output + footer

    # ── AADHAR lookup ────────────────────────────────────
    elif service == "aadhar":
        results_obj = data.get("results", {})
        records = results_obj.get("records", [])

        if not records:
            return f"{header}<b>❌ No records found.</b>{footer}"

        output = header

        # de-duplicate by (mobile, aadhaar_number)
        seen = set()
        unique = []
        for r in records:
            key = (r.get("mobile"), r.get("aadhaar_number"))
            if key not in seen:
                seen.add(key)
                unique.append(r)

        branding = data.get("branding", {})
        if branding:
            output += (
                f"<b>🔑 Key     :</b> <code>{branding.get('key_used', 'N/A')}</code>\n"
                f"<b>⏰ Expiry  :</b> <code>{results_obj.get('key_expiry', 'N/A')}</code>\n\n"
            )

        for idx, item in enumerate(unique[:5], 1):
            output += f"<b>📌 RECORD {idx}</b>\n────────────────────\n"
            output += f"<b>👤 Name      :</b> <code>{item.get('name', 'N/A')}</code>\n"
            output += f"<b>👨 Father    :</b> <code>{item.get('father_name', 'N/A')}</code>\n"
            output += f"<b>📱 Mobile    :</b> <code>{item.get('mobile', 'N/A')}</code>\n"
            if item.get("alt_mobile"):
                output += f"<b>📞 Alt Mobile:</b> <code>{item['alt_mobile']}</code>\n"
            output += f"<b>🌐 Circle    :</b> <code>{item.get('circle', 'N/A')}</code>\n"
            output += f"<b>🔢 Aadhar    :</b> <code>{item.get('aadhaar_number', 'N/A')}</code>\n"
            if item.get("email"):
                output += f"<b>📧 Email     :</b> <code>{item['email']}</code>\n"
            if item.get("address"):
                output += f"<b>🏠 Address   :</b> <code>{item['address']}</code>\n"
            if item.get("_source"):
                output += f"<b>📂 Source    :</b> <code>{item['_source']}</code>\n"
            output += "\n"

        if len(unique) > 5:
            output += f"<i>… and {len(unique) - 5} more records</i>\n"

        return output + footer

    # ── IMEI lookup ──────────────────────────────────────
    elif service == "imei":
        if not data.get("success") or not data.get("imei"):
            return f"{header}<b>❌ No device info found.</b>{footer}"

        output = header
        output += f"<b>📌 DEVICE INFO</b>\n────────────────────\n"
        output += f"<b>🔢 IMEI      :</b> <code>{data.get('imei', 'N/A')}</code>\n"
        output += f"<b>🏢 Brand     :</b> <code>{data.get('brand', 'N/A')}</code>\n"
        output += f"<b>📱 Model     :</b> <code>{data.get('model', 'N/A')}</code>\n"

        basic = data.get("basic_info", {})
        if basic:
            output += f"\n<b>📋 BASIC INFO</b>\n────────────────────\n"
            if basic.get("code_name"):
                output += f"<b>🔖 Code Name :</b> <code>{basic['code_name']}</code>\n"
            if basic.get("release_year"):
                output += f"<b>📅 Released  :</b> <code>{basic['release_year']}</code>\n"
            if basic.get("os"):
                output += f"<b>💻 OS        :</b> <code>{basic['os']}</code>\n"
            if basic.get("chipset"):
                output += f"<b>⚙️ Chipset   :</b> <code>{basic['chipset']}</code>\n"
            if basic.get("gpu"):
                output += f"<b>🎮 GPU       :</b> <code>{basic['gpu']}</code>\n"

        dims = data.get("dimensions", {})
        if dims:
            output += f"\n<b>📐 DIMENSIONS</b>\n────────────────────\n"
            output += f"<b>↕️ Height    :</b> <code>{dims.get('height', 'N/A')} mm</code>\n"
            output += f"<b>↔️ Width     :</b> <code>{dims.get('width', 'N/A')}</code>\n"
            output += f"<b>◻️ Thickness :</b> <code>{dims.get('thickness', 'N/A')}</code>\n"

        display = data.get("display", {})
        if display:
            output += f"\n<b>🖥️ DISPLAY</b>\n────────────────────\n"
            output += f"<b>📺 Type      :</b> <code>{display.get('type', 'N/A')}</code>\n"
            if display.get("resolution"):
                output += f"<b>🔲 Resolution:</b> <code>{display['resolution']}</code>\n"
            if display.get("size"):
                output += f"<b>📏 Size      :</b> <code>{display['size']}</code>\n"

        network = data.get("network", {})
        if network:
            output += f"\n<b>📡 NETWORK</b>\n────────────────────\n"
            bands = []
            if network.get("5g"):  bands.append("5G")
            if network.get("4g"):  bands.append("4G")
            if network.get("3g"):  bands.append("3G")
            if network.get("2g"):  bands.append("2G")
            output += f"<b>📶 Support   :</b> <code>{' | '.join(bands)}</code>\n"

        battery = data.get("battery", {})
        if battery:
            output += f"\n<b>🔋 BATTERY</b>\n────────────────────\n"
            output += f"<b>🔌 Type      :</b> <code>{battery.get('type', 'N/A')}</code>\n"
            output += f"<b>⚡ Capacity  :</b> <code>{battery.get('capacity', 'N/A')}</code>\n"

        camera = data.get("camera", {})
        if camera:
            output += f"\n<b>📷 CAMERA</b>\n────────────────────\n"
            output += f"<b>🔭 Main      :</b> <code>{camera.get('main', 'N/A')}</code>\n"
            output += f"<b>🤳 Selfie    :</b> <code>{camera.get('selfie', 'N/A')}</code>\n"

        if data.get("photo"):
            output += f"\n<b>🖼️ Photo     :</b> {data['photo']}\n"

        return output + footer

    # ── RTO lookup ───────────────────────────────────────
    elif service == "rto":
        details = data.get("details", {})
        rc_number = data.get("rc", "N/A")

        if not details:
            return f"{header}<b>❌ No vehicle records found.</b>{footer}"

        output = header
        output += f"<b>📌 VEHICLE INFO</b>\n────────────────────\n"
        output += f"<b>🚗 RC Number    :</b> <code>{rc_number}</code>\n"

        field_map = [
            ("Owner Name",        "👤 Owner"),
            ("Maker Model",       "🚘 Model"),
            ("Vehicle Class",     "🏷️ Class"),
            ("Fuel Type",         "⛽ Fuel"),
            ("Registration Date", "📅 Reg Date"),
            ("Registered RTO",    "🏢 RTO"),
            ("Address",           "🏠 Address"),
            ("City Name",         "🏙️ City"),
            ("Phone",             "📞 Phone"),
            ("Insurance Company", "🛡️ Insurer"),
            ("Insurance Expiry",  "📆 Ins. Expiry"),
            ("Fitness Upto",      "✅ Fitness Upto"),
            ("Tax Upto",          "💰 Tax Upto"),
            ("PUC Upto",          "🌿 PUC Upto"),
        ]

        for key, label in field_map:
            val = details.get(key)
            if val:
                output += f"<b>{label} :</b> <code>{val}</code>\n"

        return output + footer

    # ── TG lookup ────────────────────────────────────────
    elif service == "tg":
        result = data.get("result", {})
        if result.get("status") != "success" or not result.get("results"):
            return f"{header}<b>❌ No Telegram records found.</b>{footer}"

        records = result["results"]
        output = header

        seen = set()
        unique = []
        for r in records:
            key = r.get("id") or r.get("phone")
            if key not in seen:
                seen.add(key)
                unique.append(r)

        for idx, item in enumerate(unique[:5], 1):
            output += f"<b>📌 RECORD {idx}</b>\n────────────────────\n"
            output += f"<b>👤 Name    :</b> <code>{item.get('name', 'N/A')}</code>\n"
            output += f"<b>🆔 TG ID   :</b> <code>{item.get('id', 'N/A')}</code>\n"
            output += f"<b>📱 Phone   :</b> <code>{item.get('phone', 'N/A')}</code>\n"
            if item.get("username"):
                output += f"<b>🔗 Username:</b> @{item['username']}\n"
            output += "\n"

        if len(unique) > 5:
            output += f"<i>… and {len(unique) - 5} more records</i>\n"

        return output + footer

    return f"{header}<b>❌ Unknown service.</b>{footer}"


# ======================================================
# MAIN MENU
# ======================================================
async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_force_join(update, context):
        return
    
    user = update.effective_user
    db_user = await get_or_create_user(user.id, user.username, user.first_name)

    # Escape special HTML chars in name to prevent parse errors
    safe_name = html.escape(user.first_name or "User")
    
    keyboard = [
        [InlineKeyboardButton("🔍 Number Info", callback_data="menu_number"),
         InlineKeyboardButton("🆔 Aadhar Info", callback_data="menu_aadhar")],
        [InlineKeyboardButton("📱 IMEI Info", callback_data="menu_imei"),
         InlineKeyboardButton("🚗 RTO Info", callback_data="menu_rto")],
        [InlineKeyboardButton("📲 Telegram ID", callback_data="menu_tg"),
         InlineKeyboardButton("👥 Referral", callback_data="menu_referral")],
        [InlineKeyboardButton("💰 My Points", callback_data="menu_points"),
         InlineKeyboardButton("🤖 My Bots", callback_data="menu_mybots")],
        [InlineKeyboardButton("🎫 Promo Code", callback_data="menu_promo"),
         InlineKeyboardButton("📊 Stats", callback_data="menu_stats")],
        [InlineKeyboardButton("❓ Help", callback_data="menu_help")]
    ]
    
    text = (
        f"<b>🔥 Welcome {safe_name}!</b>\n\n"
        f"<b>💰 Your Points:</b> <code>{db_user.get('points', 0)}</code>\n"
        f"<b>👥 Referrals:</b> <code>{db_user.get('referral_count', 0)}</code>\n"
        f"<b>🔍 Total Searches:</b> <code>{db_user.get('total_searches', 0)}</code>\n\n"
        f"<i>Select a service below:</i>"
    )
    
    markup = InlineKeyboardMarkup(keyboard)
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode="HTML", reply_markup=markup)
    else:
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=markup)

# ======================================================
# SEARCH HANDLERS
# ======================================================
async def search_handler(update: Update, context: ContextTypes.DEFAULT_TYPE, service: str):
    """Generic search handler"""
    if not await check_force_join(update, context):
        return
    
    query = " ".join(context.args) if context.args else None
    
    if not query:
        examples = {
            "number": "/num 9876543210",
            "aadhar": "/aadhar 123456789012",
            "imei": "/imei 123456789012345",
            "rto": "/rto UP32AB1234",
            "tg": "/tg 123456789"
        }
        await update.message.reply_text(
            f"Usage: <code>{examples[service]}</code>\n\n"
            f"<b>Points required:</b> {POINTS[service]}",
            parse_mode="HTML"
        )
        return
    
    validations = {
        "number": lambda x: x.isdigit() and len(x) == 10,
        "aadhar": lambda x: x.isdigit() and len(x) == 12,
        "imei": lambda x: x.isdigit() and len(x) == 15,
        "rto": lambda x: len(x) >= 6,
        "tg": lambda x: x.isdigit()
    }
    
    if not validations[service](query):
        await update.message.reply_text(f"❌ Invalid {service} format!", parse_mode="HTML")
        return
    
    if not rate_limiter.is_allowed(str(update.effective_user.id)):
        await update.message.reply_text("⚠️ Slow down! Wait a minute.")
        return
    
    msg = await update.message.reply_text("🔍 Searching...")
    
    endpoints = {
        "number": "/lookup",
        "aadhar": "/aadhar",
        "imei": "/imei",
        "rto": "/rto",
        "tg": "/tg"
    }
    
    params = {
        "number": {"number": query},
        "aadhar": {"aadhar": query},
        "imei": {"imei": query},
        "rto": {"rc": query},
        "tg": {"userid": query}
    }
    
    bot_token = context.bot.token if context.bot else None
    
    data, success = await call_api(
        endpoints[service],
        params[service],
        API_KEYS[service],
        POINTS[service],
        update.effective_user.id,
        service,
        bot_token=bot_token
    )
    
    if not success and "Insufficient points" in str(data.get("error", "")):
        await msg.edit_text(
            f"❌ <b>Insufficient Points!</b>\n\n"
            f"Required: {POINTS[service]} points\n"
            f"Earn more via referral or promo codes!",
            parse_mode="HTML"
        )
    else:
        await msg.edit_text(
            format_api_response(data, service),
            parse_mode="HTML"
        )

# Command handlers for each service
async def number_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await search_handler(update, context, "number")

async def aadhar_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await search_handler(update, context, "aadhar")

async def imei_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await search_handler(update, context, "imei")

async def rto_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await search_handler(update, context, "rto")

async def tg_lookup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await search_handler(update, context, "tg")

# ======================================================
# REFERRAL SYSTEM
# ======================================================
async def referral_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_force_join(update, context):
        return
    
    user = await users_collection.find_one({"telegram_id": str(update.effective_user.id)})
    
    if context.args and context.args[0] == "code":
        await update.message.reply_text(
            f"<b>🔗 Your Referral Code:</b>\n"
            f"<code>{user['referral_code']}</code>\n\n"
            f"Share this code with friends!\n"
            f"When they join using this code, you get <b>{REFERRAL_BONUS['referrer']} points</b>",
            parse_mode="HTML"
        )
    elif context.args and context.args[0] == "stats":
        referrals = await referrals_collection.find(
            {"referrer_id": str(update.effective_user.id)}
        ).to_list(length=100)
        
        text = (
            f"<b>📊 Referral Statistics</b>\n\n"
            f"👥 <b>Total Referrals:</b> {len(referrals)}\n"
            f"💰 <b>Points Earned:</b> {len(referrals) * REFERRAL_BONUS['referrer']}\n"
            f"🔗 <b>Your Code:</b> <code>{user['referral_code']}</code>\n\n"
            f"<i>Recent referrals:</i>\n"
        )
        
        for ref in referrals[-5:]:
            text += f"• User joined: {ref['timestamp'].strftime('%Y-%m-%d')}\n"
        
        await update.message.reply_text(text, parse_mode="HTML")
    else:
        bot_username = (await context.bot.get_me()).username
        referral_link = f"https://t.me/{bot_username}?start=ref_{user['referral_code']}"
        
        keyboard = [
            [InlineKeyboardButton("🔗 Copy Link", url=referral_link)],
            [InlineKeyboardButton("📋 My Code", callback_data="ref_mycode"),
             InlineKeyboardButton("📊 Stats", callback_data="ref_stats")]
        ]
        
        await update.message.reply_text(
            f"<b>👥 Referral Program</b>\n\n"
            f"Invite friends and earn points!\n\n"
            f"<b>🎁 Rewards:</b>\n"
            f"• You get: <b>{REFERRAL_BONUS['referrer']} points</b>\n"
            f"• Friend gets: <b>{REFERRAL_BONUS['referee']} points</b>\n\n"
            f"<b>🔗 Your Referral Link:</b>\n"
            f"{referral_link}",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

# ======================================================
# PROMO CODE SYSTEM
# ======================================================
async def promo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_force_join(update, context):
        return
    
    if not context.args:
        await update.message.reply_text(
            "Usage: /promo YOUR_CODE\n\n"
            "Enter a promo code to get bonus points!",
            parse_mode="HTML"
        )
        return
    
    code = context.args[0].upper()
    promo = await promo_codes_collection.find_one({"code": code})
    
    if not promo:
        await update.message.reply_text("❌ Invalid promo code!", parse_mode="HTML")
        return
    
    if promo.get("expiry") and promo["expiry"] < datetime.utcnow():
        await update.message.reply_text("❌ Promo code expired!", parse_mode="HTML")
        return
    
    if promo["type"] == "single":
        used = await transactions_collection.find_one({
            "promo_code": code,
            "type": "promo_credit"
        })
        if used:
            await update.message.reply_text("❌ This promo code has already been used!", parse_mode="HTML")
            return
    elif promo["type"] == "universal":
        used = await transactions_collection.find_one({
            "user_id": str(update.effective_user.id),
            "promo_code": code,
            "type": "promo_credit"
        })
        if used:
            await update.message.reply_text("❌ You have already used this promo code!", parse_mode="HTML")
            return
    
    await users_collection.update_one(
        {"telegram_id": str(update.effective_user.id)},
        {
            "$inc": {
                "points": promo["points"],
                "total_points_earned": promo["points"]
            }
        }
    )
    
    await transactions_collection.insert_one({
        "user_id": str(update.effective_user.id),
        "type": "promo_credit",
        "amount": promo["points"],
        "promo_code": code,
        "timestamp": datetime.utcnow()
    })
    
    await update.message.reply_text(
        f"✅ <b>Promo Code Applied!</b>\n\n"
        f"You received: <b>{promo['points']} points</b>",
        parse_mode="HTML"
    )

# ======================================================
# ADMIN COMMANDS
# ======================================================
async def admin_add_promo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    
    if len(context.args) < 3:
        await update.message.reply_text(
            "Usage: /addpromo CODE TYPE POINTS [expiry_days]\n\n"
            "TYPE: single or universal\n"
            "Example: /addpromo WELCOME100 universal 100 30",
            parse_mode="HTML"
        )
        return
    
    code = context.args[0].upper()
    ptype = context.args[1].lower()
    points = int(context.args[2])
    expiry_days = int(context.args[3]) if len(context.args) > 3 else None
    
    promo = {
        "code": code,
        "type": ptype,
        "points": points,
        "created_by": str(update.effective_user.id),
        "created_at": datetime.utcnow()
    }
    
    if expiry_days:
        promo["expiry"] = datetime.utcnow() + timedelta(days=expiry_days)
    
    await promo_codes_collection.insert_one(promo)
    
    expiry_text = f"Expires: {expiry_days} days" if expiry_days else "No expiry"
    await update.message.reply_text(
        f"✅ Promo code created!\n\n"
        f"Code: <code>{code}</code>\n"
        f"Type: {ptype}\n"
        f"Points: {points}\n"
        f"{expiry_text}",
        parse_mode="HTML"
    )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    
    total_users = await users_collection.count_documents({})
    total_bots = await bots_collection.count_documents({})
    running_bots = await bots_collection.count_documents({"status": "running"})
    
    pipeline = [{"$group": {"_id": None, "total": {"$sum": "$search_count"}}}]
    agg_result = await bots_collection.aggregate(pipeline).to_list(length=1)
    total_searches = agg_result[0]["total"] if agg_result else 0
    
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_searches = await search_stats_collection.count_documents({
        "timestamp": {"$gte": today_start}
    })
    
    top_users = await users_collection.find().sort("total_searches", -1).limit(5).to_list(length=5)
    top_users_text = ""
    for u in top_users:
        top_users_text += f"• {u.get('first_name', 'Unknown')}: {u.get('total_searches', 0)} searches\n"
    
    text = (
        f"<b>📊 ADMIN STATISTICS</b>\n\n"
        f"<b>👥 Total Users:</b> {total_users}\n"
        f"<b>🤖 Total Bots:</b> {total_bots}\n"
        f"<b>🟢 Running Bots:</b> {running_bots}\n"
        f"<b>🔍 Total Searches:</b> {total_searches}\n"
        f"<b>📅 Today's Searches:</b> {today_searches}\n\n"
        f"<b>🏆 Top Users:</b>\n{top_users_text}\n"
        f"<b>💰 Total Points Distributed:</b> {await get_total_points()}"
    )
    
    await update.message.reply_text(text, parse_mode="HTML")

async def get_total_points():
    pipeline = [{"$group": {"_id": None, "total": {"$sum": "$total_points_earned"}}}]
    agg_result = await users_collection.aggregate(pipeline).to_list(length=1)
    return agg_result[0]["total"] if agg_result else 0

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized!")
        return
    
    if not context.args:
        await update.message.reply_text("Usage: /broadcast Your message here")
        return
    
    message = " ".join(context.args)
    sent = failed = 0
    
    async for user in users_collection.find({}):
        try:
            await context.bot.send_message(
                chat_id=int(user["telegram_id"]),
                text=f"<b>📢 Broadcast Message</b>\n\n{message}",
                parse_mode="HTML"
            )
            sent += 1
            await asyncio.sleep(0.05)
        except:
            failed += 1
    
    await update.message.reply_text(f"✅ Broadcast sent!\nSent: {sent}\nFailed: {failed}")

# ======================================================
# BOT MANAGEMENT
# ======================================================
class BotInstance:
    def __init__(self, token: str, owner_id: str, username: str = None):
        self.token = token
        self.owner_id = str(owner_id)
        self.username = username
        self.application = None
        self.running = False

    async def start(self):
        try:
            app = ApplicationBuilder().token(self.token).build()
            self._register_handlers(app)
            
            await app.initialize()
            await app.bot.delete_webhook(drop_pending_updates=True)
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            
            self.application = app
            self.running = True
            
            if not self.username:
                info = await app.bot.get_me()
                self.username = info.username
            
            print(f"[Bot] ✅ @{self.username} started")
            return True
            
        except Exception as e:
            print(f"[Bot] ❌ Failed: {e}")
            return False

    def _register_handlers(self, app):
        app.add_handler(CommandHandler("start", start_command))
        app.add_handler(CommandHandler("num", number_lookup))
        app.add_handler(CommandHandler("aadhar", aadhar_lookup))
        app.add_handler(CommandHandler("imei", imei_lookup))
        app.add_handler(CommandHandler("rto", rto_lookup))
        app.add_handler(CommandHandler("tg", tg_lookup))
        app.add_handler(CommandHandler("referral", referral_command))
        app.add_handler(CommandHandler("promo", promo_command))
        app.add_handler(CommandHandler("points", points_command))
        app.add_handler(CommandHandler("add_bot", add_bot_command))
        app.add_handler(CommandHandler("my_bots", my_bots_command))
        app.add_handler(CommandHandler("remove_bot", remove_bot_command))
        app.add_handler(CommandHandler("help", help_command))
        app.add_handler(CommandHandler("stats", stats_command))
        
        if is_admin(int(self.owner_id)):
            app.add_handler(CommandHandler("addpromo", admin_add_promo))
            app.add_handler(CommandHandler("adminstats", admin_stats))
            app.add_handler(CommandHandler("broadcast", admin_broadcast))
        
        app.add_handler(CallbackQueryHandler(button_handler))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

# ======================================================
# BOT MANAGER
# ======================================================
class BotManager:
    def __init__(self):
        self.instances: Dict[str, BotInstance] = {}
    
    async def add_bot(self, token: str, owner_id: str) -> Tuple[bool, str]:
        try:
            test_app = ApplicationBuilder().token(token).build()
            await test_app.initialize()
            bot_info = await test_app.bot.get_me()
            await test_app.shutdown()
            
            existing = await bots_collection.find_one({"token": token})
            if existing:
                return False, "Bot already registered"
            
            inst = BotInstance(token, owner_id, bot_info.username)
            success = await inst.start()
            
            if success:
                self.instances[token] = inst
                
                await bots_collection.insert_one({
                    "token": token,
                    "username": bot_info.username,
                    "owner_id": str(owner_id),
                    "status": "running",
                    "search_count": 0,
                    "user_count": 0,
                    "created_at": datetime.utcnow()
                })
                
                return True, f"✅ @{bot_info.username} started successfully!"
            else:
                return False, "Failed to start bot"
                
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    async def remove_bot(self, token: str, user_id: str) -> Tuple[bool, str]:
        inst = self.instances.get(token)
        if not inst:
            return False, "Bot not found"
        
        if inst.owner_id != str(user_id) and not is_admin(int(user_id)):
            return False, "Not authorized"
        
        await inst.application.stop()
        await inst.application.shutdown()
        del self.instances[token]
        await bots_collection.delete_one({"token": token})
        
        return True, "✅ Bot removed"
    
    async def get_user_bots(self, user_id) -> List[dict]:
        if is_admin(int(user_id)):
            cursor = bots_collection.find()
        else:
            cursor = bots_collection.find({"owner_id": str(user_id)})
        return await cursor.to_list(length=100)

bot_manager = BotManager()

# ======================================================
# COMMAND HANDLERS
# ======================================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    
    referrer_id = None
    if context.args and context.args[0].startswith("ref_"):
        ref_code = context.args[0][4:]
        referrer = await users_collection.find_one({"referral_code": ref_code})
        if referrer:
            referrer_id = referrer["telegram_id"]
    
    await get_or_create_user(user.id, user.username, user.first_name, referrer_id)
    await show_main_menu(update, context)

async def points_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_force_join(update, context):
        return
    
    user = await users_collection.find_one({"telegram_id": str(update.effective_user.id)})
    
    transactions = await transactions_collection.find(
        {"user_id": str(update.effective_user.id)}
    ).sort("timestamp", -1).limit(5).to_list(length=5)
    
    trans_text = ""
    for t in transactions:
        if t["type"] == "deduction":
            trans_text += f"• 🔍 {t['service']}: -{t['amount']} points\n"
        elif t["type"] == "referral_bonus":
            trans_text += f"• 👥 Referral: +{t['amount']} points\n"
        elif t["type"] == "promo_credit":
            trans_text += f"• 🎫 Promo: +{t['amount']} points\n"
    
    text = (
        f"<b>💰 Your Points</b>\n\n"
        f"<b>Current Balance:</b> <code>{user.get('points', 0)}</code>\n"
        f"<b>Total Earned:</b> <code>{user.get('total_points_earned', 0)}</code>\n"
        f"<b>Total Searches:</b> <code>{user.get('total_searches', 0)}</code>\n"
        f"<b>Referrals:</b> <code>{user.get('referral_count', 0)}</code>\n\n"
        f"<b>Recent Activity:</b>\n{trans_text if trans_text else 'No recent activity'}"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def add_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_force_join(update, context):
        return
    
    if not context.args:
        await update.message.reply_text(
            "Usage: /add_bot BOT_TOKEN\n\n"
            "Get token from @BotFather",
            parse_mode="HTML"
        )
        return
    
    token = context.args[0].strip()
    msg = await update.message.reply_text("⏳ Adding bot...")
    
    success, message = await bot_manager.add_bot(token, update.effective_user.id)
    await msg.edit_text(message, parse_mode="HTML")

async def my_bots_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await check_force_join(update, context):
        return
    
    bots = await bot_manager.get_user_bots(update.effective_user.id)
    
    if not bots:
        await update.message.reply_text("You have no bots yet. Use /add_bot to add one.")
        return
    
    text = "<b>📋 Your Bots:</b>\n\n"
    keyboard = []
    
    for bot in bots:
        status_emoji = "🟢" if bot.get("status") == "running" else "🔴"
        text += f"{status_emoji} <b>@{bot['username']}</b>\n"
        text += f"   Token: <code>{bot['token'][:8]}...</code>\n"
        text += f"   Status: {bot.get('status', 'unknown')}\n"
        text += f"   Searches: {bot.get('search_count', 0)}\n\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"❌ Remove @{bot['username']}",
                callback_data=f"remove_{bot['token'][:8]}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")])
    
    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def remove_bot_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /remove_bot TOKEN_PREFIX")
        return
    
    prefix = context.args[0]
    bot = await bots_collection.find_one({"token": {"$regex": f"^{prefix}"}})
    
    if not bot:
        await update.message.reply_text("❌ Bot not found")
        return
    
    success, message = await bot_manager.remove_bot(bot["token"], update.effective_user.id)
    await update.message.reply_text(message)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "<b>❓ Help & Commands</b>\n\n"
        "<b>🔍 Search Commands:</b>\n"
        "• /num NUMBER - Mobile number lookup (3 pts)\n"
        "• /aadhar NUMBER - Aadhar lookup (8 pts)\n"
        "• /imei NUMBER - IMEI lookup (4 pts)\n"
        "• /rto RC_NUMBER - RTO/RC lookup (5 pts)\n"
        "• /tg ID - Telegram ID lookup (10 pts)\n\n"
        "<b>👥 Referral System:</b>\n"
        "• /referral - Get your referral link\n"
        "• /referral code - Show your code\n"
        "• /referral stats - View referral stats\n\n"
        "<b>💰 Points & Promo:</b>\n"
        "• /points - Check your points\n"
        "• /promo CODE - Redeem promo code\n\n"
        "<b>🤖 Bot Management:</b>\n"
        "• /add_bot TOKEN - Add your own bot\n"
        "• /my_bots - List your bots\n"
        "• /remove_bot PREFIX - Remove a bot\n\n"
        "<b>📊 Others:</b>\n"
        "• /stats - System statistics\n"
        "• /start - Main menu"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    total_users = await users_collection.count_documents({})
    total_bots = await bots_collection.count_documents({})
    running_bots = await bots_collection.count_documents({"status": "running"})
    
    pipeline = [{"$group": {"_id": None, "total": {"$sum": "$search_count"}}}]
    agg_result = await bots_collection.aggregate(pipeline).to_list(length=1)
    total_searches = agg_result[0]["total"] if agg_result else 0
    
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    today_searches = await search_stats_collection.count_documents({
        "timestamp": {"$gte": today_start}
    })
    
    text = (
        f"<b>📊 System Statistics</b>\n\n"
        f"<b>👥 Total Users:</b> {total_users}\n"
        f"<b>🤖 Active Bots:</b> {running_bots}/{total_bots}\n"
        f"<b>🔍 Total Searches:</b> {total_searches}\n"
        f"<b>📅 Today's Searches:</b> {today_searches}\n"
        f"<b>⚡ API Status:</b> Online\n"
        f"<b>📡 Channel:</b> @blackapibox"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Please use commands.\nType /help to see available commands."
    )

# ======================================================
# BUTTON HANDLER
# ======================================================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # Re-check force join when user taps "I Joined" button
    if data == "check_force_join":
        if await check_force_join(update, context):
            await show_main_menu(update, context)
        return
    
    user = await users_collection.find_one({"telegram_id": str(update.effective_user.id)})
    
    if data == "menu_main":
        await show_main_menu(update, context)
    
    elif data == "menu_number":
        text = (
            f"<b>🔍 Number Lookup</b>\n\n"
            f"<b>Points required:</b> {POINTS['number']}\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/num 9876543210</code>\n\n"
            f"<b>Your balance:</b> {user.get('points', 0)} points"
        )
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_aadhar":
        text = (
            f"<b>🆔 Aadhar Lookup</b>\n\n"
            f"<b>Points required:</b> {POINTS['aadhar']}\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/aadhar 123456789012</code>\n\n"
            f"<b>Your balance:</b> {user.get('points', 0)} points"
        )
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_imei":
        text = (
            f"<b>📱 IMEI Lookup</b>\n\n"
            f"<b>Points required:</b> {POINTS['imei']}\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/imei 123456789012345</code>\n\n"
            f"<b>Your balance:</b> {user.get('points', 0)} points"
        )
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_rto":
        text = (
            f"<b>🚗 RTO Lookup</b>\n\n"
            f"<b>Points required:</b> {POINTS['rto']}\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/rto UP32AB1234</code>\n\n"
            f"<b>Your balance:</b> {user.get('points', 0)} points"
        )
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_tg":
        text = (
            f"<b>📲 Telegram ID Lookup</b>\n\n"
            f"<b>Points required:</b> {POINTS['tg']}\n\n"
            f"<b>Usage:</b>\n"
            f"<code>/tg 123456789</code>\n\n"
            f"<b>Your balance:</b> {user.get('points', 0)} points"
        )
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_referral":
        bot_username = (await context.bot.get_me()).username
        referral_link = f"https://t.me/{bot_username}?start=ref_{user['referral_code']}"
        
        text = (
            f"<b>👥 Referral Program</b>\n\n"
            f"<b>Your Code:</b> <code>{user['referral_code']}</code>\n"
            f"<b>Referrals:</b> {user.get('referral_count', 0)}\n\n"
            f"<b>🎁 Rewards:</b>\n"
            f"• You get: <b>{REFERRAL_BONUS['referrer']} points</b>\n"
            f"• Friend gets: <b>{REFERRAL_BONUS['referee']} points</b>\n\n"
            f"<b>🔗 Your Link:</b>\n"
            f"{referral_link}"
        )
        
        keyboard = [
            [InlineKeyboardButton("🔗 Copy Link", url=referral_link)],
            [InlineKeyboardButton("📊 Referral Stats", callback_data="ref_stats")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]
        ]
        
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data == "ref_stats":
        referrals = await referrals_collection.find(
            {"referrer_id": str(update.effective_user.id)}
        ).to_list(length=100)
        
        text = (
            f"<b>📊 Your Referral Stats</b>\n\n"
            f"<b>Total Referrals:</b> {len(referrals)}\n"
            f"<b>Points Earned:</b> {len(referrals) * REFERRAL_BONUS['referrer']}\n\n"
            f"<b>Recent Referrals:</b>\n"
        )
        
        for ref in referrals[-5:]:
            text += f"• {ref['timestamp'].strftime('%Y-%m-%d %H:%M')}\n"
        
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Back", callback_data="menu_referral")
            ]])
        )
    
    elif data == "menu_points":
        transactions = await transactions_collection.find(
            {"user_id": str(update.effective_user.id)}
        ).sort("timestamp", -1).limit(5).to_list(length=5)
        
        trans_text = ""
        for t in transactions:
            if t["type"] == "deduction":
                trans_text += f"• 🔍 {t['service']}: -{t['amount']} points\n"
            elif t["type"] == "referral_bonus":
                trans_text += f"• 👥 Referral: +{t['amount']} points\n"
            elif t["type"] == "promo_credit":
                trans_text += f"• 🎫 Promo: +{t['amount']} points\n"
        
        text = (
            f"<b>💰 Your Points</b>\n\n"
            f"<b>Current Balance:</b> <code>{user.get('points', 0)}</code>\n"
            f"<b>Total Earned:</b> <code>{user.get('total_points_earned', 0)}</code>\n"
            f"<b>Total Searches:</b> <code>{user.get('total_searches', 0)}</code>\n\n"
            f"<b>Recent Activity:</b>\n{trans_text if trans_text else 'No recent activity'}"
        )
        
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_promo":
        text = (
            f"<b>🎫 Promo Codes</b>\n\n"
            f"Use promo codes to get bonus points!\n\n"
            f"<b>How to use:</b>\n"
            f"<code>/promo YOUR_CODE</code>\n\n"
            f"<b>Your balance:</b> {user.get('points', 0)} points"
        )
        
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")
            ]])
        )
    
    elif data == "menu_stats":
        await stats_command(update, context)
    
    elif data == "menu_help":
        await help_command(update, context)
    
    elif data == "menu_mybots":
        bots = await bot_manager.get_user_bots(update.effective_user.id)
        
        if not bots:
            await query.edit_message_text(
                "You have no bots yet.\nUse /add_bot to add one.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")
                ]])
            )
            return
        
        text = "<b>📋 Your Bots:</b>\n\n"
        keyboard = []
        
        for bot in bots:
            status_emoji = "🟢" if bot.get("status") == "running" else "🔴"
            text += f"{status_emoji} <b>@{bot['username']}</b>\n"
            text += f"   Searches: {bot.get('search_count', 0)}\n\n"
            
            keyboard.append([
                InlineKeyboardButton(
                    f"❌ Remove @{bot['username']}",
                    callback_data=f"remove_{bot['token'][:8]}"
                )
            ])
        
        keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")])
        
        await query.edit_message_text(
            text, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("remove_"):
        prefix = data[7:]
        bot = await bots_collection.find_one({"token": {"$regex": f"^{prefix}"}})
        
        if not bot:
            await query.edit_message_text("❌ Bot not found")
            return
        
        keyboard = [[
            InlineKeyboardButton("✅ Yes", callback_data=f"confirm_remove_{prefix}"),
            InlineKeyboardButton("❌ No", callback_data="menu_mybots")
        ]]
        
        await query.edit_message_text(
            f"⚠️ Remove @{bot['username']}?",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    elif data.startswith("confirm_remove_"):
        prefix = data[15:]
        bot = await bots_collection.find_one({"token": {"$regex": f"^{prefix}"}})
        
        if not bot:
            await query.edit_message_text("❌ Bot not found")
            return
        
        success, message = await bot_manager.remove_bot(bot["token"], update.effective_user.id)
        
        await query.edit_message_text(
            message,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 My Bots", callback_data="menu_mybots")
            ]])
        )

# ======================================================
# WEB SERVER
# ======================================================
async def run_web_server():
    async def health(request):
        running = sum(1 for i in bot_manager.instances.values() if i.running)
        total_users = await users_collection.count_documents({})
        total_searches = await search_stats_collection.count_documents({})
        
        return web.Response(
            text=json.dumps({
                "status": "online",
                "bots_running": running,
                "total_users": total_users,
                "total_searches": total_searches
            }),
            content_type="application/json"
        )
    
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    print(f"✅ Web server on port {port}")
    
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        await runner.cleanup()

# ======================================================
# MAIN
# ======================================================
async def main():
    print("""
    ╔══════════════════════════════════════╗
    ║    DEMON_KILLER MULTI-BOT SYSTEM     ║
    ║         Ready for Action! 😈🔥        ║
    ╚══════════════════════════════════════╝
    """)
    
    await init_db()
    asyncio.create_task(run_web_server())
    
    master_bot = BotInstance(MASTER_TOKEN, str(OWNER_ID))
    success = await master_bot.start()
    
    if not success:
        print("❌ Failed to start master bot!")
        sys.exit(1)
    
    async for bot_data in bots_collection.find({"token": {"$ne": MASTER_TOKEN}}):
        if bot_data["token"] not in bot_manager.instances:
            inst = BotInstance(
                bot_data["token"],
                bot_data["owner_id"],
                bot_data.get("username")
            )
            await inst.start()
            bot_manager.instances[bot_data["token"]] = inst
    
    print("✅ All bots running!")
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
        for inst in bot_manager.instances.values():
            await inst.application.stop()
            await inst.application.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBye! 👋")
        sys.exit(0)
