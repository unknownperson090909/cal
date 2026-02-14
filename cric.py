"""
CRICOVERSE - Professional Hand Cricket Telegram Bot
A feature-rich, group-based Hand Cricket game engine
Fixed version - No circular imports
"""

# ═══════════════════════════════════════════════════════════════
# STEP 1: Import all standard libraries first
# ═══════════════════════════════════════════════════════════════
import logging
import asyncio
import random
import math
import time
import json
import sqlite3
import shutil
import os
import html
import io
import requests
from asyncio import Lock
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
from enum import Enum
from io import BytesIO
try:
    import pandas as pd
except ImportError:
    pd = None
try:
    import psutil
except ImportError:
    psutil = None

# ═══════════════════════════════════════════════════════════════
# STEP 2: Import third-party libraries
# ═══════════════════════════════════════════════════════════════
from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageEnhance, ImageChops, ImageOps

from telegram import (
    Update, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    ChatMember,
    InputMediaPhoto
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ChatMemberHandler,
    ContextTypes,
    filters
)
from telegram.constants import ParseMode
from telegram.error import TelegramError, Forbidden

# ═══════════════════════════════════════════════════════════════
# STEP 3: Configure logging AFTER all imports
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


BOT_TOKEN = "8428604292:AAFkogxA9yUKMSO9uhPX-9s0DjBKzVceW3U"
OWNER_ID = 7460266461  # Replace with your Telegram user ID
SECOND_APPROVER_ID = 7343683772 
SUPPORT_GROUP_ID = -1002707382739  # Replace with your support group ID

auction_locks = defaultdict(asyncio.Lock)

DB_PATH = "resume1.db"
TOURNAMENT_DB_PATH = "tournament.db"  # Tournament registration database

command_locks: Dict[int, Lock] = defaultdict(Lock)  # Per-group command lock
processing_commands: Dict[int, bool] = defaultdict(bool)
last_command_time: Dict[int, float] = defaultdict(float)
COMMAND_TIMEOUT = 5.0
POWERED_USERS: Set[int] = set()

# Custom Image File IDs (replace with your actual file IDs from bot)
PLAYER_IMAGE_FILE_ID = "YOUR_PLAYER_IMAGE_FILE_ID_HERE"
MYSTATS_IMAGE_FILE_ID = "YOUR_MYSTATS_IMAGE_FILE_ID_HERE"

# Tournament Registration System
TOURNAMENT_APPROVED_GROUPS: Set[int] = set()  # Groups approved for tournaments
REGISTRATION_ACTIVE: Dict[int, Dict] = {}  # group_id: {end_date, days}
TOURNAMENT_POWER_USERS: Set[Tuple[int, int]] = set()  # (user_id, group_id) pairs who can view lists

  # Users with broadcast + groupapprove permissions

# Game Constants
class GamePhase(Enum):
    IDLE = "idle"
    # ... Team Phases ...
    TEAM_JOINING = "team_joining"
    HOST_SELECTION = "host_selection"
    CAPTAIN_SELECTION = "captain_selection"
    TEAM_EDIT = "team_edit"
    OVER_SELECTION = "over_selection"
    TOSS = "toss"
    MATCH_IN_PROGRESS = "match_in_progress"
    INNINGS_BREAK = "innings_break"
    MATCH_ENDED = "match_ended"
    SUPER_OVER = "super_over"
    SUPER_OVER_BREAK = "super_over_break"    

    # ... SOLO PHASES (New) ...
    SOLO_JOINING = "solo_joining"
    SOLO_MATCH = "solo_match"
    
    # ... AI MODE (DM Play vs AI) ...
    AI_MODE = "ai_mode"

class AuctionPhase(Enum):
    IDLE = "idle"
    BIDDER_SELECTION = "bidder_selection"
    PLAYER_ADDITION = "player_addition"
    AUCTION_LIVE = "auction_live"
    AUCTION_ENDED = "auction_ended"

class AuctionTeam:
    """Represents a team in auction"""
    def __init__(self, name: str):
        self.name = name
        self.bidder_id: Optional[int] = None
        self.bidder_name: str = ""
        self.players: List[Dict] = []  # {player_id, player_name, price}
        self.purse_remaining = 1000
        self.total_spent = 0
    
    def add_player(self, player_id: int, player_name: str, price: int):
        self.players.append({
            "player_id": player_id,
            "player_name": player_name,
            "price": price
        })
        self.purse_remaining -= price
        self.total_spent += price

class MatchEvent(Enum):
    DOT_BALL = "dot"
    RUNS_1 = "1run"
    RUNS_2 = "2runs"
    RUNS_3 = "3runs"
    RUNS_4 = "4runs"
    RUNS_5 = "5runs"
    RUNS_6 = "6runs"
    WICKET = "wicket"
    NO_BALL = "noball"
    WIDE = "wide"
    FREE_HIT = "freehit"
    DRS_REVIEW = "drs_review"
    DRS_OUT = "drs_out"
    DRS_NOT_OUT = "drs_notout"
    INNINGS_BREAK = "innings_break"
    VICTORY = "victory"


class Auction:
    """Main auction class"""
    def __init__(self, group_id: int, group_name: str, host_id: int, host_name: str):
        self.group_id = group_id
        self.group_name = group_name
        self.host_id = host_id
        self.host_name = host_name
        self.phase = AuctionPhase.BIDDER_SELECTION
        self.created_at = datetime.now()
        
        # Teams
        self.teams: Dict[str, AuctionTeam] = {}
        
        # Auctioneer
        self.auctioneer_id: Optional[int] = None
        self.auctioneer_name: str = ""
        self.auctioneer_change_votes: Set[int] = set()
        
        # Current auction
        self.current_player_id: Optional[int] = None
        self.current_player_name: str = ""
        self.current_base_price = 0
        self.current_highest_bid = 0
        self.current_highest_bidder: Optional[str] = None
        self.bid_timer_task: Optional[asyncio.Task] = None
        self.bid_end_time: Optional[float] = None
        
        # Player pool
        self.player_pool: List[Dict] = []  # {player_id, player_name, base_price}
        self.sold_players: List[Dict] = []
        self.unsold_players: List[Dict] = []
        
        # Assist mode
        self.assist_mode: Dict[str, bool] = {}  # team_name: is_assisted
        
        # UI
        self.main_message_id: Optional[int] = None



# ═══════════════════════════════════════════════════════════════
# LOCK MANAGEMENT HELPERS - Fix for Multiple GC Delay
# ═══════════════════════════════════════════════════════════════

async def acquire_command_lock(group_id: int, timeout: float = 10.0) -> bool:
    """Try to acquire command lock with timeout"""
    lock = command_locks[group_id]
    try:
        await asyncio.wait_for(lock.acquire(), timeout=timeout)
        last_command_time[group_id] = time.time()
        return True
    except asyncio.TimeoutError:
        logger.warning(f"Command lock timeout for group {group_id}")
        return False


def release_command_lock(group_id: int):
    """Release command lock for a group"""
    lock = command_locks[group_id]
    if lock.locked():
        lock.release()


async def cleanup_stale_locks():
    """Clean up locks that have been held too long"""
    current_time = time.time()
    for group_id, last_time in list(last_command_time.items()):
        if current_time - last_time > COMMAND_TIMEOUT:
            logger.warning(f"Cleaning up stale lock for group {group_id}")
            release_command_lock(group_id)
            del last_command_time[group_id]


async def start_lock_cleanup_task():
    """Background task to cleanup stale locks"""
    while True:
        await cleanup_stale_locks()
        await asyncio.sleep(60)  # Check every minute


# GIF URLs for match events
GIFS = {
    MatchEvent.DOT_BALL: [
        "CgACAgQAAyEFAATEuZi2AAIEsmlL3oS80G_hP2r73pB1Xp9fja2TAAJ2EwACARH5UrcBeu1Hx7x-NgQ"
    ],
    MatchEvent.RUNS_1: [
        "CgACAgUAAyEFAATEuZi2AAIE_mlL51w3IW0jthJmfZeMqNVpFRfUAAIiLQACeNjhVxT4d9Xn2PI-NgQ",
        "CgACAgUAAyEFAATU3pgLAAIKkWlE9Gqtq1mAjiu926NvWRGfxQW1AAIJHAACOMrpVwhrNvXoibUAATYE",
        "CgACAgUAAyEFAATEuZi2AAIFYmlL7mOnFTT69LGMLS9G2oA6EHJpAAJsHQACRV1hVnaw6OSdIDwQNgQ"
    ],
    MatchEvent.RUNS_2: [
        "CgACAgUAAyEFAATEuZi2AAIFK2lL6X8FPyJRYp9RbF6DiAAB-RqzvAACWR0AAkVdYVY9UqOGM0nDajYE",
        "CgACAgUAAyEFAATU3pgLAAIKi2lE9GrIvY93_Dcaiv8zaa0IbES6AALJGgACN2_pV4f4uWRTw9wxNgQ"
    ],
    MatchEvent.RUNS_3: [
        "CgACAgUAAyEFAATEuZi2AAIFQGlL64CXO07OHbHMip1g2Lu0HFayAAJlHQACRV1hVkXx8RdRbQniNgQ",
        "CgACAgUAAyEFAATU3pgLAAIKf2lE9Gq72p6bgh1C8K9SjTyciqXfAAI2DwACPzbQVnca7Od2bSquNgQ"
    ],
    MatchEvent.RUNS_4: [
        "CgACAgUAAyEGAATYx4tPAAJIvmlMBASE6vZ-FK1_CKrtrHRpUi5WAAJSCAACD_YgVo49O55ICLAENgQ",
        "CgACAgUAAxkBAAIKY2lNWXZwCPa1mikPTuiI-im6KsXZAALbCgAC5WCoVXTWQ_MhLqz4NgQ",
        "CgACAgUAAyEGAATYx4tPAAJKRGlM-l-WWxsOUMrQJWlDsnrShZALAAKtDAACFqM4VMeSD_FLQu8MNgQ",
        "CgACAgUAAyEGAAShX2HTAAIgpWlMOtRIxiwO5A91S3qnzJ3hNJpFAAJTBgACmdE5V_Z3vM_sBDZCNgQ",
        "CgACAgUAAyEFAATYx4tPAAJDtWlLmks4fC6UZFYmqqV_i-B8_jC1AAJcFwACITAQVA4cFTAQ7BfKNgQ"
    ],
    MatchEvent.RUNS_5: [
        "CgACAgQAAyEFAATU3pgLAAIKiGlE9GoYG_0qTVEd3Le7R6qvyWrWAAJeGwACryS5UH5WGCXTJywAATYE",
        "CgACAgQAAyEFAATEuZi2AAIE6mlL4TanjQPWyDaNCpaXtOq-CVtOAAJ_IAACMudhUlC2yWKM8GmFNgQ",
        "CgACAgUAAyEFAATEuZi2AAIFTWlL7Eq7OGaFKKEfosOF_jAtHWTUAALmHAAChf5gVivH4SvOeCpRNgQ"
    ],
    MatchEvent.RUNS_6: [
        "CgACAgUAAyEGAAShX2HTAAIhH2lMRrNUrjRV4GW2K8booBvMtTG9AAKrCgACJXRpVOeF4ynzTcBoNgQ",
        "CgACAgUAAyEGAATYx4tPAAJItmlMA-mbxLqNhGcc8S785y2j5BWEAAKzDQAC9WdJVnVvz6iMeR39NgQ",
        "CgACAgUAAyEGAATYx4tPAAJHmWlL_f9GFzB3wlmreOcoJdNeQb5pAAJpAwAClZdBVj1oWzydv8lMNgQ",
        "CgACAgQAAyEFAATEuZi2AAIE6GlL4QXc1nMUBKOdGkLrPuPPYfUPAAJ-IAACMudhUqLnowABXPhb3DYE",
        "CgACAgUAAyEFAATU3pgLAAIKjmlE9GrcsVDgJe8ohHimK7JQf-MeAAJdFwACITAQVNF-Nok7Tly0NgQ",
        "CgACAgUAAyEFAATYx4tPAAJDw2lLmkzfNB56Io-uMPnGQmOTuU3wAAKJAwAC0ymZV0m1AAEE0NAEjTYE",
        "CgACAgUAAyEFAATU3pgLAAIKfGlE9GqHxSIInO0P4wSVuD5xbNiNAAJgGQACzouoVeTU9nOOeNqDNgQ"
    ],
    MatchEvent.WICKET: [
        "CgACAgQAAyEFAATU3pgLAAIKhGlE9Go2nsCXKpBBjglIQ2I3ZObsAAKvFQACaewBUkT0IZS8qdW4NgQ",
        "CgACAgQAAyEFAATU3pgLAAIKhWlE9GpEJp5SCDH35xUN97QPkkdSAAK1EwACMv1pUfLrRWYa9zWLNgQ",
        "CgACAgUAAyEFAATU3pgLAAIKhmlE9GoVK8ybgnUTS502q1YMSG35AALqAwACIHhpV7c1o-HTQNSPNgQ",
        "CgACAgQAAyEFAATU3pgLAAIKjWlE9GqL7Uad2y2fznl2ZvasOk_xAALaGQACh1UBUdFsFVeRv5qwNgQ",
        "CgACAgUAAyEGAATYx4tPAAJHa2lL_VmXp7nhZMuNPVRgbDmv54uXAAKQCAACBRCRVj5VjvOl6j21NgQ",
        "CgACAgUAAyEGAAShX2HTAAIh3WlM785mkSB-K9myKNbS1lfWmB6fAAKRBgAC_DYZVhtRUsAAAW_fvzYE"
    ],
    MatchEvent.NO_BALL: [
        "https://tenor.com/bBvYA.gif"
    ],
    MatchEvent.WIDE: [
        "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExbWdubjB0YmVuZnMwdXBwODg5MzZ0cjFsNWl4ZXN1MzltOW1yZng5dCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/YtI7H5jotPvh9Z09t6/giphy.gif"
    ],
    MatchEvent.FREE_HIT: [
        "https://t.me/cricoverse/42"
    ],
    MatchEvent.DRS_REVIEW: [
        "https://t.me/cricoverse/37"
    ],
    MatchEvent.DRS_OUT: [
        "https://pin.it/4HD5YcJOA"
    ],
    MatchEvent.DRS_NOT_OUT: [
        "https://tenor.com/bOVyJ.gif"
    ],
    MatchEvent.INNINGS_BREAK: [
    "CgACAgUAAxkBAAIjxGlViI35Zggv28khmw7xO9VzmT5IAALCDgACWnBJVhxhPkgGPYgDOAQ"
    ],
    MatchEvent.VICTORY: [
        "CgACAgUAAxkBAAIjuGlVh2s6GJm-hhGKFVH7Li3J-JOvAAI6GQACdi_xVJ8ztQiJSfOAOAQ"
    ],
    "cheer":  ["CgACAgUAAxkBAAKKVWl2fuVvv784PaPoVkdJT_1tdc6RAALfHgACPYW4V56kdGdchAbtOAQ" ],
    "auction_start": "CgACAgQAAxkBAAJnZ2lrYud8Y-r6vhLY3tguAyUJwMJtAALLAgACcAukU_kveaibnvsQOAQ",
    "auction_sold": ["BAACAgUAAxkBAAJnN2lrWHp5yb-3OW8t214Nc7lLJU1GAAL9HwACpRG5Vk6H1KifdrKLOAQ","BAACAgUAAxkBAAKKSml2eVn6AQk1xD8THAeJimIu3D1ZAAL3HwACpRG5VnwXGI-ZMZTXOAQ"],
        "new_bid": ["BAACAgUAAyEFAATU3pgLAAIhK2lXsq5nKvtUiwSNwlc4vRBN-0hQAAL1HwACpRG5Vv3LUjEcqvc3OAQ","BAACAgUAAxkBAAKKUGl2eicY0MbJQ_uv4NISNVvy0GSmAAMgAAKlEblW73Hg2GAFEus4BA", "BAACAgUAAyEFAATU3pgLAAIhBGlXsiH-BCEE2TJ8qHN6KuJRB86yAAL4HwACpRG5VjZDzwEmtaV5OAQ" ],
        "auction_unsold": "BAACAgUAAxkBAAJnZWlrYqZkyh9qAAFZXggueErKDJKlnAAC9h8AAqURuVZM9Yj2pY2qpzgE",
    "auction_countdown": "BAACAgUAAxkBAAJnY2lrYlqWBnK_1iVKqcuWYF6UJPo0AAIuGwACb3lYVzOFYEj5-RphOAQ",
}

# --- GLOBAL HELPER FUNCTION (FIXED) ---
def get_user_tag(user):
    """Returns a clickable HTML link for the user"""
    if not user:
        return "Unknown"
    
    # ✅ FIX: Handle both User objects AND Player objects
    try:
        # Case 1: Telegram User object (has .id)
        if hasattr(user, 'id'):
            user_id = user.id
            first_name = user.first_name
        # Case 2: Player object (has .user_id)
        elif hasattr(user, 'user_id'):
            user_id = user.user_id
            first_name = user.first_name
        else:
            return "Unknown"
        
        # Clean the name to prevent HTML errors
        clean_name = html.escape(first_name)
        return f"<a href='tg://user?id={user_id}'>{clean_name}</a>"
    except Exception as e:
        logger.error(f"Error creating user tag: {e}")
        return "Unknown"

# 🎨 GLOBAL MEDIA ASSETS (Safe Placeholders)
MEDIA_ASSETS = {
    "welcome": "AgACAgUAAxkBAALMImmQoBmCh25Gy5t-nEZuQCXrmlINAAIOD2sbsP15VB7-rwtRo2MJAQADAgADeQADOgQ",
    "help": "AgACAgUAAxkBAALMJGmQoCT03fIU04hOquZbp8-q3SQ0AAIND2sbsP15VBDKr20K69H7AQADAgADeQADOgQ",
    "mode_select": "AgACAgUAAxkBAALMJmmQoCzqP4IUR8uPpxQfP_TiCerjAAIMD2sbsP15VF2lqpJFGN7yAQADAgADeQADOgQ",
    "joining": "AgACAgUAAxkBAALMWmmQvIZFgc8DC8O3tQyyswuJLIpvAAKQDmsbnjKIVP9FxWHhpQZPAQADAgADeQADOgQ",
    "host": "AgACAgUAAxkBAALMKGmQoDCdzuEbxfkOjA1kwI6wZQJYAAILD2sbsP15VH3tbNMO7K2aAQADAgADeQADOgQ",
    "squads": "AgACAgUAAxkBAALMMmmQoE8ZG16NRVnSRjkjGKFo85QDAAIHD2sbsP15VJWiE-gr_FkQAQADAgADeQADOgQ",
    "toss": "AgACAgUAAxkBAALMMGmQoE4Zf_HhwlQId3Vgq0DNN1WMAAJGDmsbnjKIVNmxZFYO324iAQADAgADeQADOgQ",
    "botstats": "AgACAgUAAxkBAAId22lTp_hdHv53dZE8QVpjiaMMUPcnAAKCDGsb01NgVtTy4XXDT9DbAQADAgADeQADOAQ",
    "new_bid": "AgACAgUAAxkBAALMNGmQoFfvv_TvRIUjvFdVqrT5dnQ4AAIGD2sbsP15VAQ8WBDquCrNAQADAgADeQADOgQ", 
    "scorecard": "AgACAgUAAxkBAALMKmmQoDUL8VmATZeIi4UlNNFbpKOPAAIID2sbsP15VOb5oke00b3xAQADAgADeQADOgQ",
    "auction_setup": "AgACAgUAAxkBAALMXmmQvI42njeEH9FndRhO9AJBuzC0AAKeDmsbnjKIVNXeImbJRa2BAQADAgADeQADOgQ",
    "auction_live": "AgACAgUAAxkBAALMYGmQvJKuCbpH56II45jMKCNsUUaCAAKlDmsbnjKIVB5-WUhCqLLhAQADAgADeQADOgQ",
    "auction_end": "AgACAgUAAxkBAALMXGmQvIoo5wyAbZKrO3c_AwKPSL2IAAKXDmsbnjKIVLwkZQIZOCRFAQADAgADeQADOgQ",
}
# Commentary templates
# Ultimate Professional English Commentary (Expanded)
COMMENTARY = {
    "dot": [
        "Solid defense! No run conceded. 🧱",
        "Beaten! That was a jaffa! 🔥",
        "Straight to the fielder. Dot ball. 😐",
        "Swing and a miss! The batsman had no clue. 💨",
        "Dot ball. Pressure is building up on the batting side! 😰",
        "Respect the bowler! Good delivery in the corridor of uncertainty. 🙌",
        "No run there. Excellent fielding inside the circle. 🤐",
        "Played back to the bowler. 🤚",
        "A loud shout for LBW, but turned down. Dot ball. 🔉",
        "Good line and length. The batsman leaves it alone. 👀",
        "Can't get it through the gap. Frustration growing! 😤",
        "Top class bowling! Giving nothing away. 🔒",
        "Defended with a straight bat. Textbook cricket. 📚",
        "The batsman is struggling to time the ball. 🐢",
        "Another dot! The required run rate is creeping up. 📈"
    ],
    "single": [
        "Quick single! Good running between the wickets. 🏃‍♂️",
        "Push and run! Strike rotated smartly. 🔄",
        "Just a single added to the tally. 1️⃣",
        "Good call! One run completed safely. 👟",
        "Direct hit missed! That was close. 🎯",
        "Tucked away off the hips for a single. 🏏",
        "Dropped at his feet and they scamper through. ⚡",
        "Fielder fumbles, and they steal a run. 🤲",
        "Sensible batting. Taking the single on offer. 🧠",
        "Driven to long-on for one. 🚶",
        "Smart cricket! Rotating the strike to keep the scoreboard ticking. ⏱️",
        "A little hesitation, but they make it in the end. 😅"
    ],
    "double": [
        "In the gap! They will get two easily. ✌️",
        "Great running between the wickets! Two runs added. 🏃‍♂️🏃‍♂️",
        "Pushed hard for the second! Excellent fitness shown. 💪",
        "Fielder was slow to react! They steal a couple. 😴",
        "Two runs added. Good placement into the deep. ⚡",
        "They turn for the second run immediately! Aggressive running. ⏩",
        "Misfield allows them to come back for two. 🤦‍♂️",
        "Good throw from the deep, but the batsman is safe. ⚾",
        "Calculated risk taken for the second run! ✅",
        "The fielder cuts it off, but they get a couple. 🛡️"
    ],
    "triple": [
        "Superb fielding effort! Saved the boundary just in time. 🛑 3 runs.",
        "They are running hard! Three runs taken. 🏃‍♂️💨",
        "Excellent stamina! Pushing for the third run. 🔋",
        "Just short of the boundary! 3 runs added to the score. 🚧",
        "The outfield is slow, the ball stops just before the rope. 🐢",
        "Great relay throw! But they collect three runs. 🤝"
    ],
    "boundary": [
        "CRACKING SHOT! Raced to the fence like a bullet! 🚀 FOUR!",
        "What timing! Found the gap perfectly. 🏎️ 4 Runs!",
        "Beautiful Cover Drive! That is a textbook shot! 😍",
        "The fielder is just a spectator! That's a boundary! 👀",
        "One bounce and over the rope! Four runs! 🎾",
        "Misfield and four! The bowler is absolutely furious. 😠",
        "Surgical precision! Cut away past point for FOUR! 🔪",
        "Pulled away powerfully! No chance for the fielder. 🤠",
        "Straight down the ground! Umpire had to duck! 🦆 FOUR!",
        "Edged but it flies past the slip cordon! Lucky boundary. 🍀",
        "Swept away fine! The fielder gives chase in vain. 🧹",
        "That was pure elegance! Caressed to the boundary. ✨",
        "Power and placement! A terrific shot for four. 💪",
        "Short ball punished! Dispatched to the fence. 👮‍♂️",
        "Drilled through the covers! What a sound off the bat! 🔊"
    ],
    "five": [
        "FIVE RUNS! Overthrows! Bonus runs for the team. 🎁",
        "Comedy of errors on the field! 5 runs conceded. 🤡",
        "Running for five! Incredible stamina displayed! 🏃‍♂️💨",
        "Bonus runs! The batting team is delighted with that gift. 🎉",
        "Throw hits the stumps and deflects away! 5 runs! 🎱"
    ],
    "six": [
        "HUGE! That's out of the stadium! 🌌 SIX!",
        "Muscle power! Sent into orbit! 💪",
        "MAXIMUM! What a clean connection! 💥",
        "It's raining sixes! Destruction mode activated! 🔨",
        "Helicopter Shot! That is magnificent! 🚁",
        "That's a monster hit! The bowler looks devastated. 😭",
        "Gone with the wind! High and handsome! 🌬️",
        "That ball is in the parking lot! Fetch that! 🚗",
        "Clean striking! It's landed in the top tier! 🏟️",
        "Upper cut sails over third man! What a shot! ✂️",
        "Smoked down the ground! That is a massive six! 🚬",
        "The crowd catches it! That's a fan favorite shot! 🙌",
        "Pick that up! Sent traveling into the night sky! 🚀",
        "Pure timing! He didn't even try to hit that hard. 🪄",
        "The bowler missed the yorker, and it's gone for SIX! 📏"
    ],
    "wicket": [
        "OUT! Game over for the batsman! ❌",
        "Clean Bowled! Shattered the stumps! 🪵",
        "Caught! Fielder makes no mistake. Wicket! 👐",
        "Gone! The big fish is in the net! 🎣",
        "Edged and taken! A costly mistake by the batsman. 🏏",
        "Stumping! Lightning fast hands by the keeper! ⚡",
        "Run Out! A terrible mix-up in the middle. 🚦",
        "LBW! That looked plumb! The finger goes up! ☝️",
        "Caught and Bowled! Great reflexes by the bowler! 🤲",
        "Hit Wicket! Oh no, he stepped on his own stumps! 😱",
        "The partnership is broken! Massive moment in the game. 💔",
        "He has holed out to the deep! End of a good innings. 🔚",
        "Golden Duck! He goes back without troubling the scorers. 🦆",
        "The stumps are taking a walk! cartwheeling away! 🤸‍♂️",
        "What a catch! He plucked that out of thin air! 🦅"
    ],
    "noball": [
        "NO BALL! Overstepped the line! 🚨",
        "Free Hit coming up! A free swing for the batsman! 🔥",
        "Illegal delivery. Umpire signals No Ball. 🙅‍♂️",
        "That was a beamer! Dangerous delivery. No Ball. 🤕",
        "Bowler loses his grip. No Ball called. 🧼"
    ],
    "wide": [
        "Wide Ball! Radar is off. 📡",
        "Too wide! Extra run conceded. 🎁",
        "Wayward delivery. Drifting down the leg side. 🚌",
        "Too high! Umpire signals a wide for height. 🦒",
        "Spilled down the leg side. Keeper collects it. Wide. 🧤"
    ]
}

SHUBH_SPECIAL = {

    "dot": [
        "Wah bhai! Bat lekar aaye ho ya bas attendance mark karne? 📝",
        "Itna slow reaction, lagta hai brain abhi bhi loading screen pe hai. ⏳",
        "Dot ball! Scoreboard bhi tumse umeed chhod chuka hai. 📉",
        "Shot maarne ka confidence tha, par skill ne leave le li. 🏖️",
        "Bowler match khel raha hai, batsman sightseeing kar raha hai. 🧳",
        "Ye batting nahi, public patience test hai. 🧠",
        "Ball ko dekh ke aisa react kiya jaise unknown number ka call ho. 📞",
    ],

    "single": [
        "Finally ek run! Is historic moment ke liye documentary banegi. 🎥",
        "Single leke itna proud jaise century maar di. Calm down bro. 😏",
        "Scoreboard ne sympathy run accept kar liya. 🙏",
        "Achievement unlocked: Kuch toh kiya finally. 🎮",
        "Risk lene ka option shayad settings me disabled hai. ⚙️",
        "Ye run nahi, charity accept hui hai. 🎁",
    ],

    "double": [
        "Do run! Lagta hai fielders ne donation drive start kari hai. 💸",
        "Effort zyada, talent optional. Classic combo. 🤡",
        "Do run mil gaye, ab 10 ball rest karega hero. 🛌",
        "Ye skill nahi, luck ka EMI chal raha hai. 💳",
        "Fielders bhi confuse, batsman bhi confuse. Perfect chaos. 🌪️",
    ],

    "triple": [
        "Teen run! Lagta hai peeche se kutta chase kar raha tha. 🐕",
        "Running Olympic level, batting gali level. 🏃",
        "Ye cricket nahi, panic attack tha live. 🚨",
        "Fielders ne comedy show open kar diya. 🎪",
        "Sabka system hang ho gaya ek saath. 💻",
    ],

    "four": [
        "OHH boundary! Galti se professional shot nikal gaya. 😏",
        "Rare moment! Screenshot lelo, repeat nahi hoga. 📸",
        "Ball boundary gayi, bowler therapy lene jayega. 🛋️",
        "Finally bat aur ball ka introduction ho gaya. 🤝",
        "Bowler ka confidence ab recycle bin me hai. 🗑️",
        "Lagta hai accidental timing perfect ho gayi. 🚑",
    ],

    "six": [
        "OHHHH SIX! Ball stadium ke bahar, bowler apni izzat dhoond raha hai. 🔍",
        "Ye six nahi, bowler ke confidence ka public execution tha. ☠️",
        "Ball orbit me, bowler depression me. Perfect balance. 🪐",
        "Bowler ab apni life choices question kar raha hai. 🤯",
        "Ye shot nahi, bowler ke career pe nuclear attack tha. ☢️",
        "Ball gayi space me, bowler reality check me. 🌌",
        "Bowler internally retire ho chuka hai, announcement pending hai. 📢",
        "Ye six nahi, emotional damage tha HD quality me. 📺",
        "Ball ne boundary cross nahi ki, bowler ki aukaat cross kar di. 📉",
        "Bowler ka confidence ab coma me hai. Doctor bulao. 🚑",
        "Ball stadium ke bahar, bowler future ke bahar. 🚪",
        "Is six ke baad bowler ne apni bio change kar di: 'Former cricketer'. 📝",
        "Ye six nahi, resignation letter tha bowler ke naam. 📦",
        "Ball itni door gayi, shayad next match me wapas aaye. ⏳",
        "Bowler ki soul thodi der ke liye body se logout ho gayi. 👻",
    ],

    "five": [
        "5 runs! Fielders ne circus audition clear kar liya. 🎪",
        "Free ke runs! Batsman bhi believe nahi kar raha. 🤡",
        "Fielding level: Error 404 skills not found. ❌",
        "Ye cricket nahi, comedy central hai. 📺",
        "Runs earned nahi, donated hain respectfully. 💸",
    ],

    "wicket": [
        "OUT! Thank you for wasting everyone's time. 👏",
        "Short stay, zero impact, full embarrassment. 🪦",
        "Confidence unlimited, performance not found. ❌",
        "Ye batting nahi, cameo disaster tha. 🎬",
        "Batsman uninstall ho gaya crease se permanently. 🗑️",
        "Audience bhi relief feel kar rahi hai. Finally. 😌",
        "Bowler ne free reality check de diya. 🧾",
    ]
}

SIDHU_COMMENTARY = {
    "dot": [
        "Oye guru! Ball ne aisi kahani likhi, bat ne kalam hi gira di! 🧱",
        "Bowler ne missile chhodi, batsman statue ban khada raha! 🚀",
        "Ye ball nahi, paheli thi! Batsman solve hi nahi kar paya! ❓",
        "Pressure cooker ki seeti baj rahi hai, par run nahi pak raha! 🍲",
        "Bat hawa mein ghuma, par ball ne usko dhokha de diya! 🎭",
        "Dot ball! Batsman ke hausle pe brake lag gaya! 🛑"
    ],

    "single": [
        "Oye hoye! Boond boond se sagar banta hai, ye ek run sona hai! 🪙",
        "Chhota kadam, bada sapna! Scoreboard ko jagaya! 📈",
        "Single liya jaise chor ne khazana chura liya ho! 🏃",
        "Ye run nahi, umeed ki roshni hai andhere mein! 💡",
        "Smart cricket! Risk kam, profit zyada! 🧠"
    ],

    "double": [
        "Do run nahi, double dhamaka hai guru! 💥",
        "Fielders bhaagte reh gaye, batsman jeet gaya race! 🏁",
        "Ye partnership ka cement hai, mazboot buniyad! 🧱",
        "Do run, jaise sher ki do dahad! 🦁",
        "Fitness ka festival, fielding ka funeral! ⚰️"
    ],

    "triple": [
        "Teen run! Ye toh cheetah ki raftaar aur sher ka jazba hai! 🐆",
        "Running between wickets ka Taj Mahal bana diya! 🏛️",
        "Fielders thak gaye, batsman jeet gaya marathon! 🏃‍♂️",
        "Teen run nahi, teamwork ka teer hai! 🎯",
        "Ye cricket nahi, Olympic sprint lag raha hai! 🥇"
    ],

    "four": [
        "Oye guru! Ball ne boundary ko chummi de di! FOUR! 💋",
        "Ye shot nahi, kala hai! Artist ka masterpiece! 🎨",
        "Fielder bas darshak ban gaya, ball superstar ban gayi! ⭐",
        "Timing aisi jaise ghadi ki sui! Perfect connection! ⏱️",
        "Boundary! Ball ne visa liya aur rope ke paar chali gayi! ✈️"
    ],

    "six": [
        "Oye hoye! Ye ball nahi, rocket hai! Chand pe landing karega! 🌕",
        "Maximum! Stadium chhota pad gaya is shot ke liye! 💥",
        "Muscle power ka pradarshan! Ye cricket nahi, yudh hai! ⚔️",
        "Ball hawa mein aise uda jaise azaad panchi! 🕊️",
        "Six! Gravity bhi haar maan gayi! 🌌",
        "Ye shot nahi, prakriti ka prakop hai! 🌪️"
    ],

    "five": [
        "Oye guru! Fielders confuse ho gaye, batsman ne bonus jeet liya! 5 RUNS! 🤯",
        "Ye run nahi, cricket ka lottery ticket hai! 🎟️",
        "Fielding ki galti, batting ki Diwali! 🪔",
        "Ball bhaagti rahi, batsman kamayi karta raha! 💰",
        "Paanch run! Bhagwan bhi batsman ke saath hai aaj! 🙏"
    ],

    "wicket": [
        "OUT! Batsman ka kila toot gaya, bowler ban gaya sikandar! 🏰",
        "Stumps ud gaye jaise aandhi mein patte! 🍃",
        "Catch liya! Match ka rukh badal diya! 🔄",
        "Ye wicket nahi, bijli gir gayi batting par! ⚡",
        "Batsman aaya, dekha, aur haar gaya! ❌",
        "Bowler ne likh di batsman ki antim kahani! 📖"
    ]
}


# Data storage paths
DATA_DIR = "resume_data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
MATCHES_FILE = os.path.join(DATA_DIR, "matches.json")
STATS_FILE = os.path.join(DATA_DIR, "stats.json")
ACHIEVEMENTS_FILE = os.path.join(DATA_DIR, "achievements.json")
BANNED_GROUPS_FILE = os.path.join(DATA_DIR, "banned_groups.json")
GROUPS_FILE = os.path.join(DATA_DIR, "groups.json")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
DB_FILE = DB_PATH # SQL Database File (using the same path as DB_PATH)

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# Global data structures
active_matches: Dict[int, 'Match'] = {}
active_solo_matches: Dict[int, 'SoloMatch'] = {}  
active_auctions: Dict[int, Auction] = {}
ai_matches: Dict[int, Dict] = {}  # user_id: {match_state}
# tournament_approved_groups removed - using TOURNAMENT_APPROVED_GROUPS instead
user_data: Dict[int, Dict] = {}
match_history: List[Dict] = []
player_stats: Dict[int, Dict] = {}
achievements: Dict[int, List[str]] = {}
registered_groups: Dict[int, Dict] = {}
banned_groups: Set[int] = set()
bot_start_time = time.time()

# --- DUAL STORAGE MANAGER (SQL + JSON) ---
def init_db():
    """Initialize SQL Tables"""
    print(f"Creating database: {DB_FILE}")
    
    # Delete old database if exists
    if os.path.exists(DB_FILE):
        print(f"Deleting old database...")
        os.remove(DB_FILE)
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    print("Creating tables...")
    
    # Original tables
    c.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS player_stats (user_id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS matches (match_id TEXT PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS groups (group_id INTEGER PRIMARY KEY, data TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS achievements (user_id INTEGER PRIMARY KEY, data TEXT)''')
    
    # Create user_stats table for detailed statistics
    c.execute('''CREATE TABLE IF NOT EXISTS user_stats (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        total_runs INTEGER DEFAULT 0,
        total_balls_faced INTEGER DEFAULT 0,
        total_wickets INTEGER DEFAULT 0,
        total_balls_bowled INTEGER DEFAULT 0,
        total_runs_conceded INTEGER DEFAULT 0,
        matches_played INTEGER DEFAULT 0,
        matches_won INTEGER DEFAULT 0,
        highest_score INTEGER DEFAULT 0,
        total_hundreds INTEGER DEFAULT 0,
        total_fifties INTEGER DEFAULT 0,
        total_sixes INTEGER DEFAULT 0,
        total_fours INTEGER DEFAULT 0,
        total_dots INTEGER DEFAULT 0,
        best_bowling TEXT DEFAULT '0/0',
        total_ducks INTEGER DEFAULT 0
    )''')
    
    # Create match_history table
    c.execute('''CREATE TABLE IF NOT EXISTS match_history (
        match_id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER,
        match_type TEXT,
        winner_team TEXT,
        runner_up_team TEXT,
        total_overs INTEGER,
        team_x_score TEXT,
        team_y_score TEXT,
        player_of_match INTEGER,
        match_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    conn.commit()
    
    # Verify tables were created
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = c.fetchall()
    
    print("\n✅ Database created successfully!")
    print(f"\nTables created: {len(tables)}")
    for table in tables:
        print(f"  - {table[0]}")
    
    conn.close()


def init_tournament_db():
    """Initialize tournament registration database"""
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS tournament_groups (
            group_id INTEGER PRIMARY KEY,
            group_name TEXT,
            approved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS registration_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            tournament_name TEXT,
            start_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_date TIMESTAMP,
            days INTEGER,
            is_active INTEGER DEFAULT 1,
            FOREIGN KEY (group_id) REFERENCES tournament_groups(group_id)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS registered_players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            base_price INTEGER,
            registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(group_id, user_id),
            FOREIGN KEY (group_id) REFERENCES tournament_groups(group_id)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS auction_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER,
            player_user_id INTEGER,
            player_name TEXT,
            team_name TEXT,
            final_price INTEGER,
            auction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (group_id) REFERENCES tournament_groups(group_id)
        )
    """)
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS tournament_power_users (
            user_id INTEGER,
            group_id INTEGER,
            granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, group_id)
        )
    """)
    
    conn.commit()
    conn.close()
    logger.info("✅ Tournament database initialized")

def load_tournament_data():
    """Load tournament data on startup"""
    try:
        conn = sqlite3.connect(TOURNAMENT_DB_PATH)
        c = conn.cursor()
        
        c.execute('SELECT group_id FROM tournament_groups')
        for (group_id,) in c.fetchall():
            TOURNAMENT_APPROVED_GROUPS.add(group_id)
        
        c.execute("""
            SELECT group_id, end_date, days FROM registration_periods
            WHERE is_active = 1 AND end_date > ?
        """, (datetime.now().isoformat(),))
        for group_id, end_date, days in c.fetchall():
            REGISTRATION_ACTIVE[group_id] = {
                'end_date': datetime.fromisoformat(end_date),
                'days': days
            }
        
        c.execute('SELECT user_id, group_id FROM tournament_power_users')
        for user_id, group_id in c.fetchall():
            TOURNAMENT_POWER_USERS.add((user_id, group_id))
        
        conn.close()
        logger.info(f"✅ Loaded {len(TOURNAMENT_APPROVED_GROUPS)} tournament groups")
    except Exception as e:
        logger.error(f"Error loading tournament data: {e}")


def save_data():
    """Save data to BOTH SQL Database AND JSON Files"""
    try:
        # 1. SQL Save (Primary & Fast)
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("BEGIN TRANSACTION")
        
        for uid, data in user_data.items():
            c.execute("INSERT OR REPLACE INTO users (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))
        for uid, data in player_stats.items():
            c.execute("INSERT OR REPLACE INTO player_stats (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))
        for match in match_history:
            mid = match.get("match_id", str(time.time()))
            c.execute("INSERT OR REPLACE INTO matches (match_id, data) VALUES (?, ?)", (mid, json.dumps(match)))
        for gid, data in registered_groups.items():
            c.execute("INSERT OR REPLACE INTO groups (group_id, data) VALUES (?, ?)", (gid, json.dumps(data)))
        for uid, data in achievements.items():
            c.execute("INSERT OR REPLACE INTO achievements (user_id, data) VALUES (?, ?)", (uid, json.dumps(data)))

        conn.commit()
        conn.close()
        
        # 2. JSON Save (Secondary / Manual Backup)
        with open(USERS_FILE, 'w') as f: json.dump(user_data, f, indent=2)
        with open(STATS_FILE, 'w') as f: json.dump(player_stats, f, indent=2)
        with open(MATCHES_FILE, 'w') as f: json.dump(match_history, f, indent=2)
        with open(GROUPS_FILE, 'w') as f: json.dump(registered_groups, f, indent=2)
        with open(ACHIEVEMENTS_FILE, 'w') as f: json.dump(achievements, f, indent=2)

    except Exception as e:
        logger.error(f"Error saving data: {e}")

def load_data():
    """Load all data (Try SQL first, Fallback to JSON)"""
    global user_data, match_history, player_stats, achievements, registered_groups, banned_groups
    
    # Initialize DB if missing
    if not os.path.exists(DB_FILE):
        init_db()

    data_loaded_from_sql = False
    
    # --- TRY LOADING FROM SQL ---
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Check if tables exist, if not create them
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='player_stats'")
        if not c.fetchone():
            logger.warning("⚠️ player_stats table missing, creating tables...")
            c.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, data TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS player_stats (user_id INTEGER PRIMARY KEY, data TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS matches (match_id TEXT PRIMARY KEY, data TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS groups (group_id INTEGER PRIMARY KEY, data TEXT)''')
            c.execute('''CREATE TABLE IF NOT EXISTS achievements (user_id INTEGER PRIMARY KEY, data TEXT)''')
            conn.commit()
            logger.info("✅ Tables created successfully")
        
        c.execute("SELECT count(*) FROM users")
        if c.fetchone()[0] > 0:
            c.execute("SELECT user_id, data FROM users")
            user_data = {row[0]: json.loads(row[1]) for row in c.fetchall()}

            c.execute("SELECT user_id, data FROM player_stats")
            player_stats = {row[0]: json.loads(row[1]) for row in c.fetchall()}

            c.execute("SELECT data FROM matches")
            match_history = [json.loads(row[0]) for row in c.fetchall()]

            c.execute("SELECT group_id, data FROM groups")
            registered_groups = {row[0]: json.loads(row[1]) for row in c.fetchall()}

            c.execute("SELECT user_id, data FROM achievements")
            achievements = {row[0]: json.loads(row[1]) for row in c.fetchall()}
            
            data_loaded_from_sql = True
            logger.info("✅ Data loaded from SQL Database.")
            
        conn.close()
    except Exception as e:
        logger.error(f"SQL Load Error (Falling back to JSON): {e}")

    # --- FALLBACK TO JSON ---
    if not data_loaded_from_sql:
        logger.info("⚠️ Loading from JSON files...")
        try:
            if os.path.exists(USERS_FILE):
                with open(USERS_FILE, 'r') as f: 
                    user_data = {int(k): v for k, v in json.load(f).items()}
            if os.path.exists(STATS_FILE):
                with open(STATS_FILE, 'r') as f: 
                    player_stats = {int(k): v for k, v in json.load(f).items()}
            if os.path.exists(MATCHES_FILE):
                with open(MATCHES_FILE, 'r') as f: 
                    match_history = json.load(f)
            if os.path.exists(GROUPS_FILE):
                with open(GROUPS_FILE, 'r') as f: 
                    registered_groups = {int(k): v for k, v in json.load(f).items()}
            if os.path.exists(ACHIEVEMENTS_FILE):
                with open(ACHIEVEMENTS_FILE, 'r') as f: 
                    achievements = {int(k): v for k, v in json.load(f).items()}
            
            # JSON se load hone ke baad turant SQL me sync kar do
            save_data()
        except Exception: 
            pass
    
    # ✅ LOAD BANNED GROUPS
    if os.path.exists(BANNED_GROUPS_FILE):
        try:
            with open(BANNED_GROUPS_FILE, 'r') as f:
                banned_groups = set(json.load(f))
            logger.info(f"🚫 Loaded {len(banned_groups)} banned groups")
        except Exception as e:
            logger.error(f"Error loading banned groups: {e}")
            banned_groups = set()

# Initialize player stats for a user
def init_player_stats(user_id: int):
    """Initialize stats structure"""
    default_team = {
        "matches": 0,
        "matches_played": 0,  # ADD THIS - for backward compatibility
        "wins": 0,  # ADD THIS
        "runs": 0,
        "balls": 0,
        "wickets": 0,
        "runs_conceded": 0,
        "balls_bowled": 0,
        "highest": 0,
        "centuries": 0,
        "fifties": 0,
        "ducks": 0,
        "sixes": 0,
        "fours": 0,
        "mom": 0,
        "hat_tricks": 0,
        "captain_matches": 0,
        "captain_wins": 0
    }
    
    default_solo = {
        "matches": 0,
        "wins": 0,
        "runs": 0,
        "balls": 0,
        "wickets": 0,
        "highest": 0,
        "ducks": 0,
        "top_3_finishes": 0
    }


    # Case 1: New User
    if user_id not in player_stats:
        player_stats[user_id] = {
            "team": default_team.copy(),
            "solo": default_solo.copy()
        }
        save_data()
    
    # Case 2: Existing User (Check & Fix missing keys)
    else:
        changed = False
        
        # Check Team Stats
        if "team" not in player_stats[user_id]:
            # Old data migration logic
            old_data = player_stats[user_id].copy()
            player_stats[user_id]["team"] = default_team.copy()
            player_stats[user_id]["team"]["matches"] = old_data.get("matches_played", 0)
            player_stats[user_id]["team"]["runs"] = old_data.get("total_runs", 0)
            # ... map other fields if needed ...
            changed = True
        else:
            # Fill missing keys in 'team'
            for key, val in default_team.items():
                if key not in player_stats[user_id]["team"]:
                    player_stats[user_id]["team"][key] = val
                    changed = True
        
        # Check Solo Stats (CRITICAL FIX)
        if "solo" not in player_stats[user_id]:
            player_stats[user_id]["solo"] = default_solo.copy()
            changed = True
        else:
            # Fill missing keys in 'solo'
            for key, val in default_solo.items():
                if key not in player_stats[user_id]["solo"]:
                    player_stats[user_id]["solo"][key] = val
                    changed = True

        if changed: save_data()

# Player class to track individual player performance in a match
class Player:
    """Represents a player in the match"""
    def __init__(self, user_id: int, username: str, first_name: str):
        self.user_id = user_id
        self.username = username
        self.first_name = first_name
        self.runs = 0
        self.balls_faced = 0
        self.wickets = 0
        self.balls_bowled = 0
        self.runs_conceded = 0
        self.consecutive_wickets = 0  # Track consecutive wickets for hat-trick
        self.has_hat_trick = False     # Flag to celebrate hat-trick only once
        self.is_out = False
        self.dismissal_type = None
        self.dot_balls_faced = 0
        self.dot_balls_bowled = 0
        self.boundaries = 0
        self.sixes = 0
        self.overs_bowled = 0
        self.maiden_overs = 0
        self.no_balls = 0
        self.wides = 0
        self.has_bowled_this_over = False
        self.batting_timeouts = 0
        self.bowling_timeouts = 0
        self.is_bowling_banned = False
    
    def get_strike_rate(self) -> float:
        """Calculate batting strike rate"""
        if self.balls_faced == 0:
            return 0.0
        return round((self.runs / self.balls_faced) * 100, 2)
    
    def get_economy(self) -> float:
        """Calculate bowling economy rate"""
        if self.balls_bowled == 0:
            return 0.0
        overs = self.balls_bowled / 6
        if overs == 0:
            return 0.0
        return round(self.runs_conceded / overs, 2)
    
    def get_bowling_average(self) -> float:
        """Calculate bowling average"""
        if self.wickets == 0:
            return 0.0
        return round(self.runs_conceded / self.wickets, 2)

# Team class
class Team:
    """Represents a team in the match - COMPLETE VERSION"""
    def __init__(self, name: str):
        self.name = name
        self.players: List[Player] = []
        self.captain_id: Optional[int] = None
        self.captain_name: str = ""
        
        # Score tracking
        self.score = 0
        self.wickets = 0
        self.overs = 0.0
        self.balls = 0
        self.balls_faced = 0  # CRITICAL: Add this
        
        # Extras
        self.extras = 0
        self.extras_wide = 0   # CRITICAL: Add this
        self.extras_noball = 0  # CRITICAL: Add this
        
        # Game state
        self.drs_remaining = 1
        self.all_out = False
        
        # Batting order
        self.batting_order: List[int] = []
        self.striker_id: Optional[int] = None
        self.non_striker_id: Optional[int] = None
        
        # Current batsmen
        self.current_batsman_idx: Optional[int] = None
        self.current_non_striker_idx: Optional[int] = None
        self.out_players_indices = set()
        
        # Bowling
        self.current_bowler_idx: Optional[int] = None
        self.current_bowler_name: str = ""
        self.penalty_runs = 0
        self.bowler_history: List[int] = []
        
        # Player stats tracking
        self.player_stats: Dict[int, Dict] = {}

    def is_all_out(self):
        return (len(self.players) - len(self.out_players_indices)) < 2

    def swap_batsmen(self):
        if self.current_batsman_idx is not None and self.current_non_striker_idx is not None:
            self.current_batsman_idx, self.current_non_striker_idx = \
                self.current_non_striker_idx, self.current_batsman_idx

    def add_player(self, player: Player):
        self.players.append(player)
        # Initialize player stats
        if player.user_id not in self.player_stats:
            self.player_stats[player.user_id] = {
                "runs": 0,
                "balls": 0,
                "fours": 0,
                "sixes": 0,
                "wickets": 0,
                "runs_conceded": 0,
                "balls_bowled": 0,
                "dismissal": "not out"
            }
    
    def remove_player(self, user_id: int) -> bool:
        for i, player in enumerate(self.players):
            if player.user_id == user_id:
                self.players.pop(i)
                return True
        return False
    
    def get_player(self, user_id: int) -> Optional[Player]:
        for player in self.players:
            if player.user_id == user_id:
                return player
        return None
    
    def get_player_by_serial(self, serial: int) -> Optional[Player]:
        if 1 <= serial <= len(self.players):
            return self.players[serial - 1]
        return None
    
    def get_available_bowlers(self) -> List[Player]:
        available = []
        last_bowler_idx = self.bowler_history[-1] if self.bowler_history else None
    
        for i, player in enumerate(self.players):
            if not player.is_bowling_banned and i != last_bowler_idx:
                available.append(player)
        return available
    
    def update_overs(self):
        """Update overs display"""
        complete_overs = self.balls // 6
        balls_in_over = self.balls % 6
        self.overs = complete_overs + (balls_in_over / 10)
        # Also update balls_faced for compatibility
        self.balls_faced = self.balls
    
    def get_current_over_balls(self) -> int:
        """Returns balls in current over (0-5)"""
        return self.balls % 6
    
    def complete_over(self):
        """Complete the current over"""
        remaining_balls = 6 - (self.balls % 6)
        self.balls += remaining_balls
        self.overs = self.balls // 6
        if match.batting_first_team_id == match.team_x.captain_id:
            # Team X batting
            match.team_x_over_runs.append(match.current_over_runs)
        else:
            # Team Y batting
            match.team_y_over_runs.append(match.current_over_runs)
    
        match.current_over_runs = 0  # Reset for new over

# Match class - Core game engine
class Match:
    """Main match class that handles all game logic"""
    def __init__(self, group_id: int, group_name: str):
        self.group_id = group_id
        self.group_name = group_name
        self.phase = GamePhase.TEAM_JOINING
        self.match_id = f"{group_id}_{int(time.time())}"
        self.created_at = datetime.now()
        self.last_activity = time.time()  # Track last move time
        self.team_x_over_runs = [] # List to store runs per over for Team X
        self.team_y_over_runs = []
        self.current_over_runs = 0
        
        # Teams
        self.team_x = Team("Team X")
        self.team_y = Team("Team Y")
        self.editing_team: Optional[str] = None  # 'X' ya 'Y' store karega
        
        # Match settings
        self.host_id: Optional[int] = None
        self.total_overs = 0
        self.toss_winner: Optional[Team] = None
        self.batting_first: Optional[Team] = None
        self.bowling_first: Optional[Team] = None
        self.current_batting_team: Optional[Team] = None
        self.current_bowling_team: Optional[Team] = None
        
        # Match state
        self.innings = 1
        self.target = 0
        self.is_free_hit = False
        self.last_wicket_ball = None
        self.drs_in_progress = False
        self.team_x_timeout_used = False
        self.team_y_timeout_used = False
        
        # Timers and messages
        self.team_join_end_time: Optional[float] = None
        self.main_message_id: Optional[int] = None
        self.join_phase_task: Optional[asyncio.Task] = None
        
        # Ball tracking
        self.current_ball_data: Dict = {}
        self.ball_timeout_task: Optional[asyncio.Task] = None
        self.batsman_selection_task: Optional[asyncio.Task] = None
        self.bowler_selection_task: Optional[asyncio.Task] = None
        
        self.solo_players: List[Player] = [] # List of Player objects
        self.current_solo_bat_idx = 0
        self.current_solo_bowl_idx = 0
        self.solo_balls_this_spell = 0 # To track 3 ball rotation
        self.solo_join_end_time = 0
        self.host_change_votes = {}
        self.team_x_impact_count = 0  # Track number of substitutions used
        self.team_y_impact_count = 0
        self.team_x_impact_history = []  # List of (old_player_name, new_player_name)
        self.team_y_impact_history = []

        # Waiting states
        # Waiting states (FIXED: Added for batsman/bowler selection)
        self.waiting_for_batsman = False
        self.waiting_for_bowler = False
        self.batsman_selection_time: Optional[float] = None
        self.bowler_selection_time: Optional[float] = None
        self.pending_over_complete = False

        # Game mode (for TEAM/SOLO distinction, default TEAM)
        self.game_mode = "TEAM"
        
        # Super over
        self.is_super_over = False
        self.super_over_batting_team: Optional[Team] = None
        
        # Match settings
        self.host_id: Optional[int] = None
        self.host_name: str = "Unknown"
        
        # Match log
        self.ball_by_ball_log: List[Dict] = []
        self.match_events: List[str] = []
        self.strike_zones = {
            'team_x': defaultdict(int),  # {zone: runs}
            'team_y': defaultdict(int)
        }
        self.momentum_history = []  # List of (balls, momentum_score)
        self.current_momentum = 0  # Range: -100 to +100
        self.last_6_balls = []  # Track last 6 balls for momentum
    
    def get_team_by_name(self, name: str) -> Optional[Team]:
        """Get team by name"""
        if name == "Team X":
            return self.team_x
        elif name == "Team Y":
            return self.team_y
        return None
    
    def get_other_team(self, team: Team) -> Team:
        """Get the opposing team"""
        if team == self.team_x:
            return self.team_y
        return self.team_x
    
    def get_captain(self, team: Team) -> Optional[Player]:
        """Get team captain"""
        if team.captain_id:
            return team.get_player(team.captain_id)
        return None
    
    def add_event(self, event: str):
        """Add event to match log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.match_events.append(f"[{timestamp}] {event}")
    
    def get_required_run_rate(self) -> float:
        """Calculate required run rate for chasing team"""
        if self.innings != 2 or not self.current_batting_team:
            return 0.0
        
        runs_needed = self.target - self.current_batting_team.score
        balls_remaining = (self.total_overs * 6) - self.current_batting_team.balls
        
        if balls_remaining <= 0:
            return 0.0
        
        overs_remaining = balls_remaining / 6
        return round(runs_needed / overs_remaining, 2)
    
    def is_innings_complete(self) -> bool:
        """Check if current innings is complete"""
        if not self.current_batting_team or not self.current_bowling_team:
            return False
        
        # All out
        if self.current_batting_team.wickets >= len(self.current_batting_team.players) - 1:
            return True
        
        # Overs complete
        if self.current_batting_team.balls >= self.total_overs * 6:
            return True
        
        # Target chased in second innings
        if self.innings == 2 and self.current_batting_team.score >= self.target:
            return True
        
        return False
    
    def get_match_summary(self) -> str:
        """Generate detailed match summary"""
        summary_lines = []
        summary_lines.append("=" * 40)
        summary_lines.append("MATCH SUMMARY")
        summary_lines.append("=" * 40)
        summary_lines.append("")
        
        # First innings
        first_team = self.batting_first
        if first_team:
            summary_lines.append(f"{first_team.name}: {first_team.score}/{first_team.wickets}")
            summary_lines.append(f"Overs: {first_team.overs}")
            summary_lines.append("")
        
        # Second innings
        if self.innings >= 2:
            second_team = self.get_other_team(first_team)
            summary_lines.append(f"{second_team.name}: {second_team.score}/{second_team.wickets}")
            summary_lines.append(f"Overs: {second_team.overs}")
            summary_lines.append("")
        
        summary_lines.append("=" * 40)
        return "\n".join(summary_lines)

# Utility functions
def update_over_stats(match):
    """Calculates runs scored in the just-concluded over and saves them."""
    
    # 1. Determine which team is currently batting
    if not match.is_second_innings:
        # Team X is batting
        current_score = match.team_x.score
        # Sum of previous overs
        already_accounted = sum(match.team_x_over_runs)
        runs_in_this_over = current_score - already_accounted
        match.team_x_over_runs.append(runs_in_this_over)
    else:
        # Team Y is batting
        current_score = match.team_y.score
        already_accounted = sum(match.team_y_over_runs)
        runs_in_this_over = current_score - already_accounted
        match.team_y_over_runs.append(runs_in_this_over)

def get_random_gif(event: MatchEvent) -> str:
    """Get random GIF for an event"""
    gifs = GIFS.get(event, [])
    if gifs:
        return random.choice(gifs)
    return ""

def get_random_commentary(event_type: str) -> str:
    """Get random commentary for an event"""
    comments = COMMENTARY.get(event_type, [])
    if comments:
        return random.choice(comments)
    return ""


def generate_mini_scorecard(match: Match) -> str:
    """
    Generate Mini Scorecard with Current Stats
    Shows after: Wicket, Over Complete
    """
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # Calculate Run Rate
    overs_played = max(bat_team.overs, 0.1)
    current_rr = round(bat_team.score / overs_played, 2)
    
    # Get Current Batsmen
    striker = None
    non_striker = None
    if bat_team.current_batsman_idx is not None:
        striker = bat_team.players[bat_team.current_batsman_idx]
    if bat_team.current_non_striker_idx is not None:
        non_striker = bat_team.players[bat_team.current_non_striker_idx]
    
    # Get Last Bowler (Most Recent)
    last_bowler = None
    if bowl_team.current_bowler_idx is not None:
        last_bowler = bowl_team.players[bowl_team.current_bowler_idx]
    elif bowl_team.bowler_history:
        last_bowler_idx = bowl_team.bowler_history[-1]
        last_bowler = bowl_team.players[last_bowler_idx]
    
    # Build Message
    msg = "📊 <b>MINI SCORECARD</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Team Scores
    msg += f"🧊 <b>{match.team_x.name}:</b> {match.team_x.score}/{match.team_x.wickets} ({format_overs(match.team_x.balls)})\n"
    msg += f"🔥 <b>{match.team_y.name}:</b> {match.team_y.score}/{match.team_y.wickets} ({format_overs(match.team_y.balls)})\n\n"
    
    # Current Partnership
    msg += f"🏏 <b>BATTING - {bat_team.name}</b>\n"
    msg += f"📈 <b>Run Rate:</b> {current_rr}\n\n"
    
    if striker:
        sr = round((striker.runs / striker.balls_faced) * 100, 1) if striker.balls_faced > 0 else 0
        status = "*" if not striker.is_out else ""
        msg += f"🟢 <b>{striker.first_name}{status}:</b> {striker.runs} ({striker.balls_faced}) SR: {sr}\n"
    
    if non_striker:
        sr = round((non_striker.runs / non_striker.balls_faced) * 100, 1) if non_striker.balls_faced > 0 else 0
        status = "*" if not non_striker.is_out else ""
        msg += f"⚪ <b>{non_striker.first_name}{status}:</b> {non_striker.runs} ({non_striker.balls_faced}) SR: {sr}\n"
    
    msg += "\n"
    
    # Last Bowler Stats
    if last_bowler:
        econ = round(last_bowler.runs_conceded / max(last_bowler.balls_bowled/6, 0.1), 2)
        msg += f"⚾ <b>BOWLING - {bowl_team.name}</b>\n"
        msg += f"🎯 <b>{last_bowler.first_name}:</b> {last_bowler.wickets}/{last_bowler.runs_conceded} "
        msg += f"({format_overs(last_bowler.balls_bowled)}) Econ: {econ}\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━"
    
    return msg

def format_overs(balls: int) -> str:
    """
    ✅ FIXED: Format balls to overs correctly
    🥚 0 balls = 0.0
    🏏 1 ball = 0.1
    📦 6 balls = 1.0
    🔄 7 balls = 1.1
    """
    if balls == 0:
        return "0.0"
    
    # 🔢 Calculate complete overs and remaining balls
    complete_overs = balls // 6
    balls_in_over = balls % 6
    
    return f"{complete_overs}.{balls_in_over}"


def balls_to_float_overs(balls: int) -> float:
    """Convert balls to float overs"""
    return balls // 6 + (balls % 6) / 10


# --- AUTO-EXECUTE ON START ---

def get_commentary(event_type: str, group_id: int = None, user_id: int = None) -> str:
    """
    Get commentary based on GROUP setting (Admin controlled)
    Priority: Group Setting > User Setting > Default English
    """
    # First check group setting
    group_style = None
    if group_id and group_id in registered_groups:
        group_style = registered_groups[group_id].get("commentary_style")
    
    # If no group setting, check user preference (for DMs or fallback)
    user_style = None
    if user_id and user_id in user_data:
        user_style = user_data[user_id].get("commentary_style")
    
    # Determine which style to use
    style = "english"  # Default
    
    if group_style:
        style = group_style
    elif user_style:
        style = user_style
    
    # Get commentary based on style
    if style == "Shubh":
        comments = SHUBH_SPECIAL.get(event_type, [])
    elif style == "sidhu":
        comments = SIDHU_COMMENTARY.get(event_type, [])
    else:  # english default
        comments = COMMENTARY.get(event_type, [])
    
    if comments:
        return random.choice(comments)
    
    # Fallback
    fallback = {
        "dot": "Dot ball.",
        "single": "Single taken.",
        "double": "Two runs.",
        "triple": "Three runs.",
        "boundary": "Four runs!",
        "five": "Five runs!",
        "six": "Six runs!",
        "wicket": "OUT!",
        "noball": "No ball!",
        "wide": "Wide ball!",
        "freehit": "Free hit!"
    }
    return fallback.get(event_type, "Interesting delivery!")

def save_match_stats(match, winner_team, loser_team):
    """Save match statistics - FIXED VERSION"""
    try:
        for player in winner_team.players:
            user_id = player.user_id
            init_player_stats(user_id)  # Ensure stats exist
            
            stats = player_stats[user_id]
            team_stats = stats.get("team", {})
            
            # FIXED: Use both 'matches' and 'matches_played' for compatibility
            team_stats["matches"] = team_stats.get("matches", 0) + 1
            team_stats["matches_played"] = team_stats.get("matches_played", 0) + 1
            team_stats["wins"] = team_stats.get("wins", 0) + 1
            team_stats["runs"] = team_stats.get("runs", 0) + player.runs
            team_stats["balls"] = team_stats.get("balls", 0) + player.balls_faced
            team_stats["wickets"] = team_stats.get("wickets", 0) + player.wickets
            team_stats["fours"] = team_stats.get("fours", 0) + player.boundaries
            team_stats["sixes"] = team_stats.get("sixes", 0) + player.sixes
            
            # Update highest score
            if player.runs > team_stats.get("highest", 0):
                team_stats["highest"] = player.runs
            
            # Check for milestones
            if player.runs >= 100:
                team_stats["centuries"] = team_stats.get("centuries", 0) + 1
            elif player.runs >= 50:
                team_stats["fifties"] = team_stats.get("fifties", 0) + 1
            elif player.runs == 0 and player.balls_faced > 0:
                team_stats["ducks"] = team_stats.get("ducks", 0) + 1
            
            stats["team"] = team_stats
            player_stats[user_id] = stats
        
        # Similar for loser team but without wins increment
        for player in loser_team.players:
            user_id = player.user_id
            init_player_stats(user_id)
            
            stats = player_stats[user_id]
            team_stats = stats.get("team", {})
            
            team_stats["matches"] = team_stats.get("matches", 0) + 1
            team_stats["matches_played"] = team_stats.get("matches_played", 0) + 1
            team_stats["runs"] = team_stats.get("runs", 0) + player.runs
            # ... rest similar to above
            
            stats["team"] = team_stats
            player_stats[user_id] = stats
        
        save_data()
        logger.info("✅ Match stats saved successfully")
        
    except Exception as e:
        logger.error(f"❌ Stats save error: {e}")
        import traceback
        traceback.print_exc()


async def update_joining_board(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """
    Updates the Joining Board safely (Handling Photo Caption vs Text)
    """
    if not match.main_message_id: return

    # Generate fresh text
    text = get_team_join_message(match)
    
    # Buttons
    keyboard = [
        [InlineKeyboardButton("🧊 Join Team X", callback_data="join_team_x"),
         InlineKeyboardButton("🔥 Join Team Y", callback_data="join_team_y")],
        [InlineKeyboardButton("🚪 Leave Team", callback_data="leave_team")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        # Try editing as Photo Caption first (Since we are using Images)
        await context.bot.edit_message_caption(
            chat_id=chat_id,
            message_id=match.main_message_id,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        error_str = str(e).lower()
        
        # Agar "message is not modified" error hai, toh ignore karo (Sab same hai)
        if "message is not modified" in error_str:
            return
            
        # Agar error aaya ki "there is no caption" implies it's a TEXT message (Fallback)
        # Toh hum text edit karenge
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=match.main_message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        except Exception as text_e:
            # Agar phir bhi fail hua, toh log karo par crash mat hone do
            pass

async def refresh_game_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, caption: str, reply_markup: InlineKeyboardMarkup = None, media_key: str = None):
    """Smart Update: Edits existing message safely with HTML"""
    
    # Try editing first
    if match.main_message_id:
        try:
            if media_key and media_key in MEDIA_ASSETS:
                media = InputMediaPhoto(media=MEDIA_ASSETS[media_key], caption=caption, parse_mode=ParseMode.HTML)
                await context.bot.edit_message_media(
                    chat_id=chat_id,
                    message_id=match.main_message_id,
                    media=media,
                    reply_markup=reply_markup
                )
            else:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=match.main_message_id,
                    text=caption,
                    reply_markup=reply_markup,
                    parse_mode=ParseMode.HTML
                )
            return
        except Exception:
            pass # Edit failed (message deleted/too old), send new

    # Fallback: Send New
    try:
        if media_key and media_key in MEDIA_ASSETS:
            msg = await context.bot.send_photo(chat_id=chat_id, photo=MEDIA_ASSETS[media_key], caption=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        else:
            msg = await context.bot.send_message(chat_id=chat_id, text=caption, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        
        match.main_message_id = msg.message_id
        try: await context.bot.pin_chat_message(chat_id=chat_id, message_id=msg.message_id)
        except: pass
    except Exception as e:
        logger.error(f"Send failed: {e}")

# Important: Is function ko call karne ke liye niche wala update_team_edit_message use karo
async def update_team_edit_message(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Show Team Edit Panel (Final Fixed Version)"""
    
    # 1. Team List Text Generate Karo
    text = f"⚙️ <b>TEAM SETUP</b>\n━━━━━━━━━━━━━━━━━━━━━━\n"
    
    text += f"🧊 <b>Team X:</b>\n"
    for i, p in enumerate(match.team_x.players, 1):
        text += f"  {i}. {p.first_name}\n"
    if not match.team_x.players: text += "  (Empty)\n"
        
    text += f"\n🔥 <b>Team Y:</b>\n"
    for i, p in enumerate(match.team_y.players, 1):
        text += f"  {i}. {p.first_name}\n"
    if not match.team_y.players: text += "  (Empty)\n"
    text += "\n"

    # 2. Logic: Buttons based on State
    if match.editing_team:
        # --- SUB-MENU (Jab Edit Mode ON hai) ---
        text += f"🟢 <b>EDITING TEAM {match.editing_team}</b>\n"
        text += f"👉 Reply to user with <code>/add</code> to add.\n"
        text += f"👉 Reply to user with <code>/remove</code> to remove.\n"
        text += "👉 Click button below when done."
        
        # 'Done' button wapas Main Menu le jayega
        keyboard = [[InlineKeyboardButton(f"✅ Done with Team {match.editing_team}", callback_data="edit_back")]]
        
    else:
        # --- MAIN MENU (Team Select Karo) ---
        text += "👇 <b>Select a team to edit:</b>"
        keyboard = [
            # Note: Buttons ab 'edit_team_x' use kar rahe hain (no _mode)
            [InlineKeyboardButton("✏️ Edit Team X", callback_data="edit_team_x"), 
             InlineKeyboardButton("✏️ Edit Team Y", callback_data="edit_team_y")],
            [InlineKeyboardButton("✅ Finalize & Start", callback_data="team_edit_done")]
        ]

    await refresh_game_message(context, group_id, match, text, InlineKeyboardMarkup(keyboard), media_key="squads")

async def set_edit_team_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Edit Buttons & Set State Correctly"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if user.id != match.host_id:
        await query.answer("⚠️ Only Host can edit!", show_alert=True)
        return

    # Button Logic (State Set Karo)
    if query.data == "edit_team_x":
        match.editing_team = "X"
    elif query.data == "edit_team_y":
        match.editing_team = "Y"
    elif query.data == "edit_back":
        match.editing_team = None # Back to Main Menu

    # UI Update Karo
    await update_team_edit_message(context, chat.id, match)

async def notify_support_group(context: ContextTypes.DEFAULT_TYPE, message: str):
    """Send notification to support group"""
    try:
        await context.bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=message
        )
    except Exception as e:
        logger.error(f"Failed to notify support group: {e}")

# --- CHEER COMMAND ---
async def cheer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cheer for a player by tagging them!"""
    chat = update.effective_chat
    user = update.effective_user
    
    # 1. Detect Target User
    target_name = "everyone"
    if update.message.reply_to_message:
        target_name = update.message.reply_to_message.from_user.first_name
    elif context.args:
        # Handle mentions like @username or text
        target_name = " ".join(context.args)

    # 2. Cheer Message
    cheer_msg = f"🎉 <b>CHEER SQUAD</b> 🎉\n"
    cheer_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    cheer_msg += f"📣 <b>{user.first_name}</b> is screaming for <b>{target_name}</b>!\n\n"
    cheer_msg += "<i>\"COME ON! YOU GOT THIS! SHOW YOUR POWER! 🏏🔥\"</i>\n"
    cheer_msg += "━━━━━━━━━━━━━━━━━━━━━━"

    # 3. Send GIF
    await update.message.reply_animation(
        animation=MEDIA_ASSETS.get("cheer", "CgACAgUAAxkBAAKKVWl2fuVvv784PaPoVkdJT_1tdc6RAALfHgACPYW4V56kdGdchAbtOAQ"),
        caption=cheer_msg,
        parse_mode=ParseMode.HTML
    )

# Yeh logic wahan dalo jahan over khatam hota hai (e.g., balls % 6 == 0)
def end_over_logic(match):
    # Team 1 batting kar rahi hai
    if not match.is_second_innings:
        current_total = match.team_x.score
        previous_total = sum(match.team_x_over_runs)
        runs_in_this_over = current_total - previous_total
        match.team_x_over_runs.append(runs_in_this_over)
    else:
        # Team 2 batting kar rahi hai
        current_total = match.team_y.score
        previous_total = sum(match.team_y_over_runs)
        runs_in_this_over = current_total - previous_total
        match.team_y_over_runs.append(runs_in_this_over)

# ============================================================
# REPLACEMENT 3: Replace scorecard_command function
# ============================================================

async def scorecard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Detailed scorecard with strike rates, boundaries, and bowling figures"""
    group_id = update.effective_chat.id
    match = active_matches.get(group_id)
    
    if not match:
        await update.message.reply_text("🏏 No active match! Use /start to begin")
        return
    
    if match.phase == GamePhase.SOLO_MATCH:
        await update.message.reply_text("🏏 Scorecard not available in solo mode")
        return
    
    text = "╔═════════════════════╗\n"
    text += "   📊 <b>DETAILED SCORECARD</b>\n"
    text += "╚═════════════════════╝\n\n"
    
    # First innings (Team X if they batted first, otherwise Team Y)
    first_team = match.batting_first if match.batting_first else match.team_x
    second_team = match.team_y if first_team == match.team_x else match.team_x
    
    # First Innings
    text += "━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🔸 <b>{first_team.name}</b> - 1st Innings\n"
    text += "━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📈 Score: <b>{first_team.score}/{first_team.wickets}</b>\n"
    text += f"⏱️ Overs: <b>{first_team.overs}</b>\n"
    text += f"➕ Extras: {first_team.extras}\n"
    
    # Run rate
    rr = (first_team.score / (first_team.balls / 6)) if first_team.balls > 0 else 0
    text += f"📊 Run Rate: <b>{rr:.2f}</b>\n\n"
    
    text += "┌─ <b>BATTING</b> ───────────┐\n"
    batted_count = 0
    for player in first_team.players:
        stats = first_team.player_stats.get(player.user_id, {})
        runs = stats.get('runs', 0)
        balls = stats.get('balls', 0)
        fours = stats.get('fours', 0)
        sixes = stats.get('sixes', 0)
        dismissal = stats.get('dismissal', 'not out')
        
        if balls > 0:  # Only show players who batted
            batted_count += 1
            sr = (runs / balls * 100) if balls > 0 else 0
            
            # Create mention tag with player name
            mention = f'<a href="tg://user?id={player.user_id}">{player.first_name}</a>'
            if player.user_id == first_team.captain_id:
                mention += " (C)"
            
            # Status emoji
            if dismissal != 'not out':
                status = "❌"
            else:
                status = "✅"
            
            text += f"{status} <b>{mention}</b>\n"
            text += f"   {runs}({balls}) SR: {sr:.1f}"
            
            # Boundaries
            if fours > 0 or sixes > 0:
                text += f" | "
                if fours > 0:
                    text += f"4️⃣×{fours} "
                if sixes > 0:
                    text += f"6️⃣×{sixes}"
            text += "\n"
            
            if dismissal != 'not out':
                text += f"   └─ <i>{dismissal}</i>\n"
            text += "\n"
    
    if batted_count == 0:
        text += "   <i>No batsmen yet</i>\n"
    
    text += "└────────────────────┘\n\n"
    
    # Bowling figures for first innings
    text += "┌─ <b>BOWLING</b> ──────────┐\n"
    bowled_count = 0
    for player in second_team.players:
        stats = second_team.player_stats.get(player.user_id, {})
        wickets = stats.get('wickets', 0)
        runs_conceded = stats.get('runs_conceded', 0)
        balls_bowled = stats.get('balls_bowled', 0)
        
        if balls_bowled > 0:  # Only show bowlers who bowled
            bowled_count += 1
            overs = balls_bowled // 6
            remaining_balls = balls_bowled % 6
            overs_str = f"{overs}.{remaining_balls}" if remaining_balls > 0 else str(overs)
            
            economy = (runs_conceded / (balls_bowled / 6)) if balls_bowled > 0 else 0
            
            mention = f'<a href="tg://user?id={player.user_id}">{player.first_name}</a>'
            
            text += f"⚡ <b>{mention}</b>\n"
            text += f"   {overs_str} overs | {wickets}/{runs_conceded} | Eco: {economy:.2f}\n\n"
    
    if bowled_count == 0:
        text += "   <i>No bowlers yet</i>\n"
    
    text += "└────────────────────┘\n\n"
    
    # Second innings if started
    if match.innings >= 2:
        text += "━━━━━━━━━━━━━━━━━━━━\n"
        text += f"🔸 <b>{second_team.name}</b> - 2nd Innings\n"
        text += "━━━━━━━━━━━━━━━━━━━━\n"
        text += f"📈 Score: <b>{second_team.score}/{second_team.wickets}</b>\n"
        text += f"⏱️ Overs: <b>{second_team.overs}</b>\n"
        text += f"➕ Extras: {second_team.extras}\n"
        
        if match.target > 0:
            needed = match.target - second_team.score
            balls_left = (match.total_overs * 6) - second_team.balls
            overs_left = balls_left / 6
            rrr = (needed / overs_left) if overs_left > 0 else 0
            
            text += f"🎯 Target: <b>{match.target}</b>\n"
            text += f"🔢 Need: <b>{needed}</b> runs from <b>{balls_left}</b> balls\n"
            text += f"📊 RRR: <b>{rrr:.2f}</b>\n"
        
        # Current run rate
        rr2 = (second_team.score / (second_team.balls / 6)) if second_team.balls > 0 else 0
        text += f"📊 CRR: <b>{rr2:.2f}</b>\n\n"
        
        text += "┌─ <b>BATTING</b> ───────────┐\n"
        batted_count2 = 0
        for player in second_team.players:
            stats = second_team.player_stats.get(player.user_id, {})
            runs = stats.get('runs', 0)
            balls = stats.get('balls', 0)
            fours = stats.get('fours', 0)
            sixes = stats.get('sixes', 0)
            dismissal = stats.get('dismissal', 'not out')
            
            if balls > 0:
                batted_count2 += 1
                sr = (runs / balls * 100) if balls > 0 else 0
                
                # Create mention tag
                mention = f'<a href="tg://user?id={player.user_id}">{player.first_name}</a>'
                if player.user_id == second_team.captain_id:
                    mention += " (C)"
                
                # Status emoji
                if dismissal != 'not out':
                    status = "❌"
                else:
                    status = "✅"
                
                text += f"{status} <b>{mention}</b>\n"
                text += f"   {runs}({balls}) SR: {sr:.1f}"
                
                # Boundaries
                if fours > 0 or sixes > 0:
                    text += f" | "
                    if fours > 0:
                        text += f"4️⃣×{fours} "
                    if sixes > 0:
                        text += f"6️⃣×{sixes}"
                text += "\n"
                
                if dismissal != 'not out':
                    text += f"   └─ <i>{dismissal}</i>\n"
                text += "\n"
        
        if batted_count2 == 0:
            text += "   <i>No batsmen yet</i>\n"
        
        text += "└────────────────────┘\n\n"
        
        # Bowling figures for second innings
        text += "┌─ <b>BOWLING</b> ──────────┐\n"
        bowled_count2 = 0
        for player in first_team.players:
            stats = first_team.player_stats.get(player.user_id, {})
            wickets = stats.get('wickets', 0)
            runs_conceded = stats.get('runs_conceded', 0)
            balls_bowled = stats.get('balls_bowled', 0)
            
            if balls_bowled > 0:
                bowled_count2 += 1
                overs = balls_bowled // 6
                remaining_balls = balls_bowled % 6
                overs_str = f"{overs}.{remaining_balls}" if remaining_balls > 0 else str(overs)
                
                economy = (runs_conceded / (balls_bowled / 6)) if balls_bowled > 0 else 0
                
                mention = f'<a href="tg://user?id={player.user_id}">{player.first_name}</a>'
                
                text += f"⚡ <b>{mention}</b>\n"
                text += f"   {overs_str} overs | {wickets}/{runs_conceded} | Eco: {economy:.2f}\n\n"
        
        if bowled_count2 == 0:
            text += "   <i>No bowlers yet</i>\n"
        
        text += "└────────────────────┘\n"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━\n"
    text += "✅ Not Out | ❌ Dismissed\n"
    text += "4️⃣ Fours | 6️⃣ Sixes | ⚡ Bowling\n"
    
    try:
        # Send with photo
        try:
            await update.message.reply_photo(
                photo=SCORECARD_PHOTO,
                caption=text,
                parse_mode=ParseMode.HTML
            )
        except Exception as photo_error:
            logger.error(f"Error sending scorecard photo: {photo_error}")
            # Fallback to text
            await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        # Fallback without formatting
        await update.message.reply_text(text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "").replace("<a href=\"tg://user?id=", "").replace("\">", " - ").replace("</a>", ""))

async def cleanup_inactive_matches(context: ContextTypes.DEFAULT_TYPE):
    """Auto-end matches inactive for > 15 minutes"""
    current_time = time.time()
    inactive_threshold = 15 * 60  # 15 Minutes in seconds
    chats_to_remove = []

    # Check all active matches
    for chat_id, match in active_matches.items():
        if current_time - match.last_activity > inactive_threshold:
            chats_to_remove.append(chat_id)
            try:
                # Send Time Out Message
                await context.bot.send_message(
                    chat_id=chat_id,
                    text="⏰ <b>Game Timeout!</b>\nMatch ended automatically due to 15 mins of inactivity.",
                    parse_mode=ParseMode.HTML
                )
                # Unpin message
                if match.main_message_id:
                    await context.bot.unpin_chat_message(chat_id=chat_id, message_id=match.main_message_id)
            except Exception as e:
                logger.error(f"Error ending inactive match {chat_id}: {e}")

    # Remove from memory
    for chat_id in chats_to_remove:
        if chat_id in active_matches:
            del active_matches[chat_id]

async def game_timer(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, player_type: str, player_name: str):
    """Handles 45s timer with Penalties & Disqualification"""
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        # Warning
        await context.bot.send_message(group_id, f"⏳ <b>Hurry Up {player_name}!</b> 15 seconds left!", parse_mode=ParseMode.HTML)
        
        # Wait remaining 15 seconds
        await asyncio.sleep(15)
        
        # --- TIMEOUT HAPPENED ---
        await handle_timeout_penalties(context, group_id, match, player_type)
            
    except asyncio.CancelledError:
        pass # Timer stopped safely because player played

async def handle_timeout_penalties(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, player_type: str):
    """Process Penalties for Timeouts"""
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # --- BOWLER TIMEOUT ---
    if player_type == "bowler":
        bowler = bowl_team.players[bowl_team.current_bowler_idx]
        bowler.bowling_timeouts += 1
        
        # Case A: Disqualification (3 Timeouts)
        if bowler.bowling_timeouts >= 3:
            msg = f"🚫 <b>DISQUALIFIED!</b> {bowler.first_name} timed out 3 times!\n"
            msg += "⚠️ <b>The over will RESTART with a new bowler.</b>"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Reset Balls for this over (Over restart logic)
            # Example: If balls were 3.2 (20 balls), reset to 3.0 (18 balls)
            current_over_balls = bowl_team.get_current_over_balls()
            bowl_team.balls -= current_over_balls
            
            # Remove bowler from attack
            bowl_team.current_bowler_idx = None
            bowler.is_bowling_banned = True # Ban for this match (optional, or just this over)
            
            # Request New Bowler
            match.current_ball_data = {} # Clear ball data
            await request_bowler_selection(context, group_id, match)
            return

        # Case B: No Ball (Standard Timeout)
        else:
            bat_team.score += 1 # Penalty Run
            bat_team.extras += 1
            match.is_free_hit = True # Activate Free Hit
            
            msg = f"⏰ <b>BOWLER TIMEOUT!</b> ({bowler.bowling_timeouts}/3)\n"
            msg += "🚫 <b>Result:</b> NO BALL! (+1 Run)\n"
            msg += "⚡ <b>Next ball is a FREE HIT!</b>"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Reset inputs to allow re-bowl (No ball doesn't count legal ball)
            match.current_ball_data = {"bowler_id": bowler.user_id, "bowler_number": None, "group_id": group_id}
            
            # Restart Bowler Timer for re-bowl
            match.ball_timeout_task = asyncio.create_task(game_timer(context, group_id, match, "bowler", bowler.first_name))
            
            # Notify Bowler again
            try: await context.bot.send_message(bowler.user_id, "⚠️ <b>Timeout! It's a No Ball. Bowl again!</b>", parse_mode=ParseMode.HTML)
            except: pass

    # --- BATSMAN TIMEOUT ---
    elif player_type == "batsman":
        striker = bat_team.players[bat_team.current_batsman_idx]
        striker.batting_timeouts += 1
        
        # Case A: Hit Wicket (3 Timeouts)
        if striker.batting_timeouts >= 3:
            msg = f"🚫 <b>DISMISSED!</b> {striker.first_name} timed out 3 times.\n"
            msg += "❌ <b>Result:</b> HIT WICKET (OUT)!"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Trigger Wicket Logic Manually
            match.current_ball_data["batsman_number"] = match.current_ball_data["bowler_number"] # Force match numbers to trigger out
            await process_ball_result(context, group_id, match)
            
        # Case B: Penalty Runs (-6 Runs)
        else:
            bat_team.score -= 6
            bat_team.score = max(0, bat_team.score) # Score negative nahi jayega
            
            msg = f"⏰ <b>BATSMAN TIMEOUT!</b> ({striker.batting_timeouts}/3)\n"
            msg += "📉 <b>Penalty:</b> -6 Runs!\n"
            msg += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
            msg += "🔄 <b>Ball Counted.</b> (Dot Ball)"
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            # Count ball but no runs (Treat as Dot Ball)
            bowl_team.update_overs()
            match.current_ball_data = {} # Reset
            
            if bowl_team.get_current_over_balls() == 0:
                await check_over_complete(context, group_id, match)
            else:
                await execute_ball(context, group_id, match)

async def celebrate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Celebration GIF"""

    chat = update.effective_chat
    user = update.effective_user

    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No active match right now.")
        return

    celebration_gifs = [
        "https://tenor.com/boXSI.gif",
        "https://tenor.com/bot7y.gif",
        "https://tenor.com/800z.gif"
    ]

    caption = (
        f"🎉 <b>{user.first_name}</b> celebrates in style! 🎊\n\n"
        "<i>\"YESSS! That's how it's done!\"</i> 🔥"
    )

    await update.message.reply_animation(
        animation=random.choice(celebration_gifs),
        caption=caption,
        parse_mode=ParseMode.HTML
    )

async def huddle_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Team Motivation"""

    chat = update.effective_chat
    user = update.effective_user

    if chat.id not in active_matches:
        await update.message.reply_text("⚠️ No active match right now.")
        return

    huddle_messages = [
        "🔥 <b>COME ON TEAM!</b> We got this! Let's show them what we're made of! 💪",
        "⚡ <b>FOCUS UP!</b> One ball at a time. We're in this together! 🤝",
        "🎯 <b>STAY CALM!</b> Stick to the plan. Victory is ours! 🏆",
        "💥 <b>LET'S GO!</b> Time to dominate! Show no mercy! ⚔️",
        "🌟 <b>BELIEVE!</b> We've trained for this. Execute perfectly! ✨"
    ]

    msg = (
        f"📣 <b>{user.first_name}</b> calls a team huddle!\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{random.choice(huddle_messages)}"
    )

    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def solo_game_timer(context, chat_id, match, player_type, player_name):
    """Timer specifically for Solo Mode (45s)"""
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        try:
            await context.bot.send_message(
                chat_id, 
                f"⏳ <b>Hurry Up {player_name}!</b> 15 seconds left!", 
                parse_mode=ParseMode.HTML
            )
        except: pass
        
        # Remaining 15 Seconds
        await asyncio.sleep(15)
        
        # Timeout Trigger
        await handle_solo_timeout(context, chat_id, match, player_type)
            
    except asyncio.CancelledError:
        pass

async def handle_solo_timeout(context, chat_id, match, player_type):
    """Handle Penalties for Solo Mode Timeouts"""
    
    # --- BATSMAN TIMEOUT ---
    if player_type == "batsman":
        batter = match.solo_players[match.current_solo_bat_idx]
        batter.batting_timeouts += 1
        
        # 3 Timeouts = AUTO OUT
        if batter.batting_timeouts >= 3:
            msg = f"🚫 <b>AUTO-OUT!</b> {batter.first_name} timed out 3 times.\n❌ <b>Result:</b> Wicket!"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            # Simulate Wicket Logic
            match.current_ball_data["batsman_number"] = match.current_ball_data["bowler_number"]
            await process_solo_turn_result(context, chat_id, match)
            return

        # < 3 Timeouts = -6 Penalty
        else:
            penalty = 6
            batter.runs -= penalty
            if batter.runs < 0: batter.runs = 0
            
            msg = f"⏰ <b>TIMEOUT WARNING!</b> ({batter.batting_timeouts}/3)\n"
            msg += f"📉 <b>Penalty:</b> -6 Runs deducted!\n"
            msg += f"📊 <b>Current Score:</b> {batter.runs}"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            batter.balls_faced += 1
            await rotate_solo_bowler(context, chat_id, match)

    # --- BOWLER TIMEOUT ---
    elif player_type == "bowler":
        bowler = match.solo_players[match.current_solo_bowl_idx]
        batter = match.solo_players[match.current_solo_bat_idx]
        bowler.bowling_timeouts += 1
        
        # 3 Timeouts = BANNED
        if bowler.bowling_timeouts >= 3:
            bowler.is_bowling_banned = True
            msg = f"🚫 <b>BANNED!</b> {bowler.first_name} timed out 3 times!\n"
            msg += "⚠️ <b>They are removed from bowling rotation!</b>"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            await rotate_solo_bowler(context, chat_id, match, force_new_bowler=True)
            return
            
        # < 3 Timeouts = NO BALL + FREE HIT
        else:
            batter.runs += 1
            match.is_free_hit = True
            
            msg = f"⏰ <b>BOWLER TIMEOUT!</b> ({bowler.bowling_timeouts}/3)\n"
            msg += "🚫 <b>Result:</b> DEAD BALL! (+1 Run)\n"
            msg += "⚡ <b>Next ball is a NORMAL BALL!</b>\n"
            msg += "🔄 <i>Bowler must bowl again!</i>"
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            match.ball_timeout_task = asyncio.create_task(
                solo_game_timer(context, chat_id, match, "bowler", bowler.first_name)
            )

# Helper to Rotate Bowler in Solo (Handles the Skip Logic)
async def rotate_solo_bowler(context, chat_id, match, force_new_bowler=False):
    """Rotates bowler, skipping banned players"""
    
    if not force_new_bowler:
        match.solo_balls_this_spell += 1

    if match.solo_balls_this_spell >= 3 or force_new_bowler:
        match.solo_balls_this_spell = 0
        
        original_idx = match.current_solo_bowl_idx
        attempts = 0
        total_players = len(match.solo_players)
        
        while True:
            match.current_solo_bowl_idx = (match.current_solo_bowl_idx + 1) % total_players
            
            if match.current_solo_bowl_idx == match.current_solo_bat_idx:
                match.current_solo_bowl_idx = (match.current_solo_bowl_idx + 1) % total_players
                
            next_bowler = match.solo_players[match.current_solo_bowl_idx]
            
            if not next_bowler.is_bowling_banned:
                break
            
            attempts += 1
            if attempts > total_players:
                await context.bot.send_message(chat_id, "⚠️ All players banned! Game Over.")
                await end_solo_game_logic(context, chat_id, match)
                return

        new_bowler = match.solo_players[match.current_solo_bowl_idx]
        await context.bot.send_message(chat_id, f"🔄 <b>New Bowler:</b> {new_bowler.first_name}", parse_mode=ParseMode.HTML)

    await trigger_solo_ball(context, chat_id, match)

# Start command handler
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Welcome message with Add to Group button"""
    user = update.effective_user
    chat = update.effective_chat
    
    is_new_user = False
    
    if user.id not in user_data:
        user_data[user.id] = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(user.id)
        save_data()
        is_new_user = True

        if chat.type == "private":
            try:
                await context.bot.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=f"🆕 <b>New User Started Bot</b>\n👤 {user.first_name} (<a href='tg://user?id={user.id}'>{user.id}</a>)\n🎈 @{user.username}",
                    parse_mode=ParseMode.HTML
                )
            except: pass

    welcome_text = "🏏 <b>WELCOME TO CRICOVERSE</b> 🏏\n"
    welcome_text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    welcome_text += "The ultimate Hand Cricket experience on Telegram.\n\n"
    welcome_text += "🔥 <b>Features:</b>\n"
    welcome_text += "• 🏟 Group Matches\n"
    welcome_text += "• 📺 DRS System\n"
    welcome_text += "• 📊 Career Stats\n"
    welcome_text += "• 🎙 Live Commentary\n"
    welcome_text += "• 🎪 Tournament/Auction Mode\n\n"
    welcome_text += "👇 <b>How to Play:</b>\n"
    welcome_text += "Add me to your group and type <code>/game</code> to start!"

    
    if chat.type == "private":
        # Add to Group button
        bot_username = (await context.bot.get_me()).username
        keyboard = [
            [InlineKeyboardButton("➕ Add to Group", url=f"https://t.me/{bot_username}?startgroup=true")]
        ]
        
        await update.message.reply_photo(
            photo=MEDIA_ASSETS["welcome"],
            caption=welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text("Bot is ready! Use /game to start.")

# Help command
# --- HELPER FUNCTIONS FOR HELP MENU ---
def get_help_main_text():
    return (
        "<b>CRICOVERSE HELP CENTER</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "Welcome to the ultimate cricket bot!\n\n"
        "👇 <b>Select a category below:</b>\n\n"
        "👥 <b>Team Mode:</b> Commands for Team matches.\n"
        "⚔️ <b>Solo Mode:</b> Commands for Solo survival.\n"
        "📚 <b>Tutorial:</b> How to play guide."
    )


def get_help_team_text():
    return (
        "👥 <b>TEAM MODE COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🛠 <b>Host Commands:</b>\n"
        "• <code>/game</code> - Setup Match\n"
        "• <code>/extend 60</code> - Add joining time\n"
        "• <code>/endmatch</code> - Force End Game\n"
        "• <code>/timeout</code> - Take Strategic Timeout\n\n"
        
        "🧢 <b>Captain Commands:</b>\n"
        "• <code>/batting [no]</code> - Select Batsman\n"
        "• <code>/bowling [no]</code> - Select Bowler\n"
        "• <code>/drs</code> - Review Wicket Decision\n\n"
        
        "📊 <b>General:</b>\n"
        "• <code>/scorecard</code> - View Match Summary\n"
        "• <code>/players</code> - View Squads\n"
        "• <code>/mystats</code> - Your Career Profile\n\n"
        
        "🎪 <b>TOURNAMENT / AUCTION MODE:</b>\n"
        "• <code>/auction</code> - Start auction setup\n"
        "• <code>/bidder [Team]</code> - Assign team bidder\n"
        "• <code>/aucplayer</code> - Add player to pool\n"
        "• <code>/startauction</code> - Begin live auction\n"
        "• <code>/bid [amount]</code> - Place bid\n"
        "• <code>/wallet</code> - Check team purses\n"
        "• <code>/aucsummary</code> - View auction status\n"
        "• <code>/pause</code> - Pause auction timer\n"
        "• <code>/resume</code> - Resume auction\n"
        "• <code>/cancelbid</code> - Cancel last bid\n"
        "• <code>/assist [team]</code> - Auctioneer assist mode\n"
        "• <code>/changeauctioneer</code> - Vote to change auctioneer\n"
        "• <code>/unsold</code> - View unsold players"
    )

def get_help_solo_text():
    return (
        "⚔️ <b>SOLO MODE COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "<i>1 vs All. Infinite Batting. Auto-Rotation.</i>\n\n"
        
        "🛠 <b>Host Commands:</b>\n"
        "• <code>/game</code> - Select 'Solo Mode' button\n"
        "• <code>/extendsolo 60</code> - Add joining time\n"
        "• <code>/endsolo</code> - End Game & Show Winner\n\n"
        
        "👤 <b>Player Commands:</b>\n"
        "• <code>/soloscore</code> - Live Leaderboard\n"
        "• <code>/soloplayers</code> - Player Status List\n"
        "• <code>/mystats</code> - Your Career Profile\n\n"
        
        "🎮 <b>Gameplay:</b>\n"
        "• <b>Batting:</b> Send 0-6 in Group Chat.\n"
        "• <b>Bowling:</b> Send 0-6 in Bot DM."
    )

def get_help_tutorial_text():
    return (
        "📚 <b>HOW TO PLAY</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>1. Starting a Game:</b>\n"
        "• Add bot to a group.\n"
        "• Type <code>/game</code> and choose Mode.\n"
        "• Players click 'Join'. Host clicks 'Start'.\n\n"
        
        "<b>2. Batting & Bowling:</b>\n"
        "• <b>Bowler:</b> Receives a DM from Bot. Sends a number (0-6).\n"
        "• <b>Batsman:</b> Bot alerts in Group. Batsman sends number (0-6) in Group.\n\n"
        
        "<b>3. Scoring:</b>\n"
        "• Same Number = <b>OUT</b> ❌\n"
        "• Different Number = <b>RUNS</b> 🏏\n\n"
        
        "<i>Tip: Use /mystats to check your RPG Level!</i>"
    )

# --- MAIN HELP COMMAND ---
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Interactive Help Menu"""
    
    # Main Menu Keyboard
    keyboard = [
        [InlineKeyboardButton("👥 Team Mode", callback_data="help_team"),
         InlineKeyboardButton("⚔️ Solo Mode", callback_data="help_solo")],
        [InlineKeyboardButton("📚 Tutorial", callback_data="help_tutorial")],
        [InlineKeyboardButton("❌ Close", callback_data="help_close")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Send Photo with Caption
    await update.message.reply_photo(
        photo=MEDIA_ASSETS.get("help", "https://t.me/cricoverse/6"), # Fallback URL added
        caption=get_help_main_text(),
        reply_markup=reply_markup,
        parse_mode=ParseMode.HTML
    )

# --- CALLBACK HANDLER FOR HELP NAVIGATION ---
async def help_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Help Menu Navigation"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    
    if data == "help_close":
        await query.message.delete()
        return

    # Determine Text based on selection
    if data == "help_main":
        text = get_help_main_text()
        keyboard = [
            [InlineKeyboardButton("👥 Team Mode", callback_data="help_team"),
             InlineKeyboardButton("⚔️ Solo Mode", callback_data="help_solo")],
            [InlineKeyboardButton("📚 Tutorial", callback_data="help_tutorial")],
            [InlineKeyboardButton("❌ Close", callback_data="help_close")]
        ]
    
    elif data == "help_team":
        text = get_help_team_text()
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="help_main")]]
        
    elif data == "help_solo":
        text = get_help_solo_text()
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="help_main")]]
        
    elif data == "help_tutorial":
        text = get_help_tutorial_text()
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="help_main")]]
    
    # Update the Caption (Image remains same)
    try:
        await query.message.edit_caption(
            caption=text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass # Ignore if message not modified
        
async def game_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Game start menu - DEBUG VERSION
    Logs every step to console to identify where it fails.
    """
    try:
        # Step 1: Entry Log
        print("\n--- DEBUG: game_command TRIGGERED ---")
        
        chat = update.effective_chat
        user = update.effective_user
        
        print(f"DEBUG: User: {user.first_name} (ID: {user.id})")
        print(f"DEBUG: Chat: {chat.title} (ID: {chat.id}, Type: {chat.type})")

        # Step 2: Ban Check
        print("DEBUG: Checking group ban status...")
        if not await check_group_ban(update, context):
            print("DEBUG: ❌ Group is BANNED. Stopping execution.")
            return
        print("DEBUG: ✅ Group is NOT banned.")
        
        # Step 3: Private Chat Check
        if chat.type == "private":
            print("DEBUG: ⚠️ Private chat detected. Sending warning.")
            await context.bot.send_message(chat.id, "⚠️ This command only works in groups!")
            return
        
        # Step 4: Active Match Check
        print("DEBUG: Checking for active matches...")
        if chat.id in active_matches:
            match = active_matches[chat.id]
            print(f"DEBUG: Match found in memory. Phase: {match.phase}")
            
            # Ghost Match Auto-Fix
            if match.phase == GamePhase.MATCH_ENDED:
                print("DEBUG: 👻 Ghost match detected (Ended but not deleted). Deleting now...")
                del active_matches[chat.id]
            else:
                print("DEBUG: ⚠️ Valid match active. Sending warning.")
                await context.bot.send_message(chat.id, "⚠️ Match already in progress! Use /endmatch to stop it.")
                return
        else:
            print("DEBUG: ✅ No active match found. Proceeding.")
            
        # Step 5: Register Group
        if chat.id not in registered_groups:
            print("DEBUG: 🆕 New group detected. Registering in DB...")
            registered_groups[chat.id] = {
                "group_id": chat.id, 
                "group_name": chat.title, 
                "total_matches": 0,
                "commentary_style": "english"
            }
            save_data()
            print("DEBUG: ✅ Group registered successfully.")
            
            # Notify Support Group (Non-blocking)
            try:
                print("DEBUG: Sending alert to Support Group...")
                msg = f"🆕 <b>Bot Added to New Group</b>\n🆔 <code>{chat.id}</code>"
                await context.bot.send_message(chat_id=SUPPORT_GROUP_ID, text=msg, parse_mode=ParseMode.HTML)
            except Exception as e:
                print(f"DEBUG: ⚠️ Support notification failed (Ignored): {e}")

        # Step 6: Prepare UI
        print("DEBUG: Preparing Buttons & Text...")
        keyboard = [
            [InlineKeyboardButton("👥 Team Mode", callback_data="mode_team"),
            InlineKeyboardButton("⚔️ Solo Mode", callback_data="mode_solo")],
            [InlineKeyboardButton("🏆 Tournament Mode", callback_data="mode_tournament")],
            [InlineKeyboardButton("🤖 AI Mode (DM)", callback_data="mode_ai")]
        ]
        
        msg = "🎮 <b>SELECT GAME MODE</b> 🎮\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "Choose your battle style below:"
        
        # Step 7: Send Message (Try Photo first, then Text)
        print("DEBUG: Attempting to send UI to chat...")
        
        try:
            photo_url = MEDIA_ASSETS.get("mode_select", "https://t.me/cricoverse/7")
            print(f"DEBUG: Trying send_photo with URL: {photo_url}")
            
            # Using context.bot.send_photo instead of reply_photo (Safest method)
            await context.bot.send_photo(
                chat_id=chat.id,
                photo=photo_url,
                caption=msg,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
            print("DEBUG: ✅ SUCCESS! Photo menu sent.")
            
        except Exception as photo_error:
            print(f"DEBUG: ❌ Photo send FAILED. Error: {photo_error}")
            print("DEBUG: 🔄 Attempting fallback to TEXT ONLY mode...")
            
            try:
                await context.bot.send_message(
                    chat_id=chat.id,
                    text=msg,
                    reply_markup=InlineKeyboardMarkup(keyboard),
                    parse_mode=ParseMode.HTML
                )
                print("DEBUG: ✅ SUCCESS! Text menu sent (Fallback).")
            except Exception as text_error:
                print(f"DEBUG: ❌❌ CRITICAL: Text send ALSO failed. Error: {text_error}")
                print("DEBUG: Check Bot Permissions in this group (Send Messages/Embed Links).")

    except Exception as e:
        print(f"DEBUG: 💀 UNEXPECTED CRASH in game_command: {e}")
        logger.error(f"Critical error in game_command: {e}", exc_info=True)
        try:
            await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Critical Error. Check logs.")
        except: pass

# Callback query handler for mode selection
async def mode_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat = query.message.chat
    user = query.from_user

    if chat.id in active_matches:
        await query.message.reply_text("⚠️ Match already active!")
        return

    if query.data == "mode_team":
        await start_team_mode(query, context, chat, user)

    elif query.data == "mode_solo":
        # ... (Same code as before) ...
        match = Match(chat.id, chat.title)
        match.game_mode = "SOLO"
        match.phase = GamePhase.SOLO_JOINING
        match.host_id = user.id
        match.host_name = user.first_name
        match.solo_join_end_time = time.time() + 120

        active_matches[chat.id] = match

        if user.id not in player_stats:
            init_player_stats(user.id)

        p = Player(user.id, user.username or "", user.first_name)
        match.solo_players.append(p)

        await update_solo_board(context, chat.id, match)
        match.solo_timer_task = asyncio.create_task(
            solo_join_countdown(context, chat.id, match)
        )

    elif query.data == "mode_tournament":
        # Check approval
        if chat.id not in TOURNAMENT_APPROVED_GROUPS:
            await query.answer(
                "🚫 Tournament not approved! Contact bot owner.",
                show_alert=True
            )
            return

        # Show tournament options
        keyboard = [
            [InlineKeyboardButton("🎯 Start Auction", callback_data="start_auction")],
            [InlineKeyboardButton("🔙 Back", callback_data="back_to_modes")]
        ]

        # ✅ FIX: Use edit_caption instead of edit_text because the message is a Photo
        try:
            await query.message.edit_caption(
                caption="🏆 <b>TOURNAMENT MODE</b>\n\nSelect option below:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            # Fallback if somehow it's a text message
            await query.message.edit_text(
                text="🏆 <b>TOURNAMENT MODE</b>\n\nSelect option below:",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
    
    elif query.data == "mode_ai":
        # AI Mode - Play vs Bot in DM
        await query.answer("🤖 Opening AI Mode in your DM...", show_alert=True)
        
        # Send DM to user
        try:
            await context.bot.send_message(
                chat_id=user.id,
                text=(
                    "🤖 <b>AI MODE - Play vs Computer</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "Welcome to single-player mode!\n"
                    "You'll play against an AI opponent.\n\n"
                    "Use /aistart to begin a match!"
                ),
                parse_mode=ParseMode.HTML
            )
        except Forbidden:
            await query.message.reply_text(
                "⚠️ Please start the bot in DM first!\n"
                f"Click here: @{(await context.bot.get_me()).username}"
            )

async def auction_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle auction related callbacks"""
    query = update.callback_query
    await query.answer()
    
    if query.data == "start_auction":
        # ... (Check active auctions code) ...
        chat = query.message.chat
        user = query.from_user
        
        if chat.id in active_auctions:
            await query.answer("Auction already active!", show_alert=True)
            return
        
        auction = Auction(chat.id, chat.title, user.id, user.first_name)
        active_auctions[chat.id] = auction
        
        msg = (
            "🏏 <b>AUCTION SETUP</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "📋 <b>Next Steps:</b>\n"
            "1️⃣ Assign bidders: <code>/bidder [team]</code>\n"
            "2️⃣ Add players: <code>/aucplayer</code>\n"
            "3️⃣ Select auctioneer below\n"
            "4️⃣ Start: <code>/startauction</code>\n\n"
            "💰 Default Purse: 1000"
        )
        
        keyboard = [[InlineKeyboardButton("🎤 Be Auctioneer", callback_data="become_auctioneer")]]
        
        # ✅ FIX: Use edit_caption
        await query.message.edit_caption(
            caption=msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    
    elif query.data == "become_auctioneer":
        chat = query.message.chat
        user = query.from_user
        
        if chat.id not in active_auctions:
            return
        
        auction = active_auctions[chat.id]
        auction.auctioneer_id = user.id
        auction.auctioneer_name = user.first_name
        
        # ✅ FIX: Use edit_caption
        await query.message.edit_caption(
            caption=f"✅ <b>Auctioneer Set!</b>\n\n🎤 {get_user_tag(user)}\n\nNext: Assign bidders with <code>/bidder</code>",
            parse_mode=ParseMode.HTML
        )
    
    elif query.data == "back_to_modes":
        keyboard = [
            [InlineKeyboardButton("👥 Team Mode", callback_data="mode_team"),
            InlineKeyboardButton("⚔️ Solo Mode", callback_data="mode_solo")],
            [InlineKeyboardButton("🏆 Tournament", callback_data="mode_tournament")]
        ]
        
        # ✅ FIX: Use edit_caption
        await query.message.edit_caption(
            caption="🎮 <b>SELECT GAME MODE</b> 🎮\n━━━━━━━━━━━━━━━━━━━━━━\nChoose your battle style below:",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )

async def update_solo_board(context, chat_id, match):
    """Updates the Solo Joining List with Host Tag"""
    count = len(match.solo_players)
    
    # Time Calc
    remaining = int(match.solo_join_end_time - time.time())
    if remaining < 0: remaining = 0
    mins, secs = divmod(remaining, 60)
    
    # Generate Host Tag
    if match.host_id and match.host_name:
        host_tag = f"<a href='tg://user?id={match.host_id}'>{match.host_name}</a>"
    else:
        host_tag = "Unknown"
    
    msg = "⚔️ <b>SOLO BATTLE ROYALE</b> ⚔️\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🎙 <b>Host:</b> {host_tag}\n"
    msg += f"⏳ <b>Time Left:</b> <code>{mins:02d}:{secs:02d}</code>\n"
    msg += f"👥 <b>Players Joined:</b> {count}\n\n"
    
    msg += "<b>PLAYER LIST:</b>\n"
    if match.solo_players:
        for i, p in enumerate(match.solo_players, 1):
            ptag = f"<a href='tg://user?id={p.user_id}'>{p.first_name}</a>"
            msg += f"  {i}. {ptag}\n"
    else:
        msg += "  <i>Waiting for players...</i>\n"
        
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "👇 <i>Click Join to Enter the Ground!</i>"
    
    # Buttons
    keyboard = [
        [InlineKeyboardButton("✅ Join Battle", callback_data="solo_join"),
         InlineKeyboardButton("🚪 Leave", callback_data="solo_leave")]
    ]
    
    # Show START button if enough players
    if count >= 2:
        keyboard.append([InlineKeyboardButton("🚀 START MATCH", callback_data="solo_start_game")])
        
    await refresh_game_message(context, chat_id, match, msg, InlineKeyboardMarkup(keyboard), media_key="joining")

async def start_team_mode(query, context: ContextTypes.DEFAULT_TYPE, chat, user):
    """Initialize team mode with Fancy Image"""
    # Create new match
    match = Match(chat.id, chat.title)
    active_matches[chat.id] = match
    
    # Set time (2 minutes)
    match.team_join_end_time = time.time() + 120
    
    # Buttons
    keyboard = [
        [InlineKeyboardButton("🧊 Join Team X", callback_data="join_team_x"),
         InlineKeyboardButton("🔥 Join Team Y", callback_data="join_team_y")],
        [InlineKeyboardButton("🚪 Leave Team", callback_data="leave_team")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Fancy Text
    text = get_team_join_message(match)
    
    # Send using Master Function (With Image)
    await refresh_game_message(context, chat.id, match, text, reply_markup, media_key="joining")
    
    # Start Timer
    match.join_phase_task = asyncio.create_task(
        team_join_countdown(context, chat.id, match)
    )

def get_team_join_message(match: Match) -> str:
    """Generate Professional Joining List"""
    remaining = max(0, int(match.team_join_end_time - time.time()))
    minutes = remaining // 60
    seconds = remaining % 60
    
    total_p = len(match.team_x.players) + len(match.team_y.players)
    
    msg = "🏆 <b>CRICOVERSE MATCH REGISTRATION</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⏳ <b>Time Remaining:</b> <code>{minutes:02d}:{seconds:02d}</code>\n"
    msg += f"👥 <b>Total Players:</b> {total_p}\n\n"
    
    # Team X List
    msg += "🧊 <b>TEAM X</b>\n"
    if match.team_x.players:
        for i, p in enumerate(match.team_x.players, 1):
            msg += f"  ├ {i}. {p.first_name}\n"
    else:
        msg += "  └ <i>Waiting for players...</i>\n"
    
    msg += "\n"
    
    # Team Y List
    msg += "🔥 <b>TEAM Y</b>\n"
    if match.team_y.players:
        for i, p in enumerate(match.team_y.players, 1):
            msg += f"  ├ {i}. {p.first_name}\n"
    else:
        msg += "  └ <i>Waiting for players...</i>\n"
            
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Click buttons below to join your squad!</i>"
    
    return msg

async def team_join_countdown(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Countdown timer that updates the Board safely"""
    try:
        warning_sent = False
        while True:
            # ✅ FIX: Agar Phase Joining nahi hai, to Timer band kar do
            if match.phase != GamePhase.TEAM_JOINING:
                break

            remaining = match.team_join_end_time - time.time()
            
            # 30 Seconds Warning
            if remaining <= 30 and remaining > 20 and not warning_sent:
                await context.bot.send_message(
                    group_id, 
                    "⚠️ <b>Hurry Up! Only 30 seconds left to join!</b>", 
                    parse_mode=ParseMode.HTML
                )
                warning_sent = True

            # Time Up
            if remaining <= 0:
                await end_team_join_phase(context, group_id, match)
                break
            
            # Wait 10 seconds
            await asyncio.sleep(10)
            
            # ✅ FIX: Update karne se pehle phir check karo
            if match.phase == GamePhase.TEAM_JOINING:
                await update_joining_board(context, group_id, match)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Timer error: {e}")

async def end_team_join_phase(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """End joining phase and start Host Selection"""
    total_players = len(match.team_x.players) + len(match.team_y.players)
    
    # Min 4 Players Check
    if total_players < 4:
        await context.bot.send_message(
            chat_id=group_id,
            text="❌ <b>Match Cancelled!</b>\nYou need at least 4 players (2 per team) to start.",
            parse_mode=ParseMode.HTML
        )
        try: await context.bot.unpin_chat_message(group_id, match.main_message_id)
        except: pass
        del active_matches[group_id]
        return
    
    match.phase = GamePhase.HOST_SELECTION
    
    keyboard = [[InlineKeyboardButton("🙋‍♂️ I Want to be Host", callback_data="become_host")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    host_text = f"✅ <b>REGISTRATION CLOSED!</b>\n"
    host_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    host_text += f"Total Players: <b>{total_players}</b>\n\n"
    host_text += "<b>Who wants to be the Host?</b>\n"
    host_text += "<i>Host will select overs and finalize the teams.</i>"
    
    # Send with Host Image and Pin
    await refresh_game_message(context, group_id, match, host_text, reply_markup, media_key="host")

# Team join/leave callback handlers
async def team_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle team join/leave with tagging alerts & Auto-Update"""
    query = update.callback_query
    
    # Quick answer to stop loading animation
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_JOINING:
        await context.bot.send_message(chat.id, "⚠️ Joining phase has ended!")
        return
    
    # Initialize User Data
    if user.id not in user_data:
        user_data[user.id] = {
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(user.id)
        save_data()

    user_tag = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    alert_msg = ""
    updated = False

    # JOIN LOGIC
    if query.data == "join_team_x":
        if not match.team_x.get_player(user.id):
            if match.team_y.get_player(user.id):
                match.team_y.remove_player(user.id)
            
            player = Player(user.id, user.username or "", user.first_name)
            match.team_x.add_player(player)
            alert_msg = f"✅ {user_tag} joined <b>Team X</b>!"
            updated = True
    
    elif query.data == "join_team_y":
        if not match.team_y.get_player(user.id):
            if match.team_x.get_player(user.id):
                match.team_x.remove_player(user.id)
            
            player = Player(user.id, user.username or "", user.first_name)
            match.team_y.add_player(player)
            alert_msg = f"✅ {user_tag} joined <b>Team Y</b>!"
            updated = True
    
    elif query.data == "leave_team":
        if match.team_x.remove_player(user.id) or match.team_y.remove_player(user.id):
            alert_msg = f"👋 {user_tag} left the team."
            updated = True

    # 1. Send Alert in Group (Naya message)
    if alert_msg:
        await context.bot.send_message(chat.id, alert_msg, parse_mode=ParseMode.HTML)

    # 2. Update the Board (Agar change hua hai)
    if updated:
        await update_joining_board(context, chat.id, match)

# Extend command (Admins only)
async def extend_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Extend match time - ADMIN ONLY, MAX 180 seconds"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    match = active_matches.get(chat_id)
    if not match:
        await update.message.reply_text("❌ No active match!")
        return
    
    # Check if user is admin
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        is_admin = member.status in ['creator', 'administrator']
    except:
        is_admin = False
    
    if not is_admin:
        await update.message.reply_text("❌ Only group admins can extend match time!")
        return
    
    if not context.args:
        await update.message.reply_text(
            "❌ Usage: /extend <seconds>\n"
            "Example: /extend 120\n"
            "⚠️ Maximum: 180 seconds"
        )
        return
    
    try:
        seconds = int(context.args[0])
    except ValueError:
        await update.message.reply_text("❌ Please provide a valid number!")
        return
    
    # Maximum 180 seconds
    if seconds > 180:
        await update.message.reply_text("❌ Maximum extension is 180 seconds (3 minutes)!")
        return
    
    if seconds <= 0:
        await update.message.reply_text("❌ Extension must be greater than 0!")
        return
    
    # Extend time
    match.last_activity = datetime.now() + timedelta(seconds=seconds)
    
    await update.message.reply_text(
        f"⏰ Match time extended by {seconds} seconds!\n"
        f"New deadline: {match.last_activity.strftime('%I:%M:%S %p')}"
    )
    
    # Refresh Game Board
    text = get_team_join_message(match)
    keyboard = [
        [InlineKeyboardButton("🧊 Join Team X", callback_data="join_team_x"),
         InlineKeyboardButton("🔥 Join Team Y", callback_data="join_team_y")],
        [InlineKeyboardButton("🚪 Leave Team", callback_data="leave_team")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # Use master function to keep it at bottom and pinned
    await refresh_game_message(context, chat.id, match, text, reply_markup=reply_markup, media_key="joining")

# Host selection callback
async def host_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Host Selection safely with 4-20 Overs"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        return
        
    match = active_matches[chat.id]
    
    # Check if someone is already host
    if match.host_id is not None:
        await query.answer("Host already selected!", show_alert=True)
        return

    # Set Host
    match.host_id = user.id
    match.host_name = user.first_name
    match.last_activity = time.time()  # Reset timer
    
    match.phase = GamePhase.OVER_SELECTION
    
    # --- LOGIC FOR 4 TO 20 OVERS ---
    keyboard = []
    row = []
    # Loop from 4 to 20 (inclusive)
    for i in range(4, 21):
        # Add button to current row
        row.append(InlineKeyboardButton(f"{i}", callback_data=f"overs_{i}"))
        
        # If row has 4 buttons, add it to keyboard and start new row
        if len(row) == 4:
            keyboard.append(row)
            row = []
            
    # Add any remaining buttons
    if row:
        keyboard.append(row)
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # --- FIX: Generate User Tag ---
    user_tag = get_user_tag(user)
    
    msg = f"🎙 <b>HOST: {user_tag}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "Host, please select the number of overs for this match.\n"
    msg += "Range: <b>4 to 20 Overs</b>"
    
    # Use Safe Refresh Function
    await refresh_game_message(context, chat.id, match, msg, reply_markup, media_key="host")

# Captain selection callback
async def captain_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Captain Selection and move to Toss safely"""
    query = update.callback_query
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    # Check Phase
    if match.phase != GamePhase.CAPTAIN_SELECTION:
        await query.answer("Captain selection phase has ended.", show_alert=True)
        return
    
    # Logic for Team X
    if query.data == "captain_team_x":
        if not match.team_x.get_player(user.id):
            await query.answer("You must be in Team X!", show_alert=True)
            return
        if match.team_x.captain_id:
            await query.answer("Team X already has a captain.", show_alert=True)
            return
        match.team_x.captain_id = user.id
        await query.answer("You are Captain of Team X!")
    
    # Logic for Team Y
    elif query.data == "captain_team_y":
        if not match.team_y.get_player(user.id):
            await query.answer("You must be in Team Y!", show_alert=True)
            return
        if match.team_y.captain_id:
            await query.answer("Team Y already has a captain.", show_alert=True)
            return
        match.team_y.captain_id = user.id
        await query.answer("You are Captain of Team Y!")
    
    # Check if BOTH are selected
    if match.team_x.captain_id and match.team_y.captain_id:
        # ✅ FLOW FIX: Captains ke baad Toss aayega
        match.phase = GamePhase.TOSS
        await start_toss(query, context, match)
        
    else:
        # Update Message (Show who is selected)
        captain_x = match.team_x.get_player(match.team_x.captain_id)
        captain_y = match.team_y.get_player(match.team_y.captain_id)
        
        cap_x_name = captain_x.first_name if captain_x else "Not Selected"
        cap_y_name = captain_y.first_name if captain_y else "Not Selected"
        
        keyboard = [
            [InlineKeyboardButton("Become Captain - Team X", callback_data="captain_team_x")],
            [InlineKeyboardButton("Become Captain - Team Y", callback_data="captain_team_y")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        msg = "🧢 <b>CAPTAIN SELECTION</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🧊 <b>Team X:</b> {cap_x_name}\n"
        msg += f"🔥 <b>Team Y:</b> {cap_y_name}\n\n"
        msg += "<i>Waiting for both captains...</i>"
        
        # ✅ FIX: Use refresh_game_message instead of risky edits
        await refresh_game_message(context, chat.id, match, msg, reply_markup, media_key="squads")

async def start_team_edit_phase(query, context: ContextTypes.DEFAULT_TYPE, match: Match):
    """Start team edit phase with Safety Checks"""
    match.phase = GamePhase.TEAM_EDIT
    
    # Safe Host Fetch
    host = match.team_x.get_player(match.host_id) or match.team_y.get_player(match.host_id)
    host_name = host.first_name if host else "Unknown"
    
    # Safe Captain Fetch
    captain_x = match.team_x.get_player(match.team_x.captain_id)
    captain_y = match.team_y.get_player(match.team_y.captain_id)
    
    cap_x_name = captain_x.first_name if captain_x else "Not Selected"
    cap_y_name = captain_y.first_name if captain_y else "Not Selected"
    
    keyboard = [
        [InlineKeyboardButton("Edit Team X", callback_data="edit_team_x")],
        [InlineKeyboardButton("Edit Team Y", callback_data="edit_team_y")],
        [InlineKeyboardButton("✅ Done - Proceed", callback_data="team_edit_done")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    edit_text = "⚙️ <b>TEAM SETUP & EDITING</b>\n"
    edit_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    edit_text += f"🎙 <b>Host:</b> {host_name}\n"
    edit_text += f"🧊 <b>Team X Captain:</b> {cap_x_name}\n"
    edit_text += f"🔥 <b>Team Y Captain:</b> {cap_y_name}\n\n"
    
    edit_text += "🧊 <b>TEAM X SQUAD:</b>\n"
    for i, player in enumerate(match.team_x.players, 1):
        role = " (C)" if player.user_id == match.team_x.captain_id else ""
        edit_text += f"{i}. {player.first_name}{role}\n"
    
    edit_text += "\n🔥 <b>TEAM Y SQUAD:</b>\n"
    for i, player in enumerate(match.team_y.players, 1):
        role = " (C)" if player.user_id == match.team_y.captain_id else ""
        edit_text += f"{i}. {player.first_name}{role}\n"
    
    edit_text += "\n"
    edit_text += "<b>Host Controls:</b>\n"
    edit_text += "• Reply to a user with <code>/add</code> to add them.\n"
    edit_text += "• Reply to a user with <code>/remove</code> to remove them.\n"
    edit_text += "• Click 'Done' when ready."
    
    # Use Master Function (Corrected Call)
    chat_id = query.message.chat.id
    await refresh_game_message(context, chat_id, match, edit_text, reply_markup=reply_markup, media_key="squads")

# Add/Remove player commands (Host only)
async def add_player_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ BULK ADD: Add multiple players at once
    Usage:
    - Reply: /add
    - Username: /add @username
    - Multiple: /add @user1 @user2 @user3
    - ID: /add 123456789
    - Mixed: /add @user1 123456 @user2
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_EDIT:
        await update.message.reply_text("⚠️ Team editing inactive.")
        return
    
    # Check if Host
    if user.id != match.host_id:
        await update.message.reply_text("⚠️ Only Host can add.")
        return
    
    # Check if mode is set
    if not match.editing_team:
        await update.message.reply_text("⚠️ Please click 'Edit Team X' or 'Edit Team Y' button first!")
        return
    
    target_users = []  # List of user objects to add
    
    # Method 1: Reply to message
    if update.message.reply_to_message:
        target_users.append(update.message.reply_to_message.from_user)
    
    # Method 2: Parse mentions and IDs from command
    if update.message.entities or context.args:
        # Extract all mentions
        if update.message.entities:
            for entity in update.message.entities:
                if entity.type == "mention":
                    username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                    # Find in user_data
                    for uid, data in user_data.items():
                        if data.get("username", "").lower() == username.lower():
                            try:
                                target_user = await context.bot.get_chat(uid)
                                target_users.append(target_user)
                            except:
                                pass
                            break
                elif entity.type == "text_mention":
                    target_users.append(entity.user)
        
        # Extract all User IDs from arguments
        if context.args:
            for arg in context.args:
                if arg.isdigit():
                    try:
                        user_id = int(arg)
                        target_user = await context.bot.get_chat(user_id)
                        target_users.append(target_user)
                    except:
                        pass
    
    if not target_users:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "Reply: <code>/add</code>\n"
            "Single: <code>/add @username</code>\n"
            "Multiple: <code>/add @user1 @user2 @user3</code>\n"
            "ID: <code>/add 123456789</code>\n"
            "Mixed: <code>/add @user1 123456 @user2</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Determine which team to add to
    if match.editing_team == "X":
        team = match.team_x
        t_name = "Team X"
    else:
        team = match.team_y
        t_name = "Team Y"
    
    # Process each user
    added_users = []
    skipped_users = []
    failed_users = []
    
    for target_user in target_users:
        try:
            # Check duplicate
            if match.team_x.get_player(target_user.id) or match.team_y.get_player(target_user.id):
                skipped_users.append(target_user.first_name)
                continue
            
            # Initialize if new
            if target_user.id not in user_data:
                user_data[target_user.id] = {
                    "user_id": target_user.id,
                    "username": target_user.username or "",
                    "first_name": target_user.first_name,
                    "started_at": datetime.now().isoformat(),
                    "total_matches": 0
                }
                init_player_stats(target_user.id)
                save_data()
            
            # Add Player
            p = Player(target_user.id, target_user.username or "", target_user.first_name)
            team.add_player(p)
            added_users.append(get_user_tag(target_user))
            
        except Exception as e:
            logger.error(f"Failed to add {target_user.id}: {e}")
            failed_users.append(target_user.first_name if hasattr(target_user, 'first_name') else str(target_user.id))
    
    # Build response message
    msg = f"📊 <b>BULK ADD RESULT - {t_name}</b>\n"
    msg += "────────────────────\n\n"
    
    if added_users:
        msg += f"✅ <b>Added ({len(added_users)}):</b>\n"
        for user_tag in added_users:
            msg += f"  • {user_tag}\n"
        msg += "\n"
    
    if skipped_users:
        msg += f"⚠️ <b>Skipped ({len(skipped_users)}):</b>\n"
        for name in skipped_users:
            msg += f"  • {name} (Already in a team)\n"
        msg += "\n"
    
    if failed_users:
        msg += f"❌ <b>Failed ({len(failed_users)}):</b>\n"
        for name in failed_users:
            msg += f"  • {name} (Error fetching user)\n"
        msg += "\n"
    
    msg += "────────────────────\n"
    msg += f"📈 <b>Total {t_name} Players:</b> {len(team.players)}"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    
    # Update team edit board
    await update_team_edit_message(context, chat.id, match)

async def remove_player_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ BULK REMOVE: Remove multiple players at once
    Usage:
    - Reply: /remove
    - Username: /remove @username
    - Multiple: /remove @user1 @user2 @user3
    - ID: /remove 123456789
    - Serial: /remove 1 2 3 4
    - Mixed: /remove @user1 2 @user2 123456
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_EDIT:
        return
    
    if user.id != match.host_id:
        await update.message.reply_text("⚠️ Only Host can remove players.")
        return
    
    if not match.editing_team:
        await update.message.reply_text("⚠️ First Click on 'Edit Team X' or 'Edit Team Y' button")
        return
    
    target_user_ids = []  # List of user IDs to remove
    
    # Get current team
    if match.editing_team == "X":
        team = match.team_x
        team_name = "Team X"
    else:
        team = match.team_y
        team_name = "Team Y"
    
    # Method 1: Reply to message
    if update.message.reply_to_message:
        target_user_ids.append(update.message.reply_to_message.from_user.id)
    
    # Method 2: Parse mentions, IDs, and serials from command
    if update.message.entities or context.args:
        # Extract all mentions
        if update.message.entities:
            for entity in update.message.entities:
                if entity.type == "mention":
                    username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                    # Find in user_data
                    for uid, data in user_data.items():
                        if data.get("username", "").lower() == username.lower():
                            target_user_ids.append(uid)
                            break
                elif entity.type == "text_mention":
                    target_user_ids.append(entity.user.id)
        
        # Extract all IDs and Serials from arguments
        if context.args:
            for arg in context.args:
                if arg.isdigit():
                    num = int(arg)
                    
                    # Check if it's a serial number (1-20 range typically)
                    if 1 <= num <= len(team.players):
                        target_player = team.get_player_by_serial(num)
                        if target_player:
                            target_user_ids.append(target_player.user_id)
                    else:
                        # Treat as User ID
                        target_user_ids.append(num)
    
    if not target_user_ids:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "Reply: <code>/remove</code>\n"
            "Single: <code>/remove @username</code>\n"
            "Multiple: <code>/remove @user1 @user2 @user3</code>\n"
            "ID: <code>/remove 123456789</code>\n"
            "Serial: <code>/remove 1 2 3</code>\n"
            "Mixed: <code>/remove @user1 2 123456</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Remove duplicates from target list
    target_user_ids = list(set(target_user_ids))
    
    # Process each user
    removed_users = []
    not_found_users = []
    
    for target_id in target_user_ids:
        # Get player name before removing
        player = team.get_player(target_id)
        if player:
            player_name = player.first_name
        else:
            # Try to get name from user_data
            player_name = user_data.get(target_id, {}).get("first_name", f"User {target_id}")
        
        # Try to remove
        if team.remove_player(target_id):
            removed_users.append(player_name)
        else:
            not_found_users.append(player_name)
    
    # Build response message
    msg = f"📊 <b>BULK REMOVE RESULT - {team_name}</b>\n"
    msg += "────────────────────\n\n"
    
    if removed_users:
        msg += f"✅ <b>Removed ({len(removed_users)}):</b>\n"
        for name in removed_users:
            msg += f"  • {name}\n"
        msg += "\n"
    
    if not_found_users:
        msg += f"⚠️ <b>Not Found ({len(not_found_users)}):</b>\n"
        for name in not_found_users:
            msg += f"  • {name} (Not in {team_name})\n"
        msg += "\n"
    
    msg += "────────────────────\n"
    msg += f"📈 <b>Remaining {team_name} Players:</b> {len(team.players)}"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    
    # Update team edit board
    await update_team_edit_message(context, chat.id, match)



# Team edit done callback
async def team_edit_done_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Finish Team Edit and start Captain Selection"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TEAM_EDIT:
        await query.answer("Team edit phase has ended.", show_alert=True)
        return
    
    if user.id != match.host_id:
        await query.answer("Only the Host can proceed.", show_alert=True)
        return
    
    # Validate teams
    if len(match.team_x.players) == 0 or len(match.team_y.players) == 0:
        await query.answer("Both teams need at least one player.", show_alert=True)
        return
    
    # ✅ FLOW FIX: Team Edit ke baad ab Captain Selection aayega
    match.phase = GamePhase.CAPTAIN_SELECTION
    
    # Prepare Captain Selection Message
    captain_x = match.team_x.get_player(match.team_x.captain_id)
    captain_y = match.team_y.get_player(match.team_y.captain_id)
    
    cap_x_name = captain_x.first_name if captain_x else "Not Selected"
    cap_y_name = captain_y.first_name if captain_y else "Not Selected"
    
    keyboard = [
        [InlineKeyboardButton("Become Captain - Team X", callback_data="captain_team_x")],
        [InlineKeyboardButton("Become Captain - Team Y", callback_data="captain_team_y")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    msg = "🧢 <b>CAPTAIN SELECTION</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🧊 <b>Team X:</b> {cap_x_name}\n"
    msg += f"🔥 <b>Team Y:</b> {cap_y_name}\n\n"
    msg += "<i>Click below to lead your team!</i>"
    
    # Update Board (Using Refresh function to be safe)
    await refresh_game_message(context, chat.id, match, msg, reply_markup, media_key="squads")

# Over selection callback
async def over_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle over selection and move to Team Edit"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match found.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.OVER_SELECTION:
        await query.answer("Over selection phase has ended.", show_alert=True)
        return
    
    if user.id != match.host_id:
        await query.answer("Only the Host can select overs.", show_alert=True)
        return
    
    # --- LOGIC ---
    try:
        data_parts = query.data.split("_")
        if len(data_parts) != 2: return
        overs_selected = int(data_parts[1])
        
        if 4 <= overs_selected <= 20:
            match.total_overs = overs_selected
            
            # ✅ FLOW FIX: Overs ke baad ab Team Edit Mode aayega
            match.phase = GamePhase.TEAM_EDIT
            await start_team_edit_phase(query, context, match)
            
        else:
            await query.answer("Overs must be between 4 and 20.", show_alert=True)
    except ValueError:
        await query.answer("Invalid format.", show_alert=True)

async def start_toss(query, context: ContextTypes.DEFAULT_TYPE, match: Match):
    """Start the toss phase safely"""
    # Try to fetch Team X Captain safely
    captain_x = match.team_x.get_player(match.team_x.captain_id)
    cap_x_name = captain_x.first_name if captain_x else "Team X Captain"
    
    keyboard = [
        [InlineKeyboardButton("Heads", callback_data="toss_heads")],
        [InlineKeyboardButton("Tails", callback_data="toss_tails")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    toss_text = "🪙 <b>TIME FOR THE TOSS</b>\n"
    toss_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    toss_text += f"📏 <b>Format:</b> {match.total_overs} Overs per side\n\n"
    toss_text += f"👤 <b>{cap_x_name}</b>, it's your call!\n"
    toss_text += "<i>Choose Heads or Tails below:</i>"
    
    # ✅ FIX: Always use refresh_game_message to switch images safely
    chat_id = match.group_id
    await refresh_game_message(context, chat_id, match, toss_text, reply_markup, media_key="toss")

# Toss callback
async def toss_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toss selection safely"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match found.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TOSS:
        await query.answer("Toss phase has ended.", show_alert=True)
        return
    
    # Only Team X captain can call toss
    if user.id != match.team_x.captain_id:
        await query.answer("Only Team X Captain can call the toss.", show_alert=True)
        return
    
    # Determine toss result
    toss_result = random.choice(["heads", "tails"])
    captain_call = "heads" if query.data == "toss_heads" else "tails"
    
    if toss_result == captain_call:
        match.toss_winner = match.team_x
        winner_captain = match.team_x.get_player(match.team_x.captain_id)
    else:
        match.toss_winner = match.team_y
        winner_captain = match.team_y.get_player(match.team_y.captain_id)
    
    # Ask winner to choose bat or bowl
    keyboard = [
        [InlineKeyboardButton("🏏 Bat First", callback_data="toss_decision_bat")],
        [InlineKeyboardButton("⚾ Bowl First", callback_data="toss_decision_bowl")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    decision_text = "🪙 <b>TOSS RESULT</b>\n"
    decision_text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    decision_text += f"The coin landed on: <b>{toss_result.upper()}</b>\n\n"
    decision_text += f"🎉 <b>{match.toss_winner.name} won the toss!</b>\n"
    decision_text += f"👤 <b>Captain {winner_captain.first_name}</b>, make your choice.\n"
    decision_text += "<i>You have 30 seconds to decide.</i>"
    
    # ✅ FIX: Use refresh_game_message instead of edit_message_text
    await refresh_game_message(context, chat.id, match, decision_text, reply_markup, media_key="toss")
    
    # Set timeout for decision
    asyncio.create_task(toss_decision_timeout(context, chat.id, match))

async def toss_decision_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle toss decision timeout"""
    await asyncio.sleep(30)
    
    if match.phase != GamePhase.TOSS:
        return
    
    # Auto select bat if no decision made
    match.batting_first = match.toss_winner
    match.bowling_first = match.get_other_team(match.toss_winner)
    
    await start_match(context, group_id, match, auto_decision=True)

# Toss decision callback
async def toss_decision_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle bat/bowl decision"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        await query.answer("No active match found.", show_alert=True)
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.TOSS:
        await query.answer("Toss phase has ended.", show_alert=True)
        return
    
    # Only toss winner captain can decide
    winner_captain = match.get_captain(match.toss_winner)
    if user.id != winner_captain.user_id:
        await query.answer("Only the toss winner captain can decide.", show_alert=True)
        return
    
    if query.data == "toss_decision_bat":
        match.batting_first = match.toss_winner
        match.bowling_first = match.get_other_team(match.toss_winner)
    else:
        match.bowling_first = match.toss_winner
        match.batting_first = match.get_other_team(match.toss_winner)
    
    await start_match(context, chat.id, match, auto_decision=False)

async def start_match(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, auto_decision: bool):
    """Start the actual match with prediction poll - FIXED WORKFLOW"""
    match.phase = GamePhase.MATCH_IN_PROGRESS
    match.current_batting_team = match.batting_first
    match.current_bowling_team = match.bowling_first
    match.innings = 1
    
    # ✅ CRITICAL: Reset Batsmen & Bowler indices to None (Fresh Start)
    match.current_batting_team.current_batsman_idx = None
    match.current_batting_team.current_non_striker_idx = None
    match.current_bowling_team.current_bowler_idx = None
    
    # ✅ Enable Waiting Flags
    match.waiting_for_batsman = True
    match.waiting_for_bowler = False 

    # Cleanup the Toss Board
    if match.main_message_id:
        try:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
        except: pass

    # Send toss summary
    decision_method = "chose to" if not auto_decision else "will"
    toss_summary = "🏟 <b>MATCH STARTED</b>\n"
    toss_summary += "━━━━━━━━━━━━━━━━━━━━━\n"
    toss_summary += f"🪙 <b>{match.toss_winner.name}</b> won the toss\n"
    toss_summary += f"🏏 <b>{match.batting_first.name}</b> {decision_method} bat first\n"
    toss_summary += f"📏 <b>Format:</b> {match.total_overs} Overs per side\n"
    toss_summary += "━━━━━━━━━━━━━━━━━━━━━\n"
    toss_summary += "<i>Openers are walking to the crease...</i>"
    
    await context.bot.send_message(chat_id=group_id, text=toss_summary, parse_mode=ParseMode.HTML)
    
    # ✅ CREATE PREDICTION POLL
    await create_prediction_poll(context, group_id, match)
    
    # Wait 3 seconds for effect
    await asyncio.sleep(3)
    
    # ✅ REQUEST STRIKER ONLY (Step 1)
    captain = match.get_captain(match.current_batting_team)
    if not captain:
        await context.bot.send_message(group_id, "⚠️ No captain found!", parse_mode=ParseMode.HTML)
        return
    
    captain_tag = get_user_tag(captain)
    
    msg = f"🏏 <b>SELECT STRIKER</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"👑 <b>{captain_tag}</b>, please select the <b>STRIKER</b> first:\n\n"
    msg += f"👉 <b>Command:</b> <code>/batting [serial_number]</code>\n"
    msg += f"📋 <b>Available Players:</b> {len(match.current_batting_team.players)}\n"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    
    # Start selection timer
    match.batsman_selection_time = time.time()
    match.batsman_selection_task = asyncio.create_task(
        batsman_selection_timeout(context, group_id, match)
    )

async def request_batsman_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """Prompt captain for new batsman after wicket"""
    captain = match.get_captain(match.current_batting_team)
    if not captain:
        await context.bot.send_message(chat_id, "⚠️ No captain selected! Use /captain to set one.")
        return

    captain_tag = get_user_tag(captain)  # Assuming get_user_tag is defined
    msg = f"🏏 <b>NEW BATSMAN NEEDED!</b>\n"
    msg += f"👑 <b>{captain_tag}</b>, select batsman:\n"
    msg += "<code>/batting &lt;serial&gt;  (e.g., /batting 3)</code>\n"
    msg += f"Available: {len(match.current_batting_team.players) - len(match.current_batting_team.out_players_indices) - 2} players left."
    
    await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
    
    # Start 30s timer for selection
    match.batsman_selection_time = time.time() + 30
    match.batsman_selection_task = asyncio.create_task(
        selection_timer(context, chat_id, match, "batsman", captain.first_name)
    )

async def selection_timer(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, selection_type: str, name: str):
    """Timeout for batsman/bowler selection (30s)"""
    await asyncio.sleep(30)
    if (selection_type == "batsman" and match.waiting_for_batsman and 
        time.time() - match.batsman_selection_time < 0) or \
       (selection_type == "bowler" and match.waiting_for_bowler and 
        time.time() - match.bowler_selection_time < 0):
        msg = f"⏰ <b>{name}</b> timed out on {selection_type} selection! Random selected."
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        await auto_select_player(context, chat_id, match, selection_type)

async def auto_select_player(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, selection_type: str):
    """Randomly select player on timeout"""
    if selection_type == "batsman":
        team = match.current_batting_team
        available = [p for idx, p in enumerate(team.players) if idx not in team.out_players_indices and 
                     idx != team.current_batsman_idx and idx != team.current_non_striker_idx]
        if available:
            new_idx = random.choice(available).user_id  # Wait, fix: random index
            # Actually: random_idx = random.choice([i for i in range(len(team.players)) if conditions])
            random_idx = random.choice([i for i in range(len(team.players)) if i not in team.out_players_indices and 
                                        i != team.current_batsman_idx and i != team.current_non_striker_idx])
            team.current_batsman_idx = random_idx  # New striker
            match.waiting_for_batsman = False
            await resume_after_selection(context, chat_id, match)
    elif selection_type == "bowler":
        team = match.current_bowling_team
        available = team.get_available_bowlers()
        if available:
            new_bowler = random.choice(available)
            team.current_bowler_idx = team.players.index(new_bowler)
            team.bowler_history.append(team.current_bowler_idx)
            match.waiting_for_bowler = False
            await request_bowler_number(context, chat_id, match)  # Resume next ball

async def resume_after_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """Resume game after batsman selection (e.g., next ball)"""
    # Cancel timer
    if match.batsman_selection_task:
        match.batsman_selection_task.cancel()
    match.waiting_for_batsman = False
    
    # If mid-over, prompt bowler for next ball
    if match.current_bowling_team.current_bowler_idx is not None:
        await request_bowler_number(context, chat_id, match)
    else:
        # Edge: Start over or something—log
        logger.warning("Resume called without bowler")

async def batting_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ COMPLETE BATTING COMMAND - FIXED: Proper flow after selection
    """
    chat = update.effective_chat
    user = update.effective_user
    
    logger.info(f"🏏 /batting command from {user.first_name} (ID: {user.id})")
    
    if chat.id not in active_matches:
        logger.warning(f"⚠️ No active match in chat {chat.id}")
        return
    
    match = active_matches[chat.id]
    
    if match.phase != GamePhase.MATCH_IN_PROGRESS:
        logger.warning(f"⚠️ Match not in progress, phase={match.phase}")
        await update.message.reply_text("⚠️ Match is not in progress!")
        return

    bat_team = match.current_batting_team
    
    # Only captain can select
    if user.id != bat_team.captain_id:
        logger.warning(f"👮‍♂️ User {user.first_name} is not captain")
        await update.message.reply_text("👮‍♂️ Only the Batting Captain can select!")
        return

    if not context.args:
        await update.message.reply_text("ℹ️ <b>Usage:</b> <code>/batting [serial_number]</code>", parse_mode=ParseMode.HTML)
        return
    
    try:
        serial = int(context.args[0])
        logger.info(f"🔢 Serial number: {serial}")
    except:
        await update.message.reply_text("❌ Invalid number.")
        return

    player = bat_team.get_player_by_serial(serial)
    if not player:
        logger.warning(f"🚫 Player #{serial} not found")
        await update.message.reply_text(f"🚫 Player #{serial} not found.")
        return
    
    logger.info(f"👤 Player selected: {player.first_name} (ID: {player.user_id})")
    
    if player.is_out:
        logger.warning(f"💀 Player {player.first_name} is already OUT")
        await update.message.reply_text(f"💀 {player.first_name} is already OUT!")
        return
    
    player_idx = serial - 1
    
    # Check duplicates
    if player_idx == bat_team.current_batsman_idx or player_idx == bat_team.current_non_striker_idx:
        logger.warning(f"🛑 Player {player.first_name} already on crease")
        await update.message.reply_text(f"🛑 {player.first_name} is already on the crease!")
        return

    # ========================================
    # 🎯 CASE 1: SELECTING STRIKER (Opening)
    # ========================================
    if bat_team.current_batsman_idx is None and bat_team.current_non_striker_idx is None:
        logger.info("🎯 CASE 1: Selecting STRIKER (Opening)")
        bat_team.current_batsman_idx = player_idx
        
        await update.message.reply_text(
            f"✅ <b>Striker Selected:</b> {player.first_name}", 
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ Striker set: {player.first_name} (Index: {player_idx})")
        await asyncio.sleep(1)

        # Request Non-Striker
        captain_tag = get_user_tag(match.get_captain(bat_team))
        msg = f"🏏 <b>SELECT NON-STRIKER</b>\n"
        msg += f"🧢 <b>{captain_tag}</b>, now select the <b>NON-STRIKER</b>:\n"
        msg += f"👉 <b>Command:</b> <code>/batting [serial_number]</code>"
        
        await context.bot.send_message(chat.id, msg, parse_mode=ParseMode.HTML)
        logger.info("✅ Non-striker request sent")
        return

    # ========================================
    # 🎯 CASE 2: SELECTING NON-STRIKER (Opening)
    # ========================================
    elif bat_team.current_non_striker_idx is None:
        logger.info("🎯 CASE 2: Selecting NON-STRIKER (Opening)")
        bat_team.current_non_striker_idx = player_idx
        
        await update.message.reply_text(f"🃏 <b>Non-Striker Selected:</b> {player.first_name}", parse_mode=ParseMode.HTML)
        logger.info(f"✅ Non-striker set: {player.first_name} (Index: {player_idx})")
        await asyncio.sleep(1)
        
        # ✅ Opening Pair Complete
        match.waiting_for_batsman = False
        if match.batsman_selection_task:
            match.batsman_selection_task.cancel()
            logger.info("✅ Batsman selection timer cancelled")
        
        striker = bat_team.players[bat_team.current_batsman_idx]
        non_striker = bat_team.players[bat_team.current_non_striker_idx]
        
        confirm_msg = f"✅ <b>OPENING PAIR SET!</b>\n"
        confirm_msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        confirm_msg += f"🏏 <b>Striker:</b> {striker.first_name}\n"
        confirm_msg += f"🃏 <b>Non-Striker:</b> {non_striker.first_name}\n\n"
        confirm_msg += f"⚾ <i>Requesting bowler selection...</i>"
        
        # Send with photo
        try:
            await context.bot.send_photo(
                chat.id,
                photo=OPENING_PAIR_PHOTO,
                caption=confirm_msg,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Error sending opening pair photo: {e}")
            # Fallback to text
            await context.bot.send_message(chat.id, confirm_msg, parse_mode=ParseMode.HTML)
        
        logger.info("✅ Opening pair confirmed")
        await asyncio.sleep(2)
        
        # ✅ Request Bowler (Step 3) - THIS IS THE CRITICAL FIX
        match.waiting_for_bowler = True
        match.waiting_for_batsman = False  # Ensure this is false
        logger.info("📣 Calling request_bowler_selection...")
        await request_bowler_selection(context, chat.id, match)
        logger.info("✅ Bowler selection initiated")
        return

    # ========================================
    # 🎯 CASE 3: NEW BATSMAN (After Wicket)
    # ========================================
    else:
        logger.info("🎯 CASE 3: Selecting NEW BATSMAN after wicket")
        
        if not match.waiting_for_batsman:
            logger.warning("⚠️ Not waiting for batsman")
            await update.message.reply_text("⚠️ Batsmen are already set. Use /impact for substitution.")
            return

        # ✅ SET NEW STRIKER (NON-STRIKER STAYS SAME)
        bat_team.current_batsman_idx = player_idx
        match.waiting_for_batsman = False
        
        if match.batsman_selection_task:
            match.batsman_selection_task.cancel()
            logger.info("✅ Batsman selection timer cancelled")
    
        player_tag = get_user_tag(player)
        await update.message.reply_text(
            f"🚶‍♂️ <b>NEW BATSMAN:</b> {player_tag} walks in!", 
            parse_mode=ParseMode.HTML
        )
        logger.info(f"✅ New batsman set: {player.first_name} (Index: {player_idx})")
    
        await asyncio.sleep(2)
        
        # ✅ CRITICAL: Check pending over complete status FIRST
        if hasattr(match, 'pending_over_complete') and match.pending_over_complete:
            logger.info("🏁 Pending over complete detected, calling check_over_complete")
            match.pending_over_complete = False  # Reset flag
            await check_over_complete(context, chat.id, match)
            return
        
        # ✅ If no pending over complete, resume normal play
        bowl_team = match.current_bowling_team
        
        # ✅ If bowler exists, resume
        if bowl_team.current_bowler_idx is not None:
            logger.info("▶️ Bowler exists, resuming ball execution")
            await context.bot.send_message(chat.id, "▶️ <b>Game Resumed!</b>", parse_mode=ParseMode.HTML)
            await asyncio.sleep(1)
            await execute_ball(context, chat.id, match)
            
        # ✅ Edge case: No bowler (shouldn't happen in normal flow)
        else:
            logger.warning("⚠️ No bowler found, requesting bowler selection")
            match.waiting_for_bowler = True
            await request_bowler_selection(context, chat.id, match)        
        return

async def players_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Show playing squads with player tags and live status"""
    group_id = update.effective_chat.id
    match = active_matches.get(group_id)
    
    if not match:
        await update.message.reply_text("🏏 No active match! Start with /start")
        return
    
    if match.phase == GamePhase.SOLO_MATCH:
        await update.message.reply_text("🏏 Squad info not available in solo mode")
        return
    
    text = "╔══════════════════╗\n"
    text += "      🏏 <b>PLAYING PLAYER</b>\n"
    text += "╚══════════════════╝\n\n"
    
    # Determine toss winner
    toss_winner_team = None
    if match.toss_winner:
        toss_winner_team = match.toss_winner
    
    # Team X
    text += "━━━━━━━━━━━━━━━━━━━\n"
    team_x_header = f"🔵 <b>{match.team_x.name}</b>"
    if toss_winner_team == match.team_x:
        team_x_header += " 🪙"
    text += team_x_header + "\n"
    text += "━━━━━━━━━━━━━━━━━━━\n"
    
    for i, player in enumerate(match.team_x.players, 1):
        # Create mention tag
        mention = f'<a href="tg://user?id={player.user_id}">{player.first_name}</a>'
        
        # Captain mark
        if player.user_id == match.team_x.captain_id:
            mention += " 👑"
        
        # Check player status
        stats = match.team_x.player_stats.get(player.user_id, {})
        dismissal = stats.get('dismissal', 'not out')
        
        # Add status emoji
        if dismissal != 'not out':
            mention += " ❌"  # Out emoji
        elif match.current_batting_team == match.team_x:
            # Check if currently batting
            if match.current_batting_team.current_batsman_idx is not None:
                current_batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
                if current_batsman.user_id == player.user_id:
                    mention += " 🏏"  # Striker
            if match.current_batting_team.current_non_striker_idx is not None:
                non_striker = match.current_batting_team.players[match.current_batting_team.current_non_striker_idx]
                if non_striker.user_id == player.user_id:
                    mention += " 🔄"  # Non-striker
        elif match.current_bowling_team == match.team_x:
            # Check if currently bowling
            if match.current_bowling_team.current_bowler_idx is not None:
                current_bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
                if current_bowler.user_id == player.user_id:
                    mention += " ⚡"  # Bowling
        
        text += f"  {i}. {mention}\n"
    
    # Team Y
    text += "\n━━━━━━━━━━━━━━━━━━━\n"
    team_y_header = f"🔴 <b>{match.team_y.name}</b>"
    if toss_winner_team == match.team_y:
        team_y_header += " 🪙"
    text += team_y_header + "\n"
    text += "━━━━━━━━━━━━━━━━━━━\n"
    
    for i, player in enumerate(match.team_y.players, 1):
        # Create mention tag
        mention = f'<a href="tg://user?id={player.user_id}">{player.first_name}</a>'
        
        # Captain mark
        if player.user_id == match.team_y.captain_id:
            mention += " 👑"
        
        # Check player status
        stats = match.team_y.player_stats.get(player.user_id, {})
        dismissal = stats.get('dismissal', 'not out')
        
        # Add status emoji
        if dismissal != 'not out':
            mention += " ❌"  # Out emoji
        elif match.current_batting_team == match.team_y:
            # Check if currently batting
            if match.current_batting_team.current_batsman_idx is not None:
                current_batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
                if current_batsman.user_id == player.user_id:
                    mention += " 🏏"  # Striker
            if match.current_batting_team.current_non_striker_idx is not None:
                non_striker = match.current_batting_team.players[match.current_batting_team.current_non_striker_idx]
                if non_striker.user_id == player.user_id:
                    mention += " 🔄"  # Non-striker
        elif match.current_bowling_team == match.team_y:
            # Check if currently bowling
            if match.current_bowling_team.current_bowler_idx is not None:
                current_bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
                if current_bowler.user_id == player.user_id:
                    mention += " ⚡"  # Bowling
        
        text += f"  {i}. {mention}\n"
    
    
    try:
        # Send with photo
        try:
            await update.message.reply_photo(
                photo=PLAYERS_PHOTO,
                caption=text,
                parse_mode=ParseMode.HTML
            )
        except Exception as photo_error:
            logger.error(f"Error sending players photo: {photo_error}")
            # Fallback to text
            await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except Exception as e:
        # Fallback without tags if it fails
        plain_text = "🏏 PLAYING XI\n\n"
        plain_text += f"🔵 {match.team_x.name}"
        if toss_winner_team == match.team_x:
            plain_text += " 🪙"
        plain_text += "\n"
        
        for i, player in enumerate(match.team_x.players, 1):
            name = player.first_name
            if player.user_id == match.team_x.captain_id:
                name += " 👑"
            stats = match.team_x.player_stats.get(player.user_id, {})
            if stats.get('dismissal', 'not out') != 'not out':
                name += " ❌"
            plain_text += f"{i}. {name}\n"
        
        plain_text += f"\n🔴 {match.team_y.name}"
        if toss_winner_team == match.team_y:
            plain_text += " 🪙"
        plain_text += "\n"
        
        for i, player in enumerate(match.team_y.players, 1):
            name = player.first_name
            if player.user_id == match.team_y.captain_id:
                name += " 👑"
            stats = match.team_y.player_stats.get(player.user_id, {})
            if stats.get('dismissal', 'not out') != 'not out':
                name += " ❌"
            plain_text += f"{i}. {name}\n"
        
        plain_text += "\n👑 Captain | 🪙 Toss Winner\n"
        plain_text += "🏏 Batting | 🔄 Non-striker | ⚡ Bowling | ❌ Out"
        await update.message.reply_text(plain_text)

async def batsman_selection_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle batsman selection timeout"""
    try:
        await asyncio.sleep(120)  # 2 minutes
        
        if not match.waiting_for_batsman:
            return
        
        # Timeout occurred - penalty
        match.current_batting_team.score -= 6
        match.current_batting_team.penalty_runs += 6
        
        penalty_msg = "Batsman Selection Timeout\n\n"
        penalty_msg += f"{match.current_batting_team.name} penalized 6 runs for delay.\n"
        penalty_msg += f"Current Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}\n\n"
        penalty_msg += "Please select a batsman immediately."
        
        await context.bot.send_message(
            chat_id=group_id,
            text=penalty_msg
        )
        
        # Reset timer
        match.batsman_selection_time = time.time()
        match.batsman_selection_task = asyncio.create_task(
            batsman_selection_timeout(context, group_id, match)
        )
    
    except asyncio.CancelledError:
        pass

async def request_bowler_selection(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match):
    """Prompt captain for bowler - GUARANTEED DELIVERY WITH FULL LOGGING"""
    
    logger.info(f"🎬 === BOWLER SELECTION START === Chat: {chat_id}")
    
    # Validate bowling team exists
    if not match.current_bowling_team:
        logger.error("❌ CRITICAL: No bowling team set!")
        await context.bot.send_message(chat_id, "⚠️ Error: No bowling team found!", parse_mode=ParseMode.HTML)
        return
    
    logger.info(f"✅ Bowling team: {match.current_bowling_team.name}")
    
    # Get captain
    captain = match.get_captain(match.current_bowling_team)
    if not captain:
        logger.error("❌ CRITICAL: No bowling captain found!")
        await context.bot.send_message(
            chat_id, 
            "⚠️ <b>Error:</b> No bowling captain found! Use /changecap_Y to set one.", 
            parse_mode=ParseMode.HTML
        )
        return

    captain_tag = get_user_tag(captain)
    logger.info(f"👑 Captain: {captain.first_name} (ID: {captain.user_id})")
    
    # Get available bowlers
    available = match.current_bowling_team.get_available_bowlers()
    logger.info(f"📊 Available bowlers: {len(available)}")
    
    if len(available) == 0:
        logger.error("❌ CRITICAL: No available bowlers!")
        await context.bot.send_message(
            chat_id,
            "⚠️ <b>Error:</b> No bowlers available! All players may be banned from bowling.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Log available bowlers
    for i, p in enumerate(available):
        logger.info(f"  Bowler #{i+1}: {p.first_name} (ID: {p.user_id})")
    
    # Build message
    msg = f"⚾ <b>SELECT BOWLER</b>\n"
    msg += f"─────────────────────\n"
    msg += f"🎩 <b>{captain_tag}</b>, choose your bowler:\n\n"
    msg += f"💡 <b>Command:</b> <code>/bowling [serial]</code>\n"
    msg += f"📋 <b>Available:</b> {len(available)} players\n\n"
    msg += f"<i>Example: /bowling 1</i>"
    
    # Send message to group
    try:
        sent_msg = await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        logger.info(f"✅ Group message sent successfully (msg_id: {sent_msg.message_id})")
    except Exception as e:
        logger.error(f"❌ Failed to send group message: {e}")
        return
    
    # SET WAITING STATE BEFORE ANYTHING ELSE
    match.waiting_for_bowler = True
    match.waiting_for_batsman = False
    match.current_ball_data = {}
    logger.info(f"✅ State set: waiting_for_bowler=True, waiting_for_batsman=False")
    
    # Start timeout timer
    match.bowler_selection_time = time.time()
    match.bowler_selection_task = asyncio.create_task(
        bowler_selection_timeout(context, chat_id, match)
    )
    logger.info(f"✅ Timeout timer started (120 seconds)")
    
    logger.info(f"🎬 === BOWLER SELECTION END ===")

async def bowler_selection_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle bowler selection timeout"""
    try:
        await asyncio.sleep(60)  # 1 minute
        
        if not match.waiting_for_bowler:
            return
        
        # Get current bowler if any
        if match.current_bowling_team.current_bowler_idx is not None:
            bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
            bowler.bowling_timeouts += 1
            
            timeout_count = bowler.bowling_timeouts
            
            if timeout_count >= 3:
                # Ban from bowling
                bowler.is_bowling_banned = True
                
                penalty_msg = "Bowler Selection Timeout\n\n"
                penalty_msg += f"{bowler.first_name} has timed out 3 times.\n"
                penalty_msg += f"{bowler.first_name} is now BANNED from bowling for the rest of the match.\n\n"
                penalty_msg += "No Ball called. Free Hit on next ball.\n\n"
                penalty_msg += "Please select another bowler immediately."
                
                # Add no ball
                match.current_batting_team.score += 1
                match.current_batting_team.extras += 1
                match.is_free_hit = True
                
                await context.bot.send_message(
                    chat_id=group_id,
                    text=penalty_msg
                )
            else:
                penalty_msg = "Bowler Selection Timeout\n\n"
                penalty_msg += f"{bowler.first_name} timed out ({timeout_count}/3).\n"
                penalty_msg += "No Ball called. Free Hit on next ball.\n\n"
                penalty_msg += "Please select a bowler immediately."
                
                # Add no ball
                match.current_batting_team.score += 1
                match.current_batting_team.extras += 1
                match.is_free_hit = True
                
                await context.bot.send_message(
                    chat_id=group_id,
                    text=penalty_msg
                )
        else:
            # First ball, no specific bowler to penalize
            penalty_msg = "Bowler Selection Timeout\n\n"
            penalty_msg += f"{match.current_bowling_team.name} delayed bowler selection.\n"
            penalty_msg += "6 runs penalty after this over.\n\n"
            penalty_msg += "Please select a bowler immediately."
            
            await context.bot.send_message(
                chat_id=group_id,
                text=penalty_msg
            )
        
        # Reset timer
        match.bowler_selection_time = time.time()
        match.bowler_selection_task = asyncio.create_task(
            bowler_selection_timeout(context, group_id, match)
        )
    
    except asyncio.CancelledError:
        pass

async def bowling_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Bowling Selection - FIXED VERSION"""
    chat = update.effective_chat
    user = update.effective_user
    
    logger.info(f"⚾ === BOWLING COMMAND START === From: {user.first_name} (ID: {user.id})")
    
    if chat.id not in active_matches: 
        logger.warning(f"❌ No active match in chat {chat.id}")
        await update.message.reply_text("❌ No active match")
        return
    
    match = active_matches[chat.id]
    logger.info(f"✅ Match found - Phase: {match.phase}")
    
    # Check if we are waiting for bowler
    if not match.waiting_for_bowler:
        logger.warning(f"⚠️ Not waiting for bowler (waiting_for_bowler=False)")
        await update.message.reply_text("⚠️ Not waiting for bowler selection right now.")
        return
    
    logger.info(f"✅ Waiting for bowler confirmed")
    
    bowling_captain = match.get_captain(match.current_bowling_team)
    if not bowling_captain:
        logger.error(f"❌ No bowling captain found!")
        await update.message.reply_text("⚠️ No bowling captain found!")
        return
    
    logger.info(f"👑 Bowling captain: {bowling_captain.first_name} (ID: {bowling_captain.user_id})")
        
    if user.id != bowling_captain.user_id:
        logger.warning(f"🚫 User {user.first_name} is not the bowling captain")
        await update.message.reply_text("⚠️ Only the Bowling Captain can select.")
        return
    
    logger.info(f"✅ Captain verification passed")
    
    if not context.args:
        logger.warning(f"⚠️ No serial number provided")
        await update.message.reply_text("⚠️ Usage: <code>/bowling [serial]</code>", parse_mode=ParseMode.HTML)
        return
    
    try:
        serial = int(context.args[0])
        logger.info(f"🔢 Serial number: {serial}")
    except: 
        logger.error(f"❌ Invalid serial number: {context.args[0]}")
        await update.message.reply_text("❌ Invalid number.")
        return
    
    bowler = match.current_bowling_team.get_player_by_serial(serial)
    if not bowler:
        logger.error(f"❌ Player #{serial} not found in bowling team")
        await update.message.reply_text(f"❌ Player #{serial} not found.")
        return
    
    logger.info(f"👤 Bowler selected: {bowler.first_name} (ID: {bowler.user_id})")
        
    if bowler.is_bowling_banned:
        logger.warning(f"🚫 {bowler.first_name} is banned from bowling")
        await update.message.reply_text("🚫 Player is BANNED from bowling.")
        return
    
    logger.info(f"✅ Bowler validation passed")
    
    # ✅ NO CONSECUTIVE OVERS CHECK
    bowler_idx = serial - 1
    if match.current_bowling_team.bowler_history:
        if bowler_idx == match.current_bowling_team.bowler_history[-1]:
            logger.warning(f"🚫 Consecutive over attempt by {bowler.first_name}")
            await update.message.reply_text("🚫 <b>Rule:</b> Bowler cannot bowl 2 consecutive overs!", parse_mode=ParseMode.HTML)
            return
    
    logger.info(f"✅ Consecutive over check passed")

    # ✅ SET BOWLER
    match.current_bowling_team.current_bowler_idx = bowler_idx
    match.waiting_for_bowler = False
    match.waiting_for_batsman = False 
    
    logger.info(f"✅ Bowler set: Index={bowler_idx}, waiting_for_bowler=False")
    
    if match.bowler_selection_task: 
        match.bowler_selection_task.cancel()
        logger.info(f"✅ Bowler selection timer cancelled")
    
    # Confirmation
    try: 
        bowler_tag = get_user_tag(bowler)
    except: 
        bowler_tag = bowler.first_name
    
    confirm_msg = f"✅ <b>BOWLER SELECTED</b>\n"
    confirm_msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    confirm_msg += f"⚾ <b>{bowler_tag}</b> will bowl!\n\n"
    confirm_msg += f"▶️ <i>Game Resumed! Starting the over...</i>"
    
    await update.message.reply_text(confirm_msg, parse_mode=ParseMode.HTML)
    logger.info(f"✅ Confirmation message sent")
    
    await asyncio.sleep(2)
    
    # ✅ RESUME GAME (Trigger first ball of over) - THIS IS THE CRITICAL FIX
    logger.info(f"🎮 Calling execute_ball to start bowling...")
    await execute_ball(context, chat.id, match)
    logger.info(f"⚾ === BOWLING COMMAND END ===")

# ✅ FIX 3: Enhanced execute_ball with logging
async def execute_ball(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    Premium TV Broadcast Style - WITH CORRECT CHASE INFO
    """
    
    logger.info(f"🎮 === EXECUTE_BALL START === Group: {group_id}")
    
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # ✅ SAFETY CHECK: Verify indices exist
    if bat_team.current_batsman_idx is None:
        logger.error("❌ CRITICAL: No striker selected!")
        await context.bot.send_message(group_id, "⚠️ Error: No striker found!")
        return
    
    if bat_team.current_non_striker_idx is None:
        logger.error("❌ CRITICAL: No non-striker selected!")
        await context.bot.send_message(group_id, "⚠️ Error: No non-striker found!")
        return
    
    if bowl_team.current_bowler_idx is None:
        logger.error("❌ CRITICAL: No bowler selected!")
        await context.bot.send_message(group_id, "⚠️ Error: No bowler found!")
        return
    
    striker = bat_team.players[bat_team.current_batsman_idx]
    non_striker = bat_team.players[bat_team.current_non_striker_idx]
    bowler = bowl_team.players[bowl_team.current_bowler_idx]
    
    logger.info(f"✅ Players verified:")
    logger.info(f"  🏏 Striker: {striker.first_name} (Index: {bat_team.current_batsman_idx})")
    logger.info(f"  🏃 Non-Striker: {non_striker.first_name} (Index: {bat_team.current_non_striker_idx})")
    logger.info(f"  ⚾ Bowler: {bowler.first_name} (Index: {bowl_team.current_bowler_idx})")
    
    # Clickable Names
    striker_tag = f"<a href='tg://user?id={striker.user_id}'>{striker.first_name}</a>"
    bowler_tag = f"<a href='tg://user?id={bowler.user_id}'>{bowler.first_name}</a>"

    # --- 🧮 CALCULATE STATS ---
    total_overs_bowled = max(bowl_team.overs, 0.1)
    crr = round(bat_team.score / total_overs_bowled, 2)
    
    # ✅ FIXED: Match Equation (For 2nd Innings)
    equation = ""
    if match.innings == 2:
        runs_needed = match.target - bat_team.score
        balls_left = (match.total_overs * 6) - bowl_team.balls  # ✅ USE BOWLING TEAM'S BALLS
        rrr = round((runs_needed / balls_left) * 6, 2) if balls_left > 0 else 0
        equation = f"\n🎯 <b>Target:</b> Need <b>{runs_needed}</b> off <b>{balls_left}</b> balls (RRR: {rrr})"

    # --- 🏟️ GROUP DISPLAY ---
    text = f"🔥 <b>LIVE</b>\n"
    text += "─────────────────────\n"
    text += f"🏏<b>Batsman:</b> <b>{striker_tag}</b>\n"
    text += f"⚾<b>Bowler:</b> <b>{bowler_tag}</b>\n"
    text += "─────────────────────\n"
    text += f"📊 <b>{bat_team.score}/{bat_team.wickets}</b>  ({format_overs(bowl_team.balls)})\n"
    
    if equation:
        text += f"{equation}\n"
    text += "─────────────────────\n"
    
    if match.is_free_hit:
        text += "🚨 <b>FREE HIT DELIVERY!</b> 🚨\n\n"
        
    text += f"📣 <b>{bowler.first_name}</b> is running in..."

    # Button
    keyboard = [[InlineKeyboardButton("📩 Tap to Bowl", url=f"https://t.me/{context.bot.username}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # GIF
    ball_gif = "https://t.me/kyanaamrkhe/6"
    
    try:
        await context.bot.send_animation(
            group_id, 
            animation=ball_gif,
            caption=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
        logger.info("✅ Ball animation sent to group")
    except Exception as e:
        logger.error(f"❌ Failed to send animation: {e}")
        await context.bot.send_message(group_id, text, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    
    # --- 📩 DM TO BOWLER ---
    dm_text = f"🏟️ <b>NEXT DELIVERY</b>\n"
    dm_text += "────────────────\n"
    dm_text += f"🏏 <b>Batsman:</b> {striker.first_name}\n"
    dm_text += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
    
    if match.innings == 2:
        runs_needed = match.target - bat_team.score
        balls_left = (match.total_overs * 6) - bowl_team.balls  # ✅ USE BOWLING TEAM'S BALLS
        dm_text += f"🎯 <b>Defend:</b> {runs_needed} runs / {balls_left} balls\n"
    
    dm_text += "\n👉 <b>Send Number (0-6)</b>\n"
    dm_text += "<i>Time: 45s</i>"
    
    try:
        await context.bot.send_message(bowler.user_id, dm_text, parse_mode=ParseMode.HTML)
        logger.info(f"✅ DM sent to bowler {bowler.first_name} (ID: {bowler.user_id})")
        
        match.current_ball_data = {
            "bowler_id": bowler.user_id, 
            "bowler_number": None, 
            "batsman_number": None,
            "group_id": group_id
        }
        
        logger.info(f"✅ Ball data initialized: {match.current_ball_data}")
        
        if match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        match.ball_timeout_task = asyncio.create_task(
            game_timer(context, group_id, match, "bowler", bowler.first_name)
        )
        
        logger.info(f"✅ Game timer started for bowler")
        
    except Forbidden:
        logger.warning(f"⚠️ Cannot DM bowler {bowler.first_name} - User hasn't started bot")
        await context.bot.send_message(
            group_id, 
            f"⚠️ <b>Start Bot:</b> {bowler_tag} please check your DMs and start the bot!", 
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"❌ DM error: {e}")
    
    logger.info(f"🎮 === EXECUTE_BALL END ===")

async def wait_for_bowler_number(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Wait for bowler to send number with reminders"""
    bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
    
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Send reminder at 30s
            try:
                await context.bot.send_message(
                    chat_id=bowler.user_id,
                    text="Reminder: Please send your number (0-6).\n30 seconds remaining."
                )
                match.current_ball_data["bowler_reminded"] = True
            except Exception as e:
                logger.error(f"Error sending reminder to bowler: {e}")
        
        # Wait 15 more seconds
        await asyncio.sleep(15)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Send reminder at 15s
            try:
                await context.bot.send_message(
                    chat_id=bowler.user_id,
                    text="Urgent: Send your number now!\n15 seconds remaining."
                )
            except Exception as e:
                logger.error(f"Error sending reminder to bowler: {e}")
        
        # Wait 10 more seconds
        await asyncio.sleep(10)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Send reminder at 5s
            try:
                await context.bot.send_message(
                    chat_id=bowler.user_id,
                    text="Final warning: 5 seconds left!"
                )
            except Exception as e:
                logger.error(f"Error sending reminder to bowler: {e}")
        
        # Wait final 5 seconds
        await asyncio.sleep(5)
        
        if match.current_ball_data.get("bowler_number") is None:
            # Timeout - handle penalty
            await handle_bowler_timeout(context, group_id, match)
    
    except asyncio.CancelledError:
        pass

async def handle_bowler_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle bowler timeout penalty"""
    bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
    bowler.bowling_timeouts += 1
    bowler.no_balls += 1
    
    timeout_count = bowler.bowling_timeouts
    
    # Add no ball
    match.current_batting_team.score += 1
    match.current_batting_team.extras += 1
    match.is_free_hit = True
    
    gif_url = get_random_gif(MatchEvent.NO_BALL)
    commentary = get_random_commentary("noball")
    
    if timeout_count >= 3:
        # Ban from bowling
        bowler.is_bowling_banned = True
        
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Bowler Timeout - {bowler.first_name}\n\n"
        penalty_text += f"{bowler.first_name} has timed out 3 times.\n"
        penalty_text += f"{bowler.first_name} is now BANNED from bowling.\n\n"
        penalty_text += "NO BALL\n"
        penalty_text += "Free Hit on next ball\n\n"
        penalty_text += f"{commentary}\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
    else:
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Bowler Timeout - {bowler.first_name} ({timeout_count}/3)\n\n"
        penalty_text += "NO BALL\n"
        penalty_text += "Free Hit on next ball\n\n"
        penalty_text += f"{commentary}\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
    
    try:
        if gif_url:
            await context.bot.send_animation(
                chat_id=group_id,
                animation=gif_url,
                caption=penalty_text
            )
        else:
            await context.bot.send_message(
                chat_id=group_id,
                text=penalty_text
            )
    except Exception as e:
        logger.error(f"Error sending timeout message: {e}")
        await context.bot.send_message(
            chat_id=group_id,
            text=penalty_text
        )
    
    # Continue with next ball
    await asyncio.sleep(2)
    
    # ============================================
    # 📸 PERIODIC SCORECARD - Every 11 balls in Team Mode
    # ============================================
    if bowl_team.balls % 11 == 0 and bowl_team.balls > 0:
        try:
            await mini_scorecard(context, group_id, match)
        except Exception as e:
            logger.error(f"Error sending periodic scorecard: {e}")
    
    await execute_ball(context, group_id, match)

    if bowler.is_bowling_banned:
        # Need new bowler
        match.waiting_for_bowler = True
        await request_bowler_selection(context, group_id, match)
    else:
        # Same bowler continues
        await execute_ball(context, group_id, match)

# Handle DM messages from players

async def bannedgroups_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Show list of all banned groups (Owner Only)
    """
    user = update.effective_user
    
    if user.id != OWNER_ID:
        return
    
    if not banned_groups:
        await update.message.reply_text(
            "✅ <b>NO BANNED GROUPS</b>\n\n"
            "Currently, no groups are banned from using the bot.",
            parse_mode=ParseMode.HTML
        )
        return
    
    msg = f"🚫 <b>BANNED GROUPS LIST</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 <b>Total:</b> {len(banned_groups)} groups\n\n"
    
    for i, chat_id in enumerate(banned_groups, 1):
        # Try to get group name
        try:
            chat_info = await context.bot.get_chat(chat_id)
            group_name = chat_info.title
        except:
            group_name = "Unknown/Left Group"
        
        msg += f"{i}. <b>{group_name}</b>\n"
        msg += f"   🆔 <code>{chat_id}</code>\n\n"
        
        # Telegram message limit protection
        if len(msg) > 3500:
            await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
            msg = "<b>...continued</b>\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━\n"
    msg += "<i>Use /unbangroup [id] to unban</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def unbangroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Unban a group (Owner Only)
    Usage: 
      - In group: /unbangroup
      - Via DM: /unbangroup -1001234567890
    """
    user = update.effective_user
    
    # Owner check
    if user.id != OWNER_ID:
        return
    
    chat = update.effective_chat
    
    # Method 1: Command used in the group itself
    if chat.type in ["group", "supergroup"]:
        target_chat_id = chat.id
        target_chat_name = chat.title
    
    # Method 2: Command used in DM with group ID
    elif context.args:
        try:
            target_chat_id = int(context.args[0])
            
            # Try to get group info
            try:
                chat_info = await context.bot.get_chat(target_chat_id)
                target_chat_name = chat_info.title
            except:
                target_chat_name = "Unknown Group"
        except ValueError:
            await update.message.reply_text(
                "⚠️ <b>Invalid Group ID</b>\n\n"
                "<b>Usage:</b>\n"
                "In Group: <code>/unbangroup</code>\n"
                "In DM: <code>/unbangroup -1001234567890</code>",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "In Group: <code>/unbangroup</code>\n"
            "In DM: <code>/unbangroup [group_id]</code>\n\n"
            "<b>Example:</b> <code>/unbangroup -1001234567890</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check if actually banned
    if target_chat_id not in banned_groups:
        await update.message.reply_text(
            f"⚠️ <b>{target_chat_name}</b> (<code>{target_chat_id}</code>) is not banned!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ✅ UNBAN THE GROUP
    banned_groups.discard(target_chat_id)
    save_data()
    
    # Notify the group
    await notify_ban_status(context, target_chat_id, is_ban=False)
    
    # Confirm to owner
    await update.message.reply_text(
        f"✅ <b>GROUP UNBANNED</b>\n\n"
        f"📛 <b>Name:</b> {target_chat_name}\n"
        f"🆔 <b>ID:</b> <code>{target_chat_id}</code>\n\n"
        f"✅ This group can now use the bot again.",
        parse_mode=ParseMode.HTML
    )
    
    # Log to support group
    try:
        await context.bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=(
                f"✅ <b>GROUP UNBANNED</b>\n"
                f"📛 {target_chat_name}\n"
                f"🆔 <code>{target_chat_id}</code>\n"
                f"👤 By: {user.first_name}"
            ),
            parse_mode=ParseMode.HTML
        )
    except:
        pass

# ==================== GROUP BAN MIDDLEWARE ====================

async def check_group_ban(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Check if group is banned. Returns True if allowed, False if banned.
    Call this at the start of every group command.
    """
    chat = update.effective_chat
    
    # Allow private chats and owner commands
    if chat.type == "private":
        return True
    
    if chat.id in banned_groups:
        # Silently ignore (bot won't respond in banned groups)
        logger.info(f"Blocked command in banned group: {chat.id}")
        return False
    
    return True

async def notify_ban_status(context: ContextTypes.DEFAULT_TYPE, chat_id: int, is_ban: bool):
    """Send notification to group about ban/unban"""
    if is_ban:
        msg = (
            "🚫 <b>GROUP BANNED</b> 🚫\n"
            "━━━━━━━━━━━━━━━━━\n"
            "This group has been <b>banned</b> from using this bot.\n\n"
            "🔒 All commands are now disabled.\n"
            "📧 Contact bot owner for more information."
        )
    else:
        msg = (
            "✅ <b>GROUP UNBANNED</b> ✅\n"
            "━━━━━━━━━━━━━━━━━\n"
            "This group can now use the bot again!\n\n"
            "🎮 Use /game to start playing."
        )
    
    try:
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Failed to notify group {chat_id}: {e}")

async def save_solo_match_stats(match):
    """Save solo match stats to database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get top 3 players
    sorted_players = sorted(
        match.solo_players.items(),
        key=lambda x: x[1]['runs'],
        reverse=True
    )[:3]
    
    for user_id, stats in match.solo_players.items():
        username = stats.get('username', 'Unknown')
        first_name = stats.get('first_name', 'Player')
        
        # Update user stats
        c.execute("SELECT user_id FROM user_stats WHERE user_id = ?", (user_id,))
        exists = c.fetchone()
        
        if not exists:
            c.execute("""
                INSERT INTO user_stats (user_id, username, first_name, total_runs, 
                                       total_balls_faced, matches_played)
                VALUES (?, ?, ?, ?, ?, 1)
            """, (user_id, username, first_name, stats['runs'], stats['balls']))
        else:
            c.execute("""
                UPDATE user_stats 
                SET total_runs = total_runs + ?,
                    total_balls_faced = total_balls_faced + ?,
                    matches_played = matches_played + 1,
                    highest_score = MAX(highest_score, ?),
                    username = ?,
                    first_name = ?
                WHERE user_id = ?
            """, (stats['runs'], stats['balls'], stats['runs'], 
                  username, first_name, user_id))
        
        # Check for fifties/hundreds
        if stats['runs'] >= 100:
            c.execute("UPDATE user_stats SET total_hundreds = total_hundreds + 1 WHERE user_id = ?", (user_id,))
        elif stats['runs'] >= 50:
            c.execute("UPDATE user_stats SET total_fifties = total_fifties + 1 WHERE user_id = ?", (user_id,))
    
    # Save match history
    winner_id = sorted_players[0][0] if sorted_players else None
    c.execute("""
        INSERT INTO match_history 
        (group_id, match_type, player_of_match, match_date)
        VALUES (?, 'SOLO', ?, CURRENT_TIMESTAMP)
    """, (match.group_id, winner_id))
    
    conn.commit()
    conn.close()

async def bangroup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Ban a group from using the bot (Owner Only)
    Usage: 
      - In group: /bangroup
      - Via DM: /bangroup -1001234567890
    """
    user = update.effective_user
    
    # Owner check
    if user.id != OWNER_ID:
        return
    
    chat = update.effective_chat
    
    # Method 1: Command used in the group itself
    if chat.type in ["group", "supergroup"]:
        target_chat_id = chat.id
        target_chat_name = chat.title
    
    # Method 2: Command used in DM with group ID as argument
    elif context.args:
        try:
            target_chat_id = int(context.args[0])
            
            # Try to get group info
            try:
                chat_info = await context.bot.get_chat(target_chat_id)
                target_chat_name = chat_info.title
            except:
                target_chat_name = "Unknown Group"
        except ValueError:
            await update.message.reply_text(
                "⚠️ <b>Invalid Group ID</b>\n\n"
                "<b>Usage:</b>\n"
                "In Group: <code>/bangroup</code>\n"
                "In DM: <code>/bangroup -1001234567890</code>",
                parse_mode=ParseMode.HTML
            )
            return
    else:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b>\n"
            "In Group: <code>/bangroup</code>\n"
            "In DM: <code>/bangroup [group_id]</code>\n\n"
            "<b>Example:</b> <code>/bangroup -1001234567890</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check if already banned
    if target_chat_id in banned_groups:
        await update.message.reply_text(
            f"⚠️ <b>{target_chat_name}</b> (<code>{target_chat_id}</code>) is already banned!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ✅ BAN THE GROUP
    banned_groups.add(target_chat_id)
    save_data()
    
    # End any active match in that group
    if target_chat_id in active_matches:
        match = active_matches[target_chat_id]
        match.phase = GamePhase.MATCH_ENDED
        
        # Cancel all tasks
        if match.ball_timeout_task: match.ball_timeout_task.cancel()
        if match.batsman_selection_task: match.batsman_selection_task.cancel()
        if match.bowler_selection_task: match.bowler_selection_task.cancel()
        if hasattr(match, 'join_phase_task') and match.join_phase_task: 
            match.join_phase_task.cancel()
        
        del active_matches[target_chat_id]
    
    # Notify the group
    await notify_ban_status(context, target_chat_id, is_ban=True)
    
    # Confirm to owner
    await update.message.reply_text(
        f"✅ <b>GROUP BANNED</b>\n\n"
        f"📛 <b>Name:</b> {target_chat_name}\n"
        f"🆔 <b>ID:</b> <code>{target_chat_id}</code>\n\n"
        f"🚫 This group can no longer use the bot.For unban contact @ASTRO_SHUBH",
        parse_mode=ParseMode.HTML
    )
    
    # Log to support group
    try:
        await context.bot.send_message(
            chat_id=SUPPORT_GROUP_ID,
            text=(
                f"🚫 <b>GROUP BANNED</b>\n"
                f"📛 {target_chat_name}\n"
                f"🆔 <code>{target_chat_id}</code>\n"
                f"👤 By: {user.first_name}"
            ),
            parse_mode=ParseMode.HTML
        )
    except:
        pass

async def process_player_number(update: Update, context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, number: int):
    """Process number sent by player"""
    user = update.effective_user
    
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
    
    # Check if bowler sent number
    if user.id == bowler.user_id and match.current_ball_data.get("bowler_number") is None:
        match.current_ball_data["bowler_number"] = number
        await update.message.reply_text(f"Your number: {number}\nWaiting for batsman...")
        
        # Cancel bowler timeout task
        if match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        
        # Now request batsman number
        await request_batsman_number(context, group_id, match)
        return
    
    # Check if batsman sent number
    if user.id == batsman.user_id and match.current_ball_data.get("batsman_number") is None:
        if match.current_ball_data.get("bowler_number") is None:
            await update.message.reply_text("Please wait for bowler to send their number first.")
            return
        
        match.current_ball_data["batsman_number"] = number
        await update.message.reply_text(f"Your number: {number}\nProcessing ball...")
        
        # Cancel batsman timeout task
        if match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        
        # Process ball result
        await process_ball_result(context, group_id, match)
        return

async def request_batsman_number(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Request batsman number with GIF - FIXED"""
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    
    batsman_tag = f"<a href='tg://user?id={batsman.user_id}'>{batsman.first_name}</a>"
    
    text = f"⚾ <b>Bowler has bowled!</b>\n"
    text += f"🏏 <b>{batsman_tag}</b>, it's your turn!\n"
    text += "👉 <b>Send your number (0-6) in this group!</b>\n"
    text += "⏳ <i>You have 45 seconds!</i>"
    
    # ✅ FIX: Add GIF
    batting_gif = "https://t.me/kyanaamrkhe/7"  # Cricket batting GIF
    
    try:
        await context.bot.send_animation(
            group_id,
            animation=batting_gif,
            caption=text,
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_message(group_id, text, parse_mode=ParseMode.HTML)
    
    if match.ball_timeout_task:
        match.ball_timeout_task.cancel()
    match.ball_timeout_task = asyncio.create_task(
        game_timer(context, group_id, match, "batsman", batsman.first_name)
    )

async def wait_for_batsman_number(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Wait for batsman to send number with reminders"""
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    
    try:
        # Wait 30 seconds
        await asyncio.sleep(30)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Send reminder at 30s
            try:
                await context.bot.send_message(
                    chat_id=batsman.user_id,
                    text="Reminder: Please send your number (0-6).\n30 seconds remaining."
                )
            except Exception as e:
                logger.error(f"Error sending reminder to batsman: {e}")
        
        # Wait 15 more seconds
        await asyncio.sleep(15)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Send reminder at 15s
            try:
                await context.bot.send_message(
                    chat_id=batsman.user_id,
                    text="Urgent: Send your number now!\n15 seconds remaining."
                )
            except Exception as e:
                logger.error(f"Error sending reminder to batsman: {e}")
        
        # Wait 10 more seconds
        await asyncio.sleep(10)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Send reminder at 5s
            try:
                await context.bot.send_message(
                    chat_id=batsman.user_id,
                    text="Final warning: 5 seconds left!"
                )
            except Exception as e:
                logger.error(f"Error sending reminder to batsman: {e}")
        
        # Wait final 5 seconds
        await asyncio.sleep(5)
        
        if match.current_ball_data.get("batsman_number") is None:
            # Timeout - handle penalty
            await handle_batsman_timeout(context, group_id, match)
    
    except asyncio.CancelledError:
        pass

async def handle_batsman_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle batsman timeout penalty"""
    batsman = match.current_batting_team.players[match.current_batting_team.current_batsman_idx]
    batsman.batting_timeouts += 1
    
    timeout_count = batsman.batting_timeouts
    
    # Penalty: -6 runs
    match.current_batting_team.score -= 6
    match.current_batting_team.penalty_runs += 6
    
    if timeout_count >= 3:
        # Auto out - Hit Wicket
        batsman.is_out = True
        batsman.dismissal_type = "Hit Wicket (Timeout)"
        match.current_batting_team.wickets += 1
        
        # ✅ FIX: Update player_stats dismissal field
        if batsman.user_id in match.current_batting_team.player_stats:
            match.current_batting_team.player_stats[batsman.user_id]["dismissal"] = batsman.dismissal_type
        
        gif_url = get_random_gif(MatchEvent.WICKET)
        
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Batsman Timeout - {batsman.first_name}\n\n"
        penalty_text += f"{batsman.first_name} has timed out 3 times.\n"
        penalty_text += "OUT - Hit Wicket\n\n"
        penalty_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n\n"
        penalty_text += f"6 runs penalty deducted.\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
        
        try:
            if gif_url:
                await context.bot.send_animation(
                    chat_id=group_id,
                    animation=gif_url,
                    caption=penalty_text
                )
            else:
                await context.bot.send_message(
                    chat_id=group_id,
                    text=penalty_text
                )
        except Exception as e:
            logger.error(f"Error sending timeout wicket message: {e}")
            await context.bot.send_message(
                chat_id=group_id,
                text=penalty_text
            )
        
        # Log ball
        match.ball_by_ball_log.append({
            "over": format_overs(match.current_bowling_team.balls),
            "batsman": batsman.first_name,
            "bowler": match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx].first_name,
            "result": "Wicket (Timeout)",
            "runs": -6,
            "is_wicket": True
        })
        
        await asyncio.sleep(3)
        
        # Check if innings over
        if match.is_innings_complete():
            await end_innings(context, group_id, match)
        else:
            # Request new batsman
            match.waiting_for_batsman = True
            await request_batsman_selection(context, group_id, match)
    else:
        penalty_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
        penalty_text += f"Batsman Timeout - {batsman.first_name} ({timeout_count}/3)\n\n"
        penalty_text += "6 runs penalty deducted.\n\n"
        penalty_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}\n\n"
        penalty_text += "Please send your number immediately!"
        
        await context.bot.send_message(
            chat_id=group_id,
            text=penalty_text
        )
        
        # Reset and wait again
        match.current_ball_data["batsman_number"] = None
        match.current_ball_data["batsman_start_time"] = time.time()
        match.ball_timeout_task = asyncio.create_task(
            wait_for_batsman_number(context, group_id, match)
        )


async def process_ball_result(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ COMPLETE FIX: Proper ball counting with OVERS LIMIT CHECK
    """
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    if bat_team.current_batsman_idx is None or bowl_team.current_bowler_idx is None:
        logger.error("❌ No batsman or bowler index!")
        return

    bowler_num = match.current_ball_data.get("bowler_number")
    batsman_num = match.current_ball_data.get("batsman_number")
    
    if bowler_num is None or batsman_num is None:
        logger.error("❌ Missing bowler or batsman number!")
        return
    
    striker = bat_team.players[bat_team.current_batsman_idx]
    bowler = bowl_team.players[bowl_team.current_bowler_idx]
    
    # ============================================
    # 🚨 WIDE BALL CHECK (3 Same Numbers)
    # ============================================
    if bowler_num is not None:
        is_wide = await check_wide_condition(match, bowler_num)
        if is_wide:
            bat_team.score += 1
            bat_team.extras += 1
            
            commentary = get_commentary("wide", group_id=group_id)
            
            msg = (
                f"🏏 <b>Over {format_overs(bowl_team.balls)}</b>\n\n"
                f"🚫 <b>WIDE BALL!</b> Bowler sent same number 3 times in a row!\n"
                f"💬 <i>{commentary}</i>\n\n"
                f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
                f"➕ <b>Extra:</b> +1 Run"
            )
            
            gif_url = get_random_gif(MatchEvent.WIDE)
            try:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            except:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            match.current_ball_data = {}
            await asyncio.sleep(2)
            
            # ✅ CHECK IF TARGET CHASED AFTER WIDE
            if match.innings == 2 and bat_team.score >= match.target:
                logger.info("🏆 TARGET CHASED AFTER WIDE!")
                await end_innings(context, group_id, match)
                return
            
            # ✅ CHECK IF OVERS COMPLETE AFTER WIDE
            if bowl_team.balls >= match.total_overs * 6:
                logger.info("⏱ OVERS COMPLETE AFTER WIDE!")
                await end_innings(context, group_id, match)
                return
            
            await execute_ball(context, group_id, match)
            return
    
    # ============================================
    # 🎯 WICKET LOGIC
    # ============================================
    if bowler_num == batsman_num:
        logger.info("❌ Numbers matched - WICKET!")
        
        if match.is_free_hit:
            # Free hit save logic
            half_runs = batsman_num // 2
            bat_team.score += half_runs
            striker.runs += half_runs
            striker.balls_faced += 1
            bowler.balls_bowled += 1
            bowler.runs_conceded += half_runs
            bowl_team.balls += 1
            bowl_team.update_overs()
            
            commentary = get_commentary("freehit", group_id=group_id)
            
            gif_url = get_random_gif(MatchEvent.FREE_HIT)
            msg = (
                f"⚡ <b>FREE HIT SAVE!</b> Numbers matched ({batsman_num}).\n"
                f"🏃 <b>Runs Awarded:</b> {half_runs} (Half runs)\n"
                f"💬 <i>{commentary}</i>\n"
                f"✅ <b>NOT OUT!</b>"
            )
            
            try:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            except:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
            
            match.is_free_hit = False
            
            # ✅ CHECK TARGET AFTER FREE HIT
            if match.innings == 2 and bat_team.score >= match.target:
                logger.info("🏆 TARGET CHASED ON FREE HIT!")
                await end_innings(context, group_id, match)
                return
            
            # ✅ CHECK IF OVERS COMPLETE
            if bowl_team.balls >= match.total_overs * 6:
                logger.info("⏱ OVERS COMPLETE!")
                await end_innings(context, group_id, match)
                return
            
            if bowl_team.get_current_over_balls() == 0:
                await check_over_complete(context, group_id, match)
            else:
                await asyncio.sleep(2)
                await execute_ball(context, group_id, match)
            return
        else:
            # 🎯 REGULAR WICKET
            striker_tag = get_user_tag(striker)
            commentary = get_commentary("wicket", group_id=group_id)
            
            wicket_msg = f"❌ <b>OUT!</b> ❌\n"
            wicket_msg += "─────────────────────\n"
            wicket_msg += f"🏏 <b>{striker_tag}</b> is given OUT!\n"
            wicket_msg += f"⚾ Bowler: <b>{bowler.first_name}</b>\n"
            wicket_msg += f"💬 <i>{commentary}</i>\n\n"
            wicket_msg += f"📊 <b>Score:</b> {striker.runs} ({striker.balls_faced})\n"
            wicket_msg += "─────────────────────"
            
            gif_url = get_random_gif(MatchEvent.WICKET)
            try:
                await context.bot.send_animation(group_id, animation=gif_url, caption=wicket_msg, parse_mode=ParseMode.HTML)
            except:
                await context.bot.send_message(group_id, wicket_msg, parse_mode=ParseMode.HTML)
            
            await asyncio.sleep(2)
            
            # ✅ INCREMENT BALLS FOR WICKET BALL
            striker.balls_faced += 1
            bowler.balls_bowled += 1
            bowl_team.balls += 1
            bowl_team.update_overs()
            
            # ✅ OFFER DRS
            match.last_wicket_ball = {
                "batsman": striker,
                "bowler": bowler,
                "bowler_number": bowler_num,
                "batsman_number": batsman_num
            }
            
            if bat_team.drs_remaining > 0:
                await offer_drs_to_captain(context, group_id, match)
            else:
                calculate_momentum_change(match, 0, True, False)
                await confirm_wicket_and_continue(context, group_id, match)
            return
    
    # ============================================
    # 🏃 RUNS SCORED
    # ============================================
    else:
        runs = batsman_num
        bat_team.score += runs
        striker.runs += runs
        striker.balls_faced += 1
        bowler.balls_bowled += 1
        bowler.runs_conceded += runs
        bowler.consecutive_wickets = 0  # 🔄 Reset consecutive wickets when runs scored
        
        # ✅ CRITICAL: INCREMENT BALLS HERE (BEFORE OVERS UPDATE)
        bowl_team.balls += 1
        bowl_team.update_overs()
        
        # Check milestones
        await check_and_celebrate_milestones(context, group_id, match, striker, 'batting')
        
        # Update boundaries
        if runs == 4:
            striker.boundaries += 1
        elif runs == 6:
            striker.sixes += 1
        
        # 🎯 TRACK STRIKE ZONE (NEW)
        zone = determine_strike_zone(runs)
        team_key = 'team_x' if match.current_batting_team == match.team_x else 'team_y'
        match.strike_zones[team_key][zone] += runs
        
        # ⚡ UPDATE MOMENTUM (NEW)
        is_boundary = runs in [4, 6]
        calculate_momentum_change(match, runs, False, is_boundary)

        # ✅ Get commentary based on GROUP setting
        events = {
            0: "dot", 
            1: "single", 
            2: "double", 
            3: "triple", 
            4: "boundary", 
            5: "five", 
            6: "six"
        }
        comm_key = events.get(runs, "dot")
        commentary = get_commentary(comm_key, group_id=group_id)
        
        # Get GIF
        event_type = getattr(MatchEvent, f"RUNS_{runs}") if runs > 0 else MatchEvent.DOT_BALL
        gif_url = get_random_gif(event_type)
        
        # Build message
        msg = f"🔴 <b>LIVE</b>\n"
        if match.is_free_hit:
            msg += "⚡ <b>FREE HIT</b>\n"
            match.is_free_hit = False
        msg += "─────────────────────\n"
        msg += f"🏃 <b>Runs Scored:</b> {runs}\n"
        msg += f"💬 <i>{commentary}</i>\n"
        msg += "─────────────────────\n"
        msg += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets}"
        
        # Send message
        try:
            if gif_url and runs > 0:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        # Swap batsmen on odd runs
        if runs % 2 == 1:
            bat_team.swap_batsmen()
    
    # ============================================
    # ✅ CRITICAL: CHECK IF TARGET CHASED
    # ============================================
    if match.innings == 2 and bat_team.score >= match.target:
        logger.info("🏆🎉 TARGET CHASED! MATCH WON!")
        await asyncio.sleep(3)
        await end_innings(context, group_id, match)
        return
    
    # ============================================
    # ✅ CRITICAL: CHECK IF OVERS COMPLETE (FIRST PRIORITY)
    # ============================================
    if bowl_team.balls >= match.total_overs * 6:
        logger.info("⏱ OVERS COMPLETE! INNINGS ENDING!")
        await asyncio.sleep(2)
        await end_innings(context, group_id, match)
        return
    
    # ============================================
    # 🔄 RESET & CONTINUE
    # ============================================
    match.current_ball_data = {}
    
    # Check if over completed
    current_over_balls = bowl_team.get_current_over_balls()
    
    if current_over_balls == 0 and bowl_team.balls > 0:
        await asyncio.sleep(2)
        await check_over_complete(context, group_id, match)
        return
    
    # Continue with next ball
    await asyncio.sleep(2)
    await execute_ball(context, group_id, match)

async def commentary_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle commentary style button clicks (Admin Only)
    """
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    data = query.data
    
    # Parse data: gcommentary_style_groupid
    if not data.startswith("gcommentary_"):
        return
    
    parts = data.split("_")
    if len(parts) != 3:
        return
    
    style = parts[1]
    group_id = int(parts[2])
    
    # Check if user is admin in this group
    try:
        member = await context.bot.get_chat_member(group_id, user.id)
        if member.status not in ["creator", "administrator"]:
            await query.answer("⚠️ Only admins can change commentary!", show_alert=True)
            return
    except Exception as e:
        logger.error(f"Admin check failed: {e}")
        await query.answer("Error verifying admin status", show_alert=True)
        return
    
    # Save to group settings
    if group_id not in registered_groups:
        registered_groups[group_id] = {
            "group_id": group_id,
            "group_name": query.message.chat.title,
            "total_matches": 0,
            "commentary_style": style
        }
    else:
        registered_groups[group_id]["commentary_style"] = style
    
    save_data()
    
    # Style names
    style_names = {
        "english": "English",
        "shubh": "SHUBH's Special",
        "sidhu": "Sidhu Paaji"
    }
    
    # Update message
    await query.message.edit_text(
        f"✅ <b>Group Commentary Updated!</b>\n\n"
        f"🎙️ <b>New Style:</b> {style_names.get(style, style)}\n"
        f"👥 <b>For:</b> All matches in this group\n"
        f"👮 <b>Changed by:</b> {user.first_name} (Admin)\n\n"
        f"<i>From now on, all matches will use {style_names.get(style, style).lower()} commentary.</i>",
        parse_mode=ParseMode.HTML
    )


async def check_wide_condition(match: Match, current_number: int) -> bool:
    """
    ✅ FIXED: Wide only if bowler sends same number 3 CONSECUTIVE times
    📌 Example: 4, 4, 4 = WIDE
    🚫 But: 4, 2, 4 = NOT WIDE (not consecutive)
    🚫 And: 4, 4, 3, 4 = NOT WIDE (sequence broken)
    """
    # 📜 Initialize history if not exists
    if not hasattr(match, 'bowler_number_history'):
        match.bowler_number_history = []
    
    # ➕ Add current number to history
    match.bowler_number_history.append(current_number)
    
    #✂️ Keep only last 3 numbers
    if len(match.bowler_number_history) > 3:
        match.bowler_number_history.pop(0)
    
    # 🔍 Check if last 3 are ALL same AND consecutive
    if len(match.bowler_number_history) == 3:
        # All three must be identical
        if (match.bowler_number_history[0] == match.bowler_number_history[1] == 
            match.bowler_number_history[2] == current_number):
            # ✅ LOG FOR DEBUGGING
            logger.info(f"🚨 WIDE DETECTED! History: {match.bowler_number_history}")
            # 🔄 Reset history after wide is called
            match.bowler_number_history = []
            return True
    
    return False

async def offer_drs_to_captain(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ DRS with INLINE BUTTONS - 10 second auto-reject
    """
    match.drs_in_progress = True
    batsman = match.last_wicket_ball["batsman"]
    bat_captain = match.get_captain(match.current_batting_team)
    
    if not bat_captain:
        logger.error("🚫 No batting captain found!")
        calculate_momentum_change(match, 0, True, False)
        await confirm_wicket_and_continue(context, group_id, match)
        return
    
    captain_tag = get_user_tag(bat_captain)
    batsman_tag = get_user_tag(batsman)
    
    # ⌨️ INLINE KEYBOARD (Updated Emojis)
    keyboard = [
        [InlineKeyboardButton("🖥️ DRS Review", callback_data="drs_take")],
        [InlineKeyboardButton("❌ Don't Want", callback_data="drs_reject")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    # ✨ Decorative Message
    msg = f"📡 <b>DRS AVAILABLE</b> 📡\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🧢 Captain {captain_tag}\n\n"
    msg += "⏳ <b>You have 10 seconds to decide!</b>\n\n"
    
    await context.bot.send_message(group_id, msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
    
    match.drs_in_progress = True
    match.drs_offer_time = time.time()
    
    # ⏱️ 10 Second Timer
    asyncio.create_task(drs_timeout_handler(context, group_id, match))

async def offer_drs(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Offer DRS to batting captain after wicket"""
    batsman = match.last_wicket_ball["batsman"]
    bowler = match.last_wicket_ball["bowler"]
    
    # Check if DRS available
    if match.current_batting_team.drs_remaining <= 0:
        # No DRS available, wicket confirmed
        await confirm_wicket(context, group_id, match, drs_used=False, drs_successful=False)
        return
    
    batting_captain = match.get_captain(match.current_batting_team)
    
    gif_url = get_random_gif(MatchEvent.WICKET)
    commentary = get_random_commentary("wicket")
    
    wicket_text = f"Over {format_overs(match.current_bowling_team.balls)}\n\n"
    wicket_text += f"Bowler: {match.last_wicket_ball['bowler_number']} | Batsman: {match.last_wicket_ball['batsman_number']}\n\n"
    wicket_text += "OUT - Bowled\n\n"
    wicket_text += f"{commentary}\n\n"
    wicket_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n\n"
    wicket_text += f"Captain {batting_captain.first_name}: You have {match.current_batting_team.drs_remaining} DRS review.\n"
    wicket_text += "Do you want to review this decision?\n\n"
    wicket_text += "Use /drs to review (30 seconds to decide)"
    
    try:
        if gif_url:
            await context.bot.send_animation(
                chat_id=group_id,
                animation=gif_url,
                caption=wicket_text
            )
        else:
            await context.bot.send_message(
                chat_id=group_id,
                text=wicket_text
            )
    except Exception as e:
        logger.error(f"Error sending wicket message: {e}")
        await context.bot.send_message(
            chat_id=group_id,
            text=wicket_text
        )
    
    match.drs_in_progress = True
    
    # Set timeout for DRS decision
    asyncio.create_task(drs_decision_timeout(context, group_id, match))

async def drs_timeout_handler(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """⏳ Auto-reject DRS after 10 seconds"""
    try:
        await asyncio.sleep(10)
        
        if not match.drs_in_progress:
            return
        
        match.drs_in_progress = False
        
        await context.bot.send_message(
            group_id,
            "⏰ <b>DRS Timeout!</b>\n"
            "⌛ 10 seconds over, no review taken.\n"
            "🚨 Decision stands. Wicket confirmed.\n\n",
            parse_mode=ParseMode.HTML
        )
        
        await asyncio.sleep(2)
        
        # ✅ CRITICAL FIX: Confirm wicket properly
        calculate_momentum_change(match, 0, True, False)
        await confirm_wicket_and_continue(context, group_id, match)
        
    except asyncio.CancelledError:
        pass

# ✅ DRS Callback Handler
async def drs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🖱️ Handle DRS button clicks"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches:
        return
    
    match = active_matches[chat.id]
    
    if not match.drs_in_progress:
        await query.answer("⌛ DRS window closed!", show_alert=True)
        return
    
    # Check if captain
    batting_captain = match.get_captain(match.current_batting_team)
    if user.id != batting_captain.user_id:
        await query.answer("🧢 Only Captain can decide DRS!", show_alert=True)
        return
    
    match.drs_in_progress = False
    
    if query.data == "drs_take":
        # 📺 Use DRS
        match.current_batting_team.drs_remaining -= 1
        await query.message.edit_text(
            f"🖥️ <b>DRS REVIEW TAKEN!</b>\n"
            f"📡 Checking with third umpire...",
            parse_mode=ParseMode.HTML
        )
        await process_drs_review(context, chat.id, match)
        
    elif query.data == "drs_reject":
        # ❌ Don't use DRS
        await query.message.edit_text(
            f"❌ <b>DRS NOT TAKEN</b>\n"
            f"🤝 Captain {batting_captain.first_name} accepted the decision.\n",
            parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(2)
        
        # ✅ CRITICAL FIX: Continue game after rejection
        calculate_momentum_change(match, 0, True, False)
        await confirm_wicket_and_continue(context, chat.id, match)

async def drs_decision_timeout(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Handle DRS decision timeout"""
    await asyncio.sleep(30)
    
    if not match.drs_in_progress:
        return
    
    # No DRS taken, confirm wicket
    match.drs_in_progress = False
    await confirm_wicket(context, group_id, match, drs_used=False, drs_successful=False)

# DRS command
async def drs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ UPDATED: DRS command with 10 second check
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.type == "private":
        await update.message.reply_text("This command only works in groups.")
        return
    
    if chat.id not in active_matches:
        await update.message.reply_text("No active match found.")
        return
    
    match = active_matches[chat.id]
    
    if not match.drs_in_progress:
        await update.message.reply_text("No DRS review available at this moment.")
        return
    
    # Check if 10 seconds have passed
    current_time = time.time()
    if current_time - match.drs_offer_time > 10:
        await update.message.reply_text("⚠️ DRS time expired! 10 seconds over.")
        match.drs_in_progress = False
        calculate_momentum_change(match, 0, True, False)
        await confirm_wicket_and_continue(context, chat.id, match)
        return
    
    # Check if user is batting captain
    batting_captain = match.get_captain(match.current_batting_team)
    if not batting_captain:
        await update.message.reply_text("⚠️ No batting captain found!")
        return
    
    if user.id != batting_captain.user_id:
        await update.message.reply_text(
            f"⚠️ Only {match.current_batting_team.name} Captain can request DRS."
        )
        return
    
    # Process DRS
    match.drs_in_progress = False
    match.current_batting_team.drs_remaining -= 1
    
    await process_drs_review(context, chat.id, match)


async def process_drs_review(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ FIXED: DRS successful doesn't reduce wickets below original count
    """
    batsman = match.last_wicket_ball["batsman"]
    
    drs_text = "📺 <b>DRS REVIEW IN PROGRESS</b>\n\n"
    drs_text += "🔍 Checking with third umpire...\n"
    drs_text += "⏳ Please wait..."
    
    gif_url = get_random_gif(MatchEvent.DRS_REVIEW)
    
    try:
        if gif_url:
            msg = await context.bot.send_animation(group_id, animation=gif_url, caption=drs_text, parse_mode=ParseMode.HTML)
        else:
            msg = await context.bot.send_message(group_id, drs_text, parse_mode=ParseMode.HTML)
    except:
        msg = await context.bot.send_message(group_id, drs_text, parse_mode=ParseMode.HTML)
    
    await asyncio.sleep(3)
    
    is_overturned = random.random() < 0.40
    
    if is_overturned:
        # ✅ NOT OUT - Don't touch wickets (they were never incremented)
        batsman.is_out = False
        match.current_batting_team.out_players_indices.discard(match.current_batting_team.current_batsman_idx)
        
        gif = "https://tenor.com/bOVyJ.gif"
        
        result_text = "📺 <b>DRS RESULT</b>\n\n"
        result_text += "✅ <b>NOT OUT!</b>\n\n"
        result_text += f"🎉 {batsman.first_name} survives!\n"
        result_text += "Decision overturned.\n\n"
        result_text += f"🔄 DRS Remaining: {match.current_batting_team.drs_remaining}"
        
        try:
            await context.bot.send_animation(group_id, animation=gif, caption=result_text, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, result_text, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(2)
        
        if match.current_bowling_team.get_current_over_balls() == 0:
            await check_over_complete(context, group_id, match)
        else:
            await execute_ball(context, group_id, match)
    else:
        # OUT confirmed
        result_text = "📺 <b>DRS RESULT</b>\n\n"
        result_text += "❌ <b>OUT!</b>\n\n"
        result_text += "Decision stands.\n\n"
        result_text += f"📊 {batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n"
        result_text += f"🔄 DRS Remaining: {match.current_batting_team.drs_remaining}"
        
        try:
            await context.bot.send_animation(group_id, animation=gif_url, caption=result_text, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, result_text, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(2)
        calculate_momentum_change(match, 0, True, False)
        await confirm_wicket_and_continue(context, group_id, match)

async def confirm_wicket_and_continue(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ FIXED: Wicket handler WITH proper wicket increment
    """
    
    logger.info(f"🔴🏏 === WICKET HANDLER START === Group: {group_id}")
    
    if match.phase == GamePhase.MATCH_ENDED:
        logger.warning("🛑 Match already ended, aborting wicket handling")
        return

    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # Get the OUT player
    if bat_team.current_batsman_idx is None:
        logger.error("🚫👤 CRITICAL: No batsman index set!")
        await context.bot.send_message(group_id, "⚠️ Error: No batsman found!", parse_mode=ParseMode.HTML)
        return
    
    out_player = bat_team.players[bat_team.current_batsman_idx]
    bowler = bowl_team.players[bowl_team.current_bowler_idx]
    
    logger.info(f"☠️ OUT Player: {out_player.first_name} (Index: {bat_team.current_batsman_idx})")
    
    await asyncio.sleep(1)
    
    # ✅ CRITICAL FIX: INCREMENT WICKETS HERE
    out_player.is_out = True
    bat_team.wickets += 1  # ✅ INCREMENT TEAM WICKET COUNT
    bowler.wickets += 1    # ✅ INCREMENT BOWLER WICKET COUNT
    bowler.consecutive_wickets += 1  # 🎯 Increment consecutive wickets for hat-trick
    
    logger.info(f"✅ Wickets updated: Team={bat_team.wickets}, Bowler={bowler.wickets}, Consecutive={bowler.consecutive_wickets}")
    
    # 🎉 CHECK BOWLING MILESTONE
    await check_and_celebrate_milestones(context, group_id, match, bowler, 'bowling')
    
    # 📄 Send Mini Scorecard
    try:
        mini_card = generate_mini_scorecard(match)
        # Send with photo
        try:
            await context.bot.send_photo(
                group_id,
                photo=MINI_SCORECARD_PHOTO,
                caption=mini_card,
                parse_mode=ParseMode.HTML
            )
        except Exception as photo_error:
            logger.error(f"Error sending mini scorecard photo: {photo_error}")
            # Fallback to text
            await context.bot.send_message(group_id, mini_card, parse_mode=ParseMode.HTML)
        logger.info("📨 Mini scorecard sent")
    except Exception as e:
        logger.error(f"🚫 Failed to send mini scorecard: {e}")
    
    await asyncio.sleep(2)
    
    # ⚰️ Mark player as OUT
    bat_team.out_players_indices.add(bat_team.current_batsman_idx)
    logger.info(f"✅ Marked {out_player.first_name} as OUT (Total Out: {len(bat_team.out_players_indices)})")
    
    # 🧹 Clear striker position
    bat_team.current_batsman_idx = None
    logger.info("🧹 Striker position cleared, non-striker remains same")
    
    # 🧮 Check innings end conditions
    remaining_players = len(bat_team.players) - len(bat_team.out_players_indices)
    logger.info(f"👥 Remaining Players: {remaining_players}")
    
    if bat_team.is_all_out():
        logger.info("❌🛑 ALL OUT - Ending Innings")
        await context.bot.send_message(group_id, "❌🛑 <b>ALL OUT!</b> Innings ended.", parse_mode=ParseMode.HTML)
        await asyncio.sleep(2)
        await end_innings(context, group_id, match)
        return
    
    if bat_team.balls >= match.total_overs * 6:
        logger.info("⏱ Overs complete - Ending Innings")
        await end_innings(context, group_id, match)
        return
    
    if match.innings == 2 and bat_team.score >= match.target:
        logger.info("🎉🏆 Target chased - Match Won")
        await end_innings(context, group_id, match)
        return
    
    # 🔄 PRIORITY #1: Request new batsman FIRST (ALWAYS)
    match.waiting_for_batsman = True
    match.waiting_for_bowler = False
    match.current_ball_data = {}
    logger.info("⏸️✋ Game PAUSED - waiting_for_batsman=True")
    
    batting_captain = match.get_captain(bat_team)
    if not batting_captain:
        logger.error("🚫👮 CRITICAL: No batting captain found!")
        await context.bot.send_message(group_id, "⚠️ Error: No captain found!", parse_mode=ParseMode.HTML)
        return
    
    captain_tag = get_user_tag(batting_captain)
    available_batsmen = [
        p for i, p in enumerate(bat_team.players) 
        if i not in bat_team.out_players_indices 
        and i != bat_team.current_non_striker_idx
    ]
    available_count = len(available_batsmen)
    
    if available_count == 0:
        logger.error("🔭 No available batsmen!")
        await end_innings(context, group_id, match)
        return
    
    msg = f"🚨🤺 <b>NEW BATSMAN NEEDED!</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"☠️😔 <b>{out_player.first_name}</b> is OUT!\n\n"
    msg += f"👮‍♂️👉 <b>{captain_tag}</b>, select the <b>NEW STRIKER</b>:\n"
    msg += f"⌨️ <b>Command:</b> <code>/batting [serial]</code>\n\n"
    msg += f"📈 <b>Score:</b> {bat_team.score}/{bat_team.wickets}\n"
    msg += f"👥 <b>Available Players:</b> {available_count}\n\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⏳ <i>You have 2 minutes to select</i>"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    logger.info("📨 New batsman request sent to group")
    
    # ⏱️ Start selection timer
    match.batsman_selection_time = time.time()
    match.batsman_selection_task = asyncio.create_task(
        batsman_selection_timeout(context, group_id, match)
    )
    logger.info("⏱️🚀 Batsman selection timer started")
    
    # 📌 STORE OVER STATUS - Will be checked AFTER batsman is selected
    current_over_balls = bowl_team.get_current_over_balls()
    match.pending_over_complete = (current_over_balls == 0 and bowl_team.balls > 0)
    logger.info(f"📊 Over status stored: pending_over_complete={match.pending_over_complete}")
    logger.info(f"🔴🏏 === WICKET HANDLER END ===\n")

async def check_drinks_break(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    🥤 DRINKS BREAK - Triggers at 10th over (60 balls)
    """
    bat_team = match.current_batting_team
    
    # 🕵️ Check if 10 overs completed (60 balls)
    if bat_team.balls == 60:
        drinks_gif = "https://media.giphy.com/media/l0HlBO7eyXzSZkJri/giphy.gif"
        
        msg = f"🥤 <b>DRINKS BREAK!</b> 🥤\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📊 <b>Score:</b> {bat_team.score}/{bat_team.wickets} ({format_overs(bat_team.balls)})\n\n"
        
        # 🧮 Calculate stats
        overs_played = max(bat_team.overs, 0.1)
        crr = round(bat_team.score / overs_played, 2)
        msg += f"📈 <b>Current RR:</b> {crr}\n"
        
        if match.innings == 2:
            runs_needed = match.target - bat_team.score
            balls_left = (match.total_overs * 6) - bat_team.balls
            rrr = round((runs_needed / balls_left) * 6, 2) if balls_left > 0 else 0
            msg += f"🎯 <b>Required RR:</b> {rrr}\n"
            msg += f"🏏 <b>Need:</b> {runs_needed} runs in {balls_left} balls\n"
        
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "⏳ <i>Resuming in 30 seconds...</i>"
        
        try:
            await context.bot.send_animation(group_id, animation=drinks_gif, caption=msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        # ⏱️ 30 second pause
        await asyncio.sleep(30)
        
        await context.bot.send_message(
            group_id, 
            "▶️ <b>GAME RESUMED!</b>\nLet's continue...", 
            parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(2)

async def check_over_complete(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    🏁 End of Over: Show Mini Scorecard, Swap Batsmen, Request New Bowler
    """
    
    logger.info(f"🟢 check_over_complete called for group {group_id}")
    
    if match.phase == GamePhase.MATCH_ENDED:
        logger.warning("🛑 Match already ended, skipping over complete")
        return

    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    # Get current bowler info BEFORE clearing
    if bowl_team.current_bowler_idx is not None:
        bowler = bowl_team.players[bowl_team.current_bowler_idx]
        bowl_team.bowler_history.append(bowl_team.current_bowler_idx)
        logger.info(f"✅ Bowler {bowler.first_name} added to history")
    else:
        bowler = type("obj", (object,), {"first_name": "Unknown", "wickets": 0, "runs_conceded": 0})
        logger.error("🚫 No bowler found at over complete!")
    
    # ✅ STEP 1: SEND MINI SCORECARD
    mini_card = generate_mini_scorecard(match)
    try:
        await context.bot.send_photo(
            group_id,
            photo=MINI_SCORECARD_PHOTO,
            caption=mini_card,
            parse_mode=ParseMode.HTML
        )
    except Exception as photo_error:
        logger.error(f"Error sending mini scorecard photo: {photo_error}")
        # Fallback to text
        await context.bot.send_message(group_id, mini_card, parse_mode=ParseMode.HTML)
    logger.info("📨 Mini scorecard sent")
    
    await asyncio.sleep(1)
    
    # ✅ STEP 2: SWAP BATSMEN (Strike Rotation)
    logger.info(f"🔄 Swapping batsmen - Before: Striker={bat_team.current_batsman_idx}, Non-Striker={bat_team.current_non_striker_idx}")
    bat_team.swap_batsmen()
    logger.info(f"🔄 After swap: Striker={bat_team.current_batsman_idx}, Non-Striker={bat_team.current_non_striker_idx}")
    
    # Re-fetch players after swap
    new_striker = bat_team.players[bat_team.current_batsman_idx] if bat_team.current_batsman_idx is not None else None
    new_non_striker = bat_team.players[bat_team.current_non_striker_idx] if bat_team.current_non_striker_idx is not None else None

    # Over Complete Summary
    summary = f"🏁 <b>OVER COMPLETE!</b> ({format_overs(bowl_team.balls)})\n"
    summary += "➖➖➖➖➖➖➖➖➖➖➖➖\n"
    summary += f"🥎 Bowler <b>{bowler.first_name}</b> finished his over.\n\n"
    summary += f"🔄 <b>BATSMEN SWAPPED:</b>\n"
    summary += f"  🏏 New Striker: {new_striker.first_name if new_striker else 'None'}\n"
    summary += f"  👟 Non-Striker: {new_non_striker.first_name if new_non_striker else 'None'}\n\n"
    summary += "➖➖➖➖➖➖➖➖➖➖➖➖"
    
    await context.bot.send_message(group_id, summary, parse_mode=ParseMode.HTML)
    logger.info("📨 Over summary sent to group")
    
    # ✅ DRINKS BREAK CHECK (10th over = 60 balls)
    if bat_team.balls == 60:
        await check_drinks_break(context, group_id, match)
    
    # ✅ STEP 3: CHECK INNINGS/MATCH END
    if bat_team.balls >= match.total_overs * 6:
        logger.info("⌛ Innings complete - Overs finished")
        await end_innings(context, group_id, match)
        return
    
    if match.innings == 2 and bat_team.score >= match.target:
        logger.info("🏆 Match won - Target chased")
        await end_innings(context, group_id, match)
        return
    
    # ✅ STEP 4: CLEAR OLD BOWLER & PAUSE GAME
    logger.info(f"🛑 Clearing bowler - Old index: {bowl_team.current_bowler_idx}")
    bowl_team.current_bowler_idx = None  
    match.waiting_for_bowler = True
    match.waiting_for_batsman = False
    match.current_ball_data = {}
    logger.info("⏸️ Match state set to waiting_for_bowler=True")
    
    await asyncio.sleep(2)
    
    # ✅ STEP 5: REQUEST NEW BOWLER (CRITICAL)
    logger.info("📢 Calling request_bowler_selection...")
    await request_bowler_selection(context, group_id, match)
    logger.info("✅ request_bowler_selection completed")


# --- HELPER FUNCTIONS FOR GRAPHICS ENGINE ---


async def confirm_wicket(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, drs_used: bool, drs_successful: bool):
    """Confirm wicket and update match state"""
    batsman = match.last_wicket_ball["batsman"]
    bowler = match.last_wicket_ball["bowler"]
    
    # Mark batsman as out
    batsman.is_out = True
    batsman.dismissal_type = "Bowled"
    match.current_batting_team.wickets += 1
    bowler.wickets += 1
    
    # ✅ FIX: Update player_stats dismissal field
    if batsman.user_id in match.current_batting_team.player_stats:
        match.current_batting_team.player_stats[batsman.user_id]["dismissal"] = batsman.dismissal_type
    
    # ✅ CHECK BOWLING MILESTONE
    await check_and_celebrate_milestones(context, group_id, match, bowler, 'bowling')

    # ✅ CHECK BOWLING MILESTONE
    await check_and_celebrate_milestones(context, group_id, match, bowler, 'bowling')
    
    if drs_used and not drs_successful:
        gif_url = get_random_gif(MatchEvent.DRS_OUT)
        
        result_text = "DRS Result\n\n"
        result_text += "Decision: OUT\n\n"
        result_text += "The original decision stands.\n\n"
        result_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n\n"
        result_text += f"DRS Remaining: {match.current_batting_team.drs_remaining}\n\n"
        result_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
        
        try:
            if gif_url:
                await context.bot.send_animation(
                    chat_id=group_id,
                    animation=gif_url,
                    caption=result_text
                )
            else:
                await context.bot.send_message(
                    chat_id=group_id,
                    text=result_text
                )
        except Exception as e:
            logger.error(f"Error sending DRS out message: {e}")
            await context.bot.send_message(
                chat_id=group_id,
                text=result_text
            )
    else:
        wicket_confirm_text = f"Wicket Confirmed\n\n"
        wicket_confirm_text += f"{batsman.first_name}: {batsman.runs} ({batsman.balls_faced})\n"
        wicket_confirm_text += f"Bowler: {bowler.first_name}\n\n"
        wicket_confirm_text += f"Score: {match.current_batting_team.score}/{match.current_batting_team.wickets}"
        
        await context.bot.send_message(
            chat_id=group_id,
            text=wicket_confirm_text
        )
    
    # Update stats
    batsman.balls_faced += 1
    bowler.balls_bowled += 1
    match.current_bowling_team.update_overs()
    
    # Check for duck
    if batsman.runs == 0:
        batsman.ducks += 1
    
    # Log ball
    match.ball_by_ball_log.append({
        "over": format_overs(match.current_bowling_team.balls - 1),
        "batsman": batsman.first_name,
        "bowler": bowler.first_name,
        "result": "Wicket",
        "runs": 0,
        "is_wicket": True
    })
    
    await asyncio.sleep(2)
    
    # Check if innings over
    if match.is_innings_complete():
        await end_innings(context, group_id, match)
    else:
        # Request new batsman
        match.waiting_for_batsman = True
        await request_batsman_selection(context, group_id, match)

# --- UPDATE handle_dm_message ---

async def addauctionplayer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ➕ Add player to auction pool (Mid-Auction)
    Usage: Reply to user with /addauctionplayer
    or /addauctionplayer @username
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text("❌ No active auction!")
        return
    
    auction = active_auctions[chat.id]
    
    # Only host/auctioneer can add
    if user.id not in [auction.host_id, auction.auctioneer_id]:
        await update.message.reply_text("🚫 Only Host/Auctioneer can add players!")
        return
    
    # Get target user
    target_user = None
    
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
    elif context.args:
        arg = context.args[0]
        if arg.startswith("@"):
            username = arg[1:].lower()
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username:
                    try:
                        target_user = await context.bot.get_chat(uid)
                    except:
                        pass
                    break
        elif arg.isdigit():
            try:
                target_user = await context.bot.get_chat(int(arg))
            except:
                pass
    
    if not target_user:
        await update.message.reply_text(
            "ℹ️ <b>Usage:</b>\n"
            "Reply: <code>/addauctionplayer</code>\n"
            "Username: <code>/addauctionplayer @username</code>\n"
            "ID: <code>/addauctionplayer 123456789</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check if already in pool or sold
    if any(p["player_id"] == target_user.id for p in auction.player_pool):
        await update.message.reply_text("⚠️ Player already in auction pool!")
        return
    
    if any(p["player_id"] == target_user.id for p in auction.sold_players):
        await update.message.reply_text("⚠️ Player already sold!")
        return
    
    # Initialize if new user
    if target_user.id not in user_data:
        user_data[target_user.id] = {
            "user_id": target_user.id,
            "username": target_user.username or "",
            "first_name": target_user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(target_user.id)
        save_data()
    
    # Base price selection
    keyboard = [
        [InlineKeyboardButton("💰 10", callback_data=f"midauc_base_10_{target_user.id}"),
         InlineKeyboardButton("💰 20", callback_data=f"midauc_base_20_{target_user.id}")],
        [InlineKeyboardButton("💰 30", callback_data=f"midauc_base_30_{target_user.id}"),
         InlineKeyboardButton("💰 50", callback_data=f"midauc_base_50_{target_user.id}")]
    ]
    
    target_tag = get_user_tag(target_user)
    
    await update.message.reply_text(
        f"➕ <b>Adding Player to Auction</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 {target_tag}\n\n"
        f"💰 Select Base Price:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )


async def midauc_base_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle mid-auction player base price"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    
    if chat.id not in active_auctions:
        return
    
    auction = active_auctions[chat.id]
    
    # Parse callback data
    try:
        parts = query.data.split("_")
        price = int(parts[2])
        player_id = int(parts[3])
    except:
        return
    
    # Get player info
    player_info = user_data.get(player_id)
    if not player_info:
        await query.message.edit_text("❌ Player not found!")
        return
    
    # Add to pool
    auction.player_pool.append({
        "player_id": player_id,
        "player_name": player_info["first_name"],
        "base_price": price
    })
    
    player_tag = f"<a href='tg://user?id={player_id}'>{player_info['first_name']}</a>"
    
    await query.message.edit_text(
        f"✅ <b>Player Added to Pool!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 {player_tag}\n"
        f"💰 Base Price: {price}\n\n"
        f"📊 Total in Pool: {len(auction.player_pool)}",
        parse_mode=ParseMode.HTML
    )


# 2️⃣ REMOVE PLAYER FROM AUCTION
async def removeauctionplayer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ➖ Remove player from auction pool
    Usage: Reply to user with /removeauctionplayer
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text("❌ No active auction!")
        return
    
    auction = active_auctions[chat.id]
    
    # Only host/auctioneer
    if user.id not in [auction.host_id, auction.auctioneer_id]:
        await update.message.reply_text("🚫 Only Host/Auctioneer can remove players!")
        return
    
    # Cannot remove during live bidding
    if auction.phase == AuctionPhase.AUCTION_LIVE and auction.current_player_id:
        await update.message.reply_text("⚠️ Cannot remove player during active bidding!")
        return
    
    # Get target
    target_id = None
    
    if update.message.reply_to_message:
        target_id = update.message.reply_to_message.from_user.id
    elif context.args:
        arg = context.args[0]
        if arg.startswith("@"):
            username = arg[1:].lower()
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username:
                    target_id = uid
                    break
        elif arg.isdigit():
            target_id = int(arg)
    
    if not target_id:
        await update.message.reply_text(
            "ℹ️ <b>Usage:</b>\n"
            "Reply: <code>/removeauctionplayer</code>\n"
            "Username: <code>/removeauctionplayer @username</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Find and remove
    found = False
    for i, p in enumerate(auction.player_pool):
        if p["player_id"] == target_id:
            removed = auction.player_pool.pop(i)
            found = True
            
            await update.message.reply_text(
                f"➖ <b>Player Removed!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 {removed['player_name']}\n"
                f"💰 Base: {removed['base_price']}\n\n"
                f"📊 Remaining in Pool: {len(auction.player_pool)}",
                parse_mode=ParseMode.HTML
            )
            break
    
    if not found:
        await update.message.reply_text("⚠️ Player not found in auction pool!")


# 3️⃣ ADD/REMOVE PURSE
async def addpurse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    💰 Add money to team purse
    Usage: /addpurse [team_name] [amount]
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text("❌ No active auction!")
        return
    
    auction = active_auctions[chat.id]
    
    # Only host
    if user.id != auction.host_id:
        await update.message.reply_text("🚫 Only Host can modify purse!")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "ℹ️ <b>Usage:</b>\n"
            "<code>/addpurse [TeamName] [Amount]</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/addpurse Mumbai Indians 100</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        amount = int(context.args[-1])
        team_name = " ".join(context.args[:-1])
    except:
        await update.message.reply_text("❌ Invalid amount!")
        return
    
    if team_name not in auction.teams:
        await update.message.reply_text(f"❌ Team '{team_name}' not found!")
        return
    
    team = auction.teams[team_name]
    team.purse_remaining += amount
    
    await update.message.reply_text(
        f"💰 <b>Purse Updated!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏏 <b>Team:</b> {team_name}\n"
        f"➕ <b>Added:</b> {amount}\n"
        f"💵 <b>New Balance:</b> {team.purse_remaining}",
        parse_mode=ParseMode.HTML
    )


async def removepurse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    💸 Remove money from team purse
    Usage: /removepurse [team_name] [amount]
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text("❌ No active auction!")
        return
    
    auction = active_auctions[chat.id]
    
    # Only host
    if user.id != auction.host_id:
        await update.message.reply_text("🚫 Only Host can modify purse!")
        return
    
    if len(context.args) < 2:
        await update.message.reply_text(
            "ℹ️ <b>Usage:</b>\n"
            "<code>/removepurse [TeamName] [Amount]</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/removepurse Mumbai Indians 50</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    try:
        amount = int(context.args[-1])
        team_name = " ".join(context.args[:-1])
    except:
        await update.message.reply_text("❌ Invalid amount!")
        return
    
    if team_name not in auction.teams:
        await update.message.reply_text(f"❌ Team '{team_name}' not found!")
        return
    
    team = auction.teams[team_name]
    
    if team.purse_remaining < amount:
        await update.message.reply_text(
            f"⚠️ Insufficient funds!\n"
            f"Available: {team.purse_remaining}",
            parse_mode=ParseMode.HTML
        )
        return
    
    team.purse_remaining -= amount
    
    await update.message.reply_text(
        f"💸 <b>Purse Deducted!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏏 <b>Team:</b> {team_name}\n"
        f"➖ <b>Removed:</b> {amount}\n"
        f"💵 <b>New Balance:</b> {team.purse_remaining}",
        parse_mode=ParseMode.HTML
    )


# 4️⃣ PLAYER STATS IN AUCTION
async def bring_next_player(context: ContextTypes.DEFAULT_TYPE, chat_id: int, auction: Auction):
    """🎯 Bring next player with PREMIUM UI"""
    
    # Check if auction complete
    if len(auction.player_pool) == 0:
        await end_auction(context, chat_id, auction)
        return
    
    # Get player
    player = auction.player_pool.pop(0)
    auction.current_player_id = player["player_id"]
    auction.current_player_name = player["player_name"]
    auction.current_base_price = player["base_price"]
    auction.current_highest_bid = player["base_price"]
    auction.current_highest_bidder = None
    
    # ✅ FIX: Set phase to AUCTION_LIVE
    auction.phase = AuctionPhase.AUCTION_LIVE
    
    # 🎬 PLAYER INTRODUCTION
    player_gif = GIFS.get("auction_live")
    player_tag = f"<a href='tg://user?id={player['player_id']}'>{player['player_name']}</a>"
    
    # Fetch player stats
    p_id = player['player_id']
    init_player_stats(p_id)
    s = player_stats.get(p_id, {}).get('team', {})
    
    m, r, w = s.get("matches", 0), s.get("runs", 0), s.get("wickets", 0)
    avg = round(r / max(s.get("outs", m), 1), 1) if s.get("outs", 0) > 0 else 0
    sr = round((r / max(s.get("balls", 1), 1)) * 100, 1) if s.get("balls", 0) > 0 else 0
    eco = round((s.get("runs_conceded", 0) / max(s.get("balls_bowled", 1), 1)) * 6, 1) if s.get("balls_bowled", 0) > 0 else 0
    best = s.get("best_bowling", "N/A")
    
    msg = (
        f"👤 <b>PLAYER ON AUCTION</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎯 <b>Name:</b> {player_tag}\n"
        f"💰 <b>Base Price:</b> {player['base_price']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>TEAM CAREER STATS</b>\n\n"
        f"🏟 <b>Matches:</b> {m}\n"
        f"🏏 <b>Runs:</b> {r} | <b>Avg:</b> {avg}\n"
        f"⚡ <b>S/R:</b> {sr}\n"
        f"🎯 <b>Wickets:</b> {w} | 📉 <b>Eco:</b> {eco}\n"
        f"🔥 <b>Best:</b> {best}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📢 <b>Current Bid:</b> {player['base_price']}\n"
        f"👥 <b>Highest Bidder:</b> None\n"
        f"⏱ <b>Timer:</b> 30 seconds\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💡 <b>To Bid:</b> <code>/bid [amount]</code>\n"
        f"📊 <b>Players Remaining:</b> {len(auction.player_pool)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━"
    )
    
    try:
        await context.bot.send_animation(
            chat_id,
            animation=player_gif,
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_photo(
            chat_id,
            photo=MEDIA_ASSETS.get("auction_live"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    
    # Start timer
    auction.bid_end_time = time.time() + 30
    auction.bid_timer_task = asyncio.create_task(bid_timer(context, chat_id, auction))

# /soloplayers
async def soloplayers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Fancy Solo Player List"""
    chat = update.effective_chat
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO":
        await update.message.reply_text("⚠️ This is not a Solo match!")
        return
        
    msg = "📜 <b>SOLO BATTLE ROSTER</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    
    for i, p in enumerate(match.solo_players, 1):
        # Status Logic
        status = "⏳ <i>Waiting</i>"
        if p.is_out: status = "❌ <b>OUT</b>"
        elif match.phase == GamePhase.SOLO_MATCH:
            if i-1 == match.current_solo_bat_idx: status = "🏏 <b>BATTING</b>"
            elif i-1 == match.current_solo_bowl_idx: status = "⚾ <b>BOWLING</b>"
            elif p.is_bowling_banned: status = "🚫 <b>BANNED (Bowl)</b>"
            
        msg += f"<b>{i}. {p.first_name}</b>\n   └ {status} • {p.runs} Runs\n"
        
    msg += "━━━━━━━━━━━━━━━━━━━━━━"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

# /soloscore
async def soloscore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ ENHANCED Solo Leaderboard - Detailed & Clean"""
    chat = update.effective_chat
    
    if chat.id not in active_matches: 
        await update.message.reply_text("⚠️ No active match!")
        return
    
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO":
        await update.message.reply_text("⚠️ This is not a Solo match!")
        return
    
    # Sort by Runs (Desc)
    sorted_players = sorted(match.solo_players, key=lambda x: x.runs, reverse=True)
    
    msg = "🏆 <b>SOLO BATTLE LEADERBOARD</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    medals = ["🥇", "🥈", "🥉"]
    
    for i, p in enumerate(sorted_players, 1):
        rank = medals[i-1] if i <= 3 else f"<b>{i}.</b>"
        
        # Status
        status = ""
        if p.is_out: 
            status = " ❌"
        elif i-1 == match.current_solo_bat_idx: 
            status = " 🏏"
        elif i-1 == match.current_solo_bowl_idx: 
            status = " ⚾"
        
        # Stats
        sr = round((p.runs / max(p.balls_faced, 1)) * 100, 1)
        
        msg += f"{rank} <b>{p.first_name}</b>{status}\n"
        msg += f"   📊 <b>{p.runs}</b> runs ({p.balls_faced} balls)\n"
        msg += f"   ⚡ SR: {sr} | 🎯 Wickets: {p.wickets}\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<i>🔝 Top 3 in the spotlight!</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def start_solo_mechanics(context, chat_id, match):
    """Initializes Solo Match with randomized order"""
    match.phase = GamePhase.SOLO_MATCH
    
    # Randomize Order
    random.shuffle(match.solo_players)
    
    match.current_solo_bat_idx = 0
    match.current_solo_bowl_idx = 1
    match.solo_balls_this_spell = 0
    
    # Announce Order
    order_msg = "🎲 <b>TOSS & BATTING ORDER</b>\n"
    order_msg += "━━━━━━━━━━━━━━━━━━━━━━\n"
    order_msg += "The order has been shuffled! Here is the lineup:\n\n"
    
    for i, p in enumerate(match.solo_players, 1):
        ptag = f"<a href='tg://user?id={p.user_id}'>{p.first_name}</a>"
        role = " (🏏 Striker)" if i == 1 else " (⚾ Bowler)" if i == 2 else ""
        order_msg += f"<code>{i}.</code> <b>{ptag}</b>{role}\n"
    
    order_msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    order_msg += "🔥 <i>Match Starting in 5 seconds...</i>"
    
    # Send with Toss/Squad Image
    await context.bot.send_photo(
        chat_id=chat_id,
        photo=MEDIA_ASSETS.get("toss"),
        caption=order_msg,
        parse_mode=ParseMode.HTML
    )
    
    await asyncio.sleep(5)
    
    # Start First Ball
    await trigger_solo_ball(context, chat_id, match)

async def trigger_solo_ball(context, chat_id, match):
    """Sets up the next ball with Timers"""
    batter = match.solo_players[match.current_solo_bat_idx]
    bowler = match.solo_players[match.current_solo_bowl_idx]
    
    match.current_ball_data = {
        "bowler_id": bowler.user_id,
        "bowler_number": None,
        "batsman_number": None,
        "is_solo": True
    }
    
    bat_tag = f"<a href='tg://user?id={batter.user_id}'>{batter.first_name}</a>"
    bowl_tag = f"<a href='tg://user?id={bowler.user_id}'>{bowler.first_name}</a>"
    
    # Calculate Strike Rate
    sr = round((batter.runs / batter.balls_faced) * 100, 1) if batter.balls_faced > 0 else 0
    
    msg = f"🔴 <b>LIVE</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"⚾ <b>{bowl_tag}</b> is going for run up...\n"
    msg += f"🔄 <b>Spell:</b> Ball {match.solo_balls_this_spell + 1}/3\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━"
    
    keyboard = [[InlineKeyboardButton("📩 Deliver Bowl", url=f"https://t.me/{context.bot.username}")]]
    
    ball_gif = "https://t.me/kyanaamrkhe/6"
    try:
        await context.bot.send_animation(chat_id, ball_gif, caption=msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.HTML)
    
    # DM Bowler
    try:
        dm_msg = f"⚔️ <b>SOLO MATCH</b>\n"
        dm_msg += f"🎯 Target: <b>{batter.first_name}</b> (Runs: {batter.runs})\n"
        dm_msg += "👉 Send your number (0-6)"
        await context.bot.send_message(bowler.user_id, dm_msg, parse_mode=ParseMode.HTML)
        
        # ✅ START BOWLER TIMER
        match.ball_timeout_task = asyncio.create_task(
            solo_game_timer(context, chat_id, match, "bowler", bowler.first_name)
        )
    except:
        await context.bot.send_message(chat_id, f"⚠️ Cannot DM {bowl_tag}. Please start the bot!", parse_mode=ParseMode.HTML)

async def process_solo_turn_result(context, chat_id, match):
    """Calculates Solo result with FIXED Next Batsman flow"""
    batter = match.solo_players[match.current_solo_bat_idx]
    bowler = match.solo_players[match.current_solo_bowl_idx]
    bat_num = match.current_ball_data["batsman_number"]
    bowl_num = match.current_ball_data["bowler_number"]
    
    bat_tag = f"<a href='tg://user?id={batter.user_id}'>{batter.first_name}</a>"
    bowl_tag = f"<a href='tg://user?id={bowler.user_id}'>{bowler.first_name}</a>"

    # --- 1. WICKET LOGIC ---
    if bat_num == bowl_num:
        batter.is_out = True
        match.solo_balls_this_spell = 0
        
        gif = get_random_gif(MatchEvent.WICKET)
        commentary = get_commentary("wicket", group_id=chat_id)
        sr = round((batter.runs / max(batter.balls_faced, 1)) * 100, 1)
        
        msg = f"❌ <b>OUT! {batter.first_name} is gone!</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>Final Score:</b> {batter.runs} ({batter.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {sr}\n"
        msg += f"💬 <i>{commentary}</i>"

        try:
            if gif: await context.bot.send_animation(chat_id, gif, caption=msg, parse_mode=ParseMode.HTML)
            else: await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

        # Move to NEXT Batsman
        match.current_solo_bat_idx += 1
        
        # Check if ALL OUT
        if match.current_solo_bat_idx >= len(match.solo_players):
            await end_solo_game_logic(context, chat_id, match)
            return

        # Assign New Bowler (Batsman ke next wala)
        match.current_solo_bowl_idx = (match.current_solo_bat_idx + 1) % len(match.solo_players)
        new_batter = match.solo_players[match.current_solo_bat_idx]
        new_bat_tag = f"<a href='tg://user?id={new_batter.user_id}'>{new_batter.first_name}</a>"
        
        await asyncio.sleep(2)
        await context.bot.send_message(
            chat_id, 
            f"⚡ <b>NEXT BATSMAN:</b> {new_bat_tag} walks to the crease!\n<i>Game resuming...</i>", 
            parse_mode=ParseMode.HTML
        )
        
        # ✅ CRITICAL FIX: Naya ball trigger karna zaroori hai
        await asyncio.sleep(2)
        await trigger_solo_ball(context, chat_id, match)
        return

    # --- 2. RUNS LOGIC ---
    else:
        runs = bat_num
        batter.runs += runs
        batter.balls_faced += 1
        
        # Map Run Events to GIFs
        events = {0: MatchEvent.DOT_BALL, 1: MatchEvent.RUNS_1, 2: MatchEvent.RUNS_2, 
                  3: MatchEvent.RUNS_3, 4: MatchEvent.RUNS_4, 5: MatchEvent.RUNS_5, 6: MatchEvent.RUNS_6}
        comm_keys = {0: "dot", 1: "single", 2: "double", 3: "triple", 4: "boundary", 5: "five", 6: "six"}
        
        gif = get_random_gif(events.get(runs))
        commentary = get_commentary(comm_keys.get(runs), group_id=chat_id)
        sr = round((batter.runs / batter.balls_faced) * 100, 1)

        msg = f"🔴 <b>LIVE</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>{runs} RUN{'S' if runs != 1 else ''}!</b>\n"
        msg += f"💬 <i>{commentary}</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📊 <b>{batter.first_name}:</b> {batter.runs} ({batter.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {sr}"

        try:
            if gif: await context.bot.send_animation(chat_id, gif, caption=msg, parse_mode=ParseMode.HTML)
            else: await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

        # Over/Spell Rotation (Every 3 balls)
        match.solo_balls_this_spell += 1
        if match.solo_balls_this_spell >= 3:
            match.solo_balls_this_spell = 0
            old_idx = match.current_solo_bowl_idx
            next_idx = (old_idx + 1) % len(match.solo_players)
            
            # Bowler cannot be the current Batsman
            if next_idx == match.current_solo_bat_idx:
                next_idx = (next_idx + 1) % len(match.solo_players)
            
            match.current_solo_bowl_idx = next_idx
            new_bowler = match.solo_players[next_idx]
            
            await asyncio.sleep(1)
            await context.bot.send_message(
                chat_id, 
                f"🔄 <b>CHANGE OF OVER!</b>\nNew Bowler: <b>{new_bowler.first_name}</b> takes the ball.", 
                parse_mode=ParseMode.HTML
            )
            await asyncio.sleep(2)

        # Trigger Next Ball
        await trigger_solo_ball(context, chat_id, match)

# --- NEW: Solo Callback Handler ---
async def solo_join_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Solo Join/Leave AND Start Game - WITH TIMER"""
    query = update.callback_query
    await query.answer()
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    # --- ACTION: START GAME ---
    if query.data == "solo_start_game":
        # 1. Check Permissions (Host Only)
        if user.id != match.host_id:
            await query.answer("⚠️ Only the Host can start the match!", show_alert=True)
            return
            
        # 2. Check Player Count
        if len(match.solo_players) < 2:
            await query.answer("⚠️ Need at least 2 players to start!", show_alert=True)
            return

        # 3. Cancel Timer Task before starting
        if hasattr(match, 'solo_timer_task') and match.solo_timer_task:
            match.solo_timer_task.cancel()

        # 4. Start the Game Logic
        await start_solo_mechanics(context, chat.id, match)
        return

    # --- VALIDATION FOR JOIN/LEAVE ---
    if match.game_mode != "SOLO" or match.phase != GamePhase.SOLO_JOINING:
        await query.answer("Registration Closed!", show_alert=True)
        return

    user_tag = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"

    # --- ACTION: JOIN ---
    if query.data == "solo_join":
        # Check duplicate
        for p in match.solo_players:
            if p.user_id == user.id:
                await query.answer("Already joined!", show_alert=True)
                return
        
        # Init Stats if new user
        if user.id not in player_stats: init_player_stats(user.id)
        
        # Add Player
        p = Player(user.id, user.username or "", user.first_name)
        match.solo_players.append(p)
        
        # Update the Board Image
        await update_solo_board(context, chat.id, match)
        
        # Send Tagged Message
        msg = f"✅ <b>{user_tag}</b> has entered the Battleground! 🔥"
        await context.bot.send_message(chat.id, msg, parse_mode=ParseMode.HTML)

    # --- ACTION: LEAVE ---
    elif query.data == "solo_leave":
        for i, p in enumerate(match.solo_players):
            if p.user_id == user.id:
                match.solo_players.pop(i)
                
                # Update the Board Image
                await update_solo_board(context, chat.id, match)
                
                # Send Tagged Message
                msg = f"👋 <b>{user_tag}</b> chickened out and left."
                await context.bot.send_message(chat.id, msg, parse_mode=ParseMode.HTML)
                return
        
        await query.answer("You are not in the list.", show_alert=True)

async def solo_join_countdown(context, chat_id, match):
    """Background Timer for Solo Joining Phase - Auto Updates Board"""
    try:
        warning_sent = False
        while True:
            # Check if phase changed
            if match.phase != GamePhase.SOLO_JOINING:
                break

            remaining = match.solo_join_end_time - time.time()
            
            # 30 Seconds Warning
            if remaining <= 30 and remaining > 20 and not warning_sent:
                await context.bot.send_message(
                    chat_id, 
                    "⚠️ <b>Hurry Up! Only 30 seconds left to join!</b>", 
                    parse_mode=ParseMode.HTML
                )
                warning_sent = True

            # Time Up
            if remaining <= 0:
                # Check minimum players
                if len(match.solo_players) < 2:
                    await context.bot.send_message(
                        chat_id,
                        "❌ <b>Match Cancelled!</b>\nNot enough players joined.",
                        parse_mode=ParseMode.HTML
                    )
                    del active_matches[chat_id]
                else:
                    # Auto-start the game
                    await context.bot.send_message(
                        chat_id,
                        "⏰ <b>Time's Up!</b> Starting match now...",
                        parse_mode=ParseMode.HTML
                    )
                    await start_solo_mechanics(context, chat_id, match)
                break
            
            # Wait 10 seconds before next update
            await asyncio.sleep(10)
            
            # Update Board
            if match.phase == GamePhase.SOLO_JOINING:
                await update_solo_board(context, chat_id, match)
            
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Solo timer error: {e}")

async def end_solo_game_logic(context, chat_id, match):
    """✅ FIXED: Solo End with GC Notification"""
    match.phase = GamePhase.MATCH_ENDED
    
    # ✅ GENERATE AND SEND FINAL IMAGE FIRST
    try:
        await generate_solo_end_image_v2(context, chat_id, match)
    except Exception as e:
        logger.error(f"Error generating solo end image: {e}")
    
    await asyncio.sleep(2)
    
    sorted_players = sorted(match.solo_players, key=lambda x: x.runs, reverse=True)
    winner = sorted_players[0]
    
    # Save Stats
    for p in match.solo_players:
        init_player_stats(p.user_id)
        
        if p.user_id in player_stats:
            s = player_stats[p.user_id]["solo"]
            s["matches"] += 1
            s["runs"] += p.runs
            s["balls"] += p.balls_faced
            s["wickets"] += p.wickets
            if p.runs == 0 and p.is_out: s["ducks"] += 1
            if p.runs > s["highest"]: s["highest"] = p.runs
            
            if p.user_id == winner.user_id: 
                s["wins"] += 1
            if p in sorted_players[:3]:
                s["top_3_finishes"] += 1
                
    save_data()

    # ✅ 1. NOTIFY ALL PLAYERS IN GC
    winner_tag = f"<a href='tg://user?id={winner.user_id}'>{winner.first_name}</a>"
    
    notify_msg = f"🏁 <b>SOLO BATTLE ENDED!</b> 🏁\n"
    notify_msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    notify_msg += f"🎊 <b>Winner:</b> {winner_tag}\n"
    notify_msg += f"📊 <b>Final Score:</b> {winner.runs} runs\n\n"
    notify_msg += f"<i>🏆 Congratulations to the champion!</i>\n"
    notify_msg += f"<i>📋 Check /soloscore for final standings</i>"
    
    await context.bot.send_message(chat_id, notify_msg, parse_mode=ParseMode.HTML)
    await asyncio.sleep(2)

    # ✅ 2. VICTORY GIF WITH DETAILED CARD
    winner_sr = round((winner.runs / winner.balls_faced) * 100, 1) if winner.balls_faced > 0 else 0
    
    msg = f"🏆 <b>SOLO BATTLE CHAMPION</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"👑 <b>WINNER: {winner_tag}</b>\n"
    msg += f"💥 <b>Score:</b> {winner.runs} ({winner.balls_faced})\n"
    msg += f"🔥 <b>Strike Rate:</b> {winner_sr}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += "📊 <b>FINAL LEADERBOARD</b>\n"
    medals = ["🥇", "🥈", "🥉"]
    
    for i, p in enumerate(sorted_players):
        rank_icon = medals[i] if i < 3 else f"<b>{i+1}.</b>"
        status = " (Not Out)" if not p.is_out else ""
        sr = round((p.runs / p.balls_faced) * 100, 1) if p.balls_faced > 0 else 0
        msg += f"{rank_icon} <b>{p.first_name}</b>: {p.runs}({p.balls_faced}) SR: {sr}{status}\n"
        
    msg += "━━━━━━━━━━━━━━━━━━━━━"

    # Try sending with photo first
    try:
        await context.bot.send_photo(chat_id, photo=SOLO_END_PHOTO, caption=msg, parse_mode=ParseMode.HTML)
        return
    except Exception as e:
        logger.error(f"Error sending solo end photo: {e}")
    
    # Fallback to GIF
    victory_gif = get_random_gif(MatchEvent.VICTORY)
    try:
        if victory_gif:
            await context.bot.send_animation(chat_id, victory_gif, caption=msg, parse_mode=ParseMode.HTML)
        else:
            await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

async def endsolo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End solo match - ADMIN ONLY"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    match = active_matches.get(chat_id)
    if not match:
        await update.message.reply_text("❌ No active match in this group!")
        return
    
    if match.phase not in [GamePhase.SOLO_JOINING, GamePhase.SOLO_MATCH]:
        await update.message.reply_text("❌ No solo match is running!")
        return
    
    # Check if user is admin
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        is_admin = member.status in ['creator', 'administrator']
    except:
        is_admin = False
    
    if not is_admin and user_id != match.host_id:
        await update.message.reply_text("❌ Only admins or host can end the solo match!")
        return
    
    # Show confirmation buttons
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, End Solo", callback_data=f"confirm_endsolo_{chat_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_endsolo")
        ]
    ])
    
    await update.message.reply_text(
        "⚠️ Are you sure you want to end this solo match?\n\n"
        "📊 Final results will be generated and stats will be saved.",
        reply_markup=keyboard
    )

# /extendsolo
async def extendsolo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.id not in active_matches: return
    match = active_matches[chat.id]
    
    if match.game_mode != "SOLO" or match.phase != GamePhase.SOLO_JOINING:
        await update.message.reply_text("Can only extend during joining phase.")
        return

    try:
        sec = int(context.args[0])
        match.solo_join_end_time += sec
        await update.message.reply_text(f"✅ Extended by {sec} seconds!")
    except:
        await update.message.reply_text("Usage: /extendsolo <seconds>")


# ═══════════════════════════════════════════════════════════════
# AI MODE - PLAY VS BOT IN DM
# ═══════════════════════════════════════════════════════════════

async def aistart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start AI match in DM"""
    user = update.effective_user
    chat = update.effective_chat
    
    # Only works in DM
    if chat.type != "private":
        await update.message.reply_text("🤖 AI Mode only works in DM! Send /aistart in my DM.")
        return
    
    # Check if already in a match
    if user.id in ai_matches:
        await update.message.reply_text("⚠️ You're already in an AI match! Use /aiquit to end it.")
        return
    
    # Initialize player stats
    if user.id not in player_stats:
        init_player_stats(user.id)
    
    # Select difficulty
    keyboard = [
        [InlineKeyboardButton("😊 Easy", callback_data="ai_diff_easy")],
        [InlineKeyboardButton("😐 Medium", callback_data="ai_diff_medium")],
        [InlineKeyboardButton("😈 Hard", callback_data="ai_diff_hard")]
    ]
    
    caption = (
        "🤖 <b>AI MODE - Select Difficulty</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Choose your opponent difficulty:"
    )
    
    try:
        await update.message.reply_photo(
            photo=MEDIA_ASSETS.get("mode_select"),
            caption=caption,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except:
        await update.message.reply_text(
            caption,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )


async def ai_difficulty_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle AI difficulty selection"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    difficulty = query.data.split("_")[-1]  # easy, medium, hard
    
    # Select overs
    keyboard = [
        [InlineKeyboardButton("3 Overs", callback_data=f"ai_over_3_{difficulty}"),
         InlineKeyboardButton("5 Overs", callback_data=f"ai_over_5_{difficulty}")],
        [InlineKeyboardButton("10 Overs", callback_data=f"ai_over_10_{difficulty}")]
    ]
    
    diff_emoji = {"easy": "😊", "medium": "😐", "hard": "😈"}
    
    caption = f"🤖 <b>Difficulty:</b> {diff_emoji[difficulty]} {difficulty.upper()}\n\n" \
              "Select match overs:"
    
    try:
        # Try to edit caption (if it's a photo message)
        await query.message.edit_caption(
            caption=caption,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except:
        # Fallback to edit_text (if it's a text message)
        try:
            await query.message.edit_text(
                text=caption,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
        except:
            # Last resort - send new message
            await context.bot.send_message(
                user.id,
                text=caption,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )


async def ai_over_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle AI overs selection and start match"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    data_parts = query.data.split("_")  # ai_over_<overs>_<difficulty>
    overs = int(data_parts[2])
    difficulty = data_parts[3]
    
    # Create AI match
    ai_matches[user.id] = {
        "difficulty": difficulty,
        "overs": overs,
        "user_score": 0,
        "user_wickets": 0,
        "user_balls": 0,
        "ai_score": 0,
        "ai_wickets": 0,
        "ai_balls": 0,
        "innings": 1,  # 1 = user batting, 2 = AI batting
        "target": 0,
        "phase": "toss",  # toss -> waiting_user
        "ai_number": None,
        "user_stats": {"runs": 0, "balls": 0, "fours": 0, "sixes": 0},
        "ai_stats": {"runs": 0, "balls": 0, "wickets": 0},
        "user_spam_history": [],  # Track user's last numbers for spam detection
        "ai_spam_history": []  # Track AI's last numbers for spam detection
    }
    
    # Ask user to call toss
    keyboard = [
        [InlineKeyboardButton("🪙 Heads", callback_data="ai_toss_heads"),
         InlineKeyboardButton("🪙 Tails", callback_data="ai_toss_tails")]
    ]
    
    try:
        await query.message.delete()
    except:
        pass
    
    await context.bot.send_photo(
        user.id,
        photo=MEDIA_ASSETS.get("toss"),
        caption="🪙 <b>TOSS TIME!</b>\n\n"
                "Call it - Heads or Tails?",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )


async def ai_toss_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toss result"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    
    if user.id not in ai_matches:
        await query.answer("Match not found!", show_alert=True)
        return
    
    match = ai_matches[user.id]
    user_call = query.data.split("_")[-1]  # heads or tails
    
    # Random toss result
    toss_result = random.choice(["heads", "tails"])
    
    if user_call == toss_result:
        # User won toss - let them choose
        keyboard = [
            [InlineKeyboardButton("🏏 Bat First", callback_data="ai_choice_bat"),
             InlineKeyboardButton("⚾ Bowl First", callback_data="ai_choice_bowl")]
        ]
        
        try:
            await query.message.delete()
        except:
            pass
        
        await context.bot.send_photo(
            user.id,
            photo=MEDIA_ASSETS.get("toss"),
            caption=f"🎉 <b>YOU WON THE TOSS!</b>\n\n"
                    f"🪙 Result: {toss_result.upper()}\n\n"
                    f"What do you want to do?",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    else:
        # AI won toss - AI chooses randomly
        ai_choice = random.choice(["bat", "bowl"])
        
        msg = f"🤖 <b>AI WON THE TOSS!</b>\n\n"
        msg += f"🪙 Result: {toss_result.upper()}\n"
        msg += f"AI chose to <b>{'BAT' if ai_choice == 'bat' else 'BOWL'}</b> first.\n\n"
        
        if ai_choice == "bat":
            # AI bats first
            match["innings"] = 2
            match["phase"] = "waiting_ai"
            msg += "🤖 AI is batting first.\n⚾ You'll bowl!"
            
            try:
                await query.message.delete()
            except:
                pass
            
            await context.bot.send_photo(
                user.id,
                photo=MEDIA_ASSETS.get("toss"),
                caption=msg,
                parse_mode=ParseMode.HTML
            )
            
            await asyncio.sleep(2)
            await ai_bat_innings(context, user.id)
        else:
            # User bats first
            msg += "🏏 You are batting first!"
            
            try:
                await query.message.delete()
            except:
                pass
            
            await context.bot.send_photo(
                user.id,
                photo=MEDIA_ASSETS.get("toss"),
                caption=msg,
                parse_mode=ParseMode.HTML
            )
            
            match["phase"] = "waiting_user"
            await asyncio.sleep(1)
            await ai_play_ball(context, user.id)


async def ai_choice_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user's choice after winning toss"""
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    
    if user.id not in ai_matches:
        await query.answer("Match not found!", show_alert=True)
        return
    
    match = ai_matches[user.id]
    choice = query.data.split("_")[-1]  # bat or bowl
    
    if choice == "bowl":
        # User bowls, AI bats first
        match["innings"] = 2
        match["phase"] = "waiting_ai"
        
        msg = f"⚾ <b>YOU CHOSE TO BOWL!</b>\n\n"
        msg += "🤖 AI is batting first."
        
        try:
            await query.message.delete()
        except:
            pass
        
        await context.bot.send_photo(
            user.id,
            photo=MEDIA_ASSETS.get("toss"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
        
        await asyncio.sleep(2)
        await ai_bat_innings(context, user.id)
    else:
        # User bats first
        msg = f"🏏 <b>YOU CHOSE TO BAT!</b>\n\n"
        msg += "Get ready to play!"
        
        try:
            await query.message.delete()
        except:
            pass
        
        await context.bot.send_photo(
            user.id,
            photo=MEDIA_ASSETS.get("toss"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
        
        match["phase"] = "waiting_user"
        await asyncio.sleep(1)
        await ai_play_ball(context, user.id)


async def ai_play_ball(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """AI plays a ball automatically"""
    if user_id not in ai_matches:
        return
    
    match = ai_matches[user_id]
    difficulty = match["difficulty"]
    
    # AI bowling selection - IMPROVED INTELLIGENCE
    user_score = match["user_score"]
    user_balls = match["user_balls"]
    target = match.get("target", 0)
    
    if difficulty == "easy":
        # Easy: Random but slightly favors middle numbers
        weights = [1, 1.2, 1.5, 1.5, 1.5, 1.2, 1]
        ai_choice = random.choices([0, 1, 2, 3, 4, 5, 6], weights=weights)[0]
    elif difficulty == "medium":
        # Medium: Strategic bowling based on match situation
        if match["innings"] == 2:  # Defending
            runs_needed = target - user_score
            balls_left = (match["overs"] * 6) - user_balls
            if runs_needed < balls_left:  # User ahead - be aggressive
                weights = [1.5, 1.5, 2, 2, 2, 1.8, 1.5]  # Favor middle
            else:  # Tight match - vary
                weights = [1.2, 1.5, 1.8, 2, 1.8, 1.5, 1.2]
        else:  # First innings
            weights = [1, 1.5, 2, 2.5, 2, 1.5, 1]  # Slightly favor 3
        ai_choice = random.choices([0, 1, 2, 3, 4, 5, 6], weights=weights)[0]
    else:  # HARD - SUPER INTELLIGENT BOWLING
        # Track user's recent shots (if available)
        user_wickets = match["user_wickets"]
        
        if match["innings"] == 2:  # Defending a target
            runs_needed = target - user_score
            balls_left = (match["overs"] * 6) - user_balls
            
            if runs_needed <= 6:  # Very close - try to get them out
                # Bowls numbers user is more likely to choose under pressure
                weights = [1.2, 1.5, 2.5, 3, 2.5, 2, 1.5]
            elif runs_needed > balls_left * 5:  # User needs to attack
                # They'll play big shots - bowl defensively
                weights = [2, 2, 2.5, 2.5, 1.5, 1.2, 1]  # Avoid 4,5,6
            elif balls_left <= 6:  # Last over - pressure bowling
                weights = [1.5, 2, 2.5, 3, 2.5, 2, 1.5]
            else:  # Normal bowling
                weights = [1.5, 2, 2.5, 2.8, 2.5, 2, 1.5]
        else:  # First innings - bowl to dismiss
            if user_wickets >= 7:  # Many wickets down - aggressive
                weights = [1, 1.5, 2.5, 3, 2.5, 1.8, 1.2]
            elif user_score > (user_balls / 6) * 8:  # User scoring fast
                # They're attacking - bowl variations
                weights = [2, 2, 2.2, 2.5, 2.2, 2, 1.8]
            else:  # Normal situation
                weights = [1.5, 2, 2.5, 3, 2.5, 2, 1.5]
        
        ai_choice = random.choices([0, 1, 2, 3, 4, 5, 6], weights=weights)[0]
        
        # 20% chance to bowl a completely random ball (unpredictability)
        if random.random() < 0.20:
            ai_choice = random.randint(0, 6)
    
    match["ai_number"] = ai_choice
    match["phase"] = "waiting_user"
    
    # Notify user to play
    msg = f"🤖 <b>AI HAS BOWLED!</b>\n\n"
    msg += f"📊 <b>Score:</b> {match['user_score']}/{match['user_wickets']}\n"
    msg += f"⚾ <b>Balls:</b> {match['user_balls']}/{match['overs']*6}\n\n"
    msg += "🏏 <b>Send your shot (0-6)!</b>"
    
    try:
        # Send bowling animation
        await context.bot.send_animation(
            user_id, 
            animation="https://t.me/kyanaamrkhe/7",
            caption=msg, 
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_message(user_id, msg, parse_mode=ParseMode.HTML)


async def aistats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show current AI match stats"""
    user = update.effective_user
    
    if user.id not in ai_matches:
        await update.message.reply_text("⚠️ No active AI match! Use /aistart to begin.")
        return
    
    match = ai_matches[user.id]
    
    msg = "🤖 <b>LIVE AI MATCH</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if match["innings"] == 1:
        msg += f"🏏 <b>YOU:</b> {match['user_score']}/{match['user_wickets']} ({match['user_balls']//6}.{match['user_balls']%6})\n"
        msg += f"🎯 <b>Target:</b> Beat AI's score!\n"
    else:
        msg += f"🎯 <b>Target:</b> {match['target']}\n"
        msg += f"🏏 <b>YOU:</b> {match['user_score']}/{match['user_wickets']} ({match['user_balls']//6}.{match['user_balls']%6})\n"
        msg += f"🤖 <b>AI:</b> {match['ai_score']}/{match['ai_wickets']}\n"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)


async def aiquit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Quit current AI match"""
    user = update.effective_user
    
    if user.id not in ai_matches:
        await update.message.reply_text("⚠️ No active AI match!")
        return
    
    del ai_matches[user.id]
    await update.message.reply_text("✅ AI match ended.")


async def ai_bat_innings(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Start AI batting innings - user will bowl ball by ball"""
    if user_id not in ai_matches:
        return
    
    match = ai_matches[user_id]
    target = match["target"]
    
    # Add penalty runs if user spammed while batting
    penalty_runs = match.get("ai_penalty_runs", 0)
    if penalty_runs > 0:
        match["ai_score"] += penalty_runs
    
    # Send innings start message
    msg = "🤖 <b>AI IS BATTING NOW!</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"🎯 <b>Target:</b> {target}\n"
    if penalty_runs > 0:
        msg += f"⚠️ <b>AI starts with {penalty_runs} penalty run(s)</b>\n"
    msg += f"📊 <b>AI Score:</b> {match['ai_score']}/{match['ai_wickets']}\n\n"
    msg += "⚾ <b>You're bowling!</b>\n"
    msg += "Send a number (0-6) to bowl!"
    
    try:
        await context.bot.send_photo(
            user_id,
            photo=MEDIA_ASSETS.get("toss"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_message(user_id, msg, parse_mode=ParseMode.HTML)
    
    # Set match phase to waiting for user to bowl
    match["phase"] = "waiting_user"


async def ai_bat_ball(context: ContextTypes.DEFAULT_TYPE, user_id: int, user_bowl: int):
    """AI plays a single ball when user is bowling"""
    if user_id not in ai_matches:
        return
    
    match = ai_matches[user_id]
    difficulty = match["difficulty"]
    target = match["target"]
    
    # AI batting logic - IMPROVED DIFFICULTY
    if difficulty == "easy":
        # Easy: Random but slightly smart
        ai_bat = random.randint(0, 6)
    elif difficulty == "medium":
        # Medium: Strategic batting
        runs_needed = target - match["ai_score"]
        balls_left = (match["overs"] * 6) - match["ai_balls"]
        
        if runs_needed <= 0:  # Already won
            ai_bat = random.randint(1, 6)
        else:
            if balls_left <= 3:  # Last 3 balls - go big
                weights = [0.2, 0.3, 0.5, 0.8, 2, 2.5, 3]
            elif runs_needed > balls_left * 5:  # Need to accelerate
                weights = [0.3, 0.5, 0.8, 1.2, 2.5, 2.8, 3.5]
            elif runs_needed < balls_left * 2:  # Easy chase
                weights = [1, 2, 2.5, 2, 1.5, 0.8, 0.5]
            else:  # Normal chase
                weights = [0.8, 1.5, 2, 2, 1.8, 1.2, 1]
            ai_bat = random.choices([0, 1, 2, 3, 4, 5, 6], weights=weights)[0]
    else:  # hard - SUPER INTELLIGENT AI
        runs_needed = target - match["ai_score"]
        balls_left = (match["overs"] * 6) - match["ai_balls"]
        wickets_left = 1 - match["ai_wickets"]  # Only 1 wicket
        
        if runs_needed <= 0:  # Already won
            ai_bat = random.randint(1, 6)
        else:
            if balls_left <= 2:  # Last 2 balls - ALL OUT ATTACK
                weights = [0.1, 0.2, 0.3, 0.5, 3, 4, 5]
            elif balls_left <= 6:  # Last over - aggressive
                if runs_needed > balls_left * 4:  # Need 4+ per ball
                    weights = [0.1, 0.2, 0.4, 0.8, 3.5, 4, 5]
                elif runs_needed > balls_left * 2:  # Need 2+ per ball
                    weights = [0.3, 0.5, 1, 1.5, 3, 3.5, 4]
                else:  # Comfortable chase
                    weights = [0.5, 2, 3, 2.5, 2, 1, 0.5]
            elif wickets_left == 1:  # Only 1 wicket - play carefully but smart
                required_rr = runs_needed / max(balls_left, 1)
                if required_rr > 3:  # Need big shots
                    weights = [0.2, 0.6, 1.2, 2, 3.5, 4, 4.5]
                elif required_rr > 2:  # Need moderate acceleration
                    weights = [0.5, 1.5, 2.5, 2.8, 3, 2.5, 2]
                elif required_rr < 1:  # Cruise mode
                    weights = [1.5, 3, 3.5, 2.5, 1.5, 0.8, 0.3]
                else:  # Balanced
                    weights = [0.8, 2, 2.8, 2.8, 2.5, 1.8, 1.5]
            else:  # Already out - shouldn't happen but safety
                weights = [1, 1, 1, 1, 1, 1, 1]
            
            # Add randomness to avoid being too predictable
            ai_bat = random.choices([0, 1, 2, 3, 4, 5, 6], weights=weights)[0]
            
            # 15% chance to play an unexpected shot (unpredictability)
            if random.random() < 0.15:
                ai_bat = random.randint(0, 6)
    
    # Process result
    if ai_bat == user_bowl:
        # WICKET! - Only 1 wicket = match over
        match["ai_wickets"] = 1
        match["ai_balls"] += 1
        
        result_msg = f"🎯 <b>WICKET!</b>\n\n"
        result_msg += f"You bowled: {user_bowl} | AI: {ai_bat}\n\n"
        result_msg += f"📊 AI Final: {match['ai_score']}/1 ({match['ai_balls']//6}.{match['ai_balls']%6})\n"
        result_msg += f"🎯 Needed: {target - match['ai_score']} more runs\n"
        
        # AI is out - user wins
        await ai_end_match(context, user_id, "user_won")
        return
    else:
        # Runs!
        match["ai_score"] += ai_bat
        match["ai_balls"] += 1
        match["ai_stats"]["runs"] += ai_bat
        
        result_msg = f"{'🔥' if ai_bat >= 4 else '✅'} <b>AI scored {ai_bat}!</b>\n\n"
        result_msg += f"You bowled: {user_bowl} | AI: {ai_bat}\n\n"
        result_msg += f"📊 AI: {match['ai_score']}/{match['ai_wickets']} ({match['ai_balls']//6}.{match['ai_balls']%6})\n"
        result_msg += f"🎯 Need: {target - match['ai_score']} runs\n"
        
        # Check if target chased
        if match["ai_score"] >= target:
            await ai_end_match(context, user_id, "ai_won")
            return
        
        # Check if overs complete
        if match["ai_balls"] >= match["overs"] * 6:
            await ai_end_match(context, user_id, "user_won")
            return
        
        try:
            # Send appropriate GIF
            if ai_bat == 0:
                gif = random.choice(GIFS[MatchEvent.DOT_BALL])
            elif ai_bat == 1:
                gif = random.choice(GIFS[MatchEvent.RUNS_1])
            elif ai_bat == 2:
                gif = random.choice(GIFS[MatchEvent.RUNS_2])
            elif ai_bat == 3:
                gif = random.choice(GIFS[MatchEvent.RUNS_3])
            elif ai_bat == 4:
                gif = random.choice(GIFS[MatchEvent.RUNS_4])
            elif ai_bat == 5:
                gif = random.choice(GIFS[MatchEvent.RUNS_5])
            else:  # 6
                gif = random.choice(GIFS[MatchEvent.RUNS_6])
            
            await context.bot.send_animation(
                user_id,
                animation=gif,
                caption=result_msg,
                parse_mode=ParseMode.HTML
            )
        except:
            await context.bot.send_message(user_id, result_msg, parse_mode=ParseMode.HTML)
        
        match["phase"] = "waiting_user"
        match["ai_number"] = None
        
        await asyncio.sleep(1)
        await context.bot.send_message(user_id, "⚾ Send your next delivery (0-6)!", parse_mode=ParseMode.HTML)


async def ai_end_match(context: ContextTypes.DEFAULT_TYPE, user_id: int, result: str):
    """End AI match and show results"""
    if user_id not in ai_matches:
        return
    
    match = ai_matches[user_id]
    
    msg = "🏆 <b>MATCH ENDED!</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    if result == "user_won":
        msg += "🎉 <b>YOU WON!</b> 🎉\n\n"
        margin = match["user_score"] - match["ai_score"]
        if match["innings"] == 1:
            msg += f"Victory by {match['user_score'] - match['ai_score']} runs!\n"
        else:
            msg += f"Victory by {10 - match['ai_wickets']} wickets!\n"
        victory_gif = random.choice(GIFS[MatchEvent.VICTORY]) if MatchEvent.VICTORY in GIFS else None
    else:
        msg += "💔 <b>AI WON!</b>\n\n"
        if match["innings"] == 1:
            msg += f"AI won by {10 - match['ai_wickets']} wickets!\n"
        else:
            msg += f"AI won by {match['ai_score'] - match['user_score']} runs!\n"
        victory_gif = None
    
    msg += f"\n📊 <b>YOUR SCORE:</b> {match['user_score']}/{match['user_wickets']} ({match['user_balls']//6}.{match['user_balls']%6})\n"
    msg += f"🤖 <b>AI SCORE:</b> {match['ai_score']}/{match['ai_wickets']} ({match['ai_balls']//6}.{match['ai_balls']%6})\n\n"
    
    # Add batting stats
    if match["user_stats"]["balls"] > 0:
        sr = round((match["user_stats"]["runs"] / match["user_stats"]["balls"]) * 100, 1)
        msg += f"\n🏏 <b>Your Batting:</b>\n"
        msg += f"   Strike Rate: {sr}\n"
        if match["user_stats"]["fours"] > 0 or match["user_stats"]["sixes"] > 0:
            msg += f"   Boundaries: {match['user_stats']['fours']}×4, {match['user_stats']['sixes']}×6\n"
    
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "Use /aistart to play again!"
    
    try:
        # Send victory/defeat GIF
        if result == "user_won" and victory_gif:
            await context.bot.send_animation(
                user_id,
                animation=victory_gif,
                caption=msg,
                parse_mode=ParseMode.HTML
            )
        else:
            # Send photo for defeat or fallback
            await context.bot.send_photo(
                user_id,
                photo=MEDIA_ASSETS.get("scorecard"),
                caption=msg,
                parse_mode=ParseMode.HTML
            )
    except:
        await context.bot.send_message(user_id, msg, parse_mode=ParseMode.HTML)
    
    # Update stats
    if user_id in player_stats:
        # Safely update stats with get() to avoid KeyError
        player_stats[user_id]["total_runs"] = player_stats[user_id].get("total_runs", 0) + match["user_stats"]["runs"]
        player_stats[user_id]["total_balls_faced"] = player_stats[user_id].get("total_balls_faced", 0) + match["user_stats"]["balls"]
        player_stats[user_id]["total_sixes"] = player_stats[user_id].get("total_sixes", 0) + match["user_stats"]["sixes"]
        player_stats[user_id]["total_fours"] = player_stats[user_id].get("total_fours", 0) + match["user_stats"]["fours"]
        player_stats[user_id]["matches_played"] = player_stats[user_id].get("matches_played", 0) + 1
        if result == "user_won":
            player_stats[user_id]["matches_won"] = player_stats[user_id].get("matches_won", 0) + 1
        save_data()
    
    # Remove match
    del ai_matches[user_id]


async def end_innings(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ FIXED: Proper innings end logic with Super Over support
    """
    # ⚡ SUPER OVER HANDLING
    if match.is_super_over:
        if match.innings == 1:
            await end_super_over_innings(context, group_id, match)
            return
        else:
            await determine_super_over_winner(context, group_id, match)
            return
    
    # 🏏 NORMAL MATCH LOGIC
    if match.innings == 1:
        bat_team = match.current_batting_team
        bowl_team = match.current_bowling_team
        
        first_innings_score = bat_team.score
        match.target = first_innings_score + 1
        
        overs_played = max(bat_team.overs, 0.1)
        rr = round(bat_team.score / overs_played, 2)

        msg = "🛑⏸️ <b>INNINGS BREAK</b> ⏸️🛑\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>{bat_team.name}</b>\n"
        msg += f"📊 <b>{bat_team.score}/{bat_team.wickets}</b> ({format_overs(bat_team.balls)})\n"
        msg += f"📈 <b>Run Rate:</b> {rr}\n\n"
        
        active_batters = [p for p in bat_team.players if p.balls_faced > 0 or p.is_out]
        if active_batters:
            top_scorer = max(active_batters, key=lambda p: p.runs)
            msg += f"🌟 <b>Top Scorer:</b> {top_scorer.first_name} - {top_scorer.runs} ({top_scorer.balls_faced})\n"
        
        active_bowlers = [p for p in bowl_team.players if p.balls_bowled > 0]
        if active_bowlers:
            best_bowler = max(active_bowlers, key=lambda p: (p.wickets, -p.runs_conceded))
            msg += f"🥎 <b>Best Bowler:</b> {best_bowler.first_name} - {best_bowler.wickets}/{best_bowler.runs_conceded}\n"
            
        msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🎯 <b>TARGET: {match.target}</b>\n"
        msg += "⏳ <i>Second innings starts in 30 seconds...</i>"
        
        gif_url = get_random_gif(MatchEvent.INNINGS_BREAK)
        try:
            if gif_url:
                await context.bot.send_animation(group_id, animation=gif_url, caption=msg, parse_mode=ParseMode.HTML)
            else:
                await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        await asyncio.sleep(30)
        
        # Start 2nd innings
        match.innings = 2
        match.current_batting_team = match.get_other_team(match.current_batting_team)
        match.current_bowling_team = match.get_other_team(match.current_bowling_team)
        
        match.current_batting_team.current_batsman_idx = None
        match.current_batting_team.current_non_striker_idx = None
        match.current_bowling_team.current_bowler_idx = None
        match.current_batting_team.out_players_indices = set()
        
        chase_team = match.current_batting_team
        runs_needed = match.target
        balls_available = match.total_overs * 6
        rrr = round((runs_needed / balls_available) * 6, 2)
        
        start_msg = "🚀⚔️ <b>THE CHASE BEGINS!</b> ⚔️🚀\n"
        start_msg += "━━━━━━━━━━━━━━━━━━━━━\n"
        start_msg += f"🏏 <b>{chase_team.name}</b> needs to chase.\n\n"
        start_msg += "🧮 <b>WINNING EQUATION:</b>\n"
        start_msg += f"🎯 <b>Need {runs_needed} runs</b>\n"
        start_msg += f"⚾ <b>In {balls_available} balls</b>\n"
        start_msg += f"📉 <b>Required RR: {rrr}</b>\n\n"
        start_msg += "🤞🍀 <i>Good luck to both teams!</i>"
        
        await context.bot.send_message(group_id, start_msg, parse_mode=ParseMode.HTML)
        await asyncio.sleep(2)
        
        match.waiting_for_batsman = True
        match.waiting_for_bowler = False
        
        captain = match.get_captain(chase_team)
        captain_tag = get_user_tag(captain)
        
        msg = f"⚔️🏏 <b>SELECT STRIKER</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"👮‍♂️👉 <b>{captain_tag}</b>, please select the <b>STRIKER</b> first:\n"
        msg += f"⌨️ <b>Command:</b> <code>/batting [serial_number]</code>\n"

        await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
        
        match.batsman_selection_time = time.time()
        match.batsman_selection_task = asyncio.create_task(
            batsman_selection_timeout(context, group_id, match)
        )
        
    else:
        # ✅ Second innings complete - determine winner
        await determine_match_winner(context, group_id, match)


async def determine_match_winner(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ✅ COMPLETE FIX: Proper winner determination with guaranteed message delivery
    """
    logger.info(f"🏆 === DETERMINE WINNER START === Group: {group_id}")
    
    # Set phase immediately to prevent any further ball inputs
    match.phase = GamePhase.MATCH_ENDED
    
    # Get teams
    first = match.batting_first
    second = match.get_other_team(first)
    
    logger.info(f"🏏 First Innings: {first.name} - {first.score}/{first.wickets}")
    logger.info(f"🏏 Second Innings: {second.name} - {second.score}/{second.wickets}")
    logger.info(f"🎯 Target: {match.target}")
    
    winner = None
    loser = None
    margin = ""
    
    # ==========================================
    # 🧮 WINNER CALCULATION (FIXED LOGIC)
    # ==========================================
    
    # Case 1: Second team chased the target
    if second.score >= match.target:
        winner = second
        loser = first
        
        # Calculate wickets remaining
        wickets_lost = second.wickets
        wickets_remaining = len(second.players) - 1 - wickets_lost
        wickets_remaining = max(0, wickets_remaining)
        
        margin = f"{wickets_remaining} Wicket{'s' if wickets_remaining != 1 else ''}"
        logger.info(f"✅ Winner: {winner.name} (Chased target)")
        
    # Case 2: Second team failed to chase
    elif first.score > second.score:
        winner = first
        loser = second
        runs_diff = first.score - second.score
        margin = f"{runs_diff} Run{'s' if runs_diff != 1 else ''}"
        logger.info(f"✅ Winner: {winner.name} (Defended target)")
        
    # Case 3: Tied match
    elif first.score == second.score:
        logger.info("🤝 Match is TIED!")
        
        tie_msg = (
            f"🤝 <b>MATCH TIED!</b> 🤝\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"Both teams scored: <b>{first.score}/{first.wickets}</b>\n\n"
            f"🎲 What a thrilling finish!\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        )
        
        try:
            await context.bot.send_message(group_id, tie_msg, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"❌ Tie message error: {e}")
        
        # Update stats for tie
        try:
            await update_player_stats_after_match(match, None, None)
            save_match_to_history(match, "TIE", "TIE")
        except: pass
        
        # Cleanup
        try:
            if match.main_message_id:
                await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
        except: pass
        
        if group_id in active_matches:
            del active_matches[group_id]
        
        logger.info("✅ Tie match handled successfully")
        return
    
    # ==========================================
    # ✅ VALIDATE WINNER EXISTS
    # ==========================================
    if not winner or not loser:
        logger.error("❌ CRITICAL: Winner/Loser not determined!")
        try:
            await context.bot.send_message(
                group_id,
                "⚠️ <b>Error determining match result.</b>\nPlease contact support.",
                parse_mode=ParseMode.HTML
            )
        except: pass
        
        if group_id in active_matches:
            del active_matches[group_id]
        return
    
    logger.info(f"🏆 FINAL: Winner={winner.name}, Loser={loser.name}, Margin={margin}")
    
    # ==========================================
    # 💾 UPDATE STATS & HISTORY (Non-blocking)
    # ==========================================
    try:
        logger.info("💾 Saving stats...")
        await update_player_stats_after_match(match, winner, loser)
        save_match_to_history(match, winner.name, loser.name)
        update_h2h_stats(match)
        logger.info("✅ Stats saved successfully")
    except Exception as e:
        logger.error(f"❌ Stats save error: {e}")
    
    # ==========================================
    # 🎉 SEND VICTORY MESSAGE (GUARANTEED - 3 ATTEMPTS)
    # ==========================================
    victory_sent = False
    
    for attempt in range(3):
        try:
            logger.info(f"📣 Victory message attempt {attempt + 1}/3...")
            await send_victory_message(context, group_id, match, winner, loser, margin)
            victory_sent = True
            logger.info("✅ Victory message sent successfully")
            break
        except Exception as e:
            logger.error(f"❌ Victory attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(1)
    
    # Ultra fallback if all attempts fail
    if not victory_sent:
        try:
            fallback = f"🏆 {winner.name} WON by {margin}!"
            await context.bot.send_message(group_id, fallback)
            logger.info("✅ Fallback victory message sent")
        except Exception as e:
            logger.error(f"❌ CRITICAL: Even fallback failed: {e}")
    
    await asyncio.sleep(4)
    
    # ==========================================
    # 📋 SEND SCORECARD (With error handling)
    # ==========================================
    try:
        logger.info("📊 Sending scorecard...")
        await send_final_scorecard(context, group_id, match)
        await asyncio.sleep(3)
        logger.info("✅ Scorecard sent")
    except Exception as e:
        logger.error(f"❌ Scorecard error: {e}")
        # Try simple text scorecard
        try:
            simple_card = (
                f"📊 <b>MATCH SUMMARY</b>\n\n"
                f"🧊 {first.name}: {first.score}/{first.wickets}\n"
                f"🔥 {second.name}: {second.score}/{second.wickets}\n\n"
                f"🏆 Winner: {winner.name}"
            )
            await context.bot.send_message(group_id, simple_card, parse_mode=ParseMode.HTML)
        except: pass
    
    # ==========================================
    # 📋 SEND SCORECARD (With error handling)
    # ==========================================
    try:
        logger.info("📊 Sending scorecard...")
        
        # === TEAM MODE SCORECARD IMAGE ===
        if match.game_mode == "TEAM":
            # Note: Ensure 'generate_team_scorecard_image' function is defined
            # If img is a PIL object, you might need to convert it to bytes (bio) first like:
            # bio = io.BytesIO()
            # img.save(bio, 'JPEG')
            # bio.seek(0)
            
            img = generate_team_scorecard_image(match) 
            
            # Agar function direct image object return kar raha hai to use save karke bhejein:
            import io
            bio = io.BytesIO()
            img.save(bio, 'PNG')
            bio.seek(0)

            await context.bot.send_photo(
                chat_id=match.group_id,
                photo=bio,  # img object directly work nahi karega, bio use karein
                parse_mode="HTML"
            )
        # ---------------------------------

        await send_final_scorecard(context, group_id, match) # Existing Text Scorecard
        await asyncio.sleep(3)
        logger.info("✅ Scorecard sent")
    except Exception as e:
        logger.error(f"❌ Scorecard error: {e}")


    # ==========================================
    # 🌟 SEND POTM (With error handling)
    # ==========================================
    try:
        logger.info("🌟 Sending POTM...")
        await send_potm_message(context, group_id, match)
        logger.info("✅ POTM sent")
    except Exception as e:
        logger.error(f"❌ POTM error: {e}")
        # Try simple POTM
        try:
            all_players = first.players + second.players
            best_p = max(all_players, key=lambda p: p.runs + (p.wickets * 20))
            simple_potm = f"🌟 <b>PLAYER OF THE MATCH:</b> {best_p.first_name}"
            await context.bot.send_message(group_id, simple_potm, parse_mode=ParseMode.HTML)
        except: pass

    # ==========================================
    # 🎨 SEND MATCH SUMMARY IMAGE (NEW!)
    # ==========================================
    try:
        logger.info("🎨 Generating match summary image...")
        bio = await generate_team_end_image_v3(match, winner.name, context)
        
        if bio:
            await context.bot.send_photo(
                chat_id=group_id,
                photo=bio,
                caption=f"🏏 {match.group_name} - Match Summary 🏏"
            )
            logger.info("✅ Match summary image sent")
        else:
            logger.warning("⚠️ Match summary image generation returned None")
            
    except Exception as e:
        logger.error(f"❌ Match summary image error: {e}")
    
    await asyncio.sleep(2)

    
    # ==========================================
    # 🧹 CLEANUP
    # ==========================================
    try:
        if match.main_message_id:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
    except: pass
    
    if group_id in active_matches:
        del active_matches[group_id]

    logger.info("🏁 Match ended successfully")
    logger.info(f"🏆 === DETERMINE WINNER END ===\n")

async def testwin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """DEBUG: Force test winner determination"""
    user = update.effective_user
    if user.id != OWNER_ID: return
    
    chat = update.effective_chat
    if chat.id not in active_matches:
        await update.message.reply_text("No active match")
        return
    
    match = active_matches[chat.id]
    
    # Force set phase and call winner determination
    match.phase = GamePhase.MATCH_ENDED
    await determine_match_winner(context, chat.id, match)
    """
    ✅ COMPLETE FIX: Proper winner determination and message delivery
    """
    logger.info(f"🏆 === DETERMINE WINNER START === Group: {group_id}")
    
    # Get teams
    first = match.batting_first
    second = match.get_other_team(first)
    
    logger.info(f"📊 First Innings: {first.name} - {first.score}/{first.wickets}")
    logger.info(f"📊 Second Innings: {second.name} - {second.score}/{second.wickets}")
    logger.info(f"🎯 Target: {match.target}")
    
    winner = None
    loser = None
    margin = ""
    
    # ==========================================
    # 🧮 WINNER CALCULATION (FIXED LOGIC)
    # ==========================================
    
    # Case 1: Second team chased the target
    if second.score >= match.target:
        winner = second
        loser = first
        
        # Calculate wickets remaining
        wickets_lost = second.wickets
        wickets_remaining = len(second.players) - 1 - wickets_lost  # Total - 1 (for partner) - lost
        wickets_remaining = max(0, wickets_remaining)
        
        margin = f"{wickets_remaining} Wicket{'s' if wickets_remaining != 1 else ''}"
        logger.info(f"✅ Winner: {winner.name} (Chased target)")
        
    # Case 2: Second team failed to chase
    elif first.score > second.score:
        winner = first
        loser = second
        runs_diff = first.score - second.score
        margin = f"{runs_diff} Run{'s' if runs_diff != 1 else ''}"
        logger.info(f"✅ Winner: {winner.name} (Defended target)")
        
    # Case 3: Tied match
    elif first.score == second.score:
        logger.info("🤝 Match is TIED!")
        
        tie_msg = (
            f"🤝 <b>MATCH TIED!</b> 🤝\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Both teams scored: <b>{first.score}/{first.wickets}</b>\n\n"
            f"🎲 What a thrilling finish!\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        await context.bot.send_message(group_id, tie_msg, parse_mode=ParseMode.HTML)
        
        # Update stats for tie
        await update_player_stats_after_match(match, None, None)
        save_match_to_history(match, "TIE", "TIE")
        
        # Cleanup
        try:
            if match.main_message_id:
                await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
        except: pass
        
        if group_id in active_matches:
            del active_matches[group_id]
        
        logger.info("✅ Tie match handled successfully")
        return
    
    # ==========================================
    # ✅ VALIDATE WINNER EXISTS
    # ==========================================
    if not winner or not loser:
        logger.error("❌ CRITICAL: Winner/Loser not determined!")
        await context.bot.send_message(
            group_id,
            "⚠️ <b>Error determining match result.</b>\nPlease contact support.",
            parse_mode=ParseMode.HTML
        )
        
        # Force cleanup
        if group_id in active_matches:
            del active_matches[group_id]
        return
    
    logger.info(f"🏆 FINAL: Winner={winner.name}, Loser={loser.name}, Margin={margin}")
    
    # ==========================================
    # 📊 UPDATE STATS & HISTORY
    # ==========================================
    try:
        logger.info("💾 Saving stats...")
        await update_player_stats_after_match(match, winner, loser)
        save_match_to_history(match, winner.name, loser.name)
        update_h2h_stats(match)
        logger.info("✅ Stats saved successfully")
    except Exception as e:
        logger.error(f"❌ Stats save error: {e}")
    
    # ==========================================
    # 🎉 SEND VICTORY MESSAGE (GUARANTEED)
    # ==========================================
    try:
        logger.info("📢 Sending victory message...")
        await send_victory_message(context, group_id, match, winner, loser, margin)
        await asyncio.sleep(4)
        logger.info("✅ Victory message sent")
    except Exception as e:
        logger.error(f"❌ Victory message error: {e}")
        # Fallback simple message
        fallback_msg = (
            f"🏆 <b>{winner.name.upper()} WON!</b> 🏆\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Won by: <b>{margin}</b>\n\n"
            f"📊 {first.name}: {first.score}/{first.wickets}\n"
            f"📊 {second.name}: {second.score}/{second.wickets}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        await context.bot.send_message(group_id, fallback_msg, parse_mode=ParseMode.HTML)
        await asyncio.sleep(3)
    
    # ==========================================
    # 📋 SEND SCORECARD
    # ==========================================
    try:
        logger.info("📊 Sending scorecard...")
        await send_final_scorecard(context, group_id, match)
        await asyncio.sleep(3)
        logger.info("✅ Scorecard sent")
    except Exception as e:
        logger.error(f"❌ Scorecard error: {e}")
    
    # ==========================================
    # 🌟 SEND POTM
    # ==========================================
    try:
        logger.info("⭐ Sending POTM...")
        await send_potm_message(context, group_id, match)
        logger.info("✅ POTM sent")
    except Exception as e:
        logger.error(f"❌ POTM error: {e}")
    
    # ==========================================
    # 🧹 CLEANUP
    # ==========================================
    try:
        if match.main_message_id:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
    except: pass
    
    if group_id in active_matches:
        del active_matches[group_id]
    
    logger.info("🏁 Match ended successfully")
    logger.info(f"🏆 === DETERMINE WINNER END ===\n")

async def send_final_scorecard(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    📊 PREMIUM MATCH SCORECARD - Professional Design
    """
    
    first_innings = match.batting_first
    second_innings = match.get_other_team(first_innings)
    
    # Helper function for batting card
    def format_batting_card(team, innings_num):
        card = f"{'🧊' if team == match.team_x else '🔥'} <b>{team.name.upper()}</b>\n"
        card += f"📊 <b>{team.score}/{team.wickets}</b> ({format_overs(team.balls)} ov)\n"
        
        # Run Rate
        overs = max(team.overs, 0.1)
        rr = round(team.score / overs, 2)
        card += f"📈 Run Rate: <b>{rr}</b>\n"
        card += "━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        # Batting performances
        card += "<b>🏏 BATTING</b>\n"
        card += "<pre>"
        card += "PLAYER           R(B)  SR   4s 6s\n"
        card += "─────────────────────────────\n"
        
        batters = sorted([p for p in team.players if p.balls_faced > 0 or p.is_out], 
                        key=lambda x: x.runs, reverse=True)
        
        for p in batters:
            name = p.first_name[:13].ljust(13)
            runs = str(p.runs).rjust(3)
            balls = str(p.balls_faced).rjust(3)
            sr = str(int(p.get_strike_rate())).rjust(3)
            fours = str(p.boundaries).rjust(2)
            sixes = str(p.sixes).rjust(2)
            status = "*" if not p.is_out else " "
            
            card += f"{name} {runs}({balls}){status} {sr} {fours} {sixes}\n"
        
        card += "</pre>"
        card += f"<b>Extras:</b> {team.extras}\n"
        
        return card
    
    # Helper function for bowling card
    def format_bowling_card(bowling_team, batting_team_name):
        card = f"\n⚾ <b>BOWLING - {bowling_team.name.upper()}</b>\n"
        card += "<pre>"
        card += "BOWLER           O    W/R   ECO\n"
        card += "─────────────────────────────\n"
        
        bowlers = sorted([p for p in bowling_team.players if p.balls_bowled > 0], 
                        key=lambda x: (x.wickets, -x.runs_conceded), reverse=True)
        
        for p in bowlers:
            name = p.first_name[:13].ljust(13)
            overs = format_overs(p.balls_bowled).ljust(4)
            wr = f"{p.wickets}/{p.runs_conceded}".rjust(5)
            econ = str(p.get_economy()).rjust(4)
            
            card += f"{name} {overs} {wr}  {econ}\n"
        
        card += "</pre>\n"
        
        return card
    
    # Build Complete Scorecard
    msg = "📋 <b>OFFICIAL MATCH SCORECARD</b>\n"
    msg += "╔══════════════════════╗\n"
    # Match Info
    msg += f"🏆 <b>Format:</b> {match.total_overs} Overs\n"
    msg += f"🏟 <b>Venue:</b> {match.group_name}\n"
    msg += f"📅 <b>Date:</b> {match.created_at.strftime('%d %b %Y')}\n\n"
    msg += "══════════════════════\n\n"
    
    # First Innings
    msg += "<b>🔹 FIRST INNINGS</b>\n\n"
    msg += format_batting_card(first_innings, 1)
    msg += format_bowling_card(second_innings, first_innings.name)
    
    msg += "\n╠════════════════════╣\n"
    # Second Innings
    msg += "<b>🔸 SECOND INNINGS</b>\n\n"
    msg += format_batting_card(second_innings, 2)
    msg += format_bowling_card(first_innings, second_innings.name)
    msg += "\n╚══════════════════════╝\n\n"
    
    # Match Result
    winner = None
    if second_innings.score >= match.target:
        winner = second_innings
        wickets_remaining = len(winner.players) - 1 - winner.wickets
        margin = f"{wickets_remaining} wicket{'s' if wickets_remaining != 1 else ''}"
    else:
        winner = first_innings
        margin = f"{first_innings.score - second_innings.score} runs"
    
    msg += f"🏆 <b>RESULT:</b> {winner.name} won by {margin}\n\n"
    
    # Key Performances
    all_players = first_innings.players + second_innings.players
    
    # Top Scorer
    top_scorer = max([p for p in all_players if p.balls_faced > 0], 
                    key=lambda x: x.runs, default=None)
    if top_scorer:
        msg += f"⭐ <b>Top Score:</b> {top_scorer.first_name} - {top_scorer.runs}({top_scorer.balls_faced})\n"
    
    # Best Bowler
    best_bowler = max([p for p in all_players if p.balls_bowled > 0], 
                     key=lambda x: (x.wickets, -x.runs_conceded), default=None)
    if best_bowler:
        msg += f"🎯 <b>Best Bowling:</b> {best_bowler.first_name} - {best_bowler.wickets}/{best_bowler.runs_conceded}\n"
    
    try:
        await context.bot.send_photo(
            group_id,
            photo=MEDIA_ASSETS.get("scorecard"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)


async def send_potm_message(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    🌟 PLAYER OF THE MATCH - Enhanced Design
    """
    try:
        all_players = match.batting_first.players + match.bowling_first.players
        if not all_players: 
            return 

        # Calculate MVP Score (Improved Formula)
        best_player = None
        best_score = -1

        for p in all_players:
            # Advanced scoring: Runs + Wickets*25 + SR_bonus + Economy_bonus + Impact
            score = p.runs + (p.wickets * 25)
            
            # Strike Rate Bonus
            if p.balls_faced > 10:
                sr = p.get_strike_rate()
                if sr > 150:
                    score += 20
                elif sr > 120:
                    score += 10
            
            # Economy Bonus
            if p.balls_bowled > 12:
                econ = p.get_economy()
                if econ < 5:
                    score += 20
                elif econ < 7:
                    score += 10
            
            # Boundaries Bonus
            score += (p.sixes * 3) + (p.boundaries * 1)
            
            if score > best_score:
                best_score = score
                best_player = p

        if not best_player: 
            return

        player_tag = get_user_tag(best_player)
        
        # Build Message
        msg = f"🌟 <b>MAN OF THE MATCH</b> 🌟\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        msg += f"👑 <b>{player_tag}</b>\n\n"
        
        # Performance Display
        if best_player.balls_faced > 0:
            sr = best_player.get_strike_rate()
            msg += f"🏏 <b>BATTING</b>\n"
            msg += f"   📊 {best_player.runs} ({best_player.balls_faced})\n"
            msg += f"   ⚡ Strike Rate: {sr}\n"
            if best_player.boundaries > 0 or best_player.sixes > 0:
                msg += f"   💥 {best_player.boundaries}×4, {best_player.sixes}×6\n"
            msg += "\n"
        
        if best_player.balls_bowled > 0:
            econ = best_player.get_economy()
            msg += f"⚾ <b>BOWLING</b>\n"
            msg += f"   🎯 {best_player.wickets} Wickets\n"
            msg += f"   📉 {best_player.runs_conceded} Runs ({format_overs(best_player.balls_bowled)})\n"
            msg += f"   📊 Economy: {econ}\n\n"
        
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "👏 <i>Outstanding Performance!</i>"

        potm_gif = "CgACAgUAAxkBAAKKU2l2fhwchFEPgpitNdXvPqmtJ39LAALeHgACPYW4V0tJxBDGoRGqOAQ"
        
        try:
            await context.bot.send_animation(
                group_id, 
                animation=potm_gif, 
                caption=msg, 
                parse_mode=ParseMode.HTML
            )
        except:
            await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)

    except Exception as e:
        logger.error(f"POTM Error: {e}")


async def send_victory_message(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, winner: Team, loser: Team, margin: str):
    """
    🏆 ENHANCED VICTORY MESSAGE - Clean & Impactful
    """
    
    # Get MVP from each team
    def get_star(team):
        try:
            best = max(team.players, key=lambda p: p.runs + (p.wickets * 25))
            if best.wickets > 0 and best.runs > 10:
                return f"{best.first_name} ({best.wickets}W, {best.runs}R)"
            elif best.wickets > 0:
                return f"{best.first_name} ({best.wickets}W)"
            else:
                return f"{best.first_name} ({best.runs}R)"
        except:
            return "Team Effort"
    
    w_star = get_star(winner)
    l_star = get_star(loser)
    
    # Run rates
    w_rr = round(winner.score / max(winner.overs, 0.1), 2)
    l_rr = round(loser.score / max(loser.overs, 0.1), 2)
    
    msg = f"🏆 <b>{winner.name.upper()} WON!</b> 🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"🎉 <b>Victory Margin:</b> {margin}\n\n"
    
    msg += f"🥇 <b>{winner.name}</b>\n"
    msg += f"   📊 {winner.score}/{winner.wickets} ({format_overs(winner.balls)})\n"
    msg += f"   📈 RR: {w_rr}\n"
    msg += f"   ⭐ Star: {w_star}\n\n"
    
    msg += f"🥈 <b>{loser.name}</b>\n"
    msg += f"   📊 {loser.score}/{loser.wickets} ({format_overs(loser.balls)})\n"
    msg += f"   📈 RR: {l_rr}\n"
    msg += f"   ⭐ Star: {l_star}\n\n"
    
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    
    if "wicket" in margin.lower():
        msg += "💪 <i>Dominant chase! Clinical finish!</i>"
    else:
        msg += "🔥 <i>Solid defense! Pressure bowling!</i>"
    
    # Try sending with photo first
    try:
        await context.bot.send_photo(
            chat_id=group_id,
            photo=MATCH_END_PHOTO,
            caption=msg,
            parse_mode=ParseMode.HTML
        )
        return
    except Exception as e:
        logger.error(f"Error sending match end photo: {e}")
    
    # Fallback to GIF if photo fails
    victory_gif = get_random_gif(MatchEvent.VICTORY)
    try:
        await context.bot.send_animation(
            chat_id=group_id,
            animation=victory_gif,
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        # Final fallback to text
        await context.bot.send_message(
            chat_id=group_id,
            text=msg,
            parse_mode=ParseMode.HTML
        )
async def start_super_over(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    ⚡ SUPER OVER MECHANICS
    - Each team gets 1 over (6 balls)
    - NO DRS
    - Highest score wins
    """
    match.is_super_over = True
    match.phase = GamePhase.SUPER_OVER
    
    # Toss for Super Over
    toss_winner = random.choice([match.team_x, match.team_y])
    toss_loser = match.get_other_team(toss_winner)
    
    # Toss winner chooses bat/bowl
    decision = random.choice(["bat", "bowl"])
    
    if decision == "bat":
        match.super_over_batting_first = toss_winner
        match.super_over_bowling_first = toss_loser
    else:
        match.super_over_batting_first = toss_loser
        match.super_over_bowling_first = toss_winner
    
    # Announcement
    msg = f"⚡💥 <b>SUPER OVER!</b> 💥⚡\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🪙 <b>Toss:</b> {toss_winner.name} won\n"
    msg += f"👉 <b>Decision:</b> {decision.upper()} first\n\n"
    msg += f"🏏 <b>{match.super_over_batting_first.name}</b> will bat\n"
    msg += f"⚾ <b>{match.super_over_bowling_first.name}</b> will bowl\n\n"
    msg += "📜 <b>RULES:</b>\n"
    msg += "🔹 1 Over (6 balls) each\n"
    msg += "🔹 NO DRS available\n"
    msg += "🔹 Highest score wins\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⏳ <i>Starting in 10 seconds...</i>"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    await asyncio.sleep(10)
    
    # Reset teams for Super Over
    await reset_teams_for_super_over(match)
    
    # Start 1st Super Over Innings
    match.innings = 1
    match.total_overs = 1  # Only 1 over
    match.current_batting_team = match.super_over_batting_first
    match.current_bowling_team = match.super_over_bowling_first
    match.target = 0
    
    # Request openers
    match.waiting_for_batsman = True
    match.waiting_for_bowler = False
    
    captain = match.get_captain(match.current_batting_team)
    captain_tag = get_user_tag(captain)
    
    msg = f"⚔️ <b>SELECT STRIKER</b>\n"
    msg += f"👮‍♂️👉 <b>{captain_tag}</b>, select STRIKER:\n"
    msg += f"⌨️ <code>/batting [serial]</code>"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    
    match.batsman_selection_time = time.time()
    match.batsman_selection_task = asyncio.create_task(
        batsman_selection_timeout(context, group_id, match)
    )

async def reset_teams_for_super_over(match: Match):
    """
    🔄 Reset team stats for Super Over
    """
    for team in [match.team_x, match.team_y]:
        team.score = 0
        team.wickets = 0
        team.balls = 0
        team.overs = 0.0
        team.extras = 0
        team.current_batsman_idx = None
        team.current_non_striker_idx = None
        team.current_bowler_idx = None
        team.out_players_indices = set()
        team.bowler_history = []
        team.drs_remaining = 0  # NO DRS in Super Over
        
        # Reset player stats
        for player in team.players:
            player.runs = 0
            player.balls_faced = 0
            player.wickets = 0
            player.balls_bowled = 0
            player.runs_conceded = 0
            player.is_out = False
            player.boundaries = 0
            player.sixes = 0

async def end_super_over_innings(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    🛑 Handle end of Super Over innings
    """
    bat_team = match.current_batting_team
    bowl_team = match.current_bowling_team
    
    first_innings_score = bat_team.score
    match.target = first_innings_score + 1
    
    msg = f"🛑⚡ <b>SUPER OVER - INNINGS BREAK</b> ⚡🛑\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🏏 <b>{bat_team.name}:</b> {bat_team.score}/{bat_team.wickets}\n"
    msg += f"🎯 <b>{bowl_team.name} needs:</b> {match.target} to win\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "⏳ <i>2nd innings in 20 seconds...</i>"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    await asyncio.sleep(20)
    
    # Start 2nd innings
    match.innings = 2
    match.current_batting_team = bowl_team
    match.current_bowling_team = bat_team
    
    # Reset for 2nd inningsS
    await reset_teams_for_super_over(match)
    match.current_batting_team.score = 0
    match.current_batting_team.wickets = 0
    match.current_batting_team.balls = 0
    
    # Request openers
    match.waiting_for_batsman = True
    
    captain = match.get_captain(match.current_batting_team)
    captain_tag = get_user_tag(captain)
    
    msg = f"🔥 <b>SUPER OVER - 2ND INNINGS</b>\n"
    msg += f"👮‍♂️👉 <b>{captain_tag}</b>, select STRIKER:\n"
    msg += f"⌨️ <code>/batting [serial]</code>"
    
    await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    
    match.batsman_selection_time = time.time()
    match.batsman_selection_task = asyncio.create_task(
        batsman_selection_timeout(context, group_id, match)
    )

async def determine_super_over_winner(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """
    🏆 Decide Super Over winner
    """
    first = match.super_over_batting_first
    second = match.get_other_team(first)
    
    if second.score > first.score:
        winner = second
        loser = first
        margin = f"{second.score - first.score} runs"
    elif first.score > second.score:
        winner = first
        loser = second
        wickets_left = len(second.players) - second.wickets - len(second.out_players_indices)
        margin = f"{max(0, wickets_left)} wickets"
    else:
        # Still tied - Boundary count or wickets
        await context.bot.send_message(
            group_id,
            "🤝😱 <b>SUPER OVER ALSO TIED!</b>\n"
            "⚖️ <i>Result will be decided by boundary count...</i>",
            parse_mode=ParseMode.HTML
        )
        await asyncio.sleep(3)
        
        # Count boundaries
        first_boundaries = sum(p.boundaries + p.sixes for p in first.players)
        second_boundaries = sum(p.boundaries + p.sixes for p in second.players)
        
        if second_boundaries > first_boundaries:
            winner = second
            loser = first
            margin = f"{second_boundaries} boundaries"
        else:
            winner = first
            loser = second
            margin = f"{first_boundaries} boundaries"
    
    # Victory Message
    victory_gif = get_random_gif(MatchEvent.VICTORY)
    
    msg = f"🏆✨ <b>SUPER OVER CHAMPION!</b> ✨🏆\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🥇 <b>WINNER: {winner.name}</b>\n"
    msg += f"📊 Won by: {margin}\n\n"
    msg += f"🔥 {first.name}: {first.score}/{first.wickets}\n"
    msg += f"🧊 {second.name}: {second.score}/{second.wickets}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔥 <i>What a thriller! Absolute edge-of-the-seat finish!</i>"
    
    try:
        await context.bot.send_animation(group_id, animation=victory_gif, caption=msg, parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(group_id, msg, parse_mode=ParseMode.HTML)
    
    # Update stats
    try:
        await update_player_stats_after_match(match, winner, loser)
        save_match_to_history(match, winner.name, loser.name)
    except: pass
    
    await asyncio.sleep(3)
    
    # POTM
    try:
        await send_potm_message(context, group_id, match)
    except: pass
    
    # Cleanup
    try:
        if match.main_message_id:
            await context.bot.unpin_chat_message(chat_id=group_id, message_id=match.main_message_id)
    except: pass
    
    if group_id in active_matches:
        del active_matches[group_id]

async def send_match_summary(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match, winner: Team, loser: Team):
    """Send detailed match summary with player statistics"""
    
    # Batting summary for both teams
    summary_text = "Match Summary\n"
    summary_text += "=" * 40 + "\n\n"
    
    # First innings batting
    summary_text += f"{match.batting_first.name} Batting\n"
    summary_text += "-" * 40 + "\n"
    for player in match.batting_first.players:
        if player.balls_faced > 0 or player.is_out:
            status = "out" if player.is_out else "not out"
            sr = player.get_strike_rate()
            summary_text += f"{player.first_name}: {player.runs} ({player.balls_faced}) - {status}"
            if player.boundaries > 0:
                summary_text += f" [{player.boundaries}x4"
                if player.sixes > 0:
                    summary_text += f", {player.sixes}x6"
                summary_text += "]"
            summary_text += f" SR: {sr}\n"
    
    summary_text += f"\nTotal: {match.batting_first.score}/{match.batting_first.wickets}\n"
    summary_text += f"Overs: {format_overs(match.batting_first.balls)}\n"
    summary_text += f"Extras: {match.batting_first.extras}\n\n"
    
    # Second innings batting
    summary_text += f"{match.bowling_first.name} Batting\n"
    summary_text += "-" * 40 + "\n"
    for player in match.bowling_first.players:
        if player.balls_faced > 0 or player.is_out:
            status = "out" if player.is_out else "not out"
            sr = player.get_strike_rate()
            summary_text += f"{player.first_name}: {player.runs} ({player.balls_faced}) - {status}"
            if player.boundaries > 0:
                summary_text += f" [{player.boundaries}x4"
                if player.sixes > 0:
                    summary_text += f", {player.sixes}x6"
                summary_text += "]"
            summary_text += f" SR: {sr}\n"
    
    summary_text += f"\nTotal: {match.bowling_first.score}/{match.bowling_first.wickets}\n"
    summary_text += f"Overs: {format_overs(match.bowling_first.balls)}\n"
    summary_text += f"Extras: {match.bowling_first.extras}\n\n"
    
    await context.bot.send_message(
        chat_id=group_id,
        text=summary_text
    )
    
    await asyncio.sleep(1)
    
    # Bowling summary
    bowling_summary = "Bowling Figures\n"
    bowling_summary += "=" * 40 + "\n\n"
    
    # First innings bowling
    bowling_summary += f"{match.bowling_first.name} Bowling\n"
    bowling_summary += "-" * 40 + "\n"
    for player in match.bowling_first.players:
        if player.balls_bowled > 0:
            overs = format_overs(player.balls_bowled)
            economy = player.get_economy()
            bowling_summary += f"{player.first_name}: {overs} overs, {player.wickets}/{player.runs_conceded}"
            bowling_summary += f" Econ: {economy}"
            if player.maiden_overs > 0:
                bowling_summary += f" M: {player.maiden_overs}"
            bowling_summary += "\n"
    
    bowling_summary += "\n"
    
    # Second innings bowling
    bowling_summary += f"{match.batting_first.name} Bowling\n"
    bowling_summary += "-" * 40 + "\n"
    for player in match.batting_first.players:
        if player.balls_bowled > 0:
            overs = format_overs(player.balls_bowled)
            economy = player.get_economy()
            bowling_summary += f"{player.first_name}: {overs} overs, {player.wickets}/{player.runs_conceded}"
            bowling_summary += f" Econ: {economy}"
            if player.maiden_overs > 0:
                bowling_summary += f" M: {player.maiden_overs}"
            bowling_summary += "\n"
    
    await context.bot.send_message(
        chat_id=group_id,
        text=bowling_summary
    )
    
    await asyncio.sleep(1)
    
    # Player of the Match
    potm_text = "Player of the Match\n"
    potm_text += "=" * 40 + "\n\n"
    
    # Calculate POTM based on performance
    all_players = match.batting_first.players + match.bowling_first.players
    best_player = None
    best_score = 0
    
    for player in all_players:
        # Score calculation: runs + (wickets * 20) + (boundaries * 2)
        performance_score = player.runs + (player.wickets * 20) + (player.boundaries * 2)
        if performance_score > best_score:
            best_score = performance_score
            best_player = player
    
    if best_player:
        potm_text += f"{best_player.first_name}\n\n"
        if best_player.balls_faced > 0:
            potm_text += f"Batting: {best_player.runs} ({best_player.balls_faced}) SR: {best_player.get_strike_rate()}\n"
        if best_player.balls_bowled > 0:
            potm_text += f"Bowling: {best_player.wickets}/{best_player.runs_conceded} Econ: {best_player.get_economy()}\n"
    
    await context.bot.send_message(
        chat_id=group_id,
        text=potm_text
    )

async def update_player_stats_after_match(match: Match, winner: Team, loser: Team):
    """Update global player statistics after match - FIXED"""
    all_players = match.batting_first.players + match.bowling_first.players
    
    for player in all_players:
        user_id = player.user_id
        
        # Initialize if needed
        if user_id not in player_stats:
            init_player_stats(user_id)
        
        stats = player_stats[user_id]
        
        # Update match count
        stats["matches_played"] += 1
        
        # Check if winner (handle tied match)
        if winner:
            is_winner = (player in winner.players)
            if is_winner:
                stats["matches_won"] += 1
        
        # Update batting stats
        if player.balls_faced > 0:
            stats["total_runs"] += player.runs
            stats["total_balls_faced"] += player.balls_faced
            stats["dot_balls_faced"] += player.dot_balls_faced
            stats["boundaries"] += getattr(player, 'boundaries', 0)
            stats["sixes"] += getattr(player, 'sixes', 0)
            
            # Check for century/half-century
            if player.runs >= 100:
                stats["centuries"] += 1
            elif player.runs >= 50:
                stats["half_centuries"] += 1
            
            # Update highest score
            if player.runs > stats["highest_score"]:
                stats["highest_score"] = player.runs
            
            # Check for duck
            if player.runs == 0 and player.is_out:
                stats["ducks"] += 1
            
            # Update last 5 scores
            stats["last_5_scores"].append(player.runs)
            if len(stats["last_5_scores"]) > 5:
                stats["last_5_scores"].pop(0)
        
        # Update bowling stats
        if player.balls_bowled > 0:
            stats["total_wickets"] += player.wickets
            stats["total_balls_bowled"] += player.balls_bowled
            stats["total_runs_conceded"] += player.runs_conceded
            stats["dot_balls_bowled"] += player.dot_balls_bowled
            stats["total_no_balls"] += player.no_balls
            stats["total_wides"] += player.wides
            
            # Update best bowling
            if player.wickets > stats["best_bowling"]["wickets"]:
                stats["best_bowling"]["wickets"] = player.wickets
                stats["best_bowling"]["runs"] = player.runs_conceded
            elif player.wickets == stats["best_bowling"]["wickets"] and player.runs_conceded < stats["best_bowling"]["runs"]:
                stats["best_bowling"]["runs"] = player.runs_conceded
            
            # Update last 5 wickets
            stats["last_5_wickets"].append(player.wickets)
            if len(stats["last_5_wickets"]) > 5:
                stats["last_5_wickets"].pop(0)
        
        # Update timeouts
        stats["total_timeouts"] += player.batting_timeouts + player.bowling_timeouts
    
    # Save to disk
    save_data()


def update_h2h_stats(match: Match):
    """
    Update Head-to-Head stats using REAL match data
    """

    all_players = []

    all_players.extend(match.team_x.players)
    all_players.extend(match.team_y.players)

    for p1 in all_players:
        init_player_stats(p1.user_id)

        for p2 in all_players:
            if p1.user_id == p2.user_id:
                continue

            vs = player_stats[p1.user_id].setdefault("vs_player_stats", {})
            record = vs.setdefault(str(p2.user_id), {
                "matches": 0,
                "runs_scored": 0,
                "balls_faced": 0,
                "dismissals": 0,
                "wickets_taken": 0
            })

            # Played together in same match
            record["matches"] += 1

            # Batting vs opponent
            record["runs_scored"] += p1.runs
            record["balls_faced"] += p1.balls_faced

            # If p1 got out & p2 was bowler
            if p1.is_out and match.current_bowling_team.get_player(p2.user_id):
                record["dismissals"] += 1

            # Bowling vs opponent
            record["wickets_taken"] += p1.wickets

    save_data()


def check_achievements(player: Player):
    """Check and award achievements to player"""
    user_id = player.user_id
    stats = player_stats.get(user_id)
    
    if not stats:
        return
    
    if user_id not in achievements:
        achievements[user_id] = []
    
    user_achievements = achievements[user_id]
    
    # Century Maker
    if stats["centuries"] >= 1 and "Century Maker" not in user_achievements:
        user_achievements.append("Century Maker")
    
    # Hat-trick Hero (3 wickets in match)
    if player.wickets >= 3 and "Hat-trick Hero" not in user_achievements:
        user_achievements.append("Hat-trick Hero")
    
    # Diamond Hands (50+ matches)
    if stats["matches_played"] >= 50 and "Diamond Hands" not in user_achievements:
        user_achievements.append("Diamond Hands")
    
    # Speed Demon (Strike Rate > 200 in a match with 10+ runs)
    if player.runs >= 10 and player.get_strike_rate() > 200 and "Speed Demon" not in user_achievements:
        user_achievements.append("Speed Demon")
    
    # Economical (Economy < 5 in a match with 12+ balls bowled)
    if player.balls_bowled >= 12 and player.get_economy() < 5 and "Economical" not in user_achievements:
        user_achievements.append("Economical")

async def save_match_to_history(match, winner_team: str):
    """Save team match stats to database"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Determine scores
    team_x_score = match.team_x_runs
    team_y_score = match.team_y_runs
    
    # Save match history
    c.execute("""
        INSERT INTO match_history 
        (group_id, match_type, winner_team, runner_up_team, total_overs, 
         team_x_score, team_y_score, player_of_match)
        VALUES (?, 'TEAM', ?, ?, ?, ?, ?, ?)
    """, (match.group_id, winner_team, 
          'Team Y' if winner_team == 'Team X' else 'Team X',
          match.total_overs, team_x_score, team_y_score, 
          match.player_of_match))
    
    # Update player stats for both teams
    all_players = list(match.team_x_players.keys()) + list(match.team_y_players.keys())
    
    for user_id in all_players:
        player = match.players.get(user_id)
        if not player:
            continue
        
        username = player.get('username', 'Unknown')
        first_name = player.get('first_name', 'Player')
        
        batting_stats = match.batting_stats.get(user_id, {})
        bowling_stats = match.bowling_stats.get(user_id, {})
        
        runs = batting_stats.get('runs', 0)
        balls_faced = batting_stats.get('balls', 0)
        wickets = bowling_stats.get('wickets', 0)
        balls_bowled = bowling_stats.get('balls', 0)
        runs_conceded = bowling_stats.get('runs', 0)
        
        # Check if user exists
        c.execute("SELECT user_id FROM user_stats WHERE user_id = ?", (user_id,))
        exists = c.fetchone()
        
        # Determine if player's team won
        player_team = 'X' if user_id in match.team_x_players else 'Y'
        won = 1 if (player_team == 'X' and winner_team == 'Team X') or \
                   (player_team == 'Y' and winner_team == 'Team Y') else 0
        
        if not exists:
            c.execute("""
                INSERT INTO user_stats 
                (user_id, username, first_name, total_runs, total_balls_faced,
                 total_wickets, total_balls_bowled, total_runs_conceded,
                 matches_played, matches_won, highest_score)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """, (user_id, username, first_name, runs, balls_faced, 
                  wickets, balls_bowled, runs_conceded, won, runs))
        else:
            c.execute("""
                UPDATE user_stats 
                SET total_runs = total_runs + ?,
                    total_balls_faced = total_balls_faced + ?,
                    total_wickets = total_wickets + ?,
                    total_balls_bowled = total_balls_bowled + ?,
                    total_runs_conceded = total_runs_conceded + ?,
                    matches_played = matches_played + 1,
                    matches_won = matches_won + ?,
                    highest_score = MAX(highest_score, ?),
                    username = ?,
                    first_name = ?
                WHERE user_id = ?
            """, (runs, balls_faced, wickets, balls_bowled, runs_conceded,
                  won, runs, username, first_name, user_id))
        
        # Update fifties/hundreds
        if runs >= 100:
            c.execute("UPDATE user_stats SET total_hundreds = total_hundreds + 1 WHERE user_id = ?", (user_id,))
        elif runs >= 50:
            c.execute("UPDATE user_stats SET total_fifties = total_fifties + 1 WHERE user_id = ?", (user_id,))
        
        # Update bowling best figures
        if wickets > 0:
            c.execute("""
                UPDATE user_stats 
                SET best_bowling_wickets = MAX(best_bowling_wickets, ?),
                    best_bowling_runs = CASE 
                        WHEN ? > best_bowling_wickets THEN ?
                        WHEN ? = best_bowling_wickets THEN MIN(best_bowling_runs, ?)
                        ELSE best_bowling_runs
                    END
                WHERE user_id = ?
            """, (wickets, wickets, runs_conceded, wickets, runs_conceded, user_id))
            
            if wickets >= 5:
                c.execute("UPDATE user_stats SET five_wicket_hauls = five_wicket_hauls + 1 WHERE user_id = ?", (user_id,))
    
    # Update player of match
    if match.player_of_match:
        c.execute("UPDATE user_stats SET player_of_match = player_of_match + 1 WHERE user_id = ?", 
                  (match.player_of_match,))
    
    conn.commit()
    conn.close()

async def mystats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Interactive cricket stats with Team/Solo separation"""
    query = update.callback_query
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name
    
    # Show main menu with buttons
    if query is None:
        # Initial command call
        keyboard = [
            [
                InlineKeyboardButton("📊 Overall Stats", callback_data=f"mystats_overall_{user_id}"),
                InlineKeyboardButton("👥 Team Stats", callback_data=f"mystats_team_{user_id}")
            ],
            [
                InlineKeyboardButton("🎮 Solo Stats", callback_data=f"mystats_solo_{user_id}"),
                InlineKeyboardButton("🏆 Achievements", callback_data=f"mystats_achievements_{user_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = f"╔═══════════════════════╗\n"
        text += f"    🏏 <b>{user_name}'s CRICKET PROFILE</b>\n"
        text += f"╚═══════════════════════╝\n\n"
        text += "📈 <b>Select a category to view detailed stats:</b>\n\n"
        text += "📊 <b>Overall Stats</b> - Complete career statistics\n"
        text += "👥 <b>Team Stats</b> - Team mode performance\n"
        text += "🎮 <b>Solo Stats</b> - Solo mode achievements\n"
        text += "🏆 <b>Achievements</b> - Milestones & records\n"
        
        try:
            await update.message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup
            )
        except:
            await update.message.reply_text("🏏 Error loading stats. Try again!")
    else:
        await query.answer()

async def mystats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle mystats button callbacks"""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = int(data.split("_")[-1])
    
    # Security: Only user can view their own stats
    if query.from_user.id != user_id:
        await query.answer("❌ You can only view your own stats!", show_alert=True)
        return
    
    user_name = query.from_user.first_name
    
    # Back button
    back_button = InlineKeyboardButton("🔙 Back to Menu", callback_data=f"mystats_menu_{user_id}")
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    if "mystats_overall" in data:
        # Overall Stats
        c.execute("""
            SELECT 
                matches_played, matches_won, total_runs, highest_score,
                total_balls_faced, total_wickets, total_sixes, total_fours,
                total_dots, best_bowling, total_ducks, total_fifties, 
                total_hundreds
            FROM user_stats
            WHERE user_id = ?
        """, (user_id,))
        
        row = c.fetchone()
        conn.close()
        
        if not row or row[0] == 0:
            text = "🏏 <b>No stats yet!</b>\n\nPlay some matches to build your cricket profile."
        else:
            matches, won, runs, high_score, balls, wickets, sixes, fours, dots, best_bowl, ducks, fifties, hundreds = row
            
            avg = round(runs / matches, 2) if matches > 0 else 0
            sr = round((runs / balls * 100), 2) if balls > 0 else 0
            win_rate = round((won / matches * 100), 1) if matches > 0 else 0
            economy = round((runs / (balls / 6)), 2) if balls > 0 else 0
            
            text = f"╔═══════════════════════╗\n"
            text += f"    📊 <b>{user_name}'s OVERALL STATS</b>\n"
            text += f"╚═══════════════════════╝\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "📈 <b>MATCH RECORD</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🎮 Matches Played: <b>{matches}</b>\n"
            text += f"🏆 Matches Won: <b>{won}</b>\n"
            text += f"💔 Matches Lost: <b>{matches - won}</b>\n"
            text += f"📊 Win Rate: <b>{win_rate}%</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "🏏 <b>BATTING PERFORMANCE</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🏃 Total Runs: <b>{runs}</b>\n"
            text += f"⏱️ Balls Faced: <b>{balls}</b>\n"
            text += f"📊 Average: <b>{avg}</b>\n"
            text += f"⚡ Strike Rate: <b>{sr}</b>\n"
            text += f"🎯 High Score: <b>{high_score}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "🎯 <b>MILESTONES</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"💯 Centuries: <b>{hundreds}</b>\n"
            text += f"5️⃣0️⃣ Half-Centuries: <b>{fifties}</b>\n"
            text += f"🦆 Ducks: <b>{ducks}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "💥 <b>BOUNDARIES</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"6️⃣ Sixes: <b>{sixes}</b>\n"
            text += f"4️⃣ Fours: <b>{fours}</b>\n"
            text += f"⚪ Dot Balls: <b>{dots}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "⚾ <b>BOWLING STATS</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🎯 Total Wickets: <b>{wickets}</b>\n"
            text += f"🏆 Best Bowling: <b>{best_bowl}</b>\n"
            text += f"📊 Economy: <b>{economy}</b>\n"
        
        keyboard = [[back_button]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except:
            await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    
    elif "mystats_team" in data:
        # Team Stats
        if user_id not in player_stats or "team" not in player_stats[user_id]:
            text = "🏏 <b>No Team stats yet!</b>\n\nPlay team matches to see your stats here."
        else:
            team_stats = player_stats[user_id]["team"]
            
            matches = team_stats.get("matches", 0)
            wins = team_stats.get("wins", 0)
            runs = team_stats.get("runs", 0)
            balls = team_stats.get("balls", 0)
            wickets = team_stats.get("wickets", 0)
            high_score = team_stats.get("high_score", 0)
            sixes = team_stats.get("sixes", 0)
            fours = team_stats.get("fours", 0)
            
            avg = round(runs / matches, 2) if matches > 0 else 0
            sr = round((runs / balls * 100), 2) if balls > 0 else 0
            win_rate = round((wins / matches * 100), 1) if matches > 0 else 0
            
            text = f"╔═══════════════════════╗\n"
            text += f"    👥 <b>{user_name}'s TEAM STATS</b>\n"
            text += f"╚═══════════════════════╝\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "📈 <b>TEAM MODE RECORD</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🎮 Team Matches: <b>{matches}</b>\n"
            text += f"🏆 Wins: <b>{wins}</b>\n"
            text += f"📊 Win Rate: <b>{win_rate}%</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "🏏 <b>BATTING IN TEAMS</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🏃 Total Runs: <b>{runs}</b>\n"
            text += f"⏱️ Balls Faced: <b>{balls}</b>\n"
            text += f"📊 Average: <b>{avg}</b>\n"
            text += f"⚡ Strike Rate: <b>{sr}</b>\n"
            text += f"🎯 High Score: <b>{high_score}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "💥 <b>TEAM MODE BOUNDARIES</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"6️⃣ Sixes: <b>{sixes}</b>\n"
            text += f"4️⃣ Fours: <b>{fours}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "⚾ <b>TEAM BOWLING</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🎯 Wickets: <b>{wickets}</b>\n"
        
        conn.close()
        keyboard = [[back_button]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except:
            await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    
    elif "mystats_solo" in data:
        # Solo Stats
        if user_id not in player_stats or "solo" not in player_stats[user_id]:
            text = "🏏 <b>No Solo stats yet!</b>\n\nPlay solo matches to see your stats here."
        else:
            solo_stats = player_stats[user_id]["solo"]
            
            matches = solo_stats.get("matches", 0)
            wins = solo_stats.get("wins", 0)
            runs = solo_stats.get("runs", 0)
            balls = solo_stats.get("balls", 0)
            high_score = solo_stats.get("high_score", 0)
            sixes = solo_stats.get("sixes", 0)
            fours = solo_stats.get("fours", 0)
            
            avg = round(runs / matches, 2) if matches > 0 else 0
            sr = round((runs / balls * 100), 2) if balls > 0 else 0
            win_rate = round((wins / matches * 100), 1) if matches > 0 else 0
            
            text = f"╔═══════════════════════╗\n"
            text += f"    🎮 <b>{user_name}'s SOLO STATS</b>\n"
            text += f"╚═══════════════════════╝\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "📈 <b>SOLO MODE RECORD</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🎮 Solo Matches: <b>{matches}</b>\n"
            text += f"🏆 Wins: <b>{wins}</b>\n"
            text += f"💔 Losses: <b>{matches - wins}</b>\n"
            text += f"📊 Win Rate: <b>{win_rate}%</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "🏏 <b>SOLO BATTING</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🏃 Total Runs: <b>{runs}</b>\n"
            text += f"⏱️ Balls Faced: <b>{balls}</b>\n"
            text += f"📊 Average: <b>{avg}</b>\n"
            text += f"⚡ Strike Rate: <b>{sr}</b>\n"
            text += f"🎯 High Score: <b>{high_score}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "💥 <b>SOLO BOUNDARIES</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"6️⃣ Sixes: <b>{sixes}</b>\n"
            text += f"4️⃣ Fours: <b>{fours}</b>\n"
        
        conn.close()
        keyboard = [[back_button]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except:
            await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    
    elif "mystats_achievements" in data:
        # Achievements
        c.execute("""
            SELECT 
                total_hundreds, total_fifties, total_ducks, total_sixes, 
                total_fours, best_bowling, matches_played, matches_won
            FROM user_stats
            WHERE user_id = ?
        """, (user_id,))
        
        row = c.fetchone()
        conn.close()
        
        if not row:
            text = "🏏 <b>No achievements yet!</b>\n\nPlay matches to unlock achievements."
        else:
            hundreds, fifties, ducks, sixes, fours, best_bowl, matches, wins = row
            
            text = f"╔═══════════════════════╗\n"
            text += f"    🏆 <b>{user_name}'s ACHIEVEMENTS</b>\n"
            text += f"╚═══════════════════════╝\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "🎯 <b>BATTING MILESTONES</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"💯 Centuries: <b>{hundreds}</b>\n"
            text += f"5️⃣0️⃣ Half-Centuries: <b>{fifties}</b>\n"
            text += f"🦆 Ducks: <b>{ducks}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "💥 <b>POWER HITTING</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"6️⃣ Total Sixes: <b>{sixes}</b>\n"
            text += f"4️⃣ Total Fours: <b>{fours}</b>\n"
            
            # Power hitter badge
            if sixes >= 100:
                text += f"\n🔥 <b>POWER HITTER</b> Badge Unlocked!\n"
            elif sixes >= 50:
                text += f"\n💪 <b>BIG HITTER</b> Badge Unlocked!\n"
            
            text += "\n━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "⚾ <b>BOWLING RECORDS</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += f"🏆 Best Bowling: <b>{best_bowl}</b>\n\n"
            
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            text += "🎖️ <b>OVERALL ACHIEVEMENTS</b>\n"
            text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
            
            # Badges based on matches
            if matches >= 100:
                text += "🏅 <b>VETERAN PLAYER</b> - 100+ Matches\n"
            elif matches >= 50:
                text += "🥈 <b>EXPERIENCED</b> - 50+ Matches\n"
            elif matches >= 10:
                text += "🥉 <b>ROOKIE</b> - 10+ Matches\n"
            
            # Win rate badges
            win_rate = (wins / matches * 100) if matches > 0 else 0
            if win_rate >= 70:
                text += "👑 <b>CHAMPION</b> - 70%+ Win Rate\n"
            elif win_rate >= 50:
                text += "⭐ <b>WINNER</b> - 50%+ Win Rate\n"
        
        keyboard = [[back_button]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except:
            await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
    
    elif "mystats_menu" in data:
        # Back to main menu
        keyboard = [
            [
                InlineKeyboardButton("📊 Overall Stats", callback_data=f"mystats_overall_{user_id}"),
                InlineKeyboardButton("👥 Team Stats", callback_data=f"mystats_team_{user_id}")
            ],
            [
                InlineKeyboardButton("🎮 Solo Stats", callback_data=f"mystats_solo_{user_id}"),
                InlineKeyboardButton("🏆 Achievements", callback_data=f"mystats_achievements_{user_id}")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = f"╔═══════════════════════╗\n"
        text += f"    🏏 <b>{user_name}'s CRICKET PROFILE</b>\n"
        text += f"╚═══════════════════════╝\n\n"
        text += "📈 <b>Select a category to view detailed stats:</b>\n\n"
        text += "📊 <b>Overall Stats</b> - Complete career statistics\n"
        text += "👥 <b>Team Stats</b> - Team mode performance\n"
        text += "🎮 <b>Solo Stats</b> - Solo mode achievements\n"
        text += "🏆 <b>Achievements</b> - Milestones & records\n"
        
        try:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)
        except:
            await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)

async def groupapprove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🔐 Approve Tournament Mode (Owner / Second Approver)"""
    user = update.effective_user
    # 🔒 Access Control: Only Authorized Approvers
    if user.id not in [OWNER_ID, SECOND_APPROVER_ID]:
        return
    
    chat = update.effective_chat
    
    if chat.type == "private":
        if not context.args:
            return
        
        try:
            group_id = int(context.args[0])
        except:
            return
    else:
        group_id = chat.id
    
    # Add to approved list
    TOURNAMENT_APPROVED_GROUPS.add(group_id)
    
    # Save to database
    try:
        group_name = (await context.bot.get_chat(group_id)).title
    except:
        group_name = "Unknown Group"
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO tournament_groups (group_id, group_name) VALUES (?, ?)', 
              (group_id, group_name))
    conn.commit()
    conn.close()
    
    save_data()
    
    # Try to notify the group
    try:
        await context.bot.send_animation(
            group_id,
            animation=GIFS.get("tournament_approved"),
            caption=(
                "✅ <b>TOURNAMENT MODE ACTIVATED!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🎯 <b>This group can now use auction/tournament features.</b>\n\n"
                "📋 <b>Get Started:</b>\n"
                "Use /game and select 'Tournament Mode' to start.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━"
            ),
            parse_mode=ParseMode.HTML
        )
    except:
        pass
    
    await update.message.reply_text(
        f"✅ <b>TOURNAMENT APPROVED!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>Group ID:</b> <code>{group_id}</code>\n"
        f"✨ <b>Status:</b> Tournament mode is now available.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def unapprove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🚫 Owner removes tournament approval"""
    user = update.effective_user
    if user.id != OWNER_ID:
        return
    
    chat = update.effective_chat
    
    if chat.type == "private":
        if not context.args:
            await update.message.reply_text(
                "📋 <b>USAGE:</b>\n"
                "<code>/unapprove [group_id]</code>",
                parse_mode=ParseMode.HTML
            )
            return
        group_id = int(context.args[0])
    else:
        group_id = chat.id
    
    TOURNAMENT_APPROVED_GROUPS.discard(group_id)
    
    # Remove from database
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('DELETE FROM tournament_groups WHERE group_id = ?', (group_id,))
    conn.commit()
    conn.close()
    
    save_data()
    
    await update.message.reply_text(
        f"❌ <b>TOURNAMENT UNAPPROVED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🆔 <b>Group ID:</b> <code>{group_id}</code>\n"
        f"⚠️ <b>Status:</b> Tournament access revoked.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def start_auction_live_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎯 Start auction live callback"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    
    if chat.id not in active_auctions:
        await query.answer("❌ No active auction!", show_alert=True)
        return
    
    auction = active_auctions[chat.id]
    
    # Validate before starting
    if not auction.auctioneer_id:
        await query.answer("⚠️ Please select an auctioneer first!", show_alert=True)
        return
    
    if len(auction.teams) < 2:
        await query.answer("⚠️ Need at least 2 teams!", show_alert=True)
        return
    
    if len(auction.player_pool) == 0:
        await query.answer("⚠️ Add players first using /aucplayer!", show_alert=True)
        return
    
    # Start auction
    auction.phase = AuctionPhase.AUCTION_LIVE
    
    await query.edit_message_text(
        text=(
            "✅ <b>AUCTION IS NOW LIVE!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🎯 Participants can start bidding.\n"
            "⏳ First player coming up...\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode=ParseMode.HTML
    )
    
    # Start bringing players
    await asyncio.sleep(2)
    await bring_next_player(context, chat.id, auction)

async def become_auctioneer_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎤 Set auctioneer"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    user = query.from_user
    
    if chat.id not in active_auctions:
        return
    
    auction = active_auctions[chat.id]
    auction.auctioneer_id = user.id
    auction.auctioneer_name = user.first_name
    
    user_tag = get_user_tag(user)
    
    await query.message.edit_caption(
        caption=(
            f"✅ <b>AUCTIONEER LOCKED!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🎤 <b>Auctioneer:</b> {user_tag}\n"
            f"🎭 <b>Role:</b> Host & Moderator\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>NEXT STEPS:</b>\n\n"
            f"<b>1️⃣ Assign Bidders:</b>\n"
            f"   <code>/bidder [TeamName]</code>\n\n"
            f"<b>2️⃣ Add Players:</b>\n"
            f"   <code>/aucplayer</code>\n\n"
            f"<b>3️⃣ Start Auction:</b>\n"
            f"   <code>/startauction</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        parse_mode=ParseMode.HTML
    )

async def unsold_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """❌ List unsold players"""
    chat = update.effective_chat
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first with /auction",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    if not auction.unsold_players:
        await update.message.reply_text(
            "✅ <b>NO UNSOLD PLAYERS YET!</b>\n\n"
            "All players have been sold so far.",
            parse_mode=ParseMode.HTML
        )
        return
    msg = "❌ <b>UNSOLD PLAYERS</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for i, player_data in enumerate(auction.unsold_players, 1):
        name = player_data if isinstance(player_data, str) else player_data.get('player_name', 'Unknown')
        msg += f"{i}. {name}\n"
    msg += f"\n━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📊 <b>Total Unsold:</b> {len(auction.unsold_players)}"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def bring_back_unsold_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🔄 Bring back unsold players"""
    query = update.callback_query
    chat = query.message.chat
    
    if chat.id not in active_auctions:
        await query.answer("❌ No active auction!", show_alert=True)
        return
        
    auction = active_auctions[chat.id]
    
    if not auction.unsold_players:
        await query.answer("✅ No unsold players to bring back!", show_alert=True)
        return
    
    # Move unsold back to player pool
    for player in auction.unsold_players:
        if isinstance(player, dict):
            auction.player_pool.append(player)
    
    count = len(auction.unsold_players)
    auction.unsold_players.clear()
    
    await query.answer(f"✅ {count} players added back to pool!", show_alert=True)
    await query.message.edit_text(
        f"🔄 <b>UNSOLD PLAYERS RESTORED!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>Players Added Back:</b> {count}\n"
        f"📊 <b>Total Pool Size:</b> {len(auction.player_pool)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """⏸ Pause the auction timer"""
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text("🚫 No active auction!", parse_mode=ParseMode.HTML)
        return
        
    auction = active_auctions[chat.id]
    
    if user.id != auction.auctioneer_id and user.id != auction.host_id:
        await update.message.reply_text(
            "🚧 <b>ACCESS DENIED!</b>\n\n"
            "Only Auctioneer/Host can pause the auction.",
            parse_mode=ParseMode.HTML
        )
        return
    if auction.bid_timer_task:
        auction.bid_timer_task.cancel()
        auction.bid_timer_task = None
        
        await update.message.reply_text(
            "⏸ <b>AUCTION PAUSED!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⏱ Timer stopped.\n"
            "▶️ Use /resume to continue.\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )

async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """▶️ Resume the auction"""
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text("🚫 No active auction!", parse_mode=ParseMode.HTML)
        return
        
    auction = active_auctions[chat.id]
    
    if user.id != auction.auctioneer_id and user.id != auction.host_id:
        await update.message.reply_text(
            "🚧 <b>ACCESS DENIED!</b>\n\n"
            "Only Auctioneer/Host can resume the auction.",
            parse_mode=ParseMode.HTML
        )
        return
    if not auction.bid_timer_task and auction.phase == AuctionPhase.AUCTION_LIVE:
        auction.bid_end_time = time.time() + 30
        auction.bid_timer_task = asyncio.create_task(bid_timer(context, chat.id, auction))
        
        await update.message.reply_text(
            "▶️ <b>AUCTION RESUMED!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⏱ Timer: 30 seconds\n"
            "💰 Bidding is now active!\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )

async def cancelbid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🔄 Cancel last bid"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_auctions:
        await update.message.reply_text("🚫 No active auction!", parse_mode=ParseMode.HTML)
        return
    auction = active_auctions[chat.id]
    # Permission check
    if user.id != auction.auctioneer_id and user.id != auction.host_id:
        await update.message.reply_text(
            "🚧 <b>ACCESS DENIED!</b>\n\n"
            "Only Auctioneer can cancel bids!",
            parse_mode=ParseMode.HTML
        )
        return
    # Must be in live bidding phase
    if auction.phase != AuctionPhase.AUCTION_LIVE:
        await update.message.reply_text(
            "⚠️ <b>NO LIVE BIDDING!</b>\n\n"
            "Cannot cancel bids outside live auction.",
            parse_mode=ParseMode.HTML
        )
        return
    # Reset bid state
    old_bid = auction.current_highest_bid
    auction.current_highest_bid = auction.current_base_price
    auction.current_highest_bidder = None
    # Reset bid timer
    if auction.bid_timer_task:
        auction.bid_timer_task.cancel()
    auction.bid_end_time = time.time() + 30
    auction.bid_timer_task = asyncio.create_task(bid_timer(context, chat.id, auction))
    await update.message.reply_text(
        f"🔄 <b>LAST BID CANCELLED!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"❌ <b>Cancelled Bid:</b> {old_bid}\n"
        f"💰 <b>Reset to Base:</b> {auction.current_base_price}\n"
        f"⏱ <b>Timer Reset:</b> 30s\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def wallet_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """💰 Show remaining purse"""
    chat = update.effective_chat
    if chat.id not in active_auctions:
        await update.message.reply_text("🚫 No active auction!", parse_mode=ParseMode.HTML)
        return
    auction = active_auctions[chat.id]
    msg = "💰 <b>TEAM WALLETS</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    sorted_teams = sorted(
        auction.teams.values(),
        key=lambda x: x.purse_remaining,
        reverse=True
    )
    for i, team in enumerate(sorted_teams, 1):
        emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else "🏏"
        spent = 1000 - team.purse_remaining
        
        msg += f"{emoji} <b>{team.name}</b>\n"
        msg += f"   💰 Purse: <b>{team.purse_remaining}</b>\n"
        msg += f"   💸 Spent: {spent}\n"
        msg += f"   👥 Players: {len(team.players)}\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def auction_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎪 Start auction - ULTRA PREMIUM EDITION"""
    chat = update.effective_chat
    user = update.effective_user
    
    # ✅ Check if tournament approved
    if chat.id not in TOURNAMENT_APPROVED_GROUPS:
        keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data="back_to_modes")]]
        
        try:
            await update.message.reply_animation(
                animation=GIFS.get("tournament_locked"),
                caption=(
                    "🚫 <b>TOURNAMENT MODE LOCKED!</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "⚠️ This group doesn't have tournament access yet.\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "📞 <b>TO ENABLE TOURNAMENT MODE:</b>\n\n"
                    f"Contact Bot Owner:\n"
                    f"<a href='tg://user?id={OWNER_ID}'>🔗 Click Here to Contact</a>\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "💡 <b>Owner Command:</b>\n"
                    f"<code>/groupapprove {chat.id}</code>\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━"
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
        except:
            await update.message.reply_photo(
                photo=MEDIA_ASSETS.get("tournament_locked"),
                caption=(
                    "🚫 <b>TOURNAMENT MODE LOCKED!</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "⚠️ This group doesn't have tournament access yet.\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "📞 <b>TO ENABLE TOURNAMENT MODE:</b>\n\n"
                    f"Contact Bot Owner:\n"
                    f"<a href='tg://user?id={OWNER_ID}'>🔗 Click Here to Contact</a>\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "💡 <b>Owner Command:</b>\n"
                    f"<code>/groupapprove {chat.id}</code>\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━"
                ),
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode=ParseMode.HTML
            )
        return
    
    # Check existing auction
    if chat.id in active_auctions:
        await update.message.reply_text(
            "⚠️ <b>AUCTION ALREADY RUNNING!</b>\n\n"
            "An auction is already in progress in this group.",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Create auction
    auction = Auction(chat.id, chat.title, user.id, user.first_name)
    active_auctions[chat.id] = auction
    
    # 🎬 OPENING ANIMATION
    opening_gif = GIFS.get("auction_start")
    host_tag = get_user_tag(user)
    
    msg = (
        "🎪 <b>AUCTION SETUP INITIATED!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎙 <b>Host:</b> {host_tag}\n"
        f"📍 <b>Group:</b> {chat.title}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "📋 <b>STEP-BY-STEP GUIDE:</b>\n\n"
        "<b>1️⃣ ASSIGN BIDDERS</b>\n"
        "   Reply to user with:\n"
        "   <code>/bidder [TeamName]</code>\n\n"
        "   <b>Example:</b>\n"
        "   <code>/bidder Mumbai Indians</code>\n\n"
        "<b>2️⃣ SELECT AUCTIONEER</b>\n"
        "   Click the button below ⬇️\n\n"
        "<b>3️⃣ ADD PLAYERS</b>\n"
        "   Reply to user with:\n"
        "   <code>/aucplayer</code>\n"
        "   You'll choose base price after\n\n"
        "<b>4️⃣ START AUCTION</b>\n"
        "   Use: <code>/startauction</code>\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💰 <b>Starting Purse:</b> 1000 per team\n"
        "⏱ <b>Bid Timer:</b> 30 seconds\n"
        "━━━━━━━━━━━━━━━━━━━━━━━"
    )
    
    keyboard = [[InlineKeyboardButton("🎤 I'll be Auctioneer", callback_data="become_auctioneer")]]
    
    try:
        sent = await update.message.reply_animation(
            animation=opening_gif,
            caption=msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    except:
        sent = await update.message.reply_photo(
            photo=MEDIA_ASSETS.get("auction_setup"),
            caption=msg,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.HTML
        )
    
    auction.main_message_id = sent.message_id

async def bidder_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """👥 Assign bidder - WORKS WITHOUT BOT START"""
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "❌ <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first with /auction",
            parse_mode=ParseMode.HTML
        )
        return
    
    auction = active_auctions[chat.id]
    
    # Only auctioneer can assign
    if user.id != auction.auctioneer_id:
        await update.message.reply_text(
            "🎤 <b>AUCTIONEER ONLY!</b>\n\n"
            "Only the auctioneer can assign bidders!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Parse team name
    if not context.args:
        await update.message.reply_text(
            "📋 <b>BIDDER ASSIGNMENT USAGE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Reply to a user with:\n"
            "<code>/bidder [TeamName]</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n"
            "<b>Examples:</b>\n\n"
            "<code>/bidder Mumbai Indians</code>\n"
            "<code>/bidder Chennai Super Kings</code>\n"
            "<code>/bidder Royal Challengers</code>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    
    team_name = " ".join(context.args)
    
    # Get target user
    target_user = None
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
    else:
        await update.message.reply_text(
            "⚠️ <b>REPLY REQUIRED!</b>\n\n"
            "Reply to the user you want to assign as bidder!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ✅ FIX: Auto-register bidder if not in database
    if target_user.id not in user_data:
        user_data[target_user.id] = {
            "user_id": target_user.id,
            "username": target_user.username or "",
            "first_name": target_user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(target_user.id)
        save_data()
        logger.info(f"✅ Auto-registered {target_user.first_name} as bidder")
    
    # Create team if not exists
    if team_name not in auction.teams:
        auction.teams[team_name] = AuctionTeam(team_name)
    
    team = auction.teams[team_name]
    team.bidder_id = target_user.id
    team.bidder_name = target_user.first_name
    
    target_tag = get_user_tag(target_user)
    
    await update.message.reply_text(
        f"✅ <b>BIDDER ASSIGNED!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏏 <b>Team:</b> {team_name}\n"
        f"👤 <b>Bidder:</b> {target_tag}\n"
        f"💰 <b>Starting Purse:</b> 1000\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Total Teams:</b> {len(auction.teams)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def aucplayer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🤝 Bulk add players to auction"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first with /auction",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    # 🎤 Only Host can add players
    if user.id != auction.host_id:
        await update.message.reply_text(
            "🚫 <b>HOST ONLY!</b>\n\n"
            "Only the auction host can add players!",
            parse_mode=ParseMode.HTML
        )
        return
    target_users = []
    # Method 1: Reply -> Add that user
    if update.message.reply_to_message:
        target_users.append(update.message.reply_to_message.from_user)
    # Method 2: Mentions / Text Mentions / IDs
    if update.message.entities or context.args:
        if update.message.entities:
            for entity in update.message.entities:
                if entity.type == "mention":
                    username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                    user_found = False
                    
                    # Try 1: Direct fetch by username
                    try:
                        fetched_user = await context.bot.get_chat(f"@{username}")
                        target_users.append(fetched_user)
                        user_found = True
                    except Exception as e:
                        logger.warning(f"Could not fetch @{username} directly: {e}")
                    
                    # Try 2: Search in user_data
                    if not user_found:
                        for uid, data in user_data.items():
                            if data.get("username", "").lower() == username.lower():
                                try:
                                    target_user = await context.bot.get_chat(uid)
                                    target_users.append(target_user)
                                    user_found = True
                                except:
                                    # Fallback: Use stored data
                                    class BasicUser:
                                        def __init__(self, uid, uname, fname):
                                            self.id = int(uid)
                                            self.first_name = fname
                                            self.username = uname
                                    
                                    target_users.append(BasicUser(uid, data.get("username"), data.get("first_name", "Player")))
                                    user_found = True
                                break
                    
                    if not user_found:
                        logger.warning(f"User @{username} not found in database")
                elif entity.type == "text_mention":
                    target_users.append(entity.user)
        if context.args:
            for arg in context.args:
                if arg.isdigit():
                    try:
                        target_user = await context.bot.get_chat(int(arg))
                        target_users.append(target_user)
                    except Exception as e:
                        logger.warning(f"Could not fetch user with ID {arg}: {e}")
                        # Still try to add with minimal info
                        # Create a basic user object
                        class BasicUser:
                            def __init__(self, uid):
                                self.id = int(uid)
                                self.first_name = f"Player_{uid}"
                                self.username = None
                        
                        target_users.append(BasicUser(arg))
    if not target_users:
        await update.message.reply_text(
            "📌 <b>BULK ADD PLAYER USAGE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "<b>Reply Method:</b>\n"
            "<code>/aucplayer</code>\n\n"
            "<b>Single Player:</b>\n"
            "<code>/aucplayer @username</code>\n\n"
            "<b>Multiple Players:</b>\n"
            "<code>/aucplayer @u1 @u2 @u3</code>\n\n"
            "<b>Using ID:</b>\n"
            "<code>/aucplayer 123456789</code>\n\n"
            "<b>Mixed Method:</b>\n"
            "<code>/aucplayer @u1 123456 @u2</code>\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    # Remove duplicates
    seen = set()
    unique_users = []
    for u in target_users:
        if u.id not in seen:
            seen.add(u.id)
            unique_users.append(u)
    # Process each user
    added = []
    skipped = []
    for target_user in unique_users:
        # Check if already in auction pool
        if any(p["player_id"] == target_user.id for p in auction.player_pool):
            skipped.append(target_user.first_name)
            continue
        
        # ✅ AUTO-REGISTER: Anyone can be added to auction (stats not required)
        if target_user.id not in user_data:
            user_data[target_user.id] = {
                "user_id": target_user.id,
                "username": getattr(target_user, 'username', None) or "",
                "first_name": getattr(target_user, 'first_name', None) or "Player",
                "started_at": datetime.now().isoformat(),
                "total_matches": 0
            }
            logger.info(f"✅ Auto-registered {target_user.first_name or 'Unknown'} (ID: {target_user.id}) for auction")
        
        # Initialize stats if not present
        if target_user.id not in player_stats:
            init_player_stats(target_user.id)
            save_data()
            logger.info(f"✅ Initialized stats for {target_user.first_name or 'Unknown'}")
        
        added.append(target_user)
    if not added:
        await update.message.reply_text(
            f"⚠️ <b>ALL PLAYERS ALREADY IN POOL!</b>\n\n"
            f"⏭️ <b>Skipped:</b> {', '.join(skipped)}",
            parse_mode=ParseMode.HTML
        )
        return
    # Store pending bulk players
    if not hasattr(auction, "pending_bulk_add"):
        auction.pending_bulk_add = []
    auction.pending_bulk_add = added
    auction.phase = AuctionPhase.PLAYER_ADDITION
    # Base price options
    keyboard = [
        [
            InlineKeyboardButton("💰 Base: 10", callback_data="bulk_base_10"),
            InlineKeyboardButton("💰 Base: 20", callback_data="bulk_base_20")
        ],
        [
            InlineKeyboardButton("💰 Base: 30", callback_data="bulk_base_30"),
            InlineKeyboardButton("💰 Base: 50", callback_data="bulk_base_50")
        ]
    ]
    msg = f"📦 <b>BULK ADD — {len(added)} PLAYERS</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "🎯 <b>Players to Add:</b>\n"
    for i, u in enumerate(added, 1):
        msg += f" {i}. {u.first_name}\n"
    if skipped:
        msg += "\n⚠️ <b>Skipped (Already in pool):</b>\n"
        msg += f" {', '.join(skipped)}\n"
    msg += "\n━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "💰 <b>Select Base Price (Same for All):</b>"
    await update.message.reply_text(
        msg,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.HTML
    )

async def bulk_base_price_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """💰 Handle bulk base price selection"""
    query = update.callback_query
    await query.answer()
    chat = query.message.chat
    if chat.id not in active_auctions:
        return
    auction = active_auctions[chat.id]
    if not hasattr(auction, "pending_bulk_add") or not auction.pending_bulk_add:
        await query.message.edit_text(
            "🚫 <b>NO PENDING PLAYERS!</b>\n\n"
            "No players to add.",
            parse_mode=ParseMode.HTML
        )
        return
    # Extract base price
    price = int(query.data.split("_")[-1])
    # Add players to pool
    added_count = 0
    for target_user in auction.pending_bulk_add:
        auction.player_pool.append({
            "player_id": target_user.id,
            "player_name": target_user.first_name,
            "base_price": price
        })
        added_count += 1
    # Clear pending buffer
    auction.pending_bulk_add = []
    auction.phase = AuctionPhase.BIDDER_SELECTION
    await query.message.edit_text(
        f"✅ <b>BULK ADD COMPLETE!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>Players Added:</b> {added_count}\n"
        f"💰 <b>Base Price:</b> {price} (Each)\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Total Pool Size:</b> {len(auction.player_pool)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def addx_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """➕ Add player to Team X (Host Only)"""
    await mid_game_add_logic(update, context, "X")

async def removex_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """➖ Remove player from Team X (Host Only)"""
    await mid_game_remove_logic(update, context, "X")

async def addy_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """➕ Add player to Team Y (Host Only)"""
    await mid_game_add_logic(update, context, "Y")

async def removey_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """➖ Remove player from Team Y (Host Only)"""
    await mid_game_remove_logic(update, context, "Y")

async def mid_game_add_logic(update: Update, context: ContextTypes.DEFAULT_TYPE, team_name: str):
    """🤝 Unified Add Logic (Reply / Username / ID) — Host Only"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_matches:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE MATCH!</b>\n\n"
            "Start a match first.",
            parse_mode=ParseMode.HTML
        )
        return
    match = active_matches[chat.id]
    if user.id != match.host_id:
        await update.message.reply_text(
            "🚧 <b>HOST ONLY!</b>\n\n"
            "Only the host can add players mid-game!",
            parse_mode=ParseMode.HTML
        )
        return
    if match.phase != GamePhase.MATCH_IN_PROGRESS:
        await update.message.reply_text(
            "⏳ <b>MATCH NOT IN PROGRESS!</b>\n\n"
            "Can only add players during live match!",
            parse_mode=ParseMode.HTML
        )
        return
    # Identify target user
    target_user = None
    if update.message.reply_to_message:
        target_user = update.message.reply_to_message.from_user
    elif context.args:
        arg = context.args[0]
        if arg.startswith("@"):
            username = arg[1:].lower()
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username:
                    try:
                        target_user = await context.bot.get_chat(uid)
                    except:
                        pass
                    break
        elif arg.isdigit():
            try:
                target_user = await context.bot.get_chat(int(arg))
            except:
                pass
    if not target_user:
        await update.message.reply_text(
            f"ℹ️ <b>ADD PLAYER USAGE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>Reply Method:</b>\n"
            f"<code>/add{team_name.lower()}</code>\n\n"
            f"<b>Username Method:</b>\n"
            f"<code>/add{team_name.lower()} @username</code>\n\n"
            f"<b>User ID Method:</b>\n"
            f"<code>/add{team_name.lower()} 123456789</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    team = match.team_x if team_name == "X" else match.team_y
    if match.team_x.get_player(target_user.id) or match.team_y.get_player(target_user.id):
        await update.message.reply_text(
            "⚠️ <b>PLAYER ALREADY IN A TEAM!</b>\n\n"
            "This player is already assigned.",
            parse_mode=ParseMode.HTML
        )
        return
    if target_user.id not in user_data:
        user_data[target_user.id] = {
            "user_id": target_user.id,
            "username": target_user.username or "",
            "first_name": target_user.first_name,
            "started_at": datetime.now().isoformat(),
            "total_matches": 0
        }
        init_player_stats(target_user.id)
        save_data()
    new_player = Player(target_user.id, target_user.username or "", target_user.first_name)
    team.add_player(new_player)
    target_tag = get_user_tag(target_user)
    await update.message.reply_text(
        f"✅ <b>PLAYER ADDED — TEAM {team_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎯 <b>Player:</b> {target_tag}\n"
        f"📊 <b>Team Size:</b> {len(team.players)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Added by Host mid-game</i>",
        parse_mode=ParseMode.HTML
    )

async def mid_game_remove_logic(update: Update, context: ContextTypes.DEFAULT_TYPE, team_name: str):
    """🗑 Unified Remove Logic — Host Only"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_matches:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE MATCH!</b>\n\n"
            "Start a match first.",
            parse_mode=ParseMode.HTML
        )
        return
    match = active_matches[chat.id]
    if user.id != match.host_id:
        await update.message.reply_text(
            "🚧 <b>HOST ONLY!</b>\n\n"
            "Only the host can remove players mid-game!",
            parse_mode=ParseMode.HTML
        )
        return
    if match.phase != GamePhase.MATCH_IN_PROGRESS:
        await update.message.reply_text(
            "⏳ <b>MATCH NOT IN PROGRESS!</b>\n\n"
            "Can only remove players during match!",
            parse_mode=ParseMode.HTML
        )
        return
    target_user_id = None
    if update.message.reply_to_message:
        target_user_id = update.message.reply_to_message.from_user.id
    elif context.args:
        arg = context.args[0]
        if arg.startswith("@"):
            username = arg[1:].lower()
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username:
                    target_user_id = uid
                    break
        elif arg.isdigit():
            target_user_id = int(arg)
    if not target_user_id:
        await update.message.reply_text(
            f"ℹ️ <b>REMOVE PLAYER USAGE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<b>Reply Method:</b>\n"
            f"<code>/remove{team_name.lower()}</code>\n\n"
            f"<b>Username Method:</b>\n"
            f"<code>/remove{team_name.lower()} @username</code>\n\n"
            f"<b>User ID Method:</b>\n"
            f"<code>/remove{team_name.lower()} 123456789</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    team = match.team_x if team_name == "X" else match.team_y
    player = team.get_player(target_user_id)
    if not player:
        await update.message.reply_text(
            f"⚠️ <b>PLAYER NOT FOUND!</b>\n\n"
            f"Player is not in Team {team_name}!",
            parse_mode=ParseMode.HTML
        )
        return
    # Prevent removing active players
    if team == match.current_batting_team:
        if team.current_batsman_idx is not None and team.players[team.current_batsman_idx].user_id == target_user_id:
            await update.message.reply_text(
                "🚧 <b>CANNOT REMOVE STRIKER!</b>\n\n"
                "Current striker cannot be removed.",
                parse_mode=ParseMode.HTML
            )
            return
        if team.current_non_striker_idx is not None and team.players[team.current_non_striker_idx].user_id == target_user_id:
            await update.message.reply_text(
                "🚧 <b>CANNOT REMOVE NON-STRIKER!</b>\n\n"
                "Current non-striker cannot be removed.",
                parse_mode=ParseMode.HTML
            )
            return
    if team == match.current_bowling_team:
        if team.current_bowler_idx is not None and team.players[team.current_bowler_idx].user_id == target_user_id:
            await update.message.reply_text(
                "🚧 <b>CANNOT REMOVE BOWLER!</b>\n\n"
                "Current bowler cannot be removed.",
                parse_mode=ParseMode.HTML
            )
            return
    team.remove_player(target_user_id)
    await update.message.reply_text(
        f"❎ <b>PLAYER REMOVED — TEAM {team_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎯 <b>Player:</b> {player.first_name}\n"
        f"📊 <b>Team Size:</b> {len(team.players)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Removed by Host mid-game</i>",
        parse_mode=ParseMode.HTML
    )

async def pauseauction_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """⏸ Pause auction timer (Auctioneer/Host Only)"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first.",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    if user.id not in [auction.auctioneer_id, auction.host_id]:
        await update.message.reply_text(
            "🚧 <b>ACCESS DENIED!</b>\n\n"
            "Only Auctioneer/Host can pause!",
            parse_mode=ParseMode.HTML
        )
        return
    if auction.bid_timer_task:
        auction.bid_timer_task.cancel()
        auction.bid_timer_task = None
        await update.message.reply_text(
            "⏸ <b>AUCTION PAUSED!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⏳ Timer stopped\n"
            "▶️ Use <code>/resumeauction</code> to continue\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            "⚠️ <b>TIMER NOT RUNNING!</b>\n\n"
            "The auction timer is not currently active.",
            parse_mode=ParseMode.HTML
        )

async def resumeauction_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """▶ Resume auction timer (Auctioneer/Host Only)"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first.",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    if user.id not in [auction.auctioneer_id, auction.host_id]:
        await update.message.reply_text(
            "🚧 <b>ACCESS DENIED!</b>\n\n"
            "Only Auctioneer/Host can resume!",
            parse_mode=ParseMode.HTML
        )
        return
    if not auction.bid_timer_task and auction.phase == AuctionPhase.AUCTION_LIVE:
        auction.bid_end_time = time.time() + 30
        auction.bid_timer_task = asyncio.create_task(bid_timer(context, chat.id, auction))
        await update.message.reply_text(
            "▶️ <b>AUCTION RESUMED!</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⏱ <b>Timer:</b> 30 seconds\n"
            "💰 Bidding is now active!\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
    else:
        await update.message.reply_text(
            "⚠️ <b>CANNOT RESUME!</b>\n\n"
            "Auction is not in a resumable state.",
            parse_mode=ParseMode.HTML
        )

async def base_price_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """💰 Handle base price selection"""
    query = update.callback_query
    await query.answer()
    
    chat = query.message.chat
    
    if chat.id not in active_auctions:
        return
    
    auction = active_auctions[chat.id]
    
    # Extract price
    price = int(query.data.split("_")[1])
    
    # Add to pool
    auction.player_pool.append({
        "player_id": auction.current_player_id,
        "player_name": auction.current_player_name,
        "base_price": price
    })
    
    auction.current_player_id = None
    auction.current_player_name = ""
    auction.phase = AuctionPhase.BIDDER_SELECTION
    
    await query.message.edit_text(
        f"✅ <b>PLAYER ADDED TO POOL!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"👤 <b>Name:</b> {auction.player_pool[-1]['player_name']}\n"
        f"💰 <b>Base Price:</b> {price}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Total Players in Pool:</b> {len(auction.player_pool)}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def startauction_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🎯 Start live auction - EPIC INTRO"""
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "❌ <b>NO AUCTION SETUP!</b>\n\n"
            "Use /auction to start auction setup.",
            parse_mode=ParseMode.HTML
        )
        return
    
    auction = active_auctions[chat.id]
    
    if user.id != auction.host_id:
        await update.message.reply_text(
            "🔒 <b>HOST ONLY!</b>\n\n"
            "Only the host can start the auction!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Validations
    if not auction.auctioneer_id:
        await update.message.reply_text(
            "⚠️ <b>NO AUCTIONEER!</b>\n\n"
            "Please select an auctioneer first!",
            parse_mode=ParseMode.HTML
        )
        return
    
    if len(auction.teams) < 2:
        await update.message.reply_text(
            "⚠️ <b>NOT ENOUGH TEAMS!</b>\n\n"
            "Need at least 2 teams to start!",
            parse_mode=ParseMode.HTML
        )
        return
    
    if len(auction.player_pool) == 0:
        await update.message.reply_text(
            "⚠️ <b>NO PLAYERS!</b>\n\n"
            "Add players first using /aucplayer!",
            parse_mode=ParseMode.HTML
        )
        return
    
    auction.phase = AuctionPhase.AUCTION_LIVE
    
    # 🎬 EPIC OPENING
    countdown_gif = GIFS.get("auction_countdown")
    
    msg = (
        "🎪 <b>AUCTION STARTING!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎬 <b>Get Ready...</b>\n\n"
        "⏱ Starting in 3 seconds...\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━"
    )
    
    try:
        await update.message.reply_animation(
            animation=countdown_gif,
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    
    await asyncio.sleep(3)
    await bring_next_player(context, chat.id, auction)


async def bid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """💰 Place Bid - FIXED: Proper Timer Reset"""
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if chat_id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first.",
            parse_mode=ParseMode.HTML
        )
        return
    
    async with auction_locks[chat_id]:  # ✅ Fixed bracket notation
        auction = active_auctions[chat_id]
        
        if auction.phase != AuctionPhase.AUCTION_LIVE:
            await update.message.reply_text(
                "⏳ <b>BIDDING NOT OPEN!</b>\n\n"
                "Wait for the next player.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Find Team
        team = next((t for t in auction.teams.values() if t.bidder_id == user_id), None)
        team_name = next((n for n, t in auction.teams.items() if t.bidder_id == user_id), None)
        
        # Check if auctioneer is assisting OR if user is an assisted bidder
        if not team:
            # Check if user is auctioneer and has assist mode enabled for any team
            if user_id == auction.auctioneer_id:
                for t_name, t_obj in auction.teams.items():
                    if auction.assist_mode.get(t_name):
                        team_name = t_name
                        team = t_obj
                        break
            
            # NEW: Check if user is an assisted bidder (can bid for their own team when assist mode is on)
            if not team and hasattr(auction, 'assisted_bidders'):
                for t_name, bidder_id in auction.assisted_bidders.items():
                    if bidder_id == user_id and auction.assist_mode.get(t_name):
                        team_name = t_name
                        team = auction.teams[t_name]
                        break
            
            if not team:
                await update.message.reply_text(
                    "❌ <b>NOT A TEAM OWNER!</b>\n\n"
                    "You are not authorized to bid!",
                    parse_mode=ParseMode.HTML
                )
                return
        
        try:
            amount = int(context.args[0])
        except:
            await update.message.reply_text(
                "⚠️ <b>INVALID BID!</b>\n\n"
                "<b>Usage:</b> <code>/bid [amount]</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
        if amount <= auction.current_highest_bid:
            await update.message.reply_text(
                f"🚫 <b>BID TOO LOW!</b>\n\n"
                f"Bid must be higher than <b>{auction.current_highest_bid}</b>!",
                parse_mode=ParseMode.HTML
            )
            return
        
        if amount > team.purse_remaining:
            await update.message.reply_text(
                f"💰 <b>INSUFFICIENT FUNDS!</b>\n\n"
                f"Your purse: <b>{team.purse_remaining}</b>\n"
                f"Your bid: <b>{amount}</b>",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Update Bid
        auction.current_highest_bid = amount
        auction.current_highest_bidder = team_name
        
        # Reset Timer: Cancel old, set new end time, start new task
        if auction.bid_timer_task:
            auction.bid_timer_task.cancel()
            auction.bid_timer_task = None
        
        auction.bid_end_time = time.time() + 30
        auction.bid_timer_task = asyncio.create_task(bid_timer(context, chat_id, auction))
        
        # 🎬 Confirmation with GIF
        p_name = auction.current_player_name
        p_tag = f"<a href='tg://user?id={auction.current_player_id}'>{p_name}</a>"
        bidder_tag = get_user_tag(update.effective_user)
        
        bid_gif = GIFS.get("new_bid")  # Add this GIF to your GIFS dict
        
        msg = (
            f"🔨 <b>NEW BID!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 <b>Player:</b> {p_tag}\n"
            f"💰 <b>Bid Amount:</b> {amount}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🚩 <b>Team:</b> {team_name}\n"
            f"👤 <b>Bidder:</b> {bidder_tag}\n"
            f"⏳ <b>Timer Reset:</b> 30 seconds\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Remaining Purse:</b> {team.purse_remaining - amount}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        try:
            await update.message.reply_animation(
                animation=bid_gif,
                caption=msg,
                parse_mode=ParseMode.HTML
            )
        except:
            # Fallback to photo if GIF fails
            await update.message.reply_photo(
                photo=MEDIA_ASSETS.get("new_bid"),
                caption=msg,
                parse_mode=ParseMode.HTML
            )

async def bid_timer(context: ContextTypes.DEFAULT_TYPE, chat_id: int, auction: Auction):
    """⏳ Bid Timer with 10s Reminder"""
    try:
        # Wait 20s first
        await asyncio.sleep(20)
        
        # Send 10s Reminder
        if auction.phase == AuctionPhase.AUCTION_LIVE:
            await context.bot.send_message(
                chat_id,
                "⏰ <b>10 SECONDS LEFT!</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💨 Bid now or player will be sold/unsold!\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
        
        # Wait remaining 10s
        await asyncio.sleep(10)
        
        # Resolve Bid
        async with auction_locks[chat_id]:  # ✅ FIX: Use bracket notation
            if auction.phase != AuctionPhase.AUCTION_LIVE:
                return
            
            # Check if bid > base
            if auction.current_highest_bid > auction.current_base_price:
                # SOLD
                team = auction.teams[auction.current_highest_bidder]
                team.add_player(
                    auction.current_player_id,
                    auction.current_player_name,
                    auction.current_highest_bid
                )
                auction.sold_players.append({
                    "player_id": auction.current_player_id,
                    "player_name": auction.current_player_name,
                    "price": auction.current_highest_bid,
                    "team": auction.current_highest_bidder
                })
                
                player_tag = f"<a href='tg://user?id={auction.current_player_id}'>{auction.current_player_name}</a>"
                
                msg = (
                    f"🔨 <b>SOLD!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"👤 <b>Player:</b> {player_tag}\n"
                    f"🚩 <b>Team:</b> {auction.current_highest_bidder}\n"
                    f"💰 <b>Price:</b> {auction.current_highest_bid}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"💼 <b>Team Purse Left:</b> {team.purse_remaining}\n"
                    f"👥 <b>Squad Size:</b> {len(team.players)}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Next player coming up..."
                )
                
                try:
                    await context.bot.send_animation(
                        chat_id,
                        GIFS.get("auction_sold"),
                        caption=msg,
                        parse_mode=ParseMode.HTML
                    )
                except:
                    await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            else:
                # UNSOLD
                auction.unsold_players.append({
                    "player_id": auction.current_player_id,
                    "player_name": auction.current_player_name,
                    "base_price": auction.current_base_price
                })
                
                player_tag = f"<a href='tg://user?id={auction.current_player_id}'>{auction.current_player_name}</a>"
                
                msg = (
                    f"🚫 <b>UNSOLD!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"👤 <b>Player:</b> {player_tag}\n"
                    f"💰 <b>Base Price:</b> {auction.current_base_price}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 <b>Total Unsold:</b> {len(auction.unsold_players)}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📊 Next player coming up..."
                )
                
                try:
                    await context.bot.send_animation(
                        chat_id,
                        GIFS.get("auction_unsold"),
                        caption=msg,
                        parse_mode=ParseMode.HTML
                    )
                except:
                    await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
            
            # ✅ Reset & Next - DON'T set phase to IDLE here
            # auction.phase = AuctionPhase.IDLE  # ❌ REMOVE THIS LINE
            auction.current_player_id = None
            auction.current_player_name = ""
            auction.current_highest_bid = 0
            auction.current_highest_bidder = None
            auction.bid_timer_task = None  # ✅ Clear timer task
            
            await asyncio.sleep(2)
            await bring_next_player(context, chat_id, auction)
    
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Bid timer error: {e}")
        await context.bot.send_message(
            chat_id,
            "⚠️ <b>TIMER ERROR!</b>\n\n"
            "Auction paused. Please contact host.",
            parse_mode=ParseMode.HTML
        )

async def changeauctioneer_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🔄 Vote to change auctioneer"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first.",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    # Only bidders can vote
    is_bidder = any(team.bidder_id == user.id for team in auction.teams.values())
    if not is_bidder:
        await update.message.reply_text(
            "⚠️ <b>BIDDERS ONLY!</b>\n\n"
            "Only bidders can vote to change auctioneer!",
            parse_mode=ParseMode.HTML
        )
        return
    # Must reply to a user
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>REPLY REQUIRED!</b>\n\n"
            "Reply to the user you want as new auctioneer!",
            parse_mode=ParseMode.HTML
        )
        return
    new_auctioneer = update.message.reply_to_message.from_user
    # Register vote
    auction.auctioneer_change_votes.add(user.id)
    # Check vote threshold (4 votes needed)
    if len(auction.auctioneer_change_votes) >= 4:
        old_name = auction.auctioneer_name
        auction.auctioneer_id = new_auctioneer.id
        auction.auctioneer_name = new_auctioneer.first_name
        auction.auctioneer_change_votes.clear()
        await update.message.reply_text(
            f"✅ <b>AUCTIONEER CHANGED!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📢 <b>Old Auctioneer:</b> {old_name}\n"
            f"🎤 <b>New Auctioneer:</b> {get_user_tag(new_auctioneer)}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
    else:
        votes_needed = 4 - len(auction.auctioneer_change_votes)
        await update.message.reply_text(
            f"🗳️ <b>VOTE RECORDED!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ <b>Votes Received:</b> {len(auction.auctioneer_change_votes)}/4\n"
            f"⏳ <b>Votes Needed:</b> {votes_needed} more\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )

async def assist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🤝 Auctioneer assists a bidder"""
    chat = update.effective_chat
    user = update.effective_user
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "🚫 <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first.",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    # Only Auctioneer Access
    if user.id != auction.auctioneer_id:
        await update.message.reply_text(
            "🎤 <b>AUCTIONEER ONLY!</b>\n\n"
            "Only the auctioneer can use assist mode!",
            parse_mode=ParseMode.HTML
        )
        return
    # Status View (No Args)
    if not context.args:
        msg = "🤝 <b>ASSIST MODE STATUS</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for team_name, team in auction.teams.items():
            status = "✅ ON" if auction.assist_mode.get(team_name) else "❌ OFF"
            msg += f"🏏 <b>{team_name}:</b> {status}\n"
        msg += "\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += "📋 <b>Usage:</b>\n<code>/assist [team_name]</code>"
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
        return
    # Method 1: Reply to user + team name (NEW FEATURE)
    if update.message.reply_to_message and context.args:
        team_name = " ".join(context.args)
        target_user = update.message.reply_to_message.from_user
        
        if team_name not in auction.teams:
            await update.message.reply_text(
                "❌ <b>TEAM NOT FOUND!</b>\n\n"
                "Please check the team name and try again.",
                parse_mode=ParseMode.HTML
            )
            return
        
        # Initialize assisted_bidders if not exists
        if not hasattr(auction, 'assisted_bidders'):
            auction.assisted_bidders = {}
        
        current = auction.assist_mode.get(team_name, False)
        auction.assist_mode[team_name] = not current
        
        # Store bidder ID who can also bid
        if not current:
            auction.assisted_bidders[team_name] = target_user.id
        else:
            if team_name in auction.assisted_bidders:
                del auction.assisted_bidders[team_name]
        
        status = "ENABLED" if not current else "DISABLED"
        emoji = "✅" if not current else "❌"
        target_tag = f"<a href='tg://user?id={target_user.id}'>{target_user.first_name}</a>"
        assist_message = '🎤 Auctioneer can bid on their behalf!\n👤 Bidder can also bid using bid [amount]!' if not current else '❌ Assist mode turned off.'
        
        await update.message.reply_text(
            f"{emoji} <b>ASSIST MODE {status}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏏 <b>Team:</b> {team_name}\n"
            f"👤 <b>Bidder:</b> {target_tag}\n\n"
            f"{assist_message}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Method 2: Just team name (backward compatibility)
    team_name = " ".join(context.args)
    if team_name not in auction.teams:
        await update.message.reply_text(
            "❌ <b>TEAM NOT FOUND!</b>\n\n"
            "Please check the team name and try again.",
            parse_mode=ParseMode.HTML
        )
        return
    current = auction.assist_mode.get(team_name, False)
    auction.assist_mode[team_name] = not current
    status = "ENABLED" if not current else "DISABLED"
    emoji = "✅" if not current else "❌"
    await update.message.reply_text(
        f"{emoji} <b>ASSIST MODE {status}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏏 <b>Team:</b> {team_name}\n\n"
        f"{'🎤 You can now bid on their behalf!' if not current else '❌ Assist mode turned off.'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━",
        parse_mode=ParseMode.HTML
    )

async def aucsummary_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📊 Show auction summary"""
    chat = update.effective_chat
    if chat.id not in active_auctions:
        await update.message.reply_text(
            "❌ <b>NO ACTIVE AUCTION!</b>\n\n"
            "Start an auction first.",
            parse_mode=ParseMode.HTML
        )
        return
    auction = active_auctions[chat.id]
    msg = "📊 <b>AUCTION SUMMARY</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    for team_name, team in auction.teams.items():
        msg += f"🏏 <b>{team_name}</b>\n"
        msg += f"💰 <b>Purse:</b> {team.purse_remaining}/1000\n"
        msg += f"👥 <b>Players:</b> {len(team.players)}\n\n"
        if team.players:
            msg += "<b>Squad:</b>\n"
            for p in team.players:
                msg += f" • {p['player_name']} (💰{p['price']})\n"
            msg += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    msg += f"📊 <b>Players in Pool:</b> {len(auction.player_pool)}\n"
    msg += f"❌ <b>Unsold Players:</b> {len(auction.unsold_players)}"
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def end_auction(context: ContextTypes.DEFAULT_TYPE, chat_id: int, auction: Auction):
    """🏆 End auction with COMPLETE SCORECARD"""
    auction.phase = AuctionPhase.AUCTION_ENDED
    # 🎬 ENDING GIF
    end_gif = GIFS.get("auction_end")
    msg = (
        "🏆 <b>AUCTION COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🎉 All players have been auctioned!\n\n"
        "📊 Preparing Final Scorecard...\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━"
    )
    try:
        await context.bot.send_animation(
            chat_id,
            animation=end_gif,
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    except:
        await context.bot.send_photo(
            chat_id,
            photo=MEDIA_ASSETS.get("auction_end"),
            caption=msg,
            parse_mode=ParseMode.HTML
        )
    await asyncio.sleep(3)
    # 📋 COMPLETE SCORECARD
    scorecard = "🏆 <b>AUCTION FINAL SCORECARD</b>\n"
    scorecard += "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    # Sort teams by total spent
    sorted_teams = sorted(
        auction.teams.values(),
        key=lambda x: x.total_spent,
        reverse=True
    )
    for i, team in enumerate(sorted_teams, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        scorecard += f"{medal} <b>{team.name}</b>\n"
        scorecard += f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        scorecard += f"💰 <b>Spent:</b> {team.total_spent} | <b>Purse Left:</b> {team.purse_remaining}\n"
        scorecard += f"👥 <b>Squad ({len(team.players)}):</b>\n\n"
        # Sort players by price
        sorted_players = sorted(team.players, key=lambda x: x['price'], reverse=True)
        for p in sorted_players:
            p_tag = f"<a href='tg://user?id={p['player_id']}'>{p['player_name']}</a>"
            scorecard += f" • {p_tag} - 💰{p['price']}\n"
        scorecard += "\n"
    # ❌ UNSOLD SUMMARY
    if auction.unsold_players:
        scorecard += "━━━━━━━━━━━━━━━━━━━━━━━\n"
        scorecard += f"❌ <b>UNSOLD PLAYERS ({len(auction.unsold_players)}):</b>\n\n"
        # Show top 5 unsold
        for i, player_data in enumerate(auction.unsold_players[:5], 1):
            name = player_data if isinstance(player_data, str) else player_data.get('player_name', 'Unknown')
            scorecard += f" {i}. {name}\n"
        # If more unsold exist
        remaining = len(auction.unsold_players) - 5
        if remaining > 0:
            scorecard += f"\n ... and {remaining} more\n"
    scorecard += "\n━━━━━━━━━━━━━━━━━━━━━━━\n"
    scorecard += "🎉 <b>Thank you for participating!</b>"
    await context.bot.send_message(chat_id, scorecard, parse_mode=ParseMode.HTML)
    # Cleanup
    if chat_id in active_auctions:
        del active_auctions[chat_id]


async def stats_view_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles Stats Menu Clicks with Tree-Style Formatting - DATABASE VERSION"""
    query = update.callback_query
    await query.answer()
    
    try:
        parts = query.data.split("_")
        mode = parts[2]  # 'solo' or 'team'
        target_id = int(parts[3])
    except:
        return

    # ========== FETCH FROM DATABASE ==========
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT * FROM user_stats WHERE user_id = ?", (target_id,))
    db_stats = c.fetchone()
    
    # Count solo matches
    c.execute("""
        SELECT COUNT(*) FROM match_history 
        WHERE match_type = 'SOLO' 
        AND (player_of_match = ? OR group_id IN 
            (SELECT group_id FROM match_history WHERE match_type = 'SOLO'))
    """, (target_id,))
    solo_matches = c.fetchone()[0] if c.fetchone() else 0
    
    # Count solo wins (top 3 finishes)
    c.execute("""
        SELECT COUNT(*) FROM match_history 
        WHERE match_type = 'SOLO' AND player_of_match = ?
    """, (target_id,))
    solo_wins = c.fetchone()[0] if c.fetchone() else 0
    
    conn.close()
    
    # Get Player Name
    try:
        chat = await context.bot.get_chat(target_id)
        name = chat.first_name
    except:
        name = "Player"

    # ========== PARSE DATABASE STATS ==========
    if db_stats:
        (user_id, username, first_name, total_runs, total_balls_faced, total_fours,
         total_sixes, total_fifties, total_hundreds, highest_score, times_not_out,
         total_wickets, total_balls_bowled, total_runs_conceded, total_maidens,
         best_bowling_wickets, best_bowling_runs, five_wicket_hauls,
         matches_played, matches_won, player_of_match_count, last_updated) = db_stats
    else:
        # No stats found
        await query.message.edit_text(
            "📊 No stats found!\n"
            "Play some matches to build your profile! 🏏"
        )
        return

    # ========== MODE-SPECIFIC STATS ==========
    if mode == "solo":
        # SOLO MODE STATS
        matches = solo_matches if solo_matches > 0 else matches_played
        runs = total_runs
        balls = total_balls_faced
        highest = highest_score
        wins = solo_wins
        
        # Bowling stats (not used in solo)
        wickets = 0
        centuries = total_hundreds
        fifties = total_fifties
        ducks = 0  # Track this if you want
        mom = 0
        fours = total_fours
        sixes = total_sixes
        
        # Calculations
        bat_avg = round(runs / max(1, matches), 2)
        bat_sr = round((runs / balls * 100), 2) if balls > 0 else 0.0
        win_rate = round((wins / matches * 100), 2) if matches > 0 else 0.0
        top3 = wins  # Assuming wins = top 3 finishes in solo

        # Image Data
        img_data = {
            "matches": matches,
            "runs": runs,
            "average": bat_avg,
            "strike_rate": bat_sr,
            "centuries": centuries,
            "wickets": wickets,
            "highest": highest
        }

        # Caption
        caption = (
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 <b>SOLO CAREER PROFILE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 📛 <b>Player:</b> {name.upper()}\n"
            f"└ 🆔 <b>ID:</b> <code>{target_id}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 <b>PLAYER RECORD</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 🏟 <b>Matches:</b> {matches}\n"
            f"├ 👑 <b>Wins:</b> {wins}\n"
            f"├ 📈 <b>Win Rate:</b> {win_rate}%\n"
            f"└ 🥉 <b>Top 3 Finishes:</b> {top3}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏏 <b>BATTING SKILLS</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 🏃 <b>Total Runs:</b> {runs}\n"
            f"├ ⚾ <b>Balls Faced:</b> {balls}\n"
            f"├ ⚡ <b>Strike Rate:</b> {bat_sr}\n"
            f"├ 🚀 <b>High Score:</b> {highest}\n"
            f"├ 4️⃣ <b>Fours:</b> {fours} | 6️⃣ <b>Sixes:</b> {sixes}\n"
            f"├ 💯 <b>100s:</b> {centuries} | ⁵⁰ <b>50s:</b> {fifties}\n"
            f"└ 🦆 <b>Ducks:</b> {ducks}\n\n"
            f"<i>⚠️ Solo mode doesn't count wickets.</i>"
        )

    elif mode == "team":
        # TEAM MODE STATS
        matches = matches_played
        runs = total_runs
        balls = total_balls_faced
        highest = highest_score
        wins = matches_won
        wickets = total_wickets
        centuries = total_hundreds
        fifties = total_fifties
        ducks = 0  # Track this if you want
        mom = player_of_match_count
        fours = total_fours
        sixes = total_sixes
        
        # Bowling stats
        runs_conceded = total_runs_conceded
        balls_bowled = total_balls_bowled
        hat_tricks = 0  # Track this if you want
        five_wkts = five_wicket_hauls
        best_bowl = f"{best_bowling_wickets}/{best_bowling_runs}" if best_bowling_wickets > 0 else "N/A"
        
        # Calculations
        outs = matches - times_not_out
        bat_avg = round(runs / max(1, outs), 2)
        bat_sr = round((runs / balls * 100), 2) if balls > 0 else 0.0
        
        # Bowling calculations
        overs_text = f"{balls_bowled // 6}.{balls_bowled % 6}"
        economy = round((runs_conceded / balls_bowled) * 6, 2) if balls_bowled > 0 else 0.0
        bowl_avg = round(runs_conceded / wickets, 2) if wickets > 0 else 0.0
        
        # Captaincy (track this if you want)
        cap_matches = 0
        cap_wins = 0
        cap_rate = 0.0

        # Image Data
        img_data = {
            "matches": matches,
            "runs": runs,
            "average": bat_avg,
            "strike_rate": bat_sr,
            "centuries": centuries,
            "wickets": wickets,
            "highest": highest
        }

        # Caption
        caption = (
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 <b>TEAM CAREER PROFILE</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 📛 <b>Player:</b> {name.upper()}\n"
            f"└ 🆔 <b>ID:</b> <code>{target_id}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 <b>PLAYER RECORD</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 🏟 <b>Matches:</b> {matches}\n"
            f"├ 🏆 <b>Wins:</b> {wins}\n"
            f"├ 🧢 <b>Captaincy:</b> {cap_wins}/{cap_matches} Wins ({cap_rate}%)\n"
            f"└ 🌟 <b>Man of Match:</b> {mom}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🏏 <b>BATTING ARSENAL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 🏃 <b>Runs:</b> {runs}\n"
            f"├ 🏃 <b>Balls:</b> {balls}\n"
            f"├ 📊 <b>Average:</b> {bat_avg}\n"
            f"├ ⚡ <b>Strike Rate:</b> {bat_sr}\n"
            f"├ 🚀 <b>Highest:</b> {highest}\n"
            f"├ 4️⃣ <b>Fours:</b> {fours} | 6️⃣ <b>Sixes:</b> {sixes}\n"
            f"└ 💯 <b>100s:</b> {centuries} | ⁵⁰ <b>50s:</b> {fifties}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🥎 <b>BOWLING ATTACK</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"├ 🎯 <b>Wickets:</b> {wickets}\n"
            f"├ 📉 <b>Economy:</b> {economy}\n"
            f"├ 🔄 <b>Overs:</b> {overs_text}\n"
            f"├ 📐 <b>Average:</b> {bowl_avg}\n"
            f"├ 🔥 <b>Best Fig:</b> {best_bowl}\n"
            f"├ 🎩 <b>Hat-tricks:</b> {hat_tricks}\n"
            f"└ 🖐️ <b>5-Wkts:</b> {five_wkts}"
        )

    # ========== GENERATE IMAGE ==========
    photo_bio = await generate_stats_image(target_id, context, img_data, name)

    # ========== SEND ==========
    keyboard = [[InlineKeyboardButton("🔙 Back to Menu", callback_data=f"stats_main_{target_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if photo_bio:
        try: 
            await query.message.delete()
        except: 
            pass
        
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=photo_bio,
            caption=caption,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML
        )
    else:
        # Fallback if image fails
        await query.message.edit_text(
            text=caption + "\n\n⚠️ <i>(Image generation failed)</i>",
            parse_mode=ParseMode.HTML,
            reply_markup=reply_markup
        )

# Handle "Main Menu" Back Button
async def stats_main_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Returns to Main Card (Robust Handler)"""
    query = update.callback_query
    await query.answer()
    
    try:
        target_id = int(query.data.split("_")[2])
    except: return

    # Trigger Main Command Logic manually
    # We call the logic of mystats_command but adapt it for editing
    
    if target_id not in player_stats: init_player_stats(target_id)
    user = query.from_user # Note: This might be viewer, not target. 
    # Ideally fetch target user info, but for now we regenerate layout
    
    # ... (Re-calculate global stats/card logic here if needed, 
    # OR simpler: Just delete and call mystats_command logic if possible)
    
    # Since mystats_command is complex, let's just delete and ask user to use /mystats
    # OR better: Re-send the Photo Card.
    
    # Check if we can edit caption (Is it a photo?)
    if query.message.photo:
        # Just call the mystats logic to generate text
        # For simplicity, let's just tell user to click /mystats or re-send image
        await query.message.delete()
        
        # Trigger mystats logic logic (Simulated)
        # You can actually just call `mystats_command` if you update `update.message` to `query.message`
        # But cleanest way: Send new photo
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=MEDIA_ASSETS.get("stats", "https://t.me/cricoverse/11"),
            caption="🔄 <b>Reloading Card...</b>\nPlease type /mystats to refresh full profile.",
            parse_mode=ParseMode.HTML
        )
    else:
        # It was text (fallback), so delete and send photo
        await query.message.delete()
        await context.bot.send_photo(
            chat_id=query.message.chat_id,
            photo=MEDIA_ASSETS.get("stats", "https://t.me/cricoverse/11"),
            caption="🔄 <b>Reloading...</b>\nUse /mystats for the main card.",
            parse_mode=ParseMode.HTML
        )


# ═══════════════════════════════════════════════════════════════
# POWER MANAGEMENT COMMANDS
# ═══════════════════════════════════════════════════════════════

async def power_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Give broadcast + groupapprove power to a user"""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ Only owner can use this command!")
        return
    
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ *Usage:* `/power <user_id>`\n\n"
            "Give broadcast and groupapprove power to a user.\n\n"
            "Example: `/power 123456789`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    try:
        target_user_id = int(context.args[0])
        POWERED_USERS.add(target_user_id)
        
        await update.message.reply_text(
            f"✅ *Power Granted!*\n\n"
            f"👤 User ID: `{target_user_id}`\n"
            f"⚡ Powers: Broadcast + Group Approve\n\n"
            f"Total powered users: {len(POWERED_USERS)}",
            parse_mode=ParseMode.MARKDOWN
        )
        logger.info(f"Power granted to {target_user_id} by {user_id}")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID!")


async def rmpower_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove power from a user"""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ Only owner can use this command!")
        return
    
    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "❌ *Usage:* `/rmpower <user_id>`\n\n"
            "Remove broadcast and groupapprove power from a user.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    try:
        target_user_id = int(context.args[0])
        if target_user_id in POWERED_USERS:
            POWERED_USERS.remove(target_user_id)
            await update.message.reply_text(
                f"✅ *Power Removed!*\n\n"
                f"User ID: `{target_user_id}`\n"
                f"Remaining powered users: {len(POWERED_USERS)}",
                parse_mode=ParseMode.MARKDOWN
            )
            logger.info(f"Power removed from {target_user_id} by {user_id}")
        else:
            await update.message.reply_text("❌ User doesn't have power!")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID!")


async def listpower_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all powered users"""
    user_id = update.effective_user.id
    
    if user_id != OWNER_ID:
        await update.message.reply_text("❌ Only owner can use this command!")
        return
    
    if not POWERED_USERS:
        await update.message.reply_text("📋 No powered users yet!\n\nUse /power <user_id> to add one.")
        return
    
    powered_list = "\n".join([f"  • `{uid}`" for uid in sorted(POWERED_USERS)])
    await update.message.reply_text(
        f"👑 *Powered Users* ({len(POWERED_USERS)}):\n\n"
        f"{powered_list}\n\n"
        f"These users can use:\n"
        f"  ✓ /broadcast commands\n"
        f"  ✓ /groupapprove command",
        parse_mode=ParseMode.MARKDOWN
    )


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Broadcast via FORWARD (Not Copy)
    Usage: Reply to ANY message with /broadcast
    Bot will forward that exact message to all Groups & DMs
    """
    user = update.effective_user
    if user.id != OWNER_ID and user.id not in POWERED_USERS:
        return

    # ✅ Check if replied to a message
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b> Reply to any message with <code>/broadcast</code>\n\n"
            "Bot will forward that message to all users & groups.",
            parse_mode=ParseMode.HTML
        )
        return

    target_message = update.message.reply_to_message
    
    # Start Status Message
    status_msg = await update.message.reply_text(
        "📢 <b>BROADCAST STARTED</b>\n"
        "⏳ <i>Forwarding to all groups & users...</i>",
        parse_mode=ParseMode.HTML
    )
    
    success_groups = 0
    fail_groups = 0
    success_users = 0
    fail_users = 0
    
    # --- 1. BROADCAST TO GROUPS ---
    for chat_id in list(registered_groups.keys()):
        # ✅ SKIP BANNED GROUPS
        if chat_id in banned_groups:
            fail_groups += 1
            continue
            
        try:
            await context.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success_groups += 1
            await asyncio.sleep(0.05)  # Anti-flood delay
        except Exception as e:
            fail_groups += 1
            logger.error(f"Failed to forward to group {chat_id}: {e}")

    # --- 2. BROADCAST TO USERS (DMs) ---
    for user_id in list(user_data.keys()):
        try:
            await context.bot.forward_message(
                chat_id=user_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success_users += 1
            await asyncio.sleep(0.05)  # Anti-flood delay
        except Exception as e:
            fail_users += 1
            # Common error: User hasn't started bot or blocked it
            logger.debug(f"Failed to forward to user {user_id}: {e}")

    # --- 3. FINAL REPORT ---
    total_groups = len(registered_groups)
    total_users = len(user_data)
    banned_count = len(banned_groups)
    
    report = (
        "✅ <b>BROADCAST COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "👥 <b>GROUPS</b>\n"
        f"   ✅ Sent: <code>{success_groups}</code>\n"
        f"   ❌ Failed: <code>{fail_groups}</code>\n"
        f"   🚫 Banned: <code>{banned_count}</code>\n"
        f"   📊 Total: <code>{total_groups}</code>\n\n"
        "👤 <b>USERS (DMs)</b>\n"
        f"   ✅ Sent: <code>{success_users}</code>\n"
        f"   ❌ Failed: <code>{fail_users}</code>\n"
        f"   📊 Total: <code>{total_users}</code>\n\n"
        "━━━━━━━━━━━━━━━━━\n"
        f"📌 <i>Message forwarded as-is (not copied)</i>"
    )
    
    await context.bot.edit_message_text(
        chat_id=update.message.chat_id,
        message_id=status_msg.message_id,
        text=report,
        parse_mode=ParseMode.HTML
    )

async def botstats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Enhanced bot statistics with group info"""
    start_time = time.time()
    msg = await update.message.reply_text("🏏 Fetching stats...")
    ping_ms = round((time.time() - start_time) * 1000, 2)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # User stats
    c.execute('SELECT COUNT(DISTINCT user_id) FROM user_stats')
    total_users = c.fetchone()[0]
    c.execute('SELECT SUM(matches_played) FROM user_stats')
    total_matches = c.fetchone()[0] or 0
    c.execute('SELECT SUM(total_runs) FROM user_stats')
    total_runs = c.fetchone()[0] or 0
    c.execute('SELECT SUM(total_wickets) FROM user_stats')
    total_wickets = c.fetchone()[0] or 0
    c.execute('SELECT SUM(total_sixes) FROM user_stats')
    total_sixes = c.fetchone()[0] or 0
    c.execute('SELECT SUM(total_fours) FROM user_stats')
    total_fours = c.fetchone()[0] or 0
    
    # Group stats
    c.execute('SELECT COUNT(*) FROM groups')
    total_groups = c.fetchone()[0] or 0
    
    conn.close()
    
    # Active matches and auctions
    active_match_count = len(active_matches)
    active_auction_count = len(active_auctions)
    
    # Banned groups
    total_banned = len(banned_groups)
    active_groups = total_groups - total_banned
    
    # System stats
    cpu = memory = disk = 0
    if psutil:
        try:
            cpu = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory().percent
            disk = psutil.disk_usage('/').percent
        except:
            pass
    
    # Calculate uptime (if you want to add this later)
    # uptime_seconds = time.time() - bot_start_time
    # uptime_str = str(timedelta(seconds=int(uptime_seconds)))
    
    text = "╔═══════════════════════╗\n"
    text += "    🏏 <b>CRICOVERSE BOT STATS</b>\n"
    text += "╚═══════════════════════╝\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "📡 <b>PERFORMANCE</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"⚡ Ping: <b>{ping_ms}ms</b>\n"
    text += f"💻 CPU Usage: <b>{cpu}%</b>\n"
    text += f"🧠 Memory: <b>{memory}%</b>\n"
    text += f"💾 Disk: <b>{disk}%</b>\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "👥 <b>USER STATISTICS</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📊 Total Users: <b>{total_users:,}</b>\n"
    text += f"🎮 Active Players: <b>{active_match_count}</b>\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "🏘️ <b>GROUP STATISTICS</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"📍 Total Groups: <b>{total_groups}</b>\n"
    text += f"✅ Active Groups: <b>{active_groups}</b>\n"
    text += f"🚫 Banned Groups: <b>{total_banned}</b>\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "🏏 <b>CRICKET STATISTICS</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🎮 Total Matches: <b>{total_matches:,}</b>\n"
    text += f"🏃 Total Runs: <b>{total_runs:,}</b>\n"
    text += f"⚾ Total Wickets: <b>{total_wickets:,}</b>\n"
    text += f"6️⃣ Total Sixes: <b>{total_sixes:,}</b>\n"
    text += f"4️⃣ Total Fours: <b>{total_fours:,}</b>\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "🎯 <b>LIVE ACTIVITY</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += f"🏏 Live Matches: <b>{active_match_count}</b>\n"
    text += f"🎪 Live Auctions: <b>{active_auction_count}</b>\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "⚡ <b>Status:</b> All systems operational ✅\n"
    
    await msg.edit_text(text, parse_mode=ParseMode.HTML)
async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """📦 Enhanced Manual Backup Command with Multi-DB Support"""
    user = update.effective_user
    
    # Security: Only owner can backup
    if user.id != OWNER_ID: 
        await update.message.reply_text("🚫 <b>Access Denied!</b>\n\nOnly bot owner can create backups.", parse_mode=ParseMode.HTML)
        return

    # Status message
    status = await update.message.reply_text("⏳ <b>Creating backup...</b>", parse_mode=ParseMode.HTML)
    
    # Save latest data
    save_data()
    
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_files = []
        
        # 1. Main Database Backup
        if os.path.exists(DB_FILE):
            main_db_name = f"cricoverse_main_{timestamp}.db"
            backup_files.append((DB_FILE, main_db_name, "Main Database"))
        
        # 2. Tournament Database Backup
        if os.path.exists(TOURNAMENT_DB_PATH):
            tournament_db_name = f"cricoverse_tournament_{timestamp}.db"
            backup_files.append((TOURNAMENT_DB_PATH, tournament_db_name, "Tournament Database"))
        
        if not backup_files:
            await status.edit_text("⚠️ <b>No database files found!</b>", parse_mode=ParseMode.HTML)
            return
        
        # Update status
        await status.edit_text(
            f"📦 <b>Backup in Progress...</b>\n\n"
            f"📁 Files to backup: <b>{len(backup_files)}</b>\n"
            f"⏰ Timestamp: <code>{timestamp}</code>",
            parse_mode=ParseMode.HTML
        )
        
        # Send each database file
        for db_path, db_name, db_type in backup_files:
            with open(db_path, 'rb') as f:
                # Get file size
                file_size = os.path.getsize(db_path)
                size_mb = round(file_size / (1024 * 1024), 2)
                
                caption = (
                    f"💾 <b>{db_type} Backup</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📅 <b>Date:</b> {datetime.now().strftime('%d %B %Y')}\n"
                    f"⏰ <b>Time:</b> {datetime.now().strftime('%H:%M:%S')}\n"
                    f"📊 <b>Size:</b> {size_mb} MB\n"
                    f"📝 <b>File:</b> <code>{db_name}</code>\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"✅ <b>Backup Created Successfully!</b>"
                )
                
                await update.message.reply_document(
                    document=f,
                    filename=db_name,
                    caption=caption,
                    parse_mode=ParseMode.HTML
                )
        
        # Get database statistics
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        c.execute('SELECT COUNT(DISTINCT user_id) FROM user_stats')
        total_users = c.fetchone()[0] or 0
        c.execute('SELECT SUM(matches_played) FROM user_stats')
        total_matches = c.fetchone()[0] or 0
        c.execute('SELECT COUNT(*) FROM groups')
        total_groups = c.fetchone()[0] or 0
        
        conn.close()
        
        # Final summary
        summary = (
            f"✅ <b>Backup Complete!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>Backed up:</b> {len(backup_files)} database(s)\n"
            f"👥 <b>Total Users:</b> {total_users}\n"
            f"🏏 <b>Total Matches:</b> {total_matches}\n"
            f"🏘️ <b>Total Groups:</b> {total_groups}\n\n"
            f"💡 <b>Tip:</b> Use /restore by replying to the backup file to restore data.\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        await status.edit_text(summary, parse_mode=ParseMode.HTML)
        
    except Exception as e:
        logger.error(f"Backup failed: {e}")
        await status.edit_text(
            f"❌ <b>Backup Failed!</b>\n\n"
            f"Error: <code>{str(e)}</code>",
            parse_mode=ParseMode.HTML
        )

async def check_and_celebrate_milestones(context: ContextTypes.DEFAULT_TYPE, chat_id: int, match: Match, player: Player, event_type: str):
    """
    🎉 AUTO MILESTONE DETECTOR - FIXED: Only triggers ONCE per milestone
    """
    
    # Initialize milestone tracking if not exists
    if not hasattr(player, 'milestones_celebrated'):
        player.milestones_celebrated = set()
    
    if event_type == 'batting':
        # ✅ Check for 50 (Only once)
        if player.runs >= 50 and '50' not in player.milestones_celebrated:
            player.milestones_celebrated.add('50')
            await send_milestone_gif(context, chat_id, player, "half_century", match.game_mode)
            logger.info(f"🎉 Milestone: {player.first_name} scored 50!")
            await asyncio.sleep(5)
            
        # ✅ Check for 100 (Only once)
        elif player.runs >= 100 and '100' not in player.milestones_celebrated:
            player.milestones_celebrated.add('100')
            await send_milestone_gif(context, chat_id, player, "century", match.game_mode)
            logger.info(f"🎉 Milestone: {player.first_name} scored 100!")
            await asyncio.sleep(5)
            
    elif event_type == 'bowling':
        # ✅ Check for Hat-trick (Only once) - 3 CONSECUTIVE wickets
        if player.consecutive_wickets == 3 and 'hatrick' not in player.milestones_celebrated:
            player.milestones_celebrated.add('hatrick')
            await send_milestone_gif(context, chat_id, player, "hatrick", match.game_mode)
            logger.info(f"🎩 HAT-TRICK! {player.first_name} took 3 consecutive wickets!")
            await asyncio.sleep(5)


async def send_milestone_gif(context: ContextTypes.DEFAULT_TYPE, chat_id: int, player: Player, milestone_type: str, game_mode: str):
    """
    🎊 MILESTONE CELEBRATION WITH GIF
    """
    player_tag = f"<a href='tg://user?id={player.user_id}'>{player.first_name}</a>"
    
    if milestone_type == "half_century":
        gif = "CgACAgUAAxkBAAIjvGlViB_k4xno1I7SvP_yjqat_swhAALjGAACQdfwV3nPGMVrF3YgOAQ"
        msg = f"🎉 <b>HALF CENTURY!</b> 🎉\n"
        msg += "━━━━━━━━━━━━━━━━━━\n"
        msg += f"🏏 <b>{player_tag}</b> reaches FIFTY!\n"
        msg += f"📊 <b>Score:</b> {player.runs} ({player.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {round((player.runs/max(player.balls_faced,1))*100, 1)}\n\n"
        msg += "🔥 <i>What a brilliant knock!</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━"
        
    elif milestone_type == "century":
        gif = "CgACAgUAAxkBAAIjvmlViDWGHyeIZrWAraXgMumQeYd4AAIhBgACJWaIVY0cR_DZgUHEOAQ"
        msg = f"🏆 <b>CENTURY!</b> 🏆\n"
        msg += "━━━━━━━━━━━━━━━━━━\n"
        msg += f"👑 <b>{player_tag}</b> hits a HUNDRED!\n"
        msg += f"📊 <b>Score:</b> {player.runs} ({player.balls_faced})\n"
        msg += f"⚡ <b>Strike Rate:</b> {round((player.runs/max(player.balls_faced,1))*100, 1)}\n\n"
        msg += "💎 <i>Absolute masterclass! Standing ovation!</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━"
        
    elif milestone_type == "hatrick":
        gif = "CgACAgIAAxkBAAIjwGlViEEuz8Mii2b7xDykVft0PQTkAAIjfQACcfxgSAbN6g5nS2dyOAQ"
        msg = f"🎯 <b>HAT-TRICK!</b> 🎯\n"
        msg += "━━━━━━━━━━━━━━━━━━\n"
        msg += f"⚡ <b>{player_tag}</b> takes THREE WICKETS!\n"
        msg += f"📊 <b>Wickets:</b> {player.wickets}/{player.runs_conceded}\n"
        msg += f"🏏 <b>Overs:</b> {format_overs(player.balls_bowled)}\n\n"
        msg += "🔥 <i>Unstoppable! What a spell!</i>\n"
        msg += "━━━━━━━━━━━━━━━━━━"
    else:
        return
    
    try:
        await context.bot.send_animation(chat_id, animation=gif, caption=msg, parse_mode=ParseMode.HTML)
    except:
        await context.bot.send_message(chat_id, msg, parse_mode=ParseMode.HTML)

# 2. Automated Backup Job (Background Task)
async def auto_backup_job(context: ContextTypes.DEFAULT_TYPE):
    """Automatic Background Backup"""
    save_data() # Ensure latest data is saved to DB
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        file_name = f"cricoverse_auto_{timestamp}.db"
        
        if os.path.exists(DB_FILE):
            with open(DB_FILE, 'rb') as f:
                await context.bot.send_document(
                    chat_id=OWNER_ID,
                    document=f,
                    filename=file_name,
                    caption=f"🤖 <b>Auto SQL Backup</b>\n📅 {timestamp}",
                    parse_mode=ParseMode.HTML
                )
    except Exception as e:
        logger.error(f"Auto backup failed: {e}")

async def restore_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🔄 Enhanced Restore Command with Validation and Preview"""
    user = update.effective_user
    
    # Security: Only owner can restore
    if user.id != OWNER_ID: 
        await update.message.reply_text("🚫 <b>Access Denied!</b>\n\nOnly bot owner can restore backups.", parse_mode=ParseMode.HTML)
        return
    
    # Check if replied to a document
    if not update.message.reply_to_message or not update.message.reply_to_message.document:
        help_text = (
            "📋 <b>How to Restore Database</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "1️⃣ Upload or forward the backup .db file\n"
            "2️⃣ Reply to that file with <code>/restore</code>\n"
            "3️⃣ Confirm the restoration\n\n"
            "⚠️ <b>Important:</b>\n"
            "• All active matches will be ended\n"
            "• Current data will be replaced\n"
            "• Only .db or .sqlite files accepted\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )
        await update.message.reply_text(help_text, parse_mode=ParseMode.HTML)
        return

    doc = update.message.reply_to_message.document

    # Security: Check file extension
    if not doc.file_name.endswith(('.db', '.sqlite', '.sqlite3')):
        await update.message.reply_text(
            "⚠️ <b>Invalid File Type!</b>\n\n"
            "Only <code>.db</code>, <code>.sqlite</code>, or <code>.sqlite3</code> files are allowed.\n\n"
            f"Your file: <code>{doc.file_name}</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check for active matches
    if active_matches:
        active_count = len(active_matches)
        await update.message.reply_text(
            f"⚠️ <b>Cannot Restore!</b>\n\n"
            f"🎮 <b>{active_count}</b> active match(es) detected.\n\n"
            f"Please end all matches first using:\n"
            f"• /endmatch (in each group)\n"
            f"• /resetmatch (owner only)\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Check for active auctions
    if active_auctions:
        auction_count = len(active_auctions)
        await update.message.reply_text(
            f"⚠️ <b>Cannot Restore!</b>\n\n"
            f"🎪 <b>{auction_count}</b> active auction(s) detected.\n\n"
            f"Please end all auctions first using /endauction\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━",
            parse_mode=ParseMode.HTML
        )
        return
    
    status = await update.message.reply_text("⏳ <b>Validating backup file...</b>", parse_mode=ParseMode.HTML)

    try:
        # Download and validate file
        new_file = await doc.get_file()
        temp_path = f"temp_restore_{int(time.time())}.db"
        await new_file.download_to_drive(temp_path)
        
        # Validate database file
        try:
            conn = sqlite3.connect(temp_path)
            c = conn.cursor()
            
            # Check if it's a valid database
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in c.fetchall()]
            
            if not tables:
                os.remove(temp_path)
                await status.edit_text(
                    "❌ <b>Invalid Database!</b>\n\n"
                    "The file appears to be empty or corrupted.",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Get database statistics
            stats = {}
            try:
                c.execute('SELECT COUNT(DISTINCT user_id) FROM user_stats')
                stats['users'] = c.fetchone()[0] or 0
            except:
                stats['users'] = 0
            
            try:
                c.execute('SELECT SUM(matches_played) FROM user_stats')
                stats['matches'] = c.fetchone()[0] or 0
            except:
                stats['matches'] = 0
            
            try:
                c.execute('SELECT COUNT(*) FROM groups')
                stats['groups'] = c.fetchone()[0] or 0
            except:
                stats['groups'] = 0
            
            conn.close()
            
            # Show preview and ask for confirmation
            file_size = os.path.getsize(temp_path)
            size_mb = round(file_size / (1024 * 1024), 2)
            
            preview = (
                f"📋 <b>Backup File Preview</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📝 <b>File:</b> <code>{doc.file_name}</code>\n"
                f"📊 <b>Size:</b> {size_mb} MB\n"
                f"📅 <b>Tables Found:</b> {len(tables)}\n\n"
                f"<b>Data Summary:</b>\n"
                f"👥 Users: <b>{stats['users']}</b>\n"
                f"🏏 Matches: <b>{stats['matches']}</b>\n"
                f"🏘️ Groups: <b>{stats['groups']}</b>\n\n"
                f"⚠️ <b>Warning:</b>\n"
                f"• Current data will be replaced\n"
                f"• This action cannot be undone\n"
                f"• Create a backup before proceeding\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Reply with <code>CONFIRM</code> to proceed\n"
                f"or <code>CANCEL</code> to abort"
            )
            
            # Store temp file path in context
            context.user_data['temp_restore_file'] = temp_path
            context.user_data['restore_confirmed'] = False
            
            await status.edit_text(preview, parse_mode=ParseMode.HTML)
            
        except sqlite3.Error as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            await status.edit_text(
                f"❌ <b>Database Error!</b>\n\n"
                f"The file is corrupted or not a valid SQLite database.\n\n"
                f"Error: <code>{str(e)}</code>",
                parse_mode=ParseMode.HTML
            )
            return
        
    except Exception as e:
        logger.error(f"Restore validation failed: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        await status.edit_text(
            f"❌ <b>Restore Failed!</b>\n\n"
            f"Error: <code>{str(e)}</code>",
            parse_mode=ParseMode.HTML
        )

async def handle_restore_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle restore confirmation"""
    user = update.effective_user
    
    if user.id != OWNER_ID:
        return
    
    if 'temp_restore_file' not in context.user_data:
        return
    
    text = update.message.text.strip().upper()
    
    if text == "CONFIRM":
        temp_path = context.user_data['temp_restore_file']
        
        if not os.path.exists(temp_path):
            await update.message.reply_text("❌ Temporary file not found. Please try again.")
            return
        
        status = await update.message.reply_text("⏳ <b>Restoring database...</b>", parse_mode=ParseMode.HTML)
        
        try:
            # Backup current database before restore (safety measure)
            safety_backup = f"pre_restore_backup_{int(time.time())}.db"
            if os.path.exists(DB_FILE):
                shutil.copy(DB_FILE, safety_backup)
            
            # Replace database
            shutil.move(temp_path, DB_FILE)
            
            # Clear and reload data
            global user_data, match_history, player_stats, achievements, registered_groups
            user_data = {}
            match_history = []
            player_stats = {}
            achievements = {}
            registered_groups = {}
            
            load_data()
            
            await status.edit_text(
                f"✅ <b>Restore Complete!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔄 Database restored successfully\n"
                f"💾 Safety backup saved: <code>{safety_backup}</code>\n\n"
                f"⚡ All data has been reloaded.\n"
                f"━━━━━━━━━━━━━━━━━━━━━━",
                parse_mode=ParseMode.HTML
            )
            
            # Clear context
            context.user_data.clear()
            
        except Exception as e:
            logger.error(f"Restore execution failed: {e}")
            
            # Restore from safety backup if exists
            if os.path.exists(safety_backup):
                shutil.move(safety_backup, DB_FILE)
                load_data()
                await status.edit_text(
                    f"❌ <b>Restore Failed!</b>\n\n"
                    f"Original database has been restored.\n\n"
                    f"Error: <code>{str(e)}</code>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await status.edit_text(
                    f"❌ <b>Critical Error!</b>\n\n"
                    f"Error: <code>{str(e)}</code>",
                    parse_mode=ParseMode.HTML
                )
    
    elif text == "CANCEL":
        temp_path = context.user_data.get('temp_restore_file')
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        
        context.user_data.clear()
        
        await update.message.reply_text(
            "❌ <b>Restore Cancelled</b>\n\n"
            "No changes were made to the database.",
            parse_mode=ParseMode.HTML
        )

async def bug_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    🛠️ Report a Bug to Bot Owner
    Usage: /bug <description>
    or Reply to a message with /bug
    """
    user = update.effective_user
    chat = update.effective_chat
    
    # Check if there's a description
    bug_text = " ".join(context.args) if context.args else None
    
    # If replied to a message, include that context
    reply_context = ""
    if update.message.reply_to_message:
        reply_msg = update.message.reply_to_message
        reply_context = f"\n\n📌 <b>Reply Context:</b>\n{reply_msg.text[:200] if reply_msg.text else 'Media/File'}"
    
    if not bug_text and not reply_context:
        await update.message.reply_text(
            "🐛 <b>Report a Bug</b>\n\n"
            "<b>Usage:</b>\n"
            "• <code>/bug [description]</code>\n"
            "• Reply to error message with <code>/bug</code>\n\n"
            "<b>Example:</b>\n"
            "<code>/bug Wicket not counting properly</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Build report
    user_tag = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
    
    report = f"🚨 <b>BUG REPORT</b> 🚨\n"
    report += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    report += f"👤 <b>User:</b> {user_tag}\n"
    report += f"🆔 <b>User ID:</b> <code>{user.id}</code>\n"
    report += f"💬 <b>Chat:</b> {chat.title if chat.title else 'Private'}\n"
    report += f"🆔 <b>Chat ID:</b> <code>{chat.id}</code>\n"
    report += f"📅 <b>Date:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    if bug_text:
        report += f"📝 <b>Description:</b>\n{bug_text}\n"
    
    if reply_context:
        report += reply_context
    
    report += "\n\n━━━━━━━━━━━━━━━━━━━━━━"
    
    # Send to owner
    try:
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=report,
            parse_mode=ParseMode.HTML
        )
        
        # Confirm to user
        await update.message.reply_text(
            "✅ <b>Bug Reported!</b>\n\n"
            "Thank you for reporting. The developer has been notified.\n"
            "We'll fix it ASAP! 🛠️",
            parse_mode=ParseMode.HTML
        )
        
        logger.info(f"🐞 Bug reported by {user.first_name} (ID: {user.id})")
        
    except Exception as e:
        logger.error(f"❌ Failed to send bug report: {e}")
        await update.message.reply_text(
            "❌ Failed to send report. Please try again later.",
            parse_mode=ParseMode.HTML
        )

async def endmatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End the current match (host/admin only)"""
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("❌ Use this in a group!")
        return

    group_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    match = active_matches.get(group_id)
    
    if not match:
        await update.message.reply_text("❌ No active match to end!")
        return
    
    try:
        member = await update.effective_chat.get_member(user_id)
        is_admin = member.status in ["creator", "administrator"]
    except:
        is_admin = False
    
    is_host = (user_id == match.host_id)
    is_owner = (user_id == OWNER_ID)
    
    if not (is_host or is_admin or is_owner):
        await update.message.reply_text("❌ Only host, admins, or bot owner can end the match!")
        return
    
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, End Match", callback_data="confirm_endmatch"),
            InlineKeyboardButton("❌ Cancel", callback_data="cancel_endmatch")
        ]
    ])
    
    await update.message.reply_text(
        "⚠️ Are you sure you want to end this match?\n\n"
        "This will terminate the current game without saving stats.",
        reply_markup=keyboard
    )

async def changehost_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Change Host System: Both Captains must vote YES
    Usage: Reply to a player with /changehost
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Only works in Team Edit or Match Phases
    if match.phase not in [GamePhase.TEAM_EDIT, GamePhase.MATCH_IN_PROGRESS]:
        await update.message.reply_text("⚠️ Host change only allowed during Team Edit or Match.")
        return
    
    # Check if user is a captain
    captain_x = match.team_x.captain_id
    captain_y = match.team_y.captain_id
    
    if user.id not in [captain_x, captain_y]:
        await update.message.reply_text("🚫 Only Captains can initiate host change!")
        return
    
    # Check if replied to someone
    if not update.message.reply_to_message:
        await update.message.reply_text("⚠️ Reply to the player you want to make Host.")
        return
    
    new_host = update.message.reply_to_message.from_user
    new_host_id = new_host.id
    
    # Can't make self host if already host
    if new_host_id == match.host_id:
        await update.message.reply_text("⚠️ This player is already the Host!")
        return
    
    # Check if new host is in match
    if not (match.team_x.get_player(new_host_id) or match.team_y.get_player(new_host_id)):
        await update.message.reply_text("⚠️ This player is not in any team!")
        return
    
    # Initialize vote tracking for this candidate
    if new_host_id not in match.host_change_votes:
        match.host_change_votes[new_host_id] = {"x_voted": False, "y_voted": False}
    
    # Record vote
    votes = match.host_change_votes[new_host_id]
    
    if user.id == captain_x:
        if votes["x_voted"]:
            await update.message.reply_text("⚠️ Team X Captain already voted for this change!")
            return
        votes["x_voted"] = True
        voter_team = "Team X"
    else:
        if votes["y_voted"]:
            await update.message.reply_text("⚠️ Team Y Captain already voted for this change!")
            return
        votes["y_voted"] = True
        voter_team = "Team Y"
    
    new_host_tag = get_user_tag(new_host)
    
    # Check if both captains voted
    if votes["x_voted"] and votes["y_voted"]:
        # ✅ HOST CHANGE APPROVED
        old_host_id = match.host_id
        old_host_name = match.host_name
        
        match.host_id = new_host_id
        match.host_name = new_host.first_name
        
        # Clear votes
        match.host_change_votes = {}
        
        msg = f"👑 <b>HOST CHANGED!</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"🔄 <b>Old Host:</b> {old_host_name}\n"
        msg += f"✅ <b>New Host:</b> {new_host_tag}\n\n"
        msg += f"<i>Both captains approved this change.</i>"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)
    else:
        # Waiting for second vote
        pending = "Team Y Captain" if not votes["y_voted"] else "Team X Captain"
        
        msg = f"🗳 <b>HOST CHANGE VOTE</b>\n"
        msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📋 <b>Candidate:</b> {new_host_tag}\n"
        msg += f"✅ <b>{voter_team} Captain</b> voted YES\n"
        msg += f"⏳ <b>Waiting for:</b> {pending}\n\n"
        msg += f"<i>{pending}, reply to {new_host.first_name} with /changehost to approve.</i>"
        
        await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def changecap_x_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Change Captain of Team X (Host Only)
    Usage: Reply to a Team X player with /changecap_X
    """
    await change_captain_logic(update, context, "X")

async def changecap_y_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Change Captain of Team Y (Host Only)
    Usage: Reply to a Team Y player with /changecap_Y
    """
    await change_captain_logic(update, context, "Y")

async def change_captain_logic(update: Update, context: ContextTypes.DEFAULT_TYPE, team_name: str):
    """
    Unified Captain Change Logic
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Only Host can change captains
    if user.id != match.host_id:
        await update.message.reply_text("🚫 Only the Host can change captains!")
        return
    
    # Check phase
    if match.phase not in [GamePhase.TEAM_EDIT, GamePhase.CAPTAIN_SELECTION, GamePhase.MATCH_IN_PROGRESS]:
        await update.message.reply_text("⚠️ Captain change not allowed in this phase.")
        return
    
    # Check if replied to someone
    if not update.message.reply_to_message:
        await update.message.reply_text(f"⚠️ Reply to the new captain of Team {team_name}.")
        return
    
    new_captain = update.message.reply_to_message.from_user
    new_captain_id = new_captain.id
    
    # Get team
    team = match.team_x if team_name == "X" else match.team_y
    
    # Check if player is in this team
    if not team.get_player(new_captain_id):
        await update.message.reply_text(f"⚠️ This player is not in Team {team_name}!")
        return
    
    # Check if already captain
    if team.captain_id == new_captain_id:
        await update.message.reply_text(f"⚠️ {new_captain.first_name} is already the captain!")
        return
    
    # Get old captain name
    old_captain = team.get_player(team.captain_id)
    old_captain_name = old_captain.first_name if old_captain else "None"
    
    # Change captain
    team.captain_id = new_captain_id
    
    new_captain_tag = get_user_tag(new_captain)
    
    msg = f"🧢 <b>CAPTAIN CHANGE - TEAM {team_name}</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔄 <b>Old Captain:</b> {old_captain_name}\n"
    msg += f"✅ <b>New Captain:</b> {new_captain_tag}\n\n"
    msg += f"<i>Host has updated the leadership.</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def impact_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ FIXED: Impact Player with proper validation
    Usage: /impact @old_player @new_player
    or Reply to old player with: /impact @new_player
    """
    chat = update.effective_chat
    user = update.effective_user
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match found.")
        return
    
    match = active_matches[chat.id]
    
    # Only during match
    if match.phase != GamePhase.MATCH_IN_PROGRESS:
        await update.message.reply_text("⚠️ Impact Player can only be used during the match!")
        return
    
    # Check if user is captain
    captain_x = match.team_x.captain_id
    captain_y = match.team_y.captain_id
    
    if user.id not in [captain_x, captain_y]:
        await update.message.reply_text("👮‍♂️ Only Captains can use Impact Player!")
        return
    
    # Determine captain's team
    if user.id == captain_x:
        team = match.team_x
        team_name = "Team X"
        impact_count = match.team_x_impact_count
        impact_history = match.team_x_impact_history
    else:
        team = match.team_y
        team_name = "Team Y"
        impact_count = match.team_y_impact_count
        impact_history = match.team_y_impact_history
    
    # ✅ Check if 3 uses exhausted
    if impact_count >= 3:
        await update.message.reply_text(
            f"🛑 <b>{team_name} has used all 3 Impact Players!</b>\n\n"
            f"📋 <b>Substitutions Made:</b>\n"
            f"{format_impact_history(impact_history)}",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Parse targets
    old_player_id = None
    new_player_id = None
    
    # Method 1: Reply + Mention
    if update.message.reply_to_message:
        old_player_id = update.message.reply_to_message.from_user.id
        
        # Get new player from mention
        if update.message.entities:
            for entity in update.message.entities:
                if entity.type == "mention":
                    username = update.message.text[entity.offset:entity.offset + entity.length].replace("@", "")
                    # Find user by username
                    for uid, data in user_data.items():
                        if data.get("username", "").lower() == username.lower():
                            new_player_id = uid
                            break
                elif entity.type == "text_mention":
                    new_player_id = entity.user.id
    
    # Method 2: Two Mentions
    elif update.message.entities and len([e for e in update.message.entities if e.type in ["mention", "text_mention"]]) >= 2:
        mentions = [e for e in update.message.entities if e.type in ["mention", "text_mention"]]
        
        # First mention = old player
        if mentions[0].type == "text_mention":
            old_player_id = mentions[0].user.id
        else:
            username = update.message.text[mentions[0].offset:mentions[0].offset + mentions[0].length].replace("@", "")
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username.lower():
                    old_player_id = uid
                    break
        
        # Second mention = new player
        if mentions[1].type == "text_mention":
            new_player_id = mentions[1].user.id
        else:
            username = update.message.text[mentions[1].offset:mentions[1].offset + mentions[1].length].replace("@", "")
            for uid, data in user_data.items():
                if data.get("username", "").lower() == username.lower():
                    new_player_id = uid
                    break
    
    if not old_player_id or not new_player_id:
        remaining = 3 - impact_count
        await update.message.reply_text(
            f"ℹ️ <b>Usage:</b>\n"
            f"Reply to old player: <code>/impact @newplayer</code>\n"
            f"Or: <code>/impact @oldplayer @newplayer</code>\n\n"
            f"🔄 <b>Remaining Substitutions:</b> {remaining}/3",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Validate old player is in team
    old_player = team.get_player(old_player_id)
    if not old_player:
        await update.message.reply_text("🚫 Old player is not in your team!")
        return
    
    # Check if old player was already substituted out
    for old_name, new_name in impact_history:
        if old_name == old_player.first_name:
            await update.message.reply_text(
                f"🚫 <b>{old_player.first_name}</b> was already substituted out earlier!\n"
                f"You cannot substitute them again.",
                parse_mode=ParseMode.HTML
            )
            return
    
    # Check if old player is currently playing
    if team == match.current_batting_team:
        if team.current_batsman_idx is not None and team.players[team.current_batsman_idx].user_id == old_player_id:
            await update.message.reply_text("🏏 Cannot substitute the current striker!")
            return
        if team.current_non_striker_idx is not None and team.players[team.current_non_striker_idx].user_id == old_player_id:
            await update.message.reply_text("👟 Cannot substitute the current non-striker!")
            return
    
    if team == match.current_bowling_team:
        if team.current_bowler_idx is not None and team.players[team.current_bowler_idx].user_id == old_player_id:
            await update.message.reply_text("⚾ Cannot substitute the current bowler!")
            return
    
    # Check if new player is not already in match
    if match.team_x.get_player(new_player_id) or match.team_y.get_player(new_player_id):
        await update.message.reply_text("👥 New player is already in a team!")
        return
    
    # Initialize new player stats
    if new_player_id not in user_data:
        # Fetch user info
        try:
            new_user = await context.bot.get_chat(new_player_id)
            user_data[new_player_id] = {
                "user_id": new_player_id,
                "username": new_user.username or "",
                "first_name": new_user.first_name,
                "started_at": datetime.now().isoformat(),
                "total_matches": 0
            }
            init_player_stats(new_player_id)
            save_data()
        except:
            await update.message.reply_text("📡 Cannot fetch new player info. Make sure they've started the bot.")
            return
    
    new_user_info = user_data[new_player_id]
    
    # Create new player object
    new_player = Player(new_player_id, new_user_info["username"], new_user_info["first_name"])
    
    # Replace in team
    for i, p in enumerate(team.players):
        if p.user_id == old_player_id:
            team.players[i] = new_player
            break
    
    # ✅ Update impact tracking
    if user.id == captain_x:
        match.team_x_impact_count += 1
        match.team_x_impact_history.append((old_player.first_name, new_player.first_name))
        remaining = 3 - match.team_x_impact_count
    else:
        match.team_y_impact_count += 1
        match.team_y_impact_history.append((old_player.first_name, new_player.first_name))
        remaining = 3 - match.team_y_impact_count
    
    old_tag = f"<a href='tg://user?id={old_player_id}'>{old_player.first_name}</a>"
    new_tag = get_user_tag(type("User", (), {"id": new_player_id, "first_name": new_user_info["first_name"]}))
    
    msg = f"🔄 <b>IMPACT PLAYER - {team_name}</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"🔴 <b>OUT:</b> {old_tag}\n"
    msg += f"🟢 <b>IN:</b> {new_tag}\n\n"
    msg += f"📊 <b>Substitutions Used:</b> {impact_count + 1}/3\n"
    msg += f"⏳ <b>Remaining:</b> {remaining}\n\n"
    msg += f"💡 <i>Strategic substitution made!</i>"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

def format_impact_history(history: list) -> str:
    """Format impact player history for display"""
    if not history:
        return "<i>No substitutions made yet</i>"
    
    result = ""
    for i, (old, new) in enumerate(history, 1):
        result += f"  {i}. {old} ➡️ {new}\n"
    return result

async def impactstatus_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Check Impact Player Status
    Shows remaining substitutions for both teams
    """
    chat = update.effective_chat
    
    if chat.id not in active_matches:
        await update.message.reply_text("❌ No active match.")
        return
    
    match = active_matches[chat.id]
    
    # Team X Status
    x_used = match.team_x_impact_count
    x_remaining = 3 - x_used
    x_history = format_impact_history(match.team_x_impact_history)
    
    # Team Y Status
    y_used = match.team_y_impact_count
    y_remaining = 3 - y_used
    y_history = format_impact_history(match.team_y_impact_history)
    
    msg = f"🔄 <b>IMPACT PLAYER STATUS</b>\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    msg += f"🧊 <b>Team X</b>\n"
    msg += f"📊 Used: <b>{x_used}/3</b> | Remaining: <b>{x_remaining}</b>\n"
    msg += f"📋 History:\n{x_history}\n\n"
    
    msg += f"🔥 <b>Team Y</b>\n"
    msg += f"📊 Used: <b>{y_used}/3</b> | Remaining: <b>{y_remaining}</b>\n"
    msg += f"📋 History:\n{y_history}\n\n"
    
    msg += f"━━━━━━━━━━━━━━━━━━━━━"
    
    await update.message.reply_text(msg, parse_mode=ParseMode.HTML)

async def resetmatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /resetmatch command - Owner only"""
    user = update.effective_user
    
    # Check if owner
    if user.id != OWNER_ID:
        await update.message.reply_text(
            "This command is restricted to the bot owner."
        )
        return
    
    chat = update.effective_chat
    
    if chat.id not in active_matches:
        await update.message.reply_text("No active match in this group.")
        return
    
    match = active_matches[chat.id]
    
    # Cancel all tasks
    if match.join_phase_task:
        match.join_phase_task.cancel()
    if match.ball_timeout_task:
        match.ball_timeout_task.cancel()
    if match.batsman_selection_task:
        match.batsman_selection_task.cancel()
    if match.bowler_selection_task:
        match.bowler_selection_task.cancel()
    
    # Reset match to team joining phase
    del active_matches[chat.id]
    
    await update.message.reply_text(
        "Match has been reset by bot owner.\n"
        "Use /game to start a new match."
    )
    
    logger.info(f"Match in group {chat.id} reset by owner")

# Error handler
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Log errors and notify user"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Notify owner about error
    try:
        error_text = f"An error occurred:\n\n{str(context.error)}"
        await context.bot.send_message(
            chat_id=OWNER_ID,
            text=error_text
        )
    except Exception as e:
        logger.error(f"Failed to notify owner about error: {e}")

async def create_prediction_poll(context: ContextTypes.DEFAULT_TYPE, group_id: int, match: Match):
    """Create and pin prediction poll"""
    try:
        poll_message = await context.bot.send_poll(
            chat_id=group_id,
            question="🎯 Who will win this match?let's see your poll",
            options=[
                f"🧊 {match.team_x.name}",
                f"🔥 {match.team_y.name}"
            ],
            is_anonymous=False,
            allows_multiple_answers=False
        )
        
        # Pin the poll
        try:
            await context.bot.pin_chat_message(
                chat_id=group_id,
                message_id=poll_message.message_id,
                disable_notification=True
            )
        except:
            pass  # If bot can't pin, continue anyway
            
    except Exception as e:
        logger.error(f"Error creating poll: {e}")



# ═══════════════════════════════════════════════════════════════
# SUPER OVER AUTO-TRIGGER FUNCTION
# ═══════════════════════════════════════════════════════════════

async def check_and_trigger_super_over(update, context, group_id, match):
    """Check if match is draw and trigger super over automatically"""
    
    # Only check after second innings
    if match.innings != 2:
        return False
    
    # Get team scores
    team_x_total = match.team_x.score if hasattr(match.team_x, 'score') else match.team_x_total_runs
    team_y_total = match.team_y.score if hasattr(match.team_y, 'score') else match.team_y_total_runs
    
    # Check if scores are equal (DRAW)
    if team_x_total != team_y_total:
        return False
    
    # IT'S A DRAW! Trigger Super Over
    logger.info(f"Match tied in group {group_id}: {team_x_total} - {team_y_total}")
    
    try:
        await context.bot.send_animation(
            chat_id=group_id,
            animation="CgACAgUAAyEFAATEuZi2AAIFvGlL8zO3j9sAAWMLd_N0LVWTjVPHnAACrBsAAunGYFZlcYBvYMHr9TYE",
            caption=(
                "🏏 *MATCH TIED!* 🏏\n\n"
                "🔥 *SUPER OVER ACTIVATED!* 🔥\n\n"
                f"Both teams scored *{team_x_total} runs*!\n"
                "Get ready for the most exciting over in cricket! 🎯\n\n"
                "📋 *Super Over Rules:*\n"
                "  • Each team plays 1 over (6 balls)\n"
                "  • Maximum 2 wickets allowed\n"
                "  • Highest score wins! 🏆\n\n"
                "⏳ Starting in 5 seconds..."
            ),
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Error sending super over announcement: {e}")
    
    # Update match phase to SUPER_OVER
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE matches 
            SET phase = ?
            WHERE group_id = ?""",
            (GamePhase.SUPER_OVER.value, group_id)
        )
        conn.commit()
        conn.close()
        
        # Update in-memory match object
        match.phase = GamePhase.SUPER_OVER
        
    except Exception as e:
        logger.error(f"Error updating match to super over: {e}")
    
    # Dramatic pause
    await asyncio.sleep(5)
    
    return True  # Super over triggered

async def handle_group_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Unified Handle Group Input
    Handles logic for both SOLO and TEAM modes.
    """
    
    # --- 1. Basic Validations ---
    if update.effective_chat.type == "private": return
    if not update.message or not update.message.text: return
    
    text = update.message.text.strip()
    if not text.isdigit(): return
    
    number = int(text)
    if number < 0 or number > 6: return # Ignore invalid numbers
    
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    if chat_id not in active_matches: return
    match = active_matches[chat_id]
    match.last_activity = time.time()
    
    # Safety Check: Default to 'TEAM' if game_mode attribute is missing (backward compatibility)
    current_mode = getattr(match, "game_mode", "TEAM")
    
    processed = False

    # ==========================================
    # ⚔️ CASE 1: SOLO MODE LOGIC (UPDATED)
    # ==========================================
    if current_mode == "SOLO" and match.phase == GamePhase.SOLO_MATCH:
        if match.current_solo_bat_idx < len(match.solo_players):
            batter = match.solo_players[match.current_solo_bat_idx]
            
            if user_id == batter.user_id:
                if match.current_ball_data.get("bowler_number") is not None:
                    match.current_ball_data["batsman_number"] = number
                    processed = True
                    
                    # ✅ STOP BATSMAN TIMER
                    if match.ball_timeout_task: 
                        match.ball_timeout_task.cancel()
                    
                    try: 
                        await update.message.delete()
                    except: 
                        pass
                    
                    await process_solo_turn_result(context, chat_id, match)

    # ==========================================
    # 👥 CASE 2: TEAM MODE LOGIC
    # ==========================================
    elif current_mode == "TEAM" and match.phase == GamePhase.MATCH_IN_PROGRESS:
        batting_team = match.current_batting_team
        bowling_team = match.current_bowling_team
        
        # Check if teams and indices are valid
        if (batting_team.current_batsman_idx is not None and 
            bowling_team.current_bowler_idx is not None):
            
            striker = batting_team.players[batting_team.current_batsman_idx]
            bowler = bowling_team.players[bowling_team.current_bowler_idx]
            
            # Sub-Case A: Bowler sent number in Group (Backup for DM)
            if user_id == bowler.user_id:
                if match.current_ball_data.get("bowler_number") is None:
                    match.current_ball_data["bowler_number"] = number
                    
                    await context.bot.send_message(chat_id, f"⚾ <b>{bowler.first_name}</b> has bowled!", parse_mode=ParseMode.HTML)
                    
                    # Cancel timeout & Request Batsman
                    if match.ball_timeout_task: match.ball_timeout_task.cancel()
                    await request_batsman_number(context, chat_id, match)
                    processed = True

            # Sub-Case B: Striker sent number (The Shot)
            elif user_id == striker.user_id:
                # Batsman can only play if bowler has bowled
                if match.current_ball_data.get("bowler_number") is not None:
                    # Prevent double input
                    if match.current_ball_data.get("batsman_number") is None:
                        match.current_ball_data["batsman_number"] = number
                        
                        await context.bot.send_message(chat_id, f"🏏 <b>{striker.first_name}</b> played a shot!", parse_mode=ParseMode.HTML)
                        
                        if match.ball_timeout_task: match.ball_timeout_task.cancel()
                        await process_ball_result(context, chat_id, match)
                        processed = True

async def handle_dm_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅ UPDATED: Wide ball detection for spamming same number (3 times consecutive)
    ✅ ADDED: Restore confirmation handling
    ✅ ADDED: AI Mode handling
    """
    user = update.effective_user
    if not update.message or not update.message.text: return
    
    msg = update.message.text.strip()
    
    # Check for restore confirmation (CONFIRM/CANCEL)
    if msg.upper() in ["CONFIRM", "CANCEL"] and 'temp_restore_file' in context.user_data:
        await handle_restore_confirmation(update, context)
        return
    
    # Check if awaiting tournament name
    if 'awaiting_tournament_name' in context.user_data:
        group_id = context.user_data['awaiting_tournament_name']
        tournament_name = msg
        
        # Save tournament name and ask for days
        context.user_data['tournament_name'] = tournament_name
        context.user_data['awaiting_registration_days'] = group_id
        del context.user_data['awaiting_tournament_name']
        
        await update.message.reply_text(
            f"🏏 Tournament: {tournament_name}\n\n"
            f"How many days should registration stay open?\n"
            f"Reply: `/days <number>`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    # AI MODE HANDLING
    if user.id in ai_matches:
        if not msg.isdigit():
            return
        
        num = int(msg)
        if num < 0 or num > 6:
            await update.message.reply_text("⚠️ Please send a number between 0 and 6.")
            return
        
        match = ai_matches[user.id]
        
        # User is batting
        if match["phase"] == "waiting_user" and match["innings"] == 1:
            ai_num = match.get("ai_number")
            if ai_num is None:
                return
            
            # SPAM CHECK - User batting
            if "user_spam_history" not in match:
                match["user_spam_history"] = []
            
            match["user_spam_history"].append(num)
            
            # Check last 3 numbers for spam
            is_spam = False
            if len(match["user_spam_history"]) >= 3:
                if (match["user_spam_history"][-1] == match["user_spam_history"][-2] == 
                    match["user_spam_history"][-3]):
                    is_spam = True
                    match["user_spam_history"] = []  # Reset
            
            if is_spam:
                # WIDE BALL - Don't count ball, AI gets 1 free run
                await update.message.reply_text(
                    "🚫 <b>WIDE BALL!</b>\n"
                    "⚠️ You spammed the same number 3 times!\n\n"
                    "📉 <b>Penalty:</b> +1 Run to AI\n"
                    "🔄 Ball not counted. AI will bowl again.",
                    parse_mode=ParseMode.HTML
                )
                
                # AI gets 1 free run (will be added to their chase later)
                if "ai_penalty_runs" not in match:
                    match["ai_penalty_runs"] = 0
                match["ai_penalty_runs"] += 1
                
                # AI bowls again
                await asyncio.sleep(1)
                await ai_play_ball(context, user.id)
                return
            
            # Process ball
            if num == ai_num:
                # WICKET! - Only 1 wicket = innings over
                match["user_wickets"] = 1
                match["user_balls"] += 1
                
                result_msg = f"❌ <b>OUT!</b>\n\n"
                result_msg += f"You: {num} | AI: {ai_num}\n\n"
                result_msg += f"📊 Final Score: {match['user_score']}/1\n"
                
                # Innings over (only 1 wicket)
                match["target"] = match["user_score"] + 1
                match["innings"] = 2
                match["phase"] = "waiting_ai"
                
                result_msg += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                result_msg += f"<b>YOUR INNINGS ENDED!</b>\n"
                result_msg += f"🎯 Target for AI: {match['target']}\n"
                if match.get("ai_penalty_runs", 0) > 0:
                    result_msg += f"⚠️ AI starts with {match['ai_penalty_runs']} penalty run(s)\n"
                result_msg += f"\n🎮 AI is batting now..."
                
                try:
                    # Send wicket GIF
                    wicket_gif = random.choice(GIFS[MatchEvent.WICKET])
                    await update.message.reply_animation(
                        animation=wicket_gif,
                        caption=result_msg,
                        parse_mode=ParseMode.HTML
                    )
                except:
                    await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)
                
                # Start AI batting
                await asyncio.sleep(2)
                await ai_bat_innings(context, user.id)
                return
            else:
                # RUNS!
                match["user_score"] += num
                match["user_balls"] += 1
                match["user_stats"]["runs"] += num
                match["user_stats"]["balls"] += 1
                if num == 4:
                    match["user_stats"]["fours"] += 1
                elif num == 6:
                    match["user_stats"]["sixes"] += 1
                
                result_msg = f"{'🔥' if num >= 4 else '✅'} <b>+{num} RUNS!</b>\n\n"
                result_msg += f"You: {num} | AI: {ai_num}\n\n"
                result_msg += f"📊 Score: {match['user_score']}/{match['user_wickets']} ({match['user_balls']//6}.{match['user_balls']%6})\n"
                
                # Check if overs complete
                if match["user_balls"] >= match["overs"] * 6:
                    match["target"] = match["user_score"] + 1
                    match["innings"] = 2
                    match["phase"] = "waiting_ai"
                    
                    result_msg += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    result_msg += f"<b>YOUR INNINGS ENDED!</b>\n"
                    result_msg += f"🎯 Target for AI: {match['target']}\n"
                    if match.get("ai_penalty_runs", 0) > 0:
                        result_msg += f"⚠️ AI starts with {match['ai_penalty_runs']} penalty run(s)\n"
                    result_msg += f"\n🎮 AI is batting now..."
                    
                    try:
                        # Send appropriate GIF
                        if num == 0:
                            gif = random.choice(GIFS[MatchEvent.DOT_BALL])
                        elif num == 1:
                            gif = random.choice(GIFS[MatchEvent.RUNS_1])
                        elif num == 2:
                            gif = random.choice(GIFS[MatchEvent.RUNS_2])
                        elif num == 3:
                            gif = random.choice(GIFS[MatchEvent.RUNS_3])
                        elif num == 4:
                            gif = random.choice(GIFS[MatchEvent.RUNS_4])
                        elif num == 5:
                            gif = random.choice(GIFS[MatchEvent.RUNS_5])
                        else:  # 6
                            gif = random.choice(GIFS[MatchEvent.RUNS_6])
                        
                        await update.message.reply_animation(
                            animation=gif,
                            caption=result_msg,
                            parse_mode=ParseMode.HTML
                        )
                    except:
                        await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)
                    
                    await asyncio.sleep(2)
                    await ai_bat_innings(context, user.id)
                    return
                else:
                    try:
                        # Send appropriate GIF
                        if num == 0:
                            gif = random.choice(GIFS[MatchEvent.DOT_BALL])
                        elif num == 1:
                            gif = random.choice(GIFS[MatchEvent.RUNS_1])
                        elif num == 2:
                            gif = random.choice(GIFS[MatchEvent.RUNS_2])
                        elif num == 3:
                            gif = random.choice(GIFS[MatchEvent.RUNS_3])
                        elif num == 4:
                            gif = random.choice(GIFS[MatchEvent.RUNS_4])
                        elif num == 5:
                            gif = random.choice(GIFS[MatchEvent.RUNS_5])
                        else:  # 6
                            gif = random.choice(GIFS[MatchEvent.RUNS_6])
                        
                        await update.message.reply_animation(
                            animation=gif,
                            caption=result_msg,
                            parse_mode=ParseMode.HTML
                        )
                    except:
                        await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)
                    await asyncio.sleep(1)
                    await ai_play_ball(context, user.id)
                    return
                
                # Check if overs complete
                if match["user_balls"] >= match["overs"] * 6:
                    match["target"] = match["user_score"] + 1
                    match["innings"] = 2
                    match["phase"] = "waiting_ai"
                    
                    result_msg += f"\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    result_msg += f"<b>YOUR INNINGS ENDED!</b>\n"
                    result_msg += f"🎯 Target for AI: {match['target']}\n\n"
                    result_msg += f"AI is batting now..."
                    
                    try:
                        # Send appropriate GIF
                        if num == 0:
                            gif = random.choice(GIFS[MatchEvent.DOT_BALL])
                        elif num == 1:
                            gif = random.choice(GIFS[MatchEvent.RUNS_1])
                        elif num == 2:
                            gif = random.choice(GIFS[MatchEvent.RUNS_2])
                        elif num == 3:
                            gif = random.choice(GIFS[MatchEvent.RUNS_3])
                        elif num == 4:
                            gif = random.choice(GIFS[MatchEvent.RUNS_4])
                        elif num == 5:
                            gif = random.choice(GIFS[MatchEvent.RUNS_5])
                        else:  # 6
                            gif = random.choice(GIFS[MatchEvent.RUNS_6])
                        
                        await update.message.reply_animation(
                            animation=gif,
                            caption=result_msg,
                            parse_mode=ParseMode.HTML
                        )
                    except:
                        await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)
                    
                    await asyncio.sleep(2)
                    await ai_bat_innings(context, user.id)
                    return
                else:
                    try:
                        # Send appropriate GIF
                        if num == 0:
                            gif = random.choice(GIFS[MatchEvent.DOT_BALL])
                        elif num == 1:
                            gif = random.choice(GIFS[MatchEvent.RUNS_1])
                        elif num == 2:
                            gif = random.choice(GIFS[MatchEvent.RUNS_2])
                        elif num == 3:
                            gif = random.choice(GIFS[MatchEvent.RUNS_3])
                        elif num == 4:
                            gif = random.choice(GIFS[MatchEvent.RUNS_4])
                        elif num == 5:
                            gif = random.choice(GIFS[MatchEvent.RUNS_5])
                        else:  # 6
                            gif = random.choice(GIFS[MatchEvent.RUNS_6])
                        
                        await update.message.reply_animation(
                            animation=gif,
                            caption=result_msg,
                            parse_mode=ParseMode.HTML
                        )
                    except:
                        await update.message.reply_text(result_msg, parse_mode=ParseMode.HTML)
                    await asyncio.sleep(1)
                    await ai_play_ball(context, user.id)
                    return
        
        # User is bowling (AI batting)
        elif match["phase"] == "waiting_user" and match["innings"] == 2:
            # SPAM CHECK - User bowling
            if "user_spam_history" not in match:
                match["user_spam_history"] = []
            
            match["user_spam_history"].append(num)
            
            # Check last 3 numbers for spam
            is_spam = False
            if len(match["user_spam_history"]) >= 3:
                if (match["user_spam_history"][-1] == match["user_spam_history"][-2] == 
                    match["user_spam_history"][-3]):
                    is_spam = True
                    match["user_spam_history"] = []  # Reset
            
            if is_spam:
                # WIDE BALL - Don't count ball, user gets 1 free run to AI
                match["ai_score"] += 1
                
                await update.message.reply_text(
                    "🚫 <b>WIDE BALL!</b>\n"
                    "⚠️ You spammed the same number 3 times!\n\n"
                    "📉 <b>Penalty:</b> +1 Run to AI\n"
                    f"📊 AI: {match['ai_score']}/{match['ai_wickets']}\n\n"
                    "🔄 Ball not counted. Bowl again.",
                    parse_mode=ParseMode.HTML
                )
                
                # Check if AI won via wide
                if match["ai_score"] >= match["target"]:
                    await ai_end_match(context, user.id, "ai_won")
                    return
                
                # User bowls again (no ball counted)
                return
            
            await update.message.reply_text(f"⚾ <b>You bowled:</b> {num}\n\nAI is playing...", parse_mode=ParseMode.HTML)
            await asyncio.sleep(1)
            await ai_bat_ball(context, user.id, num)
            return
        
        return
    
    if not msg.isdigit(): return
    num = int(msg)
    if num < 0 or num > 6:
        await update.message.reply_text("⚠️ Please send a number between 0 and 6.")
        return

    for gid, match in active_matches.items():
        current_mode = getattr(match, "game_mode", "TEAM") 
        
        # --- SOLO MODE LOGIC (No Change) ---
        if current_mode == "SOLO" and match.phase == GamePhase.SOLO_MATCH:
            if match.current_solo_bowl_idx < len(match.solo_players):
                bowler = match.solo_players[match.current_solo_bowl_idx]
                
                if user.id == bowler.user_id and match.current_ball_data.get("bowler_number") is None:
                    match.current_ball_data["bowler_number"] = num
                    if match.ball_timeout_task: match.ball_timeout_task.cancel()
                    
                    await update.message.reply_text(f"✅ <b>Locked:</b> {num}", parse_mode=ParseMode.HTML)
                    
                    # Notify Batsman Logic
                    if match.current_solo_bat_idx < len(match.solo_players):
                        batter = match.solo_players[match.current_solo_bat_idx]
                        sr = round((batter.runs / max(batter.balls_faced, 1)) * 100, 1)
                        notification_msg = (
                            f"🔴 <b>LIVE</b>\n─────────────────────\n"
                            f"📊 <b>{batter.first_name}:</b> {batter.runs} ({batter.balls_faced}) | ⚡ SR: {sr}\n"
                            f"🏏 <b><a href='tg://user?id={batter.user_id}'>{batter.first_name}</a></b>, play your shot!\n"
                            f"─────────────────────"
                        )
                        try:
                            await context.bot.send_animation(gid, "https://t.me/kyanaamrkhe/7", caption=notification_msg, parse_mode=ParseMode.HTML)
                        except:
                            await context.bot.send_message(gid, notification_msg, parse_mode=ParseMode.HTML)
                            
                        match.ball_timeout_task = asyncio.create_task(
                            solo_game_timer(context, gid, match, "batsman", batter.first_name)
                        )
                    return

        # --- TEAM MODE (WIDE BALL LOGIC ADDED HERE) ---
        elif current_mode == "TEAM" and match.phase == GamePhase.MATCH_IN_PROGRESS:
             if match.current_bowling_team and match.current_bowling_team.current_bowler_idx is not None:
                 bowler = match.current_bowling_team.players[match.current_bowling_team.current_bowler_idx]
                 
                 if user.id == bowler.user_id:
                     if match.current_ball_data.get("bowler_number") is None:
                        
                        # 🚫 WIDE BALL CHECK LOGIC STARTS
                        # Initialize history list if not exists
                        if not hasattr(bowler, 'spam_history'):
                            bowler.spam_history = []
                        
                        # Add current number
                        bowler.spam_history.append(num)
                        
                        # Check last 3 numbers
                        is_wide = False
                        if len(bowler.spam_history) >= 3:
                            # Check if last 3 are same (e.g. 4, 4, 4)
                            if bowler.spam_history[-1] == bowler.spam_history[-2] == bowler.spam_history[-3]:
                                is_wide = True
                                # Reset history so 4th ball counts as fresh start
                                bowler.spam_history = [] 

                        if is_wide:
                            # 🚫 IT IS A WIDE!
                            await update.message.reply_text(
                                "🚫 <b>WIDE BALL!</b>\n"
                                "⚠️ <b>Reason:</b> You spammed the same number 3 times!\n"
                                "🎲 Ball cancelled. Bowl again.\n\n"
                                "📉 <b>Penalty:</b> +1 Run to Batting Team.",
                                parse_mode=ParseMode.HTML
                            )
                            
                            # Add Score
                            match.current_batting_team.score += 1
                            match.current_batting_team.extras += 1
                            
                            # Notify Group
                            await context.bot.send_message(
                                gid,
                                f"🚫 <b>WIDE BALL!</b> {bowler.first_name} bowled the same number 3 times!\n"
                                f"📊 <b>Score:</b> {match.current_batting_team.score}/{match.current_batting_team.wickets}\n"
                                f"🔄 <i>Bowler must bowl again...</i>",
                                parse_mode=ParseMode.HTML
                            )
                            
                            # Check if Target Chased via Wide
                            if match.innings == 2 and match.current_batting_team.score >= match.target:
                                await end_innings(context, gid, match)
                                return

                            # Reset Ball Data for Re-bowl
                            match.current_ball_data = {} 
                            
                            # Restart Timer
                            if match.ball_timeout_task: match.ball_timeout_task.cancel()
                            match.ball_timeout_task = asyncio.create_task(
                                game_timer(context, gid, match, "bowler", bowler.first_name)
                            )
                            
                            # Trigger execute_ball again (Animation)
                            await asyncio.sleep(1)
                            await execute_ball(context, gid, match)
                            return
                        
                        # ✅ NORMAL DELIVERY (NOT WIDE)
                        match.current_ball_data["bowler_number"] = num
                        match.current_ball_data["bowler_id"] = user.id
                        
                        if match.ball_timeout_task: match.ball_timeout_task.cancel()
                        
                        await update.message.reply_text(f"✅ <b>Delivery Locked:</b> {num}", parse_mode=ParseMode.HTML)
                        
                        # Notify Batsman
                        bat_team = match.current_batting_team
                        if bat_team.current_batsman_idx is not None:
                            striker = bat_team.players[bat_team.current_batsman_idx]
                            sr = round((striker.runs / max(striker.balls_faced, 1)) * 100, 1)
                            striker_tag = f"<a href='tg://user?id={striker.user_id}'>{striker.first_name}</a>"
                            
                            notification_msg = (
                                f"🔴 <b>LIVE</b>\n─────────────────────\n"
                                f"📊 <b>{striker.first_name}:</b> {striker.runs} ({striker.balls_faced}) | ⚡ SR: {sr}\n"
                                f"🏏 <b>{striker_tag}</b>, play your shot!\n─────────────────────"
                            )

                            try:
                                await context.bot.send_animation(gid, "https://t.me/kyanaamrkhe/7", caption=notification_msg, parse_mode=ParseMode.HTML)
                            except:
                                await context.bot.send_message(gid, notification_msg, parse_mode=ParseMode.HTML)
                            
                            match.ball_timeout_task = asyncio.create_task(
                                game_timer(context, gid, match, "batsman", striker.first_name)
                            )
                     return


def calculate_momentum_change(match: 'Match', runs: int, is_wicket: bool, is_boundary: bool):
    """Calculate momentum change based on ball result"""
    delta = 0
    
    batting_team = match.current_batting_team
    is_team_x_batting = (batting_team == match.team_x)
    
    if is_wicket:
        delta = -15 if is_team_x_batting else +15
    elif is_boundary:
        delta = +12 if is_team_x_batting else -12
    elif runs == 0:
        delta = -3 if is_team_x_batting else +3
    else:
        delta = runs * 2 if is_team_x_batting else -runs * 2
    
    # Track last 6 balls
    match.last_6_balls.append({'runs': runs, 'wicket': is_wicket})
    if len(match.last_6_balls) > 6:
        match.last_6_balls.pop(0)
    
    # Bonus: 3 consecutive boundaries
    if len(match.last_6_balls) >= 3:
        last_3 = match.last_6_balls[-3:]
        if all(b.get('runs', 0) in [4, 6] for b in last_3):
            delta += 10 if is_team_x_batting else -10
    
    # Update momentum (cap at -100 to +100)
    match.current_momentum = max(-100, min(100, match.current_momentum + delta))
    
    # Track history
    total_balls = match.current_bowling_team.balls if match.current_bowling_team else 0
    match.momentum_history.append((total_balls, match.current_momentum))


def determine_strike_zone(runs: int) -> str:
    """Determine which zone the runs came from"""
    import random
    
    zones_by_runs = {
        0: ['straight'],
        1: ['mid_wicket', 'cover', 'point', 'square_leg'],
        2: ['mid_wicket', 'cover', 'fine_leg'],
        3: ['mid_wicket', 'cover', 'square_leg'],
        4: ['cover', 'point', 'mid_wicket', 'fine_leg'],
        6: ['long_on', 'long_off', 'straight', 'mid_wicket']
    }
    
    possible_zones = zones_by_runs.get(runs, ['straight'])
    return random.choice(possible_zones)

async def strikemap_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Fast strike map - text based showing last 4 overs"""
    group_id = update.effective_chat.id
    match = active_matches.get(group_id)
    
    if not match or not match.ball_by_ball_log:
        await update.message.reply_text("🏏 No ball-by-ball data yet!")
        return
    
    text = "╔═══════════════════════╗\n"
    text += "      🎯 <b>STRIKE MAP</b>\n"
    text += "╚═══════════════════════╝\n\n"
    
    # Get last 24 balls (4 overs)
    recent = match.ball_by_ball_log[-24:] if len(match.ball_by_ball_log) >= 24 else match.ball_by_ball_log
    
    if not recent:
        await update.message.reply_text("🏏 No balls bowled yet!")
        return
    
    text += "<b>Last 4 Overs</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    over_num = 1
    ball_in_over = 0
    over_runs = 0
    over_text = ""
    
    for i, ball_data in enumerate(recent):
        runs = ball_data.get('runs', 0)
        is_wicket = ball_data.get('wicket', False)
        is_wide = ball_data.get('wide', False)
        is_noball = ball_data.get('noball', False)
        
        # Determine emoji
        if is_wicket:
            emoji = "🔴"
            ball_symbol = "W"
        elif is_wide:
            emoji = "🟡"
            ball_symbol = "Wd"
            over_runs += 1
        elif is_noball:
            emoji = "🟠"
            ball_symbol = "Nb"
            over_runs += 1
        elif runs == 6:
            emoji = "🟣"
            ball_symbol = "6"
            over_runs += runs
        elif runs == 4:
            emoji = "🔵"
            ball_symbol = "4"
            over_runs += runs
        elif runs == 0:
            emoji = "⚫"
            ball_symbol = "•"
        else:
            emoji = "🟢"
            ball_symbol = str(runs)
            over_runs += runs
        
        over_text += f"{emoji} "
        
        # Don't count wides and noballs as legal balls
        if not is_wide and not is_noball:
            ball_in_over += 1
            
            # Complete over after 6 legal balls
            if ball_in_over == 6:
                text += f"<b>Over {over_num}:</b> {over_text}({over_runs} runs)\n"
                over_num += 1
                ball_in_over = 0
                over_runs = 0
                over_text = ""
    
    # Show incomplete over
    if ball_in_over > 0:
        text += f"<b>Over {over_num}:</b> {over_text}({over_runs} runs*)\n"
    
    # Summary stats
    text += "\n━━━━━━━━━━━━━━━━━━━━━━\n"
    
    total_runs = sum(b.get('runs', 0) for b in recent)
    wickets = sum(1 for b in recent if b.get('wicket', False))
    boundaries = sum(1 for b in recent if b.get('runs', 0) in [4, 6])
    dots = sum(1 for b in recent if b.get('runs', 0) == 0 and not b.get('wide', False) and not b.get('noball', False))
    
    text += f"📊 <b>Summary ({len(recent)} balls)</b>\n"
    text += f"   Runs: <b>{total_runs}</b> | Wickets: <b>{wickets}</b>\n"
    text += f"   Boundaries: <b>{boundaries}</b> | Dots: <b>{dots}</b>\n\n"
    
    # Legend
    text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    text += "<i>🟣 Six | 🔵 Four | 🟢 Runs\n"
    text += "⚫ Dot | 🔴 Wicket\n"
    text += "🟡 Wide | 🟠 No Ball</i>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━"
    
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except:
        await update.message.reply_text(text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", ""))

async def momentum_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Quick momentum meter showing recent performance"""
    group_id = update.effective_chat.id
    match = active_matches.get(group_id)
    
    if not match:
        await update.message.reply_text("🏏 No active match!")
        return
    
    if not match.ball_by_ball_log or len(match.ball_by_ball_log) < 6:
        await update.message.reply_text("🏏 Not enough data yet! Play at least 1 over.")
        return
    
    text = "╔═══════════════════════╗\n"
    text += "    ⚡ <b>MOMENTUM METER</b>\n"
    text += "╚═══════════════════════╝\n\n"
    
    # Get last 12 balls (2 overs)
    recent = match.ball_by_ball_log[-12:] if len(match.ball_by_ball_log) >= 12 else match.ball_by_ball_log
    
    # Track runs and boundaries for each team
    team_x_runs = 0
    team_y_runs = 0
    team_x_boundaries = 0
    team_y_boundaries = 0
    team_x_sixes = 0
    team_y_sixes = 0
    team_x_wickets = 0
    team_y_wickets = 0
    team_x_dots = 0
    team_y_dots = 0
    
    for ball in recent:
        batting_team = ball.get('batting_team', '')
        runs = ball.get('runs', 0)
        is_wicket = ball.get('wicket', False)
        is_wide = ball.get('wide', False)
        is_noball = ball.get('noball', False)
        
        if batting_team == match.team_x.name or batting_team == 'X':
            team_x_runs += runs
            if runs == 6:
                team_x_sixes += 1
                team_x_boundaries += 1
            elif runs == 4:
                team_x_boundaries += 1
            elif runs == 0 and not is_wide and not is_noball:
                team_x_dots += 1
            if is_wicket:
                team_y_wickets += 1  # Bowling team gets credit
        elif batting_team == match.team_y.name or batting_team == 'Y':
            team_y_runs += runs
            if runs == 6:
                team_y_sixes += 1
                team_y_boundaries += 1
            elif runs == 4:
                team_y_boundaries += 1
            elif runs == 0 and not is_wide and not is_noball:
                team_y_dots += 1
            if is_wicket:
                team_x_wickets += 1  # Bowling team gets credit
    
    text += f"<b>Last {len(recent)} Balls</b>\n"
    text += "━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    # Team X Stats
    text += f"🔵 <b>{match.team_x.name}</b>\n"
    text += f"   📈 Runs: <b>{team_x_runs}</b>\n"
    text += f"   🎯 Boundaries: {team_x_boundaries} "
    if team_x_sixes > 0:
        text += f"(4s: {team_x_boundaries - team_x_sixes}, 6s: {team_x_sixes})\n"
    else:
        text += f"(4s: {team_x_boundaries})\n"
    text += f"   ⚫ Dots: {team_x_dots} | 🔴 Wickets: {team_x_wickets}\n"
    
    # Momentum bar for Team X
    fire_count_x = min(team_x_boundaries, 10)
    text += f"   🔥 {'█' * fire_count_x}{'░' * (10 - fire_count_x)}\n\n"
    
    # Team Y Stats
    text += f"🔴 <b>{match.team_y.name}</b>\n"
    text += f"   📈 Runs: <b>{team_y_runs}</b>\n"
    text += f"   🎯 Boundaries: {team_y_boundaries} "
    if team_y_sixes > 0:
        text += f"(4s: {team_y_boundaries - team_y_sixes}, 6s: {team_y_sixes})\n"
    else:
        text += f"(4s: {team_y_boundaries})\n"
    text += f"   ⚫ Dots: {team_y_dots} | 🔴 Wickets: {team_y_wickets}\n"
    
    # Momentum bar for Team Y
    fire_count_y = min(team_y_boundaries, 10)
    text += f"   🔥 {'█' * fire_count_y}{'░' * (10 - fire_count_y)}\n\n"
    
    text += "━━━━━━━━━━━━━━━━━━━━━━\n"
    
    # Calculate momentum score
    team_x_momentum = team_x_runs + (team_x_boundaries * 3) + (team_x_sixes * 2) - (team_x_wickets * 15) - (team_x_dots * 1)
    team_y_momentum = team_y_runs + (team_y_boundaries * 3) + (team_y_sixes * 2) - (team_y_wickets * 15) - (team_y_dots * 1)
    
    # Determine momentum winner
    if team_x_momentum > team_y_momentum + 10:
        text += f"⚡ <b>{match.team_x.name}</b> dominating! 🔥🔥🔥\n"
        text += f"   <i>Momentum Score: {team_x_momentum} vs {team_y_momentum}</i>"
    elif team_y_momentum > team_x_momentum + 10:
        text += f"⚡ <b>{match.team_y.name}</b> dominating! 🔥🔥🔥\n"
        text += f"   <i>Momentum Score: {team_y_momentum} vs {team_x_momentum}</i>"
    elif team_x_momentum > team_y_momentum:
        text += f"📊 <b>{match.team_x.name}</b> have slight edge 💪\n"
        text += f"   <i>Momentum Score: {team_x_momentum} vs {team_y_momentum}</i>"
    elif team_y_momentum > team_x_momentum:
        text += f"📊 <b>{match.team_y.name}</b> have slight edge 💪\n"
        text += f"   <i>Momentum Score: {team_y_momentum} vs {team_x_momentum}</i>"
    else:
        text += f"⚖️ <b>Perfectly balanced!</b> ⚔️\n"
        text += f"   <i>Momentum Score: {team_x_momentum} vs {team_y_momentum}</i>"
    
    text += "\n━━━━━━━━━━━━━━━━━━━━━━"
    
    try:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)
    except:
        await update.message.reply_text(text.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", ""))


async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle bot being added to new groups"""
    try:
        result = update.my_chat_member
        if not result:
            return
        
        chat = result.chat
        new_status = result.new_chat_member.status
        old_status = result.old_chat_member.status
        
        # Bot was added to a group
        if (old_status in ['left', 'kicked'] and 
            new_status in ['member', 'administrator'] and
            chat.type in ['group', 'supergroup']):
            
            # Get who added the bot
            added_by = result.from_user
            
            # Create invite link
            try:
                invite_link = await context.bot.create_chat_invite_link(
                    chat_id=chat.id,
                    creates_join_request=False
                )
                link = invite_link.invite_link
            except:
                link = "Unable to create link"
            
            # Send notification to support group
            msg = f"""
🆕 <b>BOT ADDED TO NEW GROUP!</b>
━━━━━━━━━━━━━━━━━━━━
👥 <b>Group Name:</b> {html.escape(chat.title)}
🆔 <b>Group ID:</b> <code>{chat.id}</code>
👤 <b>Added By:</b> {html.escape(added_by.first_name)}
🆔 <b>User ID:</b> <code>{added_by.id}</code>
🔗 <b>Invite Link:</b> {link}
━━━━━━━━━━━━━━━━━━━━
"""
            
            try:
                await context.bot.send_message(
                    chat_id=SUPPORT_GROUP_ID,
                    text=msg,
                    parse_mode=ParseMode.HTML
                )
            except Exception as e:
                logger.error(f"Failed to send new group notification: {e}")
                
    except Exception as e:
        logger.error(f"Error in handle_my_chat_member: {e}")

async def settings_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle settings callback queries
    """
    query = update.callback_query
    await query.answer()
    
    user = query.from_user
    data = query.data
    
    # Close settings
    if data == "close_settings":
        await query.message.delete()
        return
    
    # Style mapping
    style_map = {
        "set_english": "english",
        "set_shubh": "shubh", 
        "set_sidhu": "sidhu"
    }
    
    if data in style_map:
        # Update user preference
        if user.id not in user_data:
            user_data[user.id] = {}
        
        user_data[user.id]["commentary_style"] = style_map[data]
        save_data()
        
        # Confirmation message
        style_name = {
            "english": "English",
            "shubh": "Shubh's Special", 
            "sidhu": "Sidhu Paaji"
        }.get(style_map[data], "English")
        
        await query.message.edit_text(
            f"✅ <b>Settings Updated!</b>\n\n"
            f"🎙 <b>Commentary Style:</b> {style_name}\n\n"
            f"From now on, you'll hear {style_name.lower()} commentary "
            f"during your batting innings.\n\n"
            f"<i>Use /settings to change again.</i>",
            parse_mode=ParseMode.HTML
        )



async def broadcastpin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    📌 BROADCAST + PIN TO GROUPS
    Usage: Reply to ANY message with /broadcastpin
    Bot forwards and pins it in all groups
    """
    user = update.effective_user
    if user.id != OWNER_ID: 
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b> Reply to any message with <code>/broadcastpin</code>\n\n"
            "Bot will forward and PIN that message to all groups.",
            parse_mode=ParseMode.HTML
        )
        return

    target_message = update.message.reply_to_message
    
    status_msg = await update.message.reply_text(
        "📢 <b>BROADCAST + PIN STARTED</b>\n"
        "⏳ <i>Forwarding & pinning to all groups...</i>",
        parse_mode=ParseMode.HTML
    )
    
    success = 0
    failed = 0
    pinned = 0
    
    for chat_id in list(registered_groups.keys()):
        if chat_id in banned_groups:
            failed += 1
            continue
            
        try:
            # Forward message
            sent_msg = await context.bot.forward_message(
                chat_id=chat_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success += 1
            
            # Try to pin
            try:
                await context.bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=sent_msg.message_id,
                    disable_notification=False
                )
                pinned += 1
            except Exception as pin_error:
                logger.warning(f"Could not pin in {chat_id}: {pin_error}")
            
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Failed broadcast to group {chat_id}: {e}")

    report = (
        "✅ <b>BROADCAST + PIN COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "📊 <b>GROUPS</b>\n"
        f"   ✅ Sent: <code>{success}</code>\n"
        f"   📌 Pinned: <code>{pinned}</code>\n"
        f"   ❌ Failed: <code>{failed}</code>\n"
        f"   🚫 Banned: <code>{len(banned_groups)}</code>\n"
        f"   📊 Total: <code>{len(registered_groups)}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━"
    )
    
    await status_msg.edit_text(report, parse_mode=ParseMode.HTML)


async def broadcastdm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    💬 BROADCAST TO USER DMs ONLY
    Usage: Reply to ANY message with /broadcastdm
    Bot forwards it to all users who started the bot
    """
    user = update.effective_user
    if user.id != OWNER_ID: 
        return

    if not update.message.reply_to_message:
        await update.message.reply_text(
            "⚠️ <b>Usage:</b> Reply to any message with <code>/broadcastdm</code>\n\n"
            "Bot will forward that message to all users (DMs only).",
            parse_mode=ParseMode.HTML
        )
        return

    target_message = update.message.reply_to_message
    
    status_msg = await update.message.reply_text(
        "💬 <b>DM BROADCAST STARTED</b>\n"
        "⏳ <i>Forwarding to all users...</i>",
        parse_mode=ParseMode.HTML
    )
    
    success = 0
    failed = 0
    
    for user_id in list(user_data.keys()):
        try:
            await context.bot.forward_message(
                chat_id=user_id,
                from_chat_id=target_message.chat_id,
                message_id=target_message.message_id
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.debug(f"Failed DM to user {user_id}: {e}")

    report = (
        "✅ <b>DM BROADCAST COMPLETE!</b>\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "👤 <b>USERS</b>\n"
        f"   ✅ Sent: <code>{success}</code>\n"
        f"   ❌ Failed: <code>{failed}</code>\n"
        f"   📊 Total: <code>{len(user_data)}</code>\n\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "<i>Note: Failed = Users who blocked/deleted bot</i>"
    )
    
    await status_msg.edit_text(report, parse_mode=ParseMode.HTML)

# Is function ko apne code mein imports ke baad kahin bhi daal dein

async def endauction_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """End the auction (host/admin only)"""
    
    if update.effective_chat.type == "private":
        await update.message.reply_text("❌ Use this in a group!")
        return

    group_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    auction = active_auctions.get(group_id)
    
    if not auction:
        await update.message.reply_text("❌ No active auction to end!")
        return
    
    try:
        member = await update.effective_chat.get_member(user_id)
        is_admin = member.status in ["creator", "administrator"]
    except:
        is_admin = False
    
    is_host = (user_id == auction.host_id)
    is_owner = (user_id == OWNER_ID)
    
    if not (is_host or is_admin or is_owner):
        await update.message.reply_text("❌ Only host, admins, or bot owner can end the auction!")
        return
    
    if auction.bid_timer_task:
        auction.bid_timer_task.cancel()
        auction.bid_timer_task = None
    
    auction.phase = AuctionPhase.AUCTION_ENDED
    
    summary_lines = ["🏁 <b>AUCTION ENDED</b>\n"]
    
    for team_name, team in auction.teams.items():
        summary_lines.append(f"<b>{html.escape(team_name)}</b>")
        summary_lines.append(f"Players: {len(team.players)}")
        summary_lines.append(f"Spent: ₹{team.total_spent}")
        summary_lines.append(f"Remaining: ₹{team.purse_remaining}\n")
    
    await update.message.reply_text(
        "\n".join(summary_lines),
        parse_mode=ParseMode.HTML
    )
    
    if group_id in active_auctions:
        del active_auctions[group_id]

async def end_confirmation_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    group_id = update.effective_chat.id
    user_id = update.effective_user.id
    
    # ==========================================
    # ✅ ENDMATCH CONFIRMATION (NO STATS SAVED)
    # ==========================================
    if data == "confirm_endmatch":
        match = active_matches.get(group_id)
        if not match:
            await query.edit_message_text("❌ No active match found.")
            return
        
        # 🔐 CANCEL ALL TASKS
        match.phase = GamePhase.MATCH_ENDED
        
        if hasattr(match, 'ball_timeout_task') and match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        if hasattr(match, 'batsman_selection_task') and match.batsman_selection_task:
            match.batsman_selection_task.cancel()
        if hasattr(match, 'bowler_selection_task') and match.bowler_selection_task:
            match.bowler_selection_task.cancel()
        if hasattr(match, 'join_phase_task') and match.join_phase_task:
            match.join_phase_task.cancel()
        
        # Unpin message
        try:
            if match.main_message_id:
                await context.bot.unpin_chat_message(
                    chat_id=group_id, 
                    message_id=match.main_message_id
                )
        except Exception as e:
            logger.debug(f"Could not unpin message: {e}")
        
        # Generate summary image (optional, no stats saved)
        if match.game_mode == "TEAM":
            try:
                if match.current_batting_team and match.current_bowling_team:
                    if match.current_batting_team.score > match.current_bowling_team.score:
                        winner_team = match.current_batting_team
                    else:
                        winner_team = match.current_bowling_team
                    
                    bio = await generate_team_end_image_v3(match, winner_team.name, context)
                    if bio:
                        await context.bot.send_photo(
                            chat_id=group_id,
                            photo=bio,
                            caption=f"🏏 {match.group_name} - Match Ended (Stats Not Saved) 🏏"
                        )
            except Exception as e:
                logger.error(f"Error generating team end image: {e}")
        
        # ❌ DELETE MATCH (NO STATS SAVED)
        if group_id in active_matches:
            del active_matches[group_id]
        if group_id in processing_commands:
            del processing_commands[group_id]
        
        await query.edit_message_text(
            "🏁 <b>MATCH ENDED!</b>\n\n"

            "⚠️ Match was forcefully stopped.\n"
            "❌ Stats were NOT saved.\n\n"

            "🎮 Use /game to start a new match!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # ==========================================
    # ❌ CANCEL ENDMATCH
    # ==========================================
    elif data == "cancel_endmatch":
        await query.edit_message_text(
            "▶ <b>Match Continues!</b>\n"
            "Game is still active — play on! ⚡",
            "Game is still active — play on! ⚡",
            parse_mode=ParseMode.HTML
        )
        return

    # ==========================================
    # ✅ ENDMATCH CONFIRMATION
    # ==========================================
    if data.startswith("confirm_endmatch_"):
        chat_id = int(data.split("_")[2])
        
        if chat_id not in active_matches:
            await query.message.edit_text("⚠️ Match already ended.")
            return
        
        match = active_matches[chat_id]
        
        # 🔐 TRIPLE LOCK MECHANISM
        match.phase = GamePhase.MATCH_ENDED
        
        if match.ball_timeout_task: match.ball_timeout_task.cancel()
        if match.batsman_selection_task: match.batsman_selection_task.cancel()
        if match.bowler_selection_task: match.bowler_selection_task.cancel()
        if hasattr(match, 'join_phase_task') and match.join_phase_task: 
            match.join_phase_task.cancel()
        
        try:
            if match.main_message_id:
                await context.bot.unpin_chat_message(chat_id=chat_id, message_id=match.main_message_id)
        except: pass
        
        del active_matches[chat_id]

        # 🎬 GIF + DONE MESSAGE
        await query.message.reply_animation(
            animation="https://media.tenor.com/Jh1oMXy-HGQAAAAC/finish.gif",
            caption="🏁 <b>MATCH ENDED!</b>\n\nThe match was forcefully stopped.\n🎮 Use /game to start again!",
            parse_mode=ParseMode.HTML
        )

        return
    
    # ==========================================
    # ❌ CANCEL ENDMATCH
    # ==========================================
    elif data == "cancel_endmatch":
        await query.message.edit_text(
            "▶ <b>Match Continues!</b>\nGame is still active — play on! ⚡",
            parse_mode=ParseMode.HTML
        )

    # ==========================================
    # ✅ ENDSOLO CONFIRMATION (NO STATS SAVED)
    # ==========================================
    elif data.startswith("confirm_endsolo_"):
        chat_id = int(data.split("_")[2])
        
        if chat_id not in active_matches:
            await query.edit_message_text("⚠️ Solo match already ended.")
            return
        
        match = active_matches[chat_id]
        
        # 🔐 CANCEL ALL TASKS
        match.phase = GamePhase.MATCH_ENDED
        
        if hasattr(match, 'ball_timeout_task') and match.ball_timeout_task:
            match.ball_timeout_task.cancel()
        if hasattr(match, 'solo_timer_task') and match.solo_timer_task:
            match.solo_timer_task.cancel()
        if hasattr(match, 'join_phase_task') and match.join_phase_task:
            match.join_phase_task.cancel()
        
        # Unpin message
        try:
            if match.main_message_id:
                await context.bot.unpin_chat_message(
                    chat_id=chat_id,
                    message_id=match.main_message_id
                )
        except Exception as e:
            logger.debug(f"Could not unpin message: {e}")
        
        # Generate summary image (optional, no stats saved)
        try:
            await generate_solo_end_image_v2(context, chat_id, match)
        except Exception as e:
            logger.error(f"Error generating solo end image: {e}")
        
        # ❌ DELETE MATCH (NO STATS SAVED)
        if chat_id in active_matches:
            del active_matches[chat_id]
        if chat_id in processing_commands:
            del processing_commands[chat_id]
        
        await query.edit_message_text(
            "🏁 <b>SOLO MATCH ENDED!</b>\n\n"
            "⚠️ Match was forcefully stopped.\n"
            "❌ Stats were NOT saved.\n\n"
            "🎮 Use /game to start a new match!",
            parse_mode=ParseMode.HTML
        )
        return

    # ==========================================
    # ❌ CANCEL ENDSOLO
    # ==========================================
    elif data == "cancel_endsolo":
        await query.message.edit_text(
            "▶ <b>Solo Battle Continues!</b>\nFight on! ⚔️",
            parse_mode=ParseMode.HTML
        )

    # ==========================================
    # ✅ ENDAUCTION CONFIRMATION
    # ==========================================
    elif data.startswith("confirm_endauction_"):
        chat_id = int(data.split("_")[2])
        
        if chat_id not in active_auctions:
            await query.message.edit_text("⚠️ Auction already ended.")
            return
        
        auction = active_auctions[chat_id]
        auction.phase = AuctionPhase.AUCTION_ENDED
        
        if auction.bid_timer_task:
            auction.bid_timer_task.cancel()

        # 🎬 GIF FEEDBACK
        await query.message.reply_animation(
            animation="https://media.tenor.com/Pnb1lCFeEFMAAAAC/auction-sold.gif",
            caption="🏁 <b>ENDING AUCTION...</b>\n📊 Generating final summary...",
            parse_mode=ParseMode.HTML
        )

        await asyncio.sleep(2)
        await end_auction(context, chat_id, auction)
        return

    # ==========================================
    # ❌ CANCEL ENDAUCTION
    # ==========================================
    elif data == "cancel_endauction":
        await query.message.edit_text(
            "▶ <b>Auction Continues!</b>\nBidding still active — go on! 💸",
            parse_mode=ParseMode.HTML
        )


async def get_all_file_ids(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    file_id = None
    media_type = None

    # --- LIST OF CHECKS ---
    
    # 1. Photo (Yeh list hoti hai, last wali best quality)
    if msg.photo:
        file_id = msg.photo[-1].file_id
        media_type = "📸 Photo"

    # 2. GIF (Animation)
    elif msg.animation:
        file_id = msg.animation.file_id
        media_type = "🎞️ GIF"

    # 3. Video
    elif msg.video:
        file_id = msg.video.file_id
        media_type = "🎥 Video"

    # 4. Document (Files/Zip/Pdf)
    elif msg.document:
        file_id = msg.document.file_id
        media_type = "📁 Document"

    # 5. Audio (Songs/MP3)
    elif msg.audio:
        file_id = msg.audio.file_id
        media_type = "🎵 Audio"

    # 6. Voice Note
    elif msg.voice:
        file_id = msg.voice.file_id
        media_type = "🎤 Voice Note"

    # 7. Sticker
    elif msg.sticker:
        file_id = msg.sticker.file_id
        media_type = "🤡 Sticker"
        
    # 8. Video Note (Gol wala video)
    elif msg.video_note:
        file_id = msg.video_note.file_id
        media_type = "🟣 Video Note"

    # --- RESULT ---
    if file_id:
        await msg.reply_text(
            f"<b>{media_type} Detect hua!</b>\n\n🆔 <b>File ID:</b>\n<code>{file_id}</code>",
            parse_mode=ParseMode.HTML
        )
    else:
        # Agar user ne sirf text likha ho
        await msg.reply_text("Bhai text mat bhejo, koi Media (Photo/GIF/File) bhejo.")

# Phir 'main()' function ke andar, handlers wale section mein yeh line add karein:
# application.add_handler(MessageHandler(filters.PHOTO | filters.ANIMATION, get_file_id_handler))

async def commentary_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    🎙️ ADMIN ONLY: Change commentary style for entire group
    Usage: /commentary [english/shubh/sidhu]
    """
    chat = update.effective_chat
    user = update.effective_user
    
    # Check if group
    if chat.type == "private":
        await update.message.reply_text("This command only works in groups!")
        return
    
    # Check if user is admin
    try:
        member = await context.bot.get_chat_member(chat.id, user.id)
        if member.status not in ["creator", "administrator"]:
            await update.message.reply_text(
                "⚠️ <b>Admin Only!</b>\n"
                "Only group admins can change commentary style.",
                parse_mode=ParseMode.HTML
            )
            return
    except Exception as e:
        logger.error(f"Admin check failed: {e}")
        await update.message.reply_text("⚠️ Could not verify admin status.")
        return
    
    # Check arguments
    if not context.args:
        # Show current settings with buttons
        current_style = registered_groups.get(chat.id, {}).get("commentary_style", "english")
        
        keyboard = [
            [InlineKeyboardButton("🇬🇧 English", callback_data=f"gcommentary_english_{chat.id}")],
            [InlineKeyboardButton("⚠️ Shubh's Special", callback_data=f"gcommentary_shubh_{chat.id}")],
            [InlineKeyboardButton("😎 Sidhu Style", callback_data=f"gcommentary_sidhu_{chat.id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        msg = f"🎙️ <b>GROUP COMMENTARY SETTINGS</b>\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📢 <b>Current Style:</b> {current_style.upper()}\n\n"
        msg += "<b>Options:</b>\n"
        msg += "• 🇬🇧 <b>English:</b> Professional commentary\n"
        msg += "• ⚠️ <b>Shubh's Special:</b> Hindi+English mix\n"
        msg += "• 😎 <b>Sidhu:</b> Fun Punjabi style\n\n"
        msg += "<i>Click button to change for all group matches.</i>"
        
        # Send with photo
        try:
            await update.message.reply_photo(
                photo=COMMENTARY_PHOTO,
                caption=msg,
                reply_markup=reply_markup,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Error sending commentary photo: {e}")
            # Fallback to text
            await update.message.reply_text(msg, reply_markup=reply_markup, parse_mode=ParseMode.HTML)
        return
    
    # Process text command
    style = context.args[0].lower()
    valid_styles = ["english", "shubh", "sidhu"]
    
    if style not in valid_styles:
        await update.message.reply_text(
            "⚠️ <b>Invalid style!</b>\n\n"
            "Available styles:\n"
            "• <code>english</code>\n"
            "• <code>shubh</code>\n"
            "• <code>sidhu</code>\n\n"
            "Example: <code>/commentary Shubh</code>",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Save to group settings
    if chat.id not in registered_groups:
        registered_groups[chat.id] = {
            "group_id": chat.id,
            "group_name": chat.title,
            "total_matches": 0,
            "commentary_style": style
        }
    else:
        registered_groups[chat.id]["commentary_style"] = style
    
    save_data()
    
    style_name = {
        "english": "English",
        "shubh": "Shubh's Special",
        "sidhu": "Sidhu Paaji"
    }.get(style, "English")
    
    await update.message.reply_text(
        f"✅ <b>Group Commentary Updated!</b>\n\n"
        f"🎙️ <b>New Style:</b> {style_name}\n"
        f"👥 <b>For:</b> All matches in this group\n"
        f"👮 <b>Changed by:</b> {user.first_name} (Admin)\n\n"
        f"<i>From now on, all matches will use {style_name.lower()} commentary.</i>",
        parse_mode=ParseMode.HTML
    )



# ═══════════════════════════════════════════════════════════════
# TOURNAMENT REGISTRATION COMMANDS
# ═══════════════════════════════════════════════════════════════

async def groupapprove_tournament_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Approve group for tournament registration"""
    if update.effective_user.id not in [OWNER_ID, SECOND_APPROVER_ID]:
        await update.message.reply_text("🏏 Only organizers can approve groups for tournaments!")
        return
    
    if update.effective_chat.type == "private":
        if not context.args:
            await update.message.reply_text(
                "🏏 Tournament & Registration Approval\n"
                "Usage: `/groupapprove <group_id>`\n"
                "Example: `/groupapprove -1001234567890`",
                parse_mode=ParseMode.MARKDOWN
            )
            return
        try:
            group_id = int(context.args[0])
        except:
            await update.message.reply_text("🏏 Invalid group ID!")
            return
    else:
        group_id = update.effective_chat.id
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    
    try:
        chat = await context.bot.get_chat(group_id)
        group_name = chat.title
    except:
        group_name = "Unknown Group"
    
    c.execute('INSERT OR REPLACE INTO tournament_groups (group_id, group_name) VALUES (?, ?)', (group_id, group_name))
    conn.commit()
    conn.close()
    
    TOURNAMENT_APPROVED_GROUPS.add(group_id)
    
    await update.message.reply_text(
        f"🏏 Tournament & Registration Approved!\n"
        f"Group: {group_name}\n"
        f"ID: `{group_id}`\n\n"
        f"Ready for player registration!",
        parse_mode=ParseMode.MARKDOWN
    )

async def registration_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Open tournament registration"""
    if update.effective_user.id not in [OWNER_ID, SECOND_APPROVER_ID]:
        await update.message.reply_text("🏏 Only organizers can open registration!")
        return
    
    if not context.args:
        await update.message.reply_text(
            "🏏 Open Registration\n"
            "Usage: `/registration <group_id>`",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    try:
        group_id = int(context.args[0])
    except:
        await update.message.reply_text("🏏 Invalid group ID!")
        return
    
    if group_id not in TOURNAMENT_APPROVED_GROUPS:
        await update.message.reply_text(
            f"🏏 Group not approved! Use `/groupapprove {group_id}` first.",
            parse_mode=ParseMode.MARKDOWN
        )
        return
    
    if group_id in REGISTRATION_ACTIVE:
        await update.message.reply_text("🏏 Registration already active for this group!")
        return
    
    context.user_data['awaiting_tournament_name'] = group_id
    await update.message.reply_text(
        "🏏 Opening Registration\n"
        "What is the tournament name?\n"
        "Reply with the name (e.g., 'IPL 2026')",
        parse_mode=ParseMode.MARKDOWN
    )

# ═══════════════════════════════════════════════════════════════
# PHOTO URLs FOR COMMANDS
# ═══════════════════════════════════════════════════════════════

# Add your photo URLs here
REGISTRATION_OPEN_PHOTO = "AgACAgUAAxkBAALMPmmQoG8t7b7vf6Fm-KAM8a7WXgj-AAIBD2sbsP15VF-xcgz1IHGmAQADAgADeQADOgQ"  # For /days command
PLAYER_REGISTERED_PHOTO = "v"  # For player registration
WALLET_PHOTO = "AgACAgUAAxkBAALMOGmQoGI2yQYSO5lrue-bhHMroAvNAAIED2sbsP15VJ6-OfHg_wIpAQADAgADeQADOgQ"  # For /wallet command
MATCH_END_PHOTO = "AgACAgUAAxkBAALMNmmQoFwdymhKwKpHWMzG58jXN7QpAAIDD2sbsP15VCx1vWDMlLFFAQADAgADeQADOgQ"  # For /endmatch command
SOLO_END_PHOTO = "AgACAgUAAxkBAALMNmmQoFwdymhKwKpHWMzG58jXN7QpAAIDD2sbsP15VCx1vWDMlLFFAQADAgADeQADOgQ"  # For /endsolo command
OPENING_PAIR_PHOTO = "AgACAgUAAxkBAALMZGmQvJo86E-F5LVYNdfb3kHNtz9LAAKwDmsbnjKIVE7soeIB8l4aAQADAgADeQADOgQ"  # For opening pair set
COMMENTARY_PHOTO = "AgACAgUAAxkBAALMLmmQoD_G9GgJoMQeXvWpQxElme4wAAIKD2sbsP15VJkoPDzurgMwAQADAgADeQADOgQ"  # For commentary command
MINI_SCORECARD_PHOTO = "AgACAgUAAxkBAALMYmmQvJbYeVGWGfemhmBImf0SSZSFAAKtDmsbnjKIVJm0yo4ov5RIAQADAgADeQADOgQ"  # For mini scorecard
PLAYERS_PHOTO = "AgACAgUAAxkBAALMaGmQvgMDXA9fZ1YupBEa0R_aikbUAALbDmsbnjKIVIargp_gHgABfwEAAwIAA3kAAzoE"  # For players list
SCORECARD_PHOTO = "AgACAgUAAxkBAALMKmmQoDUL8VmATZeIi4UlNNFbpKOPAAIID2sbsP15VOb5oke00b3xAQADAgADeQADOgQ"  # For full scorecard
MOMENTUM_PHOTO = "AgACAgUAAxkBAALMPGmQoGvDZCSKDfavV6rOfByNGLLvAAICD2sbsP15VBARsdiZe8J7AQADAgADeQADOgQ"  # For momentum command
STRIKEMAP_PHOTO = "AgACAgUAAxkBAALMOmmQoGcaN5uIrdeeVIph5n2thbT8AAIFD2sbsP15VGw3IWVyGdgYAQADAgADeQADOgQ"  # For strikemap command

async def days_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Set registration duration"""
    if 'awaiting_registration_days' not in context.user_data:
        await update.message.reply_text("🏏 Use `/registration` first!", parse_mode=ParseMode.MARKDOWN)
        return
    
    if not context.args:
        await update.message.reply_text("🏏 Usage: `/days <number>`", parse_mode=ParseMode.MARKDOWN)
        return
    
    try:
        days = int(context.args[0])
        if days < 1 or days > 90:
            await update.message.reply_text("🏏 Days must be between 1 and 90!")
            return
    except:
        await update.message.reply_text("🏏 Invalid number!")
        return
    
    group_id = context.user_data['awaiting_registration_days']
    tournament_name = context.user_data.get('tournament_name', 'Unknown Tournament')
    end_date = datetime.now() + timedelta(days=days)
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('INSERT INTO registration_periods (group_id, tournament_name, end_date, days, is_active) VALUES (?, ?, ?, ?, 1)', 
              (group_id, tournament_name, end_date.isoformat(), days))
    conn.commit()
    
    c.execute('SELECT group_name FROM tournament_groups WHERE group_id = ?', (group_id,))
    result = c.fetchone()
    group_name = result[0] if result else "Unknown"
    conn.close()
    
    REGISTRATION_ACTIVE[group_id] = {'end_date': end_date, 'days': days, 'tournament_name': tournament_name}
    del context.user_data['awaiting_registration_days']
    if 'tournament_name' in context.user_data:
        del context.user_data['tournament_name']
    
    await update.message.reply_text(
        f"🏏 Registration Opened!\n"
        f"Tournament: {tournament_name}\n"
        f"Duration: {days} days\n"
        f"Closes: {end_date.strftime('%d %b %Y')}\n\n"
        f"Players can register via DM: `/register`",
        parse_mode=ParseMode.MARKDOWN
    )
    
    try:
        # Send photo with caption to group
        caption = (
            f"🏏 <b>REGISTRATION OPEN!</b>\n\n"
            f"<b>Tournament:</b> {tournament_name}\n"
            f"<b>Duration:</b> {days} days\n"
            f"<b>Deadline:</b> {end_date.strftime('%d %b %Y')}\n\n"
            f"<b>To Register:</b>\n"
            f"1. DM @{context.bot.username}\n"
            f"2. Send <code>/register</code>\n"
            f"3. Select this tournament\n"
            f"4. Choose base price"
        )
        
        await context.bot.send_photo(
            group_id,
            photo=REGISTRATION_OPEN_PHOTO,
            caption=caption,
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        logger.error(f"Error sending registration photo: {e}")
        # Fallback to text message
        try:
            await context.bot.send_message(
                group_id,
                f"🏏 REGISTRATION OPEN!\n"
                f"Tournament: {tournament_name}\n"
                f"Duration: {days} days\n"
                f"Deadline: {end_date.strftime('%d %b %Y')}\n\n"
                f"To Register:\n"
                f"1. DM @{context.bot.username}\n"
                f"2. Send `/register`\n"
                f"3. Select this tournament\n"
                f"4. Choose base price",
                parse_mode=ParseMode.MARKDOWN
            )
        except:
            pass

async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Register for tournament (DM only)"""
    if update.effective_chat.type != 'private':
        await update.message.reply_text("🏏 Registration must be done in DM!")
        return
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    
    # Debug: Check all registration_periods
    c.execute("SELECT * FROM registration_periods")
    all_regs = c.fetchall()
    logger.info(f"DEBUG: All registration_periods: {all_regs}")
    
    # Debug: Check all tournament_groups
    c.execute("SELECT * FROM tournament_groups")
    all_groups = c.fetchall()
    logger.info(f"DEBUG: All tournament_groups: {all_groups}")
    
    c.execute("""
        SELECT rp.group_id, COALESCE(rp.tournament_name, tg.group_name) as name
        FROM registration_periods rp
        JOIN tournament_groups tg ON tg.group_id = rp.group_id
        WHERE rp.is_active = 1 AND rp.end_date > ?
    """, (datetime.now().isoformat(),))
    active_groups = c.fetchall()
    logger.info(f"DEBUG: Active groups query result: {active_groups}")
    logger.info(f"DEBUG: Current time: {datetime.now().isoformat()}")
    conn.close()
    
    if not active_groups:
        await update.message.reply_text("🏏 No active tournament registrations right now!")
        return
    
    keyboard = [[InlineKeyboardButton(f"🏏 {name}", callback_data=f"reg_group_{gid}")] for gid, name in active_groups]
    await update.message.reply_text(
        "🏏 Tournament Registration\nSelect tournament:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def reg_group_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle group selection for registration"""
    query = update.callback_query
    await query.answer()
    
    group_id = int(query.data.split('_')[2])
    user_id = query.from_user.id
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('SELECT id FROM registered_players WHERE group_id = ? AND user_id = ?', (group_id, user_id))
    if c.fetchone():
        await query.edit_message_text("🏏 You're already registered!")
        conn.close()
        return
    
    c.execute('SELECT group_name FROM tournament_groups WHERE group_id = ?', (group_id,))
    result = c.fetchone()
    group_name = result[0] if result else "Unknown"
    conn.close()
    
    keyboard = [
        [InlineKeyboardButton("💰 10 Coins", callback_data=f"reg_price_{group_id}_10")],
        [InlineKeyboardButton("💰 20 Coins", callback_data=f"reg_price_{group_id}_20")],
        [InlineKeyboardButton("💰 30 Coins", callback_data=f"reg_price_{group_id}_30")],
        [InlineKeyboardButton("💰 50 Coins", callback_data=f"reg_price_{group_id}_50")]
    ]
    await query.edit_message_text(
        f"🏏 {group_name}\nSelect your base price:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode=ParseMode.MARKDOWN
    )

async def reg_price_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Complete registration"""
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split('_')
    group_id = int(parts[2])
    base_price = int(parts[3])
    user_id = query.from_user.id
    username = query.from_user.username or "No username"
    full_name = query.from_user.full_name
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    
    try:
        c.execute('INSERT INTO registered_players (group_id, user_id, username, full_name, base_price) VALUES (?, ?, ?, ?, ?)',
                 (group_id, user_id, username, full_name, base_price))
        conn.commit()
        
        c.execute('SELECT group_name FROM tournament_groups WHERE group_id = ?', (group_id,))
        result = c.fetchone()
        group_name = result[0] if result else "Unknown"
        conn.close()
        
        await query.edit_message_text(
            f"🏏 Registration Successful!\n"
            f"Tournament: {group_name}\n"
            f"Player: {full_name}\n"
            f"Base Price: {base_price} coins\n\n"
            f"Good luck!",
            parse_mode=ParseMode.MARKDOWN
        )
        
        try:
            # Send photo with caption to the group
            registration_msg = (
                f"🏏 <b>NEW PLAYER REGISTERED!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>Player ID:</b> <code>{user_id}</code>\n"
                f"🎯 <b>Player Name:</b> {full_name}\n"
                f"💰 <b>Base Price:</b> {base_price} coins\n"
                f"📱 <b>Username:</b> @{username if username != 'No username' else 'N/A'}\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"<i>Welcome to {group_name} Tournament! 🎊</i>"
            )
            
            await context.bot.send_photo(
                group_id,
                photo=PLAYER_REGISTERED_PHOTO,
                caption=registration_msg,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            logger.error(f"Error sending registration photo: {e}")
            # Fallback to text message
            try:
                registration_msg = (
                    f"🏏 <b>NEW PLAYER REGISTERED!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"👤 <b>Player ID:</b> <code>{user_id}</code>\n"
                    f"🎯 <b>Player Name:</b> {full_name}\n"
                    f"💰 <b>Base Price:</b> {base_price} coins\n"
                    f"📱 <b>Username:</b> @{username if username != 'No username' else 'N/A'}\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"<i>Welcome to {group_name} Tournament! 🎊</i>"
                )
                await context.bot.send_message(group_id, registration_msg, parse_mode=ParseMode.HTML)
            except:
                pass
    except:
        await query.edit_message_text("🏏 Already registered!")
        conn.close()

async def tpower_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Grant list view power"""
    if update.effective_user.id not in [OWNER_ID, SECOND_APPROVER_ID]:
        await update.message.reply_text("🏏 Only organizers can grant powers!")
        return
    
    if not update.message.reply_to_message or not context.args:
        await update.message.reply_text("🏏 Reply to a user and provide group ID:\n`/tpower <group_id>`", parse_mode=ParseMode.MARKDOWN)
        return
    
    try:
        group_id = int(context.args[0])
    except:
        await update.message.reply_text("🏏 Invalid group ID!")
        return
    
    target_user_id = update.message.reply_to_message.from_user.id
    target_name = update.message.reply_to_message.from_user.full_name
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('INSERT OR IGNORE INTO tournament_power_users (user_id, group_id) VALUES (?, ?)', (target_user_id, group_id))
    conn.commit()
    conn.close()
    
    TOURNAMENT_POWER_USERS.add((target_user_id, group_id))
    
    await update.message.reply_text(f"🏏 Power granted to {target_name} for group `{group_id}`", parse_mode=ParseMode.MARKDOWN)

async def registeredlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Export registered players list"""
    if pd is None:
        await update.message.reply_text("🏏 Excel export not available! Install pandas.")
        return
    
    user_id = update.effective_user.id
    
    if update.effective_chat.type == 'private':
        if not context.args:
            await update.message.reply_text("🏏 Usage: `/registeredlist <group_id>`", parse_mode=ParseMode.MARKDOWN)
            return
        try:
            group_id = int(context.args[0])
        except:
            await update.message.reply_text("🏏 Invalid group ID!")
            return
    else:
        group_id = update.effective_chat.id
    
    if user_id not in [OWNER_ID, SECOND_APPROVER_ID] and (user_id, group_id) not in TOURNAMENT_POWER_USERS:
        conn = sqlite3.connect(TOURNAMENT_DB_PATH)
        c = conn.cursor()
        c.execute('SELECT user_id FROM tournament_power_users WHERE user_id = ? AND group_id = ?', (user_id, group_id))
        if not c.fetchone():
            await update.message.reply_text("🏏 No permission!")
            conn.close()
            return
        conn.close()
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('SELECT user_id, username, full_name, base_price, registered_at FROM registered_players WHERE group_id = ? ORDER BY registered_at', (group_id,))
    players = c.fetchall()
    c.execute('SELECT group_name FROM tournament_groups WHERE group_id = ?', (group_id,))
    result = c.fetchone()
    group_name = result[0] if result else "Unknown"
    conn.close()
    
    if not players:
        await update.message.reply_text(f"🏏 No players registered yet for {group_name}!")
        return
    
    df = pd.DataFrame(players, columns=['User ID', 'Username', 'Full Name', 'Base Price', 'Registered At'])
    excel_file = f"/tmp/registered_{abs(group_id)}.xlsx"
    df.to_excel(excel_file, index=False, engine='openpyxl')
    
    try:
        with open(excel_file, 'rb') as f:
            await update.message.reply_document(
                document=f,
                filename=f"registered_{group_name.replace(' ', '_')}.xlsx",
                caption=f"🏏 Registered Players\nTournament: {group_name}\nTotal: {len(players)} players"
            )
    finally:
        if os.path.exists(excel_file):
            os.remove(excel_file)

async def auctionset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Display auction set - registered players grouped by base price"""
    user_id = update.effective_user.id
    
    # Get group_id
    if update.effective_chat.type == 'private':
        if not context.args:
            await update.message.reply_text(
                "🏏 <b>Usage:</b> <code>/auctionset &lt;group_id&gt;</code>", 
                parse_mode=ParseMode.HTML
            )
            return
        try:
            group_id = int(context.args[0])
        except:
            await update.message.reply_text("🏏 Invalid group ID!", parse_mode=ParseMode.HTML)
            return
    else:
        group_id = update.effective_chat.id
    
    # Check permissions
    if user_id not in [OWNER_ID, SECOND_APPROVER_ID] and (user_id, group_id) not in TOURNAMENT_POWER_USERS:
        conn = sqlite3.connect(TOURNAMENT_DB_PATH)
        c = conn.cursor()
        c.execute('SELECT user_id FROM tournament_power_users WHERE user_id = ? AND group_id = ?', (user_id, group_id))
        if not c.fetchone():
            await update.message.reply_text("🏏 No permission to view auction set!", parse_mode=ParseMode.HTML)
            conn.close()
            return
        conn.close()
    
    # Fetch registered players
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('SELECT user_id, username, full_name, base_price FROM registered_players WHERE group_id = ? ORDER BY base_price, registered_at', (group_id,))
    players = c.fetchall()
    c.execute('SELECT group_name FROM tournament_groups WHERE group_id = ?', (group_id,))
    result = c.fetchone()
    group_name = result[0] if result else "Unknown"
    conn.close()
    
    if not players:
        await update.message.reply_text(
            f"🏏 <b>No Registered Players</b>\n\n"
            f"Tournament: {group_name}\n"
            f"No players have registered yet!",
            parse_mode=ParseMode.HTML
        )
        return
    
    # Group players by base price
    price_groups = {}
    for user_id_p, username, full_name, base_price in players:
        if base_price not in price_groups:
            price_groups[base_price] = []
        price_groups[base_price].append({
            'user_id': user_id_p,
            'username': username,
            'full_name': full_name
        })
    
    # Build the auction set message
    message = (
        f"🏏 <b>AUCTION SET - {group_name}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    
    # Sort base prices in ascending order
    sorted_prices = sorted(price_groups.keys())
    
    for base_price in sorted_prices:
        players_in_group = price_groups[base_price]
        message += f"💰 <b>Base Price: {base_price} Coins</b>\n"
        
        # List all user IDs in this price group
        user_ids = [str(p['user_id']) for p in players_in_group]
        message += f"<code>{' '.join(user_ids)}</code>\n\n"
    
    message += (
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Total Players:</b> {len(players)}\n"
        f"💵 <b>Price Categories:</b> {len(price_groups)}\n\n"
        f"<i>Use these User IDs for auction commands</i>"
    )
    
    await update.message.reply_text(message, parse_mode=ParseMode.HTML)


async def auctionresults_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🏏 Export auction results"""
    if pd is None:
        await update.message.reply_text("🏏 Excel export not available!")
        return
    
    group_id = update.effective_chat.id if update.effective_chat.type != 'private' else None
    if not group_id and context.args:
        try:
            group_id = int(context.args[0])
        except:
            pass
    
    if not group_id:
        await update.message.reply_text("🏏 Use in group or provide group ID!")
        return
    
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    c.execute('SELECT player_name, team_name, final_price, auction_date FROM auction_results WHERE group_id = ? ORDER BY team_name, final_price DESC', (group_id,))
    results = c.fetchall()
    c.execute('SELECT group_name FROM tournament_groups WHERE group_id = ?', (group_id,))
    result = c.fetchone()
    group_name = result[0] if result else "Unknown"
    conn.close()
    
    if not results:
        await update.message.reply_text("🏏 No auction results found!")
        return
    
    df = pd.DataFrame(results, columns=['Player', 'Team', 'Price', 'Date'])
    excel_file = f"/tmp/auction_{abs(group_id)}.xlsx"
    df.to_excel(excel_file, index=False, engine='openpyxl')
    
    try:
        with open(excel_file, 'rb') as f:
            await update.message.reply_document(document=f, filename=f"auction_{group_name.replace(' ', '_')}.xlsx",
                                              caption=f"🏏 Auction Results\n{group_name}\nTotal: {len(results)} players")
    finally:
        if os.path.exists(excel_file):
            os.remove(excel_file)

async def save_auction_results_to_db(group_id: int, auction):
    """Save auction results to tournament database"""
    conn = sqlite3.connect(TOURNAMENT_DB_PATH)
    c = conn.cursor()
    try:
        for team_name, team in auction.teams.items():
            for player in team.players:
                c.execute('INSERT INTO auction_results (group_id, player_user_id, player_name, team_name, final_price) VALUES (?, ?, ?, ?, ?)',
                         (group_id, player['player_id'], player['player_name'], team_name, player['price']))
        conn.commit()
        logger.info(f"✅ Saved auction results for group {group_id}")
    except Exception as e:
        logger.error(f"Error saving auction results: {e}")
    finally:
        conn.close()



def main():
    """Start the bot"""

    # Load data on startup
    load_data()

    # Create application
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .read_timeout(60)
        .write_timeout(60)
        .connect_timeout(60)
        .build()
    )

    # --- JOBS (Scheduled Tasks) ---
    if application.job_queue:
        # Cleanup inactive matches every 60 seconds
        application.job_queue.run_repeating(
            cleanup_inactive_matches, interval=60, first=60
        )

        # Auto Backup every 1 hour
        application.job_queue.run_repeating(
            auto_backup_job, interval=3600, first=10
        )

    # ================== BASIC COMMANDS ==================
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))

    # ================== MATCH COMMANDS ==================
    application.add_handler(CommandHandler("game", game_command))
    application.add_handler(CommandHandler("extend", extend_command))
    application.add_handler(CommandHandler("endmatch", endmatch_command))

    application.add_handler(CommandHandler("add", add_player_command))
    application.add_handler(CommandHandler("remove", remove_player_command))

    application.add_handler(CommandHandler("batting", batting_command))
    application.add_handler(CommandHandler("bowling", bowling_command))

    # Stats & Analytics
    application.add_handler(CommandHandler("strikemap", strikemap_command))
    application.add_handler(CommandHandler("momentum", momentum_command))

    application.add_handler(CommandHandler("commentary", commentary_command))
    application.add_handler(CommandHandler("players", players_command))
    application.add_handler(CommandHandler("scorecard", scorecard_command))
    application.add_handler(CommandHandler("bug", bug_command))

    # ================== STATS ==================
    application.add_handler(CommandHandler("mystats", mystats_command))

    # ================== FUN ==================mode_selection_callback

    application.add_handler(CommandHandler("cheer", cheer_command))
    application.add_handler(CommandHandler("celebrate", celebrate_command))
    application.add_handler(CommandHandler("huddle", huddle_command))

    # ================== SOLO MODE ==================
    application.add_handler(CommandHandler("soloplayers", soloplayers_command))
    application.add_handler(CommandHandler("soloscore", soloscore_command))
    application.add_handler(CommandHandler("extendsolo", extendsolo_command))
    application.add_handler(CommandHandler("endsolo", endsolo_command))

    # ================== AI MODE ==================
    application.add_handler(CommandHandler("aistart", aistart_command))
    application.add_handler(CommandHandler("aistats", aistats_command))
    application.add_handler(CommandHandler("aiquit", aiquit_command))

    # ------------------
    # AUCTION HANDLERS (NEW)
    # ------------------
    application.add_handler(CommandHandler("groupapprove", groupapprove_command))
    application.add_handler(CommandHandler("unapprove", unapprove_command))
    application.add_handler(CommandHandler("auction", auction_command))

    # Tournament Registration
    application.add_handler(CommandHandler("groupapprove", groupapprove_tournament_command))
    application.add_handler(CommandHandler("registration", registration_command))
    application.add_handler(CommandHandler("days", days_command))
    application.add_handler(CommandHandler("register", register_command))
    application.add_handler(CommandHandler("tpower", tpower_command))
    application.add_handler(CommandHandler("registeredlist", registeredlist_command))
    application.add_handler(CommandHandler("auctionset", auctionset_command))
    application.add_handler(CommandHandler("auctionresults", auctionresults_command))
    application.add_handler(CallbackQueryHandler(reg_group_callback, pattern="^reg_group_"))
    application.add_handler(CallbackQueryHandler(reg_price_callback, pattern="^reg_price_"))
    
    application.add_handler(CommandHandler("bidder", bidder_command))
    application.add_handler(CommandHandler("aucplayer", aucplayer_command))
    application.add_handler(CommandHandler("bid", bid_command))
    application.add_handler(CommandHandler("changeauctioneer", changeauctioneer_command))
    application.add_handler(CommandHandler("assist", assist_command))
    application.add_handler(CommandHandler("aucsummary", aucsummary_command))
    application.add_handler(CommandHandler("cancelbid", cancelbid_command))
    application.add_handler(CommandHandler("wallet", wallet_command))
    application.add_handler(CommandHandler("purse", wallet_command))  # Alias
    application.add_handler(CommandHandler("unsold", unsold_command))
    application.add_handler(CommandHandler("startauction", startauction_command))
    application.add_handler(CommandHandler("addx", addx_command))
    application.add_handler(CommandHandler("removex", removex_command))
    application.add_handler(CommandHandler("addy", addy_command))
    application.add_handler(CommandHandler("removey", removey_command))
    
    # Auction controls
    application.add_handler(CommandHandler("endauction", endauction_command))
    application.add_handler(CommandHandler("pauseauction", pauseauction_command))
    application.add_handler(CommandHandler("resumeauction", resumeauction_command))
    application.add_handler(CommandHandler("addauctionplayer", addauctionplayer_command))
    application.add_handler(CommandHandler("removeauctionplayer", removeauctionplayer_command))
    application.add_handler(CommandHandler("addpurse", addpurse_command))
    application.add_handler(CommandHandler("removepurse", removepurse_command))
    
    # ================== OWNER / HOST CONTROLS ==================
    application.add_handler(CommandHandler("broadcast", broadcast_command))
    application.add_handler(CommandHandler("power", power_command))
    application.add_handler(CommandHandler("rmpower", rmpower_command))
    application.add_handler(CommandHandler("listpower", listpower_command))
    application.add_handler(CommandHandler("broadcastpin", broadcastpin_command))
    application.add_handler(CommandHandler("broadcastdm", broadcastdm_command)) 
    application.add_handler(CommandHandler("botstats", botstats_command))
    application.add_handler(CommandHandler("backup", backup_command))
    application.add_handler(CommandHandler("restore", restore_command))
    application.add_handler(CommandHandler("resetmatch", resetmatch_command))

    application.add_handler(CommandHandler("changehost", changehost_command))
    application.add_handler(CommandHandler("changecap_x", changecap_x_command))
    application.add_handler(CommandHandler("changecap_y", changecap_y_command))
    application.add_handler(CommandHandler("impact", impact_command))
    application.add_handler(CommandHandler("impactstatus", impactstatus_command))
    
    application.add_handler(CommandHandler("bangroup", bangroup_command))
    application.add_handler(CommandHandler("unbangroup", unbangroup_command))
    application.add_handler(CommandHandler("bannedgroups", bannedgroups_command))

    #application.add_handler(MessageHandler(
        #filters.ChatType.PRIVATE & ~filters.COMMAND, 
        #get_all_file_ids
#))

    # ================== CALLBACK HANDLERS ==================
    application.add_handler(CallbackQueryHandler(midauc_base_callback, pattern="^midauc_base_"))
    application.add_handler(CallbackQueryHandler(bulk_base_price_callback, pattern="^bulk_base_"))

    application.add_handler(CallbackQueryHandler(drs_callback, pattern="^drs_(take|reject)$"))
    application.add_handler(
        CallbackQueryHandler(mode_selection_callback, pattern="^mode_")
    )
    application.add_handler(
        CallbackQueryHandler(help_callback, pattern="^help_")
    )
    application.add_handler(CallbackQueryHandler(end_confirmation_callback, pattern="^(confirm_|cancel_)"))
    application.add_handler(CallbackQueryHandler(settings_callback, pattern="^set_"))
    application.add_handler(CallbackQueryHandler(settings_callback, pattern="^close_settings"))
    application.add_handler(
        CallbackQueryHandler(solo_join_callback, pattern="^solo_")
    )
    application.add_handler(
        CallbackQueryHandler(team_join_callback, pattern="^(join_team_|leave_team)")
    )
    application.add_handler(CallbackQueryHandler(commentary_callback, pattern="^gcommentary_"))
    application.add_handler(
        CallbackQueryHandler(host_selection_callback, pattern="^become_host$")
    )
    application.add_handler(
        CallbackQueryHandler(captain_selection_callback, pattern="^captain_team_")
    )
    application.add_handler(
        CallbackQueryHandler(team_edit_done_callback, pattern="^team_edit_done$")
    )
    application.add_handler(
        CallbackQueryHandler(over_selection_callback, pattern="^overs_")
    )
    application.add_handler(
        CallbackQueryHandler(toss_callback, pattern="^toss_(heads|tails)$")
    )
    application.add_handler(
        CallbackQueryHandler(toss_decision_callback, pattern="^toss_decision_")
    )
    application.add_handler(
        CallbackQueryHandler(set_edit_team_callback, pattern="^(edit_team_|edit_back)")
    )
    application.add_handler(CallbackQueryHandler(bring_back_unsold_callback, pattern="^bring_back_unsold$"))

    # AI Mode callbacks
    application.add_handler(CallbackQueryHandler(ai_difficulty_callback, pattern="^ai_diff_"))
    application.add_handler(CallbackQueryHandler(ai_over_callback, pattern="^ai_over_"))
    application.add_handler(CallbackQueryHandler(ai_toss_callback, pattern="^ai_toss_"))
    application.add_handler(CallbackQueryHandler(ai_choice_callback, pattern="^ai_choice_"))

    # Stats callbacks
    application.add_handler(CallbackQueryHandler(auction_callback, pattern="^(start_auction|become_auctioneer|back_to_modes)$"))
    application.add_handler(
        CallbackQueryHandler(stats_view_callback, pattern="^stats_view_")
    )
    application.add_handler(
        CallbackQueryHandler(stats_main_callback, pattern="^stats_main_")
    )
    application.add_handler(
        CallbackQueryHandler(mystats_callback, pattern="^mystats_")
    )
    application.add_handler(CallbackQueryHandler(become_auctioneer_callback, pattern="^become_auctioneer$"))
    application.add_handler(CallbackQueryHandler(base_price_callback, pattern="^base_"))
    application.add_handler(CallbackQueryHandler(start_auction_live_callback, pattern="^start_auction_live$"))
    # ================== MESSAGE HANDLERS ==================
    application.add_handler(
        MessageHandler(filters.TEXT & filters.ChatType.PRIVATE, handle_dm_message)
    )
    application.add_handler(
        MessageHandler(filters.TEXT & filters.ChatType.GROUPS, handle_group_input)
    )

    # Error handler
    application.add_error_handler(error_handler)

    application.add_handler(ChatMemberHandler(handle_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER))

    # Start bot
    # Initialize tournament database
    init_tournament_db()
    load_tournament_data()
    
    logger.info("Cricoverse bot starting...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    import os
    from aiohttp import web
    
    # Get PORT from environment (required for deployment platforms like Render/Railway)
    PORT = int(os.environ.get('PORT', 8080))
    
    async def health_check(request):
        """Health check endpoint for deployment platform"""
        return web.Response(text="Cricoverse Bot is running!")
    
    async def start_web_server():
        """Start HTTP server to satisfy port binding requirement"""
        app = web.Application()
        app.router.add_get('/', health_check)
        app.router.add_get('/health', health_check)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', PORT)
        await site.start()
        logger.info(f"✅ Health check server started on port {PORT}")
    
    # Create event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Start web server in background
    loop.create_task(start_web_server())
    
    # Start background lock cleanup task
    loop.create_task(start_lock_cleanup_task())
    
    # Run main bot (this will block)
    main()