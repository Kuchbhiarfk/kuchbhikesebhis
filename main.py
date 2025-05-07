import logging
import asyncio
import signal
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)
from aiohttp import ClientSession
from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)
logger = logging.getLogger(__name__)

# Telegram Bot Token (main bot)
MAIN_BOT_TOKEN = '7490132707:AAEtQQO3Rd3noe9_j9hUS44eMJ68heZ8e0Q'

# Main bot owner ID
MAIN_BOT_OWNER_ID = 5487643307  # Replace with your Telegram user ID (get from @userinfobot)

# MongoDB configuration
MONGO_URL = "mongodb+srv://namanjain123eudhc:opmaster@cluster0.5iokvxo.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "telegram_bot"
MAIN_COLLECTION = "main_bot_data"
ADDED_BOTS_COLLECTION = "added_bots_data"

# Cache for image data
IMAGE_CACHE = None

# Default expiration time (in seconds)
DEFAULT_EXPIRE_TIME = 30

# Default start message for added bots
DEFAULT_START_MSG = (
    "<b>Welcome to Copyright Protector bot 🥰</b>\n\n"
    "<b>Below Given channels link to join 😁</b>\n\n"
    "<quote><b>𝐓𝐡𝐢𝐬 𝐁𝐨𝐭 𝐜𝐨𝐝𝐞 𝐦𝐚𝐝𝐞 𝐛𝐲 𝐇𝐀𝐂𝐊𝐇𝐄𝐈𝐒𝐓 😈</b></quote>\n\n"
    "<i>☆ Create Same bot like this using</i> - <b>@HACKHEIST_PROTECTOR_BOT</b>"
    "{buttons}"
)

# Default link message
DEFAULT_LINK_MSG = (
    "<b>Hello 👑,</b>\n"
    "Link to join your requested channel 👇\n\n"
    "<b><a href='{link}'>𝐂𝐋𝐈𝐂𝐊 𝐌𝐄 :)</a></b>\n\n"
    "<i>NOTE: Link expires in {expire_time} seconds. Join fast!</i>"
)

# Conversation states for broadcast
CHOOSE_DESTINATION, CHOOSE_DELETE_TIME, BROADCAST_MESSAGE = range(3)

# MongoDB client
try:
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    client.server_info()  # Test connection
except ConnectionFailure as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    client = None
    db = None

# Load main bot data
def get_main_bot_data():
    if db is None:  # Fixed: Explicit None check
        logger.error("MongoDB not connected, using default main bot data")
        return {
            'channels': {},
            'expire_time': DEFAULT_EXPIRE_TIME,
            'link_msg': DEFAULT_LINK_MSG,
            'user_ids': []
        }
    try:
        collection = db[MAIN_COLLECTION]
        data = collection.find_one({"_id": "main"})
        if not data:
            default_data = {
                '_id': "main",
                'channels': {},
                'expire_time': DEFAULT_EXPIRE_TIME,
                'link_msg': DEFAULT_LINK_MSG,
                'user_ids': []
            }
            collection.insert_one(default_data)
            return default_data
        return data
    except Exception as e:
        logger.error(f"Failed to get main bot data: {e}")
        return {
            'channels': {},
            'expire_time': DEFAULT_EXPIRE_TIME,
            'link_msg': DEFAULT_LINK_MSG,
            'user_ids': []
        }

# Update main bot data
def update_main_bot_data(data):
    if db is None:  # Fixed: Explicit None check
        logger.error("MongoDB not connected, cannot update main bot data")
        return
    try:
        collection = db[MAIN_COLLECTION]
        collection.update_one(
            {"_id": "main"},
            {"$set": {
                'channels': data.get('channels', {}),
                'expire_time': data.get('expire_time', DEFAULT_EXPIRE_TIME),
                'link_msg': data.get('link_msg', DEFAULT_LINK_MSG),
                'user_ids': data.get('user_ids', [])
            }},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Failed to update main bot data: {e}")

# Load added bot data
def get_added_bot_data(bot_token=None):
    if db is None:  # Fixed: Explicit None check
        logger.error("MongoDB not connected, using empty added bots data")
        return {} if not bot_token else None
    try:
        collection = db[ADDED_BOTS_COLLECTION]
        if bot_token:
            data = collection.find_one({"_id": bot_token})
            return data
        else:
            return {doc['_id']: doc for doc in collection.find()}
    except Exception as e:
        logger.error(f"Failed to get added bots data: {e}")
        return {} if not bot_token else None

# Upsert added bot data
def upsert_added_bot_data(bot_token, data):
    if db is None:  # Fixed: Explicit None check
        logger.error("MongoDB not connected, cannot upsert added bot data")
        return
    try:
        collection = db[ADDED_BOTS_COLLECTION]
        collection.update_one(
            {"_id": bot_token},
            {"$set": {
                'who_added': data.get('who_added'),
                'bot_token': data.get('bot_token'),
                'bot_username': data.get('bot_username'),
                'channels': data.get('channels', {}),
                'expire_time': data.get('expire_time', DEFAULT_EXPIRE_TIME),
                'start_msg': data.get('start_msg', DEFAULT_START_MSG),
                'link_msg': data.get('link_msg', DEFAULT_LINK_MSG),
                'user_ids': data.get('user_ids', [])
            }},
            upsert=True
        )
    except Exception as e:
        logger.error(f"Failed to upsert added bot data: {e}")

# Delete added bot data
def delete_added_bot_data(bot_token):
    if db is None:  # Fixed: Explicit None check
        logger.error("MongoDB not connected, cannot delete added bot data")
        return
    try:
        collection = db[ADDED_BOTS_COLLECTION]
        collection.delete_one({"_id": bot_token})
    except Exception as e:
        logger.error(f"Failed to delete added bot data: {e}")

async def preload_image():
    """Preload image to cache."""
    global IMAGE_CACHE
    if IMAGE_CACHE is None:
        image_url = "https://i.ibb.co/W6d91vd/66f7961e.jpg"
        async with ClientSession() as session:
            try:
                async with session.get(image_url, timeout=5) as response:
                    if response.status == 200:
                        IMAGE_CACHE = await response.read()
                        logger.info("Image preloaded successfully")
                    else:
                        logger.error(f"Failed to preload image: HTTP {response.status}")
            except Exception as e:
                logger.error(f"Image preload failed: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command."""
    if IMAGE_CACHE is None:
        await update.message.reply_text("Image not available. Please try again later.")
        return

    bot_token = context.bot.token
    is_main_bot = bot_token == MAIN_BOT_TOKEN
    user_id = update.effective_user.id

    # Add user ID to user_ids list
    if is_main_bot:
        main_bot_data = get_main_bot_data()
        if user_id not in main_bot_data.get('user_ids', []):
            main_bot_data['user_ids'] = main_bot_data.get('user_ids', []) + [user_id]
            update_main_bot_data(main_bot_data)
    else:
        bot_data = get_added_bot_data(bot_token)
        if bot_data and user_id not in bot_data.get('user_ids', []):
            bot_data['user_ids'] = bot_data.get('user_ids', []) + [user_id]
            upsert_added_bot_data(bot_token, bot_data)

    if is_main_bot:
        caption = (
            "<b>○𝐖𝐞𝐥𝐜𝐨𝐦𝐞 𝐌𝐲 𝐅𝐫𝐢𝐞𝐧𝐝 !!</b>\n\n"
            "<b>❀ Any Problem Contact Us :)</b>\n"
            "<b>♛ HACKHEIST - @HACKHEISTBOT</b>\n\n"
            "<b>𝐅𝐄𝐀𝐓𝐔𝐑𝐄𝐒 💀</b>\n"
            "<i>1. You can add mutiple Channels\n</i>"
            "<i>2. You Can Broadcast to Bots users + Channels which added with Broadcast Msg Delete feature 😁</i>\n\n"
            "<b>For adding your Bot just send > /addbot 1256:giecujwcv like this</b>\n\n"
            "<b>✥ Code Design by HACKHEIST 😈</b>"
        )
        main_bot_data = get_main_bot_data()
        channels = main_bot_data.get('channels', {})
    else:
        bot_data = get_added_bot_data(bot_token) or {}
        caption = bot_data.get('start_msg', DEFAULT_START_MSG)
        channels = bot_data.get('channels', {})

    reply_markup = None
    if channels:
        keyboard = [[InlineKeyboardButton(name, callback_data=channel)] for name, channel in channels.items()]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if not is_main_bot and '{buttons}' in caption:
            caption = caption.replace('{buttons}', '')
        elif not is_main_bot:
            caption += "\n\n<b>Channels:</b>\n{buttons}"

    try:
        await update.message.reply_photo(
            photo=IMAGE_CACHE,
            caption=caption,
            parse_mode='HTML',
            reply_markup=reply_markup,
            protect_content=True
        )
    except Exception as e:
        logger.error(f"Failed to send photo: {e}")
        await update.message.reply_text("Error sending image. Please try again.")

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Start broadcast conversation."""
    bot_token = context.bot.token
    user_id = update.effective_user.id
    is_main_bot = bot_token == MAIN_BOT_TOKEN

    if is_main_bot:
        if user_id != MAIN_BOT_OWNER_ID:
            await update.message.reply_text("Only the main bot owner can use this command.", parse_mode='HTML')
            return ConversationHandler.END
        await update.message.reply_text(
            "<b>Where do you want to send the broadcast message?</b>\n"
            "1. Main bot users\n"
            "2. Main bot channels and groups\n"
            "3. Added bots' users\n"
            "4. Added bots' channels and groups\n"
            "5. All of the above\n\n"
            "<b>Combinations allowed:</b> 1&2, 2&3, 3&4, 2&4, 1&4, 1&3\n\n"
            "Reply with your choice (e.g., '1', '1&2', '5').",
            parse_mode='HTML'
        )
    else:
        bot_data = get_added_bot_data(bot_token)
        if not bot_data or bot_data.get('who_added') != user_id:
            await update.message.reply_text("Only the bot owner can use this command.", parse_mode='HTML')
            return ConversationHandler.END
        await update.message.reply_text(
            "<b>Where do you want to send the broadcast message?</b>\n"
            "1. Your bot's users\n"
            "2. Your bot's channels and groups\n\n"
            "Reply with your choice (e.g., '1' or '2').",
            parse_mode='HTML'
        )
    return CHOOSE_DESTINATION

async def choose_destination(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle destination choice."""
    bot_token = context.bot.token
    is_main_bot = bot_token == MAIN_BOT_TOKEN
    user_choice = update.message.text.strip()

    if is_main_bot:
        valid_choices = ['1', '2', '3', '4', '5', '1&2', '2&3', '3&4', '2&4', '1&4', '1&3']
        if user_choice not in valid_choices:
            await update.message.reply_text(
                "Invalid choice. Please select from: 1, 2, 3, 4, 5, 1&2, 2&3, 3&4, 2&4, 1&4, 1&3",
                parse_mode='HTML'
            )
            return CHOOSE_DESTINATION
    else:
        valid_choices = ['1', '2']
        if user_choice not in valid_choices:
            await update.message.reply_text(
                "Invalid choice. Please select: 1 or 2",
                parse_mode='HTML'
            )
            return CHOOSE_DESTINATION

    context.user_data['broadcast_destinations'] = user_choice
    await update.message.reply_text(
        "Enter deletion time in seconds (e.g., 60 for 1 minute). Messages will be deleted after this time.",
        parse_mode='HTML'
    )
    return CHOOSE_DELETE_TIME

async def choose_delete_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handle deletion time input."""
    try:
        delete_time = int(update.message.text.strip())
        if delete_time < 10 or delete_time > 86400:
            await update.message.reply_text(
                "Deletion time must be between 10 seconds and 86400 seconds (24 hours).",
                parse_mode='HTML'
            )
            return CHOOSE_DELETE_TIME
        context.user_data['delete_time'] = delete_time
        await update.message.reply_text(
            "Enter the message to broadcast (supports HTML, max 4096 characters).",
            parse_mode='HTML'
        )
        return BROADCAST_MESSAGE
    except ValueError:
        await update.message.reply_text(
            "Please provide a valid number of seconds.",
            parse_mode='HTML'
        )
        return CHOOSE_DELETE_TIME

async def broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Send broadcast message and report stats."""
    message_text = update.message.text
    if len(message_text) > 4096:
        await update.message.reply_text(
            "Message is too long. Keep it under 4096 characters.",
            parse_mode='HTML'
        )
        return BROADCAST_MESSAGE

    bot_token = context.bot.token
    is_main_bot = bot_token == MAIN_BOT_TOKEN
    destinations = context.user_data['broadcast_destinations']
    delete_time = context.user_data['delete_time']
    bot = context.bot

    # Initialize sets and counters
    user_ids = set()
    channel_ids = set()
    main_users_sent = 0
    main_channels_sent = 0
    added_users_sent = 0
    added_channels_sent = 0
    messages_to_delete = []

    # Process destinations
    if is_main_bot:
        main_bot_data = get_main_bot_data()
        added_bots_data = get_added_bot_data()
        if destinations == '5':
            destinations = '1&2&3&4'
        dest_list = destinations.split('&')
        if '1' in dest_list:
            user_ids.update(main_bot_data.get('user_ids', []))
        if '2' in dest_list:
            channel_ids.update(main_bot_data.get('channels', {}).values())
        if '3' in dest_list:
            for bot_data in added_bots_data.values():
                user_ids.update(bot_data.get('user_ids', []))
        if '4' in dest_list:
            for bot_data in added_bots_data.values():
                channel_ids.update(bot_data.get('channels', {}).values())
    else:
        bot_data = get_added_bot_data(bot_token) or {}
        if destinations == '1':
            user_ids.update(bot_data.get('user_ids', []))
        elif destinations == '2':
            channel_ids.update(bot_data.get('channels', {}).values())

    # Send to users
    for user_id in user_ids:
        try:
            sent_message = await bot.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            messages_to_delete.append((user_id, sent_message.message_id))
            if is_main_bot:
                main_bot_data = get_main_bot_data()
                if user_id in main_bot_data.get('user_ids', []):
                    main_users_sent += 1
                else:
                    added_users_sent += 1
            else:
                main_users_sent += 1  # For added bot, treat as "your bot"
        except Exception as e:
            logger.error(f"Failed to send broadcast to user {user_id}: {e}")

    # Send to channels/groups
    for channel_id in channel_ids:
        try:
            sent_message = await bot.send_message(
                chat_id=channel_id,
                text=message_text,
                parse_mode='HTML',
                disable_web_page_preview=True
            )
            messages_to_delete.append((channel_id, sent_message.message_id))
            if is_main_bot:
                main_bot_data = get_main_bot_data()
                if channel_id in main_bot_data.get('channels', {}).values():
                    main_channels_sent += 1
                else:
                    added_channels_sent += 1
            else:
                main_channels_sent += 1  # For added bot, treat as "your bot"
        except Exception as e:
            logger.error(f"Failed to send broadcast to channel {channel_id}: {e}")

    # Schedule deletion
    if messages_to_delete:
        asyncio.create_task(schedule_message_deletion(bot, messages_to_delete, delete_time))

    # Prepare stats message
    if is_main_bot:
        stats_message = (
            f"<b>Successful Broadcast to total users and channels:</b>\n"
            f"Main bot - {main_users_sent}\n"
            f"Main bot channels/groups - {main_channels_sent}\n"
            f"Added bots - {added_users_sent}\n"
            f"Added bot channels/groups - {added_channels_sent}\n\n"
            f"Messages will be deleted after {delete_time} seconds."
        )
    else:
        stats_message = (
            f"<b>Successful Broadcast:</b>\n"
            f"Your bot - {main_users_sent}\n"
            f"Your bot channels/groups - {main_channels_sent}\n\n"
            f"Messages will be deleted after {delete_time} seconds."
        )

    await update.message.reply_text(
        stats_message if messages_to_delete else "No messages sent. Check if there are users or channels available.",
        parse_mode='HTML'
    )

    # Clear user data
    context.user_data.clear()
    return ConversationHandler.END

async def schedule_message_deletion(bot, messages, delete_time):
    """Schedule deletion of broadcast messages."""
    await asyncio.sleep(delete_time)
    for chat_id, message_id in messages:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.info(f"Deleted broadcast message in chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to delete message in chat {chat_id}: {e}")

async def broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancel broadcast conversation."""
    await update.message.reply_text("Broadcast cancelled.", parse_mode='HTML')
    context.user_data.clear()
    return ConversationHandler.END

async def add_bot(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /addbot command."""
    bot_token = context.bot.token
    if bot_token != MAIN_BOT_TOKEN:
        await update.message.reply_text("This command is only available in the main bot.", parse_mode='HTML')
        return

    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "Use format: /addbot <bot_token>\nExample: /addbot 123456:ABC-DEF",
            parse_mode='HTML'
        )
        return

    new_bot_token = context.args[0].strip()
    user_id = update.effective_user.id

    if not new_bot_token or ':' not in new_bot_token:
        await update.message.reply_text("Invalid bot token format.", parse_mode='HTML')
        return

    added_bots_data = get_added_bot_data()
    if new_bot_token in added_bots_data:
        await update.message.reply_text("This bot is already added.", parse_mode='HTML')
        return

    try:
        app = ApplicationBuilder().token(new_bot_token).build()
        bot_info = await app.bot.get_me()
        bot_username = f"@{bot_info.username}"
    except Exception as e:
        await update.message.reply_text(f"Invalid bot token: {e}", parse_mode='HTML')
        return

    bot_data = {
        '_id': new_bot_token,
        'who_added': user_id,
        'bot_token': new_bot_token,
        'bot_username': bot_username,
        'channels': {},
        'expire_time': DEFAULT_EXPIRE_TIME,
        'start_msg': DEFAULT_START_MSG,
        'link_msg': DEFAULT_LINK_MSG,
        'user_ids': []
    }
    upsert_added_bot_data(new_bot_token, bot_data)

    # Add handlers for added bot
    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("broadcast", broadcast_start)],
        states={
            CHOOSE_DESTINATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_destination)],
            CHOOSE_DELETE_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_delete_time)],
            BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_message)],
        },
        fallbacks=[CommandHandler("cancel", broadcast_cancel)],
    )
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("addchannel", add_channel))
    app.add_handler(CommandHandler("removechannel", remove_channel))
    app.add_handler(CommandHandler("expiretime", set_expire_time))
    app.add_handler(CommandHandler("setstartmsg", set_start_msg))
    app.add_handler(CommandHandler("setlinkmsg", set_link_msg))
    app.add_handler(CallbackQueryHandler(button))
    app.add_handler(broadcast_conv)
    app.add_error_handler(error_handler)

    try:
        await app.initialize()
        await app.start()
        await app.updater.start_polling(drop_pending_updates=True)
        bot_data['app'] = app
        upsert_added_bot_data(new_bot_token, bot_data)
        await update.message.reply_text(
            f"Bot {bot_username} added successfully! Manage it with /start (for bot check), /addchannel(1st make admin bot ), /removechannel, /expiretime(link expire time), /setstartmsg(Edit /start msg), /setlinkmsg(Edit link send msg), /broadcast",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Failed to start bot {new_bot_token}: {e}")
        await update.message.reply_text(f"Failed to start bot: {e}", parse_mode='HTML')
        delete_added_bot_data(new_bot_token)

async def add_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /addchannel command."""
    bot_token = context.bot.token
    user_id = update.effective_user.id
    is_main_bot = bot_token == MAIN_BOT_TOKEN

    if is_main_bot and user_id != MAIN_BOT_OWNER_ID:
        await update.message.reply_text("Only the main bot owner can add channels.", parse_mode='HTML')
        return
    if not is_main_bot:
        bot_data = get_added_bot_data(bot_token)
        if not bot_data or bot_data.get('who_added') != user_id:
            await update.message.reply_text("Only the bot owner can add channels.", parse_mode='HTML')
            return

    if not context.args:
        await update.message.reply_text(
            "Use format:\n/addchannel\nChannel name : channel_id\nChannel name2 : channel_id2",
            parse_mode='HTML'
        )
        return

    text = ' '.join(context.args).split('\n')
    added, errors = [], []
    
    for line in text:
        line = line.strip()
        if not line:
            continue
        if ':' not in line:
            errors.append(f"Invalid format: {line}")
            continue
        try:
            name, channel_id = [part.strip() for part in line.split(':', 1)]
            if not name or not channel_id or not channel_id.startswith('-100'):
                errors.append(f"Invalid data: {line}")
                continue
            if is_main_bot:
                main_bot_data = get_main_bot_data()
                main_bot_data['channels'][name] = channel_id
                update_main_bot_data(main_bot_data)
            else:
                bot_data = get_added_bot_data(bot_token)
                bot_data['channels'][name] = channel_id
                upsert_added_bot_data(bot_token, bot_data)
            added.append(name)
        except Exception as e:
            errors.append(f"Error: {line} - {str(e)}")

    response = ""
    if added:
        response += "<b>Added channels:</b>\n" + "\n".join(added) + "\n"
    if errors:
        response += "<b>Errors:</b>\n" + "\n".join(errors)
    
    await update.message.reply_text(response or "No valid channels added.", parse_mode='HTML')

async def remove_channel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /removechannel command."""
    bot_token = context.bot.token
    user_id = update.effective_user.id
    is_main_bot = bot_token == MAIN_BOT_TOKEN

    if is_main_bot and user_id != MAIN_BOT_OWNER_ID:
        await update.message.reply_text("Only the main bot owner can remove channels.", parse_mode='HTML')
        return
    if not is_main_bot:
        bot_data = get_added_bot_data(bot_token)
        if not bot_data or bot_data.get('who_added') != user_id:
            await update.message.reply_text("Only the bot owner can remove channels.", parse_mode='HTML')
            return

    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "Use format: /removechannel <channel_name>",
            parse_mode='HTML'
        )
        return

    channel_name = context.args[0].strip()
    if is_main_bot:
        main_bot_data = get_main_bot_data()
        channels = main_bot_data.get('channels', {})
        if channel_name not in channels:
            await update.message.reply_text(f"Channel '{channel_name}' not found.", parse_mode='HTML')
            return
        del channels[channel_name]
        update_main_bot_data(main_bot_data)
    else:
        bot_data = get_added_bot_data(bot_token)
        channels = bot_data.get('channels', {})
        if channel_name not in channels:
            await update.message.reply_text(f"Channel '{channel_name}' not found.", parse_mode='HTML')
            return
        del channels[channel_name]
        upsert_added_bot_data(bot_token, bot_data)

    await update.message.reply_text(f"Channel '{channel_name}' removed.", parse_mode='HTML')

async def set_expire_time(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /expiretime command."""
    bot_token = context.bot.token
    user_id = update.effective_user.id
    is_main_bot = bot_token == MAIN_BOT_TOKEN

    if is_main_bot and user_id != MAIN_BOT_OWNER_ID:
        await update.message.reply_text("Only the main bot owner can set expire time.", parse_mode='HTML')
        return
    if not is_main_bot:
        bot_data = get_added_bot_data(bot_token)
        if not bot_data or bot_data.get('who_added') != user_id:
            await update.message.reply_text("Only the bot owner can set expire time.", parse_mode='HTML')
            return

    if not context.args or len(context.args) != 1:
        await update.message.reply_text(
            "Use format: /expiretime <seconds>\nExample: /expiretime 60",
            parse_mode='HTML'
        )
        return

    try:
        seconds = int(context.args[0])
        if seconds < 10 or seconds > 86400:
            await update.message.reply_text(
                "Expiration time must be between 10 seconds and 86400 seconds (24 hours).",
                parse_mode='HTML'
            )
            return
        if is_main_bot:
            main_bot_data = get_main_bot_data()
            main_bot_data['expire_time'] = seconds
            update_main_bot_data(main_bot_data)
        else:
            bot_data = get_added_bot_data(bot_token)
            bot_data['expire_time'] = seconds
            upsert_added_bot_data(bot_token, bot_data)
        await update.message.reply_text(
            f"Invite link expiration time set to {seconds} seconds.",
            parse_mode='HTML'
        )
    except ValueError:
        await update.message.reply_text(
            "Please provide a valid number of seconds.",
            parse_mode='HTML'
        )

async def set_start_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setstartmsg command."""
    bot_token = context.bot.token
    user_id = update.effective_user.id

    if bot_token == MAIN_BOT_TOKEN:
        await update.message.reply_text("This command is only available for added bots.", parse_mode='HTML')
        return

    bot_data = get_added_bot_data(bot_token)
    if not bot_data or bot_data.get('who_added') != user_id:
        await update.message.reply_text("Only the bot owner can set the start message.", parse_mode='HTML')
        return

    if not context.args:
        await update.message.reply_text(
            "Use format: /setstartmsg <message>\nUse {buttons} to include channel buttons.",
            parse_mode='HTML'
        )
        return

    message = ' '.join(context.args)
    if len(message) > 4096:
        await update.message.reply_text(
            "Message is too long. Keep it under 4096 characters.",
            parse_mode='HTML'
        )
        return

    bot_data['start_msg'] = message
    upsert_added_bot_data(bot_token, bot_data)
    await update.message.reply_text(
        "Start message updated successfully. Use /start to preview.",
        parse_mode='HTML'
    )

async def set_link_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /setlinkmsg command."""
    bot_token = context.bot.token
    user_id = update.effective_user.id
    is_main_bot = bot_token == MAIN_BOT_TOKEN

    if is_main_bot and user_id != MAIN_BOT_OWNER_ID:
        await update.message.reply_text("Only the main bot owner can set the link message.", parse_mode='HTML')
        return
    if not is_main_bot:
        bot_data = get_added_bot_data(bot_token)
        if not bot_data or bot_data.get('who_added') != user_id:
            await update.message.reply_text("Only the bot owner can set the link message.", parse_mode='HTML')
            return

    if not context.args:
        await update.message.reply_text(
            "Use format: /setlinkmsg <message>\nUse {link} for the invite link and {expire_time} for expiration time.",
            parse_mode='HTML'
        )
        return

    message = ' '.join(context.args)
    if len(message) > 4096:
        await update.message.reply_text(
            "Message is too long. Keep it under 4096 characters.",
            parse_mode='HTML'
        )
        return
    if '{link}' not in message:
        await update.message.reply_text(
            "Message must include {link} placeholder for the invite link.",
            parse_mode='HTML'
        )
        return

    if is_main_bot:
        main_bot_data = get_main_bot_data()
        main_bot_data['link_msg'] = message
        update_main_bot_data(main_bot_data)
    else:
        bot_data = get_added_bot_data(bot_token)
        bot_data['link_msg'] = message
        upsert_added_bot_data(bot_token, bot_data)
    await update.message.reply_text(
        "Invite link message updated successfully.",
        parse_mode='HTML'
    )

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle button clicks."""
    query = update.callback_query
    await query.answer()

    bot_token = context.bot.token
    is_main_bot = bot_token == MAIN_BOT_TOKEN
    if is_main_bot:
        main_bot_data = get_main_bot_data()
        expire_time = main_bot_data.get('expire_time', DEFAULT_EXPIRE_TIME)
        link_msg = main_bot_data.get('link_msg', DEFAULT_LINK_MSG)
    else:
        bot_data = get_added_bot_data(bot_token) or {}
        expire_time = bot_data.get('expire_time', DEFAULT_EXPIRE_TIME)
        link_msg = bot_data.get('link_msg', DEFAULT_LINK_MSG)

    try:
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=query.data,
            expire_date=datetime.now() + timedelta(seconds=expire_time),
            member_limit=None
        )

        message_text = link_msg.format(link=invite_link.invite_link, expire_time=expire_time)

        message = await query.message.reply_text(
            message_text,
            parse_mode='HTML',
            protect_content=True,
            disable_web_page_preview=True
        )
        
        asyncio.create_task(handle_link_cleanup(context.bot, query.data, invite_link.invite_link, message, expire_time))
    except Exception as e:
        await query.message.reply_text(f"Failed to generate link: {e}", parse_mode='HTML')

async def handle_link_cleanup(bot, chat_id, invite_link, message, expire_time):
    """Clean up invite links."""
    await asyncio.sleep(expire_time + 10)
    try:
        await bot.delete_message(chat_id=message.chat_id, message_id=message.message_id)
        await bot.revoke_chat_invite_link(chat_id=chat_id, invite_link=invite_link)
        logger.info("Invite link revoked and message deleted")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors."""
    logger.warning(f"Error: {context.error}")

async def main():
    """Run main bot and added bots."""
    await preload_image()

    main_app = ApplicationBuilder().token(MAIN_BOT_TOKEN).build()
    broadcast_conv = ConversationHandler(
        entry_points=[CommandHandler("broadcast", broadcast_start)],
        states={
            CHOOSE_DESTINATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_destination)],
            CHOOSE_DELETE_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_delete_time)],
            BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_message)],
        },
        fallbacks=[CommandHandler("cancel", broadcast_cancel)],
    )
    main_app.add_handler(CommandHandler("start", start))
    main_app.add_handler(CommandHandler("addchannel", add_channel))
    main_app.add_handler(CommandHandler("removechannel", remove_channel))
    main_app.add_handler(CommandHandler("expiretime", set_expire_time))
    main_app.add_handler(CommandHandler("setlinkmsg", set_link_msg))
    main_app.add_handler(CommandHandler("addbot", add_bot))
    main_app.add_handler(CallbackQueryHandler(button))
    main_app.add_handler(broadcast_conv)
    main_app.add_error_handler(error_handler)

    added_bots_data = get_added_bot_data()
    for bot_token in list(added_bots_data.keys()):
        try:
            app = ApplicationBuilder().token(bot_token).build()
            broadcast_conv = ConversationHandler(
                entry_points=[CommandHandler("broadcast", broadcast_start)],
                states={
                    CHOOSE_DESTINATION: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_destination)],
                    CHOOSE_DELETE_TIME: [MessageHandler(filters.TEXT & ~filters.COMMAND, choose_delete_time)],
                    BROADCAST_MESSAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_message)],
                },
                fallbacks=[CommandHandler("cancel", broadcast_cancel)],
            )
            app.add_handler(CommandHandler("start", start))
            app.add_handler(CommandHandler("addchannel", add_channel))
            app.add_handler(CommandHandler("removechannel", remove_channel))
            app.add_handler(CommandHandler("expiretime", set_expire_time))
            app.add_handler(CommandHandler("setstartmsg", set_start_msg))
            app.add_handler(CommandHandler("setlinkmsg", set_link_msg))
            app.add_handler(CallbackQueryHandler(button))
            app.add_handler(broadcast_conv)
            app.add_error_handler(error_handler)
            await app.initialize()
            await app.start()
            await app.updater.start_polling(drop_pending_updates=True)
            bot_data = get_added_bot_data(bot_token)
            bot_data['app'] = app
            upsert_added_bot_data(bot_token, bot_data)
        except Exception as e:
            logger.error(f"Failed to start bot {bot_token}: {e}")
            delete_added_bot_data(bot_token)

    try:
        await main_app.initialize()
        await main_app.start()
        await main_app.updater.start_polling(drop_pending_updates=True)
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await main_app.updater.stop()
        await main_app.stop()
        await main_app.shutdown()
        added_bots_data = get_added_bot_data()
        for bot_token, data in list(added_bots_data.items()):
            if 'app' in data:
                try:
                    await data['app'].updater.stop()
                    await data['app'].stop()
                    await data['app'].shutdown()
                except Exception as e:
                    logger.error(f"Failed to shutdown bot {bot_token}: {e}")
                bot_data = get_added_bot_data(bot_token)
                if bot_data:
                    del bot_data['app']
                    upsert_added_bot_data(bot_token, bot_data)
        if client:
            client.close()

if __name__ == "__main__":
    # Create a new event loop explicitly
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def signal_handler(sig, frame):
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
