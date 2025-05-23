import asyncio
import uuid
import base64
import logging
from queue import Queue
from typing import List, Dict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError
from motor.motor_asyncio import AsyncIOMotorClient  # Use motor for async MongoDB
from pymongo.errors import ConnectionFailure

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Bot configuration
BOT_TOKEN = "7931405874:AAGodglFGX3zOG49z5dxMff_GpaNLgxZ9OE"
OWNER_ID = 5487643307
MONGODB_URI = "mongodb+srv://namanjain123eudhc:opmaster@cluster0.5iokvxo.mongodb.net/?retryWrites=true&w=majority"
DB_NAME = "Cluster0"

# Task queue for worker system
task_queue = Queue()
WORKER_COUNT = 3  # Number of concurrent workers

# MongoDB async client
try:
    client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    db = client[DB_NAME]
    users_collection = db["users"]
    logs_collection = db["logs"]
except ConnectionFailure as e:
    logger.error(f"MongoDB connection failed: {e}")
    raise SystemExit("Failed to connect to MongoDB.")

# Global configuration (loaded asynchronously)
async def initialize_logs():
    default_config = {
        "_id": "config",
        "force_sub_channels": [],
        "approved_channels": [],
        "protect_content": True,
        "auto_delete_time": 600,
        "caption_template": None
    }
    if not await logs_collection.find_one({"_id": "config"}):
        await logs_collection.insert_one(default_config)
    config = await logs_collection.find_one({"_id": "config"})
    return config

# Load initial config
FORCE_SUB_CHANNEL_IDS = []
APPROVED_CHANNEL_IDS = []
PROTECT_CONTENT = True
AUTO_DELETE_TIME = 600
CAPTION_TEMPLATE = None
FORCE_SUB_INVITE_LINKS = {}
batch_storage = {}  # Temporary storage for legacy batch_ UUIDs

async def load_config():
    global FORCE_SUB_CHANNEL_IDS, APPROVED_CHANNEL_IDS, PROTECT_CONTENT, AUTO_DELETE_TIME, CAPTION_TEMPLATE
    config = await initialize_logs()
    FORCE_SUB_CHANNEL_IDS = config["force_sub_channels"]
    APPROVED_CHANNEL_IDS = config["approved_channels"]
    PROTECT_CONTENT = config["protect_content"]
    AUTO_DELETE_TIME = config["auto_delete_time"]
    CAPTION_TEMPLATE = config["caption_template"]

# Worker function to process tasks
async def worker(worker_id: int, app: Application):
    logger.info(f"Worker {worker_id} started")
    while True:
        try:
            task = task_queue.get_nowait()
        except Queue.Empty:
            await asyncio.sleep(0.1)
            continue

        try:
            task_type, update, context, data = task
            if task_type == "batch_process":
                await process_batch_task(update, context, data)
            elif task_type == "broadcast":
                await process_broadcast_task(update, context, data)
        except Exception as e:
            logger.error(f"Worker {worker_id} error: {e}")
        finally:
            task_queue.task_done()

# Process batch task (for /start with batch links)
async def process_batch_task(update: Update, context: ContextTypes.DEFAULT_TYPE, data: Dict):
    chat_id = update.effective_chat.id
    batch_id = data.get("batch_id")
    encoded_string = data.get("encoded_string")

    if batch_id:
        # Legacy batch_ UUID handling
        if batch_id in batch_storage:
            batch = batch_storage[batch_id]
            await forward_messages(update, context, batch["channel_id"], batch["from_msg"], batch["to_msg"], chat_id)
            del batch_storage[batch_id]
        else:
            await update.message.reply_text(
                "<b>❌ Invalid Link</b>\n<i>The batch link is invalid or has expired.</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
    elif encoded_string:
        # New get- format handling
        try:
            decoded_bytes = base64.b64decode(encoded_string)
            decoded_string = decoded_bytes.decode('utf-8')
            parts = decoded_string.split('-')
            if len(parts) != 3 or parts[0] != 'get':
                raise ValueError("Invalid link format")

            num1 = int(parts[1])
            num2 = int(parts[2])
            channel_id = None
            from_msg = None
            to_msg = None
            for cid in APPROVED_CHANNEL_IDS:
                abs_cid = abs(int(cid))
                from_msg_candidate = num1 // abs_cid
                to_msg_candidate = num2 // abs_cid
                if (num1 % abs_cid == 0 and num2 % abs_cid == 0 and
                        to_msg_candidate >= from_msg_candidate):
                    channel_id = cid
                    from_msg = from_msg_candidate
                    to_msg = to_msg_candidate
                    break

            if not channel_id:
                raise ValueError("No matching approved channel ID found")

            await forward_messages(update, context, channel_id, from_msg, to_msg, chat_id)
        except (base64.binascii.Error, ValueError, TelegramError) as e:
            await update.message.reply_text(
                f"<b>❌ Invalid Link</b>\n<i>The link is invalid or corrupted: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )

# Forward messages for batch processing
async def forward_messages(update: Update, context: ContextTypes.DEFAULT_TYPE, channel_id: str, from_msg: int, to_msg: int, chat_id: int):
    forwarded = []
    skipped = []
    forwarded_message_ids = []

    for msg_id in range(from_msg, to_msg + 1):
        try:
            temp_message = await context.bot.forward_message(
                chat_id=OWNER_ID,
                from_chat_id=channel_id,
                message_id=msg_id
            )
            caption = temp_message.caption
            file_name = None
            if temp_message.document:
                file_name = temp_message.document.file_name
            elif temp_message.video:
                file_name = temp_message.video.file_name
            elif temp_message.photo:
                file_name = None

            original_caption = caption or file_name or "Media"
            new_caption = CAPTION_TEMPLATE.format(caption=original_caption) if CAPTION_TEMPLATE else f"{original_caption}\nHACKHEIST"

            sent_message = await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=channel_id,
                message_id=msg_id,
                caption=new_caption if temp_message.document or temp_message.video or temp_message.photo else None,
                parse_mode="HTML" if (temp_message.document or temp_message.video or temp_message.photo) else None,
                protect_content=PROTECT_CONTENT
            )
            forwarded_message_ids.append(sent_message.message_id)
            await context.bot.delete_message(chat_id=OWNER_ID, message_id=temp_message.message_id)
            forwarded.append(msg_id)
        except TelegramError as e:
            if "message to copy not found" in str(e).lower() or "message to forward not found" in str(e).lower():
                skipped.append(msg_id)
            else:
                skipped.append(msg_id)
                await update.message.reply_text(
                    f"<b>❌ Error</b>\n<i>Failed to forward message <code>{msg_id}</code>: <code>{e}</code></i>",
                    parse_mode="HTML",
                    protect_content=PROTECT_CONTENT
                )

    if forwarded:
        notification = await context.bot.send_message(
            chat_id=chat_id,
            text=f"<b>⏰ Your above Msg_ids are deleting in {AUTO_DELETE_TIME} seconds</b>",
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )
        asyncio.create_task(schedule_deletion(context, chat_id, forwarded_message_ids, notification.message_id, AUTO_DELETE_TIME))

    feedback = []
    if skipped:
        feedback.append(f"<b>⚠️ Skipped Messages (likely deleted):</b> <code>{', '.join(map(str, skipped))}</code>")
    if not forwarded and not skipped:
        feedback.append("<i>No messages were forwarded.</i>")

    if feedback:
        await update.message.reply_text(
            "\n".join(feedback),
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )

# Process broadcast task
async def process_broadcast_task(update: Update, context: ContextTypes.DEFAULT_TYPE, data: Dict):
    broadcast_message = data["message"]
    delete_after = data["delete_after"]
    sent_successfully = 0
    blocked = 0
    message_ids = {}

    async def send_to_user(uid):
        nonlocal sent_successfully, blocked, message_ids
        try:
            if broadcast_message.text:
                sent_message = await context.bot.send_message(
                    chat_id=uid,
                    text=broadcast_message.text,
                    parse_mode="HTML" if broadcast_message.text else None,
                    protect_content=PROTECT_CONTENT
                )
            elif broadcast_message.photo:
                sent_message = await context.bot.send_photo(
                    chat_id=uid,
                    photo=broadcast_message.photo[-1].file_id,
                    caption=broadcast_message.caption,
                    parse_mode="HTML" if broadcast_message.caption else None,
                    protect_content=PROTECT_CONTENT
                )
            elif broadcast_message.video:
                sent_message = await context.bot.send_video(
                    chat_id=uid,
                    video=broadcast_message.video.file_id,
                    caption=broadcast_message.caption,
                    parse_mode="HTML" if broadcast_message.caption else None,
                    protect_content=PROTECT_CONTENT
                )
            elif broadcast_message.sticker:
                sent_message = await context.bot.send_sticker(
                    chat_id=uid,
                    sticker=broadcast_message.sticker.file_id,
                    protect_content=PROTECT_CONTENT
                )
            else:
                return

            sent_successfully += 1
            message_ids[uid] = sent_message.message_id
        except TelegramError as e:
            if "blocked by user" in str(e).lower() or "chat not found" in str(e).lower():
                blocked += 1
            else:
                logger.warning(f"Failed to send to user {uid}: {e}")

    # Fetch users asynchronously
    user_ids = [user["_id"] async for user in users_collection.find({}, {"_id": 1})]
    # Send messages in parallel with limited concurrency
    tasks = [send_to_user(uid) for uid in user_ids]
    for i in range(0, len(tasks), 10):  # Batch of 10 to avoid rate limits
        await asyncio.gather(*tasks[i:i+10])

    # Schedule deletion
    await asyncio.sleep(delete_after)
    for uid, msg_id in message_ids.items():
        try:
            await context.bot.delete_message(chat_id=uid, message_id=msg_id)
        except TelegramError:
            pass

    total_users = len(user_ids)
    await update.message.reply_text(
        f"<b>📊 Broadcast Results</b>\n"
        f"<b>Total users:</b> <code>{total_users}</code>\n"
        f"<b>Successfully sent:</b> <code>{sent_successfully}</code>\n"
        f"<b>Blocked bot:</b> <code>{blocked}</code>",
        parse_mode="HTML",
        protect_content=PROTECT_CONTENT
    )

async def schedule_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_ids: List[int], notification_message_id: int, delete_time: int):
    await asyncio.sleep(delete_time)
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except TelegramError:
            pass
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=notification_message_id,
            text="<b>✅ Your above Msg_ids deleted Successfully</b>",
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )
    except TelegramError:
        pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    args = context.args

    # Save user asynchronously
    if not await users_collection.find_one({"_id": user_id}):
        await users_collection.insert_one({"_id": user_id})

    if args:
        if not FORCE_SUB_CHANNEL_IDS:
            await update.message.reply_text(
                "<b>⚠️ No Force-Subscribe Channels</b>\n<i>Please contact the bot owner.</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
            return

        non_subscribed_channels = []
        for channel_id in FORCE_SUB_CHANNEL_IDS:
            try:
                member = await context.bot.get_chat_member(channel_id, user_id)
                if member.status not in ['member', 'administrator', 'creator']:
                    non_subscribed_channels.append(channel_id)
            except TelegramError as e:
                await update.message.reply_text(
                    f"<b>❌ Error</b>\n<i>Failed to check subscription: <code>{e}</code></i>",
                    parse_mode="HTML",
                    protect_content=PROTECT_CONTENT
                )
                return

        if non_subscribed_channels:
            bot_username = (await context.bot.get_me()).username
            batch_link = f"https://t.me/{bot_username}?start={args[0]}"
            keyboard = []
            channel_buttons = []

            for i, channel_id in enumerate(non_subscribed_channels, 1):
                if channel_id in FORCE_SUB_INVITE_LINKS:
                    invite_link = FORCE_SUB_INVITE_LINKS[channel_id]
                else:
                    try:
                        invite_link = await context.bot.export_chat_invite_link(channel_id)
                        FORCE_SUB_INVITE_LINKS[channel_id] = invite_link
                    except TelegramError as e:
                        channel_number = channel_id[4:]
                        invite_link = f"https://t.me/c/{channel_number}"
                        FORCE_SUB_INVITE_LINKS[channel_id] = invite_link
                        await update.message.reply_text(
                            f"<b>⚠️ Warning</b>\n<i>Error generating invite link: <code>{e}</code>.</i>",
                            parse_mode="HTML",
                            protect_content=PROTECT_CONTENT
                        )

                channel_buttons.append(InlineKeyboardButton(f"Join Channel {i}", url=invite_link))

            if len(channel_buttons) >= 2:
                keyboard.append(channel_buttons[:2])
                for button in channel_buttons[2:]:
                    keyboard.append([button])
            else:
                keyboard.append(channel_buttons)

            keyboard.append([InlineKeyboardButton("Try Again", url=batch_link)])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "<b>📢 Join Required Channels</b>\n<i>First, join the following channel(s):</i>",
                reply_markup=reply_markup,
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
            return

        # Enqueue batch processing task
        if args[0].startswith("batch_"):
            batch_id = args[0].replace("batch_", "")
            task_queue.put(("batch_process", update, context, {"batch_id": batch_id}))
        else:
            task_queue.put(("batch_process", update, context, {"encoded_string": args[0]}))
    else:
        await update.message.reply_text(
            "<b>👋 Welcome!</b>\n<i>Use <code>/batch</code> to forward messages.</i>",
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>Owner-only command.</i>",
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )
        return

    await update.message.reply_text(
        "<b>📢 Broadcast Message</b>\n<i>Send the message to broadcast (text, photo, video, sticker).</i>",
        parse_mode="HTML",
        protect_content=PROTECT_CONTENT
    )
    context.user_data["state"] = "awaiting_broadcast_message"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_data = context.user_data
    user_id = update.effective_user.id
    message = update.message
    text = message.text.strip() if message.text else ""

    if user_data.get("state") == "awaiting_broadcast_message":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>Owner-only action.</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
            user_data.clear()
            return

        user_data["broadcast_message"] = message
        await message.reply_text(
            "<b>⏰ Set Deletion Time</b>\n<i>Send the time (in seconds) for deletion (e.g., 30).</i>",
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )
        user_data["state"] = "awaiting_broadcast_time"
        return

    if user_data.get("state") == "awaiting_broadcast_time":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>Owner-only action.</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
            user_data.clear()
            return

        try:
            delete_after = int(text)
            if delete_after <= 0:
                raise ValueError("Time must be positive")
        except ValueError:
            await message.reply_text(
                "<b>❌ Invalid Time</b>\n<i>Send a valid number of seconds (e.g., 30).</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
            return

        # Enqueue broadcast task
        task_queue.put(("broadcast", update, context, {
            "message": user_data["broadcast_message"],
            "delete_after": delete_after
        }))
        user_data.clear()
        return

    # Handle other states (e.g., awaiting_auto_delete_time, awaiting_caption_template, etc.)
    # Add your existing logic here, using async MongoDB operations
    # Example for auto_delete_time:
    if user_data.get("state") == "awaiting_auto_delete_time":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>Owner-only action.</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
            user_data.clear()
            return

        try:
            seconds = int(text)
            if seconds <= 0 or seconds > 86400:
                raise ValueError("Time must be 1-86400 seconds")
            global AUTO_DELETE_TIME
            AUTO_DELETE_TIME = seconds
            await logs_collection.update_one(
                {"_id": "config"},
                {"$set": {"auto_delete_time": seconds}}
            )
            await message.reply_text(
                f"<b>✅ Auto-Deletion Time Set</b>\n<i>Messages will be deleted after {seconds} seconds.</i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
        except ValueError as e:
            await message.reply_text(
                f"<b>❌ Invalid Time</b>\n<i>Send a valid number of seconds: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=PROTECT_CONTENT
            )
        user_data.clear()
        return

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")
    if update and update.message:
        await update.message.reply_text(
            f"<b>❌ Bot Error</b>\n<i>An error occurred: <code>{context.error}</code></i>",
            parse_mode="HTML",
            protect_content=PROTECT_CONTENT
        )

def main():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        application = Application.builder().token(BOT_TOKEN).build()
        loop.run_until_complete(load_config())

        # Start workers
        for i in range(WORKER_COUNT):
            asyncio.ensure_future(worker(i + 1, application))

        # Register handlers
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("broadcast", broadcast))
        application.add_handler(CommandHandler("auto_delete_msg", auto_delete_msg))
        application.add_handler(CommandHandler("protect_content", protect_content))
        application.add_handler(CommandHandler("new_caption", new_caption))
        application.add_handler(CommandHandler("set_force_sub_ids", set_force_sub_ids))
        application.add_handler(CommandHandler("set_channel_ids", set_channel_ids))
        application.add_handler(CommandHandler("batch", batch))
        application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
        application.add_error_handler(error_handler)

        loop.run_until_complete(application.run_polling(allowed_updates=Update.ALL_TYPES))
    finally:
        client.close()
        loop.close()

if __name__ == "__main__":
    main()
