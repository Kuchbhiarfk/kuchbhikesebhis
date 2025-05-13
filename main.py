import asyncio
import uuid
import base64
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError
from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.int64 import Int64

# Load environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN", "7931405874:AAGodglFGX3zOG49z5dxMff_GpaNLgxZ9OE")
OWNER_ID = int(os.getenv("OWNER_ID", 5487643307))
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb+srv://namanjain123eudhc:opmaster@cluster0.5iokvxo.mongodb.net/?retryWrites=true&w=majority")
DB_NAME = os.getenv("DB_NAME", "Cluster0")

# In-memory storage
batch_storage = {}
FORCE_SUB_INVITE_LINKS = {}

def init_db():
    """Initialize MongoDB connection and ensure logs collection has a document."""
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[DB_NAME]
        logs_collection = db.logs
        if logs_collection.count_documents({}) == 0:
            logs_collection.insert_one({
                "force_sub_channel_ids": [],
                "approved_channel_ids": [],
                "protect_content": True,
                "auto_delete_time": 600,
                "caption_template": None
            })
        return db
    except Exception as e:
        print(f"MongoDB Connection Error: {e}")
        raise

async def auto_delete_msg(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /auto_delete_msg command to set global auto-deletion time (owner-only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>This command is restricted to the bot owner.</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    await update.message.reply_text(
        "<b>⏰ Set Auto-Deletion Time</b>\n<i>Please send the time in seconds for auto-deletion (e.g., 600 for 10 minutes).</i>",
        parse_mode="HTML",
        protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
    )
    context.user_data["state"] = "awaiting_auto_delete_time"

async def protect_content(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /protect_content command to toggle protect_content setting (owner-only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>This command is restricted to the bot owner.</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    args = context.args
    if not args or args[0].lower() not in ["true", "false"]:
        await update.message.reply_text(
            "<b>⚠️ Invalid Input</b>\n<i>Please use: /protect_content true or /protect_content false</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    protect_content = args[0].lower() == "true"
    db = context.bot_data["db"]
    db.logs.update_one({}, {"$set": {"protect_content": protect_content}})
    status = "enabled" if protect_content else "disabled"
    await update.message.reply_text(
        f"<b>✅ Protect Content Updated</b>\n<i>Content protection is now <b>{status}</b>.</i>",
        parse_mode="HTML",
        protect_content=protect_content
    )

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /broadcast command to initiate broadcasting (owner-only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>This command is restricted to the bot owner.</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    await update.message.reply_text(
        "<b>📢 Broadcast Message</b>\n<i>Please send the message to broadcast (text, photo, video, or sticker). Text supports HTML formatting.</i>",
        parse_mode="HTML",
        protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
    )
    context.user_data["state"] = "awaiting_broadcast_message"

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Process the broadcast message and send to all non-banned users."""
    user_data = context.user_data
    user_id = update.effective_user.id
    message = update.message
    db = context.bot_data["db"]
    protect_content = db.logs.find_one({})["protect_content"]

    if user_data.get("state") == "awaiting_broadcast_message":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>This action is restricted to the bot owner.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        user_data["broadcast_message"] = message
        await message.reply_text(
            "<b>⏰ Set Deletion Time</b>\n<i>Please send the time (in seconds) after which the broadcast message should be deleted (e.g., 30).</i>",
            parse_mode="HTML",
            protect_content=protect_content
        )
        user_data["state"] = "awaiting_broadcast_time"
        return

    if user_data.get("state") == "awaiting_broadcast_time":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>This action is restricted to the bot owner.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        text = message.text.strip() if message.text else ""
        try:
            delete_after = int(text)
            if delete_after <= 0:
                raise ValueError("Time must be positive")
        except ValueError:
            await message.reply_text(
                "<b>❌ Invalid Time</b>\n<i>Please send a valid number of seconds (e.g., 30).</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            return

        broadcast_message = user_data["broadcast_message"]
        sent_successfully = 0
        blocked = 0
        failed = 0
        message_ids = {}
        failed_users = []

        # Fetch all non-banned users
        users = list(db.users.find({"ban_status.is_banned": False}))
        total_users = len(users)
        if total_users == 0:
            await message.reply_text(
                "<b>⚠️ No Users</b>\n<i>No non-banned users found in the database.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        for user in users:
            uid = int(user["id"])  # Convert NumberLong to int
            try:
                if broadcast_message.text:
                    sent_message = await context.bot.send_message(
                        chat_id=uid,
                        text=broadcast_message.text,
                        parse_mode="HTML" if broadcast_message.text else None,
                        protect_content=protect_content
                    )
                elif broadcast_message.photo:
                    sent_message = await context.bot.send_photo(
                        chat_id=uid,
                        photo=broadcast_message.photo[-1].file_id,
                        caption=broadcast_message.caption,
                        parse_mode="HTML" if broadcast_message.caption else None,
                        protect_content=protect_content
                    )
                elif broadcast_message.video:
                    sent_message = await context.bot.send_video(
                        chat_id=uid,
                        video=broadcast_message.video.file_id,
                        caption=broadcast_message.caption,
                        parse_mode="HTML" if broadcast_message.caption else None,
                        protect_content=protect_content
                    )
                elif broadcast_message.sticker:
                    sent_message = await context.bot.send_sticker(
                        chat_id=uid,
                        sticker=broadcast_message.sticker.file_id,
                        protect_content=protect_content
                    )
                else:
                    await message.reply_text(
                        "<b>❌ Unsupported Message Type</b>\n<i>Only text, photo, video, or sticker messages are supported.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return

                sent_successfully += 1
                message_ids[uid] = sent_message.message_id
                await asyncio.sleep(0.1)  # Small delay to avoid rate limits
            except TelegramError as e:
                if "blocked by user" in str(e).lower() or "chat not found" in str(e).lower():
                    blocked += 1
                else:
                    failed += 1
                    failed_users.append((uid, str(e)))
                # Log failure to owner
                await context.bot.send_message(
                    chat_id=OWNER_ID,
                    text=f"<b>⚠️ Broadcast Failure</b>\n<i>User <code>{uid}</code>: <code>{e}</code></i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )

        # Delete messages after delay
        await asyncio.sleep(delete_after)
        deleted = 0
        for uid, msg_id in message_ids.items():
            try:
                await context.bot.delete_message(chat_id=uid, message_id=msg_id)
                deleted += 1
            except TelegramError as e:
                await context.bot.send_message(
                    chat_id=OWNER_ID,
                    text=f"<b>⚠️ Deletion Failure</b>\n<i>User <code>{uid}</code>, Message <code>{msg_id}</code>: <code>{e}</code></i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )

        # Send summary
        feedback = [
            f"<b>📊 Broadcast Summary</b>",
            f"<b>Total users:</b> <code>{total_users}</code>",
            f"<b>Successfully sent:</b> <code>{sent_successfully}</code>",
            f"<b>Blocked or invalid:</b> <code>{blocked}</code>",
            f"<b>Other failures:</b> <code>{failed}</code>",
            f"<b>Deleted:</b> <code>{deleted}</code>"
        ]
        if failed_users:
            feedback.append("<b>Failed Users:</b>")
            for uid, error in failed_users[:5]:  # Limit to 5 for brevity
                feedback.append(f"<i>User <code>{uid}</code>: <code>{error}</code></i>")
            if len(failed_users) > 5:
                feedback.append(f"<i>...and {len(failed_users) - 5} more</i>")

        await message.reply_text(
            "\n".join(feedback),
            parse_mode="HTML",
            protect_content=protect_content
        )
        user_data.clear()
        return

async def new_caption(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /new_caption command to set custom caption template (owner-only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>This command is restricted to the bot owner.</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    await update.message.reply_text(
        "<b>📝 Set Caption Template</b>\n<i>Please send the caption template, using {caption} as the placeholder for the original caption or file name (e.g., <code><b>{caption}\nHACKHEIST</b></code>).</i>",
        parse_mode="HTML",
        protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
    )
    context.user_data["state"] = "awaiting_caption_template"

async def delete_messages(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_ids: list, notification_message_id: int) -> None:
    """Delete forwarded messages and edit the notification message."""
    db = context.bot_data["db"]
    protect_content = db.logs.find_one({})["protect_content"]
    for msg_id in message_ids:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except TelegramError as e:
            try:
                await context.bot.send_message(
                    chat_id=OWNER_ID,
                    text=f"<b>⚠️ Deletion Error</b>\n<i>Failed to delete message {msg_id} in chat {chat_id}: <code>{e}</code></i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
            except TelegramError:
                pass

    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=notification_message_id,
            text="<b>✅ Your above Msg_ids deleted Successfully</b>",
            parse_mode="HTML",
            protect_content=protect_content
        )
    except TelegramError as e:
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=f"<b>⚠️ Notification Edit Error</b>\n<i>Failed to edit notification {notification_message_id} in chat {chat_id}: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
        except TelegramError:
            pass

async def schedule_deletion(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_ids: list, notification_message_id: int, delete_time: int) -> None:
    """Schedule deletion of messages after the specified time."""
    await asyncio.sleep(delete_time)
    await delete_messages(context, chat_id, message_ids, notification_message_id)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command, including deep links with Base64-encoded get- format or legacy batch_ UUID."""
    user_id = update.effective_user.id
    user = update.effective_user
    chat_id = update.effective_chat.id
    db = context.bot_data["db"]
    args = context.args

    # Check if user exists in users collection; if not, add them
    user_doc = db.users.find_one({"id": Int64(user_id)})
    if not user_doc:
        db.users.insert_one({
            "id": Int64(user_id),
            "name": user.full_name or ".",
            "ban_status": {
                "is_banned": False,
                "ban_reason": ""
            }
        })

    # Load settings from logs
    logs = db.logs.find_one({})
    force_sub_channel_ids = logs["force_sub_channel_ids"]
    approved_channel_ids = logs["approved_channel_ids"]
    protect_content = logs["protect_content"]
    auto_delete_time = logs["auto_delete_time"]
    caption_template = logs["caption_template"]

    if args:
        if not force_sub_channel_ids:
            await update.message.reply_text(
                "<b>⚠️ No Force-Subscribe Channels</b>\n<i>Please contact the bot owner to set up force-subscribe channels.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            return

        non_subscribed_channels = []
        try:
            for channel_id in force_sub_channel_ids:
                member = await context.bot.get_chat_member(channel_id, user.id)
                if member.status not in ['member', 'administrator', 'creator']:
                    non_subscribed_channels.append(channel_id)
        except TelegramError as e:
            await update.message.reply_text(
                f"<b>❌ Error</b>\n<i>Failed to check subscription: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
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
                        await update.message.reply_text(
                            f"<b>⚠️ Warning</b>\n<i>Error generating invite link for channel <code>{channel_id}</code>: <code>{e}</code>. Using fallback link.</i>",
                            parse_mode="HTML",
                            protect_content=protect_content
                        )
                        FORCE_SUB_INVITE_LINKS[channel_id] = invite_link

                channel_buttons.append(InlineKeyboardButton(f"Join Channel {i}", url=invite_link))

            if len(channel_buttons) == 2:
                keyboard.append(channel_buttons)
            elif len(channel_buttons) >= 3:
                keyboard.append(channel_buttons[:2])
                for button in channel_buttons[2:]:
                    keyboard.append([button])
            else:
                keyboard.append(channel_buttons)

            keyboard.append([InlineKeyboardButton("Try Again", url=batch_link)])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "<b>📢 Join Required Channels</b>\n<i>First, you must join the following channel(s) to proceed:</i>",
                reply_markup=reply_markup,
                parse_mode="HTML",
                protect_content=protect_content
            )
            return

        if args[0].startswith("batch_"):
            batch_id = args[0].replace("batch_", "")
            if batch_id in batch_storage:
                batch = batch_storage[batch_id]
                from_msg = batch["from_msg"]
                to_msg = batch["to_msg"]
                channel_id = batch["channel_id"]
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
                        new_caption = caption_template.format(caption=original_caption) if caption_template else f"{original_caption}\nHACKHEIST"

                        sent_message = await context.bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=channel_id,
                            message_id=msg_id,
                            caption=new_caption if temp_message.document or temp_message.video or temp_message.photo else None,
                            parse_mode="HTML" if (temp_message.document or temp_message.video or temp_message.photo) else None,
                            protect_content=protect_content
                        )
                        forwarded_message_ids.append(sent_message.message_id)
                        await context.bot.delete_message(chat_id=OWNER_ID, message_id=temp_message.message_id)
                        forwarded.append(msg_id)
                        await asyncio.sleep(2)
                    except TelegramError as e:
                        if "message to copy not found" in str(e).lower() or "message to forward not found" in str(e).lower():
                            skipped.append(msg_id)
                        else:
                            skipped.append(msg_id)
                            await update.message.reply_text(
                                f"<b>❌ Error</b>\n<i>Failed to forward message <code>{msg_id}</code>: <code>{e}</code></i>",
                                parse_mode="HTML",
                                protect_content=protect_content
                            )

                if forwarded:
                    notification = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"<b>⏰ Your above Msg_ids are deleting in {auto_delete_time} seconds</b>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    asyncio.create_task(schedule_deletion(context, chat_id, forwarded_message_ids, notification.message_id, auto_delete_time))

                feedback = []
                if skipped:
                    feedback.append(f"<b>⚠️ Skipped Messages (likely deleted):</b> <code>{', '.join(map(str, skipped))}</code>")
                if not forwarded and not skipped:
                    feedback.append("<i>No messages were forwarded.</i>")

                if feedback:
                    await update.message.reply_text(
                        "\n".join(feedback),
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                del batch_storage[batch_id]
            else:
                await update.message.reply_text(
                    "<b>❌ Invalid Link</b>\n<i>The batch link is invalid or has expired.</i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
        else:
            try:
                decoded_bytes = base64.b64decode(args[0])
                decoded_string = decoded_bytes.decode('utf-8')
                parts = decoded_string.split('-')
                if len(parts) != 3 or parts[0] != 'get':
                    raise ValueError("Invalid link format")

                num1 = int(parts[1])
                num2 = int(parts[2])
                channel_id = None
                abs_channel_id = None
                from_msg = None
                to_msg = None
                for cid in approved_channel_ids:
                    abs_cid = abs(int(cid))
                    from_msg_candidate = num1 // abs_cid
                    to_msg_candidate = num2 // abs_cid
                    if (num1 % abs_cid == 0 and num2 % abs_cid == 0 and
                            to_msg_candidate >= from_msg_candidate):
                        channel_id = cid
                        abs_channel_id = abs_cid
                        from_msg = from_msg_candidate
                        to_msg = to_msg_candidate
                        break

                if not channel_id:
                    raise ValueError("No matching approved channel ID found or invalid message IDs")

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
                        new_caption = caption_template.format(caption=original_caption) if caption_template else f"{original_caption}\nHACKHEIST"

                        sent_message = await context.bot.copy_message(
                            chat_id=chat_id,
                            from_chat_id=channel_id,
                            message_id=msg_id,
                            caption=new_caption if temp_message.document or temp_message.video or temp_message.photo else None,
                            parse_mode="HTML" if (temp_message.document or temp_message.video or temp_message.photo) else None,
                            protect_content=protect_content
                        )
                        forwarded_message_ids.append(sent_message.message_id)
                        await context.bot.delete_message(chat_id=OWNER_ID, message_id=temp_message.message_id)
                        forwarded.append(msg_id)
                        await asyncio.sleep(2)
                    except TelegramError as e:
                        if "message to copy not found" in str(e).lower() or "message to forward not found" in str(e).lower():
                            skipped.append(msg_id)
                        else:
                            skipped.append(msg_id)
                            await update.message.reply_text(
                                f"<b>❌ Error</b>\n<i>Failed to forward message <code>{msg_id}</code>: <code>{e}</code></i>",
                                parse_mode="HTML",
                                protect_content=protect_content
                            )

                if forwarded:
                    notification = await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"<b>⏰ Your above Msg_ids are deleting in {auto_delete_time} seconds</b>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    asyncio.create_task(schedule_deletion(context, chat_id, forwarded_message_ids, notification.message_id, auto_delete_time))

                feedback = []
                if skipped:
                    feedback.append(f"<b>⚠️ Skipped Messages (likely deleted):</b> <code>{', '.join(map(str, skipped))}</code>")
                if not forwarded and not skipped:
                    feedback.append("<i>No messages were forwarded.</i>")

                if feedback:
                    await update.message.reply_text(
                        "\n".join(feedback),
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
            except (base64.binascii.Error, ValueError, TelegramError) as e:
                await update.message.reply_text(
                    f"<b>❌ Invalid Link</b>\n<i>The link is invalid or corrupted: <code>{e}</code></i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
    else:
        await update.message.reply_text(
            "<b>👋 Welcome!</b>\n<i>Use <code>/batch</code> to forward a range of messages from approved channels.</i>",
            parse_mode="HTML",
            protect_content=protect_content
        )

async def set_force_sub_ids(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /set_force_sub_ids command (owner-only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>This command is restricted to the bot owner.</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    await update.message.reply_text(
        "<b>📝 Set Force-Subscribe Channels</b>\n<i>Please send a comma-separated list of channel IDs (e.g., <code>-1002498103615,-1001234567890</code>).</i>",
        parse_mode="HTML",
        protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
    )
    context.user_data["state"] = "awaiting_force_sub_ids"

async def set_channel_ids(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /set_channel_ids command (owner-only)."""
    user_id = update.effective_user.id
    if user_id != OWNER_ID:
        await update.message.reply_text(
            "<b>❌ Access Denied</b>\n<i>This command is restricted to the bot owner.</i>",
            parse_mode="HTML",
            protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
        )
        return

    await update.message.reply_text(
        "<b>📝 Set Approved Channels</b>\n<i>Please send a comma-separated list of channel IDs (e.g., <code>-10093556234,-10028495942</code>).</i>",
        parse_mode="HTML",
        protect_content=context.bot_data["db"].logs.find_one({})["protect_content"]
    )
    context.user_data["state"] = "awaiting_channel_ids"

async def batch(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /batch command to initiate multiple batch link creation."""
    db = context.bot_data["db"]
    approved_channel_ids = db.logs.find_one({})["approved_channel_ids"]
    protect_content = db.logs.find_one({})["protect_content"]

    if not approved_channel_ids:
        await update.message.reply_text(
            "<b>⚠️ No Approved Channels</b>\n<i>The owner must set approved channel IDs using <code>/set_channel_ids</code> first.</i>",
            parse_mode="HTML",
            protect_content=protect_content
        )
        return

    await update.message.reply_text(
        "<b>📋 Create Batch Links</b>\n<i>Send all batch pairs in one message, with each pair separated by a blank line.</i>\n"
        "<b>Example:</b>\n"
        "<code>https://t.me/c/2493255368/45525\nhttps://t.me/c/2493255368/45528</code>\n\n"
        "<code>https://t.me/c/2493255368/45252\nhttps://t.me/c/2493255368/45254</code>",
        parse_mode="HTML",
        protect_content=protect_content
    )
    context.user_data["batch_state"] = "awaiting_batch_links"

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle incoming messages to process settings or batch links."""
    user_data = context.user_data
    user_id = update.effective_user.id
    message = update.message
    text = message.text.strip() if message.text else ""
    db = context.bot_data["db"]
    protect_content = db.logs.find_one({})["protect_content"]

    if user_data.get("state") == "awaiting_auto_delete_time":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>This action is restricted to the bot owner.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        try:
            seconds = int(text)
            if seconds <= 0:
                raise ValueError("Time must be positive")
            if seconds > 86400:
                raise ValueError("Time must be less than 24 hours")
            db.logs.update_one({}, {"$set": {"auto_delete_time": seconds}})
            await message.reply_text(
                f"<b>✅ Auto-Deletion Time Set</b>\n<i>Messages will be deleted after {seconds} seconds for all users.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
        except ValueError as e:
            await message.reply_text(
                f"<b>❌ Invalid Time</b>\n<i>Please send a valid number of seconds (e.g., 600). Error: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
        user_data.clear()
        return

    if user_data.get("state") in ["awaiting_broadcast_message", "awaiting_broadcast_time"]:
        await handle_broadcast_message(update, context)
        return

    if user_data.get("state") == "awaiting_caption_template":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>This action is restricted to the bot owner.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        if not text or "{caption}" not in text:
            await message.reply_text(
                "<b>❌ Invalid Template</b>\n<i>The template must include {caption} as a placeholder (e.g., <code><b>{caption}\nHACKHEIST</b></code>).</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            return

        db.logs.update_one({}, {"$set": {"caption_template": text}})
        await message.reply_text(
            f"<b>✅ Caption Template Updated</b>\n<i>New template: <code>{text}</code></i>",
            parse_mode="HTML",
            protect_content=protect_content
        )
        user_data.clear()
        return

    if user_data.get("state") == "awaiting_force_sub_ids":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>This action is restricted to the bot owner.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        try:
            channel_ids = [id.strip() for id in text.split(",") if id.strip()]
            if not channel_ids:
                await message.reply_text(
                    "<b>⚠️ Invalid Input</b>\n<i>No force-subscribe channel IDs provided.</i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
                user_data.clear()
                return

            valid_ids = []
            for channel_id in channel_ids:
                if not channel_id.startswith("-100") or not channel_id[4:].isdigit():
                    await message.reply_text(
                        f"<b>❌ Invalid ID</b>\n<i>Channel ID <code>{channel_id}</code> must start with -100 followed by digits.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return
                valid_ids.append(channel_id)

            old_ids = set(db.logs.find_one({})["force_sub_channel_ids"])
            db.logs.update_one({}, {"$set": {"force_sub_channel_ids": valid_ids}})
            new_ids = set(valid_ids)

            for channel_id in old_ids - new_ids:
                FORCE_SUB_INVITE_LINKS.pop(channel_id, None)

            for channel_id in new_ids - old_ids:
                try:
                    invite_link = await context.bot.export_chat_invite_link(channel_id)
                    FORCE_SUB_INVITE_LINKS[channel_id] = invite_link
                except TelegramError as e:
                    channel_number = channel_id[4:]
                    invite_link = f"https://t.me/c/{channel_number}"
                    FORCE_SUB_INVITE_LINKS[channel_id] = invite_link
                    await message.reply_text(
                        f"<b>⚠️ Warning</b>\n<i>Error generating invite link for channel <code>{channel_id}</code>: <code>{e}</code>. Using fallback link.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )

            await message.reply_text(
                f"<b>✅ Force-Subscribe Channels Updated</b>\n<i>New IDs: <code>{', '.join(valid_ids)}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
        except Exception as e:
            await message.reply_text(
                f"<b>❌ Error</b>\n<i>Failed to process force-subscribe channel IDs: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()

    elif user_data.get("state") == "awaiting_channel_ids":
        if user_id != OWNER_ID:
            await message.reply_text(
                "<b>❌ Access Denied</b>\n<i>This action is restricted to the bot owner.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        try:
            channel_ids = [id.strip() for id in text.split(",") if id.strip()]
            if not channel_ids:
                await message.reply_text(
                    "<b>⚠️ Invalid Input</b>\n<i>No channel IDs provided.</i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
                user_data.clear()
                return

            valid_ids = []
            for channel_id in channel_ids:
                if not channel_id.startswith("-100") or not channel_id[4:].isdigit():
                    await message.reply_text(
                        f"<b>❌ Invalid ID</b>\n<i>Channel ID <code>{channel_id}</code> must start with -100 followed by digits.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return
                valid_ids.append(channel_id)

            db.logs.update_one({}, {"$set": {"approved_channel_ids": valid_ids}})
            await message.reply_text(
                f"<b>✅ Approved Channels Updated</b>\n<i>New IDs: <code>{', '.join(valid_ids)}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
        except Exception as e:
            await message.reply_text(
                f"<b>❌ Error</b>\n<i>Failed to process channel IDs: <code>{e}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()

    elif user_data.get("batch_state") == "awaiting_batch_links":
        approved_channel_ids = db.logs.find_one({})["approved_channel_ids"]
        pairs = [pair.strip().split("\n") for pair in text.split("\n\n") if pair.strip()]
        batch_pairs = []

        for i, pair in enumerate(pairs, 1):
            if len(pair) != 2:
                await message.reply_text(
                    f"<b>❌ Invalid Pair {i}</b>\n<i>Each pair must contain exactly two links.</i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
                user_data.clear()
                return

            first_link, second_link = pair
            try:
                if not first_link.startswith("https://t.me/c/"):
                    await message.reply_text(
                        f"<b>❌ Invalid Pair {i}</b>\n<i>First link must be a valid message link.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return
                parts = first_link.split("/")
                channel_id = f"-100{parts[4]}"
                first_msg_id = int(parts[5])
                if channel_id not in approved_channel_ids:
                    await message.reply_text(
                        f"<b>❌ Invalid Pair {i}</b>\n<i>First link is not from an approved channel.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return

                if not second_link.startswith("https://t.me/c/"):
                    await message.reply_text(
                        f"<b>❌ Invalid Pair {i}</b>\n<i>Second link must be a valid message link.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return
                parts = second_link.split("/")
                second_channel_id = f"-100{parts[4]}"
                second_msg_id = int(parts[5])
                if second_channel_id not in approved_channel_ids:
                    await message.reply_text(
                        f"<b>❌ Invalid Pair {i}</b>\n<i>Second link is not from an approved channel.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return
                if second_channel_id != channel_id:
                    await message.reply_text(
                        f"<b>❌ Invalid Pair {i}</b>\n<i>Both links must be from the same channel.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return
                if second_msg_id < first_msg_id:
                    await message.reply_text(
                        f"<b>❌ Invalid Pair {i}</b>\n<i>Second message ID must be >= first message ID.</i>",
                        parse_mode="HTML",
                        protect_content=protect_content
                    )
                    user_data.clear()
                    return

                batch_pairs.append({
                    "from_msg": first_msg_id,
                    "to_msg": second_msg_id,
                    "channel_id": channel_id
                })
            except (IndexError, ValueError):
                await message.reply_text(
                    f"<b>❌ Invalid Pair {i}</b>\n<i>Invalid link format.</i>",
                    parse_mode="HTML",
                    protect_content=protect_content
                )
                user_data.clear()
                return

        if not batch_pairs:
            await update.message.reply_text(
                "<b>⚠️ No Valid Pairs</b>\n<i>No valid batch pairs provided. Use <code>/batch</code> to try again.</i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
            user_data.clear()
            return

        bot_username = (await context.bot.get_me()).username
        keyboard = []
        for i, pair in enumerate(batch_pairs, 1):
            abs_channel_id = abs(int(pair["channel_id"]))
            num1 = pair["from_msg"] * abs_channel_id
            num2 = pair["to_msg"] * abs_channel_id
            get_string = f"get-{num1}-{num2}"
            encoded_string = base64.b64encode(get_string.encode('utf-8')).decode('utf-8')
            deep_link = f"https://t.me/{bot_username}?start={encoded_string}"
            keyboard.append([InlineKeyboardButton(f"Batch {i}", url=deep_link)])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "<b>✅ Batch Links Created</b>\n<i>Here are your batch links:</i>",
            reply_markup=reply_markup,
            parse_mode="HTML",
            protect_content=protect_content
        )
        user_data.clear()

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors."""
    protect_content = context.bot_data["db"].logs.find_one({})["protect_content"]
    if update and update.message:
        await update.message.reply_text(
            f"<b>❌ Bot Error</b>\n<i>An error occurred: <code>{context.error}</code></i>",
            parse_mode="HTML",
            protect_content=protect_content
        )
        try:
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=f"<b>⚠️ Bot Error</b>\n<i>Error in chat {update.effective_chat.id}: <code>{context.error}</code></i>",
                parse_mode="HTML",
                protect_content=protect_content
            )
        except TelegramError:
            pass

def main():
    """Run the bot."""
    db = init_db()
    application = Application.builder().token(BOT_TOKEN).build()
    application.bot_data["db"] = db

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("auto_delete_msg", auto_delete_msg))
    application.add_handler(CommandHandler("protect_content", protect_content))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("new_caption", new_caption))
    application.add_handler(CommandHandler("set_force_sub_ids", set_force_sub_ids))
    application.add_handler(CommandHandler("set_channel_ids", set_channel_ids))
    application.add_handler(CommandHandler("batch", batch))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)

    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__":
    main()
