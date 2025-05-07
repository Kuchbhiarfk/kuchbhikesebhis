import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from io import BytesIO
import requests
import asyncio
from flask import Flask
from threading import Thread
from datetime import datetime, timedelta

# Telegram Bot Token
BOT_TOKEN = '7541631439:AAGv0y0Y2O4srSs-tpo77tTKzMSKR-IFU7U'

# Channel details (Make sure bot is an admin in these channels)
channels = {
    "𝗖𝗹𝗮𝘀𝘀 𝟭𝟭𝘁𝗵|𝗟𝗶𝘃𝗲 🔴": "-1002175574964",
    "𝗖𝗹𝗮𝘀𝘀 𝟭𝟮𝘁𝗵|𝗟𝗶𝘃𝗲 🔴": "-1002408234754",
    "𝗖𝗹𝗮𝘀𝘀 𝟭𝟯𝘁𝗵|𝗟𝗶𝘃𝗲 🔴": "-1002225506571"
}

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Flask app for webhook
app = Flask(__name__)

# Start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Image URL
    image_url = "https://i.ibb.co/D98tcdk/66f16ac7.jpg"  # Replace with your image URL
    caption = "*:..｡o○𝗪𝗲𝗹𝗰𝗼𝗺𝗲 𝗯𝘂𝗱𝗱𝘆 🥰○o｡..:*\n\n𝐓𝐡𝐚𝐧𝐤𝐬 𝐟𝐨𝐫 𝐜𝐨𝐦𝐢𝐧𝐠 𝐡𝐞𝐫𝐞 🙏🙏\n\n𝐍𝐨𝐰 𝐂𝐥𝐢𝐜𝐤 𝐨𝐧 𝐛𝐞𝐥𝐨𝐰 𝐛𝐮𝐭𝐭𝐨𝐧𝐬 𝐭𝐨 𝐣𝐨𝐢𝐧 𝐨𝐧 𝐜𝐡𝐚𝐧𝐧𝐞𝐥 𝐰𝐡𝐚𝐭 𝐲𝐨𝐮 𝐰𝐚𝐧𝐭 ✨✨\n\n𝐈𝐟 𝐜𝐨𝐩𝐲𝐫𝐢𝐠𝐡𝐭 𝐨𝐧 𝐭𝐡𝐢𝐬 𝐛𝐨𝐭 𝐬𝐚𝐯𝐞 𝐰𝐞𝐛𝐬𝐢𝐭𝐞 𝐟𝐨𝐫 𝐧𝐞𝐰 𝐛𝐨𝐭\n❀ https://yashyasag.github.io/botupdates\n❀ https://yashyasag.github.io/botupdates"

    # Prepare buttons with channels
    keyboard = [
        [InlineKeyboardButton(name, callback_data=channel)] for name, channel in channels.items()
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Fetch and send the image
    response = requests.get(image_url)
    image = BytesIO(response.content)

    await update.message.reply_photo(
        photo=InputFile(image),
        caption=caption,
        reply_markup=reply_markup
    )

# Handle button press and send temporary invite link
async def button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    # Generate a temporary link (expire in 10 seconds)
    try:
        expire_time = datetime.now() + timedelta(seconds=10)
        invite_link = await context.bot.create_chat_invite_link(
            chat_id=query.data, expire_date=expire_time, member_limit=None
        )

        await query.message.reply_text(f"𝐇𝐞𝐲 ,\n𝕃𝕚𝕟𝕜 𝕥𝕠 𝕛𝕠𝕚𝕟 𝕠𝕗 𝕪𝕠𝕦𝕣 𝕣𝕖𝕢𝕦𝕖𝕤𝕥 𝕔𝕙𝕒𝕟𝕟𝕖𝕝 👇👇\n\n{invite_link.invite_link}\n\n𝐍𝐎𝐓𝐄 »»𝗧𝗵𝗶𝘀 𝗹𝗶𝗻𝗸 𝗮𝘂𝘁𝗼𝗺𝗮𝘁𝗶𝗰 𝗥𝗲𝘃𝗼𝗸𝗲𝗱 𝗶𝗻 𝟭𝟬𝘀𝗲𝗰𝗼𝗻𝗱𝘀\n𝗦𝗼 𝗷𝗼𝗶𝗻 𝗳𝗮𝘀𝘁 𝗼𝗿 𝗿𝗲𝗾𝘂𝗲𝘀𝘁 𝗮𝗴𝗮𝗶𝗻 𝗳𝗼𝗿 𝗻𝗲𝘄 𝗹𝗶𝗻𝗸.")
        
        # Schedule revocation after 10 seconds
        asyncio.create_task(revoke_invite_link(context.bot, query.data, invite_link.invite_link))

    except Exception as e:
        await query.message.reply_text(f"Failed to generate link: {e}")

async def revoke_invite_link(bot, chat_id, invite_link):
    """Revoke the invite link after 10 seconds."""
    await asyncio.sleep(20)
    try:
        await bot.revoke_chat_invite_link(chat_id=chat_id, invite_link=invite_link)
        logger.info(f"Invite link {invite_link} has been revoked.")
    except Exception as e:
        logger.error(f"Failed to revoke link: {e}")

# Error handler
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.warning(f"Update {update} caused error {context.error}")

def run_flask():
    app.run(host="0.0.0.0", port=8080)

def run_telegram_bot():
    application = ApplicationBuilder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button))
    application.add_error_handler(error_handler)
    application.run_polling()

if __name__ == "__main__":
    # Start both Flask and the Telegram bot
    Thread(target=run_flask).start()
    run_telegram_bot()
