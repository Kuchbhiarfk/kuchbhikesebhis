import json
import os
import asyncio
import aiohttp
import base64
import re
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
    ConversationHandler,
)
from telegram.error import TelegramError
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

# Setup logging
LOG_FILE = "/tmp/telegram_bot.log"
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()  # For Render's dashboard logs
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
MONGODB_URI = os.getenv("MONGODB_URI")
WEBHOOK_URL = os.getenv("WEBHOOK_URL")  # Optional: for webhook setup
PORT = int(os.getenv("PORT", 8443))  # Render assigns PORT

# MongoDB setup
try:
    client = MongoClient(MONGODB_URI)
    db = client["telegram_bot"]
    cards_collection = db["cards"]
    changes_collection = db["changes"]
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {str(e)}")
    raise

# Fixed FirstCards
FIRSTCARDS = ["BOOKS", "NOTES", "TESTS", "MODULES"]

# Initialize MongoDB with default FirstCards if they don't exist
def init_mongodb():
    try:
        cards_collection.create_index("text", unique=True)
        logger.info("Created unique index on 'text' field in cards collection")
    except Exception as e:
        logger.warning(f"Failed to create unique index: {str(e)}")

    existing_cards = {doc["text"] for doc in cards_collection.find({}, {"_id": 0, "text": 1})}
    for fc in FIRSTCARDS:
        if fc not in existing_cards:
            cards_collection.insert_one({"text": fc, "secondcards": []})
            logger.info(f"Inserted FirstCard: {fc}")
    logger.info("MongoDB initialization completed")

init_mongodb()

# HTML content (placeholder, replace with your full HTML)
HTML_CONTENT = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>LONDETISHUN</title>
    <!-- Your HTML content here -->
</head>
<body>
    <div id="cards-container"></div>
    <script>
        const firstcardsData = {cards.json content};
        // Your JavaScript here
    </script>
</body>
</html>
"""

# HTML output path for Render
HTML_OUTPUT = "/tmp/output.html"

# Conversation states
(CHOOSE_ACTION, CHOOSE_FIRSTCARD, INPUT_SECOND, INPUT_SUBCARD, HANDLE_CONFIRMATION,
 CHOOSE_NEXT_ACTION, CHOOSE_REMOVE, CHOOSE_SECONDCARD, CHOOSE_SUBCARD_ACTION,
 CHOOSE_SUBCARD) = range(10)

# Helper functions
def find_firstcard(firstcard_name):
    return cards_collection.find_one({"text": firstcard_name}, {"_id": 0})

def find_secondcard(firstcard, secondcard_name):
    for sc in firstcard.get("secondcards", []):
        if sc["text"].lower() == secondcard_name.lower():
            return sc
    return None

def move_secondcard_to_top(firstcard, secondcard):
    secondcards = [sc for sc in firstcard["secondcards"] if sc["text"].lower() != secondcard["text"].lower()]
    secondcards.insert(0, secondcard)
    cards_collection.update_one(
        {"text": firstcard["text"]},
        {"$set": {"secondcards": secondcards}}
    )

def encode_urls(channel_id, f_msg_id, s_msg_id):
    channel_id = int(f"-100{channel_id}")
    channel_id_encoded = channel_id * 8
    f_msg_id_encoded = int(f_msg_id) * 8
    s_msg_id_encoded = int(s_msg_id) * 8
    raw_string = f"get-{channel_id_encoded}-{f_msg_id_encoded}-{s_msg_id_encoded}"
    encoded = base64.b64encode(raw_string.encode()).decode().rstrip("=")
    return encoded

def parse_urls(text):
    # Split input into Subcard blocks (separated by empty lines or multiple newlines)
    blocks = [block.strip() for block in re.split(r'\n\s*\n', text) if block.strip()]
    subcards = []
    
    for block in blocks:
        lines = block.split('\n')
        if len(lines) < 3:
            return None, "Invalid format: Each Subcard must have a name and two URLs."
        
        sub_name = lines[0].strip()
        urls = lines[1:]
        
        # Match URLs in the block
        pattern = r"(\d+)\s+(https://t\.me/c/(\d+)/(\d+))"
        matches = re.findall(pattern, '\n'.join(urls))
        
        if len(matches) != 2 or matches[0][2] != matches[1][2]:
            return None, "Invalid URLs or mismatched channel IDs in Subcard: " + sub_name
        
        channel_id, f_msg_id = matches[0][2], matches[0][3]
        s_msg_id = matches[1][3]
        encoded_url = encode_urls(channel_id, f_msg_id, s_msg_id)
        subcards.append({"text": sub_name, "url": encoded_url})
    
    if not subcards:
        return None, "No valid Subcards found in input."
    
    return subcards, None

def compare_json(old_data, new_data):
    changes = []
    if old_data is None:
        return ["Initial data created."]
    for old_fc, new_fc in zip(old_data, new_data):
        if old_fc["text"] != new_fc["text"]:
            changes.append(f"Changed FirstCard: {old_fc['text']} to {new_fc['text']}")
        for old_sc, new_sc in zip(old_fc.get("secondcards", []), new_fc.get("secondcards", [])):
            if old_sc["text"] != new_sc["text"]:
                changes.append(f"Changed SecondCard under {new_fc['text']}: {old_sc['text']} to {new_sc['text']}")
    return changes

def save_json():
    try:
        data = list(cards_collection.find({}, {"_id": 0}))
        previous_data = changes_collection.find_one({"type": "latest_data"}, {"_id": 0})
        change_log = compare_json(previous_data.get("data") if previous_data else None, data)
        if change_log:
            changes_collection.insert_one({
                "type": "change_log",
                "changes": change_log,
                "timestamp": datetime.utcnow()
            })
        changes_collection.update_one(
            {"type": "latest_data"},
            {"$set": {"data": data}},
            upsert=True
        )
        json_data = json.dumps(data, indent=4)
        html_content = HTML_CONTENT.replace("{cards.json content}", json_data)
        with open(HTML_OUTPUT, "w") as f:
            f.write(html_content)
        logger.info("MongoDB and HTML files saved successfully")
        return change_log
    except Exception as e:
        logger.error(f"Error saving MongoDB/HTML: {str(e)}")
        raise

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Add Entry", callback_data="add"),
         InlineKeyboardButton("Remove Entry", callback_data="remove")],
        [InlineKeyboardButton("Upload JSON", callback_data="upload"),
         InlineKeyboardButton("Download JSON", callback_data="download")],
        [InlineKeyboardButton("Download HTML", callback_data="download_html"),
         InlineKeyboardButton("Update Web", callback_data="update_web")],
        [InlineKeyboardButton("List Data", callback_data="list")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    message = await update.message.reply_text(
        "Welcome! Choose an action:", reply_markup=reply_markup
    )
    context.user_data["last_message_id"] = message.message_id
    logger.info(f"Started conversation, message_id: {message.message_id}")
    return CHOOSE_ACTION

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    logger.info(f"Button clicked: {data}")

    context.user_data.pop("callback_data", None)
    context.user_data["callback_data"] = data

    try:
        if data == "add":
            return await add(update, context)
        elif data == "remove":
            return await remove(update, context)
        elif data == "upload":
            return await upload(update, context)
        elif data == "download":
            return await download(update, context)
        elif data == "download_html":
            return await download_html(update, context)
        elif data == "update_web":
            return await update_web(update, context)
        elif data == "list":
            return await list_data(update, context)
        else:
            await query.message.edit_text("Invalid action ❌.", reply_markup=query.message.reply_markup)
            return CHOOSE_ACTION
    except Exception as e:
        logger.error(f"Error in button handler: {str(e)}")
        raise

async def add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["action"] = "add"
    keyboard = [
        [InlineKeyboardButton(fc, callback_data=f"fc_{i}_{update.callback_query.id if update.callback_query else 'cmd'}")]
        for i, fc in enumerate(FIRSTCARDS)
    ]
    keyboard.append([InlineKeyboardButton("Back to Menu", callback_data=f"back_to_start_{update.callback_query.id if update.callback_query else 'cmd'}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.message.edit_text(
            "Choose FirstCard:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = update.callback_query.message.message_id
    else:
        message = await update.message.reply_text(
            "Choose FirstCard:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message.message_id
    logger.info(f"Add action initiated, message_id: {context.user_data['last_message_id']}")
    return CHOOSE_FIRSTCARD

async def choose_firstcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    logger.info(f"FirstCard choice: {choice}")

    if choice.startswith("back_to_start"):
        keyboard = [
            [InlineKeyboardButton("Add Entry", callback_data="add"),
             InlineKeyboardButton("Remove Entry", callback_data="remove")],
            [InlineKeyboardButton("Upload JSON", callback_data="upload"),
             InlineKeyboardButton("Download JSON", callback_data="download")],
            [InlineKeyboardButton("Download HTML", callback_data="download_html"),
             InlineKeyboardButton("Update Web", callback_data="update_web")],
            [InlineKeyboardButton("List Data", callback_data="list")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "Welcome! Choose an action:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_ACTION

    try:
        index = int(choice.split("_")[1])
        firstcard_name = FIRSTCARDS[index]
        context.user_data["firstcard_name"] = firstcard_name

        prompt = {
            "BOOKS": "Write Author Name:",
            "NOTES": "Write Coaching Name of Notes:",
            "TESTS": "Write Coaching Name:",
            "MODULES": "Write Coaching Name:"
        }.get(firstcard_name, "Enter Second Card Name:")
        await query.message.edit_text(prompt)
        context.user_data["last_message_id"] = query.message.message_id
        return INPUT_SECOND
    except (IndexError, ValueError) as e:
        logger.error(f"Error in choose_firstcard: {str(e)}")
        await query.message.edit_text("Invalid choice ❌. Try again.", reply_markup=query.message.reply_markup)
        return CHOOSE_FIRSTCARD

async def input_second(update: Update, context: ContextTypes.DEFAULT_TYPE):
    second_name = update.message.text.strip()
    if not second_name:
        await update.message.reply_text("⚠️ Empty name, please enter a valid name.")
        return INPUT_SECOND

    firstcard = find_firstcard(context.user_data["firstcard_name"])
    existing_secondcard = find_secondcard(firstcard, second_name)

    if existing_secondcard:
        keyboard = [
            [InlineKeyboardButton("Yes", callback_data=f"confirm_y_{update.message.message_id}"),
             InlineKeyboardButton("No", callback_data=f"confirm_n_{update.message.message_id}")],
            [InlineKeyboardButton("Back to FirstCard", callback_data=f"back_to_fc_{update.message.message_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await update.message.reply_text(
            f"⚠️ '{second_name}' already exists under {context.user_data['firstcard_name']}.\n"
            "Continue anyway?", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message.message_id
        context.user_data["second_name"] = second_name
        logger.info(f"SecondCard exists: {second_name}, message_id: {message.message_id}")
        return HANDLE_CONFIRMATION
    else:
        context.user_data["secondcard"] = {"text": second_name, "subcards": []}
        cards_collection.update_one(
            {"text": context.user_data["firstcard_name"]},
            {"$push": {"secondcards": {"$each": [context.user_data["secondcard"]], "$position": 0}}}
        )
        return await prompt_subcard(update, context)

async def handle_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    logger.info(f"Confirmation choice: {choice}")

    if choice.startswith("back_to_fc"):
        keyboard = [
            [InlineKeyboardButton(fc, callback_data=f"fc_{i}_{query.id}")]
            for i, fc in enumerate(FIRSTCARDS)
        ]
        keyboard.append([InlineKeyboardButton("Back to Menu", callback_data=f"back_to_start_{query.id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "Choose FirstCard:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_FIRSTCARD

    if choice.startswith("confirm_y"):
        firstcard = find_firstcard(context.user_data["firstcard_name"])
        secondcard = find_secondcard(firstcard, context.user_data["second_name"])
        move_secondcard_to_top(firstcard, secondcard)
        context.user_data["secondcard"] = secondcard
        return await prompt_subcard(update, context)
    else:
        await query.message.edit_text("Enter a new Second Card Name:")
        context.user_data["last_message_id"] = query.message.message_id
        return INPUT_SECOND

async def prompt_subcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    firstcard_name = context.user_data["firstcard_name"]
    prompt = {
        "BOOKS": "Write Subject of Book:",
        "NOTES": "Tell Me Subject:",
        "TESTS": "Write Class:",
        "MODULES": "Write Class:"
    }.get(firstcard_name, "Enter Subcard Name:")
    message_text = (
        f"{prompt}\nThen provide two URLs in this format:\n"
        "1 https://t.me/c/<channel_id>/<f_msg_id>\n"
        "2 https://t.me/c/<channel_id>/<s_msg_id>\n"
        "Ensure the channel_id is the same for both URLs.\n\n"
        "To add multiple Subcards, separate each Subcard with a blank line, e.g.:\n"
        "Subcard1 Name\n"
        "1 https://t.me/c/<channel_id>/<f_msg_id>\n"
        "2 https://t.me/c/<channel_id>/<s_msg_id>\n\n"
        "Subcard2 Name\n"
        "1 https://t.me/c/<channel_id>/<f_msg_id>\n"
        "2 https://t.me/c/<channel_id>/<s_msg_id>"
    )
    keyboard = [[InlineKeyboardButton("Back to FirstCard", callback_data=f"back_to_fc_{update.callback_query.id if update.callback_query else update.message.message_id}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.message.edit_text(
            message_text, reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = update.callback_query.message.message_id
    else:
        message = await update.message.reply_text(
            message_text, reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message.message_id
    logger.info(f"Prompting Subcard input, message_id: {context.user_data['last_message_id']}")
    return INPUT_SUBCARD

async def input_subcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if not text:
        await update.message.reply_text("⚠️ Empty input, please provide at least one Subcard name and URLs.")
        return INPUT_SUBCARD

    subcards, error = parse_urls(text)
    if error:
        keyboard = [[InlineKeyboardButton("Back to FirstCard", callback_data=f"back_to_fc_{update.message.message_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        message = await update.message.reply_text(
            f"⚠️ {error}\nPlease provide each Subcard in the format:\n"
            "Subcard Name\n"
            "1 https://t.me/c/<channel_id>/<f_msg_id>\n"
            "2 https://t.me/c/<channel_id>/<s_msg_id>\n"
            "Separate multiple Subcards with a blank line.", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message.message_id
        return INPUT_SUBCARD

    try:
        for subcard in subcards:
            cards_collection.update_one(
                {"text": context.user_data["firstcard_name"], "secondcards.text": context.user_data["secondcard"]["text"]},
                {"$push": {"secondcards.$.subcards": subcard}}
            )
        change_log = save_json()
    except Exception as e:
        logger.error(f"Error saving Subcards: {str(e)}")
        await update.message.reply_text(f"⚠️ Error saving data: {str(e)}")
        return INPUT_SUBCARD

    keyboard = [
        [InlineKeyboardButton("Add More Subcards", callback_data=f"n_{update.message.message_id}"),
         InlineKeyboardButton("New Second Card", callback_data=f"y_{update.message.message_id}")],
        [InlineKeyboardButton("Done", callback_data=f"d_{update.message.message_id}"),
         InlineKeyboardButton("Back to FirstCard", callback_data=f"back_to_fc_{update.message.message_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    subcard_names = ", ".join(sub["text"] for sub in subcards)
    message = await update.message.reply_text(
        f"✅ Subcard(s) '{subcard_names}' added with encoded URL(s)!", reply_markup=reply_markup
    )
    context.user_data["last_message_id"] = message.message_id
    logger.info(f"Subcards added: {subcard_names}, message_id: {message.message_id}")
    return CHOOSE_NEXT_ACTION

async def choose_next_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data
    logger.info(f"Next action: {action}")

    if action.startswith("back_to_fc"):
        keyboard = [
            [InlineKeyboardButton(fc, callback_data=f"fc_{i}_{query.id}")]
            for i, fc in enumerate(FIRSTCARDS)
        ]
        keyboard.append([InlineKeyboardButton("Back to Menu", callback_data=f"back_to_start_{query.id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "Choose FirstCard:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_FIRSTCARD

    action = action.split("_")[0]
    if action == "n":
        return await prompt_subcard(update, context)
    elif action == "y":
        return await add(update, context)
    elif action == "d":
        keyboard = [
            [InlineKeyboardButton("Add Entry", callback_data="add"),
             InlineKeyboardButton("Remove Entry", callback_data="remove")],
            [InlineKeyboardButton("Upload JSON", callback_data="upload"),
             InlineKeyboardButton("Download JSON", callback_data="download")],
            [InlineKeyboardButton("Download HTML", callback_data="download_html"),
             InlineKeyboardButton("Update Web", callback_data="update_web")],
            [InlineKeyboardButton("List Data", callback_data="list")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "Done! Data and HTML updated. Choose an action:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_ACTION
    else:
        await query.message.edit_text("Invalid action ❌.", reply_markup=query.message.reply_markup)
        return CHOOSE_NEXT_ACTION

async def remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["action"] = "remove"
    keyboard = [
        [InlineKeyboardButton(fc, callback_data=f"fc_{i}_{update.callback_query.id if update.callback_query else 'cmd'}")]
        for i, fc in enumerate(FIRSTCARDS)
    ]
    keyboard.append([InlineKeyboardButton("Back to Menu", callback_data=f"back_to_start_{update.callback_query.id if update.callback_query else 'cmd'}")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.message.edit_text(
            "Choose FirstCard to remove from:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = update.callback_query.message.message_id
    else:
        message = await update.message.reply_text(
            "Choose FirstCard to remove from:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message.message_id
    logger.info(f"Remove action initiated, message_id: {context.user_data['last_message_id']}")
    return CHOOSE_REMOVE

async def choose_remove(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    logger.info(f"Remove choice: {choice}")

    if choice.startswith("back_to_start"):
        return await start(update, context)

    try:
        index = int(choice.split("_")[1])
        firstcard_name = FIRSTCARDS[index]
        firstcard = find_firstcard(firstcard_name)
        context.user_data["firstcard"] = firstcard
        context.user_data["firstcard_name"] = firstcard_name

        if not firstcard["secondcards"]:
            keyboard = [
                [InlineKeyboardButton("Add Entry", callback_data="add"),
                 InlineKeyboardButton("Remove Entry", callback_data="remove")],
                [InlineKeyboardButton("Upload JSON", callback_data="upload"),
                 InlineKeyboardButton("Download JSON", callback_data="download")],
                [InlineKeyboardButton("Download HTML", callback_data="download_html"),
                 InlineKeyboardButton("Update Web", callback_data="update_web")],
                [InlineKeyboardButton("List Data", callback_data="list")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                f"⚠️ No data found under {firstcard_name}. Returning to menu.", reply_markup=reply_markup
            )
            context.user_data["last_message_id"] = query.message.message_id
            return CHOOSE_ACTION

        keyboard = [
            [InlineKeyboardButton(sc["text"], callback_data=f"sc_{i}_{query.id}")]
            for i, sc in enumerate(firstcard["secondcards"])
        ]
        keyboard.append([InlineKeyboardButton("Back to Menu", callback_data=f"back_to_start_{query.id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"SecondCards under {firstcard_name}:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_SECONDCARD
    except (IndexError, ValueError) as e:
        logger.error(f"Error in choose_remove: {str(e)}")
        await query.message.edit_text("Invalid choice ❌. Try again.", reply_markup=query.message.reply_markup)
        return CHOOSE_REMOVE

async def choose_secondcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    logger.info(f"SecondCard choice: {choice}")

    if choice.startswith("back_to_start"):
        return await start(update, context)

    try:
        sc_index = int(choice.split("_")[1])
        firstcard = context.user_data["firstcard"]
        secondcard = firstcard["secondcards"][sc_index]
        context.user_data["sc_index"] = sc_index
        context.user_data["secondcard"] = secondcard
        keyboard = [
            [InlineKeyboardButton("Remove Entire SecondCard", callback_data=f"full_{query.id}"),
             InlineKeyboardButton("Remove Specific Subcard", callback_data=f"sub_{query.id}")],
            [InlineKeyboardButton("Back to FirstCard", callback_data=f"back_to_remove_{query.id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"Choose action for '{secondcard['text']}':", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_SUBCARD_ACTION
    except (IndexError, ValueError) as e:
        logger.error(f"Error in choose_secondcard: {str(e)}")
        await query.message.edit_text("Invalid choice ❌. Try again.", reply_markup=query.message.reply_markup)
        return CHOOSE_SECONDCARD

async def choose_subcard_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data
    logger.info(f"Subcard action: {action}")

    if action.startswith("back_to_remove"):
        keyboard = [
            [InlineKeyboardButton(fc, callback_data=f"fc_{i}_{query.id}")]
            for i, fc in enumerate(FIRSTCARDS)
        ]
        keyboard.append([InlineKeyboardButton("Back to Menu", callback_data=f"back_to_start_{query.id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "Choose FirstCard to remove from:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_REMOVE

    firstcard = context.user_data["firstcard"]
    secondcard = context.user_data["secondcard"]
    sc_index = context.user_data["sc_index"]
    action = action.split("_")[0]

    if action == "full":
        cards_collection.update_one(
            {"text": firstcard["text"]},
            {"$pull": {"secondcards": {"text": secondcard["text"]}}}
        )
        try:
            save_json()
        except Exception as e:
            logger.error(f"Error removing SecondCard: {str(e)}")
            await query.message.edit_text(f"⚠️ Error saving data: {str(e)}", reply_markup=query.message.reply_markup)
            return CHOOSE_SUBCARD_ACTION
        keyboard = [
            [InlineKeyboardButton("Add Entry", callback_data="add"),
             InlineKeyboardButton("Remove Entry", callback_data="remove")],
            [InlineKeyboardButton("Upload JSON", callback_data="upload"),
             InlineKeyboardButton("Download JSON", callback_data="download")],
            [InlineKeyboardButton("Download HTML", callback_data="download_html"),
             InlineKeyboardButton("Update Web", callback_data="update_web")],
            [InlineKeyboardButton("List Data", callback_data="list")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"✅ Removed whole SecondCard '{secondcard['text']}'. Choose an action:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_ACTION
    elif action == "sub":
        if not secondcard["subcards"]:
            keyboard = [
                [InlineKeyboardButton("Add Entry", callback_data="add"),
                 InlineKeyboardButton("Remove Entry", callback_data="remove")],
                [InlineKeyboardButton("Upload JSON", callback_data="upload"),
                 InlineKeyboardButton("Download JSON", callback_data="download")],
                [InlineKeyboardButton("Download HTML", callback_data="download_html"),
                 InlineKeyboardButton("Update Web", callback_data="update_web")],
                [InlineKeyboardButton("List Data", callback_data="list")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                "⚠️ No subcards to remove. Choose an action:", reply_markup=reply_markup
            )
            context.user_data["last_message_id"] = query.message.message_id
            return CHOOSE_ACTION
        keyboard = [
            [InlineKeyboardButton(sub["text"], callback_data=f"sub_{i}_{query.id}")]
            for i, sub in enumerate(secondcard["subcards"])
        ]
        keyboard.append([InlineKeyboardButton("Back to SecondCard", callback_data=f"back_to_sc_{query.id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            "Choose Subcard to remove:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_SUBCARD
    else:
        await query.message.edit_text("Invalid action ❌.", reply_markup=query.message.reply_markup)
        return CHOOSE_SUBCARD_ACTION

async def choose_subcard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    choice = query.data
    logger.info(f"Subcard choice: {choice}")

    if choice.startswith("back_to_sc"):
        secondcard = context.user_data["secondcard"]
        keyboard = [
            [InlineKeyboardButton("Remove Entire SecondCard", callback_data=f"full_{query.id}"),
             InlineKeyboardButton("Remove Specific Subcard", callback_data=f"sub_{query.id}")],
            [InlineKeyboardButton("Back to FirstCard", callback_data=f"back_to_remove_{query.id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"Choose action for '{secondcard['text']}':", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_SUBCARD_ACTION

    try:
        sub_index = int(choice.split("_")[1])
        secondcard = context.user_data["secondcard"]
        removed = secondcard["subcards"].pop(sub_index)
        cards_collection.update_one(
            {"text": context.user_data["firstcard_name"], "secondcards.text": secondcard["text"]},
            {"$set": {"secondcards.$.subcards": secondcard["subcards"]}}
        )
        try:
            save_json()
        except Exception as e:
            logger.error(f"Error removing Subcard: {str(e)}")
            await query.message.edit_text(f"⚠️ Error saving data: {str(e)}", reply_markup=query.message.reply_markup)
            return CHOOSE_SUBCARD
        keyboard = [
            [InlineKeyboardButton("Add Entry", callback_data="add"),
             InlineKeyboardButton("Remove Entry", callback_data="remove")],
            [InlineKeyboardButton("Upload JSON", callback_data="upload"),
             InlineKeyboardButton("Download JSON", callback_data="download")],
            [InlineKeyboardButton("Download HTML", callback_data="download_html"),
             InlineKeyboardButton("Update Web", callback_data="update_web")],
            [InlineKeyboardButton("List Data", callback_data="list")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.message.edit_text(
            f"✅ Removed Subcard '{removed['text']}'. Choose an action:", reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = query.message.message_id
        return CHOOSE_ACTION
    except (IndexError, ValueError) as e:
        logger.error(f"Error in choose_subcard: {str(e)}")
        await query.message.edit_text("Invalid choice ❌. Try again.", reply_markup=query.message.reply_markup)
        return CHOOSE_SUBCARD

async def list_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = list(cards_collection.find({}, {"_id": 0}))
    if not data:
        message = "⚠️ No data found. Add entries using /add."
        if update.callback_query:
            await update.callback_query.message.edit_text(message)
            context.user_data["last_message_id"] = update.callback_query.message.message_id
        else:
            message_obj = await update.message.reply_text(message)
            context.user_data["last_message_id"] = message_obj.message_id
        logger.info("No data found for /list")
        return CHOOSE_ACTION

    message = "📋 Current Data:\n"
    for fc in data:
        message += f"\n📌 {fc['text']}:\n"
        for sc in fc.get("secondcards", []):
            message += f"  ├─ {sc['text']}\n"
            for sub in sc.get("subcards", []):
                message += f"  │  └─ {sub['text']} (encoded URL)\n"
        if not fc.get("secondcards"):
            message += "  └─ No SecondCards\n"

    keyboard = [
        [InlineKeyboardButton("Add Entry", callback_data="add"),
         InlineKeyboardButton("Remove Entry", callback_data="remove")],
        [InlineKeyboardButton("Upload JSON", callback_data="upload"),
         InlineKeyboardButton("Download JSON", callback_data="download")],
        [InlineKeyboardButton("Download HTML", callback_data="download_html"),
         InlineKeyboardButton("Update Web", callback_data="update_web")],
        [InlineKeyboardButton("List Data", callback_data="list")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    if update.callback_query:
        await update.callback_query.message.edit_text(
            message, reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = update.callback_query.message.message_id
    else:
        message_obj = await update.message.reply_text(
            message, reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message_obj.message_id
    logger.info(f"Listed data, message_id: {context.user_data['last_message_id']}")
    return CHOOSE_ACTION

async def upload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = list(cards_collection.find({}, {"_id": 0}))
        if not data:
            if update.callback_query:
                await update.callback_query.message.reply_text("⚠️ No data found in MongoDB.")
            else:
                await update.message.reply_text("⚠️ No data found in MongoDB.")
            logger.warning("Upload failed: No data in MongoDB")
            return CHOOSE_ACTION
        json_data = json.dumps(data, indent=4)
        with open("/tmp/cards.json", "w") as f:
            f.write(json_data)
        with open("/tmp/cards.json", "rb") as f:
            if update.callback_query:
                await update.callback_query.message.reply_document(document=f, filename="cards.json")
                await update.callback_query.message.reply_text("✅ cards.json uploaded!")
            else:
                await update.message.reply_document(document=f, filename="cards.json")
                await update.message.reply_text("✅ cards.json uploaded!")
        os.remove("/tmp/cards.json")
        logger.info("JSON uploaded successfully")
    except Exception as e:
        logger.error(f"Error in upload: {str(e)}")
        if update.callback_query:
            await update.callback_query.message.reply_text(f"⚠️ Error uploading JSON: {str(e)}")
        else:
            await update.message.reply_text(f"⚠️ Error uploading JSON: {str(e)}")
    return CHOOSE_ACTION

async def download(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        data = list(cards_collection.find({}, {"_id": 0}))
        if not data:
            if update.callback_query:
                await update.callback_query.message.reply_text("⚠️ No data found in MongoDB.")
            else:
                await update.message.reply_text("⚠️ No data found in MongoDB.")
            logger.warning("Download failed: No data in MongoDB")
            return CHOOSE_ACTION
        json_data = json.dumps(data, indent=4)
        with open("/tmp/cards.json", "w") as f:
            f.write(json_data)
        with open("/tmp/cards.json", "rb") as f:
            if update.callback_query:
                await update.callback_query.message.reply_document(document=f, filename="cards.json")
                await update.callback_query.message.reply_text("✅ cards.json downloaded!")
            else:
                await update.message.reply_document(document=f, filename="cards.json")
                await update.message.reply_text("✅ cards.json downloaded!")
        os.remove("/tmp/cards.json")
        logger.info("JSON downloaded successfully")
    except Exception as e:
        logger.error(f"Error in download: {str(e)}")
        if update.callback_query:
            await update.callback_query.message.reply_text(f"⚠️ Error downloading JSON: {str(e)}")
        else:
            await update.message.reply_text(f"⚠️ Error downloading JSON: {str(e)}")
    return CHOOSE_ACTION

async def download_html(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not os.path.exists(HTML_OUTPUT):
            if update.callback_query:
                await update.callback_query.message.reply_text(f"⚠️ {HTML_OUTPUT} not found. Try adding data first.")
            else:
                await update.message.reply_text(f"⚠️ {HTML_OUTPUT} not found. Try adding data first.")
            logger.warning(f"Download HTML failed: {HTML_OUTPUT} not found")
            return CHOOSE_ACTION
        change_log = changes_collection.find_one({"type": "change_log"}, sort=[("timestamp", -1)], projection={"_id": 0})
        if change_log and change_log.get("changes"):
            change_message = "Changes since last update:\n" + "\n".join(f"- {change}" for change in change_log["changes"])
            if update.callback_query:
                await update.callback_query.message.reply_text(change_message)
            else:
                await update.message.reply_text(change_message)
        else:
            if update.callback_query:
                await update.callback_query.message.reply_text("No changes since last update.")
            else:
                await update.message.reply_text("No changes since last update.")
        with open(HTML_OUTPUT, "rb") as f:
            if update.callback_query:
                await update.callback_query.message.reply_document(document=f, filename="output.html")
                await update.callback_query.message.reply_text(
                    "Want to update this HTML in your GitHub repo? Use /update_web"
                )
            else:
                await update.message.reply_document(document=f, filename="output.html")
                await update.message.reply_text(
                    "Want to update this HTML in your GitHub repo? Use /update_web"
                )
        logger.info("HTML downloaded successfully")
    except Exception as e:
        logger.error(f"Error in download_html: {str(e)}")
        if update.callback_query:
            await update.callback_query.message.reply_text(f"⚠️ Error downloading HTML: {str(e)}")
        else:
            await update.message.reply_text(f"⚠️ Error downloading HTML: {str(e)}")
    return CHOOSE_ACTION

async def update_web(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if not os.path.exists(HTML_OUTPUT):
            if update.callback_query:
                await update.callback_query.message.reply_text(f"⚠️ {HTML_OUTPUT} not found. Try adding data first.")
            else:
                await update.message.reply_text(f"⚠️ {HTML_OUTPUT} not found. Try adding data first.")
            logger.warning(f"Update web failed: {HTML_OUTPUT} not found")
            return CHOOSE_ACTION

        if not GITHUB_TOKEN:
            if update.callback_query:
                await update.callback_query.message.reply_text("⚠️ GitHub token not set. Please configure GITHUB_TOKEN in .env.")
            else:
                await update.message.reply_text("⚠️ GitHub token not set. Please configure GITHUB_TOKEN in .env.")
            logger.warning("Update web failed: GITHUB_TOKEN not set")
            return CHOOSE_ACTION

        REPO_OWNER = "yashyasag"
        REPO_NAME = "hiddens_officials"
        FILE_PATH = "output.html"  # Update to "docs/output.html" if needed
        branch = "main"  # Update to "gh-pages" if needed

        with open(HTML_OUTPUT, "r") as f:
            content = f.read()
        content_b64 = base64.b64encode(content.encode()).decode()

        url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/contents/{FILE_PATH}"
        headers = {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28"
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    file_data = await response.json()
                    sha = file_data["sha"]
                    payload = {
                        "message": "Update output.html via Telegram bot",
                        "content": content_b64,
                        "sha": sha,
                        "branch": branch
                    }
                elif response.status == 404:
                    payload = {
                        "message": "Initial creation of output.html via Telegram bot",
                        "content": content_b64,
                        "branch": branch
                    }
                else:
                    error_text = await response.text()
                    logger.error(f"GitHub GET failed: {response.status} - {error_text}")
                    if update.callback_query:
                        await update.callback_query.message.reply_text(f"⚠️ Failed to fetch file: {response.status} - {error_text}")
                    else:
                        await update.message.reply_text(f"⚠️ Failed to fetch file: {response.status} - {error_text}")
                    return CHOOSE_ACTION

            async with session.put(url, headers=headers, json=payload) as response:
                if response.status in (200, 201):
                    action = "Updated" if "sha" in payload else "Created"
                    if update.callback_query:
                        await update.callback_query.message.reply_text(f"✅ {action} {FILE_PATH} in GitHub repo {REPO_OWNER}/{REPO_NAME}")
                    else:
                        await update.message.reply_text(f"✅ {action} {FILE_PATH} in GitHub repo {REPO_OWNER}/{REPO_NAME}")
                    logger.info(f"GitHub {action.lower()}: {FILE_PATH}")
                else:
                    error_text = await response.text()
                    logger.error(f"GitHub PUT failed: {response.status} - {error_text}")
                    if update.callback_query:
                        await update.callback_query.message.reply_text(f"⚠️ Failed to update GitHub: {response.status} - {error_text}")
                    else:
                        await update.message.reply_text(f"⚠️ Failed to update GitHub: {response.status} - {error_text}")
    except Exception as e:
        logger.error(f"Error in update_web: {str(e)}")
        if update.callback_query:
            await update.callback_query.message.reply_text(f"⚠️ Error updating web: {str(e)}")
        else:
            await update.message.reply_text(f"⚠️ Error updating web: {str(e)}")
    return CHOOSE_ACTION

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("Add Entry", callback_data="add"),
         InlineKeyboardButton("Remove Entry", callback_data="remove")],
        [InlineKeyboardButton("Upload JSON", callback_data="upload"),
         InlineKeyboardButton("Download JSON", callback_data="download")],
        [InlineKeyboardButton("Download HTML", callback_data="download_html"),
         InlineKeyboardButton("Update Web", callback_data="update_web")],
        [InlineKeyboardButton("List Data", callback_data="list")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Operation cancelled. Choose an action:", reply_markup=reply_markup)
    context.user_data["last_message_id"] = update.message.message_id
    logger.info(f"Cancelled operation, message_id: {context.user_data['last_message_id']}")
    return CHOOSE_ACTION

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error: {context.error}")
    keyboard = [
        [InlineKeyboardButton("Add Entry", callback_data="add"),
         InlineKeyboardButton("Remove Entry", callback_data="remove")],
        [InlineKeyboardButton("Upload JSON", callback_data="upload"),
         InlineKeyboardButton("Download JSON", callback_data="download")],
        [InlineKeyboardButton("Download HTML", callback_data="download_html"),
         InlineKeyboardButton("Update Web", callback_data="update_web")],
        [InlineKeyboardButton("List Data", callback_data="list")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    error_message = f"⚠️ Error: {str(context.error)}"
    if update.callback_query:
        await update.callback_query.message.reply_text(
            error_message, reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = update.callback_query.message.message_id
    else:
        message = await update.message.reply_text(
            error_message, reply_markup=reply_markup
        )
        context.user_data["last_message_id"] = message.message_id
    return CHOOSE_ACTION

def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set in .env")
        raise ValueError("BOT_TOKEN not set in .env")
    if not MONGODB_URI:
        logger.error("MONGODB_URI not set in .env")
        raise ValueError("MONGODB_URI not set in .env")

    application = Application.builder().token(BOT_TOKEN).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("add", add),
            CommandHandler("remove", remove),
            CommandHandler("upload", upload),
            CommandHandler("download", download),
            CommandHandler("download_html", download_html),
            CommandHandler("update_web", update_web),
            CommandHandler("list", list_data),
        ],
        states={
            CHOOSE_ACTION: [
                CallbackQueryHandler(button),
                CommandHandler("start", start),
                CommandHandler("add", add),
                CommandHandler("remove", remove),
                CommandHandler("upload", upload),
                CommandHandler("download", download),
                CommandHandler("download_html", download_html),
                CommandHandler("update_web", update_web),
                CommandHandler("list", list_data),
            ],
            CHOOSE_FIRSTCARD: [CallbackQueryHandler(choose_firstcard)],
            INPUT_SECOND: [MessageHandler(filters.TEXT & ~filters.COMMAND, input_second)],
            HANDLE_CONFIRMATION: [CallbackQueryHandler(handle_confirmation)],
            INPUT_SUBCARD: [MessageHandler(filters.TEXT & ~filters.COMMAND, input_subcard)],
            CHOOSE_NEXT_ACTION: [CallbackQueryHandler(choose_next_action)],
            CHOOSE_REMOVE: [CallbackQueryHandler(choose_remove)],
            CHOOSE_SECONDCARD: [CallbackQueryHandler(choose_secondcard)],
            CHOOSE_SUBCARD_ACTION: [CallbackQueryHandler(choose_subcard_action)],
            CHOOSE_SUBCARD: [CallbackQueryHandler(choose_subcard)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        per_chat=True,
        allow_reentry=True
    )

    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        if WEBHOOK_URL:
            logger.info(f"Starting webhook on {WEBHOOK_URL}")
            application.run_webhook(
                listen="0.0.0.0",
                port=PORT,
                url_path="",
                webhook_url=WEBHOOK_URL
            )
        else:
            logger.info("Starting polling")
            loop.run_until_complete(application.run_polling())
    finally:
        loop.close()

if __name__ == "__main__":
    main()
