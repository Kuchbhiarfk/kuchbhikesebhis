import pymongo
from pymongo import MongoClient
import requests
import json
import os
import asyncio
import re
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# MongoDB setup
MONGO_URI = "mongodb+srv://elvishyadavop:ClA5yIHTbCutEnVP@cluster0.u83zlfx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"  # Replace with your MongoDB Atlas URI
client = MongoClient(MONGO_URI)
db = client["unacademy_db"]
educators_collection = db["educators"]
educators_collection.create_index("uid", unique=True)

# Global variables
fetching = False
update_context = None
update_obj = None
progress_message = None
last_educator_count = 0

def save_to_json(filename, data):
    """Save data to a JSON file temporarily."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def split_json_file(filename, max_size_mb=50):
    """Split a JSON file into parts if it exceeds max_size_mb."""
    max_size_bytes = max_size_mb * 1024 * 1024
    if not os.path.exists(filename):
        return [filename]
    
    file_size = os.path.getsize(filename)
    if file_size <= max_size_bytes:
        return [filename]
    
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if not isinstance(data, list):
        return [filename]
    
    part_files = []
    part_data = []
    part_index = 1
    current_size = 0
    item_size_estimate = file_size / len(data) if data else 1
    
    for item in data:
        item_size = len(json.dumps(item, ensure_ascii=False).encode('utf-8'))
        if current_size + item_size > max_size_bytes and part_data:
            part_filename = f"educators_part_{part_index}.json"
            save_to_json(part_filename, part_data)
            part_files.append(part_filename)
            part_data = []
            current_size = 0
            part_index += 1
        part_data.append(item)
        current_size += item_size
    
    if part_data:
        part_filename = f"educators_part_{part_index}.json"
        save_to_json(part_filename, part_data)
        part_files.append(part_filename)
    
    if os.path.exists(filename):
        os.remove(filename)
        print(f"Deleted original {filename} after splitting")
    
    return part_files

def save_educators_to_mongodb(educators):
    """Save unique educators to MongoDB."""
    for educator in educators:
        uid = educator.get("uid", "")
        username = normalize_username(educator.get("username", ""))
        if uid and username:
            try:
                educators_collection.update_one(
                    {"uid": uid},
                    {"$set": {"username": username, "uid": uid}},
                    upsert=True
                )
                print(f"Added/Updated educator: {username} (UID: {uid})")
            except pymongo.errors.DuplicateKeyError:
                print(f"Duplicate educator UID {uid} skipped.")
            except pymongo.errors.PyMongoError as e:
                print(f"Error saving educator to MongoDB: {e}")

async def fetch_educators(goal_uid="TMUVD", limit=50, max_offset=1000):
    """Fetch educators from API, starting from offset 0."""
    base_url = "https://unacademy.com/api/v1/uplus/subscription/goal_educators/"
    educators = []
    offset = 0
    known_educator_uids = set(educators_collection.distinct("uid"))

    while offset <= max_offset:
        url = f"{base_url}?goal_uid={goal_uid}&limit={limit}&offset={offset}"
        try:
            print(f"Fetching educators from API at offset {offset}...")
            response = await asyncio.get_event_loop().run_in_executor(None, lambda: requests.get(url, timeout=10))
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 at offset {offset}. Stopping fetch.")
                break

            results = data.get("results", [])
            if not results:
                print(f"No more educators at offset {offset}. Stopping fetch.")
                break

            for i, educator in enumerate(results, start=offset + 1):
                username = normalize_username(educator.get("username", ""))
                uid = educator.get("uid", "")
                if username and uid and uid not in known_educator_uids:
                    known_educator_uids.add(uid)
                    educators.append({
                        "username": username,
                        "uid": uid,
                        "first_name": educator.get("first_name", "N/A"),
                        "last_name": educator.get("last_name", "N/A"),
                        "avatar": educator.get("avatar", "N/A")
                    })
                    print(f"{i} {educator.get('first_name')} {educator.get('last_name')} : {username} : {uid}")
                else:
                    print(f"Skipping educator UID {uid} (already in MongoDB)")

            offset += limit
        except requests.RequestException as e:
            print(f"Request failed at offset {offset}: {e}")
            break

    print(f"Fetched {len(educators)} new educators.")
    return educators

async def fetch_batches(username, known_educator_uids, limit=50, max_offset=1000):
    """Fetch batches for a username and return new educators."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    new_educators = []
    offset = 0

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        try:
            response = await asyncio.get_event_loop().run_in_executor(None, lambda: requests.get(url, timeout=10))
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 for batches of {username}. Stopping fetch.")
                break

            results = data.get("results", [])
            if not results:
                print(f"No more batches for {username} at offset {offset}. Stopping fetch.")
                break

            for batch in results:
                authors = batch.get("authors", [])
                for author in authors:
                    author_uid = author.get("uid")
                    author_username = normalize_username(author.get("username", ""))
                    if author_uid and author_uid not in known_educator_uids:
                        known_educator_uids.add(author_uid)
                        new_educators.append({
                            "username": author_username,
                            "uid": author_uid,
                            "first_name": author.get("first_name", "N/A"),
                            "last_name": author.get("last_name", "N/A"),
                            "avatar": author.get("avatar", "N/A")
                        })

            offset += limit
        except requests.RequestException as e:
            print(f"Failed to fetch batches for {username}: {e}")
            break

    return new_educators

def normalize_username(username):
    """Normalize username to lowercase and remove special characters."""
    return re.sub(r'[^a-zA-Z0-9]', '', username).lower()

async def send_progress_bar():
    """Send or update the progress bar message."""
    global progress_message, update_obj, update_context
    educator_count = educators_collection.count_documents({})
    progress_text = f"Total Educators Fetched: {educator_count}"
    
    if progress_message is None:
        progress_message = await update_obj.message.reply_text(progress_text)
    else:
        try:
            await progress_message.edit_text(progress_text)
        except Exception as e:
            print(f"Error updating progress bar: {e}")
            progress_message = await update_obj.message.reply_text(progress_text)

async def upload_json():
    """Upload educators.json to Telegram, splitting if >50MB, and delete files."""
    global update_obj, update_context
    try:
        educators = list(educators_collection.find({}, {"_id": 0, "username": 1, "uid": 1}))
        save_to_json("educators.json", educators)
        json_files = split_json_file("educators.json", max_size_mb=50)
        for i, json_file in enumerate(json_files, 1):
            with open(json_file, "rb") as f:
                caption = f"Updated educators.json (Part {i} of {len(json_files)})" if len(json_files) > 1 else "Updated educators.json"
                await update_context.bot.send_document(
                    chat_id=update_obj.effective_chat.id,
                    document=f,
                    caption=caption
                )
            os.remove(json_file)
            print(f"Deleted {json_file} after upload")
    except Exception as e:
        await update_obj.message.reply_text(f"Error uploading JSON: {e}")
        for file in ["educators.json"] + [f for f in os.listdir() if f.startswith("educators_part_")]:
            if os.path.exists(file):
                os.remove(file)
                print(f"Deleted {file} due to upload error")

async def periodic_educators_upload():
    """Upload educators.json every 20 minutes."""
    global update_context, update_obj
    await asyncio.sleep(20 * 60)
    while fetching:
        try:
            print("Starting periodic educators upload...")
            await upload_json()
            educator_count = educators_collection.count_documents({})
            progress_text = f"Periodic Update: Total Educators Fetched: {educator_count}"
            await update_obj.message.reply_text(progress_text)
        except Exception as e:
            print(f"Error in periodic upload: {e}")
            await update_obj.message.reply_text(f"Error in periodic upload: {e}")
        await asyncio.sleep(20 * 60)

async def progress_updater():
    """Update progress bar every 60 seconds."""
    global last_educator_count
    while fetching:
        try:
            educator_count = educators_collection.count_documents({})
            if educator_count != last_educator_count:
                last_educator_count = educator_count
                await send_progress_bar()
        except Exception as e:
            print(f"Error in progress updater: {e}")
        await asyncio.sleep(60)

async def fetch_educators_in_background():
    """Run educators fetching as an asyncio task."""
    global fetching, progress_message
    known_educator_uids = set()

    while fetching:
        print("Starting new fetch cycle from offset 0...")
        educators = await fetch_educators()
        if educators:
            save_educators_to_mongodb(educators)
            educator_queue = [(e["username"], e["uid"]) for e in educators]
            known_educator_uids.update(e["uid"] for e in educators)
        else:
            educator_queue = []

        processed_educators = set()
        while educator_queue and fetching:
            current_educators = educator_queue
            educator_queue = []
            print(f"Processing {len(current_educators)} educators for batches...")

            for username, uid in current_educators:
                if not fetching or username in processed_educators:
                    continue
                processed_educators.add(username)
                print(f"Fetching batches for {username}...")
                new_educators = await fetch_batches(username, known_educator_uids)
                if new_educators:
                    save_educators_to_mongodb(new_educators)
                    educator_queue.extend((e["username"], e["uid"]) for e in new_educators)
                    known_educator_uids.update(e["uid"] for e in new_educators)
                    print(f"New educators found for {username}: {[e['username'] for e in new_educators]}")
                else:
                    print(f"No new educators found for {username}.")

        if fetching:
            print("Completed fetch cycle. Starting next cycle after 10 seconds...")
            await asyncio.sleep(10)
        else:
            print("Fetching stopped by user.")
            await upload_json()
            await update_obj.message.reply_text("Educators fetch stopped. Final educators.json uploaded.")

    fetching = False
    progress_message = None

async def educators_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /educators command."""
    global fetching, update_context, update_obj, last_educator_count
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return

    fetching = True
    update_context = context
    update_obj = update
    last_educator_count = 0
    await update.message.reply_text("Starting educators fetch from scratch... 📚")

    asyncio.create_task(periodic_educators_upload())
    asyncio.create_task(progress_updater())
    asyncio.create_task(fetch_educators_in_background())

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /stop command."""
    global fetching, progress_message
    if not fetching:
        await update.message.reply_text("No fetching process is running!")
        return

    fetching = False
    progress_message = None
    await update.message.reply_text("Stopping fetching process...")

async def main():
    """Start the Telegram bot."""
    bot_token = '7213717609:AAG4gF6dRvqxPcg-WaovRW2Eu1d5jxT566o'
    application = Application.builder().token(bot_token).build()

    application.add_handler(CommandHandler("educators", educators_command))
    application.add_handler(CommandHandler("stop", stop_command))

    print("Bot is starting...")
    await application.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
