import requests
import json
import os
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from threading import Thread
import asyncio
import re
from datetime import datetime
import dateutil.parser
from pymongo import MongoClient
import io

# MongoDB connection
client = MongoClient(os.environ['MONGODB_URI', 'mongodb+srv://elvishyadavop:ClA5yIHTbCutEnVP@cluster0.u83zlfx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'])
db = client['unacademy_db']
collection = db['educators']

# Global variables to control fetching
fetching = False
fetching_educators = False  # Track if /educators is running
last_json_data = {}
last_educator_count = 0
last_course_count = 0
last_batch_count = 0
progress_message = None
update_context = None
update_obj = None
loop = None

def fetch_educators(goal_uid="TMUVD", limit=50, max_offset=1000, known_educator_uids=None):
    """Fetch educators and add new ones to MongoDB."""
    base_url = "https://unacademy.com/api/v1/uplus/subscription/goal_educators/"
    educators = []  # List of (username, uid) for queue
    known_educator_uids = known_educator_uids if known_educator_uids is not None else set()
    offset = 0

    while offset <= max_offset:
        url = f"{base_url}?goal_uid={goal_uid}&limit={limit}&offset={offset}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print("Error E001 encountered. Stopping educator fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid educator results found at offset {offset}. Stopping educator fetch.")
                break

            for i, educator in enumerate(results, start=offset + 1):
                username = normalize_username(educator.get("username", ""))
                uid = educator.get("uid", "")
                if username and uid not in known_educator_uids:
                    known_educator_uids.add(uid)
                    print(f"{i} {educator.get('first_name')} {educator.get('last_name')} : {username} : {uid} : {educator.get('avatar')}")
                    educator_data = {
                        "first_name": educator.get("first_name", "N/A"),
                        "last_name": educator.get("last_name", "N/A"),
                        "username": username,
                        "uid": uid,
                        "avatar": educator.get("avatar", "N/A")
                    }
                    collection.update_one({'uid': uid}, {'$set': educator_data}, upsert=True)
                    educators.append((username, uid))

            offset += limit
        except requests.RequestException as e:
            print(f"Request failed for educators: {e}")
            break

    return educators

def fetch_educator_by_username(username):
    """Fetch educator details by username from course API."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit=1&type=latest"
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        data = response.json()

        results = data.get("results")
        if results and isinstance(results, list) and len(results) > 0:
            author = results[0].get("author")
            if author:
                return {
                    "first_name": author.get("first_name", "N/A"),
                    "last_name": author.get("last_name", "N/A"),
                    "username": normalize_username(author.get("username", "N/A")),
                    "uid": author.get("uid", "N/A"),
                    "avatar": author.get("avatar", "N/A")
                }
        print(f"No courses found for username: {username}")
        return None
    except requests.RequestException as e:
        print(f"Failed to fetch educator details for {username}: {e}")
        return None

def fetch_courses(username, limit=50, max_offset=1000, json_data=None):
    """Fetch courses for a given username and add to json_data in memory."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}&type=latest"
    seen_uids = set()
    if json_data is not None:
        json_data["courses"] = json_data.get("courses", {})
        json_data["courses"][username] = json_data["courses"].get(username, [])
    offset = 1

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 encountered for courses of {username}. Stopping course fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid course results found for {username} at offset {offset}. Stopping course fetch.")
                break

            if not results:
                print(f"No more courses found for {username} at offset {offset}. Stopping course fetch.")
                break

            for i, course in enumerate(results, start=offset):
                course_uid = course.get("uid")
                if course_uid and course_uid not in seen_uids:
                    seen_uids.add(course_uid)
                    print(f"{i} Course Name :- {course.get('name', 'N/A')}")
                    print(f"Slug :- {course.get('slug', 'N/A')}")
                    print(f"Thumbnail :- {course.get('thumbnail', 'N/A')}")
                    print(f"Uid :- {course_uid}")
                    print(f"Starts at :- {course.get('starts_at', 'N/A')}")
                    print(f"Ends at :- {course.get('ends_at', 'N/A')}")
                    print("----------------------")
                    if json_data is not None:
                        json_data["courses"][username].append({
                            "name": course.get("name", "N/A"),
                            "slug": course.get("slug", "N/A"),
                            "thumbnail": course.get("thumbnail", "N/A"),
                            "uid": course_uid,
                            "starts_at": course.get("starts_at", "N/A"),
                            "ends_at": course.get("ends_at", "N/A")
                        })

            offset += limit
        except requests.RequestException as e:
            print(f"Failed to fetch courses for {username}: {e}")
            break

def fetch_batches(username, known_educator_uids, limit=50, max_offset=1000, json_data=None):
    """Fetch batches for a given username, add new educators to DB, and add batches to json_data if provided."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    seen_batch_uids = set()
    new_educators = []
    offset = 2

    if json_data is not None:
        json_data["batches"] = json_data.get("batches", {})
        json_data["batches"][username] = json_data["batches"].get(username, [])

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 encountered for batches of {username}. Stopping batch fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid batch results found for {username} at offset {offset}. Stopping batch fetch.")
                break

            if not results:
                print(f"No more batches found for {username} at offset {offset}. Stopping batch fetch.")
                break

            for i, batch in enumerate(results, start=offset):
                batch_uid = batch.get("uid")
                if batch_uid and batch_uid not in seen_batch_uids:
                    seen_batch_uids.add(batch_uid)
                    print(f"{i} Batch Name :- {batch.get('name', 'N/A')}")
                    print(f"Slug :- {batch.get('slug', 'N/A')}")
                    print(f"Thumbnail :- {batch.get('thumbnail', 'N/A')}")
                    print(f"Uid :- {batch_uid}")
                    print(f"Starts at :- {batch.get('starts_at', 'N/A')}")
                    print(f"Ends at :- {batch.get('ends_at', 'N/A')}")
                    print(f"Completed at :- {batch.get('completed_at', 'N/A')}")
                    print("----------------------")
                    if json_data is not None:
                        json_data["batches"][username].append({
                            "name": batch.get("name", "N/A"),
                            "slug": batch.get("slug", "N/A"),
                            "thumbnail": batch.get("thumbnail", "N/A"),
                            "uid": batch_uid,
                            "starts_at": batch.get("starts_at", "N/A"),
                            "ends_at": batch.get("ends_at", "N/A"),
                            "completed_at": batch.get("completed_at", "N/A")
                        })
                    authors = batch.get("authors", [])
                    for author in authors:
                        author_uid = author.get("uid")
                        author_username = normalize_username(author.get("username", ""))
                        if author_uid and author_uid not in known_educator_uids:
                            known_educator_uids.add(author_uid)
                            educator_data = {
                                "first_name": author.get("first_name", "N/A"),
                                "last_name": author.get("last_name", "N/A"),
                                "username": author_username,
                                "uid": author_uid,
                                "avatar": author.get("avatar", "N/A")
                            }
                            collection.update_one({'uid': author_uid}, {'$set': educator_data}, upsert=True)
                            new_educators.append((author_username, author_uid))

            offset += limit
        except requests.RequestException as e:
            print(f"Failed to fetch batches for {username}: {e}")
            break

    return new_educators

def count_items(json_data):
    """Count courses and batches in json_data, educators from DB."""
    educator_count = collection.count_documents({})
    course_count = sum(len(courses) for courses in json_data.get("courses", {}).values())
    batch_count = sum(len(batches) for batches in json_data.get("batches", {}).values())
    return educator_count, course_count, batch_count

async def send_progress_bar():
    """Send or update the progress bar message."""
    global progress_message, update_obj, update_context, fetching_educators
    if fetching_educators:
        educator_count = collection.count_documents({})
        progress_text = f"Total Educators Fetched: {educator_count}"
    else:
        educator_count, course_count, batch_count = count_items(last_json_data)
        progress_text = (
            "📊 *Progress Bar*\n"
            f"Total Educators Fetched: {educator_count}\n"
            f"Total Courses Fetched: {course_count}\n"
            f"Total Batches Fetched: {batch_count}"
        )
    
    if progress_message is None:
        progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown" if not fetching_educators else None)
    else:
        try:
            await progress_message.edit_text(progress_text, parse_mode="Markdown" if not fetching_educators else None)
        except Exception as e:
            print(f"Error updating progress bar: {e}")
            progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown" if not fetching_educators else None)

async def upload_json():
    """Upload JSON data to Telegram using in-memory BytesIO based on fetching mode."""
    global update_obj, update_context, fetching_educators
    try:
        if fetching_educators:
            data = list(collection.find({}, {'_id': 0}))
            educator_count = len(data)
            json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            await update_context.bot.send_document(
                chat_id=update_obj.effective_chat.id,
                document=io.BytesIO(json_bytes),
                filename="educators.json",
                caption=f"😑 In mongodb :- {educator_count}"
            )
        else:
            # Include educators from DB in json_data for consistency
            last_json_data["educators"] = list(collection.find({}, {'_id': 0}))
            json_bytes = json.dumps(last_json_data, indent=2, ensure_ascii=False).encode('utf-8')
            await update_context.bot.send_document(
                chat_id=update_obj.effective_chat.id,
                document=io.BytesIO(json_bytes),
                filename="funkabhosda.json",
                caption="Updated funkabhosda.json"
            )
            # Upload educators.json separately
            educators_data = last_json_data["educators"]
            educators_count = len(educators_data)
            educators_bytes = json.dumps(educators_data, indent=2, ensure_ascii=False).encode('utf-8')
            await update_context.bot.send_document(
                chat_id=update_obj.effective_chat.id,
                document=io.BytesIO(educators_bytes),
                filename="educators.json",
                caption=f"😑 In mongodb :- {educators_count}"
            )
    except Exception as e:
        await update_obj.message.reply_text(f"Error uploading JSON: {e}")

async def progress_updater():
    """Update progress bar every 60 seconds and upload JSON every 2 minutes."""
    global last_educator_count, last_course_count, last_batch_count
    last_upload_time = time.time()
    
    while fetching:
        try:
            if fetching_educators:
                educator_count = collection.count_documents({})
                if educator_count != last_educator_count:
                    last_educator_count = educator_count
                    await send_progress_bar()
            else:
                educator_count, course_count, batch_count = count_items(last_json_data)
                if (educator_count != last_educator_count or
                    course_count != last_course_count or
                    batch_count != last_batch_count):
                    last_educator_count, last_course_count, last_batch_count = educator_count, course_count, batch_count
                    await send_progress_bar()
            
            current_time = time.time()
            if current_time - last_upload_time >= 120:
                await upload_json()
                last_upload_time = current_time
                
        except Exception as e:
            print(f"Error in progress updater: {e}")
        
        await asyncio.sleep(60)

def normalize_username(username):
    """Normalize username to lowercase and remove special characters."""
    return re.sub(r'[^a-zA-Z0-9]', '', username).lower()

def filter_by_time(json_data, current_time, future=True):
    """Filter courses and batches based on time (future or past)."""
    filtered_data = {
        "educators": json_data.get("educators", []),
        "courses": {},
        "batches": {}
    }
    username = list(json_data["courses"].keys())[0] if json_data.get("courses") else None
    if username:
        filtered_data["courses"][username] = []
        filtered_data["batches"][username] = []

        for course in json_data["courses"].get(username, []):
            ends_at = course.get("ends_at")
            if ends_at and ends_at != "N/A":
                try:
                    end_time = dateutil.parser.isoparse(ends_at)
                    if (future and end_time > current_time) or (not future and end_time <= current_time):
                        filtered_data["courses"][username].append(course)
                except ValueError:
                    continue

        for batch in json_data["batches"].get(username, []):
            completed_at = batch.get("completed_at")
            if completed_at and completed_at != "N/A":
                try:
                    complete_time = dateutil.parser.isoparse(completed_at)
                    if (future and complete_time > current_time) or (not future and complete_time <= current_time):
                        filtered_data["batches"][username].append(batch)
                except ValueError:
                    continue

    return filtered_data

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /add command."""
    global update_context, update_obj
    update_context = context
    update_obj = update

    if not context.args:
        await update.message.reply_text("Please provide a username. Usage: /add {username}")
        return

    raw_username = context.args[0]
    username = normalize_username(raw_username)
    await update.message.reply_text(f"Fetching data for username: {username}...")

    educator = fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"No educator found with username: {username}")
        return

    # Add to DB if not exists
    collection.update_one({'uid': educator['uid']}, {'$set': educator}, upsert=True)

    json_data = {
        "educators": [educator],
        "courses": {},
        "batches": {}
    }
    known_educator_uids = {educator["uid"]}

    print(f"Fetching courses for {username}...")
    fetch_courses(username, json_data=json_data)

    print(f"Fetching batches for {username}...")
    fetch_batches(username, known_educator_uids, json_data=json_data)

    current_time = datetime.now(dateutil.tz.tzutc())
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    all_course_count = len(json_data["courses"].get(username, []))
    all_batch_count = len(json_data["batches"].get(username, []))

    current_json_data = filter_by_time(json_data, current_time, future=True)
    completed_json_data = filter_by_time(json_data, current_time, future=False)

    current_course_count = len(current_json_data["courses"].get(username, []))
    current_batch_count = len(current_json_data["batches"].get(username, []))
    completed_course_count = len(completed_json_data["courses"].get(username, []))
    completed_batch_count = len(completed_json_data["batches"].get(username, []))

    caption_template = (
        f"Teacher Name :- {educator['first_name']} {educator['last_name']}\n"
        f"Username :- {username}\n"
        f"Uid :- {educator['uid']}\n"
        f"Total Batches :- {{batch_count}}\n"
        f"Total Courses :- {{course_count}}\n"
        f"Thumbnail :- {educator['avatar']}\n"
        f"Last Checked :- {last_checked}"
    )

    files_to_upload = [
        (json_data, all_course_count, all_batch_count, "funkabhosda.json", "All courses and batches"),
        (current_json_data, current_course_count, current_batch_count, f"{username}_current.json", "Current (future) courses and batches"),
        (completed_json_data, completed_course_count, completed_batch_count, f"{username}_completed.json", "Completed (past) courses and batches")
    ]

    for data, course_count, batch_count, filename, description in files_to_upload:
        try:
            json_bytes = json.dumps(data, indent=2, ensure_ascii=False).encode('utf-8')
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=io.BytesIO(json_bytes),
                filename=filename,
                caption=caption_template.format(course_count=course_count, batch_count=batch_count)
            )
        except Exception as e:
            await update.message.reply_text(f"Error uploading {description}: {e}")

    await update.message.reply_text(f"Data for {username} uploaded successfully (all, current, and completed)!")

async def educators_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /educators command."""
    global fetching, fetching_educators, update_context, update_obj, loop
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return

    fetching = True
    fetching_educators = True
    update_context = context
    update_obj = update
    loop = asyncio.get_running_loop()
    await update.message.reply_text("Starting educators fetch... 📚")

    asyncio.create_task(progress_updater())
    thread = Thread(target=fetch_educators_in_background)
    thread.start()

def fetch_educators_in_background():
    """Run the educators fetching process in a background thread."""
    global fetching, fetching_educators, progress_message
    
    known_educator_uids = set(doc['uid'] for doc in collection.find({}, {'uid': 1, '_id': 0}))

    print("Fetching initial educators...")
    educators = fetch_educators(known_educator_uids=known_educator_uids)

    educator_queue = educators
    processed_educators = set()

    while educator_queue and fetching:
        current_educators = educator_queue
        educator_queue = []
        print(f"\nProcessing {len(current_educators)} educators for batches...")

        for username, uid in current_educators:
            if not fetching:
                break
            if username in processed_educators:
                continue
            processed_educators.add(username)

            print(f"\nFetching batches for {username} to find new educators...")
            new_educators = fetch_batches(username, known_educator_uids)
            if new_educators:
                print(f"\nNew educators found in batches for {username}:")
                for new_username, new_uid in new_educators:
                    print(f"New: {new_username} : {new_uid}")
                educator_queue.extend(new_educators)
            else:
                print(f"\nNo new educators found in batches for {username}.")

    if fetching:
        print("\nAll educators processed.")
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Educators fetch completed! Final educators.json uploaded."), loop).result()
    else:
        print("\nFetching stopped by user.")
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Educators fetch stopped. Partial educators.json uploaded."), loop).result()
    
    fetching = False
    fetching_educators = False
    progress_message = None

async def now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /now command."""
    global fetching, fetching_educators, update_context, update_obj, loop
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return
    
    fetching = True
    fetching_educators = False
    update_context = context
    update_obj = update
    loop = asyncio.get_running_loop()
    await update.message.reply_text("Starting data fetch... ☠️")
    
    asyncio.create_task(progress_updater())
    thread = Thread(target=fetch_data_in_background)
    thread.start()

def fetch_data_in_background():
    """Run the full data fetching process in a background thread."""
    global fetching, fetching_educators, last_json_data, last_educator_count, last_course_count, last_batch_count, progress_message
    
    json_data = {
        "courses": {},
        "batches": {}
    }
    last_json_data = json_data
    known_educator_uids = set(doc['uid'] for doc in collection.find({}, {'uid': 1, '_id': 0}))

    print("Fetching initial educators...")
    educators = fetch_educators(known_educator_uids=known_educator_uids)

    educator_queue = educators
    processed_educators = set()

    while educator_queue and fetching:
        current_educators = educator_queue
        educator_queue = []
        print(f"\nProcessing {len(current_educators)} educators...")

        for username, uid in current_educators:
            if not fetching:
                break
            if username in processed_educators:
                continue
            processed_educators.add(username)

            print(f"\nFetching courses for {username}...")
            fetch_courses(username, json_data=json_data)

            print(f"\nFetching batches for {username}...")
            new_educators = fetch_batches(username, known_educator_uids, json_data=json_data)
            if new_educators:
                print(f"\nNew educators found in batches for {username}:")
                for new_username, new_uid in new_educators:
                    print(f"New: {new_username} : {new_uid}")
                educator_queue.extend(new_educators)
            else:
                print(f"\nNo new educators found in batches for {username}.")

    if fetching:
        print("\nAll educators processed. Final data prepared.")
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Fetching completed! Final funkabhosda.json and educators.json uploaded."), loop).result()
    else:
        print("\nFetching stopped by user.")
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Fetching stopped. Partial funkabhosda.json and educators.json uploaded."), loop).result()
    
    fetching = False
    fetching_educators = False
    progress_message = None

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /stop command."""
    global fetching, fetching_educators, progress_message
    if not fetching:
        await update.message.reply_text("No fetching process is running!")
        return
    
    fetching = False
    fetching_educators = False
    progress_message = None
    await update.message.reply_text("Stopping fetching process...")

async def main():
    """Start the Telegram bot."""
    bot_token = '7213717609:AAG4gF6dRvqxPcg-WaovRW2Eu1d5jxT566o'
    application = Application.builder().token(bot_token).build()
    
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("add", add_command))
    application.add_handler(CommandHandler("educators", educators_command))
    
    print("Bot is starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    try:
        await asyncio.Event().wait()
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "Cannot close a running event loop" in str(e):
            print("Event loop is running; skipping close.")
        else:
            raise
