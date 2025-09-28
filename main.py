import pymongo
from pymongo import MongoClient
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

# MongoDB setup
MONGO_URI = "mongodb+srv://elvishyadavop:ClA5yIHTbCutEnVP@cluster0.u83zlfx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"  # Replace with your MongoDB Atlas URI
client = MongoClient(MONGO_URI)
db = client["unacademy_db"]
educators_collection = db["educators"]
educators_collection.create_index("uid", unique=True)

# Global variables
fetching = False
fetching_educators = False
last_json_data = {}
last_educators_json_data = []
last_educator_count = 0
last_course_count = 0
last_batch_count = 0
progress_message = None
update_context = None
update_obj = None
loop = None

def save_to_json(filename, data):
    """Save data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def save_educators_to_mongodb(educators):
    """Save unique educators to MongoDB and return the list for JSON."""
    unique_educators = []
    seen_pairs = set()
    for educator in educators:
        username = normalize_username(educator.get("username", ""))
        uid = educator.get("uid", "")
        if username and uid and (username, uid) not in seen_pairs:
            seen_pairs.add((username, uid))
            doc = {"username": username, "uid": uid}
            try:
                educators_collection.update_one(
                    {"uid": uid},
                    {"$set": doc},
                    upsert=True
                )
                unique_educators.append(doc)
            except pymongo.errors.DuplicateKeyError:
                print(f"Duplicate educator UID {uid} skipped.")
            except pymongo.errors.PyMongoError as e:
                print(f"Error saving educator to MongoDB: {e}")
    save_to_json("educators.json", unique_educators)
    return unique_educators

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

def fetch_educators(goal_uid="TMUVD", limit=50, max_offset=1000, educators_list=None):
    """Fetch all educators from API, starting from scratch."""
    base_url = "https://unacademy.com/api/v1/uplus/subscription/goal_educators/"
    seen_usernames = set()
    educators = []
    educators_list = educators_list if educators_list is not None else []
    offset = 0
    known_educator_uids = set(educators_collection.distinct("uid"))  # Load existing UIDs

    while offset <= max_offset:
        url = f"{base_url}?goal_uid={goal_uid}&limit={limit}&offset={offset}"
        try:
            print(f"Fetching educators from API at offset {offset}...")
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 encountered at offset {offset}. Stopping educator fetch for this cycle.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid educator results found at offset {offset}. Stopping educator fetch for this cycle.")
                break

            if not results:
                print(f"No more educators found at offset {offset}. Stopping educator fetch for this cycle.")
                break

            for i, educator in enumerate(results, start=offset + 1):
                username = normalize_username(educator.get("username", ""))
                uid = educator.get("uid", "")
                if username and uid:
                    if uid not in known_educator_uids:
                        seen_usernames.add(username)
                        known_educator_uids.add(uid)
                        educators.append((username, uid))
                        print(f"{i} {educator.get('first_name')} {educator.get('last_name')} : {username} : {uid} : {educator.get('avatar')}")
                        educators_list.append({
                            "first_name": educator.get("first_name", "N/A"),
                            "last_name": educator.get("last_name", "N/A"),
                            "username": username,
                            "uid": uid,
                            "avatar": educator.get("avatar", "N/A")
                        })
                    else:
                        print(f"Skipping educator UID {uid} (already in MongoDB)")

            offset += limit
        except requests.RequestException as e:
            print(f"Request failed for educators at offset {offset}: {e}")
            break

    print(f"Fetched {len(educators)} new educators in this cycle.")
    return educators, educators_list

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

def fetch_courses(username, limit=50, max_offset=1000, json_data=None, filename="funkabhosda.json"):
    """Fetch courses for a given username and save to JSON."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}&type=latest"
    seen_uids = set()
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
                    json_data["courses"][username].append({
                        "name": course.get("name", "N/A"),
                        "slug": course.get("slug", "N/A"),
                        "thumbnail": course.get("thumbnail", "N/A"),
                        "uid": course_uid,
                        "starts_at": course.get("starts_at", "N/A"),
                        "ends_at": course.get("ends_at", "N/A")
                    })

            save_to_json(filename, json_data)
            offset += limit
        except requests.RequestException as e:
            print(f"Failed to fetch courses for {username}: {e}")
            break

def fetch_batches(username, known_educator_uids, limit=50, max_offset=1000, educators_list=None):
    """Fetch batches for a given username and append new educators to educators_list."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    seen_batch_uids = set()
    new_educators = []
    educators_list = educators_list if educators_list is not None else []
    offset = 2

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
                    authors = batch.get("authors", [])
                    for author in authors:
                        author_uid = author.get("uid")
                        author_username = normalize_username(author.get("username", ""))
                        if author_uid and author_uid not in known_educator_uids:
                            known_educator_uids.add(author_uid)
                            new_educators.append({
                                "first_name": author.get("first_name", "N/A"),
                                "last_name": author.get("last_name", "N/A"),
                                "username": author_username,
                                "uid": author_uid,
                                "avatar": author.get("avatar", "N/A")
                            })
                            educators_list.append({
                                "first_name": author.get("first_name", "N/A"),
                                "last_name": author.get("last_name", "N/A"),
                                "username": author_username,
                                "uid": author_uid,
                                "avatar": author.get("avatar", "N/A")
                            })

            offset += limit
        except requests.RequestException as e:
            print(f"Failed to fetch batches for {username}: {e}")
            break

    return new_educators, educators_list

def count_items(json_data):
    """Count educators, courses, and batches in json_data."""
    educator_count = len(json_data.get("educators", []))
    course_count = sum(len(courses) for courses in json_data.get("courses", {}).values())
    batch_count = sum(len(batches) for batches in json_data.get("batches", {}).values())
    return educator_count, course_count, batch_count

async def send_progress_bar():
    """Send or update the progress bar message."""
    global progress_message, update_obj, update_context, fetching_educators
    if fetching_educators:
        educator_count = educators_collection.count_documents({})
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
    """Upload JSON file(s) to Telegram, splitting if >50MB, and delete files."""
    global update_obj, update_context, fetching_educators
    try:
        if fetching_educators:
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
        else:
            json_files = ["funkabhosda.json"] if os.path.exists("funkabhosda.json") else []
            for json_file in json_files:
                with open(json_file, "rb") as f:
                    await update_context.bot.send_document(
                        chat_id=update_obj.effective_chat.id,
                        document=f,
                        caption="Updated funkabhosda.json"
                    )
                os.remove(json_file)
                print(f"Deleted {json_file} after upload")
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
        for file in ["educators.json", "funkabhosda.json"] + [f for f in os.listdir() if f.startswith("educators_part_")]:
            if os.path.exists(file):
                os.remove(file)
                print(f"Deleted {file} due to upload error")

async def periodic_educators_upload():
    """Fetch educators from MongoDB and upload to Telegram every 20 minutes."""
    global update_context, update_obj, loop
    # Wait 20 minutes before the first upload
    await asyncio.sleep(20 * 60)
    while fetching and fetching_educators:
        try:
            print("Starting periodic educators upload...")
            educators = list(educators_collection.find({}, {"_id": 0, "username": 1, "uid": 1}))
            educator_count = len(educators)
            print(f"Fetched {educator_count} educators from MongoDB")
            
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
                print(f"Deleted {json_file} after periodic upload")
            
            progress_text = f"Periodic Update: Total Educators Fetched: {educator_count}"
            if update_obj:
                await update_obj.message.reply_text(progress_text)
        except Exception as e:
            print(f"Error in periodic upload: {e}")
            if update_obj:
                await update_obj.message.reply_text(f"Error in periodic upload: {e}")
            for file in ["educators.json"] + [f for f in os.listdir() if f.startswith("educators_part_")]:
                if os.path.exists(file):
                    os.remove(file)
                    print(f"Deleted {file} due to periodic upload error")
        
        await asyncio.sleep(20 * 60)

async def progress_updater():
    """Update progress bar every 60 seconds, upload JSON every 2 minutes only for /now."""
    global last_json_data, last_educators_json_data, last_educator_count, last_course_count, last_batch_count
    last_upload_time = time.time()
    
    while fetching:
        try:
            if fetching_educators:
                educator_count = educators_collection.count_documents({})
                if educator_count != last_educator_count:
                    last_educator_count = educator_count
                    last_educators_json_data = list(educators_collection.find({}, {"_id": 0, "username": 1, "uid": 1}))
                    await send_progress_bar()
            else:
                with open("funkabhosda.json", "r", encoding="utf-8") as f:
                    current_json_data = json.load(f)
                educator_count, course_count, batch_count = count_items(current_json_data)
                if (educator_count != last_educator_count or
                    course_count != last_course_count or
                    batch_count != last_batch_count):
                    last_json_data = current_json_data
                    save_educators_to_mongodb(current_json_data.get("educators", []))
                    last_educators_json_data = list(educators_collection.find({}, {"_id": 0, "username": 1, "uid": 1}))
                    last_educator_count, last_course_count, last_batch_count = educator_count, course_count, batch_count
                    await send_progress_bar()
            
            # Only upload JSON every 2 minutes for /now, not /educators
            if not fetching_educators:
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
        "educators": json_data["educators"],
        "courses": {},
        "batches": {}
    }
    username = list(json_data["courses"].keys())[0] if json_data["courses"] else None
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

    json_data = {
        "educators": [educator],
        "courses": {},
        "batches": {}
    }
    all_filename = f"{username}_data.json"
    current_filename = f"{username}_current.json"
    completed_filename = f"{username}_completed.json"
    known_educator_uids = {educator["uid"]}

    print(f"Fetching courses for {username}...")
    fetch_courses(username, json_data=json_data, filename=all_filename)

    print(f"Fetching batches for {username}...")
    fetch_batches(username, known_educator_uids, json_data=json_data, filename=all_filename)

    current_time = datetime.now(dateutil.tz.tzutc())
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    all_course_count = len(json_data["courses"].get(username, []))
    all_batch_count = len(json_data["batches"].get(username, []))

    current_json_data = filter_by_time(json_data, current_time, future=True)
    completed_json_data = filter_by_time(json_data, current_time, future=False)

    save_to_json(current_filename, current_json_data)
    save_to_json(completed_filename, completed_json_data)

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
        (all_filename, all_course_count, all_batch_count, "All courses and batches"),
        (current_filename, current_course_count, current_batch_count, "Current (future) courses and batches"),
        (completed_filename, completed_course_count, completed_batch_count, "Completed (past) courses and batches")
    ]

    for filename, course_count, batch_count, description in files_to_upload:
        try:
            with open(filename, "rb") as f:
                await context.bot.send_document(
                    chat_id=update.effective_chat.id,
                    document=f,
                    caption=caption_template.format(course_count=course_count, batch_count=batch_count)
                )
            os.remove(filename)
            print(f"Deleted {filename} after upload")
        except Exception as e:
            await update.message.reply_text(f"Error uploading {description}: {e}")
            if os.path.exists(filename):
                os.remove(filename)
                print(f"Deleted {filename} due to upload error")

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

    # Reset in-memory data
    global last_educators_json_data, last_educator_count
    last_educators_json_data = []
    last_educator_count = 0

    # Start periodic upload and progress updater
    asyncio.create_task(periodic_educators_upload())
    asyncio.create_task(progress_updater())
    thread = Thread(target=fetch_educators_in_background)
    thread.start()

def fetch_educators_in_background():
    """Run the educators fetching process in a background thread."""
    global fetching, fetching_educators, last_educators_json_data, last_educator_count, progress_message
    
    while fetching and fetching_educators:
        educators_list = []
        print("Starting new fetch cycle from API...")
        educators, educators_list = fetch_educators(educators_list=educators_list)
        if educators:
            save_educators_to_mongodb(educators_list)
        else:
            print("No new educators fetched in this cycle.")

        educator_queue = [(username, uid) for username, uid in educators]
        processed_educators = set()
        known_educator_uids = set(educators_collection.distinct("uid"))

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
                new_educators, educators_list = fetch_batches(username, known_educator_uids, educators_list=educators_list)
                if new_educators:
                    save_educators_to_mongodb(educators_list)

                if new_educators:
                    print(f"\nNew educators found in batches for {username}:")
                    for educator in new_educators:
                        print(f"{educator['first_name']} {educator['last_name']} : {educator['username']} : {educator['uid']} : {educator['avatar']}")
                        educator_queue.append((educator["username"], educator["uid"]))
                else:
                    print(f"\nNo new educators found in batches for {username}.")

        if fetching:
            print("\nCompleted one fetch cycle. Starting next cycle after 10 seconds...")
            time.sleep(10)  # Brief pause before next fetch cycle
        else:
            print("\nFetching stopped by user.")
            last_educators_json_data = list(educators_collection.find({}, {"_id": 0, "username": 1, "uid": 1}))
            asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
            asyncio.run_coroutine_threadsafe(
                update_obj.message.reply_text("Educators fetch stopped. Final educators.json uploaded."), loop).result()
    
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
    global fetching, fetching_educators, last_json_data, last_educators_json_data, last_educator_count, last_course_count, last_batch_count, progress_message
    
    json_data = {
        "educators": [],
        "courses": {},
        "batches": {}
    }
    known_educator_uids = set()
    filename = "funkabhosda.json"

    print("Fetching initial educators...")
    educators, json_data["educators"] = fetch_educators(json_data=json_data, filename=filename)
    save_educators_to_mongodb(json_data["educators"])

    educator_queue = [(username, uid) for username, uid in educators]
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
            fetch_courses(username, json_data=json_data, filename=filename)

            print(f"\nFetching batches for {username}...")
            new_educators, json_data["educators"] = fetch_batches(username, known_educator_uids, json_data=json_data, filename=filename)
            save_educators_to_mongodb(json_data["educators"])

            if new_educators:
                print(f"\nNew educators found in batches for {username}:")
                for educator in new_educators:
                    print(f"{educator['first_name']} {educator['last_name']} : {educator['username']} : {educator['uid']} : {educator['avatar']}")
                    educator_queue.append((educator["username"], educator["uid"]))
            else:
                print(f"\nNo new educators found in batches for {username}.")

    if fetching:
        print("\nAll educators processed. Final data saved to funkabhosda.json and MongoDB.")
        save_educators_to_mongodb(json_data["educators"])
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Fetching completed! Final funkabhosda.json and educators.json uploaded."), loop).result()
    else:
        print("\nFetching stopped by user.")
        save_educators_to_mongodb(json_data["educators"])
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
