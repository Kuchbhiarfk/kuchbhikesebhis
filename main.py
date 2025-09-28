import requests
import json
import os
import time
import gc
import psutil
import ijson
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from threading import Thread, Lock
import asyncio
import aiofiles
from pymongo import MongoClient
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MongoDB setup
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://elvishyadavop:ClA5yIHTbCutEnVP@cluster0.u83zlfx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
DATABASE_NAME = os.getenv("DATABASE_NAME", "unacademy_data")
client = MongoClient(MONGO_URI)
db = client[DATABASE_NAME]
educators_collection = db.educators
courses_collection = db.courses
batches_collection = db.batches

# Global variables
fetching = False
last_educator_count = 0
last_course_count = 0
last_batch_count = 0
progress_message = None
update_context = None
update_obj = None
loop = None
json_lock = Lock()
filename = "funkabhosda.json"
offset_file = "offsets.json"
uploaded_file_ids = []
fetch_mode = None
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB per chunk

def get_existing_educator_uids():
    """Get all existing educator UIDs from MongoDB."""
    try:
        existing_uids = set()
        cursor = educators_collection.find({}, {"uid": 1, "_id": 0})
        for doc in cursor:
            existing_uids.add(doc.get("uid"))
        logger.info(f"Found {len(existing_uids)} existing educators in MongoDB")
        return existing_uids
    except Exception as e:
        logger.error(f"Error fetching existing educator UIDs: {e}")
        return set()

def get_existing_course_uids(username):
    """Get existing course UIDs for a specific username from MongoDB."""
    try:
        existing_uids = set()
        cursor = courses_collection.find({"username": username}, {"uid": 1, "_id": 0})
        for doc in cursor:
            existing_uids.add(doc.get("uid"))
        return existing_uids
    except Exception as e:
        logger.error(f"Error fetching existing course UIDs for {username}: {e}")
        return set()

def get_existing_batch_uids(username):
    """Get existing batch UIDs for a specific username from MongoDB."""
    try:
        existing_uids = set()
        cursor = batches_collection.find({"username": username}, {"uid": 1, "_id": 0})
        for doc in cursor:
            existing_uids.add(doc.get("uid"))
        return existing_uids
    except Exception as e:
        logger.error(f"Error fetching existing batch UIDs for {username}: {e}")
        return set()

def save_educators_to_mongodb(educators_data):
    """Save new educators to MongoDB."""
    try:
        if not educators_data:
            return 0
        
        # Add timestamp and username field
        for educator in educators_data:
            educator["created_at"] = datetime.utcnow()
            educator["updated_at"] = datetime.utcnow()
        
        result = educators_collection.insert_many(educators_data, ordered=False)
        logger.info(f"Saved {len(result.inserted_ids)} new educators to MongoDB")
        return len(result.inserted_ids)
    except Exception as e:
        logger.error(f"Error saving educators to MongoDB: {e}")
        return 0

def save_courses_to_mongodb(username, courses_data):
    """Save new courses to MongoDB."""
    try:
        if not courses_data:
            return 0
        
        # Add username and timestamp
        for course in courses_data:
            course["username"] = username
            course["created_at"] = datetime.utcnow()
            course["updated_at"] = datetime.utcnow()
        
        result = courses_collection.insert_many(courses_data, ordered=False)
        logger.info(f"Saved {len(result.inserted_ids)} new courses for {username} to MongoDB")
        return len(result.inserted_ids)
    except Exception as e:
        logger.error(f"Error saving courses for {username} to MongoDB: {e}")
        return 0

def save_batches_to_mongodb(username, batches_data):
    """Save new batches to MongoDB."""
    try:
        if not batches_data:
            return 0
        
        # Add username and timestamp
        for batch in batches_data:
            batch["username"] = username
            batch["created_at"] = datetime.utcnow()
            batch["updated_at"] = datetime.utcnow()
        
        result = batches_collection.insert_many(batches_data, ordered=False)
        logger.info(f"Saved {len(result.inserted_ids)} new batches for {username} to MongoDB")
        return len(result.inserted_ids)
    except Exception as e:
        logger.error(f"Error saving batches for {username} to MongoDB: {e}")
        return 0

def get_mongodb_counts():
    """Get counts from MongoDB."""
    try:
        educator_count = educators_collection.count_documents({})
        course_count = courses_collection.count_documents({})
        batch_count = batches_collection.count_documents({})
        return educator_count, course_count, batch_count
    except Exception as e:
        logger.error(f"Error getting MongoDB counts: {e}")
        return 0, 0, 0

async def save_to_json(filename, data):
    """Save data to a JSON file with locking."""
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        logger.info(f"Saved data to {filename}")
        gc.collect()
    except Exception as e:
        logger.error(f"Error saving JSON: {e}")

async def save_offsets(offsets):
    """Save offsets to track fetching progress."""
    try:
        async with aiofiles.open(offset_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(offsets, indent=2))
        logger.info(f"Saved offsets to {offset_file}")
    except Exception as e:
        logger.error(f"Error saving offsets: {e}")

async def load_offsets():
    """Load offsets from file."""
    try:
        if not os.path.exists(offset_file):
            return {}
        async with aiofiles.open(offset_file, 'r', encoding='utf-8') as f:
            return json.loads(await f.read())
    except Exception as e:
        logger.error(f"Error loading offsets: {e}")
        return {}

def fetch_educators(goal_uid="TMUVD", limit=10, max_offset=1000, json_data=None, filename=filename, known_educator_uids=None, start_offset=0):
    """Fetch educators and save new ones to MongoDB."""
    base_url = "https://unacademy.com/api/v1/uplus/subscription/goal_educators/"
    seen_usernames = set()
    educators = []
    json_data["educators"] = json_data.get("educators", [])
    new_educators_for_db = []
    offset = start_offset

    while offset <= max_offset:
        url = f"{base_url}?goal_uid={goal_uid}&limit={limit}&offset={offset}"
        try:
            response = requests.get(url, timeout=10, stream=True)
            response.raise_for_status()
            data = response.json()
            time.sleep(1)

            if isinstance(data, dict) and data.get("error_code") == "E001":
                logger.info("Error E001 encountered. Stopping educator fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                logger.info(f"No valid educator results found at offset {offset}. Stopping educator fetch.")
                break

            for i, educator in enumerate(results, start=offset + 1):
                username = educator.get("username")
                uid = educator.get("uid")
                if username and uid not in known_educator_uids:
                    seen_usernames.add(username)
                    known_educator_uids.add(uid)
                    educators.append((username, uid))
                    
                    educator_data = {
                        "first_name": educator.get("first_name", "N/A"),
                        "last_name": educator.get("last_name", "N/A"),
                        "username": username,
                        "uid": uid,
                        "avatar": educator.get("avatar", "N/A")
                    }
                    
                    print(f"{i} {educator.get('first_name')} {educator.get('last_name')} : {username} : {uid} : {educator.get('avatar')}")
                    json_data["educators"].append(educator_data)
                    new_educators_for_db.append(educator_data.copy())

            # Save new educators to MongoDB in batches
            if new_educators_for_db:
                save_educators_to_mongodb(new_educators_for_db)
                new_educators_for_db = []

            with json_lock:
                asyncio.run_coroutine_threadsafe(save_to_json(filename, json_data), loop).result()
                offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()
                offsets["educators"] = offset + limit
                asyncio.run_coroutine_threadsafe(save_offsets(offsets), loop).result()
            
            del data, results
            gc.collect()
            offset += limit

        except requests.RequestException as e:
            logger.error(f"Request failed for educators: {e}")
            time.sleep(5)
            continue

    return educators

def fetch_courses(username, limit=10, max_offset=1000, json_data=None, filename=filename, start_offset=1):
    """Fetch courses and save new ones to MongoDB."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}&type=latest"
    existing_course_uids = get_existing_course_uids(username)
    json_data["courses"] = json_data.get("courses", {})
    json_data["courses"][username] = json_data["courses"].get(username, [])
    new_courses_for_db = []
    offset = start_offset

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        try:
            response = requests.get(url, timeout=10, stream=True)
            response.raise_for_status()
            data = response.json()
            time.sleep(1)

            if isinstance(data, dict) and data.get("error_code") == "E001":
                logger.info(f"Error E001 encountered for courses of {username}. Stopping course fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                logger.info(f"No valid course results found for {username} at offset {offset}. Stopping course fetch.")
                break

            if not results:
                logger.info(f"No more courses found for {username} at offset {offset}. Stopping course fetch.")
                break

            for i, course in enumerate(results, start=offset):
                course_uid = course.get("uid")
                if course_uid and course_uid not in existing_course_uids:
                    existing_course_uids.add(course_uid)  # Add to local set to avoid duplicates in same batch
                    
                    course_data = {
                        "name": course.get("name", "N/A"),
                        "slug": course.get("slug", "N/A"),
                        "thumbnail": course.get("thumbnail", "N/A"),
                        "uid": course_uid,
                        "starts_at": course.get("starts_at", "N/A"),
                        "ends_at": course.get("ends_at", "N/A")
                    }
                    
                    print(f"{i} Course Name :- {course.get('name', 'N/A')}")
                    print(f"Slug :- {course.get('slug', 'N/A')}")
                    print(f"Uid :- {course_uid}")
                    print("----------------------")
                    
                    json_data["courses"][username].append(course_data)
                    new_courses_for_db.append(course_data.copy())

            # Save new courses to MongoDB in batches
            if new_courses_for_db:
                save_courses_to_mongodb(username, new_courses_for_db)
                new_courses_for_db = []

            with json_lock:
                asyncio.run_coroutine_threadsafe(save_to_json(filename, json_data), loop).result()
                offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()
                offsets[f"courses_{username}"] = offset + limit
                asyncio.run_coroutine_threadsafe(save_offsets(offsets), loop).result()
            
            del data, results
            gc.collect()
            offset += limit

        except requests.RequestException as e:
            logger.error(f"Failed to fetch courses for {username}: {e}")
            time.sleep(5)
            continue

def fetch_batches(username, known_educator_uids, limit=10, max_offset=1000, json_data=None, filename=filename, educators_only=False, start_offset=2):
    """Fetch batches and save new ones to MongoDB, optionally only for new educators."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    existing_batch_uids = get_existing_batch_uids(username) if not educators_only else set()
    new_educators = []
    new_batches_for_db = []
    new_educators_for_db = []
    
    if not educators_only:
        json_data["batches"] = json_data.get("batches", {})
        json_data["batches"][username] = json_data["batches"].get(username, [])
    
    offset = start_offset

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        try:
            response = requests.get(url, timeout=10, stream=True)
            response.raise_for_status()
            data = response.json()
            time.sleep(1)

            if isinstance(data, dict) and data.get("error_code") == "E001":
                logger.info(f"Error E001 encountered for batches of {username}. Stopping batch fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                logger.info(f"No valid batch results found for {username} at offset {offset}. Stopping batch fetch.")
                break

            if not results:
                logger.info(f"No more batches found for {username} at offset {offset}. Stopping batch fetch.")
                break

            for i, batch in enumerate(results, start=offset):
                batch_uid = batch.get("uid")
                if batch_uid and batch_uid not in existing_batch_uids:
                    existing_batch_uids.add(batch_uid)  # Add to local set to avoid duplicates in same batch
                    
                    if not educators_only:
                        batch_data = {
                            "name": batch.get("name", "N/A"),
                            "cover_photo": batch.get("cover_photo", "N/A"),
                            "exam_type": batch.get("goal", {}).get("name", "N/A"),
                            "uid": batch_uid,
                            "slug": batch.get("slug", "N/A"),
                            "syllabus_tag": batch.get("syllabus_tag", "N/A"),
                            "starts_at": batch.get("starts_at", "N/A"),
                            "completed_at": batch.get("completed_at", "N/A")
                        }
                        
                        print(f"{i} Batch Name :- {batch.get('name', 'N/A')}")
                        print(f"Uid :- {batch_uid}")
                        print("----------------------")
                        
                        json_data["batches"][username].append(batch_data)
                        new_batches_for_db.append(batch_data.copy())

                # Check for new educators in batch authors
                authors = batch.get("authors", [])
                for author in authors:
                    author_uid = author.get("uid")
                    if author_uid and author_uid not in known_educator_uids:
                        known_educator_uids.add(author_uid)
                        
                        educator_data = {
                            "first_name": author.get("first_name", "N/A"),
                            "last_name": author.get("last_name", "N/A"),
                            "username": author.get("username", "N/A"),
                            "uid": author_uid,
                            "avatar": author.get("avatar", "N/A")
                        }
                        
                        new_educators.append(educator_data)
                        json_data["educators"].append(educator_data)
                        new_educators_for_db.append(educator_data.copy())
                        print(f"New educator from batch: {author.get('first_name')} {author.get('last_name')} : {author.get('username')} : {author_uid}")

            # Save new data to MongoDB in batches
            if new_batches_for_db:
                save_batches_to_mongodb(username, new_batches_for_db)
                new_batches_for_db = []
            
            if new_educators_for_db:
                save_educators_to_mongodb(new_educators_for_db)
                new_educators_for_db = []

            with json_lock:
                asyncio.run_coroutine_threadsafe(save_to_json(filename, json_data), loop).result()
                offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()
                offsets[f"batches_{username}"] = offset + limit
                asyncio.run_coroutine_threadsafe(save_offsets(offsets), loop).result()
            
            del data, results
            gc.collect()
            offset += limit

        except requests.RequestException as e:
            logger.error(f"Failed to fetch batches for {username}: {e}")
            time.sleep(5)
            continue

    return new_educators

async def send_progress_bar(educator_count, course_count, batch_count, mode):
    """Send or update the progress bar message with MongoDB counts."""
    global progress_message, update_obj, update_context
    memory_percent = psutil.virtual_memory().percent
    db_educator_count, db_course_count, db_batch_count = get_mongodb_counts()
    
    if mode == "educators":
        progress_text = (
            "📊 *Progress Bar*\n"
            f"Session Educators Fetched: {educator_count}\n"
            f"Total Educators in DB: {db_educator_count}\n"
            f"Memory Usage: {memory_percent:.1f}%"
        )
    else:  # mode == "now"
        progress_text = (
            "📊 *Progress Bar*\n"
            f"Session Educators: {educator_count} | DB Total: {db_educator_count}\n"
            f"Session Courses: {course_count} | DB Total: {db_course_count}\n"
            f"Session Batches: {batch_count} | DB Total: {db_batch_count}\n"
            f"Memory Usage: {memory_percent:.1f}%"
        )
    
    try:
        if progress_message is None:
            progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")
        else:
            await progress_message.edit_text(progress_text, parse_mode="Markdown")
        logger.info("Progress bar updated successfully")
    except Exception as e:
        logger.error(f"Error updating progress bar: {e}")
        progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")

def split_json_file(filename, max_size=MAX_FILE_SIZE):
    """Split JSON file into chunks if larger than max_size."""
    try:
        if not os.path.exists(filename) or os.path.getsize(filename) <= max_size:
            return [filename]
        
        chunks = []
        chunk_index = 0
        current_chunk = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
        current_size = 0

        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Split educators
        for educator in data.get("educators", []):
            educator_size = len(json.dumps(educator, ensure_ascii=False).encode('utf-8'))
            if current_size + educator_size > max_size:
                chunk_filename = f"funkabhosda_part_{chunk_index}.json"
                asyncio.run_coroutine_threadsafe(save_to_json(chunk_filename, current_chunk), loop).result()
                chunks.append(chunk_filename)
                chunk_index += 1
                current_chunk = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
                current_size = 0
            current_chunk["educators"].append(educator)
            current_size += educator_size

        if fetch_mode == "now":
            # Split courses and batches (keeping the same logic as original)
            for username, courses in data.get("courses", {}).items():
                current_chunk["courses"][username] = []
                for course in courses:
                    course_size = len(json.dumps(course, ensure_ascii=False).encode('utf-8'))
                    if current_size + course_size > max_size:
                        chunk_filename = f"funkabhosda_part_{chunk_index}.json"
                        asyncio.run_coroutine_threadsafe(save_to_json(chunk_filename, current_chunk), loop).result()
                        chunks.append(chunk_filename)
                        chunk_index += 1
                        current_chunk = {"educators": [], "courses": {}, "batches": {}}
                        current_size = 0
                    current_chunk["courses"].setdefault(username, []).append(course)
                    current_size += course_size

            for username, batches in data.get("batches", {}).items():
                current_chunk["batches"][username] = []
                for batch in batches:
                    batch_size = len(json.dumps(batch, ensure_ascii=False).encode('utf-8'))
                    if current_size + batch_size > max_size:
                        chunk_filename = f"funkabhosda_part_{chunk_index}.json"
                        asyncio.run_coroutine_threadsafe(save_to_json(chunk_filename, current_chunk), loop).result()
                        chunks.append(chunk_filename)
                        chunk_index += 1
                        current_chunk = {"educators": [], "courses": {}, "batches": {}}
                        current_size = 0
                    current_chunk["batches"].setdefault(username, []).append(batch)
                    current_size += batch_size

        # Save the last chunk
        if current_chunk.get("educators") or (fetch_mode == "now" and (current_chunk.get("courses") or current_chunk.get("batches"))):
            chunk_filename = f"funkabhosda_part_{chunk_index}.json"
            asyncio.run_coroutine_threadsafe(save_to_json(chunk_filename, current_chunk), loop).result()
            chunks.append(chunk_filename)

        return chunks
    except Exception as e:
        logger.error(f"Error splitting JSON: {e}")
        return [filename]

async def upload_json():
    """Upload JSON file(s) to Telegram and clear them."""
    global update_context, update_obj, uploaded_file_ids
    max_retries = 3
    retry_delay = 5
    uploaded_file_ids = []

    try:
        if not os.path.exists(filename):
            error_msg = f"Error: {filename} file not found."
            logger.error(error_msg)
            await update_obj.message.reply_text(error_msg)
            return

        # Split JSON if needed
        chunks = split_json_file(filename)
        for chunk_file in chunks:
            for attempt in range(max_retries):
                try:
                    async with aiofiles.open(chunk_file, "rb") as f:
                        file_content = await f.read()
                    
                    message = await update_context.bot.send_document(
                        chat_id=update_obj.effective_chat.id,
                        document=file_content,
                        filename=os.path.basename(chunk_file),
                        caption=f"Updated {os.path.basename(chunk_file)}"
                    )
                    uploaded_file_ids.append(message.document.file_id)
                    logger.info(f"Uploaded {chunk_file} with file_id: {message.document.file_id}")
                    break
                except Exception as e:
                    error_msg = f"Error uploading {chunk_file} on attempt {attempt + 1}: {str(e)}"
                    logger.error(error_msg)
                    if attempt < max_retries - 1:
                        logger.info(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    else:
                        await update_obj.message.reply_text(error_msg)

        # Clear files
        for chunk_file in chunks:
            try:
                os.remove(chunk_file)
                logger.info(f"Cleared {chunk_file} from server")
            except Exception as e:
                logger.error(f"Error clearing {chunk_file}: {e}")

    except Exception as e:
        logger.error(f"Error in upload_json: {e}")

def count_items(filename, mode):
    """Count items in JSON with minimal memory usage using ijson."""
    try:
        if not os.path.exists(filename):
            logger.info(f"{filename} not found for counting")
            return 0, 0, 0
        
        educator_count = 0
        course_count = 0
        batch_count = 0
        with open(filename, 'rb') as f:
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                if prefix == "educators.item" and event == "end_map":
                    educator_count += 1
                if mode == "now":
                    if prefix.startswith("courses.") and prefix.endswith(".item") and event == "end_map":
                        course_count += 1
                    if prefix.startswith("batches.") and prefix.endswith(".item") and event == "end_map":
                        batch_count += 1
        
        logger.info(f"Session counts: Educators={educator_count}, Courses={course_count}, Batches={batch_count}")
        gc.collect()
        return educator_count, course_count, batch_count
    except Exception as e:
        logger.error(f"Error counting items: {e}")
        return 0, 0, 0

async def progress_updater():
    """Update progress bar every 30 seconds and upload JSON every 10 minutes."""
    global last_educator_count, last_course_count, last_batch_count, fetch_mode
    last_upload_time = time.time()
    
    while fetching:
        try:
            educator_count, course_count, batch_count = count_items(filename, fetch_mode)
            
            if (educator_count != last_educator_count or
                (fetch_mode == "now" and (course_count != last_course_count or batch_count != last_batch_count))):
                last_educator_count, last_course_count, last_batch_count = educator_count, course_count, batch_count
                await send_progress_bar(educator_count, course_count, batch_count, fetch_mode)
            
            current_time = time.time()
            if current_time - last_upload_time >= 600:  # 10 minutes
                await upload_json()
                last_upload_time = current_time
                # Initialize new JSON
                with json_lock:
                    initial_data = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
                    asyncio.run_coroutine_threadsafe(save_to_json(filename, initial_data), loop).result()
                
        except Exception as e:
            logger.error(f"Error in progress updater: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(30)

def fetch_data_in_background():
    """Run the fetching process in a background thread for /now."""
    global fetching, progress_message, fetch_mode, uploaded_file_ids
    
    json_data = {"educators": [], "courses": {}, "batches": {}}
    # ALWAYS start fresh - get existing UIDs from MongoDB
    known_educator_uids = get_existing_educator_uids()
    chunk_size = 5
    offsets = {"educators": 0}  # Always start from 0

    while fetching:
        logger.info(f"Memory before cycle: {psutil.virtual_memory().percent:.1f}%")
        educators = fetch_educators(json_data=json_data, known_educator_uids=known_educator_uids, start_offset=0)

        educator_queue = [(username, uid) for username, uid in educators]
        processed_educators = set()

        while educator_queue and fetching:
            current_educators = educator_queue[:chunk_size]
            educator_queue = educator_queue[chunk_size:]
            logger.info(f"\nProcessing chunk of {len(current_educators)} educators...")

            for username, uid in current_educators:
                if not fetching:
                    break
                if username in processed_educators:
                    continue
                processed_educators.add(username)

                logger.info(f"\nFetching courses for {username}...")
                fetch_courses(username, json_data=json_data, start_offset=1)
                time.sleep(1)

                logger.info(f"\nFetching batches for {username}...")
                new_educators = fetch_batches(username, known_educator_uids, json_data=json_data, start_offset=2)
                time.sleep(1)

                if new_educators:
                    logger.info(f"\nNew educators found in batches for {username}:")
                    for educator in new_educators:
                        logger.info(f"{educator['first_name']} {educator['last_name']} : {educator['username']} : {educator['uid']}")
                        educator_queue.append((educator["username"], educator["uid"]))

            del current_educators
            gc.collect()
            logger.info(f"Memory after chunk: {psutil.virtual_memory().percent:.1f}%")

        if fetching:
            logger.info("\nCompleted one full cycle. Restarting fetch for new data...")
            time.sleep(60)
        else:
            break

    logger.info("\nFetching stopped.")
    asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
    asyncio.run_coroutine_threadsafe(
        update_obj.message.reply_text(f"Fetching stopped. Final {filename} uploaded."), loop).result()
    
    fetching = False
    uploaded_file_ids = []
    progress_message = None

def fetch_educators_in_background():
    """Run the educator fetching process in a background thread for /educators."""
    global fetching, progress_message, fetch_mode, uploaded_file_ids
    
    json_data = {"educators": []}
    # ALWAYS start fresh - get existing UIDs from MongoDB
    known_educator_uids = get_existing_educator_uids()
    chunk_size = 5
    offsets = {"educators": 0}  # Always start from 0

    while fetching:
        logger.info(f"Memory before cycle: {psutil.virtual_memory().percent:.1f}%")
        educators = fetch_educators(json_data=json_data, known_educator_uids=known_educator_uids, start_offset=0)

        educator_queue = [(username, uid) for username, uid in educators]
        processed_educators = set()

        while educator_queue and fetching:
            current_educators = educator_queue[:chunk_size]
            educator_queue = educator_queue[chunk_size:]
            logger.info(f"\nProcessing chunk of {len(current_educators)} educators...")

            for username, uid in current_educators:
                if not fetching:
                    break
                if username in processed_educators:
                    continue
                processed_educators.add(username)

                logger.info(f"\nFetching batches for new educators from {username}...")
                new_educators = fetch_batches(username, known_educator_uids, json_data=json_data, educators_only=True, start_offset=2)
                time.sleep(1)

                if new_educators:
                    logger.info(f"\nNew educators found in batches for {username}:")
                    for educator in new_educators:
                        educator_queue.append((educator["username"], educator["uid"]))

            del current_educators
            gc.collect()
            logger.info(f"Memory after chunk: {psutil.virtual_memory().percent:.1f}%")

        if fetching:
            logger.info("\nCompleted one full cycle. Restarting fetch for new data...")
            time.sleep(60)
        else:
            break

    logger.info("\nFetching stopped.")
    asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
    asyncio.run_coroutine_threadsafe(
        update_obj.message.reply_text(f"Fetching stopped. Final {filename} uploaded."), loop).result()
    
    fetching = False
    uploaded_file_ids = []
    progress_message = None

async def now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /now command."""
    global fetching, update_context, update_obj, loop, fetch_mode, uploaded_file_ids
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return
    
    fetching = True
    fetch_mode = "now"
    update_context = context
    update_obj = update
    loop = asyncio.get_running_loop()
    uploaded_file_ids = []
    
    # Show MongoDB stats before starting
    db_educator_count, db_course_count, db_batch_count = get_mongodb_counts()
    start_msg = (
        f"Starting data fetch... ☠️\n"
        f"Current MongoDB stats:\n"
        f"📚 Educators: {db_educator_count}\n"
        f"📖 Courses: {db_course_count}\n"
        f"🎯 Batches: {db_batch_count}\n\n"
        f"Only new data will be added to MongoDB!"
    )
    await update.message.reply_text(start_msg)
    
    # Initialize JSON file
    with json_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({"educators": [], "courses": {}, "batches": {}}, f)
    
    # Clear offsets to start fresh
    await save_offsets({})
    
    asyncio.create_task(progress_updater())
    
    thread = Thread(target=fetch_data_in_background)
    thread.start()

async def educators_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /educators command."""
    global fetching, update_context, update_obj, loop, fetch_mode, uploaded_file_ids
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return
    
    fetching = True
    fetch_mode = "educators"
    update_context = context
    update_obj = update
    loop = asyncio.get_running_loop()
    uploaded_file_ids = []
    
    # Show MongoDB stats before starting
    db_educator_count, _, _ = get_mongodb_counts()
    start_msg = (
        f"Starting educator fetch... 😁\n"
        f"Current MongoDB stats:\n"
        f"📚 Educators: {db_educator_count}\n\n"
        f"Only new educators will be added to MongoDB!"
    )
    await update.message.reply_text(start_msg)
    
    # Initialize JSON file
    with json_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({"educators": []}, f)
    
    # Clear offsets to start fresh
    await save_offsets({})
    
    asyncio.create_task(progress_updater())
    
    thread = Thread(target=fetch_educators_in_background)
    thread.start()

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /stop command."""
    global fetching, progress_message, uploaded_file_ids
    if not fetching:
        await update.message.reply_text("No fetching process is running!")
        return
    
    fetching = False
    uploaded_file_ids = []
    progress_message = None
    await update.message.reply_text("Stopping fetching process...")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /stats command to show MongoDB statistics."""
    try:
        db_educator_count, db_course_count, db_batch_count = get_mongodb_counts()
        
        # Get some sample data
        sample_educator = educators_collection.find_one({}, {"first_name": 1, "last_name": 1, "username": 1, "_id": 0})
        sample_course = courses_collection.find_one({}, {"name": 1, "username": 1, "_id": 0})
        sample_batch = batches_collection.find_one({}, {"name": 1, "username": 1, "_id": 0})
        
        stats_text = (
            f"📊 *MongoDB Statistics*\n\n"
            f"📚 *Educators:* {db_educator_count:,}\n"
            f"📖 *Courses:* {db_course_count:,}\n"
            f"🎯 *Batches:* {db_batch_count:,}\n\n"
        )
        
        if sample_educator:
            stats_text += f"👨‍🏫 *Sample Educator:* {sample_educator.get('first_name', 'N/A')} {sample_educator.get('last_name', 'N/A')} (@{sample_educator.get('username', 'N/A')})\n"
        
        if sample_course:
            stats_text += f"📖 *Sample Course:* {sample_course.get('name', 'N/A')[:50]}...\n"
        
        if sample_batch:
            stats_text += f"🎯 *Sample Batch:* {sample_batch.get('name', 'N/A')[:50]}...\n"
        
        await update.message.reply_text(stats_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        await update.message.reply_text(f"Error getting statistics: {str(e)}")

async def clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /clear command to clear all MongoDB data (use with caution)."""
    if fetching:
        await update.message.reply_text("Cannot clear database while fetching is in progress. Stop fetching first.")
        return
    
    try:
        # Ask for confirmation
        confirmation_text = (
            "⚠️ *WARNING*\n\n"
            "This will delete ALL data from MongoDB:\n"
            f"• {educators_collection.count_documents({})} educators\n"
            f"• {courses_collection.count_documents({})} courses\n"
            f"• {batches_collection.count_documents({})} batches\n\n"
            "Send `/confirmclear` to confirm deletion."
        )
        await update.message.reply_text(confirmation_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error in clear command: {e}")
        await update.message.reply_text(f"Error: {str(e)}")

async def confirm_clear_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /confirmclear command to actually clear the database."""
    if fetching:
        await update.message.reply_text("Cannot clear database while fetching is in progress.")
        return
    
    try:
        # Delete all data
        educators_result = educators_collection.delete_many({})
        courses_result = courses_collection.delete_many({})
        batches_result = batches_collection.delete_many({})
        
        clear_text = (
            f"✅ *Database Cleared*\n\n"
            f"Deleted:\n"
            f"• {educators_result.deleted_count} educators\n"
            f"• {courses_result.deleted_count} courses\n"
            f"• {batches_result.deleted_count} batches"
        )
        await update.message.reply_text(clear_text, parse_mode="Markdown")
        
    except Exception as e:
        logger.error(f"Error clearing database: {e}")
        await update.message.reply_text(f"Error clearing database: {str(e)}")

async def main():
    """Start the Telegram bot."""
    bot_token = os.getenv("BOT_TOKEN", "7213717609:AAG4gF6dRvqxPcg-WaovRW2Eu1d5jxT566o")
    application = Application.builder().token(bot_token).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("educators", educators_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("clear", clear_command))
    application.add_handler(CommandHandler("confirmclear", confirm_clear_command))
    
    logger.info("Bot is starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    try:
        await asyncio.Event().wait()
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        client.close()  # Close MongoDB connection

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "Cannot close a running event loop" in str(e):
            logger.info("Event loop is running; skipping close.")
        else:
            raise
