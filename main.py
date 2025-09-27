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
uploaded_file_ids = []  # To store Telegram file_ids of uploaded chunks
fetch_mode = None  # 'now' or 'educators'
MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB per chunk

async def save_to_json(filename, data):
    """Save data to a JSON file with locking."""
    try:
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, indent=2, ensure_ascii=False))
        print(f"Saved data to {filename}")
        gc.collect()
    except Exception as e:
        print(f"Error saving JSON: {e}")

async def save_offsets(offsets):
    """Save offsets to track fetching progress."""
    try:
        async with aiofiles.open(offset_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(offsets, indent=2))
        print(f"Saved offsets to {offset_file}")
    except Exception as e:
        print(f"Error saving offsets: {e}")

async def load_offsets():
    """Load offsets from file."""
    try:
        if not os.path.exists(offset_file):
            return {}
        async with aiofiles.open(offset_file, 'r', encoding='utf-8') as f:
            return json.loads(await f.read())
    except Exception as e:
        print(f"Error loading offsets: {e}")
        return {}

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
            # Split courses
            for username, courses in data.get("courses", {}).items():
                current_chunk["courses"][username] = []
                for course in courses:
                    course_size = len(json.dumps(course, ensure_ascii=False).encode('utf-8'))
                    if current_size + course_size > max_size:
                        chunk_filename = f"funkabhosda_part_{chunk_index}.json"
                        asyncio.run_coroutine_threadsafe(save_to_json(chunk_filename, current_chunk), loop).result()
                        chunks.append(chunk_filename)
                        chunk_index += 1
                        current_chunk = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
                        current_size = 0
                    current_chunk["courses"].setdefault(username, []).append(course)
                    current_size += course_size

            # Split batches
            for username, batches in data.get("batches", {}).items():
                current_chunk["batches"][username] = []
                for batch in batches:
                    batch_size = len(json.dumps(batch, ensure_ascii=False).encode('utf-8'))
                    if current_size + batch_size > max_size:
                        chunk_filename = f"funkabhosda_part_{chunk_index}.json"
                        asyncio.run_coroutine_threadsafe(save_to_json(chunk_filename, current_chunk), loop).result()
                        chunks.append(chunk_filename)
                        chunk_index += 1
                        current_chunk = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
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
        print(f"Error splitting JSON: {e}")
        return [filename]

async def merge_json_files(existing_data, new_data, mode):
    """Merge new data into existing data, avoiding duplicates."""
    try:
        # Merge educators
        existing_uids = {e["uid"] for e in existing_data.get("educators", [])}
        for educator in new_data.get("educators", []):
            if educator["uid"] not in existing_uids:
                existing_data.setdefault("educators", []).append(educator)
                existing_uids.add(educator["uid"])

        if mode == "now":
            # Merge courses
            existing_data.setdefault("courses", {})
            for username, courses in new_data.get("courses", {}).items():
                existing_course_uids = {c["uid"] for c in existing_data["courses"].get(username, [])}
                for course in courses:
                    if course["uid"] not in existing_course_uids:
                        existing_data["courses"].setdefault(username, []).append(course)

            # Merge batches
            existing_data.setdefault("batches", {})
            for username, batches in new_data.get("batches", {}).items():
                existing_batch_uids = {b["uid"] for b in existing_data["batches"].get(username, [])}
                for batch in batches:
                    if batch["uid"] not in existing_batch_uids:
                        existing_data["batches"].setdefault(username, []).append(batch)

        return existing_data
    except Exception as e:
        print(f"Error merging JSON: {e}")
        return existing_data

async def download_and_merge_chunks():
    """Download all uploaded JSON chunks and merge them."""
    global uploaded_file_ids
    if not uploaded_file_ids:
        print("No previous file_ids to download")
        return {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}

    merged_data = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
    for file_id in uploaded_file_ids:
        try:
            file = await update_context.bot.get_file(file_id)
            file_content = await file.download_as_bytearray()
            chunk_data = json.loads(file_content.decode('utf-8'))
            merged_data = await merge_json_files(merged_data, chunk_data, fetch_mode)
            print(f"Merged chunk with file_id: {file_id}")
        except Exception as e:
            print(f"Error downloading/merging chunk {file_id}: {e}")
    
    await save_to_json(filename, merged_data)
    return merged_data

def count_items(filename, mode):
    """Count items in JSON with minimal memory usage using ijson."""
    try:
        if not os.path.exists(filename):
            print(f"{filename} not found for counting")
            return 0, 0, 0
        
        educator_count = 0
        course_count = 0
        batch_count = 0
        with open(filename, 'rb') as f:
            parser = ijson.parse(f)
            current_key = None
            for prefix, event, value in parser:
                if prefix == "educators.item" and event == "map_key":
                    educator_count += 1
                if mode == "now":
                    if prefix.startswith("courses.") and prefix.endswith(".item") and event == "map_key":
                        course_count += 1
                    if prefix.startswith("batches.") and prefix.endswith(".item") and event == "map_key":
                        batch_count += 1
        
        print(f"Counted: Educators={educator_count}, Courses={course_count}, Batches={batch_count}")
        gc.collect()
        return educator_count, course_count, batch_count
    except Exception as e:
        print(f"Error counting items: {e}")
        return 0, 0, 0

def fetch_educators(goal_uid="TMUVD", limit=10, max_offset=1000, json_data=None, filename=filename, known_educator_uids=None, start_offset=0):
    """Fetch educators with low memory."""
    base_url = "https://unacademy.com/api/v1/uplus/subscription/goal_educators/"
    seen_usernames = set()
    educators = []
    json_data["educators"] = json_data.get("educators", [])
    offset = start_offset

    while offset <= max_offset:
        url = f"{base_url}?goal_uid={goal_uid}&limit={limit}&offset={offset}"
        try:
            response = requests.get(url, timeout=10, stream=True)
            response.raise_for_status()
            data = response.json()
            time.sleep(1)

            if isinstance(data, dict) and data.get("error_code") == "E001":
                print("Error E001 encountered. Stopping educator fetch.")
                break

            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid educator results found at offset {offset}. Stopping educator fetch.")
                break

            for i, educator in enumerate(results, start=offset + 1):
                username = educator.get("username")
                uid = educator.get("uid")
                if username and uid not in known_educator_uids:
                    seen_usernames.add(username)
                    known_educator_uids.add(uid)
                    educators.append((username, uid))
                    print(f"{i} {educator.get('first_name')} {educator.get('last_name')} : {username} : {uid} : {educator.get('avatar')}")
                    json_data["educators"].append({
                        "first_name": educator.get("first_name", "N/A"),
                        "last_name": educator.get("last_name", "N/A"),
                        "username": username,
                        "uid": uid,
                        "avatar": educator.get("avatar", "N/A")
                    })

            with json_lock:
                asyncio.run_coroutine_threadsafe(save_to_json(filename, json_data), loop).result()
                offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()
                offsets["educators"] = offset + limit
                asyncio.run_coroutine_threadsafe(save_offsets(offsets), loop).result()
            del data, results
            gc.collect()
            offset += limit

        except requests.RequestException as e:
            print(f"Request failed for educators: {e}")
            time.sleep(5)
            continue

    return educators

def fetch_courses(username, limit=10, max_offset=1000, json_data=None, filename=filename, start_offset=1):
    """Fetch courses with low memory."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}&type=latest"
    seen_uids = set()
    json_data["courses"] = json_data.get("courses", {})
    json_data["courses"][username] = json_data["courses"].get(username, [])
    offset = start_offset

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        try:
            response = requests.get(url, timeout=10, stream=True)
            response.raise_for_status()
            data = response.json()
            time.sleep(1)

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

            with json_lock:
                asyncio.run_coroutine_threadsafe(save_to_json(filename, json_data), loop).result()
                offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()
                offsets[f"courses_{username}"] = offset + limit
                asyncio.run_coroutine_threadsafe(save_offsets(offsets), loop).result()
            del data, results
            gc.collect()
            offset += limit

        except requests.RequestException as e:
            print(f"Failed to fetch courses for {username}: {e}")
            time.sleep(5)
            continue

def fetch_batches(username, known_educator_uids, limit=10, max_offset=1000, json_data=None, filename=filename, educators_only=False, start_offset=2):
    """Fetch batches, optionally only for new educators."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    seen_batch_uids = set()
    new_educators = []
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
                    if not educators_only:
                        print(f"{i} Batch Name :- {batch.get('name', 'N/A')}")
                        print(f"Cover Photo :- {batch.get('cover_photo', 'N/A')}")
                        print(f"Exam Type :- {batch.get('goal', {}).get('name', 'N/A')}")
                        print(f"Uid :- {batch_uid}")
                        print(f"Slug :- {batch.get('slug', 'N/A')}")
                        print(f"Syllabus Tag :- {batch.get('syllabus_tag', 'N/A')}")
                        print(f"Starts At :- {batch.get('starts_at', 'N/A')}")
                        print(f"Completed At :- {batch.get('completed_at', 'N/A')}")
                        print("----------------------")
                        json_data["batches"][username].append({
                            "name": batch.get("name", "N/A"),
                            "cover_photo": batch.get("cover_photo", "N/A"),
                            "exam_type": batch.get("goal", {}).get("name", "N/A"),
                            "uid": batch_uid,
                            "slug": batch.get("slug", "N/A"),
                            "syllabus_tag": batch.get("syllabus_tag", "N/A"),
                            "starts_at": batch.get("starts_at", "N/A"),
                            "completed_at": batch.get("completed_at", "N/A")
                        })

                    authors = batch.get("authors", [])
                    for author in authors:
                        author_uid = author.get("uid")
                        if author_uid and author_uid not in known_educator_uids:
                            known_educator_uids.add(author_uid)
                            new_educators.append({
                                "first_name": author.get("first_name", "N/A"),
                                "last_name": author.get("last_name", "N/A"),
                                "username": author.get("username", "N/A"),
                                "uid": author_uid,
                                "avatar": author.get("avatar", "N/A")
                            })
                            json_data["educators"].append({
                                "first_name": author.get("first_name", "N/A"),
                                "last_name": author.get("last_name", "N/A"),
                                "username": author.get("username", "N/A"),
                                "uid": author_uid,
                                "avatar": author.get("avatar", "N/A")
                            })
                            print(f"New educator from batch: {author.get('first_name')} {author.get('last_name')} : {author.get('username')} : {author_uid}")

            with json_lock:
                asyncio.run_coroutine_threadsafe(save_to_json(filename, json_data), loop).result()
                offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()
                offsets[f"batches_{username}"] = offset + limit
                asyncio.run_coroutine_threadsafe(save_offsets(offsets), loop).result()
            del data, results
            gc.collect()
            offset += limit

        except requests.RequestException as e:
            print(f"Failed to fetch batches for {username}: {e}")
            time.sleep(5)
            continue

    return new_educators

async def send_progress_bar(educator_count, course_count, batch_count, mode):
    """Send or update the progress bar message."""
    global progress_message, update_obj, update_context
    memory_percent = psutil.virtual_memory().percent
    if mode == "educators":
        progress_text = (
            "📊 *Progress Bar*\n"
            f"Total Educators Fetched: {educator_count}\n"
            f"Memory Usage: {memory_percent:.1f}%"
        )
    else:  # mode == "now"
        progress_text = (
            "📊 *Progress Bar*\n"
            f"Total Educators Fetched: {educator_count}\n"
            f"Total Courses Fetched: {course_count}\n"
            f"Total Batches Fetched: {batch_count}\n"
            f"Memory Usage: {memory_percent:.1f}%"
        )
    
    try:
        if progress_message is None:
            progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")
        else:
            await progress_message.edit_text(progress_text, parse_mode="Markdown")
        print("Progress bar updated successfully")
    except Exception as e:
        print(f"Error updating progress bar: {e}")
        progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")

async def upload_json():
    """Upload JSON file(s) to Telegram and clear them."""
    global update_context, update_obj, uploaded_file_ids
    max_retries = 3
    retry_delay = 5
    uploaded_file_ids = []

    try:
        if not os.path.exists(filename):
            error_msg = f"Error: {filename} file not found."
            print(error_msg)
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
                    print(f"Uploaded {chunk_file} with file_id: {message.document.file_id}")
                    break
                except Exception as e:
                    error_msg = f"Error uploading {chunk_file} on attempt {attempt + 1}: {str(e)}"
                    print(error_msg)
                    if attempt < max_retries - 1:
                        print(f"Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                    else:
                        await update_obj.message.reply_text(error_msg)

        # Clear files
        for chunk_file in chunks:
            try:
                os.remove(chunk_file)
                print(f"Cleared {chunk_file} from server")
            except Exception as e:
                print(f"Error clearing {chunk_file}: {e}")

    except Exception as e:
        print(f"Error in upload_json: {e}")

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
                # Download and merge previous chunks
                await download_and_merge_chunks()
                # Initialize new JSON
                with json_lock:
                    initial_data = {"educators": [], "courses": {}, "batches": {}} if fetch_mode == "now" else {"educators": []}
                    asyncio.run_coroutine_threadsafe(save_to_json(filename, initial_data), loop).result()
                
        except Exception as e:
            print(f"Error in progress updater: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(30)

def fetch_data_in_background():
    """Run the fetching process in a background thread for /now."""
    global fetching, progress_message, fetch_mode, uploaded_file_ids
    
    json_data = {"educators": [], "courses": {}, "batches": {}}
    known_educator_uids = set()
    chunk_size = 5
    offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()

    while fetching:
        print(f"Memory before cycle: {psutil.virtual_memory().percent:.1f}%")
        start_offset = offsets.get("educators", 0)
        educators = fetch_educators(json_data=json_data, known_educator_uids=known_educator_uids, start_offset=start_offset)

        educator_queue = [(username, uid) for username, uid in educators]
        processed_educators = set()

        while educator_queue and fetching:
            current_educators = educator_queue[:chunk_size]
            educator_queue = educator_queue[chunk_size:]
            print(f"\nProcessing chunk of {len(current_educators)} educators...")

            for username, uid in current_educators:
                if not fetching:
                    break
                if username in processed_educators:
                    continue
                processed_educators.add(username)

                print(f"\nFetching courses for {username}...")
                start_offset = offsets.get(f"courses_{username}", 1)
                fetch_courses(username, json_data=json_data, start_offset=start_offset)
                time.sleep(1)

                print(f"\nFetching batches for {username}...")
                start_offset = offsets.get(f"batches_{username}", 2)
                new_educators = fetch_batches(username, known_educator_uids, json_data=json_data, start_offset=start_offset)
                time.sleep(1)

                if new_educators:
                    print(f"\nNew educators found in batches for {username}:")
                    for educator in new_educators:
                        print(f"{educator['first_name']} {educator['last_name']} : {educator['username']} : {educator['uid']} : {educator['avatar']}")
                        educator_queue.append((educator["username"], educator["uid"]))

            del current_educators
            gc.collect()
            print(f"Memory after chunk: {psutil.virtual_memory().percent:.1f}%")

        if fetching:
            print("\nCompleted one full cycle. Restarting fetch for new data...")
            time.sleep(60)
        else:
            break

    print("\nFetching stopped.")
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
    known_educator_uids = set()
    chunk_size = 5
    offsets = asyncio.run_coroutine_threadsafe(load_offsets(), loop).result()

    while fetching:
        print(f"Memory before cycle: {psutil.virtual_memory().percent:.1f}%")
        start_offset = offsets.get("educators", 0)
        educators = fetch_educators(json_data=json_data, known_educator_uids=known_educator_uids, start_offset=start_offset)

        educator_queue = [(username, uid) for username, uid in educators]
        processed_educators = set()

        while educator_queue and fetching:
            current_educators = educator_queue[:chunk_size]
            educator_queue = educator_queue[chunk_size:]
            print(f"\nProcessing chunk of {len(current_educators)} educators...")

            for username, uid in current_educators:
                if not fetching:
                    break
                if username in processed_educators:
                    continue
                processed_educators.add(username)

                print(f"\nFetching batches for new educators from {username}...")
                start_offset = offsets.get(f"batches_{username}", 2)
                new_educators = fetch_batches(username, known_educator_uids, json_data=json_data, educators_only=True, start_offset=start_offset)
                time.sleep(1)

                if new_educators:
                    print(f"\nNew educators found in batches for {username}:")
                    for educator in new_educators:
                        educator_queue.append((educator["username"], educator["uid"]))

            del current_educators
            gc.collect()
            print(f"Memory after chunk: {psutil.virtual_memory().percent:.1f}%")

        if fetching:
            print("\nCompleted one full cycle. Restarting fetch for new data...")
            time.sleep(60)
        else:
            break

    print("\nFetching stopped.")
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
    await update.message.reply_text("Starting data fetch... ☠️")
    
    with json_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({"educators": [], "courses": {}, "batches": {}}, f)
    
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
    await update.message.reply_text("Starting educator fetch... 😁")
    
    with json_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({"educators": []}, f)
    
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

async def main():
    """Start the Telegram bot."""
    bot_token = os.getenv("BOT_TOKEN", "7862470692:AAH_mtJsMyew7sKEpV77sG10Yh9uaOar83c")
    application = Application.builder().token(bot_token).build()
    
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("educators", educators_command))
    application.add_handler(CommandHandler("stop", stop_command))
    
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
