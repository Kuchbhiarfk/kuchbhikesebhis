import aiohttp
import asyncio
import json
import os
import gc
from telegram import Update
from telegram.ext import Application, CommandHandler, ConversationHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest, RetryAfter, TimedOut, NetworkError
import re
from datetime import datetime, timedelta
import dateutil.parser
import pytz
import pymongo
from bson import ObjectId

# Telegram group ID and channel ID
SETTED_GROUP_ID = -1003133358948
FORWARD_CHANNEL_ID = 2927760779  # Set this to your channel ID (without -100)

# MongoDB connections
client_original = pymongo.MongoClient("mongodb+srv://elvishyadav_opm:naman1811421@cluster0.uxuplor.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db_original = client_original["unacademy_db"]
educators_col = db_original["educators"]

client_optry = pymongo.MongoClient(os.environ.get('MONGODB_URI', 'mongodb+srv://elvishyadavop:ClA5yIHTbCutEnVP@cluster0.u83zlfx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0'))
db_optry = client_optry['unacademy_db']
collection_optry = db_optry['educators']

# Global variables
bot = None
progress_message = None
update_context = None
update_obj = None
scheduler_progress_messages = {}
optry_progress_message = None
schedule_running = False
optry_offset = 0  # Track position for Next 10

SELECT_TYPE, ENTER_ID, OPTRY_SELECT = range(3)

def save_to_json(filename, data):
    """Save data to a JSON file with minimal memory footprint."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving JSON {filename}: {e}")
        raise

async def fetch_educator_by_username(username):
    """Fetch educator details by username from course API."""
    url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit=1"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    print(f"Rate limited for {username}. Retrying after {retry_after} seconds...")
                    await asyncio.sleep(retry_after)
                    return await fetch_educator_by_username(username)
                response.raise_for_status()
                data = await response.json()
                
                results = data.get("results")
                if results and isinstance(results, list) and len(results) > 0:
                    author = results[0].get("author")
                    if author:
                        return {
                            "first_name": author.get("first_name", "N/A"),
                            "last_name": author.get("last_name", "N/A"),
                            "username": author.get("username", "N/A"),
                            "uid": author.get("uid", "N/A"),
                            "avatar": author.get("avatar", "N/A")
                        }
                print(f"No courses found for username: {username}")
                return None
        except aiohttp.ClientError as e:
            print(f"Failed to fetch educator details for {username}: {e}")
            return None

async def fetch_courses(username, limit=50, max_offset=10000):
    """Fetch courses for a given username asynchronously."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}"
    courses = []
    async with aiohttp.ClientSession() as session:
        seen_uids = set()
        offset = 0
        consecutive_empty = 0
        max_consecutive_empty = 3
        
        while offset <= max_offset:
            url = f"{base_url}&offset={offset}"
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        print(f"Rate limited for courses of {username}. Retrying after {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    
                    if isinstance(data, dict) and data.get("error_code") == "E001":
                        print(f"Error E001 encountered for courses of {username}.")
                        break
                    
                    results = data.get("results")
                    if results is None or not isinstance(results, list):
                        consecutive_empty += 1
                        if consecutive_empty >= max_consecutive_empty:
                            break
                        offset += limit
                        await asyncio.sleep(0.1)
                        continue
                    
                    if not results:
                        consecutive_empty += 1
                        if consecutive_empty >= max_consecutive_empty:
                            break
                        offset += limit
                        await asyncio.sleep(0.1)
                        continue
                    
                    consecutive_empty = 0
                    for course in results:
                        course_uid = course.get("uid")
                        if course_uid and course_uid not in seen_uids:
                            seen_uids.add(course_uid)
                            courses.append({
                                "name": course.get("name", "N/A"),
                                "slug": course.get("slug", "N/A"),
                                "thumbnail": course.get("thumbnail", "N/A"),
                                "uid": course_uid,
                                "starts_at": course.get("starts_at", "N/A"),
                                "ends_at": course.get("ends_at", "N/A"),
                                "author": course.get("author", {})
                            })
                    
                    offset += limit
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                print(f"Failed to fetch courses for {username} at offset {offset}: {e}")
                offset += limit
                await asyncio.sleep(1)
                continue
        
        print(f"Total courses fetched for {username}: {len(courses)}")
        return courses

async def fetch_batches(username, limit=50, max_offset=10000):
    """Fetch batches for a given username asynchronously."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    batches = []
    async with aiohttp.ClientSession() as session:
        seen_batch_uids = set()
        offset = 0
        consecutive_empty = 0
        max_consecutive_empty = 3
        
        while offset <= max_offset:
            url = f"{base_url}&offset={offset}"
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        print(f"Rate limited for batches of {username}. Retrying after {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    
                    if isinstance(data, dict) and data.get("error_code") == "E001":
                        print(f"Error E001 encountered for batches of {username}.")
                        break
                    
                    results = data.get("results")
                    if results is None or not isinstance(results, list):
                        consecutive_empty += 1
                        if consecutive_empty >= max_consecutive_empty:
                            break
                        offset += limit
                        await asyncio.sleep(0.1)
                        continue
                    
                    if not results:
                        consecutive_empty += 1
                        if consecutive_empty >= max_consecutive_empty:
                            break
                        offset += limit
                        await asyncio.sleep(0.1)
                        continue
                    
                    consecutive_empty = 0
                    for batch in results:
                        batch_uid = batch.get("uid")
                        if batch_uid and batch_uid not in seen_batch_uids:
                            seen_batch_uids.add(batch_uid)
                            batches.append({
                                "name": batch.get("name", "N/A"),
                                "cover_photo": batch.get("cover_photo", "N/A"),
                                "exam_type": batch.get("goal", {}).get("name", "N/A"),
                                "uid": batch_uid,
                                "slug": batch.get("slug", "N/A"),
                                "syllabus_tag": batch.get("syllabus_tag", "N/A"),
                                "starts_at": batch.get("starts_at", "N/A"),
                                "completed_at": batch.get("completed_at", "N/A"),
                                "authors": batch.get("authors", [])
                            })
                    
                    offset += limit
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                print(f"Failed to fetch batches for {username} at offset {offset}: {e}")
                offset += limit
                await asyncio.sleep(1)
                continue
        
        print(f"Total batches fetched for {username}: {len(batches)}")
        return batches

async def fetch_unacademy_schedule(schedule_url, item_type, item_data):
    """Fetch schedule for a batch or course with retry."""
    async with aiohttp.ClientSession() as session:
        for attempt in range(20):
            results_list = []
            try:
                timeout = aiohttp.ClientTimeout(total=30)
                async with session.get(schedule_url, timeout=timeout) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    results = data.get('results', [])
                    
                    if not results:
                        return [], None
                    
                    current_time = datetime.now(pytz.UTC)
                    
                    if item_type == 'course':
                        for item in results:
                            value = item.get("value", {})
                            uid = value.get("uid", None)
                            if not uid:
                                continue
                            results_list.append(fetch_unacademy_collection(
                                value.get("title", "N/A"),
                                value.get("live_class", {}).get("author", {}),
                                value.get("live_class", {}).get("live_at", "N/A"),
                                value.get("live_class", {}).get("video_url"),
                                value.get("live_class", {}).get("slides_pdf", {}),
                                value.get("is_offline", "N/A")
                            ))
                    else:
                        async def fetch_collection_item(item):
                            properties = item.get('properties', {})
                            author = properties.get('author', {})
                            permalink = properties.get('permalink', '')
                            data_id_match = re.search(r'/course/[^/]+/([A-Z0-9]+)', permalink)
                            data_id = data_id_match.group(1) if data_id_match else None
                            uid = properties.get('uid', None)
                            live_at = properties.get('live_at', 'N/A')
                            
                            if not data_id or not uid:
                                return None
                            
                            collection_url = f"https://unacademy.com/api/v3/collection/{data_id}/items?limit=10000"
                            for retry in range(3):
                                try:
                                    async with session.get(collection_url, timeout=timeout) as collection_response:
                                        if collection_response.status == 429:
                                            retry_after = int(collection_response.headers.get("Retry-After", 5))
                                            await asyncio.sleep(retry_after)
                                            continue
                                        collection_response.raise_for_status()
                                        collection_data = await collection_response.json()
                                        items = collection_data.get("results", [])
                                        for collection_item in items:
                                            value = collection_item.get("value", {})
                                            if value.get("uid") == uid:
                                                return fetch_unacademy_collection(
                                                    value.get("title", properties.get('name', 'N/A')),
                                                    value.get("live_class", {}).get("author", author),
                                                    value.get("live_at", live_at),
                                                    value.get("live_class", {}).get("video_url"),
                                                    value.get("live_class", {}).get("slides_pdf", {}),
                                                    value.get("is_offline", "N/A")
                                                )
                                        return None
                                except:
                                    if retry < 2:
                                        await asyncio.sleep(2 ** retry)
                                        continue
                                    return handle_collection_failure(live_at, properties.get('name', 'N/A'), author)
                            return None
                        
                        tasks = [fetch_collection_item(item) for item in results]
                        collection_results = await asyncio.gather(*tasks, return_exceptions=True)
                        results_list.extend([r for r in collection_results if r is not None and not isinstance(r, Exception)])
                    
                    results_list = [r for r in results_list if r]
                    results_list.sort(key=lambda x: x.get("live_at_time") or datetime.min.replace(tzinfo=pytz.UTC).isoformat(), reverse=True)
                    
                    del data
                    del results
                    
                    return results_list, None
            
            except Exception as e:
                print(f"Error in schedule API (attempt {attempt + 1}/20): {e}")
                await asyncio.sleep(2 ** min(attempt, 5))
        
        return [], None

def fetch_unacademy_collection(title, author, live_at, video_url, slides_pdf, is_offline):
    """Format collection item details."""
    current_time = datetime.now(pytz.UTC)
    live_at_time = None
    if live_at != "N/A":
        try:
            live_at_time = datetime.strptime(live_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
        except ValueError:
            live_at_time = None
    
    class_url = "N/A"
    slides_url = slides_pdf.get('with_annotation', 'N/A') if slides_pdf else "N/A"
    if live_at_time:
        if live_at_time < current_time:
            if not video_url and not (slides_pdf and slides_pdf.get('with_annotation', None)):
                class_url = "Class Cancelled"
                slides_url = "Class Cancelled"
            elif isinstance(video_url, str):
                match = re.search(r"uid=([A-Z0-9]+)", video_url)
                if match:
                    vid = match.group(1)
                    class_url = f"https://uamedia.uacdn.net/lesson-raw/{vid}/output.webm"
        else:
            class_url = "Live Soon"
            slides_url = "Live Soon"
    else:
        if isinstance(video_url, str):
            match = re.search(r"uid=([A-Z0-9]+)", video_url)
            if match:
                vid = match.group(1)
                class_url = f"https://uamedia.uacdn.net/lesson-raw/{vid}/output.webm"
        else:
            class_url = f"Live At: {live_at}"
    
    live_at_time_str = live_at_time.isoformat() if live_at_time else "N/A"
    
    return {
        "class_name": title,
        "teacher_name": f"{author.get('first_name', '')} {author.get('last_name', '')}".strip(),
        "live_at": live_at,
        "thumbnail": author.get('avatar', 'N/A'),
        "class_url": class_url,
        "slides_url": slides_url,
        "is_offline": is_offline,
        "live_at_time": live_at_time_str
    }

def handle_collection_failure(live_at, class_name, author):
    """Handle collection API failure."""
    current_time = datetime.now(pytz.UTC)
    class_url = "N/A"
    slides_url = "N/A"
    live_at_time = None
    
    if live_at != "N/A":
        try:
            live_at_time = datetime.strptime(live_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
            if live_at_time < current_time:
                class_url = "Class Cancelled"
                slides_url = "Class Cancelled"
            else:
                class_url = "Live Soon"
                slides_url = "Live Soon"
        except ValueError:
            class_url = f"Live At: {live_at}"
            slides_url = "N/A"
    
    live_at_time_str = live_at_time.isoformat() if live_at_time else "N/A"
    
    return {
        "class_name": class_name,
        "teacher_name": f"{author.get('first_name', '')} {author.get('last_name', '')}".strip(),
        "live_at": live_at,
        "thumbnail": author.get('avatar', 'N/A'),
        "class_url": class_url,
        "slides_url": slides_url,
        "is_offline": "N/A",
        "live_at_time": live_at_time_str
    }

def normalize_username(username):
    """Normalize username to lowercase and remove special characters."""
    return re.sub(r'[^a-zA-Z0-9]', '', username).lower()

def filter_by_time(courses, batches, current_time, future=True):
    """Filter courses and batches based on time (future or past)."""
    filtered_courses = []
    filtered_batches = []
    
    for course in courses:
        ends_at = course.get("ends_at")
        if ends_at and ends_at != "N/A":
            try:
                end_time = dateutil.parser.isoparse(ends_at)
                if end_time.year > 2035:
                    if not future:
                        filtered_courses.append(course)
                elif (future and end_time > current_time) or (not future and end_time <= current_time):
                    filtered_courses.append(course)
            except ValueError:
                continue
    
    for batch in batches:
        completed_at = batch.get("completed_at")
        if completed_at and completed_at != "N/A":
            try:
                complete_time = dateutil.parser.isoparse(completed_at)
                if complete_time.year > 2035:
                    if not future:
                        filtered_batches.append(batch)
                elif (future and complete_time > current_time) or (not future and complete_time <= current_time):
                    filtered_batches.append(batch)
            except ValueError:
                continue
    
    return filtered_courses, filtered_batches

def is_educator_fetched(username):
    """Check if educator has been fetched (has any msg_id in courses or batches)."""
    doc = educators_col.find_one({"username": normalize_username(username)})
    if not doc:
        return False
    
    # Check if any course or batch has msg_id
    courses = doc.get("courses", [])
    batches = doc.get("batches", [])
    
    for course in courses:
        if course.get("msg_id"):
            return True
    
    for batch in batches:
        if batch.get("msg_id"):
            return True
    
    return False

def get_next_pending_educators(offset, limit=10):
    """Get next batch of pending educators from optry MongoDB."""
    # Get all educator UIDs that are already in main DB
    fetched_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}
    
    # Get educators from optry that are NOT in main DB
    pending_educators = list(collection_optry.find(
        {"uid": {"$nin": list(fetched_uids)}}
    ).skip(offset).limit(limit))
    
    # Also check educators in main DB but not yet fetched
    partially_fetched = []
    for doc in educators_col.find().skip(0).limit(1000):  # Check reasonable amount
        if not is_educator_fetched(doc.get("username", "")):
            partially_fetched.append({
                "uid": doc.get("uid"),
                "username": doc.get("username"),
                "first_name": doc.get("first_name", "N/A"),
                "last_name": doc.get("last_name", "N/A"),
                "avatar": doc.get("avatar", "N/A")
            })
    
    # Combine and return
    all_pending = pending_educators + partially_fetched
    return all_pending[:limit]

async def send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches, current_phase):
    """Send or update progress bar for /add command."""
    global progress_message, update_obj
    
    if current_phase == "courses":
        progress_text = (
            f"Phase 1: Uploading Courses\n"
            f"Progress: {uploaded_courses}/{total_courses}\n"
            f"Batches: Pending..."
        )
    elif current_phase == "batches":
        progress_text = (
            f"Phase 1: Courses Complete\n"
            f"Phase 2: Uploading Batches\n"
            f"Progress: {uploaded_batches}/{total_batches}"
        )
    else:
        progress_text = (
            f"Upload Complete!\n"
            f"Courses: {uploaded_courses}/{total_courses}\n"
            f"Batches: {uploaded_batches}/{total_batches}"
        )
    
    if progress_message is None:
        try:
            progress_message = await update_obj.message.reply_text(progress_text)
        except Exception as e:
            print(f"Error sending progress bar: {e}")
    else:
        try:
            await progress_message.edit_text(progress_text)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                print(f"BadRequest editing progress: {e}")
        except Exception as e:
            print(f"Error editing progress bar: {e}")

async def progress_updater_add(total_courses, total_batches, get_uploaded_courses, get_uploaded_batches, phase_tracker):
    """Update progress bar for /add every 30 seconds."""
    global progress_message
    try:
        while True:
            uploaded_courses = get_uploaded_courses()
            uploaded_batches = get_uploaded_batches()
            current_phase = phase_tracker.get('phase', 'courses')
            
            if current_phase == 'courses' and uploaded_courses >= total_courses:
                phase_tracker['phase'] = 'batches'
            elif current_phase == 'batches' and uploaded_batches >= total_batches:
                phase_tracker['phase'] = 'complete'
                break
            
            await send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches, current_phase)
            await asyncio.sleep(30)
    except asyncio.CancelledError:
        pass

async def send_scheduler_progress(username, thread_id, total_courses, total_batches, new_courses, new_batches, checked_courses, checked_batches, current_phase):
    """Send or update progress bar for schedule checker in group subtopic."""
    global scheduler_progress_messages
    
    if current_phase == "courses":
        progress_text = (
            f"📊 Schedule Checking\n\n"
            f"👨‍🏫 Fetching Teacher: {username}\n\n"
            f"Total Batches: {checked_batches}/{total_batches}\n"
            f"Total Courses: {checked_courses}/{total_courses}\n\n"
            f"Phase 1: Checking Courses\n"
            f"Progress: {checked_courses}/{total_courses}\n\n"
            f"Total New Batches: {new_batches}\n"
            f"Total New Courses: {new_courses}"
        )
    elif current_phase == "batches":
        progress_text = (
            f"📊 Schedule Checking\n\n"
            f"👨‍🏫 Fetching Teacher: {username}\n\n"
            f"Total Batches: {checked_batches}/{total_batches}\n"
            f"Total Courses: {checked_courses}/{total_courses}\n\n"
            f"Phase 1: Courses Complete ✓\n"
            f"Phase 2: Checking Batches\n"
            f"Progress: {checked_batches}/{total_batches}\n\n"
            f"Total New Batches: {new_batches}\n"
            f"Total New Courses: {new_courses}"
        )
    else:
        progress_text = (
            f"✅ Schedule Check Complete!\n\n"
            f"👨‍🏫 Teacher: {username}\n\n"
            f"Total Batches Checked: {checked_batches}/{total_batches}\n"
            f"Total Courses Checked: {checked_courses}/{total_courses}\n\n"
            f"Total New Batches: {new_batches}\n"
            f"Total New Courses: {new_courses}"
        )
    
    progress_key = f"{username}_{thread_id}"
    
    if progress_key not in scheduler_progress_messages or scheduler_progress_messages[progress_key] is None:
        try:
            msg = await bot.send_message(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                text=progress_text
            )
            scheduler_progress_messages[progress_key] = msg
        except Exception as e:
            print(f"Error sending scheduler progress: {e}")
    else:
        try:
            await scheduler_progress_messages[progress_key].edit_text(progress_text)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                print(f"BadRequest editing scheduler progress: {e}")
        except Exception as e:
            print(f"Error editing scheduler progress: {e}")

async def add_educator_if_not_exists(uid, username, first_name, last_name, avatar):
    """Add educator to optry MongoDB if not exists."""
    try:
        if not collection_optry.find_one({"uid": uid}):
            collection_optry.insert_one({
                "uid": uid,
                "username": normalize_username(username),
                "avatar": avatar,
                "first_name": first_name,
                "last_name": last_name
            })
            print(f"✅ Added new educator: {username} (UID: {uid})")
            return True
        return False
    except Exception as e:
        print(f"Error adding educator {username}: {e}")
        return False

async def schedule_checker():
    """Check and update current batches and courses every 12 hours."""
    global schedule_running
    
    # Wait for 12 hours before first check
    print(f"\n{'='*60}")
    print(f"Bot started. Waiting 12 hours before first schedule check...")
    print(f"{'='*60}\n")
    await asyncio.sleep(43200)
    
    while True:
        try:
            schedule_running = True
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            
            print(f"\n{'='*60}")
            print(f"Starting schedule check at {last_checked}")
            print(f"{'='*60}\n")
            
            for doc in educators_col.find():
                username = doc.get("username", "unknown")
                last_checked_str = doc.get("last_checked_time")
                should_check = True
                
                if last_checked_str:
                    try:
                        last_checked_dt = dateutil.parser.parse(last_checked_str)
                        time_diff = current_time - last_checked_dt
                        if time_diff < timedelta(hours=12):
                            print(f"Skipping {username}, checked {time_diff} ago")
                            should_check = False
                    except ValueError:
                        print(f"Invalid last_checked_time for {username}")
                        should_check = True
                
                if not should_check:
                    continue
                
                thread_id = doc.get("subtopic_msg_id")
                print(f"\nChecking educator: {username}")
                
                progress_key = f"{username}_{thread_id}"
                scheduler_progress_messages[progress_key] = None
                
                # Fetch fresh data
                print(f"Re-fetching courses and batches for {username}...")
                courses = await fetch_courses(username)
                batches = await fetch_batches(username)
                
                # Check for new educators in courses/batches
                for course in courses:
                    author = course.get("author", {})
                    if author.get("uid") and author.get("username"):
                        await add_educator_if_not_exists(
                            author.get("uid"),
                            author.get("username"),
                            author.get("first_name", "N/A"),
                            author.get("last_name", "N/A"),
                            author.get("avatar", "N/A")
                        )
                
                for batch in batches:
                    for author in batch.get("authors", []):
                        if author.get("uid") and author.get("username"):
                            await add_educator_if_not_exists(
                                author.get("uid"),
                                author.get("username"),
                                author.get("first_name", "N/A"),
                                author.get("last_name", "N/A"),
                                author.get("avatar", "N/A")
                            )
                
                # Filter current items
                current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
                
                # Check for new courses/batches
                existing_course_uids = {c["uid"] for c in doc.get("courses", [])}
                existing_batch_uids = {b["uid"] for b in doc.get("batches", [])}
                
                new_courses = [c for c in current_courses if c["uid"] not in existing_course_uids]
                new_batches = [b for b in current_batches if b["uid"] not in existing_batch_uids]
                
                # Add new items to database
                for course in new_courses:
                    course_data = {
                        "uid": course["uid"],
                        "name": course.get("name", "N/A"),
                        "slug": course.get("slug", "N/A"),
                        "thumbnail": course.get("thumbnail", "N/A"),
                        "starts_at": course.get("starts_at", "N/A"),
                        "ends_at": course.get("ends_at", "N/A"),
                        "group_id": SETTED_GROUP_ID,
                        "last_checked_at": None,
                        "msg_id": None,
                        "channel_msg_id": None,
                        "is_completed": False,
                        "teachers": f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
                    }
                    educators_col.update_one(
                        {"_id": doc["_id"]},
                        {"$push": {"courses": course_data}}
                    )
                
                for batch in new_batches:
                    batch_data = {
                        "uid": batch["uid"],
                        "name": batch.get("name", "N/A"),
                        "slug": batch.get("slug", "N/A"),
                        "cover_photo": batch.get("cover_photo", "N/A"),
                        "exam_type": batch.get("exam_type", "N/A"),
                        "syllabus_tag": batch.get("syllabus_tag", "N/A"),
                        "starts_at": batch.get("starts_at", "N/A"),
                        "completed_at": batch.get("completed_at", "N/A"),
                        "group_id": SETTED_GROUP_ID,
                        "last_checked_at": None,
                        "msg_id": None,
                        "channel_msg_id": None,
                        "is_completed": False,
                        "teachers": ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
                    }
                    educators_col.update_one(
                        {"_id": doc["_id"]},
                        {"$push": {"batches": batch_data}}
                    )
                
                # Get updated document
                doc = educators_col.find_one({"_id": doc["_id"]})
                courses_to_check = [c for c in doc.get("courses", []) if not c.get("is_completed", False)]
                batches_to_check = [b for b in doc.get("batches", []) if not b.get("is_completed", False)]
                
                total_courses = len(courses_to_check)
                total_batches = len(batches_to_check)
                checked_courses = 0
                checked_batches = 0
                
                if total_courses == 0 and total_batches == 0:
                    print(f"No active items for {username}")
                else:
                    # PHASE 1: Check Courses
                    if total_courses > 0:
                        print(f"\nPhase 1: Checking {total_courses} courses...")
                        await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "courses")
                        
                        for course in courses_to_check:
                            try:
                                end_time_str = course.get("ends_at", "N/A")
                                if end_time_str != "N/A":
                                    try:
                                        end_time = dateutil.parser.isoparse(end_time_str)
                                    except ValueError:
                                        checked_courses += 1
                                        continue
                                    
                                    if current_time > end_time:
                                        try:
                                            if course.get("msg_id"):
                                                await bot.edit_message_caption(
                                                    chat_id=SETTED_GROUP_ID,
                                                    message_id=course["msg_id"],
                                                    caption=f"✓ Course Completed - No More Updates"
                                                )
                                            educators_col.update_one(
                                                {"_id": doc["_id"], "courses.uid": course["uid"]},
                                                {"$set": {"courses.$.is_completed": True}}
                                            )
                                            print(f"✓ Marked course {course['uid']} completed")
                                        except Exception as e:
                                            print(f"Error marking course completed: {e}")
                                    else:
                                        print(f"Updating course {course['uid']}")
                                        schedule_url = f"https://unacademy.com/api/v3/collection/{course['uid']}/items?limit=10000"
                                        
                                        results, _ = await fetch_unacademy_schedule(schedule_url, "course", course)
                                        if results is None:
                                            checked_courses += 1
                                            await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "courses")
                                            continue
                                        
                                        filename = f"temp_schedule_{username}_course_{course['uid']}_{int(datetime.now().timestamp())}.json"
                                        save_to_json(filename, results)
                                        
                                        try:
                                            old_msg_id = course.get("msg_id")
                                            if old_msg_id:
                                                try:
                                                    await bot.delete_message(chat_id=SETTED_GROUP_ID, message_id=old_msg_id)
                                                    await asyncio.sleep(2)
                                                except:
                                                    pass
                                            
                                            with open(filename, "rb") as f:
                                                new_msg = await bot.send_document(
                                                    chat_id=SETTED_GROUP_ID,
                                                    message_thread_id=thread_id,
                                                    document=f
                                                )
                                            
                                            new_msg_id = new_msg.message_id
                                            
                                            # Forward to channel
                                            channel_msg_id = None
                                            if FORWARD_CHANNEL_ID:
                                                try:
                                                    channel_msg = await bot.copy_message(
                                                        chat_id=f"-100{FORWARD_CHANNEL_ID}",
                                                        from_chat_id=SETTED_GROUP_ID,
                                                        message_id=new_msg_id,
                                                        caption=f"In channel - https://t.me/c/{FORWARD_CHANNEL_ID}/{new_msg_id}"
                                                    )
                                                    channel_msg_id = channel_msg.message_id
                                                except Exception as e:
                                                    print(f"Error forwarding to channel: {e}")
                                            
                                            educators_col.update_one(
                                                {"_id": doc["_id"], "courses.uid": course["uid"]},
                                                {"$set": {
                                                    "courses.$.msg_id": new_msg_id,
                                                    "courses.$.channel_msg_id": channel_msg_id,
                                                    "courses.$.last_checked_at": last_checked
                                                }}
                                            )
                                            await asyncio.sleep(30)
                                        except Exception as e:
                                            print(f"Error updating course: {e}")
                                        finally:
                                            if os.path.exists(filename):
                                                os.remove(filename)
                                            if 'results' in locals():
                                                del results
                                
                                checked_courses += 1
                                await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "courses")
                            
                            except Exception as e:
                                print(f"Error processing course: {e}")
                                checked_courses += 1
                    
                    # PHASE 2: Check Batches
                    if total_batches > 0:
                        print(f"\nPhase 2: Checking {total_batches} batches...")
                        await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "batches")
                        
                        for batch in batches_to_check:
                            try:
                                end_time_str = batch.get("completed_at", "N/A")
                                if end_time_str != "N/A":
                                    try:
                                        end_time = dateutil.parser.isoparse(end_time_str)
                                    except ValueError:
                                        checked_batches += 1
                                        continue
                                    
                                    if current_time > end_time:
                                        try:
                                            if batch.get("msg_id"):
                                                await bot.edit_message_caption(
                                                    chat_id=SETTED_GROUP_ID,
                                                    message_id=batch["msg_id"],
                                                    caption=f"✓ Batch Completed - No More Updates"
                                                )
                                            educators_col.update_one(
                                                {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                                {"$set": {"batches.$.is_completed": True}}
                                            )
                                            print(f"✓ Marked batch {batch['uid']} completed")
                                        except Exception as e:
                                            print(f"Error marking batch completed: {e}")
                                    else:
                                        print(f"Updating batch {batch['uid']}")
                                        schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                                        
                                        results, _ = await fetch_unacademy_schedule(schedule_url, "batch", batch)
                                        if results is None:
                                            checked_batches += 1
                                            await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "batches")
                                            continue
                                        
                                        filename = f"temp_schedule_{username}_batch_{batch['uid']}_{int(datetime.now().timestamp())}.json"
                                        save_to_json(filename, results)
                                        
                                        try:
                                            old_msg_id = batch.get("msg_id")
                                            if old_msg_id:
                                                try:
                                                    await bot.delete_message(chat_id=SETTED_GROUP_ID, message_id=old_msg_id)
                                                    await asyncio.sleep(2)
                                                except:
                                                    pass
                                            
                                            with open(filename, "rb") as f:
                                                new_msg = await bot.send_document(
                                                    chat_id=SETTED_GROUP_ID,
                                                    message_thread_id=thread_id,
                                                    document=f
                                                )
                                            
                                            new_msg_id = new_msg.message_id
                                            
                                            # Forward to channel
                                            channel_msg_id = None
                                            if FORWARD_CHANNEL_ID:
                                                try:
                                                    channel_msg = await bot.copy_message(
                                                        chat_id=f"-100{FORWARD_CHANNEL_ID}",
                                                        from_chat_id=SETTED_GROUP_ID,
                                                        message_id=new_msg_id,
                                                        caption=f"In channel - https://t.me/c/{FORWARD_CHANNEL_ID}/{new_msg_id}"
                                                    )
                                                    channel_msg_id = channel_msg.message_id
                                                except Exception as e:
                                                    print(f"Error forwarding to channel: {e}")
                                            
                                            educators_col.update_one(
                                                {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                                {"$set": {
                                                    "batches.$.msg_id": new_msg_id,
                                                    "batches.$.channel_msg_id": channel_msg_id,
                                                    "batches.$.last_checked_at": last_checked
                                                }}
                                            )
                                            await asyncio.sleep(30)
                                        except Exception as e:
                                            print(f"Error updating batch: {e}")
                                        finally:
                                            if os.path.exists(filename):
                                                os.remove(filename)
                                            if 'results' in locals():
                                                del results
                                
                                checked_batches += 1
                                await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "batches")
                            
                            except Exception as e:
                                print(f"Error processing batch: {e}")
                                checked_batches += 1
                    
                    await send_scheduler_progress(username, thread_id, total_courses, total_batches, len(new_courses), len(new_batches), checked_courses, checked_batches, "complete")
                
                educators_col.update_one({"_id": doc["_id"]}, {"$set": {"last_checked_time": last_checked}})
                print(f"Completed schedule check for {username}")
                gc.collect()
        
        except Exception as e:
            print(f"Error in schedule_checker: {e}")
        
        finally:
            schedule_running = False
            gc.collect()
        
        print(f"\nSchedule check complete. Sleeping for 12 hours...")
        await asyncio.sleep(43200)

async def send_optry_progress(educator_name, total_batches, total_courses, fetched_batches, fetched_courses):
    """Send or update progress bar for /optry command."""
    global optry_progress_message, update_obj
    
    progress_text = (
        f"📊 /optry Progress\n\n"
        f"👨‍🏫 Fetching Teacher: {educator_name}\n\n"
        f"Total Batches Fetched: {fetched_batches}\n"
        f"Total Courses Fetched: {fetched_courses}"
    )
    
    if optry_progress_message is None:
        try:
            optry_progress_message = await update_obj.message.reply_text(progress_text)
        except Exception as e:
            print(f"Error sending optry progress: {e}")
    else:
        try:
            await optry_progress_message.edit_text(progress_text)
        except BadRequest as e:
            if "message is not modified" not in str(e).lower():
                print(f"BadRequest editing optry progress: {e}")
        except Exception as e:
            print(f"Error editing optry progress: {e}")

async def optry_select_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ask user to select All or Next 10."""
    await update.message.reply_text(
        "Select mode:\n"
        "1. All - Process all pending educators one by one\n"
        "2. Next 10 - Show list of next 10 pending educators and fetch them\n\n"
        "Reply with '1' or '2'"
    )
    return OPTRY_SELECT

async def optry_handle_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user's selection for /optry mode."""
    global update_obj, optry_progress_message, optry_offset
    update_obj = update
    optry_progress_message = None
    
    selection = update.message.text.strip()
    
    if selection == '1':
        # Process ALL pending educators
        await update.message.reply_text("🚀 Processing ALL pending educators...")
        
        # Get all pending educators from optry
        fetched_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}
        all_pending = list(collection_optry.find({"uid": {"$nin": list(fetched_uids)}}))
        
        # Also add partially fetched educators
        for doc in educators_col.find():
            if not is_educator_fetched(doc.get("username", "")):
                all_pending.append({
                    "uid": doc.get("uid"),
                    "username": doc.get("username"),
                    "first_name": doc.get("first_name", "N/A"),
                    "last_name": doc.get("last_name", "N/A"),
                    "avatar": doc.get("avatar", "N/A")
                })
        
        if not all_pending:
            await update.message.reply_text("✅ No pending educators found!")
            return ConversationHandler.END
        
        await update.message.reply_text(f"✅ Found {len(all_pending)} pending educators!")
        await process_educators_list(update, context, all_pending)
        return ConversationHandler.END
    
    elif selection == '2':
        # Show next 10 pending educators
        next_10 = get_next_pending_educators(optry_offset, limit=10)
        
        if not next_10:
            await update.message.reply_text("❌ No more pending educators found!")
            optry_offset = 0  # Reset offset
            return ConversationHandler.END
        
        # Create list message
        list_msg = f"📋 Next 10 Pending Educators (Starting from position {optry_offset + 1}):\n\n"
        for idx, edu in enumerate(next_10, 1):
            first_name = edu.get("first_name", "N/A")
            last_name = edu.get("last_name", "N/A")
            username = edu.get("username", "N/A")
            list_msg += f"{optry_offset + idx}. {first_name} {last_name} [@{username}]\n"
        
        await update.message.reply_text(list_msg)
        
        # Update offset for next call
        optry_offset += len(next_10)
        
        # Now process them one by one
        await update.message.reply_text("🚀 Starting to fetch these educators...")
        await process_educators_list(update, context, next_10)
        return ConversationHandler.END
    
    else:
        await update.message.reply_text("❌ Invalid selection. Reply with '1' or '2'")
        return OPTRY_SELECT

async def process_educators_list(update: Update, context: ContextTypes.DEFAULT_TYPE, educators_list):
    """Process a list of educators."""
    total = len(educators_list)
    processed = 0
    
    for idx, edu_doc in enumerate(educators_list, 1):
        username = edu_doc.get("username", "").strip()
        if not username:
            continue
        
        # Check if already fetched (skip automatically)
        if is_educator_fetched(username):
            print(f"⚠️ Skipping {username} - already fetched")
            continue
        
        await process_single_educator(update, context, username, idx, total)
        processed += 1
    
    await update.message.reply_text(f"✅ Completed! Processed {processed}/{total} educators")

async def process_single_educator(update: Update, context: ContextTypes.DEFAULT_TYPE, username, current_idx, total_count):
    """Process a single educator - fetch and upload data."""
    global optry_progress_message
    
    try:
        username_normalized = normalize_username(username)
        
        print(f"\n[{current_idx}/{total_count}] Processing: {username}")
        
        # Check if exists
        educator_doc = educators_col.find_one({"username": username_normalized})
        
        if not educator_doc:
            # Create new educator
            educator = await fetch_educator_by_username(username)
            if not educator:
                print(f"❌ Could not fetch {username}")
                return
            
            title = f"{educator['first_name']} {educator['last_name']} [{username}]"
            topic = await context.bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
            thread_id = topic.message_thread_id
            
            educators_col.insert_one({
                "_id": ObjectId(),
                "first_name": educator["first_name"],
                "last_name": educator["last_name"],
                "username": username_normalized,
                "uid": educator["uid"],
                "avatar": educator["avatar"],
                "group_id": SETTED_GROUP_ID,
                "subtopic_msg_id": thread_id,
                "topic_title": title,
                "last_checked_time": None,
                "courses": [],
                "batches": []
            })
            
            educator_doc = educators_col.find_one({"username": username_normalized})
        
        thread_id = educator_doc["subtopic_msg_id"]
        
        # Fetch courses and batches
        courses = await fetch_courses(username)
        batches = await fetch_batches(username)
        
        # Update progress
        await send_optry_progress(
            username,
            len(batches),
            len(courses),
            0,
            0
        )
        
        # Check for new educators
        for course in courses:
            author = course.get("author", {})
            if author.get("uid") and author.get("username"):
                await add_educator_if_not_exists(
                    author.get("uid"),
                    author.get("username"),
                    author.get("first_name", "N/A"),
                    author.get("last_name", "N/A"),
                    author.get("avatar", "N/A")
                )
        
        for batch in batches:
            for author in batch.get("authors", []):
                if author.get("uid") and author.get("username"):
                    await add_educator_if_not_exists(
                        author.get("uid"),
                        author.get("username"),
                        author.get("first_name", "N/A"),
                        author.get("last_name", "N/A"),
                        author.get("avatar", "N/A")
                    )
        
        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        
        educators_col.update_one({"username": username_normalized}, {"$set": {"last_checked_time": last_checked}})
        
        current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
        completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)
        
        all_courses = current_courses + completed_courses
        all_batches = current_batches + completed_batches
        
        # Add new courses/batches to DB
        existing_doc = educators_col.find_one({"username": username_normalized})
        existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
        existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}
        
        for course in all_courses:
            if course["uid"] in existing_course_uids:
                continue
            is_current = course in current_courses
            course_data = {
                "uid": course["uid"],
                "name": course.get("name", "N/A"),
                "slug": course.get("slug", "N/A"),
                "thumbnail": course.get("thumbnail", "N/A"),
                "starts_at": course.get("starts_at", "N/A"),
                "ends_at": course.get("ends_at", "N/A"),
                "group_id": SETTED_GROUP_ID,
                "last_checked_at": None,
                "msg_id": None,
                "channel_msg_id": None,
                "is_completed": not is_current,
                "teachers": f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
            }
            educators_col.update_one({"username": username_normalized}, {"$push": {"courses": course_data}})
        
        for batch in all_batches:
            if batch["uid"] in existing_batch_uids:
                continue
            is_current = batch in current_batches
            batch_data = {
                "uid": batch["uid"],
                "name": batch.get("name", "N/A"),
                "slug": batch.get("slug", "N/A"),
                "cover_photo": batch.get("cover_photo", "N/A"),
                "exam_type": batch.get("exam_type", "N/A"),
                "syllabus_tag": batch.get("syllabus_tag", "N/A"),
                "starts_at": batch.get("starts_at", "N/A"),
                "completed_at": batch.get("completed_at", "N/A"),
                "group_id": SETTED_GROUP_ID,
                "last_checked_at": None,
                "msg_id": None,
                "channel_msg_id": None,
                "is_completed": not is_current,
                "teachers": ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
            }
            educators_col.update_one({"username": username_normalized}, {"$push": {"batches": batch_data}})
        
        # Upload educator JSON
        educator_data = {
            "username": username_normalized,
            "first_name": educator_doc.get("first_name", "N/A"),
            "last_name": educator_doc.get("last_name", "N/A"),
            "uid": educator_doc.get("uid", "N/A"),
            "avatar": educator_doc.get("avatar", "N/A"),
            "group_id": SETTED_GROUP_ID,
            "subtopic_msg_id": thread_id,
            "topic_title": educator_doc.get("topic_title", "N/A"),
            "last_checked_time": last_checked
        }
        educator_filename = f"educator_{username_normalized}_{int(datetime.now().timestamp())}.json"
        save_to_json(educator_filename, educator_data)
        
        try:
            with open(educator_filename, "rb") as f:
                await context.bot.send_document(
                    chat_id=SETTED_GROUP_ID,
                    message_thread_id=thread_id,
                    document=f,
                    caption=(
                        f"Teacher Name: {educator_data['first_name']} {educator_data['last_name']}\n"
                        f"Username: {username_normalized}\n"
                        f"Uid: {educator_data['uid']}\n"
                        f"Last Checked: {last_checked}"
                    )
                )
            await asyncio.sleep(10)
        finally:
            if os.path.exists(educator_filename):
                os.remove(educator_filename)
            del educator_data
        
        # Upload courses and batches
        fetched_courses_count = 0
        fetched_batches_count = 0
        
        async def upload_item(item, item_type):
            nonlocal fetched_courses_count, fetched_batches_count
            
            item_uid = item["uid"]
            items_field = "courses" if item_type == "course" else "batches"
            
            doc = educators_col.find_one({"username": username_normalized, f"{items_field}.uid": item_uid})
            if doc:
                for db_item in doc.get(items_field, []):
                    if db_item["uid"] == item_uid and db_item.get("msg_id"):
                        return True
            
            schedule_url = (
                f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                if item_type == "batch"
                else f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
            )
            
            results = None
            for attempt in range(5):
                try:
                    results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item)
                    if results is not None:
                        break
                    await asyncio.sleep(30)
                except:
                    await asyncio.sleep(30)
            
            if results is None:
                return False
            
            filename = f"schedule_{username_normalized}_{item_type}_{item_uid}_{int(datetime.now().timestamp())}.json"
            save_to_json(filename, results)
            
            uploaded = False
            for retry in range(5):
                try:
                    with open(filename, "rb") as f:
                        msg = await context.bot.send_document(
                            chat_id=SETTED_GROUP_ID,
                            message_thread_id=thread_id,
                            document=f
                        )
                    msg_id = msg.message_id
                    
                    # Forward to channel
                    channel_msg_id = None
                    if FORWARD_CHANNEL_ID:
                        try:
                            channel_msg = await context.bot.copy_message(
                                chat_id=f"-100{FORWARD_CHANNEL_ID}",
                                from_chat_id=SETTED_GROUP_ID,
                                message_id=msg_id,
                                caption=f"In channel - https://t.me/c/{FORWARD_CHANNEL_ID}/{msg_id}"
                            )
                            channel_msg_id = channel_msg.message_id
                        except:
                            pass
                    
                    educators_col.update_one(
                        {"username": username_normalized, f"{items_field}.uid": item_uid},
                        {"$set": {
                            f"{items_field}.$.msg_id": msg_id,
                            f"{items_field}.$.channel_msg_id": channel_msg_id,
                            f"{items_field}.$.last_checked_at": last_checked
                        }}
                    )
                    uploaded = True
                    
                    # Update progress counters
                    if item_type == "course":
                        fetched_courses_count += 1
                    else:
                        fetched_batches_count += 1
                    
                    # Update progress display
                    await send_optry_progress(username, len(all_batches), len(all_courses), fetched_batches_count, fetched_courses_count)
                    
                    await asyncio.sleep(20)
                    break
                except RetryAfter as e:
                    await asyncio.sleep(e.retry_after + 5)
                except:
                    await asyncio.sleep(30)
            
            if os.path.exists(filename):
                os.remove(filename)
            del results
            return uploaded
        
        # Process courses
        for course in all_courses:
            await upload_item(course, "course")
            await asyncio.sleep(2)
        
        all_courses.clear()
        gc.collect()
        
        # Process batches
        for batch in all_batches:
            await upload_item(batch, "batch")
            await asyncio.sleep(2)
        
        all_batches.clear()
        gc.collect()
        
        print(f"✅ Completed: {username} [{current_idx}/{total_count}]")
    
    except Exception as e:
        print(f"❌ Error processing {username}: {e}")

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /add command."""
    global update_context, update_obj, progress_message
    update_context = context
    update_obj = update
    progress_message = None
    
    if not context.args:
        await update.message.reply_text("Please provide a username. Usage: /add {username}")
        return ConversationHandler.END
    
    raw_username = context.args[0]
    username = normalize_username(raw_username)
    await update.message.reply_text(f"Fetching data for username: {username}")
    
    educator = await fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"No educator found: {username}")
        return ConversationHandler.END
    
    educator_doc = educators_col.find_one({"username": username})
    if educator_doc:
        thread_id = educator_doc["subtopic_msg_id"]
        title = educator_doc["topic_title"]
        print(f"Educator {username} already exists with thread ID {thread_id}")
    else:
        title = f"{educator['first_name']} {educator['last_name']} [{raw_username}]"
        try:
            topic = await context.bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
            thread_id = topic.message_thread_id
        except Exception as e:
            await update.message.reply_text(f"Error creating topic: {e}")
            return ConversationHandler.END
        
        educators_col.insert_one({
            "_id": ObjectId(),
            "first_name": educator["first_name"],
            "last_name": educator["last_name"],
            "username": username,
            "uid": educator["uid"],
            "avatar": educator["avatar"],
            "group_id": SETTED_GROUP_ID,
            "subtopic_msg_id": thread_id,
            "topic_title": title,
            "last_checked_time": None,
            "courses": [],
            "batches": []
        })
    
    context.user_data['thread_id'] = thread_id
    context.user_data['group_id'] = SETTED_GROUP_ID
    context.user_data['topic_title'] = title
    
    print(f"Fetching courses for {username}...")
    courses = await fetch_courses(username)
    print(f"Fetching batches for {username}...")
    batches = await fetch_batches(username)
    
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    
    educators_col.update_one({"username": username}, {"$set": {"last_checked_time": last_checked}})
    
    current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
    completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)
    
    all_courses = current_courses + completed_courses
    all_batches = current_batches + completed_batches
    
    existing_doc = educators_col.find_one({"username": username})
    existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
    existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}
    
    course_datas = []
    for course in all_courses:
        if course["uid"] in existing_course_uids:
            continue
        is_current = course in current_courses
        course_data = {
            "uid": course["uid"],
            "name": course.get("name", "N/A"),
            "slug": course.get("slug", "N/A"),
            "thumbnail": course.get("thumbnail", "N/A"),
            "starts_at": course.get("starts_at", "N/A"),
            "ends_at": course.get("ends_at", "N/A"),
            "group_id": SETTED_GROUP_ID,
            "last_checked_at": None,
            "msg_id": None,
            "channel_msg_id": None,
            "is_completed": not is_current,
            "teachers": f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
        }
        course_datas.append(course_data)
    
    if course_datas:
        educators_col.update_one({"username": username}, {"$push": {"courses": {"$each": course_datas}}})
    
    batch_datas = []
    for batch in all_batches:
        if batch["uid"] in existing_batch_uids:
            continue
        is_current = batch in current_batches
        batch_data = {
            "uid": batch["uid"],
            "name": batch.get("name", "N/A"),
            "slug": batch.get("slug", "N/A"),
            "cover_photo": batch.get("cover_photo", "N/A"),
            "exam_type": batch.get("exam_type", "N/A"),
            "syllabus_tag": batch.get("syllabus_tag", "N/A"),
            "starts_at": batch.get("starts_at", "N/A"),
            "completed_at": batch.get("completed_at", "N/A"),
            "group_id": SETTED_GROUP_ID,
            "last_checked_at": None,
            "msg_id": None,
            "channel_msg_id": None,
            "is_completed": not is_current,
            "teachers": ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
        }
        batch_datas.append(batch_data)
    
    if batch_datas:
        educators_col.update_one({"username": username}, {"$push": {"batches": {"$each": batch_datas}}})
    
    existing_doc = educators_col.find_one({"username": username})
    total_courses = len(existing_doc.get("courses", []))
    total_batches = len(existing_doc.get("batches", []))
    
    def get_uploaded_courses():
        doc = educators_col.find_one({"username": username})
        return sum(1 for c in doc.get("courses", []) if c.get("msg_id") is not None)
    
    def get_uploaded_batches():
        doc = educators_col.find_one({"username": username})
        return sum(1 for b in doc.get("batches", []) if b.get("msg_id") is not None)
    
    phase_tracker = {'phase': 'courses'}
    
    progress_task = asyncio.create_task(progress_updater_add(
        total_courses,
        total_batches,
        get_uploaded_courses,
        get_uploaded_batches,
        phase_tracker
    ))
    
    # Upload educator JSON
    educator_data = {
        "username": username,
        "first_name": educator["first_name"],
        "last_name": educator["last_name"],
        "uid": educator["uid"],
        "avatar": educator["avatar"],
        "group_id": SETTED_GROUP_ID,
        "subtopic_msg_id": thread_id,
        "topic_title": title,
        "last_checked_time": last_checked
    }
    educator_filename = f"educator_{username}_{int(datetime.now().timestamp())}.json"
    save_to_json(educator_filename, educator_data)
    try:
        with open(educator_filename, "rb") as f:
            await context.bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=(
                    f"Teacher Name: {educator['first_name']} {educator['last_name']}\n"
                    f"Username: {username}\n"
                    f"Uid: {educator['uid']}\n"
                    f"Last Checked: {last_checked}"
                )
            )
        print(f"✓ Educator JSON uploaded")
        await asyncio.sleep(10)
    except Exception as e:
        print(f"Error uploading educator JSON: {e}")
    finally:
        if os.path.exists(educator_filename):
            os.remove(educator_filename)
        del educator_data
    
    # Function to update item
    async def update_item(item, item_type):
        item_uid = item["uid"]
        item_name = item.get("name", "Unknown")
        items_field = "courses" if item_type == "course" else "batches"
        
        doc = educators_col.find_one({"username": username, f"{items_field}.uid": item_uid})
        if doc:
            for db_item in doc.get(items_field, []):
                if db_item["uid"] == item_uid and db_item.get("msg_id") is not None:
                    print(f"Skipping uploaded {item_type} {item_uid}")
                    return True
        
        print(f"Processing {item_type} {item_uid} ({item_name})...")
        
        schedule_url = (
            f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
            if item_type == "batch"
            else f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
        )
        
        results = None
        fetch_attempts = 0
        
        while results is None and fetch_attempts < 5:
            fetch_attempts += 1
            try:
                results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item)
                if results is None:
                    await asyncio.sleep(30)
            except Exception as e:
                print(f"Fetch error: {e}")
                await asyncio.sleep(30)
        
        if results is None:
            print(f"FAILED to fetch {item_type} {item_uid}")
            return False
        
        schedule_filename = f"schedule_{username}_{item_type}_{item_uid}_{int(datetime.now().timestamp())}.json"
        try:
            save_to_json(schedule_filename, results)
        except Exception as e:
            print(f"Error saving JSON: {e}")
            del results
            return False
        
        uploaded = False
        retries = 0
        
        while not uploaded and retries < 5:
            retries += 1
            try:
                with open(schedule_filename, "rb") as f:
                    msg = await context.bot.send_document(
                        chat_id=SETTED_GROUP_ID,
                        message_thread_id=thread_id,
                        document=f
                    )
                msg_id = msg.message_id
                
                # Forward to channel
                channel_msg_id = None
                if FORWARD_CHANNEL_ID:
                    try:
                        channel_msg = await context.bot.copy_message(
                            chat_id=f"-100{FORWARD_CHANNEL_ID}",
                            from_chat_id=SETTED_GROUP_ID,
                            message_id=msg_id,
                            caption=f"In channel - https://t.me/c/{FORWARD_CHANNEL_ID}/{msg_id}"
                        )
                        channel_msg_id = channel_msg.message_id
                    except Exception as e:
                        print(f"Error forwarding to channel: {e}")
                
                uploaded = True
                
                educators_col.update_one(
                    {"username": username, f"{items_field}.uid": item_uid},
                    {"$set": {
                        f"{items_field}.$.last_checked_at": last_checked,
                        f"{items_field}.$.msg_id": msg_id,
                        f"{items_field}.$.channel_msg_id": channel_msg_id
                    }}
                )
                
                await asyncio.sleep(20)
            
            except RetryAfter as e:
                wait_time = e.retry_after + 5
                print(f"Rate limited, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
            except (TimedOut, NetworkError) as e:
                print(f"Network error: {e}")
                await asyncio.sleep(30)
            except Exception as e:
                print(f"Upload error: {e}")
                await asyncio.sleep(20)
        
        try:
            if os.path.exists(schedule_filename):
                os.remove(schedule_filename)
                print(f"✓ Deleted {schedule_filename}")
        except Exception as e:
            print(f"Could not delete file: {e}")
        
        del results
        
        if not uploaded:
            print(f"FAILED to upload {item_type} {item_uid}")
            return False
        
        print(f"COMPLETED {item_type} {item_uid}")
        return True
    
    # Process courses and batches SEPARATELY
    failed_courses = []
    failed_batches = []
    
    # PHASE 1: Upload ALL courses first
    print(f"\n{'='*60}")
    print(f"PHASE 1: Processing {len(all_courses)} courses...")
    print(f"{'='*60}\n")
    phase_tracker['phase'] = 'courses'
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'courses')
    
    for idx, course in enumerate(all_courses, 1):
        try:
            print(f"\n[COURSE {idx}/{len(all_courses)}]")
            success = await update_item(course, "course")
            if not success:
                failed_courses.append(course["uid"])
            await asyncio.sleep(2)
            
            if idx % 10 == 0:
                gc.collect()
                print(f"✓ Memory cleanup at course {idx}")
        except Exception as e:
            print(f"EXCEPTION processing course {course.get('uid', 'UNKNOWN')}: {e}")
            failed_courses.append(course["uid"])
            await asyncio.sleep(5)
    
    all_courses.clear()
    gc.collect()
    
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'courses')
    
    # PHASE 2: Upload ALL batches
    print(f"\n{'='*60}")
    print(f"PHASE 2: Processing {len(all_batches)} batches...")
    print(f"{'='*60}\n")
    phase_tracker['phase'] = 'batches'
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'batches')
    
    for idx, batch in enumerate(all_batches, 1):
        try:
            print(f"\n[BATCH {idx}/{len(all_batches)}]")
            success = await update_item(batch, "batch")
            if not success:
                failed_batches.append(batch["uid"])
            await asyncio.sleep(2)
            
            if idx % 10 == 0:
                gc.collect()
                print(f"✓ Memory cleanup at batch {idx}")
        except Exception as e:
            print(f"EXCEPTION processing batch {batch.get('uid', 'UNKNOWN')}: {e}")
            failed_batches.append(batch["uid"])
            await asyncio.sleep(5)
    
    all_batches.clear()
    gc.collect()
    
    phase_tracker['phase'] = 'complete'
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'complete')
    
    if failed_courses or failed_batches:
        failure_msg = "Some items failed to upload:\n"
        if failed_courses:
            failure_msg += f"Failed Courses: {len(failed_courses)}\n"
        if failed_batches:
            failure_msg += f"Failed Batches: {len(failed_batches)}\n"
        print(f"\n{failure_msg}")
        await update.message.reply_text(failure_msg)
    else:
        await update.message.reply_text("All items uploaded successfully!")
    
    progress_task.cancel()
    
    await update.message.reply_text(f"Upload complete! Topic: {title}")
    context.user_data['courses'] = courses
    context.user_data['batches'] = batches
    context.user_data['username'] = username
    context.user_data['last_checked'] = last_checked
    await update.message.reply_text("What do you want to fetch?\n1. Batch\n2. Course\nReply with '1' or '2', or 'cancel' to exit.")
    return SELECT_TYPE

async def select_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the selection of batch or course."""
    user_input = update.message.text.lower()
    if user_input == 'cancel':
        await update.message.reply_text("Operation cancelled.")
        return ConversationHandler.END
    if user_input not in ['1', '2']:
        await update.message.reply_text("Please reply with '1' for Batch or '2' for Course, or 'cancel'.")
        return SELECT_TYPE
    context.user_data['item_type'] = 'batch' if user_input == '1' else 'course'
    item_label = 'Batch ID' if user_input == '1' else 'Course ID'
    await update.message.reply_text(f"Please provide the {item_label} (UID).")
    return ENTER_ID

async def enter_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the batch or course ID input."""
    item_id = update.message.text.strip()
    item_type = context.user_data.get('item_type')
    courses = context.user_data.get('courses', [])
    batches = context.user_data.get('batches', [])
    username = context.user_data.get('username')
    last_checked = context.user_data.get('last_checked')
    group_id = context.user_data.get('group_id')
    thread_id = context.user_data.get('thread_id')
    topic_title = context.user_data.get('topic_title')
    
    items_field = "courses" if item_type == "course" else "batches"
    
    doc = educators_col.find_one({"username": username, f"{items_field}.uid": item_id})
    item_data = None
    if doc:
        for item in doc.get(items_field, []):
            if item["uid"] == item_id:
                item_data = item
                break
    if not item_data:
        item_data = next((item for item in (batches if item_type == 'batch' else courses) if item["uid"] == item_id), None)
        if not item_data:
            await update.message.reply_text(f"No {item_type} found with ID: {item_id}")
            return ConversationHandler.END
    
    schedule_url = (
        f"https://api.unacademy.com/api/v1/batch/{item_id}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
        if item_type == 'batch'
        else f"https://unacademy.com/api/v3/collection/{item_id}/items?limit=10000"
    )
    
    results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item_data)
    if results is None:
        await update.message.reply_text(f"Failed to fetch schedule for {item_type} ID: {item_id}")
        return ConversationHandler.END
    
    schedule_filename = f"schedule_{username}_{item_type}_{item_id}_{int(datetime.now().timestamp())}.json"
    save_to_json(schedule_filename, results)
    
    uploaded = False
    retries = 0
    while not uploaded and retries < 10:
        try:
            with open(schedule_filename, "rb") as f:
                msg = await context.bot.send_document(
                    chat_id=group_id,
                    message_thread_id=thread_id,
                    document=f
                )
            new_msg_id = msg.message_id
            
            # Forward to channel
            channel_msg_id = None
            if FORWARD_CHANNEL_ID:
                try:
                    channel_msg = await context.bot.copy_message(
                        chat_id=f"-100{FORWARD_CHANNEL_ID}",
                        from_chat_id=SETTED_GROUP_ID,
                        message_id=new_msg_id,
                        caption=f"In channel - https://t.me/c/{FORWARD_CHANNEL_ID}/{new_msg_id}"
                    )
                    channel_msg_id = channel_msg.message_id
                except:
                    pass
            
            educators_col.update_one(
                {"username": username, f"{items_field}.uid": item_id},
                {"$set": {
                    f"{items_field}.$.msg_id": new_msg_id,
                    f"{items_field}.$.channel_msg_id": channel_msg_id,
                    f"{items_field}.$.last_checked_at": last_checked
                }}
            )
            uploaded = True
            await asyncio.sleep(30)
        except Exception as e:
            print(f"Error uploading: {e}")
            retries += 1
            await asyncio.sleep(30)
    
    if os.path.exists(schedule_filename):
        os.remove(schedule_filename)
        print(f"✓ Deleted {schedule_filename}")
    
    del results
    
    if not uploaded:
        await update.message.reply_text(f"Failed to upload after retries")
        return ConversationHandler.END
    
    await update.message.reply_text(f"Schedule uploaded to: {topic_title}")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the conversation."""
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END

async def main():
    """Start the Telegram bot."""
    global bot
    bot_token = '7213717609:AAGAuuDNX_EEMZfF2D_Zoz-vDoQizBxW96I'
    application = Application.builder().token(bot_token).build()
    bot = application.bot
    
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            SELECT_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_type)],
            ENTER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    optry_handler = ConversationHandler(
        entry_points=[CommandHandler("optry", optry_select_mode)],
        states={
            OPTRY_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, optry_handle_selection)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )
    
    application.add_handler(conv_handler)
    application.add_handler(optry_handler)
    
    print("Bot is starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    asyncio.create_task(schedule_checker())
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down bot...")
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
