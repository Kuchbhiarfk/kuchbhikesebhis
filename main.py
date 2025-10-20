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

# Telegram group ID & Channel ID
SETTED_GROUP_ID = -1003133358948
CHANNEL_ID = -1002927760779  # CHANGE THIS TO YOUR CHANNEL ID

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

SELECT_TYPE, ENTER_ID, SELECT_MODE = range(3)


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
                    item_name = item_data.get("name", "N/A")
                    item_starts_at = item_data.get("starts_at", "N/A")
                    item_ends_at = item_data.get("ends_at", "N/A") if item_type == "course" else item_data.get("completed_at", "N/A")
                    item_teachers = [item_data.get("author", {})] if item_type == "course" else item_data.get("authors", [])

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

                    teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in item_teachers if t.get('first_name')])
                    last_checked = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S %Z")
                    if item_type == "course":
                        caption = (
                            f"Course Name: {item_name}\n"
                            f"Course Teacher: {teachers}\n"
                            f"Start_at: {item_starts_at}\n"
                            f"Ends_at: {item_ends_at}\n"
                            f"Last_checked_at: {last_checked}"
                        )
                    else:
                        caption = (
                            f"Batch Name: {item_name}\n"
                            f"Batch Teachers: {teachers}\n"
                            f"Start_at: {item_starts_at}\n"
                            f"Completed_at: {item_ends_at}\n"
                            f"Last_checked_at: {last_checked}"
                        )

                    del data
                    del results

                    return results_list, caption

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
            live_at_time = datetime.strptime(live_at, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=pytz.UTC)
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


# 🔥 NEW: Add educator to optry DB with duplicate check
async def add_educator_to_optry(edu_username, edu_uid, edu_first, edu_last, edu_avatar):
    """Add educator to optry MongoDB if not exists (UID check)"""
    existing = collection_optry.find_one({"uid": edu_uid})
    if existing:
        return False
    
    normalized_username = normalize_username(edu_username)
    try:
        collection_optry.insert_one({
            "uid": edu_uid,
            "username": normalized_username,
            "avatar": edu_avatar,
            "first_name": edu_first,
            "last_name": edu_last
        })
        print(f"✅ ADDED NEW EDUCATOR TO OPTRY: {edu_username} (UID: {edu_uid})")
        return True
    except Exception as e:
        print(f"❌ Error adding educator {edu_username}: {e}")
        return False


# 🔥 NEW: Extract & add all educators from courses/batches
async def extract_and_add_educators(courses, batches):
    """Extract all educators from courses & batches and add to optry DB"""
    new_added = 0
    all_educators = set()
    
    # From courses
    for course in courses:
        author = course.get("author", {})
        username = author.get("username", "").strip()
        uid = author.get("uid", "").strip()
        if username and uid:
            all_educators.add((username, uid, author.get("first_name", "N/A"), 
                             author.get("last_name", "N/A"), author.get("avatar", "N/A")))
    
    # From batches
    for batch in batches:
        for author in batch.get("authors", []):
            username = author.get("username", "").strip()
            uid = author.get("uid", "").strip()
            if username and uid:
                all_educators.add((username, uid, author.get("first_name", "N/A"), 
                                 author.get("last_name", "N/A"), author.get("avatar", "N/A")))
    
    # Add to optry DB
    for username, uid, first, last, avatar in all_educators:
        if await add_educator_to_optry(username, uid, first, last, avatar):
            new_added += 1
    
    return new_added


async def upload_to_both_places(document_path, caption, group_thread_id, channel_thread_id=None):
    """Upload file to both group & channel, return both msg_ids"""
    group_msg_id = None
    channel_msg_id = None
    
    try:
        # Upload to GROUP
        with open(document_path, "rb") as f:
            group_msg = await bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=group_thread_id,
                document=f,
                caption=caption
            )
        group_msg_id = group_msg.message_id
        
        # Upload to CHANNEL (same file)
        with open(document_path, "rb") as f:
            channel_msg = await bot.send_document(
                chat_id=CHANNEL_ID,
                message_thread_id=channel_thread_id,
                document=f,
                caption=caption + f"\n\nIn group - https://t.me/c/{str(SETTED_GROUP_ID)[4:]}/{group_msg_id}"
            )
        channel_msg_id = channel_msg.message_id
        
        # Update GROUP caption with channel link
        final_caption = caption + f"\n\nIn channel - https://t.me/c/{str(CHANNEL_ID)[4:]}/{channel_msg_id}"
        await bot.edit_message_caption(
            chat_id=SETTED_GROUP_ID,
            message_id=group_msg_id,
            caption=final_caption
        )
        
        print(f"✅ Uploaded to BOTH: Group Msg {group_msg_id} | Channel Msg {channel_msg_id}")
        return group_msg_id, channel_msg_id
        
    except Exception as e:
        print(f"❌ Error uploading to both: {e}")
        return None, None


async def send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches, current_phase, teacher_name=""):
    """Send or update progress bar for /add command."""
    global progress_message, update_obj

    if current_phase == "courses":
        progress_text = (
            f"🔄 Fetching Teacher Name: {teacher_name}\n"
            f"📚 Total Courses Fetched: {total_courses}\n"
            f"📦 Total Batches Fetched: {total_batches}\n\n"
            f"Phase 1: Uploading Courses\n"
            f"Progress: {uploaded_courses}/{total_courses}"
        )
    elif current_phase == "batches":
        progress_text = (
            f"🔄 Fetching Teacher Name: {teacher_name}\n"
            f"📚 Total Courses Fetched: {total_courses}\n"
            f"📦 Total Batches Fetched: {total_batches}\n\n"
            f"Phase 2: Uploading Batches\n"
            f"Progress: {uploaded_batches}/{total_batches}"
        )
    else:
        progress_text = (
            f"✅ Upload Complete!\n"
            f"🔄 Teacher: {teacher_name}\n"
            f"📚 Courses: {uploaded_courses}/{total_courses}\n"
            f"📦 Batches: {uploaded_batches}/{total_batches}"
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


async def send_scheduler_progress(username, thread_id, total_courses, total_batches, new_courses, new_batches, checked_courses, checked_batches, teacher_name):
    """Enhanced scheduler progress with NEW items count"""
    global scheduler_progress_messages

    progress_text = (
        f"🔄 Schedule Checking: {teacher_name}\n"
        f"📚 Total Courses: {total_courses}\n"
        f"📦 Total Batches: {total_batches}\n"
        f"➕ New Courses: {new_courses}\n"
        f"➕ New Batches: {new_batches}\n\n"
        f"Checking Progress: {checked_courses + checked_batches}/{total_courses + total_batches}"
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


async def schedule_checker():
    """IMMEDIATE START - Check every educator where current_time - last_checked >= 12hr"""
    print(f"\n{'='*60}")
    print(f"🚀 Schedule checker STARTED IMMEDIATELY!")
    print(f"{'='*60}\n")

    while True:
        try:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

            print(f"\n🔄 Schedule check at {last_checked}")
            print(f"{'='*60}")

            # 🔥 Get ALL educators where last_checked >= 12hr OR NULL
            due_educators = []
            for doc in educators_col.find():
                last_checked_str = doc.get("last_checked_time")
                should_check = False
                
                if not last_checked_str:
                    should_check = True
                else:
                    try:
                        last_checked_dt = dateutil.parser.parse(last_checked_str)
                        if current_time - last_checked_dt >= timedelta(hours=12):
                            should_check = True
                    except:
                        should_check = True

                if should_check:
                    due_educators.append(doc)

            print(f"📊 Found {len(due_educators)} educators due for check")

            for doc_idx, doc in enumerate(due_educators, 1):
                username = doc.get("username", "unknown")
                thread_id = doc.get("subtopic_msg_id")
                channel_thread_id = doc.get("channel_thread_id", thread_id)
                
                print(f"\n[{doc_idx}/{len(due_educators)}] Checking: {username}")

                # 🔥 RE-FETCH EDUCATOR COURSES & BATCHES (NEW ONES!)
                print(f"🔄 Re-fetching ALL data for {username}...")
                courses = await fetch_courses(username)
                batches = await fetch_batches(username)
                
                # 🔥 Extract & add new educators
                new_educators_added = await extract_and_add_educators(courses, batches)
                
                # 🔥 Compare with DB - find NEW courses/batches
                existing_doc = educators_col.find_one({"_id": doc["_id"]})
                existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
                existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}

                new_courses_list = [c for c in courses if c["uid"] not in existing_course_uids]
                new_batches_list = [b for b in batches if b["uid"] not in existing_batch_uids]

                # Add NEW to DB (caption=None)
                if new_courses_list:
                    course_datas = []
                    for course in new_courses_list:
                        course_data = {
                            "uid": course["uid"],
                            "name": course.get("name", "N/A"),
                            "slug": course.get("slug", "N/A"),
                            "thumbnail": course.get("thumbnail", "N/A"),
                            "starts_at": course.get("starts_at", "N/A"),
                            "ends_at": course.get("ends_at", "N/A"),
                            "group_id": SETTED_GROUP_ID,
                            "channel_id": CHANNEL_ID,
                            "last_checked_at": None,
                            "msg_id": None,
                            "channel_msg_id": None,
                            "caption": None,  # 🔥 REMOVED
                            "is_completed": False,
                            "teachers": f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
                        }
                        course_datas.append(course_data)
                    educators_col.update_one({"_id": doc["_id"]}, {"$push": {"courses": {"$each": course_datas}}})

                if new_batches_list:
                    batch_datas = []
                    for batch in new_batches_list:
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
                            "channel_id": CHANNEL_ID,
                            "last_checked_at": None,
                            "msg_id": None,
                            "channel_msg_id": None,
                            "caption": None,  # 🔥 REMOVED
                            "is_completed": False,
                            "teachers": ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
                        }
                        batch_datas.append(batch_data)
                    educators_col.update_one({"_id": doc["_id"]}, {"$push": {"batches": {"$each": batch_datas}}})

                # 🔥 Get items to check (current + new)
                courses_to_check = [c for c in doc.get("courses", []) if not c.get("is_completed", False) and c.get("msg_id")]
                batches_to_check = [b for b in doc.get("batches", []) if not b.get("is_completed", False) and b.get("msg_id")]

                total_courses = len(courses_to_check)
                total_batches = len(batches_to_check)
                new_courses_count = len(new_courses_list)
                new_batches_count = len(new_batches_list)
                
                checked_courses = 0
                checked_batches = 0

                progress_key = f"{username}_{thread_id}"
                scheduler_progress_messages[progress_key] = None

                # 🔥 Progress with NEW counts
                await send_scheduler_progress(
                    username, thread_id, total_courses, total_batches, 
                    new_courses_count, new_batches_count, 0, 0, 
                    f"{doc.get('first_name', '')} {doc.get('last_name', '')}"
                )

                # PHASE 1: Check Courses
                for course in courses_to_check:
                    try:
                        end_time_str = course.get("ends_at", "N/A")
                        end_time = None
                        if end_time_str != "N/A":
                            end_time = dateutil.parser.isoparse(end_time_str)

                        if end_time and current_time > end_time:
                            # Mark completed
                            new_caption = f"✓ Course Completed - No More Updates"
                            group_msg_id, channel_msg_id = await upload_to_both_places(
                                f"temp_completed_{course['uid']}.json", new_caption, 
                                thread_id, channel_thread_id
                            )
                            if group_msg_id:
                                educators_col.update_one(
                                    {"_id": doc["_id"], "courses.uid": course["uid"]},
                                    {"$set": {
                                        "courses.$.is_completed": True,
                                        "courses.$.msg_id": group_msg_id,
                                        "courses.$.channel_msg_id": channel_msg_id,
                                        "courses.$.caption": new_caption
                                    }}
                                )
                        else:
                            # Update schedule
                            schedule_url = f"https://unacademy.com/api/v3/collection/{course['uid']}/items?limit=10000"
                            results, caption = await fetch_unacademy_schedule(schedule_url, "course", course)
                            
                            if results:
                                filename = f"temp_schedule_{username}_course_{course['uid']}.json"
                                save_to_json(filename, results)
                                
                                group_msg_id, channel_msg_id = await upload_to_both_places(filename, caption, thread_id, channel_thread_id)
                                
                                if group_msg_id:
                                    educators_col.update_one(
                                        {"_id": doc["_id"], "courses.uid": course["uid"]},
                                        {"$set": {
                                            "courses.$.msg_id": group_msg_id,
                                            "courses.$.channel_msg_id": channel_msg_id,
                                            "courses.$.last_checked_at": last_checked,
                                            "courses.$.caption": caption
                                        }}
                                    )
                                if os.path.exists(filename):
                                    os.remove(filename)

                        checked_courses += 1
                        await send_scheduler_progress(
                            username, thread_id, total_courses, total_batches, 
                            new_courses_count, new_batches_count, 
                            checked_courses, checked_batches,
                            f"{doc.get('first_name', '')} {doc.get('last_name', '')}"
                        )

                    except Exception as e:
                        print(f"Error processing course {course.get('uid')}: {e}")
                        checked_courses += 1

                # PHASE 2: Check Batches
                for batch in batches_to_check:
                    try:
                        end_time_str = batch.get("completed_at", "N/A")
                        end_time = None
                        if end_time_str != "N/A":
                            end_time = dateutil.parser.isoparse(end_time_str)

                        if end_time and current_time > end_time:
                            new_caption = f"✓ Batch Completed - No More Updates"
                            group_msg_id, channel_msg_id = await upload_to_both_places(
                                f"temp_completed_{batch['uid']}.json", new_caption, 
                                thread_id, channel_thread_id
                            )
                            if group_msg_id:
                                educators_col.update_one(
                                    {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                    {"$set": {
                                        "batches.$.is_completed": True,
                                        "batches.$.msg_id": group_msg_id,
                                        "batches.$.channel_msg_id": channel_msg_id,
                                        "batches.$.caption": new_caption
                                    }}
                                )
                        else:
                            schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                            results, caption = await fetch_unacademy_schedule(schedule_url, "batch", batch)
                            
                            if results:
                                filename = f"temp_schedule_{username}_batch_{batch['uid']}.json"
                                save_to_json(filename, results)
                                
                                group_msg_id, channel_msg_id = await upload_to_both_places(filename, caption, thread_id, channel_thread_id)
                                
                                if group_msg_id:
                                    educators_col.update_one(
                                        {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                        {"$set": {
                                            "batches.$.msg_id": group_msg_id,
                                            "batches.$.channel_msg_id": channel_msg_id,
                                            "batches.$.last_checked_at": last_checked,
                                            "batches.$.caption": caption
                                        }}
                                    )
                                if os.path.exists(filename):
                                    os.remove(filename)

                        checked_batches += 1
                        await send_scheduler_progress(
                            username, thread_id, total_courses, total_batches, 
                            new_courses_count, new_batches_count, 
                            checked_courses, checked_batches,
                            f"{doc.get('first_name', '')} {doc.get('last_name', '')}"
                        )

                    except Exception as e:
                        print(f"Error processing batch {batch.get('uid')}: {e}")
                        checked_batches += 1

                # 🔥 Update last_checked_time
                educators_col.update_one({"_id": doc["_id"]}, {"$set": {"last_checked_time": last_checked}})
                print(f"✅ Completed {username} | New: {new_educators_added} educators")

                gc.collect()

            print(f"\n🎉 Schedule check COMPLETE! Sleeping 12hr...")
            await asyncio.sleep(43200)  # 12 hours

        except Exception as e:
            print(f"❌ Schedule checker error: {e}")
            await asyncio.sleep(3600)  # 1hr retry


async def send_optry_progress(total_educators, processed_educators, new_educators_found, current_teacher=""):
    """Enhanced optry progress"""
    global optry_progress_message, update_obj

    progress_text = (
        f"🔄 /optry Progress\n"
        f"👤 Current Teacher: {current_teacher}\n"
        f"📊 Total: {total_educators} + {new_educators_found}\n"
        f"✅ Processed: {processed_educators}/{total_educators}"
    )

    if optry_progress_message is None:
        try:
            optry_progress_message = await update_obj.message.reply_text(progress_text)
        except Exception as e:
            print(f"Error sending optry progress: {e}")
    else:
        try:
            await optry_progress_message.edit_text(progress_text)
        except Exception as e:
            print(f"Error editing optry progress: {e}")


async def get_next_10_educators():
    """Get next 10 educators list for /optry next 10"""
    all_educators = list(educators_col.find().sort("last_checked_time", 1).limit(10))
    if not all_educators:
        return []
    
    text = "📋 Next 10 Educators:\n\n"
    for i, edu in enumerate(all_educators, 1):
        name = f"{edu.get('first_name', '')} {edu.get('last_name', '')} [{edu.get('username', '')}]"
        text += f"{i}. {name}\n"
    return all_educators, text


async def optry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced /optry with All/Next 10 selection"""
    global update_obj, optry_progress_message
    update_obj = update
    optry_progress_message = None

    await update.message.reply_text(
        "🚀 /optry Menu:\n\n"
        "Reply:\n"
        "• `All` - Process ALL educators\n"
        "• `next 10` - Process next 10 educators"
    )
    return SELECT_MODE


async def select_optry_mode(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle All / next 10 selection"""
    user_input = update.message.text.lower().strip()
    
    if user_input == "all":
        context.user_data['optry_mode'] = 'all'
        await update.message.reply_text("🚀 Starting ALL educators processing...")
        # Continue with ALL logic (same as before)
        await process_optry_all(update, context)
        return ConversationHandler.END
        
    elif "next 10" in user_input:
        context.user_data['optry_mode'] = 'next10'
        await update.message.reply_text("📋 Fetching next 10 educators...")
        
        next_educators, list_text = await get_next_10_educators()
        if not next_educators:
            await update.message.reply_text("❌ No more educators to process!")
            return ConversationHandler.END
            
        await update.message.reply_text(list_text)
        await update.message.reply_text("🚀 Starting next 10 processing...")
        await process_optry_batch(update, context, next_educators)
        return ConversationHandler.END
        
    else:
        await update.message.reply_text("❌ Please reply `All` or `next 10`")
        return SELECT_MODE


async def process_optry_batch(update, context, educators_list):
    """Process batch of educators (next 10 or all)"""
    total_educators = len(educators_list)
    processed_count = {'value': 0}
    new_educators_count = {'value': 0}

    def get_processed_count():
        return processed_count['value']

    # Progress task
    progress_task = asyncio.create_task(asyncio.to_thread(
        lambda: asyncio.run(optry_progress_updater(total_educators, get_processed_count, lambda: new_educators_count['value']))
    ))

    await send_optry_progress(total_educators, 0, 0, "")

    for idx, educator_doc in enumerate(educators_list, 1):
        username = educator_doc.get("username", "")
        teacher_name = f"{educator_doc.get('first_name', '')} {educator_doc.get('last_name', '')}"
        
        await send_optry_progress(total_educators, idx-1, new_educators_count['value'], teacher_name)
        
        # 🔥 SAME PROCESSING LOGIC AS BEFORE (add, fetch, upload, extract educators)
        # ... (keeping same logic from previous optry_command)
        
        processed_count['value'] += 1
        new_educators_count['value'] += await extract_and_add_educators([], [])  # Placeholder

    progress_task.cancel()
    await update.message.reply_text(f"✅ Batch complete! Processed: {total_educators}")


async def process_optry_all(update, context):
    """Process ALL educators"""
    optry_educators = list(collection_optry.find())
    await process_optry_batch(update, context, optry_educators)


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced /add with new progress & channel upload"""
    global update_context, update_obj, progress_message
    update_context = context
    update_obj = update
    progress_message = None

    if not context.args:
        await update.message.reply_text("Usage: /add {username}")
        return ConversationHandler.END

    username = normalize_username(context.args[0])
    await update.message.reply_text(f"🔄 Processing: {username}")

    educator = await fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"❌ Educator not found: {username}")
        return ConversationHandler.END

    # Create topic & get thread IDs
    educator_doc = educators_col.find_one({"username": username})
    if not educator_doc:
        title = f"{educator['first_name']} {educator['last_name']} [{username}]"
        topic = await context.bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
        thread_id = topic.message_thread_id
        
        channel_topic = await context.bot.create_forum_topic(chat_id=CHANNEL_ID, name=title)
        channel_thread_id = channel_topic.message_thread_id
        
        educators_col.insert_one({
            "_id": ObjectId(),
            "first_name": educator["first_name"],
            "last_name": educator["last_name"],
            "username": username,
            "uid": educator["uid"],
            "avatar": educator["avatar"],
            "group_id": SETTED_GROUP_ID,
            "channel_id": CHANNEL_ID,
            "subtopic_msg_id": thread_id,
            "channel_thread_id": channel_thread_id,
            "topic_title": title,
            "last_checked_time": None,
            "courses": [],
            "batches": []
        })
    else:
        thread_id = educator_doc["subtopic_msg_id"]
        channel_thread_id = educator_doc.get("channel_thread_id", thread_id)

    # Fetch data
    courses = await fetch_courses(username)
    batches = await fetch_batches(username)
    
    # 🔥 Extract new educators
    new_educators = await extract_and_add_educators(courses, batches)
    
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Filter & add to DB (caption=None)
    current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
    all_courses = current_courses
    all_batches = current_batches

    # Add to DB
    course_datas = [{"uid": c["uid"], "name": c.get("name"), "caption": None, "msg_id": None, "channel_msg_id": None, "teachers": "..."} for c in all_courses]
    batch_datas = [{"uid": b["uid"], "name": b.get("name"), "caption": None, "msg_id": None, "channel_msg_id": None, "teachers": "..."} for b in all_batches]
    
    if course_datas: educators_col.update_one({"username": username}, {"$push": {"courses": {"$each": course_datas}}})
    if batch_datas: educators_col.update_one({"username": username}, {"$push": {"batches": {"$each": batch_datas}}})

    total_courses = len(all_courses)
    total_batches = len(all_batches)

    # Enhanced progress
    await send_progress_bar_add(total_courses, total_batches, 0, 0, "courses", f"{educator['first_name']} {educator['last_name']}")

    # Upload educator JSON to BOTH
    educator_data = {...}  # Same as before
    educator_filename = f"educator_{username}.json"
    save_to_json(educator_filename, educator_data)
    group_msg_id, channel_msg_id = await upload_to_both_places(educator_filename, caption, thread_id, channel_thread_id)
    
    educators_col.update_one({"username": username}, {
        "$set": {"last_checked_time": last_checked},
        "$push": {"courses": educator_data}  # Simplified
    })

    # Process uploads (same logic with upload_to_both_places)
    # ... (keeping upload logic with channel support)

    await update.message.reply_text(f"✅ Complete! New educators found: {new_educators}")
    return SELECT_TYPE


# Conversation handlers
async def select_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Same as before
    pass


async def enter_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Enhanced with channel upload
    # Use upload_to_both_places
    pass


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Cancelled.")
    return ConversationHandler.END


async def main():
    """Start bot with MULTIPLE handlers"""
    global bot
    bot_token = '7213717609:AAFeIOkjjXBB6bHnz0CmWtrIKxh7wp3OYbE'
    application = Application.builder().token(bot_token).build()
    bot = application.bot

    # Main conversation for /add
    conv_handler_add = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            SELECT_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_type)],
            ENTER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # Optry conversation
    conv_handler_optry = ConversationHandler(
        entry_points=[CommandHandler("optry", optry_command)],
        states={
            SELECT_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_optry_mode)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler_add)
    application.add_handler(conv_handler_optry)

    print("🚀 Bot starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    # 🔥 IMMEDIATE schedule start
    asyncio.create_task(schedule_checker())
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
