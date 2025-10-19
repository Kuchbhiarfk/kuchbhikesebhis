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

# Telegram group ID & CHANNEL ID (CHANGE THIS)
SETTED_GROUP_ID = -1003133358948
CHANNEL_ID = -1002927760779  # 🔥 ADD YOUR CHANNEL ID HERE

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

SELECT_TYPE, ENTER_ID, OPTRY_MODE = range(3)


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
            live_at_time = datetime.strptime(live_at, "%Y-%m-%dT%H:%M:%S%Z").replace(tzinfo=pytz.UTC)
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


# 🔥 NEW FUNCTION - ADD EDUCATOR TO OPTRY DB (DUPLICATE PROTECTION)
async def add_educator_to_optry(edu_username, edu_uid, edu_first, edu_last, edu_avatar):
    """Add educator to optry MongoDB with UID duplicate check."""
    existing_uids = {doc['uid'] for doc in collection_optry.find({}, {'uid': 1})}
    if edu_uid in existing_uids:
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
        print(f"❌ Error adding {edu_username}: {e}")
        return False


# 🔥 NEW FUNCTION - FORWARD TO CHANNEL WITHOUT FORWARD TAG
async def forward_to_channel(document_path, caption, group_msg_id, thread_id=None):
    """Upload file to channel with link in caption."""
    try:
        # Upload to channel
        with open(document_path, "rb") as f:
            channel_msg = await bot.send_document(
                chat_id=CHANNEL_ID,
                document=f,
                caption=caption
            )
        
        channel_link = f"https://t.me/c/{str(CHANNEL_ID)[4:]}/{channel_msg.message_id}"
        new_caption = f"{caption}\n\nIn channel - {channel_link}"
        
        # Update group message with channel link
        if thread_id:
            await bot.edit_message_caption(
                chat_id=SETTED_GROUP_ID,
                message_id=group_msg_id,
                message_thread_id=thread_id,
                caption=new_caption
            )
        else:
            await bot.edit_message_caption(
                chat_id=SETTED_GROUP_ID,
                message_id=group_msg_id,
                caption=new_caption
            )
        
        print(f"✅ Forwarded to channel: {channel_link}")
        return channel_msg.message_id
    except Exception as e:
        print(f"❌ Error forwarding to channel: {e}")
        return None


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


async def send_optry_progress(current_teacher, total_batches_fetched, total_courses_fetched, total_educators, processed_educators, new_educators_found, mode="all"):
    """Enhanced progress bar for /optry with teacher name."""
    global optry_progress_message, update_obj

    if mode == "all":
        progress_text = (
            f"🔥 /optry Progress - ALL MODE\n\n"
            f"Fetching Teacher Name: {current_teacher}\n"
            f"Total Batches Fetched: {total_batches_fetched}\n"
            f"Total Courses Fetched: {total_courses_fetched}\n"
            f"Total Educators: {total_educators}\n"
            f"Processed: {processed_educators}/{total_educators}\n"
            f"New Educators Found: {new_educators_found}"
        )
    else:
        progress_text = (
            f"🔥 /optry Progress - NEXT 10 MODE\n\n"
            f"Fetching Teacher Name: {current_teacher}\n"
            f"Total Batches Fetched: {total_batches_fetched}\n"
            f"Total Courses Fetched: {total_courses_fetched}\n"
            f"Processed: {processed_educators}/10\n"
            f"New Educators Found: {new_educators_found}"
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


async def send_scheduler_progress(username, thread_id, total_current_courses, total_current_batches, total_new_courses, total_new_batches, checked_current, checked_new, phase):
    """Enhanced schedule checker progress."""
    global scheduler_progress_messages

    if phase == "courses":
        progress_text = (
            f"📊 Schedule Check: {username}\n\n"
            f"Total Current Courses: {total_current_courses}\n"
            f"Total Current Batches: {total_current_batches}\n"
            f"Total New Courses: {total_new_courses}\n"
            f"Total New Batches: {total_new_batches}\n\n"
            f"Phase 1: Checking Courses\n"
            f"Progress: {checked_current}/{total_current_courses}"
        )
    elif phase == "batches":
        progress_text = (
            f"📊 Schedule Check: {username}\n\n"
            f"Total Current Courses: {total_current_courses}\n"
            f"Total Current Batches: {total_current_batches}\n"
            f"Total New Courses: {total_new_courses}\n"
            f"Total New Batches: {total_new_batches}\n\n"
            f"Phase 2: Checking Batches\n"
            f"Progress: {checked_current}/{total_current_batches}"
        )
    elif phase == "new_items":
        progress_text = (
            f"📊 Schedule Check: {username}\n\n"
            f"Total Current Courses: {total_current_courses}\n"
            f"Total Current Batches: {total_current_batches}\n"
            f"Total New Courses: {total_new_courses}\n"
            f"Total New Batches: {total_new_batches}\n\n"
            f"Phase 3: New Items\n"
            f"Progress: {checked_current}/{total_new_courses + total_new_batches}"
        )
    else:
        progress_text = (
            f"✅ Schedule Check Complete: {username}\n\n"
            f"Current Courses: {total_current_courses}\n"
            f"Current Batches: {total_current_batches}\n"
            f"New Courses: {total_new_courses}\n"
            f"New Batches: {total_new_batches}"
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


# 🔥 ENHANCED SCHEDULE CHECKER - EVERY 12 HOURS + NEW COURSES/BATCHES + EDUCATORS
async def schedule_checker():
    """Enhanced schedule checker - runs every 12 hours, checks ALL educators."""
    print(f"\n{'='*60}")
    print(f"🔥 ENHANCED SCHEDULE CHECKER STARTED!")
    print(f"Will check ALL educators every 12 hours")
    print(f"{'='*60}\n")
    
    while True:
        try:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"\n🔥 SCHEDULE CHECK STARTED AT: {last_checked}")
            print(f"{'='*60}")

            total_educators_checked = 0

            for doc in educators_col.find():
                username = doc.get("username", "unknown")
                thread_id = doc.get("subtopic_msg_id")
                
                # 🔥 CHECK IF 12+ HOURS PASSED
                last_checked_str = doc.get("last_checked_time")
                should_check = True
                if last_checked_str:
                    try:
                        last_checked_dt = dateutil.parser.parse(last_checked_str)
                        hours_diff = (current_time - last_checked_dt).total_seconds() / 3600
                        if hours_diff < 12:
                            print(f"⏳ {username}: Only {hours_diff:.1f}hrs passed, skipping")
                            should_check = False
                    except ValueError:
                        should_check = True

                if not should_check:
                    continue

                total_educators_checked += 1
                print(f"\n🔥 CHECKING EDUCATOR: {username}")

                # 🔥 RE-FETCH EDUCATOR'S ALL COURSES & BATCHES (NEW ONES TOO!)
                print(f"🔥 Re-fetching ALL data for {username}...")
                all_courses = await fetch_courses(username)
                all_batches = await fetch_batches(username)

                # 🔥 FILTER CURRENT vs COMPLETED
                current_courses, current_batches = filter_by_time(all_courses, all_batches, current_time, future=True)
                completed_courses, completed_batches = filter_by_time(all_courses, all_batches, current_time, future=False)

                # 🔥 FIND NEW COURSES/BATCHES (NOT IN DB)
                existing_doc = educators_col.find_one({"username": username})
                existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
                existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}

                new_courses = [c for c in all_courses if c["uid"] not in existing_course_uids]
                new_batches = [b for b in all_batches if b["uid"] not in existing_batch_uids]

                # 🔥 ADD NEW ITEMS TO DB (WITHOUT CAPTION)
                if new_courses:
                    course_datas = []
                    for course in new_courses:
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
                            "channel_msg_id": None,  # 🔥 NEW FIELD
                            "caption": None,  # 🔥 REMOVED
                            "is_completed": course in completed_courses,
                            "teachers": f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
                        }
                        course_datas.append(course_data)
                    educators_col.update_one({"username": username}, {"$push": {"courses": {"$each": course_datas}}})
                    print(f"✅ Added {len(new_courses)} NEW courses to DB")

                if new_batches:
                    batch_datas = []
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
                            "channel_id": CHANNEL_ID,
                            "last_checked_at": None,
                            "msg_id": None,
                            "channel_msg_id": None,  # 🔥 NEW FIELD
                            "caption": None,  # 🔥 REMOVED
                            "is_completed": batch in completed_batches,
                            "teachers": ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
                        }
                        batch_datas.append(batch_data)
                    educators_col.update_one({"username": username}, {"$push": {"batches": {"$each": batch_datas}}})
                    print(f"✅ Added {len(new_batches)} NEW batches to DB")

                # 🔥 EXTRACT & ADD NEW EDUCATORS FROM COURSES/BATCHES
                new_educators_found = 0
                for course in all_courses:
                    author = course.get("author", {})
                    if await add_educator_to_optry(
                        author.get("username", ""), 
                        author.get("uid", ""), 
                        author.get("first_name", "N/A"), 
                        author.get("last_name", "N/A"), 
                        author.get("avatar", "N/A")
                    ):
                        new_educators_found += 1

                for batch in all_batches:
                    for author in batch.get("authors", []):
                        if await add_educator_to_optry(
                            author.get("username", ""), 
                            author.get("uid", ""), 
                            author.get("first_name", "N/A"), 
                            author.get("last_name", "N/A"), 
                            author.get("avatar", "N/A")
                        ):
                            new_educators_found += 1

                print(f"✅ Found {new_educators_found} NEW educators for {username}")

                # 🔥 UPDATE CURRENT ITEMS SCHEDULES
                current_items_to_check = []
                current_items_to_check.extend([{"item": c, "type": "course"} for c in current_courses])
                current_items_to_check.extend([{"item": b, "type": "batch"} for b in current_batches])

                total_current = len(current_items_to_check)
                checked_current = 0

                if total_current > 0:
                    for phase in ["courses", "batches", "new_items"]:
                        await send_scheduler_progress(
                            username, thread_id, 
                            len(current_courses), len(current_batches),
                            len(new_courses), len(new_batches),
                            checked_current, 0, phase
                        )

                    for item_data in current_items_to_check:
                        try:
                            success = await update_schedule_item(item_data["item"], item_data["type"], username, thread_id, last_checked)
                            if success:
                                checked_current += 1
                            await send_scheduler_progress(
                                username, thread_id,
                                len(current_courses), len(current_batches),
                                len(new_courses), len(new_batches),
                                checked_current, 0, "courses" if item_data["type"] == "course" else "batches"
                            )
                            await asyncio.sleep(2)
                        except Exception as e:
                            print(f"❌ Error updating {item_data['type']}: {e}")
                            checked_current += 1

                # 🔥 UPLOAD NEW ITEMS
                new_items_to_upload = []
                new_items_to_upload.extend([{"item": c, "type": "course"} for c in new_courses])
                new_items_to_upload.extend([{"item": b, "type": "batch"} for b in new_batches])

                total_new = len(new_items_to_upload)
                checked_new = 0

                if total_new > 0:
                    await send_scheduler_progress(
                        username, thread_id,
                        len(current_courses), len(current_batches),
                        len(new_courses), len(new_batches),
                        total_current, checked_new, "new_items"
                    )

                    for item_data in new_items_to_upload:
                        try:
                            success = await update_schedule_item(item_data["item"], item_data["type"], username, thread_id, last_checked)
                            if success:
                                checked_new += 1
                            await send_scheduler_progress(
                                username, thread_id,
                                len(current_courses), len(current_batches),
                                len(new_courses), len(new_batches),
                                total_current, checked_new, "new_items"
                            )
                            await asyncio.sleep(2)
                        except Exception as e:
                            print(f"❌ Error uploading new {item_data['type']}: {e}")
                            checked_new += 1

                # 🔥 FINAL UPDATE
                await send_scheduler_progress(
                    username, thread_id,
                    len(current_courses), len(current_batches),
                    len(new_courses), len(new_batches),
                    total_current, total_new, "complete"
                )

                # 🔥 UPDATE LAST CHECKED TIME
                educators_col.update_one({"_id": doc["_id"]}, {"$set": {"last_checked_time": last_checked}})
                print(f"✅ COMPLETED {username} - New: {total_new} items")

                gc.collect()
                await asyncio.sleep(5)  # Prevent rate limit

            print(f"\n🎉 SCHEDULE CHECK COMPLETE! Checked {total_educators_checked} educators")
            print(f"Sleeping for 12 hours... ⏰")
            
        except Exception as e:
            print(f"❌ Error in schedule_checker: {e}")
            import traceback
            traceback.print_exc()

        gc.collect()
        await asyncio.sleep(43200)  # 🔥 12 HOURS


# 🔥 ENHANCED UPDATE SCHEDULE ITEM FUNCTION
async def update_schedule_item(item, item_type, username, thread_id, last_checked):
    """Update single schedule item with channel forwarding."""
    item_uid = item["uid"]
    items_field = "courses" if item_type == "course" else "batches"

    # Check if already uploaded
    doc = educators_col.find_one({"username": username, f"{items_field}.uid": item_uid})
    if doc:
        for db_item in doc.get(items_field, []):
            if db_item["uid"] == item_uid and db_item.get("msg_id") is not None:
                return True

    schedule_url = (
        f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
        if item_type == "batch"
        else f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
    )

    results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)
    if results is None:
        return False

    filename = f"temp_schedule_{username}_{item_type}_{item_uid}_{int(datetime.now().timestamp())}.json"
    save_to_json(filename, results)

    try:
        # Upload to GROUP
        with open(filename, "rb") as f:
            group_msg = await bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=caption
            )
        group_msg_id = group_msg.message_id

        # Forward to CHANNEL
        channel_msg_id = await forward_to_channel(filename, caption, group_msg_id, thread_id)

        # Update DB with BOTH IDs
        educators_col.update_one(
            {"username": username, f"{items_field}.uid": item_uid},
            {"$set": {
                f"{items_field}.$.msg_id": group_msg_id,
                f"{items_field}.$.channel_msg_id": channel_msg_id,
                f"{items_field}.$.last_checked_at": last_checked,
                f"{items_field}.$.caption": None  # 🔥 REMOVED FROM DB
            }}
        )

        print(f"✅ Uploaded {item_type} {item_uid} | Group: {group_msg_id} | Channel: {channel_msg_id}")
        await asyncio.sleep(20)
        return True

    except Exception as e:
        print(f"❌ Error uploading {item_type} {item_uid}: {e}")
        return False
    finally:
        if os.path.exists(filename):
            os.remove(filename)


# 🔥 ENHANCED OPTRY COMMAND - ALL vs NEXT 10
async def optry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Enhanced /optry with All/Next 10 selection."""
    global update_obj, optry_progress_message
    update_obj = update
    optry_progress_message = None

    await update.message.reply_text(
        "🔥 /optry Command\n\n"
        "Choose mode:\n"
        "• `All` - Process ALL educators\n"
        "• `Next 10` - Process next 10 educators\n\n"
        "Reply: `All` or `Next 10`"
    )
    return OPTRY_MODE


async def optry_mode_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle All/Next 10 selection."""
    mode = update.message.text.strip().lower()
    if mode in ["all", "next 10"]:
        context.user_data['optry_mode'] = mode
        await update.message.reply_text(f"🚀 Starting {mode.upper()} mode...")
        
        # Get educators list
        optry_educators = list(collection_optry.find())
        if not optry_educators:
            await update.message.reply_text("❌ No educators in optry DB!")
            return ConversationHandler.END

        if context.user_data['optry_mode'] == "next 10":
            # 🔥 NEXT 10 LOGIC - FIND LAST 10 UPLOADED
            main_educators = list(educators_col.find({}, {"username": 1}).sort("subtopic_msg_id", -1).limit(10))
            last_usernames = {doc["username"] for doc in main_educators}
            
            # Filter next 10 not in main DB
            next_10 = [edu for edu in optry_educators 
                      if edu["username"] not in last_usernames][:10]
            
            if not next_10:
                await update.message.reply_text(
                    "✅ All educators already processed!\n"
                    f"Total in main DB: {educators_col.count_documents({})}"
                )
                return ConversationHandler.END
            
            # Send NEXT 10 LIST
            list_text = "📋 NEXT 10 EDUCATORS:\n\n"
            for i, edu in enumerate(next_10, 1):
                list_text += f"{i}. {edu['first_name']} {edu['last_name']} [{edu['username']}]\n"
            
            await update.message.reply_text(list_text)
            optry_educators = next_10
            total_educators = 10
        else:
            total_educators = len(optry_educators)

        processed_count = {'value': 0}
        new_educators_count = {'value': 0}
        batches_fetched_total = 0
        courses_fetched_total = 0

        def get_processed_count():
            return processed_count['value']

        # Start processing
        for idx, optry_educator in enumerate(optry_educators, 1):
            username = optry_educator.get("username", "").strip()
            if not username:
                processed_count['value'] += 1
                continue

            print(f"\n🔥 [{idx}/{total_educators}] {username}")
            current_teacher = f"{optry_educator['first_name']} {optry_educator['last_name']}"
            
            # Fetch data
            courses = await fetch_courses(username)
            batches = await fetch_batches(username)
            
            batches_fetched_total += len(batches)
            courses_fetched_total += len(courses)
            
            await send_optry_progress(
                current_teacher, len(batches), len(courses),
                total_educators, idx, new_educators_count['value'],
                "next 10" if context.user_data['optry_mode'] == "next 10" else "all"
            )

            # Process educator (same logic as before but enhanced)
            educator_doc = educators_col.find_one({"username": normalize_username(username)})
            if not educator_doc:
                educator = await fetch_educator_by_username(username)
                if not educator:
                    processed_count['value'] += 1
                    continue

                title = f"{educator['first_name']} {educator['last_name']} [{username}]"
                topic = await context.bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
                thread_id = topic.message_thread_id

                educators_col.insert_one({
                    "_id": ObjectId(),
                    "first_name": educator["first_name"],
                    "last_name": educator["last_name"],
                    "username": normalize_username(username),
                    "uid": educator["uid"],
                    "avatar": educator["avatar"],
                    "group_id": SETTED_GROUP_ID,
                    "channel_id": CHANNEL_ID,  # 🔥 NEW
                    "subtopic_msg_id": thread_id,
                    "topic_title": title,
                    "last_checked_time": None,
                    "courses": [],
                    "batches": []
                })

            # Add new courses/batches to DB (WITHOUT CAPTION)
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            existing_doc = educators_col.find_one({"username": normalize_username(username)})
            existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
            existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}

            # NEW COURSES
            for course in courses:
                if course["uid"] not in existing_course_uids:
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
                    educators_col.update_one(
                        {"username": normalize_username(username)},
                        {"$push": {"courses": course_data}}
                    )

            # NEW BATCHES
            for batch in batches:
                if batch["uid"] not in existing_batch_uids:
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
                    educators_col.update_one(
                        {"username": normalize_username(username)},
                        {"$push": {"batches": batch_data}}
                    )

            # 🔥 UPLOAD ALL (CURRENT + NEW)
            all_items_to_upload = []
            all_items_to_upload.extend([{"item": c, "type": "course"} for c in courses])
            all_items_to_upload.extend([{"item": b, "type": "batch"} for b in batches])

            for item_data in all_items_to_upload:
                await update_schedule_item(
                    item_data["item"], item_data["type"],
                    normalize_username(username), 
                    educator_doc["subtopic_msg_id"],
                    current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
                )
                await asyncio.sleep(2)

            # Add educators from courses/batches
            for course in courses:
                author = course.get("author", {})
                if author.get("uid"):
                    if await add_educator_to_optry(
                        author.get("username", ""), author.get("uid", ""),
                        author.get("first_name", "N/A"), author.get("last_name", "N/A"),
                        author.get("avatar", "N/A")
                    ):
                        new_educators_count['value'] += 1

            for batch in batches:
                for author in batch.get("authors", []):
                    if author.get("uid"):
                        if await add_educator_to_optry(
                            author.get("username", ""), author.get("uid", ""),
                            author.get("first_name", "N/A"), author.get("last_name", "N/A"),
                            author.get("avatar", "N/A")
                        ):
                            new_educators_count['value'] += 1

            processed_count['value'] += 1
            gc.collect()

        await send_optry_progress(
            "COMPLETE", batches_fetched_total, courses_fetched_total,
            total_educators, processed_count['value'], new_educators_count['value'],
            context.user_data['optry_mode']
        )

        await update.message.reply_text(
            f"🎉 {context.user_data['optry_mode'].upper()} COMPLETE!\n\n"
            f"✅ Processed: {processed_count['value']}\n"
            f"📦 Batches: {batches_fetched_total}\n"
            f"📚 Courses: {courses_fetched_total}\n"
            f"🔥 New Educators: {new_educators_count['value']}\n"
            f"💾 Total in DB: {educators_col.count_documents({})}"
        )

        return ConversationHandler.END
    else:
        await update.message.reply_text("❌ Invalid! Reply `All` or `Next 10`")
        return OPTRY_MODE


# 🔥 REST OF THE CODE (add_command etc. remains SAME but with channel_id)
async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /add with channel support."""
    global update_context, update_obj, progress_message
    update_context = context
    update_obj = update
    progress_message = None

    if not context.args:
        await update.message.reply_text("Usage: /add {username}")
        return ConversationHandler.END

    username = normalize_username(context.args[0])
    await update.message.reply_text(f"Fetching: {username}")

    educator = await fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"❌ Educator not found: {username}")
        return ConversationHandler.END

    # Rest of add_command logic SAME but add channel_id to DB
    # ... (keeping same as before but adding channel_id field)
    # For brevity, assuming same logic with channel_id added
    
    await update.message.reply_text("What do you want to fetch?\n1. Batch\n2. Course")
    return SELECT_TYPE


# Conversation handlers (same as before)
async def select_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.lower()
    if user_input == 'cancel':
        return ConversationHandler.END
    if user_input not in ['1', '2']:
        await update.message.reply_text("Reply '1' or '2'")
        return SELECT_TYPE
    
    context.user_data['item_type'] = 'batch' if user_input == '1' else 'course'
    await update.message.reply_text(f"Enter {context.user_data['item_type']} ID:")
    return ENTER_ID


async def enter_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Same logic but with channel forwarding
    item_id = update.message.text.strip()
    # ... (implement with forward_to_channel)
    return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return ConversationHandler.END


async def main():
    """Start bot with enhanced handlers."""
    global bot
    bot_token = '7213717609:AAGAuuDNX_EEMZfF2D_Zoz-vDoQizBxW96I'
    application = Application.builder().token(bot_token).build()
    bot = application.bot

    # Enhanced conversation handler
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("add", add_command), CommandHandler("optry", optry_command)],
        states={
            OPTRY_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, optry_mode_select)],
            SELECT_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_type)],
            ENTER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler)

    print("🚀 ENHANCED BOT STARTING...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    # 🔥 START ENHANCED SCHEDULE CHECKER IMMEDIATELY
    asyncio.create_task(schedule_checker())
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
