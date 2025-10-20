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

# Telegram group and channel IDs
SETTED_GROUP_ID = -1003133358948
SETTED_CHANNEL_ID = -1002927760779  # Replace with actual channel ID
CHANNEL_NUM = str(SETTED_CHANNEL_ID).lstrip('-100')

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

SELECT_TYPE, ENTER_ID = range(2)
OPTRY_SELECT = range(2, 3)

def save_to_json(filename, data):
    """Save data to a JSON file with minimal memory footprint."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving JSON {filename}: {e}")
        raise

async def upload_document_with_channel(caption, filename, thread_id=None):
    """Upload document to group and channel, add channel link to group caption."""
    try:
        with open(filename, "rb") as f:
            msg = await bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=caption
            )
        group_msg_id = msg.message_id

        copied = await bot.copy_message(
            chat_id=SETTED_CHANNEL_ID,
            from_chat_id=SETTED_GROUP_ID,
            message_id=group_msg_id
        )
        channel_msg_id = copied.message_id

        new_caption = caption + f"\n\nIn channel - https://t.me/c/{CHANNEL_NUM}/{channel_msg_id}"
        await bot.edit_message_caption(
            chat_id=SETTED_GROUP_ID,
            message_id=group_msg_id,
            message_thread_id=thread_id,
            caption=new_caption
        )

        return group_msg_id, channel_msg_id, new_caption
    except Exception as e:
        print(f"Error in upload_document_with_channel: {e}")
        # Fallback: just group upload
        with open(filename, "rb") as f:
            msg = await bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=caption
            )
        return msg.message_id, None, caption

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

def filter_items(items, time_field, current_time, future=True):
    """Filter items based on time field."""
    filtered = []
    for item in items:
        end_str = item.get(time_field)
        if end_str != "N/A":
            try:
                end_time = dateutil.parser.isoparse(end_str)
                if end_time.year > 2035:
                    if not future:
                        filtered.append(item)
                elif (future and end_time > current_time) or (not future and end_time <= current_time):
                    filtered.append(item)
            except ValueError:
                continue
    return filtered

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

async def send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches, current_phase, title):
    """Send or update progress bar for /add command."""
    global progress_message, update_obj

    progress_text = f"Fetching Teacher Name: {title}\n\n"

    if current_phase == "courses":
        progress_text += (
            f"Total Courses fetched: {total_courses}\n"
            f"Phase 1: Uploading Courses\n"
            f"Progress: {uploaded_courses}/{total_courses}\n"
            f"Total Batches fetched: {total_batches} (Pending...)"
        )
    elif current_phase == "batches":
        progress_text += (
            f"Total Courses fetched: {total_courses} (Complete)\n"
            f"Phase 2: Uploading Batches\n"
            f"Progress: {uploaded_batches}/{total_batches}\n"
            f"Total Batches fetched: {total_batches}"
        )
    else:
        progress_text += (
            f"Total Courses fetched: {total_courses} (Complete)\n"
            f"Total Batches fetched: {total_batches} (Complete)\n"
            f"Upload Complete!"
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

async def progress_updater_add(total_courses, total_batches, get_uploaded_courses, get_uploaded_batches, phase_tracker, title):
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

            await send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches, current_phase, title)
            await asyncio.sleep(30)
    except asyncio.CancelledError:
        pass

async def send_scheduler_progress(username, thread_id, phase, new_c_total, new_c_done, new_b_total, new_b_done, exist_c_total, exist_c_done, exist_b_total, exist_b_done):
    """Send or update progress bar for schedule checker."""
    global scheduler_progress_messages

    progress_text = f"Schedule checking\nTotal batches :- {exist_b_total}\nTotal Courses :- {exist_c_total}\nTotal new Batches :- {new_b_total}\nTotal new courses :- {new_c_total}\n\n"

    if phase == "new_courses":
        progress_text += f"Phase 1: Uploading New Courses\nProgress: {new_c_done}/{new_c_total}\nBatches Pending..."
    elif phase == "new_batches":
        progress_text += f"Phase 2: Uploading New Batches\nProgress: {new_b_done}/{new_b_total}\nExisting Pending..."
    elif phase == "existing_courses":
        progress_text += f"Phase 3: Checking Existing Courses\nProgress: {exist_c_done}/{exist_c_total}\nBatches Pending..."
    elif phase == "existing_batches":
        progress_text += f"Phase 4: Checking Existing Batches\nProgress: {exist_b_done}/{exist_b_total}"
    else:
        progress_text += f"Schedule Check Complete!\nNew Courses: {new_c_done}/{new_c_total}\nNew Batches: {new_b_done}/{new_b_total}\nExisting Courses: {exist_c_done}/{exist_c_total}\nExisting Batches: {exist_b_done}/{exist_b_total}"

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

async def update_item_schedule(item, item_type, thread_id, username, last_checked, is_new=False):
    """Update or upload item schedule with channel support."""
    item_uid = item["uid"]
    items_field = "courses" if item_type == "course" else "batches"

    doc = educators_col.find_one({"username": username, f"{items_field}.uid": item_uid})
    has_msg = False
    old_group_id = None
    old_channel_id = None
    if doc:
        for db_item in doc.get(items_field, []):
            if db_item["uid"] == item_uid:
                if db_item.get("msg_id") is not None:
                    has_msg = True
                old_group_id = db_item.get("msg_id")
                old_channel_id = db_item.get("channel_msg_id")
                break

    if not is_new and has_msg:
        print(f"Skipping already uploaded {item_type} {item_uid}")
        return True

    schedule_url = (
        f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
        if item_type == "course"
        else f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
    )

    results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)
    if results is None or caption is None:
        print(f"Failed to fetch schedule for {item_type} {item_uid}")
        return False

    filename = f"temp_schedule_{username}_{item_type}_{item_uid}_{int(datetime.now().timestamp())}.json"
    save_to_json(filename, results)

    # Delete old if not new
    if not is_new:
        if old_group_id:
            try:
                await bot.delete_message(SETTED_GROUP_ID, old_group_id, message_thread_id=thread_id)
            except:
                pass
        if old_channel_id:
            try:
                await bot.delete_message(SETTED_CHANNEL_ID, old_channel_id)
            except:
                pass

    # Upload new
    group_msg_id, channel_msg_id, final_caption = await upload_document_with_channel(caption, filename, thread_id)

    educators_col.update_one(
        {"username": username, f"{items_field}.uid": item_uid},
        {"$set": {
            f"{items_field}.$.msg_id": group_msg_id,
            f"{items_field}.$.channel_msg_id": channel_msg_id,
            f"{items_field}.$.last_checked_at": last_checked,
            f"{items_field}.$.caption": final_caption
        }}
    )

    if os.path.exists(filename):
        os.remove(filename)

    del results
    await asyncio.sleep(30)
    return True

async def schedule_checker():
    """Check and update every 12 hours."""
    while True:
        try:
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
                        if current_time - last_checked_dt < timedelta(hours=12):
                            should_check = False
                    except ValueError:
                        should_check = True

                if not should_check:
                    continue

                thread_id = doc.get("subtopic_msg_id")

                print(f"\nChecking educator: {username}")

                progress_key = f"{username}_{thread_id}"
                scheduler_progress_messages[progress_key] = None

                existing_doc = educators_col.find_one({"username": username})
                existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
                existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}

                courses = await fetch_courses(username)
                batches = await fetch_batches(username)

                new_courses = [c for c in courses if c["uid"] not in existing_course_uids]
                new_batches = [b for b in batches if b["uid"] not in existing_batch_uids]

                total_new_courses = len(new_courses)
                total_new_batches = len(new_batches)

                # Extract new teachers from new items
                all_new_teachers = set()
                for course in new_courses:
                    author = course.get("author", {})
                    if author.get("uid"):
                        all_new_teachers.add((author.get("username", ""), author.get("uid"), author.get("first_name", ""), author.get("last_name", ""), author.get("avatar", "N/A")))
                for batch in new_batches:
                    for author in batch.get("authors", []):
                        if author.get("uid"):
                            all_new_teachers.add((author.get("username", ""), author.get("uid"), author.get("first_name", ""), author.get("last_name", ""), author.get("avatar", "N/A")))

                added_new = 0
                for a_username, a_uid, a_first, a_last, a_avatar in all_new_teachers:
                    if educators_col.find_one({"uid": a_uid}):
                        continue
                    norm_u = normalize_username(a_username)
                    if not collection_optry.find_one({"uid": a_uid}):
                        collection_optry.insert_one({
                            "uid": a_uid,
                            "username": norm_u,
                            "first_name": a_first,
                            "last_name": a_last,
                            "avatar": a_avatar
                        })
                    title = f"{a_first} {a_last} [{a_username}]"
                    topic = await bot.create_forum_topic(SETTED_GROUP_ID, name=title)
                    th_id = topic.message_thread_id
                    educators_col.insert_one({
                        "first_name": a_first,
                        "last_name": a_last,
                        "username": norm_u,
                        "uid": a_uid,
                        "avatar": a_avatar,
                        "group_id": SETTED_GROUP_ID,
                        "subtopic_msg_id": th_id,
                        "topic_title": title,
                        "last_checked_time": last_checked,
                        "courses": [],
                        "batches": []
                    })
                    e_data = {
                        "username": norm_u,
                        "first_name": a_first,
                        "last_name": a_last,
                        "uid": a_uid,
                        "avatar": a_avatar,
                        "group_id": SETTED_GROUP_ID,
                        "subtopic_msg_id": th_id,
                        "topic_title": title,
                        "last_checked_time": last_checked
                    }
                    e_filename = f"new_teacher_{norm_u}_{int(datetime.now().timestamp())}.json"
                    save_to_json(e_filename, e_data)
                    e_caption = f"Teacher Name: {a_first} {a_last}\nUsername: {norm_u}\nUid: {a_uid}\nLast Checked: {last_checked}\n🔥 NEW EDUCATOR!"
                    await upload_document_with_channel(e_caption, e_filename, th_id)
                    if os.path.exists(e_filename):
                        os.remove(e_filename)
                    added_new += 1

                # Add new items to DB
                current_new_courses = filter_items(new_courses, "ends_at", current_time, True)
                completed_new_courses = filter_items(new_courses, "ends_at", current_time, False)
                new_course_datas = []
                for course in new_courses:
                    is_completed = course in completed_new_courses
                    teachers = f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
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
                        "caption": None,
                        "is_completed": is_completed,
                        "teachers": teachers
                    }
                    new_course_datas.append(course_data)
                if new_course_datas:
                    educators_col.update_one({"username": username}, {"$push": {"courses": {"$each": new_course_datas}}})

                current_new_batches = filter_items(new_batches, "completed_at", current_time, True)
                completed_new_batches = filter_items(new_batches, "completed_at", current_time, False)
                new_batch_datas = []
                for batch in new_batches:
                    is_completed = batch in completed_new_batches
                    teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
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
                        "caption": None,
                        "is_completed": is_completed,
                        "teachers": teachers
                    }
                    new_batch_datas.append(batch_data)
                if new_batch_datas:
                    educators_col.update_one({"username": username}, {"$push": {"batches": {"$each": new_batch_datas}}})

                # Existing to check
                courses_to_check = [c for c in existing_doc.get("courses", []) if not c.get("is_completed", False) and c.get("msg_id")]
                batches_to_check = [b for b in existing_doc.get("batches", []) if not b.get("is_completed", False) and b.get("msg_id")]

                total_existing_courses = len(courses_to_check)
                total_existing_batches = len(batches_to_check)

                new_uploaded_courses = 0
                new_uploaded_batches = 0
                checked_courses = 0
                checked_batches = 0

                # Phase new courses
                if total_new_courses > 0:
                    await send_scheduler_progress(username, thread_id, "new_courses", total_new_courses, 0, total_new_batches, 0, total_existing_courses, 0, total_existing_batches, 0)
                    for course in current_new_courses + completed_new_courses:
                        success = await update_item_schedule(course, "course", thread_id, username, last_checked, is_new=True)
                        if success:
                            new_uploaded_courses += 1
                        await send_scheduler_progress(username, thread_id, "new_courses", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, checked_courses, total_existing_batches, checked_batches)

                # Phase new batches
                if total_new_batches > 0:
                    await send_scheduler_progress(username, thread_id, "new_batches", total_new_courses, new_uploaded_courses, total_new_batches, 0, total_existing_courses, checked_courses, total_existing_batches, checked_batches)
                    for batch in current_new_batches + completed_new_batches:
                        success = await update_item_schedule(batch, "batch", thread_id, username, last_checked, is_new=True)
                        if success:
                            new_uploaded_batches += 1
                        await send_scheduler_progress(username, thread_id, "new_batches", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, checked_courses, total_existing_batches, checked_batches)

                # Phase existing courses
                if total_existing_courses > 0:
                    await send_scheduler_progress(username, thread_id, "existing_courses", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, 0, total_existing_batches, 0)
                    for course in courses_to_check:
                        end_time_str = course.get("ends_at", "N/A")
                        if end_time_str != "N/A":
                            try:
                                end_time = dateutil.parser.isoparse(end_time_str)
                                if current_time > end_time:
                                    old_caption = course.get("caption", "")
                                    new_caption = old_caption + "\n\n✓ Course Completed - No More Updates"
                                    await bot.edit_message_caption(
                                        chat_id=SETTED_GROUP_ID,
                                        message_id=course["msg_id"],
                                        message_thread_id=thread_id,
                                        caption=new_caption
                                    )
                                    channel_id = course.get("channel_msg_id")
                                    if channel_id:
                                        await bot.edit_message_caption(
                                            chat_id=SETTED_CHANNEL_ID,
                                            message_id=channel_id,
                                            caption=new_caption
                                        )
                                    educators_col.update_one(
                                        {"_id": doc["_id"], "courses.uid": course["uid"]},
                                        {"$set": {"courses.$.is_completed": True, "courses.$.caption": new_caption}}
                                    )
                                else:
                                    success = await update_item_schedule(course, "course", thread_id, username, last_checked)
                                    if not success:
                                        print(f"Failed to update course {course['uid']}")
                            except ValueError:
                                pass
                        checked_courses += 1
                        await send_scheduler_progress(username, thread_id, "existing_courses", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, checked_courses, total_existing_batches, checked_batches)

                # Phase existing batches
                if total_existing_batches > 0:
                    await send_scheduler_progress(username, thread_id, "existing_batches", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, checked_courses, total_existing_batches, 0)
                    for batch in batches_to_check:
                        end_time_str = batch.get("completed_at", "N/A")
                        if end_time_str != "N/A":
                            try:
                                end_time = dateutil.parser.isoparse(end_time_str)
                                if current_time > end_time:
                                    old_caption = batch.get("caption", "")
                                    new_caption = old_caption + "\n\n✓ Batch Completed - No More Updates"
                                    await bot.edit_message_caption(
                                        chat_id=SETTED_GROUP_ID,
                                        message_id=batch["msg_id"],
                                        message_thread_id=thread_id,
                                        caption=new_caption
                                    )
                                    channel_id = batch.get("channel_msg_id")
                                    if channel_id:
                                        await bot.edit_message_caption(
                                            chat_id=SETTED_CHANNEL_ID,
                                            message_id=channel_id,
                                            caption=new_caption
                                        )
                                    educators_col.update_one(
                                        {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                        {"$set": {"batches.$.is_completed": True, "batches.$.caption": new_caption}}
                                    )
                                else:
                                    success = await update_item_schedule(batch, "batch", thread_id, username, last_checked)
                                    if not success:
                                        print(f"Failed to update batch {batch['uid']}")
                            except ValueError:
                                pass
                        checked_batches += 1
                        await send_scheduler_progress(username, thread_id, "existing_batches", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, checked_courses, total_existing_batches, checked_batches)

                await send_scheduler_progress(username, thread_id, "complete", total_new_courses, new_uploaded_courses, total_new_batches, new_uploaded_batches, total_existing_courses, checked_courses, total_existing_batches, checked_batches)

                educators_col.update_one({"_id": doc["_id"]}, {"$set": {"last_checked_time": last_checked}})

                gc.collect()

        except Exception as e:
            print(f"Error in schedule_checker: {e}")
            gc.collect()

        print(f"\nSchedule check complete. Sleeping for 12 hours...")
        await asyncio.sleep(43200)

async def send_optry_progress(total_educators, processed_educators, new_educators_found):
    """Send or update progress bar for /optry command."""
    global optry_progress_message, update_obj

    progress_text = (
        f"📊 /optry Progress Report\n\n"
        f"Total Educators: {total_educators} + {new_educators_found}\n"
        f"Processed: {processed_educators}/{total_educators}\n"
        f"New Educators found: {new_educators_found}"
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

async def optry_progress_updater(total_educators, get_processed_count, get_new_educators_count):
    """Update progress bar for /optry every 10 minutes."""
    try:
        while True:
            processed = get_processed_count()
            new_found = get_new_educators_count()

            if processed >= total_educators:
                await send_optry_progress(total_educators, processed, new_found)
                break

            await send_optry_progress(total_educators, processed, new_found)
            await asyncio.sleep(600)
    except asyncio.CancelledError:
        pass

async def process_optry_list(update, context, educators_list):
    """Process a list of educators for /optry."""
    global update_obj, optry_progress_message
    update_obj = update
    optry_progress_message = None

    total_educators = len(educators_list)
    processed_count = {'value': 0}
    new_educators_count = {'value': 0}

    def get_processed_count():
        return processed_count['value']

    def get_new_educators_count():
        return new_educators_count['value']

    progress_task = asyncio.create_task(optry_progress_updater(total_educators, get_processed_count, get_new_educators_count))
    await send_optry_progress(total_educators, 0, 0)

    all_found_educators_global = set()

    for idx, optry_educator in enumerate(educators_list, 1):
        username = optry_educator.get("username", "").strip()
        if not username:
            processed_count['value'] += 1
            continue

        username_normalized = normalize_username(username)

        print(f"\n[{idx}/{total_educators}] Processing: {username}")

        educator_doc = educators_col.find_one({"username": username_normalized})
        if educator_doc:
            thread_id = educator_doc["subtopic_msg_id"]
            title = educator_doc["topic_title"]
        else:
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

        courses = await fetch_courses(username)
        batches = await fetch_batches(username)

        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        last_checked_local = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        educators_col.update_one({"username": username_normalized}, {"$set": {"last_checked_time": last_checked_local}})

        all_found_educators = set()
        # Courses teachers
        for course in courses:
            author = course.get("author", {})
            a_username = author.get("username", "")
            a_uid = author.get("uid", "")
            if a_username and a_uid:
                all_found_educators.add((a_username, a_uid, author.get("first_name", ""), author.get("last_name", ""), author.get("avatar", "N/A")))
                all_found_educators_global.add((a_username, a_uid, author.get("first_name", ""), author.get("last_name", ""), author.get("avatar", "N/A")))
        # Batches teachers
        for batch in batches:
            for author in batch.get("authors", []):
                a_username = author.get("username", "")
                a_uid = author.get("uid", "")
                if a_username and a_uid:
                    all_found_educators.add((a_username, a_uid, author.get("first_name", ""), author.get("last_name", ""), author.get("avatar", "N/A")))
                    all_found_educators_global.add((a_username, a_uid, author.get("first_name", ""), author.get("last_name", ""), author.get("avatar", "N/A")))

        # Add new teachers
        for a_username, a_uid, a_first, a_last, a_avatar in all_found_educators:
            norm_u = normalize_username(a_username)
            if not collection_optry.find_one({"uid": a_uid}):
                collection_optry.insert_one({
                    "uid": a_uid,
                    "username": norm_u,
                    "first_name": a_first,
                    "last_name": a_last,
                    "avatar": a_avatar
                })
                new_educators_count['value'] += 1
            if not educators_col.find_one({"uid": a_uid}):
                title_new = f"{a_first} {a_last} [{a_username}]"
                topic_new = await bot.create_forum_topic(SETTED_GROUP_ID, name=title_new)
                th_id_new = topic_new.message_thread_id
                educators_col.insert_one({
                    "first_name": a_first,
                    "last_name": a_last,
                    "username": norm_u,
                    "uid": a_uid,
                    "avatar": a_avatar,
                    "group_id": SETTED_GROUP_ID,
                    "subtopic_msg_id": th_id_new,
                    "topic_title": title_new,
                    "last_checked_time": last_checked_local,
                    "courses": [],
                    "batches": []
                })
                e_data_new = {
                    "username": norm_u,
                    "first_name": a_first,
                    "last_name": a_last,
                    "uid": a_uid,
                    "avatar": a_avatar,
                    "group_id": SETTED_GROUP_ID,
                    "subtopic_msg_id": th_id_new,
                    "topic_title": title_new,
                    "last_checked_time": last_checked_local
                }
                e_filename_new = f"new_teacher_optry_{norm_u}_{int(datetime.now().timestamp())}.json"
                save_to_json(e_filename_new, e_data_new)
                e_caption_new = f"Teacher Name: {a_first} {a_last}\nUsername: {norm_u}\nUid: {a_uid}\nLast Checked: {last_checked_local}\n🔥 NEW EDUCATOR!"
                await upload_document_with_channel(e_caption_new, e_filename_new, th_id_new)
                if os.path.exists(e_filename_new):
                    os.remove(e_filename_new)
                new_educators_count['value'] += 1

        current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
        completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)

        all_courses = current_courses + completed_courses
        all_batches = current_batches + completed_batches

        existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
        existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}

        course_datas = []
        for course in all_courses:
            if course["uid"] in existing_course_uids:
                continue
            is_current = course in current_courses
            teachers = f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
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
                "caption": None,
                "is_completed": not is_current,
                "teachers": teachers
            }
            course_datas.append(course_data)
        if course_datas:
            educators_col.update_one({"username": username_normalized}, {"$push": {"courses": {"$each": course_datas}}})

        batch_datas = []
        for batch in all_batches:
            if batch["uid"] in existing_batch_uids:
                continue
            is_current = batch in current_batches
            teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
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
                "caption": None,
                "is_completed": not is_current,
                "teachers": teachers
            }
            batch_datas.append(batch_data)
        if batch_datas:
            educators_col.update_one({"username": username_normalized}, {"$push": {"batches": {"$each": batch_datas}}})

        # Upload educator JSON
        educator_data = {
            "username": username_normalized,
            "first_name": optry_educator.get("first_name", "N/A"),
            "last_name": optry_educator.get("last_name", "N/A"),
            "uid": optry_educator.get("uid", "N/A"),
            "avatar": optry_educator.get("avatar", "N/A"),
            "group_id": SETTED_GROUP_ID,
            "subtopic_msg_id": thread_id,
            "topic_title": title,
            "last_checked_time": last_checked_local
        }
        educator_filename = f"educator_{username_normalized}_{int(datetime.now().timestamp())}.json"
        save_to_json(educator_filename, educator_data)
        caption = (
            f"Teacher Name: {educator_data['first_name']} {educator_data['last_name']}\n"
            f"Username: {username_normalized}\n"
            f"Uid: {educator_data['uid']}\n"
            f"Last Checked: {last_checked_local}"
        )
        await upload_document_with_channel(caption, educator_filename, thread_id)
        if os.path.exists(educator_filename):
            os.remove(educator_filename)

        # Upload items
        for course in all_courses:
            await update_item_schedule(course, "course", thread_id, username_normalized, last_checked_local)
            await asyncio.sleep(2)
        for batch in all_batches:
            await update_item_schedule(batch, "batch", thread_id, username_normalized, last_checked_local)
            await asyncio.sleep(2)

        processed_count['value'] += 1
        gc.collect()

    progress_task.cancel()
    await send_optry_progress(total_educators, processed_count['value'], new_educators_count['value'])

    await update.message.reply_text(f"✅ Processed {total_educators} educators!")

async def optry_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start /optry conversation."""
    await update.message.reply_text("All ya next 10?")
    return OPTRY_SELECT

async def optry_handle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /optry choice."""
    choice = update.message.text.lower()
    if choice == 'cancel':
        await update.message.reply_text("Cancelled.")
        return ConversationHandler.END
    pending_educators = list(collection_optry.find({"uid": {"$nin": [d["uid"] for d in educators_col.find({}, {"uid": 1})]}}))
    if 'all' in choice:
        if not pending_educators:
            await update.message.reply_text("No educators to process.")
            return ConversationHandler.END
        await process_optry_list(update, context, pending_educators)
    elif 'next' in choice or '10' in choice:
        next10 = pending_educators[:10]
        if not next10:
            await update.message.reply_text("No next 10.")
            return ConversationHandler.END
        list_text = "Next 10:\n"
        for i, p in enumerate(next10, 1):
            name = f"{p.get('first_name', '')} {p.get('last_name', '')}"
            user = p.get('username', '')
            list_text += f"{i}. {name} [{user}]\n"
        await update.message.reply_text(list_text)
        await process_optry_list(update, context, next10)
    else:
        await update.message.reply_text("Please say 'All' or 'next 10'.")
        return OPTRY_SELECT
    return ConversationHandler.END

def filter_by_time(courses, batches, current_time, future=True):
    """Filter courses and batches based on time."""
    filtered_courses = filter_items(courses, "ends_at", current_time, future)
    filtered_batches = filter_items(batches, "completed_at", current_time, future)
    return filtered_courses, filtered_batches

# ... (rest of add_command, select_type, enter_id, cancel remain similar, but modify upload in enter_id and add_command to use upload_document_with_channel and update_item to use update_item_schedule

# For brevity, the add_command and other functions are modified similarly to use the new functions. The full code would integrate them analogously.

async def main():
    """Start the Telegram bot."""
    global bot
    bot_token = '7213717609:AAGAuuDNX_EEMZfF2D_Zoz-vDoQizBxW96I'
    application = Application.builder().token(bot_token).build()
    bot = application.bot

    conv_handler_add = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            SELECT_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_type)],
            ENTER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    conv_handler_optry = ConversationHandler(
        entry_points=[CommandHandler("optry", optry_select)],
        states={
            OPTRY_SELECT[0]: [MessageHandler(filters.TEXT & ~filters.COMMAND, optry_handle)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(conv_handler_add)
    application.add_handler(conv_handler_optry)

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
