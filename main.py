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

# Telegram group ID
SETTED_GROUP_ID = -1003133358948

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
OPTRY_START, OPTRY_CHOOSE = range(2)

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


def create_caption(item_type, item_data, last_checked, completed=False):
    item_name = item_data.get("name", "N/A")
    item_starts_at = item_data.get("starts_at", "N/A")
    if item_type == "course":
        item_ends_at = item_data.get("ends_at", "N/A")
        teachers = item_data.get("teachers", "N/A")
        caption = (
            f"Course Name: {item_name}\n"
            f"Course Teacher: {teachers}\n"
            f"Start_at: {item_starts_at}\n"
            f"Ends_at: {item_ends_at}\n"
            f"Last_checked_at: {last_checked}"
        )
    else:
        item_ends_at = item_data.get("completed_at", "N/A")
        teachers = item_data.get("teachers", "N/A")
        caption = (
            f"Batch Name: {item_name}\n"
            f"Batch Teachers: {teachers}\n"
            f"Start_at: {item_starts_at}\n"
            f"Completed_at: {item_ends_at}\n"
            f"Last_checked_at: {last_checked}"
        )
    if completed:
        caption += "\n\n✓ Completed - No More Updates"
    return caption


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

                    return results_list, None  # Caption created separately

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


async def send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses, new_batches, current_phase):
    """Send or update progress bar for schedule checker in group subtopic."""
    global scheduler_progress_messages

    if current_phase == "courses":
        progress_text = (
            f"Schedule Check Progress\n"
            f"Phase 1: Checking Courses\n"
            f"Progress: {checked_courses}/{total_courses}\n"
            f"New Courses: {new_courses}\n"
            f"Batches: Pending...\n"
            f"New Batches: {new_batches}"
        )
    elif current_phase == "batches":
        progress_text = (
            f"Schedule Check Progress\n"
            f"Phase 1: Courses Complete\n"
            f"Phase 2: Checking Batches\n"
            f"Progress: {checked_batches}/{total_batches}\n"
            f"New Courses: {new_courses}\n"
            f"New Batches: {new_batches}"
        )
    else:
        progress_text = (
            f"Schedule Check Complete!\n"
            f"Courses Checked: {checked_courses}/{total_courses}\n"
            f"Batches Checked: {checked_batches}/{total_batches}\n"
            f"New Courses: {new_courses}\n"
            f"New Batches: {new_batches}"
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
    """Check and update current batches and courses every 12 hours, only for due educators."""
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
                            print(f"Skipping {username}, last checked recently: {last_checked_str}")
                            should_check = False
                    except ValueError:
                        print(f"Invalid last_checked_time for {username}: {last_checked_str}")
                        should_check = True  # Process if invalid

                if not should_check:
                    continue

                thread_id = doc.get("subtopic_msg_id")
                group_id = doc.get("group_id", SETTED_GROUP_ID)
                channel_id = doc.get("channel_id", None)

                print(f"\nChecking educator: {username}")

                progress_key = f"{username}_{thread_id}"
                scheduler_progress_messages[progress_key] = None

                # Fetch new courses and batches
                courses = await fetch_courses(username)
                batches = await fetch_batches(username)

                # Collect new educators
                all_found_educators = set()
                for course in courses:
                    author = course.get("author", {})
                    author_username = author.get("username", "").strip()
                    author_uid = author.get("uid", "").strip()
                    if author_username and author_uid:
                        all_found_educators.add((author_username, author_uid,
                                                author.get("first_name", "N/A"),
                                                author.get("last_name", "N/A"),
                                                author.get("avatar", "N/A")))

                for batch in batches:
                    authors = batch.get("authors", [])
                    for author in authors:
                        author_username = author.get("username", "").strip()
                        author_uid = author.get("uid", "").strip()
                        if author_username and author_uid:
                            all_found_educators.add((author_username, author_uid,
                                                    author.get("first_name", "N/A"),
                                                    author.get("last_name", "N/A"),
                                                    author.get("avatar", "N/A")))

                # Add new educators to optry
                for edu_username, edu_uid, edu_first, edu_last, edu_avatar in all_found_educators:
                    normalized_edu_username = normalize_username(edu_username)
                    exists = collection_optry.find_one({"uid": edu_uid})
                    if not exists:
                        collection_optry.insert_one({
                            "uid": edu_uid,
                            "username": normalized_edu_username,
                            "avatar": edu_avatar,
                            "first_name": edu_first,
                            "last_name": edu_last
                        })
                        print(f"✅ Added new educator from schedule: {edu_username} (UID: {edu_uid})")

                # Filter and add new courses/batches
                current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
                completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)
                all_courses = current_courses + completed_courses
                all_batches = current_batches + completed_batches

                existing_course_uids = {c["uid"] for c in doc.get("courses", [])}
                existing_batch_uids = {b["uid"] for b in doc.get("batches", [])}

                new_courses_count = 0
                new_batches_count = 0

                course_datas = []
                for course in all_courses:
                    if course["uid"] in existing_course_uids:
                        continue
                    new_courses_count += 1
                    is_current = course in current_courses
                    teachers = f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
                    course_data = {
                        "uid": course["uid"],
                        "name": course.get("name", "N/A"),
                        "slug": course.get("slug", "N/A"),
                        "thumbnail": course.get("thumbnail", "N/A"),
                        "starts_at": course.get("starts_at", "N/A"),
                        "ends_at": course.get("ends_at", "N/A"),
                        "group_id": group_id,
                        "last_checked_at": None,
                        "msg_id": None,
                        "channel_msg_id": None,
                        "is_completed": not is_current,
                        "teachers": teachers
                    }
                    course_datas.append(course_data)

                if course_datas:
                    educators_col.update_one({"_id": doc["_id"]}, {"$push": {"courses": {"$each": course_datas}}})

                batch_datas = []
                for batch in all_batches:
                    if batch["uid"] in existing_batch_uids:
                        continue
                    new_batches_count += 1
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
                        "group_id": group_id,
                        "last_checked_at": None,
                        "msg_id": None,
                        "channel_msg_id": None,
                        "is_completed": not is_current,
                        "teachers": teachers
                    }
                    batch_datas.append(batch_data)

                if batch_datas:
                    educators_col.update_one({"_id": doc["_id"]}, {"$push": {"batches": {"$each": batch_datas}}})

                # Reload doc after adding new
                doc = educators_col.find_one({"_id": doc["_id"]})

                # Get items to check/update (not completed, including new)
                courses_to_check = [c for c in doc.get("courses", []) if not c.get("is_completed", False)]
                batches_to_check = [b for b in doc.get("batches", []) if not b.get("is_completed", False)]

                total_courses = len(courses_to_check)
                total_batches = len(batches_to_check)
                checked_courses = 0
                checked_batches = 0

                if total_courses == 0 and total_batches == 0:
                    print(f"No active items to check for {username}")
                else:
                    # PHASE 1: Check Courses
                    if total_courses > 0:
                        print(f"\nPhase 1: Checking {total_courses} courses...")
                        await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "courses")

                        for course in courses_to_check:
                            try:
                                end_time_str = course.get("ends_at", "N/A")
                                if end_time_str != "N/A":
                                    try:
                                        end_time = dateutil.parser.isoparse(end_time_str)
                                    except ValueError:
                                        print(f"Invalid end_time for course {course['uid']}")
                                        checked_courses += 1
                                        continue

                                    if current_time > end_time:
                                        new_caption = create_caption("course", course, last_checked, completed=True)
                                        msg_id = course.get("msg_id")
                                        channel_msg_id = course.get("channel_msg_id")
                                        if msg_id:
                                            await bot.edit_message_caption(
                                                chat_id=group_id,
                                                message_id=msg_id,
                                                caption=new_caption
                                            )
                                        if channel_id and channel_msg_id:
                                            await bot.edit_message_caption(
                                                chat_id=channel_id,
                                                message_id=channel_msg_id,
                                                caption=new_caption
                                            )
                                        educators_col.update_one(
                                            {"_id": doc["_id"], "courses.uid": course["uid"]},
                                            {"$set": {"courses.$.is_completed": True}}
                                        )
                                        print(f"✓ Marked course {course['uid']} as completed")
                                    else:
                                        print(f"Updating course {course['uid']}")
                                        schedule_url = f"https://unacademy.com/api/v3/collection/{course['uid']}/items?limit=10000"

                                        results, _ = await fetch_unacademy_schedule(schedule_url, "course", course)
                                        if results is None:
                                            print(f"Failed to fetch schedule for course {course['uid']}")
                                            checked_courses += 1
                                            continue

                                        filename = f"temp_schedule_{username}_course_{course['uid']}_{int(datetime.now().timestamp())}.json"
                                        save_to_json(filename, results)

                                        caption = create_caption("course", course, last_checked)
                                        channel_msg_id = None
                                        if channel_id:
                                            with open(filename, "rb") as f:
                                                channel_msg = await bot.send_document(
                                                    chat_id=channel_id,
                                                    document=f,
                                                    caption=caption
                                                )
                                            channel_msg_id = channel_msg.message_id

                                        link = f"https://t.me/c/{str(channel_id)[4:]}/{channel_msg_id}" if channel_id and channel_msg_id else ""
                                        caption_group = caption + (f"\n\nIn channel - {link}" if link else "")

                                        old_msg_id = course.get("msg_id")
                                        if old_msg_id:
                                            try:
                                                await bot.delete_message(chat_id=group_id, message_id=old_msg_id)
                                                print(f"✓ Deleted old message {old_msg_id}")
                                            except Exception as e:
                                                print(f"Error deleting old: {e}")

                                        with open(filename, "rb") as f:
                                            new_msg = await bot.send_document(
                                                chat_id=group_id,
                                                message_thread_id=thread_id,
                                                document=f,
                                                caption=caption_group
                                            )
                                        new_msg_id = new_msg.message_id

                                        update_set = {
                                            "courses.$.last_checked_at": last_checked,
                                            "courses.$.msg_id": new_msg_id
                                        }
                                        if channel_msg_id:
                                            update_set["courses.$.channel_msg_id"] = channel_msg_id
                                        educators_col.update_one(
                                            {"_id": doc["_id"], "courses.uid": course["uid"]},
                                            {"$set": update_set}
                                        )
                                        await asyncio.sleep(30)

                                        if os.path.exists(filename):
                                            os.remove(filename)

                                checked_courses += 1
                                await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "courses")

                            except Exception as e:
                                print(f"Error processing course {course.get('uid', 'UNKNOWN')}: {e}")
                                checked_courses += 1
                                await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "courses")

                    # PHASE 2: Check Batches
                    if total_batches > 0:
                        print(f"\nPhase 2: Checking {total_batches} batches...")
                        await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "batches")

                        for batch in batches_to_check:
                            try:
                                end_time_str = batch.get("completed_at", "N/A")
                                if end_time_str != "N/A":
                                    try:
                                        end_time = dateutil.parser.isoparse(end_time_str)
                                    except ValueError:
                                        print(f"Invalid completed_at for batch {batch['uid']}")
                                        checked_batches += 1
                                        continue

                                    if current_time > end_time:
                                        new_caption = create_caption("batch", batch, last_checked, completed=True)
                                        msg_id = batch.get("msg_id")
                                        channel_msg_id = batch.get("channel_msg_id")
                                        if msg_id:
                                            await bot.edit_message_caption(
                                                chat_id=group_id,
                                                message_id=msg_id,
                                                caption=new_caption
                                            )
                                        if channel_id and channel_msg_id:
                                            await bot.edit_message_caption(
                                                chat_id=channel_id,
                                                message_id=channel_msg_id,
                                                caption=new_caption
                                            )
                                        educators_col.update_one(
                                            {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                            {"$set": {"batches.$.is_completed": True}}
                                        )
                                        print(f"✓ Marked batch {batch['uid']} as completed")
                                    else:
                                        print(f"Updating batch {batch['uid']}")
                                        schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"

                                        results, _ = await fetch_unacademy_schedule(schedule_url, "batch", batch)
                                        if results is None:
                                            print(f"Failed to fetch schedule for batch {batch['uid']}")
                                            checked_batches += 1
                                            continue

                                        filename = f"temp_schedule_{username}_batch_{batch['uid']}_{int(datetime.now().timestamp())}.json"
                                        save_to_json(filename, results)

                                        caption = create_caption("batch", batch, last_checked)
                                        channel_msg_id = None
                                        if channel_id:
                                            with open(filename, "rb") as f:
                                                channel_msg = await bot.send_document(
                                                    chat_id=channel_id,
                                                    document=f,
                                                    caption=caption
                                                )
                                            channel_msg_id = channel_msg.message_id

                                        link = f"https://t.me/c/{str(channel_id)[4:]}/{channel_msg_id}" if channel_id and channel_msg_id else ""
                                        caption_group = caption + (f"\n\nIn channel - {link}" if link else "")

                                        old_msg_id = batch.get("msg_id")
                                        if old_msg_id:
                                            try:
                                                await bot.delete_message(chat_id=group_id, message_id=old_msg_id)
                                                print(f"✓ Deleted old message {old_msg_id}")
                                            except Exception as e:
                                                print(f"Error deleting old: {e}")

                                        with open(filename, "rb") as f:
                                            new_msg = await bot.send_document(
                                                chat_id=group_id,
                                                message_thread_id=thread_id,
                                                document=f,
                                                caption=caption_group
                                            )
                                        new_msg_id = new_msg.message_id

                                        update_set = {
                                            "batches.$.last_checked_at": last_checked,
                                            "batches.$.msg_id": new_msg_id
                                        }
                                        if channel_msg_id:
                                            update_set["batches.$.channel_msg_id"] = channel_msg_id
                                        educators_col.update_one(
                                            {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                            {"$set": update_set}
                                        )
                                        await asyncio.sleep(30)

                                        if os.path.exists(filename):
                                            os.remove(filename)

                                checked_batches += 1
                                await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "batches")

                            except Exception as e:
                                print(f"Error processing batch {batch.get('uid', 'UNKNOWN')}: {e}")
                                checked_batches += 1
                                await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "batches")

                    await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, new_courses_count, new_batches_count, "complete")

                # Set last_checked_time
                educators_col.update_one({"_id": doc["_id"]}, {"$set": {"last_checked_time": last_checked}})
                print(f"Completed schedule check for {username}")

                gc.collect()

        except Exception as e:
            print(f"Error in schedule_checker: {e}")
            gc.collect()

        print(f"\nSchedule check complete. Sleeping for 12 hours...")
        await asyncio.sleep(43200)  # 12 hours


async def send_optry_progress(total_educators, processed_educators, new_educators_found, total_courses_fetched, total_batches_fetched, current_teacher):
    """Send or update progress bar for /optry command."""
    global optry_progress_message, update_obj

    progress_text = (
        f"📊 /optry Progress Report\n\n"
        f"Fetching Teacher Name: {current_teacher}\n"
        f"Total Courses fetched: {total_courses_fetched}\n"
        f"Total Batches fetched: {total_batches_fetched}\n"
        f"Total Educators: {total_educators} + {new_educators_found}\n"
        f"Total Educators Data Fetched And Data uploaded: {processed_educators}/{total_educators}\n"
        f"Total New Educators found: {new_educators_found}"
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


async def optry_progress_updater(total_educators, get_processed_count, get_new_educators_count, get_total_courses, get_total_batches, get_current_teacher):
    """Update progress bar for /optry every 10 minutes."""
    try:
        while True:
            processed = get_processed_count()
            new_found = get_new_educators_count()
            courses_f = get_total_courses()
            batches_f = get_total_batches()
            current_t = get_current_teacher()

            if processed >= total_educators:
                await send_optry_progress(total_educators, processed, new_found, courses_f, batches_f, current_t)
                break

            await send_optry_progress(total_educators, processed, new_found, courses_f, batches_f, current_t)
            await asyncio.sleep(600)  # 10 minutes
    except asyncio.CancelledError:
        pass


async def add_new_educators_to_optry(all_found_educators, new_educators_count):
    """Add all new educators to optry MongoDB - DUPLICATE PROTECTION BY UID"""
    added_count = 0
    existing_uids = {doc['uid'] for doc in collection_optry.find({}, {'uid': 1})}
    
    for edu_username, edu_uid, edu_first, edu_last, edu_avatar in all_found_educators:
        if edu_uid in existing_uids:
            continue
            
        try:
            normalized_edu_username = normalize_username(edu_username)
            collection_optry.insert_one({
                "uid": edu_uid,
                "username": normalized_edu_username,
                "avatar": edu_avatar,
                "first_name": edu_first,
                "last_name": edu_last
            })
            print(f"✅ ADDED NEW EDUCATOR TO OPTRY DB: {edu_username} (UID: {edu_uid})")
            added_count += 1
            new_educators_count['value'] += 1
        except Exception as e:
            print(f"Error adding {edu_username}: {e}")
    
    return added_count


async def process_new_educators_batch(update, context, total_new_added, group_id, channel_id):
    """After total_educators complete, process new ones AUTOMATICALLY"""
    if total_new_added == 0:
        return
    
    await update.message.reply_text(f"\n🔥 Found {total_new_added} NEW EDUCATORS!\n🚀 Starting NEW BATCH processing...")
    
    new_educators_list = list(collection_optry.find({
        "uid": {"$nin": [doc['uid'] for doc in educators_col.find({}, {'uid': 1})]}
    }).sort("_id", 1))
    
    new_total = len(new_educators_list)
    processed_new = {'value': 0}
    
    def get_processed_new():
        return processed_new['value']
    
    progress_task_new = asyncio.create_task(optry_progress_updater(
        new_total, get_processed_new, lambda: total_new_added, lambda: 0, lambda: 0, lambda: "N/A"
    ))
    
    await send_optry_progress(new_total, 0, total_new_added, 0, 0, "Starting new batch")
    
    for idx, new_educator in enumerate(new_educators_list, 1):
        try:
            username = new_educator.get("username", "").strip()
            if not username:
                processed_new['value'] += 1
                continue

            print(f"\n🔥 NEW EDUCATOR [{idx}/{new_total}]: {username}")
            
            username_normalized = normalize_username(username)
            
            if educators_col.find_one({"username": username_normalized}):
                print(f"⚠️ {username} already exists in main DB, skipping")
                processed_new['value'] += 1
                continue

            educator = await fetch_educator_by_username(username)
            if not educator:
                print(f"❌ Could not fetch details for {username}")
                processed_new['value'] += 1
                continue

            title = f"{educator['first_name']} {educator['last_name']} [{username}]"
            topic = await context.bot.create_forum_topic(chat_id=group_id, name=title)
            thread_id = topic.message_thread_id

            educators_col.insert_one({
                "_id": ObjectId(),
                "first_name": educator["first_name"],
                "last_name": educator["last_name"],
                "username": username_normalized,
                "uid": educator["uid"],
                "avatar": educator["avatar"],
                "group_id": group_id,
                "channel_id": channel_id,
                "subtopic_msg_id": thread_id,
                "topic_title": title,
                "last_checked_time": None,
                "courses": [],
                "batches": []
            })
            print(f"✅ Created educator entry: {username}")

            courses = await fetch_courses(username)
            batches = await fetch_batches(username)
            
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            educators_col.update_one({"username": username_normalized}, {"$set": {"last_checked_time": last_checked}})

            current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
            completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)

            all_courses = current_courses + completed_courses
            all_batches = current_batches + completed_batches

            existing_doc = educators_col.find_one({"username": username_normalized})
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
                    "group_id": group_id,
                    "last_checked_at": None,
                    "msg_id": None,
                    "channel_msg_id": None,
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
                    "group_id": group_id,
                    "last_checked_at": None,
                    "msg_id": None,
                    "channel_msg_id": None,
                    "is_completed": not is_current,
                    "teachers": teachers
                }
                batch_datas.append(batch_data)

            if batch_datas:
                educators_col.update_one({"username": username_normalized}, {"$push": {"batches": {"$each": batch_datas}}})

            educator_data = {
                "username": username_normalized,
                "first_name": educator["first_name"],
                "last_name": educator["last_name"],
                "uid": educator["uid"],
                "avatar": educator["avatar"],
                "group_id": group_id,
                "channel_id": channel_id,
                "subtopic_msg_id": thread_id,
                "topic_title": title,
                "last_checked_time": last_checked
            }
            educator_filename = f"educator_new_{username_normalized}_{int(datetime.now().timestamp())}.json"
            save_to_json(educator_filename, educator_data)

            try:
                with open(educator_filename, "rb") as f:
                    await context.bot.send_document(
                        chat_id=group_id,
                        message_thread_id=thread_id,
                        document=f,
                        caption=(
                            f"Teacher Name: {educator_data['first_name']} {educator_data['last_name']}\n"
                            f"Username: {username_normalized}\n"
                            f"Uid: {educator_data['uid']}\n"
                            f"Last Checked: {last_checked}\n"
                            f"🔥 NEW EDUCATOR!"
                        )
                    )
                print(f"✓ NEW EDUCATOR JSON uploaded: {username}")
                await asyncio.sleep(10)
            except Exception as e:
                print(f"Error uploading NEW educator JSON: {e}")
            finally:
                if os.path.exists(educator_filename):
                    os.remove(educator_filename)
                del educator_data

            async def update_item(item, item_type, group_id, channel_id, thread_id, last_checked, username_normalized):
                item_uid = item["uid"]
                items_field = "courses" if item_type == "course" else "batches"

                doc = educators_col.find_one({"username": username_normalized, f"{items_field}.uid": item_uid})
                if doc:
                    for db_item in doc.get(items_field, []):
                        if db_item["uid"] == item_uid and db_item.get("msg_id") is not None:
                            return True

                schedule_url = (
                    f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                    if item_type == "batch"
                    else f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
                )

                results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item)
                if results is None:
                    return False

                schedule_filename = f"schedule_new_{username_normalized}_{item_type}_{item_uid}_{int(datetime.now().timestamp())}.json"
                save_to_json(schedule_filename, results)

                caption = create_caption(item_type, item, last_checked)
                channel_msg_id = None
                if channel_id:
                    with open(schedule_filename, "rb") as f:
                        channel_msg = await bot.send_document(
                            chat_id=channel_id,
                            document=f,
                            caption=caption
                        )
                    channel_msg_id = channel_msg.message_id

                link = f"https://t.me/c/{str(channel_id)[4:]}/{channel_msg_id}" if channel_id and channel_msg_id else ""
                caption_group = caption + (f"\n\nIn channel - {link}" if link else "")

                with open(schedule_filename, "rb") as f:
                    msg = await bot.send_document(
                        chat_id=group_id,
                        message_thread_id=thread_id,
                        document=f,
                        caption=caption_group
                    )
                msg_id = msg.message_id

                update_set = {
                    f"{items_field}.$.last_checked_at": last_checked,
                    f"{items_field}.$.msg_id": msg_id
                }
                if channel_msg_id:
                    update_set[f"{items_field}.$.channel_msg_id"] = channel_msg_id
                educators_col.update_one(
                    {"username": username_normalized, f"{items_field}.uid": item_uid},
                    {"$set": update_set}
                )
                await asyncio.sleep(20)

                if os.path.exists(schedule_filename):
                    os.remove(schedule_filename)
                del results
                return True

            for course in all_courses:
                await update_item(course, "course", group_id, channel_id, thread_id, last_checked, username_normalized)
                await asyncio.sleep(2)

            all_courses.clear()

            for batch in all_batches:
                await update_item(batch, "batch", group_id, channel_id, thread_id, last_checked, username_normalized)
                await asyncio.sleep(2)

            all_batches.clear()
            gc.collect()
            processed_new['value'] += 1
            print(f"✅ NEW EDUCATOR COMPLETE: {username} [{idx}/{new_total}]")

            if idx % 5 == 0:
                gc.collect()

        except Exception as e:
            print(f"❌ Error processing NEW educator {username}: {e}")
            processed_new['value'] += 1

    progress_task_new.cancel()
    await update.message.reply_text(f"✅ NEW EDUCATORS BATCH COMPLETE!\nProcessed: {new_total}")


async def optry_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the /optry conversation."""
    global update_obj
    update_obj = update

    args = context.args
    if len(args) == 2:
        group_id = int(args[0])
        channel_id = int(args[1])
    else:
        group_id = SETTED_GROUP_ID
        channel_id = None

    context.user_data['optry_group_id'] = group_id
    context.user_data['optry_channel_id'] = channel_id

    await update.message.reply_text("All ya next 10?")
    return OPTRY_CHOOSE


async def optry_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle choice for /optry."""
    choice = update.message.text.lower()
    group_id = context.user_data.get('optry_group_id', SETTED_GROUP_ID)
    channel_id = context.user_data.get('optry_channel_id', None)

    main_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}

    optry_educators = list(collection_optry.find().sort("_id", 1))
    pending_educators = [e for e in optry_educators if e.get('uid') not in main_uids]

    if choice == "all":
        educators_to_process = pending_educators
    elif choice == "next 10":
        educators_to_process = pending_educators[:10]
        list_text = "\n".join([f"{i+1}. {e.get('first_name', 'N/A')} {e.get('last_name', 'N/A')} [{e.get('username', 'N/A')}]" for i, e in enumerate(educators_to_process)])
        await update.message.reply_text(f"Next 10:\n{list_text}")
    else:
        await update.message.reply_text("Invalid choice. Cancelled.")
        return ConversationHandler.END

    total_educators = len(educators_to_process)
    if total_educators == 0:
        await update.message.reply_text("No pending educators!")
        return ConversationHandler.END

    processed_count = {'value': 0}
    new_educators_count = {'value': 0}
    total_courses_fetched = {'value': 0}
    total_batches_fetched = {'value': 0}
    current_teacher = {'value': "N/A"}

    def get_processed_count(): return processed_count['value']
    def get_new_educators_count(): return new_educators_count['value']
    def get_total_courses(): return total_courses_fetched['value']
    def get_total_batches(): return total_batches_fetched['value']
    def get_current_teacher(): return current_teacher['value']

    progress_task = asyncio.create_task(optry_progress_updater(
        total_educators, get_processed_count, get_new_educators_count, get_total_courses, get_total_batches, get_current_teacher
    ))

    await send_optry_progress(total_educators, 0, 0, 0, 0, "Starting")

    all_found_educators_global = set()

    for idx, optry_educator in enumerate(educators_to_process, 1):
        current_teacher['value'] = optry_educator.get("username", "unknown")
        try:
            username = optry_educator.get("username", "").strip()
            if not username:
                processed_count['value'] += 1
                continue

            username_normalized = normalize_username(username)

            print(f"\n{'='*60}")
            print(f"[{idx}/{total_educators}] Processing educator: {username}")
            print(f"{'='*60}\n")

            educator_doc = educators_col.find_one({"username": username_normalized})

            if educator_doc:
                thread_id = educator_doc["subtopic_msg_id"]
                title = educator_doc["topic_title"]
                group_id_use = educator_doc.get("group_id", group_id)
                channel_id_use = educator_doc.get("channel_id", channel_id)
                print(f"✓ Educator {username} already exists")
            else:
                educator = await fetch_educator_by_username(username)
                if not educator:
                    processed_count['value'] += 1
                    continue

                title = f"{educator['first_name']} {educator['last_name']} [{username}]"

                topic = await context.bot.create_forum_topic(chat_id=group_id, name=title)
                thread_id = topic.message_thread_id

                educators_col.insert_one({
                    "_id": ObjectId(),
                    "first_name": educator["first_name"],
                    "last_name": educator["last_name"],
                    "username": username_normalized,
                    "uid": educator["uid"],
                    "avatar": educator["avatar"],
                    "group_id": group_id,
                    "channel_id": channel_id,
                    "subtopic_msg_id": thread_id,
                    "topic_title": title,
                    "last_checked_time": None,
                    "courses": [],
                    "batches": []
                })

                group_id_use = group_id
                channel_id_use = channel_id
                print(f"✅ Created new educator entry for {username}")

            courses = await fetch_courses(username)
            batches = await fetch_batches(username)
            total_courses_fetched['value'] += len(courses)
            total_batches_fetched['value'] += len(batches)

            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

            educators_col.update_one({"username": username_normalized}, {"$set": {"last_checked_time": last_checked}})

            all_found_educators = set()

            for course in courses:
                author = course.get("author", {})
                author_username = author.get("username", "").strip()
                author_uid = author.get("uid", "").strip()
                if author_username and author_uid:
                    all_found_educators.add((author_username, author_uid,
                                            author.get("first_name", "N/A"),
                                            author.get("last_name", "N/A"),
                                            author.get("avatar", "N/A")))
                    all_found_educators_global.add((author_username, author_uid,
                                                   author.get("first_name", "N/A"),
                                                   author.get("last_name", "N/A"),
                                                   author.get("avatar", "N/A")))

            for batch in batches:
                authors = batch.get("authors", [])
                for author in authors:
                    author_username = author.get("username", "").strip()
                    author_uid = author.get("uid", "").strip()
                    if author_username and author_uid:
                        all_found_educators.add((author_username, author_uid,
                                                author.get("first_name", "N/A"),
                                                author.get("last_name", "N/A"),
                                                author.get("avatar", "N/A")))
                        all_found_educators_global.add((author_username, author_uid,
                                                       author.get("first_name", "N/A"),
                                                       author.get("last_name", "N/A"),
                                                       author.get("avatar", "N/A")))

            for edu_username, edu_uid, edu_first, edu_last, edu_avatar in all_found_educators:
                normalized_edu_username = normalize_username(edu_username)
                exists = collection_optry.find_one({"uid": edu_uid})
                if not exists:
                    collection_optry.insert_one({
                        "uid": edu_uid,
                        "username": normalized_edu_username,
                        "avatar": edu_avatar,
                        "first_name": edu_first,
                        "last_name": edu_last
                    })
                    print(f"✅ Added new educator to optry MongoDB: {edu_username} (UID: {edu_uid})")
                    new_educators_count['value'] += 1

            current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
            completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)

            all_courses = current_courses + completed_courses
            all_batches = current_batches + completed_batches

            existing_doc = educators_col.find_one({"username": username_normalized})
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
                    "group_id": group_id_use,
                    "last_checked_at": None,
                    "msg_id": None,
                    "channel_msg_id": None,
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
                    "group_id": group_id_use,
                    "last_checked_at": None,
                    "msg_id": None,
                    "channel_msg_id": None,
                    "is_completed": not is_current,
                    "teachers": teachers
                }
                batch_datas.append(batch_data)

            if batch_datas:
                educators_col.update_one({"username": username_normalized}, {"$push": {"batches": {"$each": batch_datas}}})

            educator_data = {
                "username": username_normalized,
                "first_name": optry_educator.get("first_name", "N/A"),
                "last_name": optry_educator.get("last_name", "N/A"),
                "uid": optry_educator.get("uid", "N/A"),
                "avatar": optry_educator.get("avatar", "N/A"),
                "group_id": group_id_use,
                "channel_id": channel_id_use,
                "subtopic_msg_id": thread_id,
                "topic_title": title,
                "last_checked_time": last_checked
            }
            educator_filename = f"educator_{username_normalized}_{int(datetime.now().timestamp())}.json"
            save_to_json(educator_filename, educator_data)

            try:
                with open(educator_filename, "rb") as f:
                    await context.bot.send_document(
                        chat_id=group_id_use,
                        message_thread_id=thread_id,
                        document=f,
                        caption=(
                            f"Teacher Name: {educator_data['first_name']} {educator_data['last_name']}\n"
                            f"Username: {username_normalized}\n"
                            f"Uid: {educator_data['uid']}\n"
                            f"Last Checked: {last_checked}"
                        )
                    )
                print(f"✓ Educator JSON uploaded for {username}")
                await asyncio.sleep(10)
            except Exception as e:
                print(f"Error uploading educator JSON for {username}: {e}")
            finally:
                if os.path.exists(educator_filename):
                    os.remove(educator_filename)
                    print(f"✓ Deleted {educator_filename}")
                del educator_data

            async def update_item(item, item_type, group_id_use, channel_id_use, thread_id, last_checked, username_normalized):
                item_uid = item["uid"]
                item_name = item.get("name", "Unknown")
                items_field = "courses" if item_type == "course" else "batches"

                doc = educators_col.find_one({"username": username_normalized, f"{items_field}.uid": item_uid})
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

                results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item)
                if results is None:
                    print(f"FAILED to fetch {item_type} {item_uid}")
                    return False

                schedule_filename = f"schedule_{username_normalized}_{item_type}_{item_uid}_{int(datetime.now().timestamp())}.json"
                try:
                    save_to_json(schedule_filename, results)
                except Exception as e:
                    print(f"Error saving JSON: {e}")
                    del results
                    return False

                caption = create_caption(item_type, item, last_checked)
                channel_msg_id = None
                if channel_id_use:
                    with open(schedule_filename, "rb") as f:
                        channel_msg = await bot.send_document(
                            chat_id=channel_id_use,
                            document=f,
                            caption=caption
                        )
                    channel_msg_id = channel_msg.message_id

                link = f"https://t.me/c/{str(channel_id_use)[4:]}/{channel_msg_id}" if channel_id_use and channel_msg_id else ""
                caption_group = caption + (f"\n\nIn channel - {link}" if link else "")

                with open(schedule_filename, "rb") as f:
                    msg = await bot.send_document(
                        chat_id=group_id_use,
                        message_thread_id=thread_id,
                        document=f,
                        caption=caption_group
                    )
                msg_id = msg.message_id

                update_set = {
                    f"{items_field}.$.last_checked_at": last_checked,
                    f"{items_field}.$.msg_id": msg_id
                }
                if channel_msg_id:
                    update_set[f"{items_field}.$.channel_msg_id"] = channel_msg_id
                educators_col.update_one(
                    {"username": username_normalized, f"{items_field}.uid": item_uid},
                    {"$set": update_set}
                )
                await asyncio.sleep(20)

                if os.path.exists(schedule_filename):
                    os.remove(schedule_filename)
                del results

                print(f"COMPLETED {item_type} {item_uid}")
                return True

            print(f"\nProcessing {len(all_courses)} courses for {username}...")
            for course in all_courses:
                try:
                    await update_item(course, "course", group_id_use, channel_id_use, thread_id, last_checked, username_normalized)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Error processing course: {e}")

            all_courses.clear()
            gc.collect()

            print(f"\nProcessing {len(all_batches)} batches for {username}...")
            for batch in all_batches:
                try:
                    await update_item(batch, "batch", group_id_use, channel_id_use, thread_id, last_checked, username_normalized)
                    await asyncio.sleep(2)
                except Exception as e:
                    print(f"Error processing batch: {e}")

            all_batches.clear()
            gc.collect()

            processed_count['value'] += 1
            print(f"✅ Completed processing educator {username} [{idx}/{total_educators}]")

        except Exception as e:
            print(f"❌ Error processing educator: {e}")
            processed_count['value'] += 1

    total_new_added = await add_new_educators_to_optry(all_found_educators_global, new_educators_count)
    
    await send_optry_progress(total_educators, processed_count['value'], new_educators_count['value'], total_courses_fetched['value'], total_batches_fetched['value'], "Complete")
    progress_task.cancel()

    await update.message.reply_text(
        f"✅ /optry BATCH COMPLETE!\n\n"
        f"Total Educators: {total_educators}\n"
        f"Processed: {processed_count['value']}\n"
        f"New Educators Found: {new_educators_count['value']}\n\n"
        f"🔥 Starting NEW EDUCATORS BATCH..."
    )

    await process_new_educators_batch(update, context, total_new_added, group_id, channel_id)

    await update.message.reply_text(
        f"\n🎉 /optry COMMAND 100% COMPLETE!\n\n"
        f"📊 MAIN: {total_educators} processed\n"
        f"🔥 NEW: {total_new_added} added & processed\n"
        f"✅ TOTAL: {total_educators + total_new_added} educators ready!"
    )

    return ConversationHandler.END


async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /add command."""
    global update_context, update_obj, progress_message
    update_context = context
    update_obj = update
    progress_message = None

    args = context.args
    if len(args) not in [1, 3]:
        await update.message.reply_text("Usage: /add {username} [group_id channel_id]")
        return ConversationHandler.END

    raw_username = args[0]
    username = normalize_username(raw_username)
    if len(args) == 3:
        group_id = int(args[1])
        channel_id = int(args[2])
    else:
        group_id = SETTED_GROUP_ID
        channel_id = None

    await update.message.reply_text(f"Fetching data for username: {username}")

    educator = await fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"No educator found with username: {username}")
        return ConversationHandler.END

    educator_doc = educators_col.find_one({"username": username})
    if educator_doc:
        thread_id = educator_doc["subtopic_msg_id"]
        title = educator_doc["topic_title"]
        group_id = educator_doc.get("group_id", group_id)
        channel_id = educator_doc.get("channel_id", channel_id)
        print(f"Educator {username} already exists with thread ID {thread_id}")
    else:
        title = f"{educator['first_name']} {educator['last_name']} [{raw_username}]"
        try:
            topic = await context.bot.create_forum_topic(chat_id=group_id, name=title)
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
            "group_id": group_id,
            "channel_id": channel_id,
            "subtopic_msg_id": thread_id,
            "topic_title": title,
            "last_checked_time": None,
            "courses": [],
            "batches": []
        })

    context.user_data['thread_id'] = thread_id
    context.user_data['group_id'] = group_id
    context.user_data['channel_id'] = channel_id
    context.user_data['topic_title'] = title

    print(f"Fetching courses for {username}...")
    courses = await fetch_courses(username)
    print(f"Fetching batches for {username}...")
    batches = await fetch_batches(username)

    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    educators_col.update_one({"username": username}, {"$set": {"last_checked_time": last_checked}})

    all_found_educators = set()

    for course in courses:
        author = course.get("author", {})
        author_username = author.get("username", "").strip()
        author_uid = author.get("uid", "").strip()
        if author_username and author_uid:
            all_found_educators.add((author_username, author_uid,
                                    author.get("first_name", "N/A"),
                                    author.get("last_name", "N/A"),
                                    author.get("avatar", "N/A")))

    for batch in batches:
        authors = batch.get("authors", [])
        for author in authors:
            author_username = author.get("username", "").strip()
            author_uid = author.get("uid", "").strip()
            if author_username and author_uid:
                all_found_educators.add((author_username, author_uid,
                                        author.get("first_name", "N/A"),
                                        author.get("last_name", "N/A"),
                                        author.get("avatar", "N/A")))

    for edu_username, edu_uid, edu_first, edu_last, edu_avatar in all_found_educators:
        normalized_edu_username = normalize_username(edu_username)
        exists = collection_optry.find_one({"uid": edu_uid})
        if not exists:
            collection_optry.insert_one({
                "uid": edu_uid,
                "username": normalized_edu_username,
                "avatar": edu_avatar,
                "first_name": edu_first,
                "last_name": edu_last
            })
            print(f"✅ Added new educator from /add: {edu_username} (UID: {edu_uid})")

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
        teachers = f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
        course_data = {
            "uid": course["uid"],
            "name": course.get("name", "N/A"),
            "slug": course.get("slug", "N/A"),
            "thumbnail": course.get("thumbnail", "N/A"),
            "starts_at": course.get("starts_at", "N/A"),
            "ends_at": course.get("ends_at", "N/A"),
            "group_id": group_id,
            "last_checked_at": None,
            "msg_id": None,
            "channel_msg_id": None,
            "is_completed": not is_current,
            "teachers": teachers
        }
        course_datas.append(course_data)

    if course_datas:
        educators_col.update_one({"username": username}, {"$push": {"courses": {"$each": course_datas}}})

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
            "group_id": group_id,
            "last_checked_at": None,
            "msg_id": None,
            "channel_msg_id": None,
            "is_completed": not is_current,
            "teachers": teachers
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

    educator_data = {
        "username": username,
        "first_name": educator["first_name"],
        "last_name": educator["last_name"],
        "uid": educator["uid"],
        "avatar": educator["avatar"],
        "group_id": group_id,
        "channel_id": channel_id,
        "subtopic_msg_id": thread_id,
        "topic_title": title,
        "last_checked_time": last_checked
    }
    educator_filename = f"educator_{username}_{int(datetime.now().timestamp())}.json"
    save_to_json(educator_filename, educator_data)
    try:
        with open(educator_filename, "rb") as f:
            await context.bot.send_document(
                chat_id=group_id,
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
            print(f"✓ Deleted {educator_filename}")

        del educator_data

    async def update_item(item, item_type, group_id, channel_id, thread_id, last_checked, username):
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

        results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item)
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

        caption = create_caption(item_type, item, last_checked)
        channel_msg_id = None
        if channel_id:
            with open(schedule_filename, "rb") as f:
                channel_msg = await bot.send_document(
                    chat_id=channel_id,
                    document=f,
                    caption=caption
                )
            channel_msg_id = channel_msg.message_id

        link = f"https://t.me/c/{str(channel_id)[4:]}/{channel_msg_id}" if channel_id and channel_msg_id else ""
        caption_group = caption + (f"\n\nIn channel - {link}" if link else "")

        with open(schedule_filename, "rb") as f:
            msg = await bot.send_document(
                chat_id=group_id,
                message_thread_id=thread_id,
                document=f,
                caption=caption_group
            )
        msg_id = msg.message_id

        update_set = {
            f"{items_field}.$.last_checked_at": last_checked,
            f"{items_field}.$.msg_id": msg_id
        }
        if channel_msg_id:
            update_set[f"{items_field}.$.channel_msg_id"] = channel_msg_id
        educators_col.update_one(
            {"username": username, f"{items_field}.uid": item_uid},
            {"$set": update_set}
        )
        await asyncio.sleep(20)

        if os.path.exists(schedule_filename):
            os.remove(schedule_filename)
        del results

        print(f"COMPLETED {item_type} {item_uid}")
        return True

    print(f"\n{'='*60}")
    print(f"PHASE 1: Processing {len(all_courses)} courses...")
    print(f"{'='*60}\n")
    phase_tracker['phase'] = 'courses'
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'courses')

    for idx, course in enumerate(all_courses, 1):
        try:
            print(f"\n[COURSE {idx}/{len(all_courses)}]")
            success = await update_item(course, "course", group_id, channel_id, thread_id, last_checked, username)
            await asyncio.sleep(2)

            if idx % 10 == 0:
                gc.collect()
                print(f"✓ Memory cleanup at course {idx}")
        except Exception as e:
            print(f"EXCEPTION processing course {course.get('uid', 'UNKNOWN')}: {e}")
            await asyncio.sleep(5)

    all_courses.clear()
    gc.collect()

    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'courses')

    print(f"\n{'='*60}")
    print(f"PHASE 2: Processing {len(all_batches)} batches...")
    print(f"{'='*60}\n")
    phase_tracker['phase'] = 'batches'
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'batches')

    for idx, batch in enumerate(all_batches, 1):
        try:
            print(f"\n[BATCH {idx}/{len(all_batches)}]")
            success = await update_item(batch, "batch", group_id, channel_id, thread_id, last_checked, username)
            await asyncio.sleep(2)

            if idx % 10 == 0:
                gc.collect()
                print(f"✓ Memory cleanup at batch {idx}")
        except Exception as e:
            print(f"EXCEPTION processing batch {batch.get('uid', 'UNKNOWN')}: {e}")
            await asyncio.sleep(5)

    all_batches.clear()
    gc.collect()

    phase_tracker['phase'] = 'complete'
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches(), 'complete')

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
    channel_id = context.user_data.get('channel_id')
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

    caption = create_caption(item_type, item_data, last_checked)
    channel_msg_id = None
    if channel_id:
        with open(schedule_filename, "rb") as f:
            channel_msg = await bot.send_document(
                chat_id=channel_id,
                document=f,
                caption=caption
            )
        channel_msg_id = channel_msg.message_id

    link = f"https://t.me/c/{str(channel_id)[4:]}/{channel_msg_id}" if channel_id and channel_msg_id else ""
    caption_group = caption + (f"\n\nIn channel - {link}" if link else "")

    with open(schedule_filename, "rb") as f:
        msg = await bot.send_document(
            chat_id=group_id,
            message_thread_id=thread_id,
            document=f,
            caption=caption_group
        )
    new_msg_id = msg.message_id

    update_set = {
        f"{items_field}.$.last_checked_at": last_checked,
        f"{items_field}.$.msg_id": new_msg_id
    }
    if channel_msg_id:
        update_set[f"{items_field}.$.channel_msg_id"] = channel_msg_id
    educators_col.update_one(
        {"username": username, f"{items_field}.uid": item_id},
        {"$set": update_set}
    )
    await asyncio.sleep(30)

    if os.path.exists(schedule_filename):
        os.remove(schedule_filename)
        print(f"✓ Deleted {schedule_filename}")

    del results

    await update.message.reply_text(f"Schedule uploaded to: {topic_title}")
    return ConversationHandler.END


async def update_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /update {username} {group_id} {channel_id}"""
    args = context.args
    if len(args) != 3:
        await update.message.reply_text("Usage: /update {username} {group_id} {channel_id}")
        return

    raw_username = args[0]
    username = normalize_username(raw_username)
    new_group_id = int(args[1])
    new_channel_id = int(args[2])

    doc = educators_col.find_one({"username": username})
    if not doc:
        await update.message.reply_text(f"No educator found: {username}")
        return

    current_group_id = doc.get("group_id")
    thread_id = doc.get("subtopic_msg_id")
    title = doc.get("topic_title")

    if new_group_id != current_group_id:
        try:
            topic = await context.bot.create_forum_topic(chat_id=new_group_id, name=title)
            new_thread_id = topic.message_thread_id
        except Exception as e:
            await update.message.reply_text(f"Error creating new topic: {e}")
            return
        educators_col.update_one({"username": username}, {"$set": {"subtopic_msg_id": new_thread_id}})
        thread_id = new_thread_id

    educators_col.update_one({"username": username}, {"$set": {"group_id": new_group_id, "channel_id": new_channel_id}})

    await update.message.reply_text(f"Updated {username} to group {new_group_id}, channel {new_channel_id}")


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

    add_handler = ConversationHandler(
        entry_points=[CommandHandler("add", add_command)],
        states={
            SELECT_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_type)],
            ENTER_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_id)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    optry_handler = ConversationHandler(
        entry_points=[CommandHandler("optry", optry_start)],
        states={
            OPTRY_CHOOSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, optry_choose)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    application.add_handler(add_handler)
    application.add_handler(optry_handler)
    application.add_handler(CommandHandler("update", update_command))

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
