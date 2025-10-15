import aiohttp
import asyncio
import json
import os
import gc
from telegram import Update, InputFile
from telegram.ext import Application, CommandHandler, ConversationHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest, RetryAfter, TimedOut, NetworkError
import re
from datetime import datetime
import dateutil.parser
import pytz
import firebase_admin
from firebase_admin import credentials, firestore
from io import BytesIO

# Telegram group ID
SETTED_GROUP_ID = -1003133358948

# Initialize Firebase
cred = credentials.Certificate({
  "type": "service_account",
  "project_id": "my-chunacademy-database",
  "private_key_id": "032481e0871375f676b6780a9a1ecba3f6d17d62",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQChpa3go7EQZSVU\ntLCCk2tGety6IfrEIiqLhY7rM4RZCeo5o5snEurGYqdMg+3XanU8CDqDpNNhNoY1\naMLmNZoPe5kiLD9GuvxJObEh8+oVJAL8lZtO5jpsqZTAgo5JYpUbn1FiNLWLh3bO\ncQsCd8SAv+LryFvcnBRPQHm2IVTD6HrzAB4WOx9VNiJzU5PtJeROehL1aGTKB+e+\nLVIK+tw+2tE4hN7tZZ9jcZO4H2c6PcHfc0O373T8I7nJNcA/goAPivozW1b2Vm4m\nA11CEyX+dopSjdje+vWPN027upSHJ8LdpwHrCMaQAL7QgM3ApUhtA9rn+8gJcrao\nZwMiL7t5AgMBAAECggEADKQNLeT/Ir4WJkKfDwcCO5e+DRw9JPtbAmAmZgQ4VFIv\nK+S8bFjRXUQ3uwcSIWdk1ZX7JXJNSWvfIOwZWbFVY+KRfhehtyFGO1+0l2ggpRiU\n1zXH2GqupPK5/Df5comwCWHzFk/y5n9obgpvBvlgt4TJ9RBNBrp56Bytp+2BWHSE\n3w/dOzJFy5zz1Egf6YOXZdgsa2C6dT5TPKIaOX/Lx1nkR4u5kBjGBNImDfegmG4K\nY1pH6vyvJ2NQV1QiZ6xh8JHLeJNzEax+clsq9dbw24Qz6Zub4pjLDUEmkD48v7OF\n/Spz1kVh78cyjsLG4Q+QflCQFUU++ix8qUoMzB8fRQKBgQDOaKI1GictvRXTogaL\nq9OOI2K0NcY6EeLKd+pYyQ46c1GRitcOX2FOnUhW2ZThiphOKxP0JA/cbQH8N9sV\nrEKnI+CEWI18X6saJH2SJILT2aEhxMsqVXQHCbOuDtPSA78NuPbPtbeVZDV60h/Z\nr1NKXOIyoAxy/iYAJtkeYrmKzwKBgQDIe/Gze+C+f/ZgcExF9oT91TJUoYFoNe7X\n5PCPrjex3zu+LtpEDQaoTxGqwZXVQWwRLl6l4USKUcSx5aXn+Y9jlmmSW7twiTRJ\n0smcgrhMBujq/GBaaLmmVvMS8XHGQ0IPkg80BTQCm8cnH7q71KhHzAuPHSkDc+yd\ncRW04tTHNwKBgQC45C9Qo+Gube7sSPnWCQ+TBg13Yaf0AmuFc88ewtKU9xF87sJf\nJH8UnXzcF0Dum2h8tMfF7LusdpTNqfb8vfZio5eM1Ym/fC7XVxKIY14xiIN2rUJT\n5IHvf/hMlQmW2TY9g47KVnthPYdOQoS3SP6x4OvZ71XXd+LwRdw9BLAxLwKBgQCN\nLjMkCL1YiXYvrYUY06QNuD/MNzuQ7kH2yOLa232fBavlnsrXlzCz/5JaZB6mYX6/\njp7aQ4tnuHNCL40okaZ3I+nORkj107j9r7GIRMmRyF/ncrhLkmoCCEL+eQZ87sor\neti38l4Q5DBXjdGLChNDFB6jto42P0FGEeeOTRo3+wKBgBb4MiTi6r5PNO3ZNF4D\nlHRuvtKFxUSMNfch41tB+OEXp1u9O2Mu4GyziKlMY2awILq6x5CkE6u2k/5R3N5W\neTCXrFk+imnZEy9McBFHIibWyQP4qVnjTm66jwAPwQQbVUTf+Wx2ZUrzmy+NCKyF\nnVQxNaTAdRTl1faZY7cGWBb3\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-fbsvc@my-chunacademy-database.iam.gserviceaccount.com",
  "client_id": "110340967495752945247",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-fbsvc%40my-chunacademy-database.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
})

firebase_admin.initialize_app(cred)
db = firestore.client()

# Firestore collection
educators_col = db.collection('educators')

# Global bot for scheduler
bot = None

# Global variables for progress
progress_message = None
update_context = None
update_obj = None

# Global variables for scheduler progress
scheduler_progress_messages = {}

# Conversation states for /add
SELECT_TYPE, ENTER_ID = range(2)

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

async def send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, current_phase):
    """Send or update progress bar for schedule checker in group subtopic."""
    global scheduler_progress_messages
    
    if current_phase == "courses":
        progress_text = (
            f"Schedule Check Progress\n"
            f"Phase 1: Checking Courses\n"
            f"Progress: {checked_courses}/{total_courses}\n"
            f"Batches: Pending..."
        )
    elif current_phase == "batches":
        progress_text = (
            f"Schedule Check Progress\n"
            f"Phase 1: Courses Complete\n"
            f"Phase 2: Checking Batches\n"
            f"Progress: {checked_batches}/{total_batches}"
        )
    else:
        progress_text = (
            f"Schedule Check Complete!\n"
            f"Courses Checked: {checked_courses}/{total_courses}\n"
            f"Batches Checked: {checked_batches}/{total_batches}"
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

async def send_document_anonymously(chat_id, thread_id, file_path, caption):
    """Send document anonymously using BytesIO to avoid showing sender."""
    try:
        with open(file_path, 'rb') as f:
            file_data = f.read()
        
        # Create BytesIO object for anonymous sending
        file_obj = BytesIO(file_data)
        file_obj.name = os.path.basename(file_path)
        
        msg = await bot.send_document(
            chat_id=chat_id,
            message_thread_id=thread_id,
            document=InputFile(file_obj),
            caption=caption,
            disable_notification=True
        )
        return msg
    except Exception as e:
        print(f"Error sending document anonymously: {e}")
        raise

async def schedule_checker():
    """Check and update current batches and courses every 2 hours."""
    global scheduler_progress_messages
    
    while True:
        try:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            
            print(f"\n{'='*60}")
            print(f"Starting schedule check at {last_checked}")
            print(f"{'='*60}\n")
            
            # Get all educators from Firestore
            docs = educators_col.stream()
            
            for doc in docs:
                doc_dict = doc.to_dict()
                username = doc_dict.get("username", "unknown")
                thread_id = doc_dict.get("subtopic_msg_id")
                
                print(f"\nChecking educator: {username}")
                
                progress_key = f"{username}_{thread_id}"
                scheduler_progress_messages[progress_key] = None
                
                courses_to_check = [c for c in doc_dict.get("courses", []) if not c.get("is_completed", False) and c.get("msg_id")]
                batches_to_check = [b for b in doc_dict.get("batches", []) if not b.get("is_completed", False) and b.get("msg_id")]
                
                total_courses = len(courses_to_check)
                total_batches = len(batches_to_check)
                checked_courses = 0
                checked_batches = 0
                
                if total_courses == 0 and total_batches == 0:
                    print(f"No active items to check for {username}")
                    continue
                
                # PHASE 1: Check Courses
                if total_courses > 0:
                    print(f"\nPhase 1: Checking {total_courses} courses...")
                    await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "courses")
                    
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
                                    caption = course.get("caption", "")
                                    new_caption = caption + "\n\n✓ Course Completed - No More Updates"
                                    try:
                                        await bot.edit_message_caption(
                                            chat_id=SETTED_GROUP_ID,
                                            message_id=course["msg_id"],
                                            caption=new_caption
                                        )
                                        # Update in Firestore
                                        doc_ref = educators_col.document(doc.id)
                                        courses_list = doc_dict.get("courses", [])
                                        for i, c in enumerate(courses_list):
                                            if c["uid"] == course["uid"]:
                                                courses_list[i]["is_completed"] = True
                                                courses_list[i]["caption"] = new_caption
                                                break
                                        doc_ref.update({"courses": courses_list})
                                        print(f"✓ Marked course {course['uid']} as completed")
                                    except Exception as e:
                                        print(f"Error marking course completed: {e}")
                                else:
                                    print(f"Updating course {course['uid']}")
                                    schedule_url = f"https://unacademy.com/api/v3/collection/{course['uid']}/items?limit=10000"
                                    
                                    results, caption = await fetch_unacademy_schedule(schedule_url, "course", course)
                                    if results is None or caption is None:
                                        print(f"Failed to fetch schedule for course {course['uid']}")
                                        checked_courses += 1
                                        await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "courses")
                                        continue
                                    
                                    filename = f"temp_schedule_{username}_course_{course['uid']}_{int(datetime.now().timestamp())}.json"
                                    save_to_json(filename, results)
                                    
                                    new_msg_id = None
                                    try:
                                        old_msg_id = course.get("msg_id")
                                        if old_msg_id:
                                            try:
                                                await bot.delete_message(
                                                    chat_id=SETTED_GROUP_ID,
                                                    message_id=old_msg_id
                                                )
                                                print(f"✓ Deleted old message {old_msg_id}")
                                                await asyncio.sleep(2)
                                            except Exception as e:
                                                print(f"Error deleting old message: {e}")
                                        
                                        # Send anonymously
                                        new_msg = await send_document_anonymously(
                                            SETTED_GROUP_ID,
                                            thread_id,
                                            filename,
                                            caption
                                        )
                                        
                                        new_msg_id = new_msg.message_id
                                        print(f"✓ Uploaded new message {new_msg_id}")
                                        
                                        # Update in Firestore
                                        doc_ref = educators_col.document(doc.id)
                                        courses_list = doc_dict.get("courses", [])
                                        for i, c in enumerate(courses_list):
                                            if c["uid"] == course["uid"]:
                                                courses_list[i]["msg_id"] = new_msg_id
                                                courses_list[i]["last_checked_at"] = last_checked
                                                courses_list[i]["caption"] = caption
                                                break
                                        doc_ref.update({"courses": courses_list})
                                        print(f"✓ Firestore updated: course {course['uid']}")
                                        await asyncio.sleep(30)
                                    except Exception as e:
                                        print(f"❌ Error updating course: {e}")
                                        import traceback
                                        traceback.print_exc()
                                    finally:
                                        if os.path.exists(filename):
                                            os.remove(filename)
                                        if 'results' in locals():
                                            del results
                                        if 'caption' in locals():
                                            del caption
                            
                            checked_courses += 1
                            await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "courses")
                            
                        except Exception as e:
                            print(f"Error processing course: {e}")
                            checked_courses += 1
                
                # PHASE 2: Check Batches
                if total_batches > 0:
                    print(f"\nPhase 2: Checking {total_batches} batches...")
                    await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "batches")
                    
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
                                    caption = batch.get("caption", "")
                                    new_caption = caption + "\n\n✓ Batch Completed - No More Updates"
                                    try:
                                        await bot.edit_message_caption(
                                            chat_id=SETTED_GROUP_ID,
                                            message_id=batch["msg_id"],
                                            caption=new_caption
                                        )
                                        # Update in Firestore
                                        doc_ref = educators_col.document(doc.id)
                                        batches_list = doc_dict.get("batches", [])
                                        for i, b in enumerate(batches_list):
                                            if b["uid"] == batch["uid"]:
                                                batches_list[i]["is_completed"] = True
                                                batches_list[i]["caption"] = new_caption
                                                break
                                        doc_ref.update({"batches": batches_list})
                                        print(f"✓ Marked batch {batch['uid']} as completed")
                                    except Exception as e:
                                        print(f"Error marking batch completed: {e}")
                                else:
                                    print(f"Updating batch {batch['uid']}")
                                    schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                                    
                                    results, caption = await fetch_unacademy_schedule(schedule_url, "batch", batch)
                                    if results is None or caption is None:
                                        print(f"Failed to fetch schedule for batch {batch['uid']}")
                                        checked_batches += 1
                                        await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "batches")
                                        continue
                                    
                                    filename = f"temp_schedule_{username}_batch_{batch['uid']}_{int(datetime.now().timestamp())}.json"
                                    save_to_json(filename, results)
                                    
                                    try:
                                        try:
                                            await bot.delete_message(
                                                chat_id=SETTED_GROUP_ID,
                                                message_id=batch["msg_id"]
                                            )
                                            print(f"Deleted old batch message")
                                        except Exception as e:
                                            print(f"Error deleting old message: {e}")
                                        
                                        # Send anonymously
                                        new_msg = await send_document_anonymously(
                                            SETTED_GROUP_ID,
                                            thread_id,
                                            filename,
                                            caption
                                        )
                                        
                                        new_msg_id = new_msg.message_id
                                        
                                        # Update in Firestore
                                        doc_ref = educators_col.document(doc.id)
                                        batches_list = doc_dict.get("batches", [])
                                        for i, b in enumerate(batches_list):
                                            if b["uid"] == batch["uid"]:
                                                batches_list[i]["msg_id"] = new_msg_id
                                                batches_list[i]["last_checked_at"] = last_checked
                                                batches_list[i]["caption"] = caption
                                                break
                                        doc_ref.update({"batches": batches_list})
                                        print(f"✓ Updated batch {batch['uid']}")
                                        await asyncio.sleep(30)
                                    except Exception as e:
                                        print(f"Error updating batch: {e}")
                                    finally:
                                        if os.path.exists(filename):
                                            os.remove(filename)
                            
                            checked_batches += 1
                            await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "batches")
                            
                        except Exception as e:
                            print(f"Error processing batch: {e}")
                            checked_batches += 1
                
                await send_scheduler_progress(username, thread_id, total_courses, total_batches, checked_courses, checked_batches, "complete")
                print(f"Completed schedule check for {username}")
                
                gc.collect()
                print(f"✓ Memory cleanup after {username}")
                
        except Exception as e:
            print(f"Error in schedule_checker: {e}")
        
        gc.collect()
        print(f"\nSchedule check complete. Sleeping for 2 hours...")
        await asyncio.sleep(7200)

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
    await update.message.reply_text(f"Fetching data for username: {username}...")

    educator = await fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"No educator found with username: {username}")
        return ConversationHandler.END

    # Check if educator exists in Firestore
    educator_docs = educators_col.where('username', '==', username).limit(1).stream()
    educator_doc = None
    doc_id = None
    
    for doc in educator_docs:
        educator_doc = doc.to_dict()
        doc_id = doc.id
        break
    
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

        # Add to Firestore
        doc_ref = educators_col.document()
        doc_id = doc_ref.id
        doc_ref.set({
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
    context.user_data['doc_id'] = doc_id

    print(f"Fetching courses for {username}...")
    courses = await fetch_courses(username)
    print(f"Fetching batches for {username}...")
    batches = await fetch_batches(username)

    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Update last checked time in Firestore
    educators_col.document(doc_id).update({"last_checked_time": last_checked})

    current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
    completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)

    all_courses = current_courses + completed_courses
    all_batches = current_batches + completed_batches

    # Get existing data from Firestore
    existing_doc = educators_col.document(doc_id).get().to_dict()
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
            "caption": None,
            "is_completed": not is_current
        }
        teachers = f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
        course_data["teachers"] = teachers
        course_datas.append(course_data)

    if course_datas:
        current_courses_list = existing_doc.get("courses", [])
        current_courses_list.extend(course_datas)
        educators_col.document(doc_id).update({"courses": current_courses_list})

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
            "caption": None,
            "is_completed": not is_current
        }
        teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
        batch_data["teachers"] = teachers
        batch_datas.append(batch_data)

    if batch_datas:
        current_batches_list = existing_doc.get("batches", [])
        current_batches_list.extend(batch_datas)
        educators_col.document(doc_id).update({"batches": current_batches_list})

    # Refresh doc
    existing_doc = educators_col.document(doc_id).get().to_dict()
    total_courses = len(existing_doc.get("courses", []))
    total_batches = len(existing_doc.get("batches", []))

    def get_uploaded_courses():
        doc = educators_col.document(doc_id).get().to_dict()
        return sum(1 for c in doc.get("courses", []) if c.get("msg_id") is not None)

    def get_uploaded_batches():
        doc = educators_col.document(doc_id).get().to_dict()
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
        # Send anonymously
        await send_document_anonymously(
            SETTED_GROUP_ID,
            thread_id,
            educator_filename,
            (
                f"Teacher Name: {educator['first_name']} {educator['last_name']}\n"
                f"Username: {username}\n"
                f"Uid: {educator['uid']}\n"
                f"Last Checked: {last_checked}"
            )
        )
        print(f"✓ Educator JSON uploaded anonymously")
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
        
        doc = educators_col.document(doc_id).get().to_dict()
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
        caption = None
        fetch_attempts = 0
        
        while results is None and fetch_attempts < 5:
            fetch_attempts += 1
            try:
                results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)
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
                # Send anonymously
                msg = await send_document_anonymously(
                    SETTED_GROUP_ID,
                    thread_id,
                    schedule_filename,
                    caption
                )
                msg_id = msg.message_id
                uploaded = True
                
                # Update in Firestore
                doc = educators_col.document(doc_id).get().to_dict()
                items_list = doc.get(items_field, [])
                for i, it in enumerate(items_list):
                    if it["uid"] == item_uid:
                        items_list[i]["last_checked_at"] = last_checked
                        items_list[i]["caption"] = caption
                        items_list[i]["msg_id"] = msg_id
                        break
                educators_col.document(doc_id).update({items_field: items_list})
                
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
        except Exception as e:
            print(f"Could not delete file: {e}")
        
        del results
        if 'caption' in locals():
            del caption

        if not uploaded:
            print(f"FAILED to upload {item_type} {item_uid}")
            return False
        
        print(f"COMPLETED {item_type} {item_uid}")
        return True

    failed_courses = []
    failed_batches = []
    
    # PHASE 1: Upload ALL courses
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
        except Exception as e:
            print(f"EXCEPTION processing course: {e}")
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
        except Exception as e:
            print(f"EXCEPTION processing batch: {e}")
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
    doc_id = context.user_data.get('doc_id')

    items_field = "courses" if item_type == "course" else "batches"

    # Find item in Firestore
    doc = educators_col.document(doc_id).get().to_dict()
    item_data = None
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
    
    results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item_data)
    if results is None or caption is None:
        await update.message.reply_text(f"Failed to fetch schedule for {item_type} ID: {item_id}")
        return ConversationHandler.END

    schedule_filename = f"schedule_{username}_{item_type}_{item_id}_{int(datetime.now().timestamp())}.json"
    save_to_json(schedule_filename, results)
    
    uploaded = False
    retries = 0
    while not uploaded and retries < 10:
        try:
            # Send anonymously
            msg = await send_document_anonymously(
                group_id,
                thread_id,
                schedule_filename,
                caption
            )
            new_msg_id = msg.message_id
            
            # Update Firestore
            doc = educators_col.document(doc_id).get().to_dict()
            items_list = doc.get(items_field, [])
            for i, it in enumerate(items_list):
                if it["uid"] == item_id:
                    items_list[i]["msg_id"] = new_msg_id
                    items_list[i]["last_checked_at"] = last_checked
                    items_list[i]["caption"] = caption
                    break
            educators_col.document(doc_id).update({items_field: items_list})
            
            uploaded = True
            await asyncio.sleep(30)
        except Exception as e:
            print(f"Error uploading: {e}")
            retries += 1
            await asyncio.sleep(30)

    if os.path.exists(schedule_filename):
        os.remove(schedule_filename)
    
    del results
    if 'caption' in locals():
        del caption

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
    bot_token = '8279128725:AAEc5BZLq6lX3Mmm7-b_dpBVUp41KusWsPM'
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

    application.add_handler(conv_handler)

    print("Bot is starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    asyncio.create_task(schedule_checker())
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("\nShutting down...")
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
