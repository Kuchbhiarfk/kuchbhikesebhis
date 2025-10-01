import aiohttp
import asyncio
import json
import os
from telegram import Update
from telegram.ext import Application, CommandHandler, ConversationHandler, MessageHandler, filters, ContextTypes
import re
from datetime import datetime
import dateutil.parser
import pytz
import pymongo
from bson import ObjectId

# Telegram group ID
SETTED_GROUP_ID = -1003133358948  # Replace with your supergroup ID

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://elvishyadav_opm:naman1811421@cluster0.uxuplor.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["unacademy_db"]
educators_col = db["educators"]

# Global bot for scheduler
bot = None

# Global variables for progress
progress_message = None
update_context = None
update_obj = None

# Conversation states for /add
SELECT_TYPE, ENTER_ID = range(2)

def save_to_json(filename, data):
    """Save data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

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

async def fetch_courses(username, limit=50, max_offset=5000):
    """Fetch courses for a given username asynchronously."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}"
    courses = []
    async with aiohttp.ClientSession() as session:
        seen_uids = set()
        offset = 0

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
                        print(f"No valid course results for {username} at offset {offset}.")
                        break

                    if not results:
                        print(f"No more courses for {username} at offset {offset}.")
                        break

                    for i, course in enumerate(results, start=offset + 1):
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
                break

        print(f"Total courses fetched for {username}: {len(courses)}")
        return courses

async def fetch_batches(username, limit=50, max_offset=5000):
    """Fetch batches for a given username asynchronously."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    batches = []
    async with aiohttp.ClientSession() as session:
        seen_batch_uids = set()
        offset = 0

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
                        print(f"No valid batch results for {username} at offset {offset}.")
                        break

                    if not results:
                        print(f"No more batches for {username} at offset {offset}.")
                        break

                    for i, batch in enumerate(results, start=offset + 1):
                        batch_uid = batch.get("uid")
                        if batch_uid and batch_uid not in seen_batch_uids:
                            seen_batch_uids.add(batch_uid)
                            print(f"{i} Batch Name :- {batch.get('name', 'N/A')}")
                            print(f"Cover Photo :- {batch.get('cover_photo', 'N/A')}")
                            print(f"Exam Type :- {batch.get('goal', {}).get('name', 'N/A')}")
                            print(f"Uid :- {batch_uid}")
                            print(f"Slug :- {batch.get('slug', 'N/A')}")
                            print(f"Syllabus Tag :- {batch.get('syllabus_tag', 'N/A')}")
                            print(f"Starts At :- {batch.get('starts_at', 'N/A')}")
                            print(f"Completed At :- {batch.get('completed_at', 'N/A')}")
                            print("----------------------")
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
                break

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
                        print(f"Rate limited for {item_type} schedule. Retrying after {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()
                    results = data.get('results', [])

                    if not results:
                        print(f"⚠️ No schedule results found for {item_type}")
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
                        print(f"Processing batch schedule for {item_name} ({item_data.get('uid', 'N/A')}) with {len(results)} items...")
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
                                            print(f"Rate limited for batch collection {data_id}. Retrying after {retry_after} seconds...")
                                            await asyncio.sleep(retry_after)
                                            continue
                                        collection_response.raise_for_status()
                                        collection_data = await collection_response.json()
                                        items = collection_data.get("results", [])
                                        for collection_item in items:
                                            value = collection_item.get("value", {})
                                            if value.get("uid") == uid:
                                                print(f"  Fetched collection item {uid} for {data_id}")
                                                return fetch_unacademy_collection(
                                                    value.get("title", properties.get('name', 'N/A')),
                                                    value.get("live_class", {}).get("author", author),
                                                    value.get("live_at", live_at),
                                                    value.get("live_class", {}).get("video_url"),
                                                    value.get("live_class", {}).get("slides_pdf", {}),
                                                    value.get("is_offline", "N/A")
                                                )
                                        return None
                                except aiohttp.ClientResponseError as e:
                                    print(f"❌ HTTP error in collection API for batch {data_id}: {e}")
                                    return handle_collection_failure(live_at, properties.get('name', 'N/A'), author)
                                except aiohttp.ClientTimeout:
                                    print(f"❌ Timeout in collection API for batch {data_id}, attempt {retry + 1}/3")
                                    if retry < 2:
                                        await asyncio.sleep(2 ** retry)
                                        continue
                                    print(f"❌ Failed after retries for batch {data_id}")
                                    return handle_collection_failure(live_at, properties.get('name', 'N/A'), author)
                                except aiohttp.ClientError as e:
                                    print(f"❌ Error in collection API for batch {data_id}: {e}")
                                    return handle_collection_failure(live_at, properties.get('name', 'N/A'), author)
                            return None

                        tasks = [fetch_collection_item(item) for item in results]
                        collection_results = await asyncio.gather(*tasks, return_exceptions=True)
                        results_list.extend([r for r in collection_results if r is not None and not isinstance(r, Exception)])
                        print(f"Completed processing batch schedule for {item_name} ({item_data.get('uid', 'N/A')})")

                    results_list = [r for r in results_list if r]
                    results_list.sort(key=lambda x: x["live_at_time"] or datetime.min.replace(tzinfo=pytz.UTC), reverse=True)

                    teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in item_teachers if t.get('first_name')])
                    last_checked = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S %Z")
                    if item_type == "course":
                        caption = (
                            f"Course Name :- {item_name}\n"
                            f"Course Teacher :- {teachers}\n"
                            f"Start_at :- {item_starts_at}\n"
                            f"Ends_at :- {item_ends_at}\n"
                            f"Last_checked_at :- {last_checked}"
                        )
                    else:
                        caption = (
                            f"Batch Name :- {item_name}\n"
                            f"Batch Teachers :- {teachers}\n"
                            f"Start_at :- {item_starts_at}\n"
                            f"Completed_at :- {item_ends_at}\n"
                            f"Last_checked_at :- {last_checked}"
                        )

                    return results_list, caption

            except aiohttp.ClientResponseError as e:
                print(f"❌ HTTP error in schedule API for {item_type} (attempt {attempt + 1}/20): {e}")
                await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientTimeout:
                print(f"❌ Timeout in schedule API for {item_type} (attempt {attempt + 1}/20)")
                await asyncio.sleep(2 ** attempt)
            except aiohttp.ClientError as e:
                print(f"❌ Error in schedule API for {item_type} (attempt {attempt + 1}/20): {e}")
                await asyncio.sleep(2 ** attempt)

        print(f"Failed to fetch schedule for {item_type} after 20 attempts.")
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

async def send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches):
    """Send or update progress bar for /add command."""
    global progress_message, update_obj, update_context
    progress_text = (
        "📊 *Add Command Progress*\n"
        f"Courses: {uploaded_courses}/{total_courses}\n"
        f"Batches: {uploaded_batches}/{total_batches}"
    )
    if progress_message is None:
        progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")
    else:
        try:
            await progress_message.edit_text(progress_text, parse_mode="Markdown")
        except Exception as e:
            print(f"Error updating progress bar: {e}")
            progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")

async def progress_updater_add(total_courses, total_batches, get_uploaded_courses, get_uploaded_batches):
    """Update progress bar for /add every 2 minutes."""
    global progress_message
    while True:
        uploaded_courses = get_uploaded_courses()
        uploaded_batches = get_uploaded_batches()
        if uploaded_courses >= total_courses and uploaded_batches >= total_batches:
            break
        await send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches)
        await asyncio.sleep(120)  # 2 minutes

async def schedule_checker():
    """Check and update current batches and courses every 2 hours."""
    while True:
        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        for doc in educators_col.find():
            for item_type, items_key, end_key in [("course", "courses", "ends_at"), ("batch", "batches", "completed_at")]:
                for item in doc.get(items_key, []):
                    if item.get("is_completed", False):
                        continue
                    end_time_str = item.get(end_key, "N/A")
                    if end_time_str != "N/A":
                        try:
                            end_time = dateutil.parser.isoparse(end_time_str)
                            if end_time <= current_time:
                                # Mark as completed
                                caption = item.get("caption", "")
                                new_caption = caption + "\nNo More Check Batch/Course Completed"
                                try:
                                    await bot.edit_message_caption(
                                        chat_id=SETTED_GROUP_ID,
                                        message_id=item["msg_id"],
                                        caption=new_caption
                                    )
                                    educators_col.update_one(
                                        {"_id": doc["_id"], f"{items_key}.uid": item["uid"]},
                                        {"$set": {f"{items_key}.$.is_completed": True, f"{items_key}.$.caption": new_caption}}
                                    )
                                    print(f"Marked {item_type} {item['uid']} as completed")
                                except Exception as e:
                                    print(f"Error editing caption for completed {item_type} {item['uid']}: {e}")
                            else:
                                # Re-fetch schedule
                                print(f"Updating {item_type} {item['uid']}")
                                schedule_url = (
                                    f"https://api.unacademy.com/api/v1/batch/{item['uid']}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                                    if item_type == "batch"
                                    else f"https://unacademy.com/api/v3/collection/{item['uid']}/items?limit=10000"
                                )
                                results = None
                                caption = None
                                while results is None:
                                    results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)
                                    if results is None:
                                        print(f"Failed to fetch schedule for {item_type} {item['uid']}, retrying...")
                                        await asyncio.sleep(60)  # Wait 1 min before retry

                                filename = f"temp_{item_type}_{item['uid']}_schedule.json"
                                save_to_json(filename, results)
                                try:
                                    await bot.delete_message(chat_id=SETTED_GROUP_ID, message_id=item["msg_id"])
                                except Exception as e:
                                    print(f"Error deleting old message for {item_type} {item['uid']}: {e}")
                                try:
                                    with open(filename, "rb") as f:
                                        new_msg = await bot.send_document(
                                            chat_id=SETTED_GROUP_ID,
                                            message_thread_id=doc["subtopic_msg_id"],
                                            document=f,
                                            caption=caption
                                        )
                                    new_msg_id = new_msg.message_id
                                    educators_col.update_one(
                                        {"_id": doc["_id"], f"{items_key}.uid": item["uid"]},
                                        {"$set": {
                                            f"{items_key}.$.msg_id": new_msg_id,
                                            f"{items_key}.$.last_checked_at": last_checked,
                                            f"{items_key}.$.caption": caption
                                        }}
                                    )
                                    print(f"Updated {item_type} {item['uid']}")
                                    await asyncio.sleep(30)  # 30-second delay per JSON upload
                                except Exception as e:
                                    print(f"Error uploading updated {item_type} {item['uid']}: {e}")
                                finally:
                                    if os.path.exists(filename):
                                        os.remove(filename)
                                        print(f"Deleted {filename}")
                        except ValueError:
                            print(f"Invalid end time for {item_type} {item['uid']}")
        await asyncio.sleep(7200)  # 2 hours

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /add command."""
    global update_context, update_obj, progress_message
    update_context = context
    update_obj = update

    if not context.args:
        await update.message.reply_text("Please provide a username. Usage: /add {username}")
        return ConversationHandler.END

    raw_username = context.args[0]
    username = normalize_username(raw_username)
    await update.message.reply_text(f"Fetching data for username: {username}...")

    # Fetch educator details
    educator = await fetch_educator_by_username(username)
    if not educator:
        await update.message.reply_text(f"No educator found with username: {username}")
        return ConversationHandler.END

    # Check if educator exists in DB
    educator_doc = educators_col.find_one({"username": username})
    if educator_doc:
        thread_id = educator_doc["subtopic_msg_id"]
        title = educator_doc["topic_title"]
        print(f"Educator {username} already exists with thread ID {thread_id}")
    else:
        # Create subtopic
        title = f"{educator['first_name']} {educator['last_name']} [{raw_username}]"
        try:
            topic = await context.bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
            thread_id = topic.message_thread_id
        except Exception as e:
            await update.message.reply_text(f"Error creating topic in group: {e}. Ensure the group is a supergroup with topics enabled and bot has permissions.")
            return ConversationHandler.END

        # Store educator in DB
        educators_col.insert_one({
            "_id": ObjectId(),
            "first_name": educator["first_name"],
            "last_name": educator["last_name"],
            "username": username,
            "uid": educator["uid"],
            "avatar": educator["avatar"],
            "subtopic_msg_id": thread_id,
            "topic_title": title,
            "last_checked_time": None,
            "courses": [],
            "batches": []
        })

    # Store in user_data
    context.user_data['thread_id'] = thread_id
    context.user_data['group_id'] = SETTED_GROUP_ID
    context.user_data['topic_title'] = title

    # Fetch courses and batches
    print(f"Fetching courses for {username}...")
    courses = await fetch_courses(username)
    print(f"Fetching batches for {username}...")
    batches = await fetch_batches(username)

    # Get current time
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Update educator last_checked
    educators_col.update_one({"username": username}, {"$set": {"last_checked_time": last_checked}})

    # Filter current and completed
    current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
    completed_courses, completed_batches = filter_by_time(courses, batches, current_time, future=False)

    all_courses = current_courses + completed_courses
    all_batches = current_batches + completed_batches

    # Save all courses to MongoDB first
    course_datas = []
    for course in all_courses:
        is_current = course in current_courses
        course_data = {
            "uid": course["uid"],
            "name": course.get("name", "N/A"),
            "slug": course.get("slug", "N/A"),
            "thumbnail": course.get("thumbnail", "N/A"),
            "starts_at": course.get("starts_at", "N/A"),
            "ends_at": course.get("ends_at", "N/A"),
            "last_checked_at": None,
            "msg_id": None,
            "caption": None,
            "is_completed": not is_current
        }
        teachers = f"{course.get('author', {}).get('first_name', '')} {course.get('author', {}).get('last_name', '')}".strip()
        course_data["teachers"] = teachers
        course_datas.append(course_data)

    if course_datas:
        educators_col.update_one({"username": username}, {"$push": {"courses": {"$each": course_datas}}})

    # Save all batches to MongoDB first
    batch_datas = []
    for batch in all_batches:
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
            "last_checked_at": None,
            "msg_id": None,
            "caption": None,
            "is_completed": not is_current
        }
        teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in batch.get("authors", [])])
        batch_data["teachers"] = teachers
        batch_datas.append(batch_data)

    if batch_datas:
        educators_col.update_one({"username": username}, {"$push": {"batches": {"$each": batch_datas}}})

    # Get existing after saving
    existing_doc = educators_col.find_one({"username": username})
    total_courses = len(existing_doc.get("courses", []))
    total_batches = len(existing_doc.get("batches", []))

    # Count how many have msg_id (uploaded schedules)
    uploaded_courses = sum(1 for c in existing_doc.get("courses", []) if c.get("msg_id") is not None)
    uploaded_batches = sum(1 for b in existing_doc.get("batches", []) if b.get("msg_id") is not None)

    def get_uploaded_courses():
        doc = educators_col.find_one({"username": username})
        return sum(1 for c in doc.get("courses", []) if c.get("msg_id") is not None)

    def get_uploaded_batches():
        doc = educators_col.find_one({"username": username})
        return sum(1 for b in doc.get("batches", []) if b.get("msg_id") is not None)

    # Start progress updater
    progress_task = asyncio.create_task(progress_updater_add(total_courses, total_batches, get_uploaded_courses, get_uploaded_batches))

    # Upload educator JSON
    educator_data = {
        "username": username,
        "first_name": educator["first_name"],
        "last_name": educator["last_name"],
        "uid": educator["uid"],
        "avatar": educator["avatar"],
        "subtopic_msg_id": thread_id,
        "topic_title": title,
        "last_checked_time": last_checked
    }
    educator_filename = f"{username}_educator.json"
    save_to_json(educator_filename, educator_data)
    try:
        with open(educator_filename, "rb") as f:
            msg = await context.bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=(
                    f"Teacher Name :- {educator['first_name']} {educator['last_name']}\n"
                    f"Username :- {username}\n"
                    f"Uid :- {educator['uid']}\n"
                    f"Thumbnail :- {educator['avatar']}\n"
                    f"Last Checked :- {last_checked}"
                )
            )
        os.remove(educator_filename)
        print(f"Deleted {educator_filename} after upload")
        await asyncio.sleep(30)  # 30-second delay for educator JSON
    except Exception as e:
        await update.message.reply_text(f"Error uploading educator JSON: {e}")
        if os.path.exists(educator_filename):
            os.remove(educator_filename)
            print(f"Deleted {educator_filename} due to upload error")

    # Function to update item
    async def update_item(item, item_type):
        item_uid = item["uid"]
        # Check if already uploaded
        doc = educators_col.find_one({"username": username, f"{item_type + 'es'}.uid": item_uid})
        if doc:
            for db_item in doc.get(item_type + "es"):
                if db_item["uid"] == item_uid and db_item.get("msg_id") is not None:
                    print(f"Skipping already uploaded {item_type} {item_uid}")
                    return

        # Fetch and upload schedule JSON
        schedule_url = (
            f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
            if item_type == "batch"
            else f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
        )
        results = None
        caption = None
        while results is None:
            results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)
            if results is None:
                print(f"Failed to fetch schedule for {item_type} {item_uid}, retrying...")
                await asyncio.sleep(60)  # Wait 1 min before retry

        schedule_filename = f"{username}_{item_type}_{item_uid}_schedule.json"
        save_to_json(schedule_filename, results)
        try:
            with open(schedule_filename, "rb") as f:
                msg = await context.bot.send_document(
                    chat_id=SETTED_GROUP_ID,
                    message_thread_id=thread_id,
                    document=f,
                    caption=caption
                )
            msg_id = msg.message_id
            os.remove(schedule_filename)
            print(f"Deleted {schedule_filename} after upload")
            await asyncio.sleep(30)  # 30-second delay for schedule JSON
        except Exception as e:
            await update.message.reply_text(f"Error uploading {item_type} schedule {item_uid}: {e}")
            if os.path.exists(schedule_filename):
                os.remove(schedule_filename)
                print(f"Deleted {schedule_filename} due to upload error")
            return

        # Update MongoDB
        educators_col.update_one(
            {"username": username, f"{item_type + 'es'}.uid": item_uid},
            {"$set": {
                f"{item_type + 'es'}.$.last_checked_at": last_checked,
                f"{item_type + 'es'}.$.caption": caption,
                f"{item_type + 'es'}.$.msg_id": msg_id
            }}
        )

    # Update courses and batches
    for course in all_courses:
        await update_item(course, "course")
    for batch in all_batches:
        await update_item(batch, "batch")

    # Final progress bar update
    await send_progress_bar_add(total_courses, total_batches, get_uploaded_courses(), get_uploaded_batches())
    progress_task.cancel()

    await update.message.reply_text(f"Data JSON files uploaded to group topic: {title}")
    context.user_data['courses'] = courses
    context.user_data['batches'] = batches
    context.user_data['username'] = username
    context.user_data['last_checked'] = last_checked
    await update.message.reply_text("What do you want to fetch?\n1. Batch\n2. Course\nReply with '1' or '2', or type 'cancel' to exit.")
    return SELECT_TYPE

async def select_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the selection of batch or course."""
    user_input = update.message.text.lower()
    if user_input == 'cancel':
        await update.message.reply_text("Operation cancelled.")
        return ConversationHandler.END
    if user_input not in ['1', '2']:
        await update.message.reply_text("Please reply with '1' for Batch or '2' for Course, or 'cancel' to exit.")
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

    # Check DB for item
    doc = educators_col.find_one({"username": username, f"{item_type + 'es'}.uid": item_id})
    item_data = None
    if doc:
        for item in doc.get(item_type + "es", []):
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
    results = None
    caption = None
    while results is None:
        results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item_data)
        if results is None:
            print(f"Failed to fetch schedule for {item_type} {item_id}, retrying...")
            await asyncio.sleep(60)  # Wait 1 min before retry

    schedule_filename = f"{username}_{item_type}_{item_id}_schedule.json"
    save_to_json(schedule_filename, results)
    try:
        await context.bot.send_message(
            chat_id=group_id,
            message_thread_id=thread_id,
            text=caption
        )
        with open(schedule_filename, "rb") as f:
            msg = await context.bot.send_document(
                chat_id=group_id,
                message_thread_id=thread_id,
                document=f,
                caption=caption
            )
        new_msg_id = msg.message_id
        educators_col.update_one(
            {"username": username, f"{item_type + 'es'}.uid": item_id},
            {"$set": {f"{item_type + 'es'}.$.msg_id": new_msg_id, f"{item_type + 'es'}.$.last_checked_at": last_checked, f"{item_type + 'es'}.$.caption": caption}}
        )
        os.remove(schedule_filename)
        print(f"Deleted {schedule_filename} after upload")
        await asyncio.sleep(30)  # 30-second delay for schedule JSON
    except Exception as e:
        await update.message.reply_text(f"Error uploading schedule JSON to group topic: {e}")
        if os.path.exists(schedule_filename):
            os.remove(schedule_filename)
            print(f"Deleted {schedule_filename} due to upload error")

    await update.message.reply_text(f"Schedule uploaded to group topic: {topic_title}")
    await update.message.reply_text(f"Finished fetching details for {item_type} ID: {item_id}")
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
