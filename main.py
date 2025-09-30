import aiohttp
import asyncio
import json
import os
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ConversationHandler, MessageHandler, filters, ContextTypes
import re
from datetime import datetime
import dateutil.parser
import pytz
import pymongo

# Telegram group ID
SETTED_GROUP_ID = -1003133358948  # Replace with your supergroup ID

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://elvishyadav_opm:naman1811421@cluster0.uxuplor.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["unacademy_db"]
educators_col = db["educators"]

# Global bot for scheduler
bot = None

# Global variables for fetching
fetching = False
last_json_data = {}
progress_message = None
update_context = None
update_obj = None
loop = None

# Conversation states for /add
SELECT_TYPE, ENTER_ID = range(2)

def save_to_json(filename, data):
    """Save data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

async def fetch_educators(goal_uid="TMUVD", limit=50, max_offset=5000, json_data=None, filename="funkabhosda.json", known_educator_uids=None):
    """Fetch educators asynchronously and save to JSON."""
    async with aiohttp.ClientSession() as session:
        seen_usernames = set()
        educators = []
        json_data["educators"] = json_data.get("educators", [])
        offset = 0

        while offset <= max_offset:
            url = f"https://unacademy.com/api/v1/uplus/subscription/goal_educators/?goal_uid={goal_uid}&limit={limit}&offset={offset}"
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        print(f"Rate limited. Retrying after {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()

                    if isinstance(data, dict) and data.get("error_code") == "E001":
                        print("Error E001 encountered. Stopping educator fetch.")
                        break

                    results = data.get("results")
                    if results is None or not isinstance(results, list):
                        print(f"No valid educator results at offset {offset}.")
                        break

                    if not results:
                        print(f"No more educators at offset {offset}.")
                        break

                    for i, educator in enumerate(results, start=offset + 1):
                        username = educator.get("username")
                        uid = educator.get("uid")
                        if username and uid and uid not in known_educator_uids:
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

                    save_to_json(filename, json_data)
                    offset += limit
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                print(f"Request failed for educators at offset {offset}: {e}")
                break

        print(f"Total educators fetched: {len(educators)}")
        return educators

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

async def fetch_courses(username, limit=50, max_offset=5000, json_data=None, filename="funkabhosda.json"):
    """Fetch courses for a given username asynchronously."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        seen_uids = set()
        json_data["courses"] = json_data.get("courses", {})
        json_data["courses"][username] = json_data["courses"].get(username, [])
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
                            json_data["courses"][username].append({
                                "name": course.get("name", "N/A"),
                                "slug": course.get("slug", "N/A"),
                                "thumbnail": course.get("thumbnail", "N/A"),
                                "uid": course_uid,
                                "starts_at": course.get("starts_at", "N/A"),
                                "ends_at": course.get("ends_at", "N/A"),
                                "author": course.get("author", {})
                            })

                    save_to_json(filename, json_data)
                    offset += limit
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                print(f"Failed to fetch courses for {username} at offset {offset}: {e}")
                break

        print(f"Total courses fetched for {username}: {len(json_data['courses'][username])}")

async def fetch_batches(username, known_educator_uids, limit=50, max_offset=5000, json_data=None, filename="funkabhosda.json"):
    """Fetch batches for a given username asynchronously."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        seen_batch_uids = set()
        new_educators = []
        json_data["batches"] = json_data.get("batches", {})
        json_data["batches"][username] = json_data["batches"].get(username, [])
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
                            json_data["batches"][username].append({
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

                    save_to_json(filename, json_data)
                    offset += limit
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                print(f"Failed to fetch batches for {username} at offset {offset}: {e}")
                break

        print(f"Total batches fetched for {username}: {len(json_data['batches'][username])}")
        return new_educators

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
                    if end_time.year > 2035:
                        if not future:
                            filtered_data["courses"][username].append(course)
                    elif (future and end_time > current_time) or (not future and end_time <= current_time):
                        filtered_data["courses"][username].append(course)
                except ValueError:
                    continue

        for batch in json_data["batches"].get(username, []):
            completed_at = batch.get("completed_at")
            if completed_at and completed_at != "N/A":
                try:
                    complete_time = dateutil.parser.isoparse(completed_at)
                    if complete_time.year > 2035:
                        if not future:
                            filtered_data["batches"][username].append(batch)
                    elif (future and complete_time > current_time) or (not future and complete_time <= current_time):
                        filtered_data["batches"][username].append(batch)
                except ValueError:
                    continue

    return filtered_data

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

async def progress_updater_add(total_courses, total_batches, uploaded_courses, uploaded_batches):
    """Update progress bar for /add every 2 minutes."""
    global progress_message
    while uploaded_courses < total_courses or uploaded_batches < total_batches:
        await send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches)
        await asyncio.sleep(120)  # Update every 2 minutes

async def upload_item(item, item_type, is_current, caption_template, uploaded_courses, uploaded_batches, new_courses, new_batches, username, last_checked, thread_id, update_obj, context):
    """Upload a course or batch item to Telegram and update MongoDB."""
    item_uid = item["uid"]
    existing_course_uids = {course["uid"] for course in new_courses} | {course["uid"] for course in educators_col.find_one({"username": username}).get("courses", [])}
    existing_batch_uids = {batch["uid"] for batch in new_batches} | {batch["uid"] for batch in educators_col.find_one({"username": username}).get("batches", [])}
    
    if (item_type == "course" and item_uid in existing_course_uids) or (item_type == "batch" and item_uid in existing_batch_uids):
        print(f"Skipping already uploaded {item_type} {item_uid}")
        return uploaded_courses, uploaded_batches, new_courses, new_batches

    # Prepare item data for MongoDB (strictly follow schema, exclude caption)
    item_data = {
        "uid": item.get("uid", "N/A"),
        "name": item.get("name", "N/A"),
        "msg_id": None,  # Will be set after upload
        "last_checked_at": last_checked,
        "is_completed": not is_current
    }

    # Fetch schedule for JSON file (not stored in MongoDB)
    schedule_url = (
        f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
        if item_type == "batch"
        else f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
    )
    results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)

    # Prepare JSON data for file (includes schedule and caption)
    json_item_data = item.copy()
    json_item_data["schedule"] = results
    json_item_data["last_checked_at"] = last_checked
    json_item_data["is_completed"] = not is_current
    json_item_data["caption"] = caption

    filename = f"{username}_{'current' if is_current else 'completed'}_{item_type}_{item_uid}.json"
    save_to_json(filename, json_item_data)

    item_name = item.get("name", "N/A")
    item_starts_at = item.get("starts_at", "N/A")
    item_ends_at = item.get("ends_at" if item_type == "course" else "completed_at", "N/A")
    teachers = (
        f"{item.get('author', {}).get('first_name', '')} {item.get('author', {}).get('last_name', '')}".strip()
        if item_type == "course"
        else ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in item.get("authors", [])])
    )

    caption = caption_template.format(
        name=item_name,
        teachers=teachers,
        starts_at=item_starts_at,
        ends_at=item_ends_at,
        last_checked=last_checked
    )

    try:
        with open(filename, "rb") as f:
            msg = await context.bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=caption
            )
        msg_id = msg.message_id
        item_data["msg_id"] = msg_id
        os.remove(filename)
        print(f"Deleted {filename} after upload")
        if item_type == "course":
            new_courses.append(item_data)
            uploaded_courses += 1
        else:
            new_batches.append(item_data)
            uploaded_batches += 1
        await asyncio.sleep(30)  # 30-second delay per JSON file upload
    except Exception as e:
        await update_obj.message.reply_text(f"Error uploading {item_type} {item_uid}: {e}")
        if os.path.exists(filename):
            os.remove(filename)
    await asyncio.sleep(0.1)  # Small delay to prevent overwhelming the event loop
    return uploaded_courses, uploaded_batches, new_courses, new_batches

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
                                new_caption = f"{item_type.capitalize()} completed. No more checks."
                                try:
                                    await bot.edit_message_caption(
                                        chat_id=SETTED_GROUP_ID,
                                        message_id=item["msg_id"],
                                        caption=new_caption
                                    )
                                    educators_col.update_one(
                                        {"_id": doc["_id"], f"{items_key}.uid": item["uid"]},
                                        {"$set": {f"{items_key}.$.is_completed": True}}
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
                                results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item)
                                if results:
                                    filename = f"temp_{item_type}_{item['uid']}.json"
                                    item_data = item.copy()
                                    item_data["schedule"] = results
                                    item_data["last_checked_at"] = last_checked
                                    save_to_json(filename, item_data)
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
                                                f"{items_key}.$.last_checked_at": last_checked
                                            }}
                                        )
                                        print(f"Updated {item_type} {item['uid']}")
                                    except Exception as e:
                                        print(f"Error uploading updated {item_type} {item['uid']}: {e}")
                                    finally:
                                        if os.path.exists(filename):
                                            os.remove(filename)
                                            print(f"Deleted {filename}")
                                else:
                                    print(f"No results for update of {item_type} {item['uid']}")
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

        # Store educator in DB with schema-compliant structure
        educators_col.insert_one({
            "_id": pymongo.ObjectId(),
            "username": username,
            "first_name": educator["first_name"],
            "last_name": educator["last_name"],
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

    # Initialize JSON data
    json_data = {
        "educators": [educator],
        "courses": {},
        "batches": {}
    }
    known_educator_uids = {educator["uid"]}

    # Fetch courses and batches
    print(f"Fetching courses for {username}...")
    await fetch_courses(username, json_data=json_data, filename="temp.json")
    print(f"Fetching batches for {username}...")
    await fetch_batches(username, known_educator_uids, json_data=json_data, filename="temp.json")

    # Get current time
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Update educator last_checked
    educators_col.update_one({"username": username}, {"$set": {"last_checked_time": last_checked}})

    # Filter current and completed
    current_json_data = filter_by_time(json_data, current_time, future=True)
    completed_json_data = filter_by_time(json_data, current_time, future=False)

    # Get existing courses/batches from DB
    existing_doc = educators_col.find_one({"username": username})
    existing_course_uids = {course["uid"] for course in existing_doc.get("courses", [])}
    existing_batch_uids = {batch["uid"] for batch in existing_doc.get("batches", [])}

    # Prepare lists for DB update
    new_courses = []
    new_batches = []

    # Count totals
    total_courses = len(current_json_data["courses"].get(username, [])) + len(completed_json_data["courses"].get(username, []))
    total_batches = len(current_json_data["batches"].get(username, [])) + len(completed_json_data["batches"].get(username, []))
    uploaded_courses = len(existing_course_uids)
    uploaded_batches = len(existing_batch_uids)

    # Start progress updater
    progress_task = asyncio.create_task(progress_updater_add(total_courses, total_batches, uploaded_courses, uploaded_batches))

    # Upload educator JSON
    educator_data = educator.copy()
    educator_data["subtopic_msg_id"] = thread_id
    educator_data["last_checked_time"] = last_checked
    educator_filename = f"{username}_educator.json"
    save_to_json(educator_filename, educator_data)
    try:
        with open(educator_filename, "rb") as f:
            await context.bot.send_document(
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
    except Exception as e:
        await update.message.reply_text(f"Error uploading educator JSON: {e}")
        if os.path.exists(educator_filename):
            os.remove(educator_filename)

    # Caption templates
    course_caption_template = (
        "Course Name :- {name}\n"
        "Course Teacher :- {teachers}\n"
        "Start_at :- {starts_at}\n"
        "Ends_at :- {ends_at}\n"
        "Last_checked_at :- {last_checked}"
    )
    batch_caption_template = (
        "Batch Name :- {name}\n"
        "Batch Teachers :- {teachers}\n"
        "Start_at :- {starts_at}\n"
        "Completed_at :- {ends_at}\n"
        "Last_checked_at :- {last_checked}"
    )

    # Upload courses and batches
    for course in current_json_data["courses"].get(username, []):
        uploaded_courses, uploaded_batches, new_courses, new_batches = await upload_item(
            course, "course", True, course_caption_template, uploaded_courses, uploaded_batches,
            new_courses, new_batches, username, last_checked, thread_id, update_obj, context
        )
    for course in completed_json_data["courses"].get(username, []):
        uploaded_courses, uploaded_batches, new_courses, new_batches = await upload_item(
            course, "course", False, course_caption_template, uploaded_courses, uploaded_batches,
            new_courses, new_batches, username, last_checked, thread_id, update_obj, context
        )
    for batch in current_json_data["batches"].get(username, []):
        uploaded_courses, uploaded_batches, new_courses, new_batches = await upload_item(
            batch, "batch", True, batch_caption_template, uploaded_courses, uploaded_batches,
            new_courses, new_batches, username, last_checked, thread_id, update_obj, context
        )
    for batch in completed_json_data["batches"].get(username, []):
        uploaded_courses, uploaded_batches, new_courses, new_batches = await upload_item(
            batch, "batch", False, batch_caption_template, uploaded_courses, uploaded_batches,
            new_courses, new_batches, username, last_checked, thread_id, update_obj, context
        )

    # Update DB with new courses/batches
    if new_courses or new_batches:
        update_dict = {}
        if new_courses:
            update_dict["$push"] = {"courses": {"$each": new_courses}}
        if new_batches:
            update_dict["$push"] = update_dict.get("$push", {})
            update_dict["$push"]["batches"] = {"$each": new_batches}
        educators_col.update_one({"username": username}, update_dict)

    # Final progress bar update
    await send_progress_bar_add(total_courses, total_batches, uploaded_courses, uploaded_batches)
    progress_task.cancel()

    # Cleanup
    if os.path.exists("temp.json"):
        os.remove("temp.json")

    await update.message.reply_text(f"Data JSON files uploaded to group topic: {title}")
    context.user_data['json_data'] = json_data
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
    json_data = context.user_data.get('json_data')
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
        item_data = next((item for item in json_data[item_type + "es"].get(username, []) if item["uid"] == item_id), None)
        if not item_data:
            await update.message.reply_text(f"No {item_type} found with ID: {item_id}")
            return ConversationHandler.END

    schedule_url = (
        f"https://api.unacademy.com/api/v1/batch/{item_id}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
        if item_type == 'batch'
        else f"https://unacademy.com/api/v3/collection/{item_id}/items?limit=10000"
    )
    results, caption = await fetch_unacademy_schedule(schedule_url, item_type, item_data)
    if not results:
        await update.message.reply_text(f"No schedule data found for {item_type} ID: {item_id}")
        return ConversationHandler.END

    schedule_filename = f"{username}_{item_type}_{item_id}_schedule.json"
    save_to_json(schedule_filename, results)
    try:
        await context.bot.send_message(
            chat_id=group_id,
            message_thread_id=thread_id,
            text=caption
        )
        with open(schedule_filename, "rb") as f:
            await context.bot.send_document(
                chat_id=group_id,
                message_thread_id=thread_id,
                document=f,
                caption=caption
            )
        os.remove(schedule_filename)
        print(f"Deleted {schedule_filename} after upload")
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

async def send_progress_bar():
    """Send or update progress bar for /now."""
    global progress_message, update_obj, update_context
    educator_count, course_count, batch_count = count_items(last_json_data)
    progress_text = (
        "📊 *Progress Bar*\n"
        f"Total Educators Fetched: {educator_count}\n"
        f"Total Courses Fetched: {course_count}\n"
        f"Total Batches Fetched: {batch_count}"
    )
    if progress_message is None:
        progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")
    else:
        try:
            await progress_message.edit_text(progress_text, parse_mode="Markdown")
        except Exception as e:
            print(f"Error updating progress bar: {e}")
            progress_message = await update_obj.message.reply_text(progress_text, parse_mode="Markdown")

async def upload_json():
    """Upload funkabhosda.json to Telegram."""
    global update_obj, update_context
    try:
        with open("funkabhosda.json", "rb") as f:
            await update_context.bot.send_document(
                chat_id=update_obj.effective_chat.id,
                document=f,
                caption="Updated funkabhosda.json"
            )
    except Exception as e:
        await update_obj.message.reply_text(f"Error uploading JSON: {e}")

async def progress_updater():
    """Update progress bar every 60 seconds for /now."""
    global last_json_data, last_educator_count, last_course_count, last_batch_count
    last_upload_time = time.time()
    while fetching:
        try:
            with open("funkabhosda.json", "r", encoding="utf-8") as f:
                current_json_data = json.load(f)
            educator_count, course_count, batch_count = count_items(current_json_data)
            if (educator_count != last_educator_count or
                course_count != last_course_count or
                batch_count != last_batch_count):
                last_json_data = current_json_data
                last_educator_count, last_course_count, last_batch_count = educator_count, course_count, batch_count
                await send_progress_bar()
            current_time = time.time()
            if current_time - last_upload_time >= 120:
                await upload_json()
                last_upload_time = current_time
        except Exception as e:
            print(f"Error in progress updater: {e}")
        await asyncio.sleep(60)

def count_items(json_data):
    """Count educators, courses, and batches in json_data."""
    educator_count = len(json_data.get("educators", []))
    course_count = sum(len(courses) for courses in json_data.get("courses", {}).values())
    batch_count = sum(len(batches) for batches in json_data.get("batches", {}).values())
    return educator_count, course_count, batch_count

async def fetch_data_in_background_async():
    """Run the fetching process asynchronously for /now."""
    global fetching, last_json_data, last_educator_count, last_course_count, last_batch_count, progress_message
    json_data = {
        "educators": [],
        "courses": {},
        "batches": {}
    }
    known_educator_uids = set()
    filename = "funkabhosda.json"

    print("Fetching initial educators...")
    educators = await fetch_educators(json_data=json_data, filename=filename, known_educator_uids=known_educator_uids)
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
            await fetch_courses(username, json_data=json_data, filename=filename)
            print(f"\nFetching batches for {username}...")
            new_educators = await fetch_batches(username, known_educator_uids, json_data=json_data, filename=filename)
            if new_educators:
                print(f"\nNew educators found in batches for {username}:")
                for educator in new_educators:
                    print(f"{educator['first_name']} {educator['last_name']} : {educator['username']} : {educator['uid']} : {educator['avatar']}")
                    educator_queue.append((educator["username"], educator["uid"]))
            else:
                print(f"\nNo new educators found in batches for {username}.")

    if fetching:
        print("\nAll educators processed. Data saved to funkabhosda.json.")
        await upload_json()
        await send_progress_bar()
        await update_obj.message.reply_text("Fetching completed! Final funkabhosda.json uploaded.")
    else:
        print("\nFetching stopped by user.")
        await upload_json()
        await update_obj.message.reply_text("Fetching stopped. Partial funkabhosda.json uploaded.")
    fetching = False
    progress_message = None

async def now_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /now command."""
    global fetching, update_context, update_obj, loop
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return
    fetching = True
    update_context = context
    update_obj = update
    loop = asyncio.get_running_loop()
    await update.message.reply_text("Starting data fetch... ☠️")
    asyncio.create_task(progress_updater())
    asyncio.create_task(fetch_data_in_background_async())

async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /stop command."""
    global fetching, progress_message
    if not fetching:
        await update.message.reply_text("No fetching process is running!")
        return
    fetching = False
    progress_message = None
    await update.message.reply_text("Stopping fetching process...")

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
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("stop", stop_command))

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
