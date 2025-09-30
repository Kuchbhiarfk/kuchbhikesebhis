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

# Group ID for topics
SETTED_GROUP_ID = -1003133358948

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://elvishyadav_opm:naman1811421@cluster0.uxuplor.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db = client["unacademy_db"]
educators_col = db["educators"]
courses_col = db["courses"]
batches_col = db["batches"]

# Global bot for scheduler
bot = None

# Global variables for /now command
fetching = False
last_json_data = {}
progress_message = None
update_context = None
update_obj = None

# Conversation states for /add command
SELECT_TYPE, ENTER_ID = range(2)

def save_to_json(filename, data):
    """Save data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

async def fetch_educators(goal_uid="TMUVD", limit=50, max_offset=1000, json_data=None, filename="funkabhosda.json", known_educator_uids=None):
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

                    save_to_json(filename, json_data)
                    offset += limit
                    await asyncio.sleep(0.1)
            except aiohttp.ClientError as e:
                print(f"Request failed for educators: {e}")
                break

        return educators

async def fetch_educator_by_username(username):
    """Fetch educator details by username from course API asynchronously."""
    url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit=1&type=latest"
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

async def fetch_courses(username, limit=50, max_offset=1000):
    """Fetch courses for a given username and return them."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}&type=latest"
    async with aiohttp.ClientSession() as session:
        seen_uids = set()
        courses = []
        offset = 1

        while offset <= max_offset:
            url = f"{base_url}&offset={offset}"
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()

                    if isinstance(data, dict) and data.get("error_code") == "E001":
                        break

                    results = data.get("results")
                    if not results or not isinstance(results, list):
                        break

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
                print(f"Failed to fetch courses for {username}: {e}")
                break

        return courses

async def fetch_batches(username, limit=50, max_offset=1000):
    """Fetch batches for a given username and return them."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    async with aiohttp.ClientSession() as session:
        seen_batch_uids = set()
        batches = []
        offset = 2

        while offset <= max_offset:
            url = f"{base_url}&offset={offset}"
            try:
                async with session.get(url, timeout=10) as response:
                    if response.status == 429:
                        retry_after = int(response.headers.get("Retry-After", 5))
                        await asyncio.sleep(retry_after)
                        continue
                    response.raise_for_status()
                    data = await response.json()

                    if isinstance(data, dict) and data.get("error_code") == "E001":
                        break

                    results = data.get("results")
                    if not results or not isinstance(results, list):
                        break

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
                print(f"Failed to fetch batches for {username}: {e}")
                break

        return batches

def normalize_username(username):
    """Normalize username to lowercase and remove special characters."""
    return re.sub(r'[^a-zA-Z0-9]', '', username).lower()

async def fetch_unacademy_schedule(schedule_url, item_type, item_data):
    """Fetch schedule for a batch or course and return sorted results."""
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
                                except (aiohttp.ClientTimeout, aiohttp.ClientError):
                                    if retry < 2:
                                        await asyncio.sleep(2 ** retry)
                                        continue
                                    return handle_collection_failure(live_at, properties.get('name', 'N/A'), author)
                            return None

                        tasks = [fetch_collection_item(item) for item in results]
                        collection_results = await asyncio.gather(*tasks, return_exceptions=True)
                        results_list.extend([r for r in collection_results if r is not None and not isinstance(r, Exception)])

                    results_list = [r for r in results_list if r]
                    results_list.sort(key=lambda x: dateutil.parser.isoparse(x["live_at"]) if x["live_at"] != "N/A" else datetime.min.replace(tzinfo=pytz.UTC), reverse=True)

                    return results_list, None

            except (aiohttp.ClientTimeout, aiohttp.ClientError) as e:
                await asyncio.sleep(2 ** attempt)

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

    return {
        "class_name": title,
        "teacher_name": f"{author.get('first_name', '')} {author.get('last_name', '')}".strip(),
        "live_at": live_at,
        "thumbnail": author.get('avatar', 'N/A'),
        "class_url": class_url,
        "slides_url": slides_url,
        "is_offline": is_offline
    }

def handle_collection_failure(live_at, class_name, author):
    """Handle collection API failure."""
    current_time = datetime.now(pytz.UTC)
    class_url = "N/A"
    slides_url = "N/A"

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

    return {
        "class_name": class_name,
        "teacher_name": f"{author.get('first_name', '')} {author.get('last_name', '')}".strip(),
        "live_at": live_at,
        "thumbnail": author.get('avatar', 'N/A'),
        "class_url": class_url,
        "slides_url": slides_url,
        "is_offline": "N/A"
    }

async def schedule_checker():
    """Check and update current batches and courses every 2 hours."""
    while True:
        await asyncio.sleep(7200)  # 2 hours
        
        current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
        last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
        
        print(f"Running schedule checker at {last_checked}")
        
        for col, item_type in [(courses_col, "course"), (batches_col, "batch")]:
            for doc in col.find({"is_completed": False}):
                end_key = "ends_at" if item_type == "course" else "completed_at"
                end_time_str = doc.get(end_key, "N/A")
                
                if end_time_str != "N/A":
                    try:
                        end_time = dateutil.parser.isoparse(end_time_str)
                        if end_time <= current_time:
                            # Mark as completed
                            caption = doc.get("caption", "")
                            new_caption = caption + "\nNo More Check - Batch/Course Completed"
                            try:
                                await bot.edit_message_caption(
                                    chat_id=SETTED_GROUP_ID,
                                    message_id=doc["msg_id"],
                                    caption=new_caption
                                )
                                col.update_one(
                                    {"_id": doc["_id"]},
                                    {"$set": {"is_completed": True, "caption": new_caption}}
                                )
                                print(f"Marked {item_type} {doc['uid']} as completed")
                            except Exception as e:
                                print(f"Error marking {item_type} {doc['uid']} as completed: {e}")
                        else:
                            # Re-fetch and update
                            print(f"Updating {item_type} {doc['uid']}")
                            item_data = doc
                            
                            if item_type == "batch":
                                schedule_url = f"https://api.unacademy.com/api/v1/batch/{doc['uid']}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
                            else:
                                schedule_url = f"https://unacademy.com/api/v3/collection/{doc['uid']}/items?limit=10000"
                            
                            results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item_data)
                            
                            if results:
                                # Update JSON file
                                item_data["schedule"] = results
                                item_data["last_checked_at"] = last_checked
                                
                                filename = f"temp_{item_type}_{doc['uid']}.json"
                                save_to_json(filename, item_data)
                                
                                # Update caption
                                if item_type == "course":
                                    teacher = item_data.get("author", {})
                                    teachers = f"{teacher.get('first_name', '')} {teacher.get('last_name', '')}".strip()
                                    caption = (
                                        f"Course Name :- {item_data.get('name', 'N/A')}\n"
                                        f"Course Teacher :- {teachers}\n"
                                        f"Start_at :- {item_data.get('starts_at', 'N/A')}\n"
                                        f"Ends_at :- {item_data.get('ends_at', 'N/A')}\n"
                                        f"Last Checked :- {last_checked}"
                                    )
                                else:
                                    authors = item_data.get("authors", [])
                                    teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in authors])
                                    caption = (
                                        f"Batch Name :- {item_data.get('name', 'N/A')}\n"
                                        f"Batch Teachers :- {teachers}\n"
                                        f"Start_at :- {item_data.get('starts_at', 'N/A')}\n"
                                        f"Completed_at :- {item_data.get('completed_at', 'N/A')}\n"
                                        f"Last Checked :- {last_checked}"
                                    )
                                
                                try:
                                    # Delete old message
                                    await bot.delete_message(
                                        chat_id=SETTED_GROUP_ID,
                                        message_id=doc["msg_id"]
                                    )
                                except Exception as e:
                                    print(f"Error deleting old message for {item_type} {doc['uid']}: {e}")
                                
                                # Send new message
                                with open(filename, "rb") as f:
                                    new_msg = await bot.send_document(
                                        chat_id=SETTED_GROUP_ID,
                                        message_thread_id=doc["thread_id"],
                                        document=f,
                                        caption=caption
                                    )
                                
                                os.remove(filename)
                                
                                # Update database
                                col.update_one(
                                    {"_id": doc["_id"]},
                                    {"$set": {
                                        "msg_id": new_msg.message_id,
                                        "last_checked_at": last_checked,
                                        "caption": caption,
                                        "schedule": results
                                    }}
                                )
                                print(f"Updated {item_type} {doc['uid']}")
                            else:
                                print(f"No results for update of {item_type} {doc['uid']}")
                    except ValueError:
                        print(f"Invalid end time for {item_type} {doc['uid']}")

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle the /add command."""
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
    else:
        # Create topic
        title = f"{educator['first_name']} {educator['last_name']} [{raw_username}]"
        try:
            topic = await context.bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
            thread_id = topic.message_thread_id
        except Exception as e:
            await update.message.reply_text(f"Error creating topic: {e}")
            return ConversationHandler.END

        # Store in DB
        educators_col.insert_one({
            "first_name": educator["first_name"],
            "last_name": educator["last_name"],
            "username": username,
            "uid": educator["uid"],
            "avatar": educator["avatar"],
            "subtopic_msg_id": thread_id,
            "topic_title": title
        })

    # Get current time
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Fetch courses and batches
    print(f"Fetching courses for {username}...")
    courses = await fetch_courses(username)
    
    print(f"Fetching batches for {username}...")
    batches = await fetch_batches(username)

    # Upload educator info
    educator_data = educator.copy()
    educator_data["subtopic_msg_id"] = thread_id
    educator_data["last_checked_time"] = last_checked
    educator_filename = f"{username}_educator.json"
    save_to_json(educator_filename, educator_data)

    educator_caption = (
        f"Teacher Name :- {educator['first_name']} {educator['last_name']}\n"
        f"Username :- {username}\n"
        f"Uid :- {educator['uid']}\n"
        f"Thumbnail :- {educator['avatar']}\n"
        f"Last Checked :- {last_checked}"
    )
    
    try:
        with open(educator_filename, "rb") as f:
            await context.bot.send_document(
                chat_id=SETTED_GROUP_ID,
                message_thread_id=thread_id,
                document=f,
                caption=educator_caption
            )
        os.remove(educator_filename)
    except Exception as e:
        await update.message.reply_text(f"Error uploading educator JSON: {e}")
        if os.path.exists(educator_filename):
            os.remove(educator_filename)

    # Function to upload individual item with schedule
    async def upload_item(item, item_type):
        item_uid = item["uid"]
        item_data = item.copy()
        item_data["educator_username"] = username
        item_data["thread_id"] = thread_id
        
        # Check if already completed
        end_key = "ends_at" if item_type == "course" else "completed_at"
        end_time_str = item.get(end_key, "N/A")
        is_completed = False
        
        if end_time_str != "N/A":
            try:
                end_time = dateutil.parser.isoparse(end_time_str)
                if end_time <= current_time:
                    is_completed = True
            except ValueError:
                pass
        
        # Fetch schedule
        if item_type == 'batch':
            schedule_url = f"https://api.unacademy.com/api/v1/batch/{item_uid}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330"
            authors = item.get("authors", [])
            teachers = ", ".join([f"{t.get('first_name', '')} {t.get('last_name', '')}".strip() for t in authors])
        else:
            schedule_url = f"https://unacademy.com/api/v3/collection/{item_uid}/items?limit=10000"
            author = item.get('author', {})
            teachers = f"{author.get('first_name', '')} {author.get('last_name', '')}".strip()

        results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item)
        item_data["schedule"] = results
        item_data["last_checked_at"] = last_checked

        filename = f"{username}_{item_type}_{item_uid}.json"
        save_to_json(filename, item_data)

        item_name = item.get("name", "N/A")
        item_starts_at = item.get("starts_at", "N/A")
        item_ends_at = item.get(end_key, "N/A")

        if item_type == "course":
            caption = (
                f"Course Name :- {item_name}\n"
                f"Course Teacher :- {teachers}\n"
                f"Start_at :- {item_starts_at}\n"
                f"Ends_at :- {item_ends_at}\n"
                f"Last Checked :- {last_checked}"
            )
        else:
            caption = (
                f"Batch Name :- {item_name}\n"
                f"Batch Teachers :- {teachers}\n"
                f"Start_at :- {item_starts_at}\n"
                f"Completed_at :- {item_ends_at}\n"
                f"Last Checked :- {last_checked}"
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
            os.remove(filename)
            
            # Store in DB
            doc = {
                "name": item_name,
                "uid": item_uid,
                "educator_username": username,
                "starts_at": item_starts_at,
                end_key: item_ends_at,
                "slug": item.get("slug", "N/A"),
                "schedule": results,
                "msg_id": msg_id,
                "thread_id": thread_id,
                "last_checked_at": last_checked,
                "is_completed": is_completed,
                "caption": caption
            }
            
            if item_type == "course":
                doc["author"] = item.get("author", {})
                doc["thumbnail"] = item.get("thumbnail", "N/A")
            else:
                doc["authors"] = item.get("authors", [])
                doc["cover_photo"] = item.get("cover_photo", "N/A")
                doc["exam_type"] = item.get("exam_type", "N/A")
                doc["syllabus_tag"] = item.get("syllabus_tag", "N/A")
            
            col = courses_col if item_type == "course" else batches_col
            col.update_one({"uid": item_uid}, {"$set": doc}, upsert=True)
            
            print(f"Uploaded {item_type} {item_name} ({item_uid})")
        except Exception as e:
            await update.message.reply_text(f"Error uploading {item_type} {item_uid}: {e}")
            if os.path.exists(filename):
                os.remove(filename)
        
        # 30 second delay between uploads
        await asyncio.sleep(30)

    # Upload all courses
    total_items = len(courses) + len(batches)
    uploaded_count = 0
    
    for course in courses:
        await upload_item(course, "course")
        uploaded_count += 1
        try:
            await update.message.reply_text(f"Progress: {uploaded_count}/{total_items} items uploaded")
        except:
            pass

    # Upload all batches
    for batch in batches:
        await upload_item(batch, "batch")
        uploaded_count += 1
        try:
            await update.message.reply_text(f"Progress: {uploaded_count}/{total_items} items uploaded")
        except:
            pass

    await update.message.reply_text(f"✅ Completed! All data uploaded to topic: {title}")
    return ConversationHandler.END

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

    col = batches_col if item_type == 'batch' else courses_col
    item_data = col.find_one({"uid": item_id})
    
    if not item_data:
        await update.message.reply_text(f"No {item_type} found with ID: {item_id}")
        return ConversationHandler.END

    schedule_url = f"https://api.unacademy.com/api/v1/batch/{item_id}/schedule/?limit=100000&offset=None&past=True&rank=100000&timezone_difference=330" if item_type == 'batch' else f"https://unacademy.com/api/v3/collection/{item_id}/items?limit=10000"
    
    results, _ = await fetch_unacademy_schedule(schedule_url, item_type, item_data)

    if not results:
        await update.message.reply_text(f"No schedule data found for {item_type} ID: {item_id}")
        return ConversationHandler.END

    schedule_filename = f"{item_type}_{item_id}_schedule.json"
    save_to_json(schedule_filename, {"schedule": results})

    try:
        with open(schedule_filename, "rb") as f:
            await update.message.reply_document(
                document=f,
                caption=f"Schedule for {item_type} {item_id}"
            )
        os.remove(schedule_filename)
    except Exception as e:
        await update.message.reply_text(f"Error uploading schedule: {e}")
        if os.path.exists(schedule_filename):
            os.remove(schedule_filename)

    await update.message.reply_text(f"Finished fetching details for {item_type} ID: {item_id}")
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the conversation."""
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END

def count_items(json_data):
    """Count educators, courses, and batches in json_data."""
    educator_count = len(json_data.get("educators", []))
    course_count = sum(len(courses) for courses in json_data.get("courses", {}).values())
    batch_count = sum(len(batches) for batches in json_data.get("batches", {}).values())
    return educator_count, course_count, batch_count

async def send_progress_bar():
    """Send or update the progress bar message."""
    global progress_message, update_obj, last_json_data
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
    """Upload the funkabhosda.json file to Telegram."""
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
    """Update progress bar every 5 minutes."""
    global last_json_data, progress_message
    
    while fetching:
        try:
            if os.path.exists("funkabhosda.json"):
                with open("funkabhosda.json", "r", encoding="utf-8") as f:
                    last_json_data = json.load(f)
                await send_progress_bar()
        except Exception as e:
            print(f"Error in progress updater: {e}")
        
        await asyncio.sleep(300)  # 5 minutes

async def fetch_data_in_background_async():
    """Run the fetching process asynchronously for /now command."""
    global fetching, last_json_data
    
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
            courses = await fetch_courses(username)
            json_data["courses"][username] = courses

            print(f"\nFetching batches for {username}...")
            batches = await fetch_batches(username)
            json_data["batches"][username] = batches

            # Extract new educators from batch authors
            for batch in batches:
                for author in batch.get("authors", []):
                    author_uid = author.get("uid")
                    if author_uid and author_uid not in known_educator_uids:
                        known_educator_uids.add(author_uid)
                        json_data["educators"].append({
                            "first_name": author.get("first_name", "N/A"),
                            "last_name": author.get("last_name", "N/A"),
                            "username": author.get("username", "N/A"),
                            "uid": author_uid,
                            "avatar": author.get("avatar", "N/A")
                        })
                        educator_queue.append((author.get("username", ""), author_uid))

            save_to_json(filename, json_data)
            last_json_data = json_data

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
    global fetching, update_context, update_obj
    if fetching:
        await update.message.reply_text("Fetching is already in progress! Use /stop to stop it.")
        return
    
    fetching = True
    update_context = context
    update_obj = update
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
    bot_token = '8279128725:AAEEJq59EUBCXAxOnfGJCnFpkY0S3nM--Ec'
    application = Application.builder().token(bot_token).build()
    
    bot = application.bot

    # Conversation handler for /add
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
    
    # Start scheduler in background
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
