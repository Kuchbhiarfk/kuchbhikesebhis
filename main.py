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

# Telegram IDs
SETTED_GROUP_ID = -1003133358948
CHANNEL_ID = -1002927760779  # Change this to your channel ID

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

SELECT_RANGE, ENTER_RANGE = range(2)


def save_to_json(filename, data):
    """Save data to JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving {filename}: {e}")


async def fetch_educator_by_username(username):
    """Fetch educator details by username."""
    url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit=1"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    await asyncio.sleep(retry_after)
                    return await fetch_educator_by_username(username)
                response.raise_for_status()
                data = await response.json()
                results = data.get("results", [])
                if results:
                    author = results[0].get("author", {})
                    return {
                        "first_name": author.get("first_name", "N/A"),
                        "last_name": author.get("last_name", "N/A"),
                        "username": author.get("username", "N/A"),
                        "uid": author.get("uid", "N/A"),
                        "avatar": author.get("avatar", "N/A")
                    }
                return None
        except:
            return None


async def fetch_courses(username, limit=50):
    """Fetch courses for username."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}"
    courses = []
    async with aiohttp.ClientSession() as session:
        offset = 0
        while True:
            url = f"{base_url}&offset={offset}"
            async with session.get(url, timeout=10) as response:
                if response.status == 429:
                    await asyncio.sleep(5)
                    continue
                data = await response.json()
                results = data.get("results", [])
                if not results:
                    break
                for course in results:
                    courses.append({
                        "name": course.get("name"),
                        "slug": course.get("slug"),
                        "thumbnail": course.get("thumbnail"),
                        "uid": course.get("uid"),
                        "starts_at": course.get("starts_at"),
                        "ends_at": course.get("ends_at"),
                        "author": course.get("author", {})
                    })
                offset += limit
            await asyncio.sleep(0.1)
    return courses


async def fetch_batches(username, limit=50):
    """Fetch batches for username."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    batches = []
    async with aiohttp.ClientSession() as session:
        offset = 0
        while True:
            url = f"{base_url}&offset={offset}"
            async with session.get(url, timeout=10) as response:
                if response.status == 429:
                    await asyncio.sleep(5)
                    continue
                data = await response.json()
                results = data.get("results", [])
                if not results:
                    break
                for batch in results:
                    batches.append({
                        "name": batch.get("name"),
                        "cover_photo": batch.get("cover_photo"),
                        "exam_type": batch.get("goal", {}).get("name"),
                        "uid": batch.get("uid"),
                        "slug": batch.get("slug"),
                        "syllabus_tag": batch.get("syllabus_tag"),
                        "starts_at": batch.get("starts_at"),
                        "completed_at": batch.get("completed_at"),
                        "authors": batch.get("authors", [])
                    })
                offset += limit
            await asyncio.sleep(0.1)
    return batches


async def fetch_unacademy_schedule(schedule_url, item_type, item_data):
    """Fetch schedule data."""
    async with aiohttp.ClientSession() as session:
        for attempt in range(5):
            try:
                timeout = aiohttp.ClientTimeout(total=30)
                async with session.get(schedule_url, timeout=timeout) as response:
                    if response.status == 429:
                        await asyncio.sleep(5)
                        continue
                    data = await response.json()
                    results = data.get('results', [])
                    
                    schedule_items = []
                    item_name = item_data.get("name")
                    item_starts_at = item_data.get("starts_at")
                    item_ends_at = item_data.get("ends_at") if item_type == "course" else item_data.get("completed_at")
                    item_teachers = [item_data.get("author")] if item_type == "course" else item_data.get("authors", [])
                    
                    teachers_str = ", ".join([f"{t.get('first_name')} {t.get('last_name')}".strip() for t in item_teachers if t.get('first_name')])
                    last_checked = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S %Z")
                    
                    if item_type == "course":
                        caption = f"Course Name: {item_name}\nCourse Teacher: {teachers_str}\nStart_at: {item_starts_at}\nEnds_at: {item_ends_at}\nLast_checked_at: {last_checked}"
                        for item in results:
                            value = item.get("value", {})
                            schedule_items.append({
                                "class_name": value.get("title"),
                                "teacher_name": f"{value.get('live_class', {}).get('author', {}).get('first_name')} {value.get('live_class', {}).get('author', {}).get('last_name')}".strip(),
                                "live_at": value.get("live_class", {}).get("live_at"),
                                "class_url": "Live Soon" if value.get("live_class", {}).get("live_at") else "N/A",
                                "slides_url": "N/A",
                                "live_at_time": value.get("live_class", {}).get("live_at")
                            })
                    else:
                        caption = f"Batch Name: {item_name}\nBatch Teachers: {teachers_str}\nStart_at: {item_starts_at}\nCompleted_at: {item_ends_at}\nLast_checked_at: {last_checked}"
                        for item in results:
                            schedule_items.append({
                                "class_name": item.get("properties", {}).get("name"),
                                "teacher_name": "Multiple",
                                "live_at": item.get("properties", {}).get("live_at"),
                                "class_url": "Live Soon" if item.get("properties", {}).get("live_at") else "N/A",
                                "slides_url": "N/A",
                                "live_at_time": item.get("properties", {}).get("live_at")
                            })
                    
                    return schedule_items, caption
            except:
                await asyncio.sleep(2)
        return [], None


def normalize_username(username):
    """Normalize username."""
    return re.sub(r'[^a-zA-Z0-9]', '', username).lower()


def filter_by_time(courses, batches, current_time, future=True):
    """Filter current/future items."""
    if future:
        return [c for c in courses if dateutil.parser.isoparse(c.get("ends_at", "2030-01-01")) > current_time], \
               [b for b in batches if dateutil.parser.isoparse(b.get("completed_at", "2030-01-01")) > current_time]
    return [], []


async def upload_to_both_places(document_path, caption, group_thread_id, old_group_msg_id=None):
    """Upload to group + channel, return msg IDs."""
    try:
        # Delete old group message
        if old_group_msg_id:
            await bot.delete_message(SETTED_GROUP_ID, old_group_msg_id)
            await asyncio.sleep(1)
        
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
                document=f,
                caption=caption
            )
        channel_msg_id = channel_msg.message_id
        
        # Add channel link to caption
        channel_link = f"https://t.me/c/1839082077/{channel_msg_id}"
        final_caption = f"{caption}\n\nIn channel - {channel_link}"
        
        # Update group caption with link
        await bot.edit_message_caption(
            chat_id=SETTED_GROUP_ID,
            message_id=group_msg_id,
            caption=final_caption
        )
        
        return group_msg_id, channel_msg_id
        
    except Exception as e:
        print(f"Upload error: {e}")
        return None, None


async def send_progress_bar(teacher_name, courses_fetched, batches_fetched, total_courses, total_batches, 
                           uploaded_courses, uploaded_batches, phase):
    """Enhanced progress bar."""
    global progress_message, update_obj
    
    if phase == "courses":
        text = f"""🔥 Fetching Teacher: {teacher_name}
📚 Total Courses Fetched: {courses_fetched}
📦 Total Batches Fetched: {batches_fetched}

Phase 1: Uploading Courses
Progress: {uploaded_courses}/{total_courses}"""
    elif phase == "batches":
        text = f"""🔥 Fetching Teacher: {teacher_name}
📚 Total Courses Fetched: {courses_fetched}
📦 Total Batches Fetched: {batches_fetched}

✅ Courses Complete!
Phase 2: Uploading Batches
Progress: {uploaded_batches}/{total_batches}"""
    else:
        text = f"""🔥 Fetching Teacher: {teacher_name}
📚 Total Courses Fetched: {courses_fetched}
📦 Total Batches Fetched: {batches_fetched}

✅ UPLOAD COMPLETE!
Courses: {total_courses}/{total_courses} ✓
Batches: {total_batches}/{total_batches} ✓"""

    if progress_message:
        try:
            await progress_message.edit_text(text)
        except:
            pass
    else:
        progress_message = await update_obj.message.reply_text(text)


async def send_optry_progress(total, processed, new_found, current_teacher=""):
    """Optry progress."""
    global optry_progress_message, update_obj
    
    if current_teacher:
        text = f"""📊 /optry Progress
🔥 Current: {current_teacher}
Processed: {processed}/{total}
New Found: {new_found}"""
    else:
        text = f"""📊 /optry Progress
Processed: {processed}/{total}
New Found: {new_found}"""
    
    if optry_progress_message:
        await optry_progress_message.edit_text(text)
    else:
        optry_progress_message = await update_obj.message.reply_text(text)


# 🔥 CHANGE 10: Add new educators from courses/batches
async def add_new_educator_to_main(edu_username, edu_uid, edu_first, edu_last, edu_avatar):
    """Add NEW educator to main DB (3069 → 3070)."""
    existing_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}
    if edu_uid in existing_uids:
        return False
    
    try:
        title = f"{edu_first} {edu_last} [{edu_username}]"
        topic = await bot.create_forum_topic(chat_id=SETTED_GROUP_ID, name=title)
        thread_id = topic.message_thread_id
        
        educators_col.insert_one({
            "_id": ObjectId(),
            "first_name": edu_first,
            "last_name": edu_last,
            "username": normalize_username(edu_username),
            "uid": edu_uid,
            "avatar": edu_avatar,
            "group_id": SETTED_GROUP_ID,
            "subtopic_msg_id": thread_id,
            "topic_title": title,
            "last_checked_time": None,
            "courses": [],
            "batches": [],
            "channel_id": CHANNEL_ID,
            "channel_msg_ids": {}
        })
        print(f"✅ NEW EDUCATOR ADDED: {edu_first} {edu_last} (Total now: {len(existing_uids)+1})")
        return True
    except:
        return False


# 🔥 CHANGE 3 & 7: Schedule - 12hr + NEW courses/batches
async def schedule_checker():
    """IMMEDIATE START - Every 12hr, only if >=12hr passed."""
    print("🚀 Schedule checker STARTED IMMEDIATELY!")
    
    while True:
        try:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            twelve_hours_ago = current_time - timedelta(hours=12)
            
            for doc in educators_col.find({
                "last_checked_time": {"$lt": twelve_hours_ago.strftime("%Y-%m-%d %H:%M:%S %Z")}
            }):
                username = doc["username"]
                thread_id = doc["subtopic_msg_id"]
                print(f"🔄 Schedule: {username}")
                
                # 🔥 CHANGE 3: REFETCH NEW courses/batches
                courses = await fetch_courses(username)
                batches = await fetch_batches(username)
                
                # 🔥 CHANGE 10: Find NEW educators
                new_educators = set()
                for course in courses:
                    author = course.get("author")
                    if author and author.get("uid") and author.get("username"):
                        new_educators.add((author["username"], author["uid"], 
                                         author["first_name"], author["last_name"], author["avatar"]))
                for batch in batches:
                    for author in batch.get("authors", []):
                        if author and author.get("uid") and author.get("username"):
                            new_educators.add((author["username"], author["uid"], 
                                             author["first_name"], author["last_name"], author["avatar"]))
                
                for edu in new_educators:
                    await add_new_educator_to_main(*edu)
                
                # Get CURRENT items only
                current_courses, current_batches = filter_by_time(courses, batches, current_time, future=True)
                
                # 🔥 CHANGE 4: NO RE-UPLOAD of existing
                existing_course_uids = {c["uid"] for c in doc.get("courses", [])}
                existing_batch_uids = {b["uid"] for b in doc.get("batches", [])}
                
                new_courses = [c for c in current_courses if c["uid"] not in existing_course_uids]
                new_batches = [b for b in current_batches if b["uid"] not in existing_batch_uids]
                
                # Update existing items (schedule only)
                existing_courses = [c for c in doc.get("courses", []) if c["uid"] in existing_course_uids and not c.get("is_completed")]
                existing_batches = [b for b in doc.get("batches", []) if b["uid"] in existing_batch_uids and not b.get("is_completed")]
                
                # Process existing (UPDATE schedule)
                for course in existing_courses:
                    if current_time > dateutil.parser.isoparse(course["ends_at"]):
                        continue  # Skip completed
                    schedule_url = f"https://unacademy.com/api/v3/collection/{course['uid']}/items?limit=10000"
                    results, caption = await fetch_unacademy_schedule(schedule_url, "course", course)
                    if results:
                        filename = f"temp_course_{course['uid']}.json"
                        save_to_json(filename, results)
                        old_group_id = course.get("group_msg_id")
                        new_group_id, new_channel_id = await upload_to_both_places(filename, caption, thread_id, old_group_id)
                        if new_group_id:
                            educators_col.update_one(
                                {"_id": doc["_id"], "courses.uid": course["uid"]},
                                {"$set": {
                                    "courses.$.group_msg_id": new_group_id,
                                    "courses.$.channel_msg_id": new_channel_id,
                                    "courses.$.last_checked_at": current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
                                }}
                            )
                        os.remove(filename)
                
                for batch in existing_batches:
                    if current_time > dateutil.parser.isoparse(batch["completed_at"]):
                        continue
                    schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/"
                    results, caption = await fetch_unacademy_schedule(schedule_url, "batch", batch)
                    if results:
                        filename = f"temp_batch_{batch['uid']}.json"
                        save_to_json(filename, results)
                        old_group_id = batch.get("group_msg_id")
                        new_group_id, new_channel_id = await upload_to_both_places(filename, caption, thread_id, old_group_id)
                        if new_group_id:
                            educators_col.update_one(
                                {"_id": doc["_id"], "batches.uid": batch["uid"]},
                                {"$set": {
                                    "batches.$.group_msg_id": new_group_id,
                                    "batches.$.channel_msg_id": new_channel_id,
                                    "batches.$.last_checked_at": current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
                                }}
                            )
                        os.remove(filename)
                
                # Add NEW items to DB
                for course in new_courses:
                    educators_col.update_one(
                        {"_id": doc["_id"]},
                        {"$push": {
                            "courses": {
                                "uid": course["uid"], "name": course["name"], "slug": course["slug"],
                                "thumbnail": course["thumbnail"], "starts_at": course["starts_at"],
                                "ends_at": course["ends_at"], "group_msg_id": None, "channel_msg_id": None,
                                "last_checked_at": None, "is_completed": False
                            }
                        }}
                    )
                
                for batch in new_batches:
                    educators_col.update_one(
                        {"_id": doc["_id"]},
                        {"$push": {
                            "batches": {
                                "uid": batch["uid"], "name": batch["name"], "slug": batch["slug"],
                                "cover_photo": batch["cover_photo"], "exam_type": batch["exam_type"],
                                "syllabus_tag": batch["syllabus_tag"], "starts_at": batch["starts_at"],
                                "completed_at": batch["completed_at"], "group_msg_id": None, "channel_msg_id": None,
                                "last_checked_at": None, "is_completed": False
                            }
                        }}
                    )
                
                # Update last_checked
                educators_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"last_checked_time": current_time.strftime("%Y-%m-%d %H:%M:%S %Z")}}
                )
                
                gc.collect()
            
        except Exception as e:
            print(f"Schedule error: {e}")
        
        print("😴 Schedule sleep 12hr")
        await asyncio.sleep(43200)  # 12 hours


# 🔥 CHANGE 1: /optry NEW FLOW
async def optry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """NEW /optry flow."""
    global update_obj
    update_obj = update
    
    await update.message.reply_text(
        "🔥 /optry\n\n"
        "1. ALL Educators\n"
        "2. Next 10 Unprocessed\n\n"
        "Reply: `1` or `2`"
    )
    return SELECT_RANGE


async def select_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Select All/10."""
    choice = update.message.text.strip()
    
    if choice == '1':
        educators = list(collection_optry.find())
        total = len(educators)
        context.user_data['educators'] = educators
        context.user_data['total'] = total
        await update.message.reply_text(f"✅ ALL {total} selected!\nEnter range: `1-{total}`")
        return ENTER_RANGE
    
    elif choice == '2':
        # 🔥 Next 10 UNPROCESSED
        processed_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}
        unprocessed = [edu for edu in collection_optry.find() if edu['uid'] not in processed_uids][:10]
        
        if not unprocessed:
            await update.message.reply_text("❌ No unprocessed educators!")
            return ConversationHandler.END
        
        context.user_data['educators'] = unprocessed
        context.user_data['total'] = len(unprocessed)
        
        text = "📋 Next 10 Unprocessed:\n\n"
        for i, edu in enumerate(unprocessed, 1):
            name = f"{edu['first_name']} {edu['last_name']} [{edu['username']}]"
            text += f"{i}. {name}\n"
        text += f"\nEnter range: `1-{len(unprocessed)}`"
        await update.message.reply_text(text)
        return ENTER_RANGE
    
    else:
        await update.message.reply_text("❌ Reply 1 or 2!")
        return SELECT_RANGE


async def enter_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process range."""
    global optry_progress_message
    optry_progress_message = None
    
    try:
        start, end = map(int, update.message.text.split('-'))
        educators = context.user_data['educators']
        total = context.user_data['total']
        
        if start < 1 or end > total or start > end:
            await update.message.reply_text(f"❌ Invalid! Use 1-{total}")
            return ENTER_RANGE
        
        selected = educators[start-1:end]
        await update.message.reply_text(f"🚀 Starting {len(selected)} educators...")
        
        processed = 0
        new_found = 0
        
        for idx, educator in enumerate(selected, start):
            username = educator["username"]
            teacher_name = f"{educator['first_name']} {educator['last_name']}"
            
            await send_optry_progress(len(selected), processed, new_found, teacher_name)
            
            # Check if exists
            doc = educators_col.find_one({"username": normalize_username(username)})
            thread_id = None
            
            if doc:
                thread_id = doc["subtopic_msg_id"]
            else:
                edu_details = await fetch_educator_by_username(username)
                if not edu_details:
                    processed += 1
                    continue
                
                title = f"{edu_details['first_name']} {edu_details['last_name']} [{username}]"
                topic = await bot.create_forum_topic(SETTED_GROUP_ID, name=title)
                thread_id = topic.message_thread_id
                
                educators_col.insert_one({
                    "_id": ObjectId(), "first_name": edu_details["first_name"],
                    "last_name": edu_details["last_name"], "username": normalize_username(username),
                    "uid": edu_details["uid"], "avatar": edu_details["avatar"],
                    "group_id": SETTED_GROUP_ID, "subtopic_msg_id": thread_id,
                    "topic_title": title, "last_checked_time": None,
                    "courses": [], "batches": [], "channel_id": CHANNEL_ID, "channel_msg_ids": {}
                })
            
            # Fetch data
            courses = await fetch_courses(username)
            batches = await fetch_batches(username)
            
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            current_courses, current_batches = filter_by_time(courses, batches, current_time, True)
            
            # 🔥 CHANGE 10: Add new educators
            all_new_educators = set()
            for course in courses:
                author = course.get("author")
                if author and author.get("uid"):
                    all_new_educators.add((author["username"], author["uid"], 
                                        author["first_name"], author["last_name"], author["avatar"]))
            for batch in batches:
                for author in batch.get("authors", []):
                    if author and author.get("uid"):
                        all_new_educators.add((author["username"], author["uid"], 
                                            author["first_name"], author["last_name"], author["avatar"]))
            
            for edu in all_new_educators:
                if await add_new_educator_to_main(*edu):
                    new_found += 1
            
            # Get existing UIDs
            existing_doc = educators_col.find_one({"username": normalize_username(username)})
            existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
            existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}
            
            new_courses = [c for c in current_courses if c["uid"] not in existing_course_uids]
            new_batches = [b for b in current_batches if b["uid"] not in existing_batch_uids]
            
            # 🔥 CHANGE 11: Enhanced progress
            global progress_message
            progress_message = None
            
            # Upload EDUCATOR JSON
            last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
            edu_data = {"first_name": educator["first_name"], "last_name": educator["last_name"], 
                       "username": username, "last_checked_time": last_checked}
            filename = f"educator_{username}.json"
            save_to_json(filename, edu_data)
            _, channel_id = await upload_to_both_places(filename, 
                f"Teacher: {teacher_name}\nUsername: {username}\nLast Checked: {last_checked}", 
                thread_id)
            if channel_id:
                educators_col.update_one({"username": normalize_username(username)}, 
                                       {"$set": {"channel_msg_ids.educator": channel_id}})
            os.remove(filename)
            
            # Upload NEW courses
            for course in new_courses:
                schedule_url = f"https://unacademy.com/api/v3/collection/{course['uid']}/items?limit=10000"
                results, caption = await fetch_unacademy_schedule(schedule_url, "course", course)
                if results:
                    filename = f"course_{course['uid']}.json"
                    save_to_json(filename, results)
                    group_id, channel_id = await upload_to_both_places(filename, caption, thread_id)
                    if group_id:
                        educators_col.update_one(
                            {"username": normalize_username(username)},
                            {"$push": {
                                "courses": {
                                    "uid": course["uid"], "name": course["name"], "slug": course["slug"],
                                    "starts_at": course["starts_at"], "ends_at": course["ends_at"],
                                    "group_msg_id": group_id, "channel_msg_id": channel_id,
                                    "last_checked_at": last_checked, "is_completed": False
                                }
                            }}
                        )
                    os.remove(filename)
                await send_progress_bar(teacher_name, len(courses), len(batches), 
                                      len(new_courses), len(new_batches), 1, 0, "courses")
                await asyncio.sleep(2)
            
            # Upload NEW batches
            for batch in new_batches:
                schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/"
                results, caption = await fetch_unacademy_schedule(schedule_url, "batch", batch)
                if results:
                    filename = f"batch_{batch['uid']}.json"
                    save_to_json(filename, results)
                    group_id, channel_id = await upload_to_both_places(filename, caption, thread_id)
                    if group_id:
                        educators_col.update_one(
                            {"username": normalize_username(username)},
                            {"$push": {
                                "batches": {
                                    "uid": batch["uid"], "name": batch["name"], "slug": batch["slug"],
                                    "cover_photo": batch["cover_photo"], "exam_type": batch["exam_type"],
                                    "syllabus_tag": batch["syllabus_tag"], "starts_at": batch["starts_at"],
                                    "completed_at": batch["completed_at"], "group_msg_id": group_id,
                                    "channel_msg_id": channel_id, "last_checked_at": last_checked, "is_completed": False
                                }
                            }}
                        )
                    os.remove(filename)
                await send_progress_bar(teacher_name, len(courses), len(batches), 
                                      len(new_courses), len(new_batches), 1, 1, "batches")
                await asyncio.sleep(2)
            
            await send_progress_bar(teacher_name, len(courses), len(batches), 
                                  len(new_courses), len(new_batches), 1, 1, "complete")
            
            educators_col.update_one({"username": normalize_username(username)}, 
                                   {"$set": {"last_checked_time": last_checked}})
            processed += 1
            gc.collect()
        
        await send_optry_progress(len(selected), processed, new_found)
        await update.message.reply_text(
            f"🎉 COMPLETE!\n"
            f"Processed: {processed}\n"
            f"New Educators: {new_found}\n"
            f"Total Now: {3069 + new_found}"
        )
        
    except ValueError:
        await update.message.reply_text("❌ Format: 1-5")
        return ENTER_RANGE
    
    return ConversationHandler.END


async def cancel(update: Update, context):
    await update.message.reply_text("❌ Cancelled!")
    return ConversationHandler.END


async def main():
    """Start bot."""
    global bot
    bot_token = '7213717609:AAFeIOkjjXBB6bHnz0CmWtrIKxh7wp3OYbE'
    application = Application.builder().token(bot_token).build()
    bot = application.bot

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("optry", optry_command)],
        states={
            SELECT_RANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_range)],
            ENTER_RANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_range)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )

    application.add_handler(conv_handler)

    print("🚀 Bot starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    # 🔥 CHANGE 2 & 6: IMMEDIATE schedule + parallel
    asyncio.create_task(schedule_checker())
    
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        print("👋 Shutdown")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
