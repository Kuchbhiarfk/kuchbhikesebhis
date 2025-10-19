import aiohttp
import asyncio
import json
import os
import gc
from telegram import Update
from telegram.ext import Application, CommandHandler, ConversationHandler, MessageHandler, filters, ContextTypes
from telegram.error import BadRequest
import re
from datetime import datetime, timedelta
import dateutil.parser
import pytz
import pymongo
from bson import ObjectId

# Telegram IDs
SETTED_GROUP_ID = -1003133358948
CHANNEL_ID = -1002927760779  # Change your channel ID

# MongoDB connections
client_original = pymongo.MongoClient("mongodb+srv://elvishyadav_opm:naman1811421@cluster0.uxuplor.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db_original = client_original["unacademy_db"]
educators_col = db_original["educators"]

client_optry = pymongo.MongoClient('mongodb+srv://elvishyadavop:ClA5yIHTbCutEnVP@cluster0.u83zlfx.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0')
db_optry = client_optry['unacademy_db']
collection_optry = db_optry['educators']

# Global variables
bot = None
current_optry_index = 0  # Track NEXT 10 position
schedule_running = True
SELECT_MODE, ENTER_RANGE = range(2)

def save_to_json(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

async def fetch_educator_by_username(username):
    url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit=1"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=10) as response:
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

async def fetch_courses(username, limit=50):
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}"
    courses = []
    async with aiohttp.ClientSession() as session:
        offset = 0
        while True:
            url = f"{base_url}&offset={offset}"
            async with session.get(url, timeout=10) as response:
                data = await response.json()
                results = data.get("results", [])
                if not results: break
                for course in results:
                    courses.append({
                        "name": course.get("name"), "slug": course.get("slug"),
                        "thumbnail": course.get("thumbnail"), "uid": course.get("uid"),
                        "starts_at": course.get("starts_at"), "ends_at": course.get("ends_at"),
                        "author": course.get("author", {})
                    })
                offset += limit
            await asyncio.sleep(0.1)
    return courses

async def fetch_batches(username, limit=50):
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    batches = []
    async with aiohttp.ClientSession() as session:
        offset = 0
        while True:
            url = f"{base_url}&offset={offset}"
            async with session.get(url, timeout=10) as response:
                data = await response.json()
                results = data.get("results", [])
                if not results: break
                for batch in results:
                    batches.append({
                        "name": batch.get("name"), "cover_photo": batch.get("cover_photo"),
                        "exam_type": batch.get("goal", {}).get("name"), "uid": batch.get("uid"),
                        "slug": batch.get("slug"), "syllabus_tag": batch.get("syllabus_tag"),
                        "starts_at": batch.get("starts_at"), "completed_at": batch.get("completed_at"),
                        "authors": batch.get("authors", [])
                    })
                offset += limit
            await asyncio.sleep(0.1)
    return batches

async def fetch_unacademy_schedule(schedule_url, item_type, item_data):
    async with aiohttp.ClientSession() as session:
        async with session.get(schedule_url, timeout=30) as response:
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
                        "class_name": value.get("title"), "teacher_name": "Live Teacher",
                        "live_at": value.get("live_class", {}).get("live_at"), "class_url": "Live Soon",
                        "slides_url": "N/A", "live_at_time": value.get("live_class", {}).get("live_at")
                    })
            else:
                caption = f"Batch Name: {item_name}\nBatch Teachers: {teachers_str}\nStart_at: {item_starts_at}\nCompleted_at: {item_ends_at}\nLast_checked_at: {last_checked}"
                for item in results:
                    schedule_items.append({
                        "class_name": item.get("properties", {}).get("name"), "teacher_name": "Batch Teacher",
                        "live_at": item.get("properties", {}).get("live_at"), "class_url": "Live Soon",
                        "slides_url": "N/A", "live_at_time": item.get("properties", {}).get("live_at")
                    })
            return schedule_items, caption

def normalize_username(username):
    return re.sub(r'[^a-zA-Z0-9]', '', username).lower()

def filter_by_time(courses, batches, current_time, future=True):
    if future:
        return [c for c in courses if dateutil.parser.isoparse(c.get("ends_at", "2030-01-01")) > current_time], \
               [b for b in batches if dateutil.parser.isoparse(b.get("completed_at", "2030-01-01")) > current_time]
    return [], []

async def upload_to_both_places(document_path, caption, group_thread_id, old_group_msg_id=None):
    try:
        if old_group_msg_id:
            await bot.delete_message(SETTED_GROUP_ID, old_group_msg_id)
            await asyncio.sleep(1)
        
        with open(document_path, "rb") as f:
            group_msg = await bot.send_document(
                chat_id=SETTED_GROUP_ID, message_thread_id=group_thread_id,
                document=f, caption=caption
            )
        group_msg_id = group_msg.message_id
        
        with open(document_path, "rb") as f:
            channel_msg = await bot.send_document(chat_id=CHANNEL_ID, document=f, caption=caption)
        channel_msg_id = channel_msg.message_id
        
        channel_link = f"https://t.me/c/1839082077/{channel_msg_id}"
        final_caption = f"{caption}\n\nIn channel - {channel_link}"
        await bot.edit_message_caption(SETTED_GROUP_ID, group_msg_id, caption=final_caption)
        
        return group_msg_id, channel_msg_id
    except Exception as e:
        print(f"Upload error: {e}")
        return None, None

async def create_educator_topic(edu_details, username):
    """ONLY CREATE TOPIC WHEN /optry or /add CALLED"""
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
    return thread_id

async def add_new_educator(edu_username, edu_uid, edu_first, edu_last, edu_avatar):
    """Add NEW educator ONLY when found in courses/batches"""
    existing_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}
    if edu_uid in existing_uids:
        return False
    
    edu_details = await fetch_educator_by_username(edu_username)
    if not edu_details:
        return False
    
    thread_id = await create_educator_topic(edu_details, edu_username)
    print(f"✅ NEW EDUCATOR ADDED: {edu_first} {edu_last}")
    return True

async def process_single_educator(educator, update, show_progress=True):
    """1-BY-1 PROCESSING - Complete educator"""
    username = educator["username"]
    teacher_name = f"{educator['first_name']} {educator['last_name']}"
    
    if show_progress:
        await update.message.reply_text(f"🔥 Processing: {teacher_name}")
    
    # Check if exists
    doc = educators_col.find_one({"username": normalize_username(username)})
    thread_id = None
    
    if not doc:
        edu_details = await fetch_educator_by_username(username)
        if not edu_details:
            return False
        thread_id = await create_educator_topic(edu_details, username)
    else:
        thread_id = doc["subtopic_msg_id"]
    
    # Fetch data
    courses = await fetch_courses(username)
    batches = await fetch_batches(username)
    current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
    current_courses, current_batches = filter_by_time(courses, batches, current_time, True)
    
    # Find NEW educators
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
    
    new_added = 0
    for edu in all_new_educators:
        if await add_new_educator(*edu):
            new_added += 1
    
    # Get existing UIDs
    existing_doc = educators_col.find_one({"username": normalize_username(username)})
    existing_course_uids = {c["uid"] for c in existing_doc.get("courses", [])}
    existing_batch_uids = {b["uid"] for b in existing_doc.get("batches", [])}
    
    new_courses = [c for c in current_courses if c["uid"] not in existing_course_uids]
    new_batches = [b for b in current_batches if b["uid"] not in existing_batch_uids]
    
    # Progress message
    if show_progress:
        await update.message.reply_text(
            f"📊 {teacher_name}\n"
            f"📚 Courses: {len(new_courses)}\n"
            f"📦 Batches: {len(new_batches)}\n"
            f"✨ New Teachers: {new_added}"
        )
    
    last_checked = current_time.strftime("%Y-%m-%d %H:%M:%S %Z")
    
    # Upload EDUCATOR JSON
    edu_data = {"first_name": educator["first_name"], "last_name": educator["last_name"], 
               "username": username, "last_checked_time": last_checked}
    filename = f"temp_educator_{username}.json"
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
            filename = f"temp_course_{course['uid']}.json"
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
        await asyncio.sleep(1)
    
    # Upload NEW batches
    for batch in new_batches:
        schedule_url = f"https://api.unacademy.com/api/v1/batch/{batch['uid']}/schedule/"
        results, caption = await fetch_unacademy_schedule(schedule_url, "batch", batch)
        if results:
            filename = f"temp_batch_{batch['uid']}.json"
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
        await asyncio.sleep(1)
    
    educators_col.update_one({"username": normalize_username(username)}, 
                           {"$set": {"last_checked_time": last_checked}})
    return True

# 🔥 FIXED SCHEDULE - NO AUTO TOPICS
async def schedule_checker():
    while schedule_running:
        try:
            current_time = datetime.now(pytz.timezone('Asia/Kolkata'))
            twelve_hours_ago = current_time - timedelta(hours=12)
            
            for doc in educators_col.find({
                "last_checked_time": {"$lt": twelve_hours_ago.strftime("%Y-%m-%d %H:%M:%S %Z")}
            }):
                username = doc["username"]
                thread_id = doc["subtopic_msg_id"]
                
                # ONLY UPDATE EXISTING - NO NEW TOPICS
                courses = await fetch_courses(username)
                batches = await fetch_batches(username)
                
                # Update existing schedules only
                existing_courses = [c for c in doc.get("courses", []) if not c.get("is_completed")]
                existing_batches = [b for b in doc.get("batches", []) if not b.get("is_completed")]
                
                for course in existing_courses:
                    if current_time > dateutil.parser.isoparse(course["ends_at"]): continue
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
                    if current_time > dateutil.parser.isoparse(batch["completed_at"]): continue
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
                
                educators_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"last_checked_time": current_time.strftime("%Y-%m-%d %H:%M:%S %Z")}}
                )
                await asyncio.sleep(2)
            
            await asyncio.sleep(43200)  # 12 hours
        except Exception as e:
            print(f"Schedule error: {e}")
            await asyncio.sleep(3600)

# 🔥 FIXED /OPTRY - NEXT 10 + 1-BY-1
async def optry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global current_optry_index
    
    # Get processed UIDs
    processed_uids = {doc['uid'] for doc in educators_col.find({}, {'uid': 1})}
    
    # Get NEXT 10 unprocessed (skip already processed)
    all_optry = list(collection_optry.find())
    unprocessed = [edu for edu in all_optry if edu['uid'] not in processed_uids]
    
    # Start from current_optry_index
    start_idx = current_optry_index
    next_10 = unprocessed[start_idx:start_idx + 10]
    
    if not next_10:
        await update.message.reply_text("✅ ALL EDUCATORS PROCESSED! 🎉")
        current_optry_index = 0
        return ConversationHandler.END
    
    # Show NEXT 10 list
    text = f"📋 **NEXT 10 EDUCATORS** (#{start_idx+1}-{start_idx+10}):\n\n"
    for i, edu in enumerate(next_10, 1):
        name = f"{edu['first_name']} {edu['last_name']} [{edu['username']}]"
        text += f"{i}. {name}\n"
    
    text += f"\n🔥 Reply range: `1-10` (or any range)\n💡 Example: `1-3` = First 3 only"
    await update.message.reply_text(text)
    
    context.user_data['next_10'] = next_10
    current_optry_index += 10  # Move to next set
    return ENTER_RANGE

async def enter_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        start, end = map(int, update.message.text.split('-'))
        next_10 = context.user_data['next_10']
        
        if start < 1 or end > len(next_10) or start > end:
            await update.message.reply_text(f"❌ Invalid! Use `1-{len(next_10)}`")
            return ENTER_RANGE
        
        selected = next_10[start-1:end]
        await update.message.reply_text(f"🚀 Starting **{len(selected)} educators** 1-BY-1...\n⏳ Wait...")
        
        success_count = 0
        for i, educator in enumerate(selected, start):
            await update.message.reply_text(f"[{i}/{len(selected)}] Processing...")
            if await process_single_educator(educator, update):
                success_count += 1
                await update.message.reply_text(f"✅ {i}. {educator['first_name']} {educator['last_name']} - DONE!")
            await asyncio.sleep(2)  # 1-BY-1 with delay
        
        await update.message.reply_text(
            f"🎉 **COMPLETE!**\n"
            f"✅ Success: {success_count}/{len(selected)}\n"
            f"📋 Next /optry = Next 10 ready!"
        )
        
    except ValueError:
        await update.message.reply_text("❌ Format: `1-5`")
        return ENTER_RANGE
    
    return ConversationHandler.END

async def add_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: `/add username`")
        return
    
    username = normalize_username(context.args[0])
    educator = collection_optry.find_one({"username": username})
    
    if not educator:
        await update.message.reply_text(f"❌ `{username}` not found in list!")
        return
    
    await update.message.reply_text(f"🔥 Adding `{username}`...")
    if await process_single_educator(educator, update):
        await update.message.reply_text(f"✅ `{username}` added successfully!")
    else:
        await update.message.reply_text(f"❌ Failed to add `{username}`!")

async def cancel(update: Update, context):
    await update.message.reply_text("❌ Cancelled!")
    return ConversationHandler.END

async def main():
    global bot
    bot_token = '7213717609:AAF7aPoR0Hfn7m5vDxsgyeNHfdlY08lv_Hg'
    application = Application.builder().token(bot_token).build()
    bot = application.bot

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("optry", optry_command)],
        states={
            ENTER_RANGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, enter_range)],
        },
        fallbacks=[CommandHandler("cancel", cancel)]
    )

    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("add", add_command))

    print("🚀 Bot starting... NO AUTO TOPICS!")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    # Schedule starts but NO auto topics
    asyncio.create_task(schedule_checker())
    
    try:
        await asyncio.Event().wait()
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
