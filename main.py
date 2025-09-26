import requests
import json
import os
import time
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from threading import Thread
import asyncio

# Global variables to control fetching
fetching = False
last_json_data = {}
last_educator_count = 0
last_course_count = 0
last_batch_count = 0
progress_message = None
update_context = None  # Store context for async operations
update_obj = None     # Store update object for async operations
loop = None           # Store main event loop

def save_to_json(filename, data):
    """Save data to a JSON file."""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_educators(goal_uid="TMUVD", limit=50, max_offset=1000, json_data=None, filename="funkabhosda.json", known_educator_uids=None):
    """Fetch educators and save to JSON."""
    base_url = "https://unacademy.com/api/v1/uplus/subscription/goal_educators/"
    seen_usernames = set()
    educators = []  # Store (username, uid) for processing
    json_data["educators"] = json_data.get("educators", [])  # Initialize educators list in JSON
    offset = 0

    while offset <= max_offset:
        url = f"{base_url}?goal_uid={goal_uid}&limit={limit}&offset={offset}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check for E001 error
            if isinstance(data, dict) and data.get("error_code") == "E001":
                print("Error E001 encountered. Stopping educator fetch.")
                break

            # Check if results exist
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
                    # Add educator to JSON
                    json_data["educators"].append({
                        "first_name": educator.get("first_name", "N/A"),
                        "last_name": educator.get("last_name", "N/A"),
                        "username": username,
                        "uid": uid,
                        "avatar": educator.get("avatar", "N/A")
                    })

            # Save to JSON after each batch
            save_to_json(filename, json_data)
            offset += limit

        except requests.RequestException as e:
            print(f"Request failed for educators: {e}")
            break

    return educators

def fetch_courses(username, limit=50, max_offset=1000, json_data=None, filename="funkabhosda.json"):
    """Fetch courses for a given username and save to JSON."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/course?username={username}&limit={limit}&type=latest"
    seen_uids = set()
    json_data["courses"] = json_data.get("courses", {})  # Initialize courses dict
    json_data["courses"][username] = json_data["courses"].get(username, [])  # Courses for this username
    offset = 1

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check for E001 error
            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 encountered for courses of {username}. Stopping course fetch.")
                break

            # Check if results exist
            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid course results found for {username} at offset {offset}. Stopping course fetch.")
                break

            if not results:
                print(f"No more courses found for {username} at offset {offset}. Stopping course fetch.")
                break

            # Process courses
            for i, course in enumerate(results, start=offset):
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
                    # Add course to JSON
                    json_data["courses"][username].append({
                        "name": course.get("name", "N/A"),
                        "slug": course.get("slug", "N/A"),
                        "thumbnail": course.get("thumbnail", "N/A"),
                        "uid": course_uid,
                        "starts_at": course.get("starts_at", "N/A"),
                        "ends_at": course.get("ends_at", "N/A")
                    })

            # Save to JSON after each batch
            save_to_json(filename, json_data)
            offset += limit

        except requests.RequestException as e:
            print(f"Failed to fetch courses for {username}: {e}")
            break

def fetch_batches(username, known_educator_uids, limit=50, max_offset=1000, json_data=None, filename="funkabhosda.json"):
    """Fetch batches for a given username, save to JSON, and return new educators."""
    base_url = f"https://unacademy.com/api/sheldon/v1/list/batch?username={username}&limit={limit}"
    seen_batch_uids = set()
    new_educators = []
    json_data["batches"] = json_data.get("batches", {})  # Initialize batches dict
    json_data["batches"][username] = json_data["batches"].get(username, [])  # Batches for this username
    offset = 2

    while offset <= max_offset:
        url = f"{base_url}&offset={offset}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Check for E001 error
            if isinstance(data, dict) and data.get("error_code") == "E001":
                print(f"Error E001 encountered for batches of {username}. Stopping batch fetch.")
                break

            # Check if results exist
            results = data.get("results")
            if results is None or not isinstance(results, list):
                print(f"No valid batch results found for {username} at offset {offset}. Stopping batch fetch.")
                break

            if not results:
                print(f"No more batches found for {username} at offset {offset}. Stopping batch fetch.")
                break

            # Process batches
            for i, batch in enumerate(results, start=offset):
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
                    # Add batch to JSON
                    json_data["batches"][username].append({
                        "name": batch.get("name", "N/A"),
                        "cover_photo": batch.get("cover_photo", "N/A"),
                        "exam_type": batch.get("goal", {}).get("name", "N/A"),
                        "uid": batch_uid,
                        "slug": batch.get("slug", "N/A"),
                        "syllabus_tag": batch.get("syllabus_tag", "N/A"),
                        "starts_at": batch.get("starts_at", "N/A"),
                        "completed_at": batch.get("completed_at", "N/A")
                    })

                    # Check for new educators
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
                            # Add new educator to JSON
                            json_data["educators"].append({                                
                                "first_name": author.get("first_name", "N/A"),
                                "last_name": author.get("last_name", "N/A"),
                                "username": author.get("username", "N/A"),
                                "uid": author_uid,
                                "avatar": author.get("avatar", "N/A")
                            })

            # Save to JSON after each batch
            save_to_json(filename, json_data)
            offset += limit

        except requests.RequestException as e:
            print(f"Failed to fetch batches for {username}: {e}")
            break

    return new_educators

def count_items(json_data):
    """Count educators, courses, and batches in json_data."""
    educator_count = len(json_data.get("educators", []))
    course_count = sum(len(courses) for courses in json_data.get("courses", {}).values())
    batch_count = sum(len(batches) for batches in json_data.get("batches", {}).values())
    return educator_count, course_count, batch_count

async def send_progress_bar():
    """Send or update the progress bar message."""
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
    """Update progress bar every 60 seconds and upload JSON every 2 minutes."""
    global last_json_data, last_educator_count, last_course_count, last_batch_count
    last_upload_time = time.time()
    
    while fetching:
        try:
            with open("funkabhosda.json", "r", encoding="utf-8") as f:
                current_json_data = json.load(f)
            
            educator_count, course_count, batch_count = count_items(current_json_data)
            
            # Update progress bar only if counts have changed
            if (educator_count != last_educator_count or
                course_count != last_course_count or
                batch_count != last_batch_count):
                last_json_data = current_json_data
                last_educator_count, last_course_count, last_batch_count = educator_count, course_count, batch_count
                await send_progress_bar()
            
            # Upload JSON every 2 minutes
            current_time = time.time()
            if current_time - last_upload_time >= 120:
                await upload_json()
                last_upload_time = current_time
                
        except Exception as e:
            print(f"Error in progress updater: {e}")
        
        await asyncio.sleep(60)

def fetch_data_in_background():
    """Run the fetching process in a background thread."""
    global fetching, last_json_data, last_educator_count, last_course_count, last_batch_count, progress_message
    
    json_data = {
        "educators": [],
        "courses": {},
        "batches": {}
    }
    known_educator_uids = set()
    filename = "funkabhosda.json"

    # Fetch initial educators
    print("Fetching initial educators...")
    educators = fetch_educators(json_data=json_data, filename=filename, known_educator_uids=known_educator_uids)

    # Process educators in a queue
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

            # Fetch courses
            print(f"\nFetching courses for {username}...")
            fetch_courses(username, json_data=json_data, filename=filename)

            # Fetch batches and get new educators
            print(f"\nFetching batches for {username}...")
            new_educators = fetch_batches(username, known_educator_uids, json_data=json_data, filename=filename)

            # Print new educators found in batches
            if new_educators:
                print(f"\nNew educators found in batches for {username}:")
                for educator in new_educators:
                    print(f"{educator['index']} {educator['first_name']} {educator['last_name']} : {educator['username']} : {educator['uid']} : {educator['avatar']}")
                    # Add new educators to queue
                    educator_queue.append((educator["username"], educator["uid"]))
            else:
                print(f"\nNo new educators found in batches for {username}.")

    if fetching:
        print("\nAll educators processed. Data saved to funkabhosda.json.")
        # Send final JSON and progress bar
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(send_progress_bar(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Fetching completed! Final funkabhosda.json uploaded."), loop).result()
    else:
        print("\nFetching stopped by user.")
        asyncio.run_coroutine_threadsafe(upload_json(), loop).result()
        asyncio.run_coroutine_threadsafe(
            update_obj.message.reply_text("Fetching stopped. Partial funkabhosda.json uploaded."), loop).result()
    
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
    
    # Start progress updater
    asyncio.create_task(progress_updater())
    
    # Run fetching in background thread
    thread = Thread(target=fetch_data_in_background)
    thread.start()

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
    # Replace 'YOUR_BOT_TOKEN' with your actual bot token
    bot_token = '7862470692:AAHdj-W6sgO9qcut3HUlvLrj5SiSetqOvi0'
    application = Application.builder().token(bot_token).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("now", now_command))
    application.add_handler(CommandHandler("stop", stop_command))
    
    # Start polling without closing the loop
    print("Bot is starting...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    
    # Keep the application running
    try:
        await asyncio.Event().wait()  # Wait indefinitely
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
