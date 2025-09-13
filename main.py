import requests
import json
import base64
import os
import time
import asyncio
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, Timeout, ConnectTimeout, ConnectionError

# Bot token (replace with your bot token from BotFather)
BOT_TOKEN = "8236348331:AAHhTHOM30Y9ULkeBWuNnUa5cl8u6rL3x2s"

# Base URL and headers
base_url = "https://studystark.in/temporary-shifted/api_proxy.php"
jwt_url = "https://studystark.in/temporary-shifted/jwt_encoder.php"
play_url = "https://studystark.in/temporary-shifted/play/play.php"
headers = {"X-Requested-With": "SPA-Client"}
jwt_headers = {"Content-Type": "application/json"}
TIMEOUT = 600  # Increased timeout for external APIs

def create_session():
    """Create a session with enhanced retry logic."""
    session = requests.Session()
    retries = Retry(
        total=10,  # More retries
        backoff_factor=2,  # Exponential backoff: 2s, 4s, 8s, etc.
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def safe_request(session, method, url, headers=None, data=None, timeout=TIMEOUT, max_retries=5):
    """Wrapper for requests with retry on network errors."""
    for attempt in range(max_retries):
        try:
            if method == "GET":
                response = session.get(url, headers=headers, timeout=timeout)
            elif method == "POST":
                response = session.post(url, headers=headers, data=data, timeout=timeout)
            response.raise_for_status()
            return response
        except (Timeout, ConnectTimeout, ConnectionError, RequestException) as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + (0.1 * attempt)  # Exponential backoff with jitter
                print(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All retries failed for {url}: {e}")
                raise
    return None

def encode_base64(subject_name, topic_name):
    """Encode subject_name - topic_name in Base64 and remove == padding."""
    combined = f"{subject_name} - {topic_name}"
    return base64.b64encode(combined.encode()).decode().rstrip('=')

def fetch_batches_by_name(batch_name, session):
    """Fetch batches by name and return a list of unique batches."""
    url = f"{base_url}?action=batches&search={batch_name}"
    response = safe_request(session, "GET", url, headers=headers)
    if response is None:
        return []
    try:
        data = response.json()
        if data.get("success") and "data" in data:
            seen = set()
            batches = []
            for batch in data["data"]:
                name = batch.get("name", "")
                batch_id = batch.get("batch_id", "")
                exam = batch.get("exam", "")
                photo = batch.get("photo", "")
                combo = (name, batch_id, exam, photo)
                if combo not in seen:
                    seen.add(combo)
                    batches.append({"name": name, "batch_id": batch_id, "exam": exam, "photo": photo})
            return batches
        return []
    except ValueError:
        print("Error parsing JSON for batches")
        return []

def fetch_content(subject_id, topic_id, topic_name, content_type, batch_id, session, base64_string, retry=False):
    """Fetch content (videos, notes, DPP notes) for a topic with retries."""
    content_url = f"{base_url}?action=content&batch_id={batch_id}&subject_id={subject_id}&topic_id={topic_id}&content_type={content_type}"
    page = 1
    seen_content = set()
    results = []

    while True:
        resp = safe_request(session, "GET", f"{content_url}&page={page}", headers=headers)
        if resp is None:
            print(f"    ❌ Failed to fetch {content_type} for topic {topic_name} (page {page}) after retries")
            break
        try:
            content_data = resp.json()
        except ValueError:
            print(f"    ❌ Error parsing JSON for {content_type} in topic {topic_name}, page {page}")
            break

        if not content_data.get("success") or not content_data.get("data"):
            if not retry and page == 1:
                return fetch_content(subject_id, topic_id, topic_name, content_type, batch_id, session, base64_string, retry=True)
            break

        for content_item in content_data["data"]:
            if content_type == "videos":
                video = content_item.get("videoDetails", {})
                name = video.get("name", "")
                video_id = content_item.get("_id", "")
                image = video.get("image", "")
                combo = (name, video_id, image)

                if combo not in seen_content:
                    seen_content.add(combo)
                    payload = {"video_key": video_id, "name": name, "image": image}
                    jwt_resp = safe_request(session, "POST", jwt_url, headers=jwt_headers, data=json.dumps(payload))
                    if jwt_resp is None:
                        url = f"ERROR -> Failed to get JWT after retries"
                    else:
                        try:
                            jwt_data = jwt_resp.json()
                            jwt_token = jwt_data.get("jwt", "NO_JWT")
                            url = f"{play_url}?batch_id={batch_id}&video_data={jwt_token}&fetch_video=1&json_response=1"
                        except ValueError:
                            url = f"ERROR -> Invalid JWT response"
                    results.append(f"🌚480🌚{name}💀{base64_string}💀 : {url}")
                    results.append(f"🌚720🌚{name}💀{base64_string}💀 : {url}")
            else:
                name = content_item.get("title", "")
                url = content_item.get("download_url", "")
                combo = (name, url)
                if combo not in seen_content:
                    seen_content.add(combo)
                    results.append(f"🌚OP🌚{name}💀{base64_string}💀 : {url}")

        paginate = content_data.get("paginate", {})
        total = paginate.get("totalCount", 0)
        limit = paginate.get("limit", 20)
        if total == 0 and content_data.get("data"):
            total = len(content_data["data"])
        if total == 0 or page * limit >= total:
            break
        page += 1

    return results[::-1]

async def safe_send_message(update: Update, text: str, context: ContextTypes.DEFAULT_TYPE):
    """Safely send a message with retry."""
    for attempt in range(3):
        try:
            await update.message.reply_text(text)
            return
        except Exception as e:
            print(f"Failed to send message (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    print("All message send attempts failed.")

async def safe_edit_message(progress_message, text: str):
    """Safely edit a message with retry."""
    for attempt in range(3):
        try:
            await progress_message.edit_text(text)
            return
        except Exception as e:
            print(f"Failed to edit message (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    print("All message edit attempts failed.")

async def safe_reply_document(update: Update, document, filename: str, context: ContextTypes.DEFAULT_TYPE):
    """Safely reply with a document with retry."""
    for attempt in range(3):
        try:
            await update.message.reply_document(document=document, filename=filename)
            return True
        except Exception as e:
            print(f"Failed to upload {filename} (attempt {attempt + 1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    return False

async def batch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /batch {batch_id} command with non-stopping retries."""
    args = context.args
    if not args:
        await safe_send_message(update, "Please provide a batch_id. Usage: /batch <batch_id>", context)
        return

    batch_id = args[0]
    session = create_session()
    batch_name = "Unknown"
    subjects_fetched = 0
    topics_fetched = 0
    try:
        progress_message = await update.message.reply_text(
            f"Batchname: {batch_name}\nSubject: -\nTopic name: -\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}"
        )
    except Exception as e:
        print(f"Failed to send initial progress message: {e}")
        progress_message = None

    # Files to store content
    subjects_file = "subjects_topics.txt"
    content_file = "content.txt"
    try:
        with open(subjects_file, "w", encoding="utf-8") as sf, open(content_file, "w", encoding="utf-8") as cf:
            # Fetch subjects with retry
            subjects_url = f"{base_url}?action=batch_details&batch_id={batch_id}"
            response = safe_request(session, "GET", subjects_url, headers=headers)
            if response is None:
                await safe_send_message(update, f"Failed to fetch subjects after retries from {subjects_url}", context)
                return
            try:
                data = response.json()
            except ValueError:
                await safe_send_message(update, f"Error parsing JSON for subjects from {subjects_url}", context)
                return

            if not data.get("success") or "data" not in data:
                await safe_send_message(update, "No subjects found in response.", context)
                return

            batch_name = data["data"].get("name", "Unknown")
            subjects = data["data"].get("subjects", [])

            last_update = time.time()
            for subj in subjects:
                subject_name = subj.get("subject", "")
                subject_id = subj.get("_id", "")
                subjects_fetched += 1
                sf.write(f"{subject_name}\n")

                # Update progress with safe edit
                if time.time() - last_update >= 10 and progress_message:
                    try:
                        await safe_edit_message(progress_message, f"Batchname: {batch_name}\nSubject: {subject_name}\nTopic name: -\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}")
                        last_update = time.time()
                    except Exception:
                        pass  # Continue even if edit fails

                # Fetch topics with retry
                topics_url = f"{base_url}?action=topics&batch_id={batch_id}&subject_id={subject_id}"
                page = 1
                unique_topics = set()

                while True:
                    resp = safe_request(session, "GET", f"{topics_url}&page={page}", headers=headers)
                    if resp is None:
                        await safe_send_message(update, f"❌ Failed to fetch topics for {subject_name} (page {page}) after retries from {topics_url}", context)
                        break
                    try:
                        topic_data = resp.json()
                    except ValueError:
                        await safe_send_message(update, f"❌ Error parsing JSON for topics in {subject_name}, page {page}", context)
                        break

                    if not topic_data.get("success") or not topic_data.get("data"):
                        if page == 1:
                            # Retry page 1
                            resp = safe_request(session, "GET", f"{topics_url}&page=1", headers=headers)
                            if resp is None:
                                await safe_send_message(update, f"❌ Retry failed for topics in {subject_name} from {topics_url}", context)
                                break
                            try:
                                topic_data = resp.json()
                            except ValueError:
                                await safe_send_message(update, f"❌ Retry failed for topics in {subject_name} from {topics_url}", context)
                                break
                        if not topic_data.get("success") or not topic_data.get("data"):
                            break

                    for item in topic_data["data"]:
                        topic_id = item.get("_id", "Unknown")
                        topic_name = item.get("name", "")
                        key = (topic_name, topic_id)
                        if key not in unique_topics:
                            unique_topics.add(key)
                            topics_fetched += 1
                            base64_string = encode_base64(subject_name, topic_name)
                            sf.write(f"{topic_name} >> {base64_string}\n")

                            # Update progress with safe edit
                            if time.time() - last_update >= 10 and progress_message:
                                try:
                                    await safe_edit_message(progress_message, f"Batchname: {batch_name}\nSubject: {subject_name}\nTopic name: {topic_name}\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}")
                                    last_update = time.time()
                                except Exception:
                                    pass  # Continue even if edit fails

                            # Fetch content with retry
                            content_types = [{"type": "videos"}, {"type": "notes"}, {"type": "DppNotes"}]
                            for content in content_types:
                                results = fetch_content(subject_id, topic_id, topic_name, content["type"], batch_id, session, base64_string)
                                for result in results:
                                    cf.write(f"{result}\n")

                    paginate = topic_data.get("paginate", {})
                    total = paginate.get("totalCount", 0)
                    limit = paginate.get("limit", 20)
                    if total == 0 and topic_data.get("data"):
                        total = len(topic_data["data"])
                    if total == 0 or page * limit >= total:
                        break
                    page += 1

            # Final progress update with safe edit
            if progress_message:
                try:
                    await safe_edit_message(progress_message, f"Batchname: {batch_name}\nSubject: Done\nTopic name: Done\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}")
                except Exception:
                    pass

        # Upload files with safe retry and fallback
        upload_success = False
        try:
            if os.path.exists(subjects_file):
                with open(subjects_file, "rb") as sf:
                    if await safe_reply_document(update, sf, "subjects_topics.txt", context):
                        upload_success = True
            if os.path.exists(content_file):
                with open(content_file, "rb") as cf:
                    if await safe_reply_document(update, cf, "content.txt", context):
                        upload_success = True
            if not upload_success:
                await safe_send_message(update, f"Fetching completed but upload failed due to network issues. Subjects: {subjects_fetched}, Topics: {topics_fetched}. Check logs for details.", context)
        except Exception as e:
            await safe_send_message(update, f"Error during upload: {e}. Fetching completed with {subjects_fetched} subjects and {topics_fetched} topics.", context)
    finally:
        # Always delete files
        for file_path in [subjects_file, content_file]:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")

async def name_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /name {batch_name} command."""
    args = context.args
    if not args:
        await safe_send_message(update, "Please provide a batch name. Usage: /name <batch_name>", context)
        return

    batch_name = " ".join(args)
    session = create_session()
    batches = fetch_batches_by_name(batch_name, session)
    if not batches:
        await safe_send_message(update, "No batches found for the given name.", context)
        return

    # Split batches into messages (max 4096 chars per message)
    message = "Available batches:\n"
    messages = []
    for i, batch in enumerate(batches):
        line = f"{i}: {batch['name']} : {batch['batch_id']} : {batch['exam']} : {batch['photo']}\n"
        if len(message) + len(line) > 4000:  # Telegram message limit ~4096
            messages.append(message)
            message = "Available batches (continued):\n"
        message += line
    if message:
        messages.append(message)

    # Send batch list with safe send
    for msg in messages:
        await safe_send_message(update, msg, context)

    # Prompt for index and store context
    context.user_data["batch_name"] = batch_name
    context.user_data["batches"] = batches
    await safe_send_message(update, "Which batch do you want to fetch? Reply with the index number.", context)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle index selection for /name command with non-stopping retries."""
    if "batches" not in context.user_data:
        await safe_send_message(update, "Please use /name <batch_name> first.", context)
        return

    try:
        index = int(update.message.text.strip())
        batches = context.user_data["batches"]
        batch_name = context.user_data["batch_name"]
        if 0 <= index < len(batches):
            batch_id = batches[index]["batch_id"]
            session = create_session()
            subjects_fetched = 0
            topics_fetched = 0
            try:
                progress_message = await update.message.reply_text(
                    f"Batchname: {batch_name}\nSubject: -\nTopic name: -\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}"
                )
            except Exception as e:
                print(f"Failed to send initial progress message: {e}")
                progress_message = None

            # Files to store content
            subjects_file = "subjects_topics.txt"
            content_file = "content.txt"
            try:
                with open(subjects_file, "w", encoding="utf-8") as sf, open(content_file, "w", encoding="utf-8") as cf:
                    # Fetch subjects with retry
                    subjects_url = f"{base_url}?action=batch_details&batch_id={batch_id}"
                    response = safe_request(session, "GET", subjects_url, headers=headers)
                    if response is None:
                        await safe_send_message(update, f"Failed to fetch subjects after retries from {subjects_url}", context)
                        return
                    try:
                        data = response.json()
                    except ValueError:
                        await safe_send_message(update, f"Error parsing JSON for subjects from {subjects_url}", context)
                        return

                    if not data.get("success") or "data" not in data:
                        await safe_send_message(update, "No subjects found in response.", context)
                        return

                    subjects = data["data"].get("subjects", [])
                    last_update = time.time()
                    for subj in subjects:
                        subject_name = subj.get("subject", "")
                        subject_id = subj.get("_id", "")
                        subjects_fetched += 1
                        sf.write(f"{subject_name}\n")

                        if time.time() - last_update >= 10 and progress_message:
                            try:
                                await safe_edit_message(progress_message, f"Batchname: {batch_name}\nSubject: {subject_name}\nTopic name: -\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}")
                                last_update = time.time()
                            except Exception:
                                pass  # Continue

                        # Fetch topics with retry (similar to batch_command)
                        topics_url = f"{base_url}?action=topics&batch_id={batch_id}&subject_id={subject_id}"
                        page = 1
                        unique_topics = set()

                        while True:
                            resp = safe_request(session, "GET", f"{topics_url}&page={page}", headers=headers)
                            if resp is None:
                                await safe_send_message(update, f"❌ Failed to fetch topics for {subject_name} (page {page}) after retries from {topics_url}", context)
                                break
                            try:
                                topic_data = resp.json()
                            except ValueError:
                                await safe_send_message(update, f"❌ Error parsing JSON for topics in {subject_name}, page {page}", context)
                                break

                            if not topic_data.get("success") or not topic_data.get("data"):
                                if page == 1:
                                    resp = safe_request(session, "GET", f"{topics_url}&page=1", headers=headers)
                                    if resp is None:
                                        await safe_send_message(update, f"❌ Retry failed for topics in {subject_name} from {topics_url}", context)
                                        break
                                    try:
                                        topic_data = resp.json()
                                    except ValueError:
                                        await safe_send_message(update, f"❌ Retry failed for topics in {subject_name} from {topics_url}", context)
                                        break
                                if not topic_data.get("success") or not topic_data.get("data"):
                                    break

                            for item in topic_data["data"]:
                                topic_id = item.get("_id", "Unknown")
                                topic_name = item.get("name", "")
                                key = (topic_name, topic_id)
                                if key not in unique_topics:
                                    unique_topics.add(key)
                                    topics_fetched += 1
                                    base64_string = encode_base64(subject_name, topic_name)
                                    sf.write(f"{topic_name} >> {base64_string}\n")

                                    if time.time() - last_update >= 10 and progress_message:
                                        try:
                                            await safe_edit_message(progress_message, f"Batchname: {batch_name}\nSubject: {subject_name}\nTopic name: {topic_name}\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}")
                                            last_update = time.time()
                                        except Exception:
                                            pass  # Continue

                                    # Fetch content
                                    content_types = [{"type": "videos"}, {"type": "notes"}, {"type": "DppNotes"}]
                                    for content in content_types:
                                        results = fetch_content(subject_id, topic_id, topic_name, content["type"], batch_id, session, base64_string)
                                        for result in results:
                                            cf.write(f"{result}\n")

                            paginate = topic_data.get("paginate", {})
                            total = paginate.get("totalCount", 0)
                            limit = paginate.get("limit", 20)
                            if total == 0 and topic_data.get("data"):
                                total = len(topic_data["data"])
                            if total == 0 or page * limit >= total:
                                break
                            page += 1

                    # Final progress update
                    if progress_message:
                        try:
                            await safe_edit_message(progress_message, f"Batchname: {batch_name}\nSubject: Done\nTopic name: Done\nSubjects fetched: {subjects_fetched}\nTopics fetched: {topics_fetched}")
                        except Exception:
                            pass

                # Upload files with safe retry and fallback
                upload_success = False
                try:
                    if os.path.exists(subjects_file):
                        with open(subjects_file, "rb") as sf:
                            if await safe_reply_document(update, sf, "subjects_topics.txt", context):
                                upload_success = True
                    if os.path.exists(content_file):
                        with open(content_file, "rb") as cf:
                            if await safe_reply_document(update, cf, "content.txt", context):
                                upload_success = True
                    if not upload_success:
                        await safe_send_message(update, f"Fetching completed but upload failed due to network issues. Subjects: {subjects_fetched}, Topics: {topics_fetched}. Check logs for details.", context)
                except Exception as e:
                    await safe_send_message(update, f"Error during upload: {e}. Fetching completed with {subjects_fetched} subjects and {topics_fetched} topics.", context)
            finally:
                # Always delete files
                for file_path in [subjects_file, content_file]:
                    if os.path.exists(file_path):
                        try:
                            os.remove(file_path)
                        except Exception as e:
                            print(f"Error deleting file {file_path}: {e}")
        else:
            await safe_send_message(update, "Invalid index.", context)
    except ValueError:
        await safe_send_message(update, "Please enter a valid number.", context)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors during bot operation without crashing."""
    try:
        exc = context.error
        print(f"Unhandled error: {exc}")
        # Avoid sending if it's a network error to prevent recursion
        if not isinstance(exc, (Timeout, ConnectTimeout, ConnectionError)):
            await safe_send_message(update, f"An error occurred: {str(exc)}", context)
    except Exception as e:
        print(f"Error in error_handler: {e}")

async def main():
    """Run the Telegram bot with enhanced timeouts and shutdown handling."""
    try:
        # Create the Application with higher timeouts for stability
        application = (
            Application.builder()
            .token(BOT_TOKEN)
            .connect_timeout(60.0)
            .read_timeout(50.0)
            .write_timeout(60.0)  # Higher for file uploads
            .pool_timeout(15.0)
            .get_updates_connect_timeout(30.0)
            .get_updates_read_timeout(50.0)
            .get_updates_write_timeout(60.0)
            .get_updates_pool_timeout(15.0)
            .build()
        )

        # Add handlers
        application.add_handler(CommandHandler("batch", batch_command))
        application.add_handler(CommandHandler("name", name_command))
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
        application.add_error_handler(error_handler)

        # Start polling
        print("Bot is running...")
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

        # Keep the bot running until interrupted
        try:
            while True:
                await asyncio.sleep(3600)  # Sleep for an hour to keep the loop alive
        except asyncio.CancelledError:
            print("Shutting down bot...")
            await application.updater.stop()
            await application.stop()
            await application.shutdown()

    except Exception as e:
        print(f"Error starting bot: {e}")
        raise

if __name__ == "__main__":
    # Check if an event loop is already running
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    try:
        # Run the main coroutine
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Received interrupt, shutting down...")
        # Cancel all tasks
        tasks = [task for task in asyncio.all_tasks(loop) if task is not asyncio.current_task(loop)]
        for task in tasks:
            task.cancel()
        # Run shutdown
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(asyncio.sleep(0))  # Allow pending tasks to complete
    finally:
        if not loop.is_closed():
            loop.close()
            print("Event loop closed.")
