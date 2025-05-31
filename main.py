import aiohttp
import asyncio
import re
import urllib.parse
from bs4 import BeautifulSoup
import os
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import logging

# Custom logging filter to suppress specific Telegram errors
class TelegramConflictFilter(logging.Filter):
    def filter(self, record):
        # Suppress specific Telegram conflict errors
        if "Conflict: terminated by other getUpdates request" in record.getMessage():
            return False
        if "Exception happened while polling for updates" in record.getMessage():
            return False
        return True

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
# Add filter to suppress Telegram conflict errors
logger.addFilter(TelegramConflictFilter())

# ---------- Common headers and cookies ----------
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

cookies = {
    "_clck": "1hjjwnc|2|fw3|0|1967",
    "verified_task": "dHJ1ZQ==",
    "countdown_end_time": "MTc0ODU3MTA5MDE3MA==",
    "auth_token": "cu7oiBffDQbRGx7%2FOhKylmKZYPBubC4Euenu4PkHPj%2FOyu1vuQDaiYALB5VP7gczbvg4LkbNd6fVxDs1vGEfb8%2FRRHDeRVH5tybmjixGaK77sBa%2BAaSO8fuw5rjRKfcdUEsqDMywyIeD3MnfMmb5nWulugGbcWwpZJox2iZQ60fhnyF0s4086P0JrK96Hd43nBiZ7Z%2Fg2U0PLPMaBbxbTuguKpd%2FtnCfIjJjxGQcSfY36WYOEB00TDMWop4BGlAtM7S21KUXXhoMuUtHC3%2BqW5R5dyGtIPcnGUsN3VZiPBXtrHtJsc6Uo3fsqlx8XNWAv9kFUTnK68Q6bDnpHekJvHLTtf2E%2FllrKNlwUNuhncWmV7vPeX0oXs0A5kOWfd5zDbc91vq2jMhjukb6e5ga1eRDGDhYikqXU00FaMjV%2BbthHPK9Pphsp%2BoZnCdl9wW8K3TgiA58QjSWQXX%2FwMTOJh1zNAB5NBbov1KpF%2FWHh7ZGnrLLwVbQMVMMZ%2FY8i1%2BAhQ%2Bb9OMi6swU2MVTXi5dT9pmAbHsl6keCd%2BOmXJ5KD3iL8xHRBvJcvFKyEBehdLIZ%2BPKXW6pSb1WeX9ZRxpuiWcRLcFq8mT%2F0v9wLYDdEHcy0zpFeZUFIsqkmXhMfbof11MuL4BgUHjlh35MFETm1b2HJ7sRpmkLoy4p5RPUmtA%3D"
}

# ---------- Retry Helper Function ----------
async def retry_request(session, method, url, max_retries=10, retry_delay=2, **kwargs):
    """Helper function to perform HTTP requests with retries."""
    for attempt in range(max_retries):
        try:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                return await response.json() if response.content_type == 'application/json' else await response.text()
        except aiohttp.ClientResponseError as e:
            logger.error(f"HTTP error on attempt {attempt + 1} for {url}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
        except aiohttp.ClientError as e:
            logger.error(f"Client error on attempt {attempt + 1} for {url}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1} for {url}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
    return None

# ---------- URL Replacement Logic with Explicit Retry for new_url ----------
async def replace_url(url, raw_text2="720"):
    """Replace URLs containing 'bhosdichod' with a new URL from the API, with explicit retries for new_url fetching."""
    if "bhosdichod" in url:
        # Extract base path and query parameters
        base_path = url.split('?')[0].replace('master.mpd', '')
        query_params = url.split('?')[1] if '?' in url else ''
        new_url = f"{base_path}hls/{raw_text2}/main.m3u8" + (f"?{query_params}" if query_params else '')

        # Prepare API request
        api_url = "https://live-api-yztz.onrender.com/api/create_stream"
        payload = {"m3u8_url": new_url}
        headers_api = {"Content-Type": "application/json"}

        max_retries = 3
        retry_delay = 2
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    logger.info(f"Attempting to fetch new URL for {new_url} (Attempt {attempt + 1}/{max_retries})")
                    async with session.post(api_url, json=payload, headers=headers_api) as response:
                        response.raise_for_status()
                        response_data = await response.json()
                        if all(key in response_data for key in ['manifest_url', 'stream_id', 'expires_at', 'token']):
                            logger.info(f"Successfully fetched new URL: {response_data['manifest_url']}")
                            return f"https://live-api-yztz.onrender.com{response_data['manifest_url']}"
                        else:
                            logger.error(f"Invalid response data for {new_url}: {response_data}")
            except aiohttp.ClientResponseError as e:
                logger.error(f"HTTP error on attempt {attempt + 1} for {new_url}: {e}")
            except aiohttp.ClientError as e:
                logger.error(f"Client error on attempt {attempt + 1} for {new_url}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error on attempt {attempt + 1} for {new_url}: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying {new_url} after {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
        logger.error(f"Failed to fetch new URL for {new_url} after {max_retries} attempts")
        return url
    return url

# ---------- Fetch Subjects ----------
async def fetch_subjects(batch_id, token):
    url = (
        f"https://streamfiles.eu.org/api/batch_details.php?"
        f"batch_id={batch_id}&token={token}&type=details"
    )
    async with aiohttp.ClientSession() as session:
        logger.info(f"Fetching subjects for batch_id {batch_id}")
        response = await retry_request(
            session,
            'GET',
            url,
            headers=headers,
            cookies=cookies,
            max_retries=10,
            retry_delay=2
        )
        if response and response.get('success') and 'data' in response and 'subjects' in response['data']:
            subjects = response['data']['subjects']
            logger.info(f"Found {len(subjects)} subjects for batch_id {batch_id}")
            return subjects
        logger.error(f"No subjects found for batch_id {batch_id}. Response: {response}")
        return []

# ---------- Fetch Topics ----------
async def get_topics(subject, batch_id, token):
    topics = []
    page = 1
    async with aiohttp.ClientSession() as session:
        while True:
            url = (
                f"https://streamfiles.eu.org/api/batch_details.php?"
                f"batch_id={batch_id}&subject_id={subject['_id']}&token={token}&type=topics&page={page}"
            )
            logger.info(f"Fetching topics for subject {subject['_id']} ({subject['slug']}) on page {page}")
            response = await retry_request(
                session,
                'GET',
                url,
                headers=headers,
                cookies=cookies,
                max_retries=10,
                retry_delay=2
            )
            if response and response.get("success") and isinstance(response.get("data"), list):
                if not response["data"]:
                    logger.info(f"No more topics found for subject {subject['_id']} ({subject['slug']}) on page {page}")
                    return topics
                topics.extend(response["data"])
                logger.info(f"Fetched {len(response['data'])} topics for subject {subject['_id']} ({subject['slug']}) on page {page}")
                page += 1
            else:
                logger.error(f"Invalid topics response for subject {subject['_id']} ({subject['slug']}) on page {page}")
                return topics
    return topics

# ---------- Fetch Section (Videos/Notes/DPPs) ----------
async def get_section(slug, typeId, _id, section_type, subject, batch_id, token):
    url = (
        f"https://streamfiles.eu.org/api/contents.php"
        f"?topic_slug={slug}"
        f"&type={section_type}"
        f"&api_type=new"
        f"&token={token}"
        f"&subject_id={typeId}"
        f"&topic_id={_id}"
        f"&batch_id={batch_id}"
        f"&subject_slug={subject['slug']}"
        f"&content_type=new"
        f"&encrypt=0"
    )
    async with aiohttp.ClientSession() as session:
        logger.info(f"Fetching {section_type} for topic {slug} in subject {subject['slug']}")
        response = await retry_request(
            session,
            'GET',
            url,
            headers=headers,
            cookies=cookies,
            max_retries=10,
            retry_delay=2
        )
        if isinstance(response, list):
            logger.info(f"Found {len(response)} {section_type} for topic {slug} in subject {subject['slug']}")
            return response
        logger.error(f"Invalid {section_type} response for topic {slug} in subject {subject['slug']}: {response}")
        return []

# ---------- Extract Video URL ----------
async def get_video_url(video, batch_id):
    video_url = video.get('video_url', '')
    video_title = urllib.parse.quote(video.get('video_title', 'Unknown Title'))
    video_poster = video.get('video_poster', '')
    video_id = video.get('video_id', '')
    subject_id = video.get('subject_id', '')
    play_url = (
        f"https://streamfiles.eu.org/play.php"
        f"?video_url={video_url}"
        f"&title={video_title}"
        f"&poster={video_poster}"
        f"&video_type=pw"
        f"&video_id={video_id}"
        f"&subject_id={subject_id}"
        f"&batch_id={batch_id}"
    )

    async with aiohttp.ClientSession() as session:
        logger.info(f"Attempting to extract video URL for {video_title} at {play_url}")
        response = await retry_request(
            session,
            'GET',
            play_url,
            headers=headers,
            cookies=cookies,
            max_retries=10,
            retry_delay=2
        )
        if response:
            soup = BeautifulSoup(response, 'html.parser')
            input_group = soup.find('div', class_='input-group')
            if input_group:
                extracted = input_group.find('input', {'id': 'video_url'})
                if extracted and extracted['value']:
                    logger.info(f"Successfully extracted video URL for {video_title}: {extracted['value']}")
                    return extracted['value']
                logger.error(f"No video URL found in input tag for {video_title}")
            else:
                logger.error(f"No input-group div found in play page for {video_title}")
        logger.error(f"Failed to extract video URL for {video_title} after retries")
        return None

# ---------- Collect Topic Contents ----------
async def collect_topic_contents(topic, subject, batch_id, token):
    result = []
    name = topic.get("name", "No Name")
    slug = topic.get("slug", "")
    typeId = topic.get("typeId", "")
    _id = topic.get("_id", "")

    logger.info(f"Processing topic {name} (slug: {slug}, id: {_id}) in subject {subject['slug']} (id: {subject['_id']})")

    # Videos
    videos = await get_section(slug, typeId, _id, "videos", subject, batch_id, token)
    if videos:
        logger.info(f"Found {len(videos)} videos for topic {name}")
        for video in reversed(videos):
            video_title = video.get('video_title', 'Unknown Title')
            real_url = await get_video_url(video, batch_id)
            if real_url:
                new_url = await replace_url(real_url)
                result.append(f"{video_title}: {new_url}")
                logger.info(f"Processed video {video_title}: {new_url}")
            else:
                logger.warning(f"No valid URL extracted for video {video_title} in topic {name}")
    else:
        logger.info(f"No videos found for topic {name}")

    # Notes
    notes = await get_section(slug, typeId, _id, "notes", subject, batch_id, token)
    if notes:
        logger.info(f"Found {len(notes)} notes for topic {name}")
        for note in reversed(notes):
            title = note.get('title', 'Unknown Title')
            download_url = note.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                logger.info(f"Added note {title}: {download_url}")
            else:
                logger.warning(f"No download URL for note {title} in topic {name}")
    else:
        logger.info(f"No notes found for topic {name}")

    # DPPs
    dpps = await get_section(slug, typeId, _id, "DppNotes", subject, batch_id, token)
    if dpps:
        logger.info(f"Found {len(dpps)} DPPs for topic {name}")
        for dpp in reversed(dpps):
            title = dpp.get('title', 'Unknown Title')
            download_url = dpp.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                logger.info(f"Added DPP {title}: {download_url}")
            else:
                logger.warning(f"No download URL for DPP {title} in topic {name}")
    else:
        logger.info(f"No DPPs found for topic {name}")

    if not result:
        logger.info(f"No content (videos, notes, DPPs) collected for topic {name} in subject {subject['slug']}")
    else:
        logger.info(f"Collected {len(result)} content items for topic {name} in subject {subject['slug']}")

    return "\n".join(result) if result else ""

# ---------- Progress Bar ----------
def create_progress_bar(progress, total, width=20):
    if total == 0:
        return "[No items to process]"
    filled = int(width * progress // total)
    bar = '█' * filled + '-' * (width - filled)
    percent = (progress / total) * 100
    return f"[{bar}] {percent:.1f}%"

# ---------- Telegram Handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("Received /start command")
    await update.message.reply_text(
        "Welcome! Use /batch_id <batch_id> [-n <filename>.txt] to start the process."
    )

async def batch_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"Received /batch_id command: {update.message.text}")
    args = context.args
    batch_id = None
    filename = "subject_content.txt"

    if len(args) < 1:
        await update.message.reply_text("Usage: /batch_id <batch_id> [-n <filename>.txt]")
        logger.warning("Invalid /batch_id command: missing batch_id")
        return

    batch_id = args[0]
    if len(args) > 2 and args[1] == "-n" and args[2].endswith(".txt"):
        filename = args[2]
        logger.info(f"Custom filename specified: {filename}")

    token = cookies["auth_token"]
    subjects = await fetch_subjects(batch_id, token)
    if not subjects:
        await update.message.reply_text("No subjects found or request failed.")
        logger.error(f"No subjects found or request failed for batch_id {batch_id}")
        return

    context.user_data['batch_id'] = batch_id
    context.user_data['filename'] = filename
    context.user_data['subjects'] = subjects
    context.user_data['state'] = 'awaiting_subject'
    logger.info(f"Stored batch_id: {batch_id}, filename: {filename}, subjects: {len(subjects)}")

    subject_list = [f"{i} {subj['_id']} - {subj['slug']}" for i, subj in enumerate(subjects, 1)]
    subject_message = "Available subjects:\n" + "\n".join(subject_list) + "\n\nPlease reply with the index or subjectId you want to fetch."
    await update.message.reply_text(subject_message)
    logger.info("Sent subject list to user")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()
    state = context.user_data.get('state', '')

    logger.info(f"Received message: {user_input}, Current state: {state}")

    if state == 'awaiting_subject':
        await handle_subject_selection(update, context)
    elif state == 'awaiting_topics':
        await handle_topic_selection(update, context)
    else:
        await update.message.reply_text("Please use /batch_id to start the process.")
        logger.warning(f"Unexpected message received: {user_input}, No active state")

async def handle_subject_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()
    subjects = context.user_data.get('subjects', [])
    batch_id = context.user_data.get('batch_id')
    filename = context.user_data.get('filename', 'subject_content.txt')
    token = cookies["auth_token"]

    logger.info(f"Processing subject selection: {user_input}")

    selected_subject = None
    try:
        index = int(user_input) - 1
        if 0 <= index < len(subjects):
            selected_subject = subjects[index]
            logger.info(f"Selected subject by index: {selected_subject['_id']} ({selected_subject['slug']})")
    except ValueError:
        for subject in subjects:
            if subject['_id'] == user_input:
                selected_subject = subject
                logger.info(f"Selected subject by ID: {selected_subject['_id']} ({selected_subject['slug']})")
                break

    if not selected_subject:
        await update.message.reply_text("Invalid input. Please provide a valid index or subjectId.")
        logger.error(f"Invalid subject input: {user_input}")
        return

    context.user_data['state'] = ''
    logger.info("Cleared awaiting_subject state")

    if selected_subject['slug'].startswith('notices'):
        await update.message.reply_text(
            f"Warning: Subject {selected_subject['slug']} appears to be a 'notices' category, "
            "which may not contain videos, notes, or DPPs."
        )
        logger.warning(f"Selected subject {selected_subject['slug']} may be a notices category")

    topics = await get_topics(selected_subject, batch_id, token)
    if not topics:
        await update.message.reply_text(
            f"No topics found for subject {selected_subject['slug']} ({selected_subject['_id']}). "
            "This subject may not contain any topics or may be a special category like 'notices'."
        )
        logger.error(f"No topics found for subject {selected_subject['slug']} ({selected_subject['_id']})")
        return

    topics_filename = f"topics_{selected_subject['slug']}.txt"
    with open(topics_filename, "w", encoding="utf-8") as f:
        for i, topic in enumerate(topics, 1):
            f.write(f"{i} {topic['name']}\n")
    logger.info(f"Saved topics to {topics_filename}")

    try:
        with open(topics_filename, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=topics_filename,
                caption="Here is the list of topics. Please reply with the indices of the topics you want to fetch (e.g., 1,2,3)."
            )
        logger.info(f"Sent topics file {topics_filename} to user")
    except Exception as e:
        await update.message.reply_text(f"Error sending topics file: {str(e)}")
        logger.error(f"Error sending topics file {topics_filename}: {e}")
        return

    context.user_data['topics'] = topics
    context.user_data['selected_subject'] = selected_subject
    context.user_data['topics_filename'] = topics_filename
    context.user_data['state'] = 'awaiting_topics'
    logger.info("Set state to awaiting_topics")

async def handle_topic_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = update.message.text.strip()
    topics = context.user_data.get('topics', [])
    selected_subject = context.user_data.get('selected_subject')
    batch_id = context.user_data.get('batch_id')
    filename = context.user_data.get('filename', 'subject_content.txt')
    token = cookies["auth_token"]

    logger.info(f"Processing topic selection: {user_input}")

    try:
        topic_indices = [int(i) - 1 for i in user_input.split(',')]
        selected_topics = [topics[i] for i in topic_indices if 0 <= i < len(topics)]
        if not selected_topics:
            await update.message.reply_text("No valid topic indices provided. Please use indices like 1,2,3.")
            logger.error("No valid topic indices provided")
            return
        logger.info(f"Selected topics: {[topic['name'] for topic in selected_topics]}")
    except (ValueError, IndexError) as e:
        await update.message.reply_text("Invalid input. Please provide topic indices like 1,2,3.")
        logger.error(f"Invalid topic indices input: {user_input}, Error: {e}")
        return

    context.user_data['state'] = ''
    logger.info("Cleared awaiting_topics state")

    topics_filename = context.user_data.get('topics_filename')
    if topics_filename and os.path.exists(topics_filename):
        try:
            os.remove(topics_filename)
            logger.info(f"Deleted topics file {topics_filename}")
        except Exception as e:
            logger.error(f"Error deleting topics file {topics_filename}: {e}")

    progress_message = await update.message.reply_text("Processing... [                    ] 0.0%")
    logger.info("Started processing selected topics")

    try:
        total_topics = len(selected_topics)
        topic_count = 0
        content_written = False

        with open(filename, "w", encoding="utf-8") as f:
            for topic in selected_topics:
                topic_count += 1
                logger.info(f"Processing topic {topic_count}/{total_topics}: {topic.get('name', 'No Name')}")
                topic_content = await collect_topic_contents(topic, selected_subject, batch_id, token)
                if topic_content:
                    f.write(f"{topic_content}\n")
                    content_written = True
                    f.flush()
                    logger.info(f"Written content for topic {topic.get('name', 'No Name')}")
                else:
                    logger.warning(f"No content written for topic {topic.get('name', 'No Name')} in subject {selected_subject['slug']}")

                await progress_message.edit_text(
                    f"Processing topic {topic_count}/{total_topics} for subject {selected_subject['slug']}...\n"
                    f"{create_progress_bar(topic_count, total_topics)}"
                )
                logger.info(f"Progress: {topic_count}/{total_topics} topics processed")

        if not content_written:
            await progress_message.edit_text(
                f"No content (videos, notes, or DPPs) found for selected topics in subject {selected_subject['slug']} ({selected_subject['_id']})."
            )
            logger.error(f"No content found for selected topics in subject {selected_subject['slug']}")
            if os.path.exists(filename):
                os.remove(filename)
                logger.info(f"Deleted empty file {filename}")
            return

        if os.path.exists(filename) and os.path.getsize(filename) > 0:
            await progress_message.edit_text("Processing complete! Uploading file...")
            logger.info("Processing complete, uploading file")

            try:
                with open(filename, "rb") as f:
                    await update.message.reply_document(document=f, filename=filename)
                logger.info(f"Sent file {filename} to user")
            except Exception as e:
                await progress_message.edit_text(f"Error sending file: {str(e)}")
                logger.error(f"Error sending file {filename}: {e}")
                return

            try:
                os.remove(filename)
                await update.message.reply_text(f"File {filename} sent and deleted from storage.")
                logger.info(f"Deleted file {filename}")
            except Exception as e:
                logger.error(f"Error deleting file {filename}: {e}")
        else:
            await progress_message.edit_text(
                f"Error: No content was written to the file for subject {selected_subject['slug']} ({selected_subject['_id']}). "
                "Please check if the selected topics contain videos, notes, or DPPs."
            )
            logger.error(f"No content written to file for subject {selected_subject['slug']}")
            if os.path.exists(filename):
                os.remove(filename)
                logger.info(f"Deleted empty file {filename}")

    except Exception as e:
        logger.error(f"Exception in handle_topic_selection for subject {selected_subject['slug']}: {e}")
        await progress_message.edit_text(f"Error processing request for subject {selected_subject['slug']}: {str(e)}")
        if os.path.exists(filename):
            os.remove(filename)
            logger.info(f"Deleted file {filename} due to error")

async def main():
    # Replace with your actual bot token
    bot_token = "8110893329:AAHqW1PuisNxVAOfYTVG61No20uam0prgl0"
    application = Application.builder().token(bot_token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("batch_id", batch_id))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    logger.info("Bot is starting...")
    try:
        await application.initialize()
        await application.start()
        await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"Error running bot: {e}")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("Bot has shut down")

if __name__ == "__main__":
    # Ensure only one instance runs by checking for existing process
    import psutil
    current_pid = os.getpid()
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.pid != current_pid and 'python' in proc.name().lower() and proc.cmdline() == psutil.Process(current_pid).cmdline():
            logger.error("Another instance of this bot is already running. Exiting.")
            exit(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logger.info("Event loop closed")
