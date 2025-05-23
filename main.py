import aiohttp
import urllib.parse
from bs4 import BeautifulSoup
import os
import asyncio
import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------- Common headers and cookies ----------
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

# Load cookies from environment variables for security
cookies = {
    "_clck": os.getenv("CLCK_COOKIE", "1hjjwnc|2|fw3|0|1967"),
    "verified_task": os.getenv("VERIFIED_TASK_COOKIE", "dHJ1ZQ=="),
    "countdown_end_time": os.getenv("COUNTDOWN_END_TIME_COOKIE", "MTc0Nzk4MTkwNTU3OA=="),
    "auth_token": os.getenv("AUTH_TOKEN_COOKIE", "cu7oiBffDQbRGx7%2FOhKylmKZYPBubC4Euenu4PkHPj%2FOyu1vuQDaiYALB5VP7gczlwp%2BlqKzYaCiMAuvv4nffM7dWQCTTTNJaNrjLCIxwleQ%2BIfrin5pJuz4juAjlioxrN8d2woRxX%2FUY5y39eYbhASTvLlTplTsH9ktR61S93UECYofiqCH9OO79fnBrc93ahIE3FfqB3hR%2FqMY677%2FVrkxVoP0G56YmxBlIXVnrK1vavK5TnZ%2B9vLBLJTV8lGBAqKL%2Fm4zsXDG0n7qfG0rG9WK2K9AhSIPAqxoH8h%2BpW621TsuKfmk5GXAB8lPSEFfxu4el5G1HQAraS69VGfeP3tC5PQyl%2FvmX5CtxD1Zzli55jLIYLFTXKUgCsHgAfd6iZ%2FhpECaeHeOken3%2FFUS3R14C5rpANjzAglAXSR1lLuqPgNYgQB9EcG8zXs8SBZYTSQom%2FM151PhS23FJ05lG5GGUvwfhYCxfKWqGYy%2B4KDUlxBygcv7VxINx08Br%2FscmCR5K7n%2BDYKc71vLM5LqrBxSyoqvt6rbZwACHh%2FSyRrKebaB7Ype%2FpOEUz%2BhfagTNX1wqAejiv9z%2Fm2BmYPYp04%2BiK0l0abkQYQ5%2FIGpLxpvizqjWxQylWKhvrLejWKMBjivgOpRf9x1Of8tpq8eqI4HTrCL82w2%2F9e7k8wsF4U%3D")
}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome! Use /batch_id <batch_id> [-n <filename>.txt] to generate and receive a file with video, DPP, and notes URLs."
    )

async def fetch_subjects(session, batch_id, token):
    url = (
        f"https://streamfiles.eu.org/api/batch_details.php?"
        f"batch_id={batch_id}&token={token}&type=details"
    )
    try:
        async with session.get(url, headers=headers, cookies=cookies) as response:
            response.raise_for_status()
            data = await response.json()
            if data.get('success') and 'data' in data and 'subjects' in data['data']:
                logger.info(f"Fetched {len(data['data']['subjects'])} subjects for batch_id {batch_id}")
                return data['data']['subjects']
            logger.warning(f"No subjects found for batch_id {batch_id}")
            return []
    except Exception as e:
        logger.error(f"Failed to fetch subjects for batch_id {batch_id}: {str(e)}")
        return []

async def get_topics(session, subject, batch_id, token):
    url = (
        f"https://streamfiles.eu.org/api/batch_details.php?"
        f"batch_id={batch_id}&subject_id={subject['_id']}&token={token}&type=topics&page=1"
    )
    try:
        async with session.get(url, headers=headers, cookies=cookies) as response:
            response.raise_for_status()
            data = await response.json()
            if data.get("success") and isinstance(data.get("data"), list):
                logger.info(f"Fetched {len(data['data'])} topics for subject {subject['subject']}")
                return data["data"]
            logger.warning(f"No topics found for subject {subject['subject']}")
            return []
    except Exception as e:
        logger.error(f"Failed to fetch topics for subject {subject['subject']}: {str(e)}")
        return []

async def get_section(session, slug, typeId, _id, section_type, subject, batch_id, token):
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
    try:
        async with session.get(url, headers=headers, cookies=cookies) as response:
            response.raise_for_status()
            data = await response.json()
            if isinstance(data, list):
                logger.info(f"Fetched {len(data)} {section_type} for topic {slug}")
                return data
            logger.warning(f"No {section_type} found for topic {slug}")
            return []
    except Exception as e:
        logger.error(f"Failed to fetch {section_type} for topic {slug}: {str(e)}")
        return []

async def get_video_url(session, video, batch_id):
    video_url = video.get('video_url', '')
    video_title = urllib.parse.quote(video.get('video_title', ''))
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
    try:
        async with session.get(play_url, headers=headers, cookies=cookies, timeout=10) as response:
            if response.status == 200:
                text = await response.text()
                soup = BeautifulSoup(text, 'html.parser')
                input_group = soup.find('div', class_='input-group')
                if input_group:
                    extracted = input_group.find('input', {'id': 'video_url'})
                    if extracted and extracted['value']:
                        logger.info(f"Extracted video URL for {video.get('video_title', 'Unknown Title')}")
                        return extracted['value']
                logger.warning(f"No video URL found for {video.get('video_title', 'Unknown Title')}")
                return None
    except Exception as e:
        logger.error(f"Failed to extract video URL for {video.get('video_title', 'Unknown Title')}: {str(e)}")
        return None

async def collect_topic_contents(session, topic, subject, batch_id, token):
    result = []
    name = topic.get("name", "No Name")
    slug = topic.get("slug", "")
    typeId = topic.get("typeId", "")
    _id = topic.get("_id", "")

    result.append("=" * 60)
    result.append(f"Topic: {name}")
    result.append(f"Slug: {slug}\n")

    # Fetch videos, notes, and DPPs concurrently
    tasks = [
        get_section(session, slug, typeId, _id, "videos", subject, batch_id, token),
        get_section(session, slug, typeId, _id, "notes", subject, batch_id, token),
        get_section(session, slug, typeId, _id, "DppNotes", subject, batch_id, token)
    ]
    videos, notes, dpps = await asyncio.gather(*tasks, return_exceptions=True)

    # Videos
    result.append("--- VIDEOS ---")
    if isinstance(videos, list) and videos:
        found_any = False
        video_tasks = [get_video_url(session, video, batch_id) for video in reversed(videos)]
        video_urls = await asyncio.gather(*video_tasks, return_exceptions=True)
        for video, url in zip(reversed(videos), video_urls):
            if isinstance(url, str) and url:
                video_title = video.get('video_title', 'Unknown Title')
                result.append(f"{video_title}: {url}")
                found_any = True
        if not found_any:
            result.append("No valid videos found.")
    else:
        result.append("No videos found.")

    # Notes
    result.append("\n--- NOTES ---")
    if isinstance(notes, list) and notes:
        found_any = False
        for note in reversed(notes):
            title = note.get('title', 'Unknown Title')
            download_url = note.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                found_any = True
        if not found_any:
            result.append("No valid notes found.")
    else:
        result.append("No notes found.")

    # DPPs
    result.append("\n--- DPPS ---")
    if isinstance(dpps, list) and dpps:
        found_any = False
        for dpp in reversed(dpps):
            title = dpp.get('title', 'Unknown Title')
            download_url = dpp.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                found_any = True
        if not found_any:
            result.append("No valid DPPs found.")
    else:
        result.append("No DPPs found.")

    result.append("\n")
    return "\n".join(result)

def create_progress_bar(progress, total, width=20):
    """Create a text-based progress bar."""
    if total == 0:
        return "[No items to process]"
    filled = int(width * progress // total)
    bar = '█' * filled + '-' * (width - filled)
    percent = (progress / total) * 100
    return f"[{bar}] {percent:.1f}%"

async def batch_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    batch_id = None
    filename = "all_videos_notes_dpps.txt"

    if len(args) < 1:
        await update.message.reply_text("Usage: /batch_id <batch_id> [-n <filename>.txt]")
        return

    batch_id = args[0]
    if len(args) > 2 and args[1] == "-n" and args[2].endswith(".txt"):
        filename = args[2]

    token = cookies["auth_token"]
    logger.info(f"Processing batch_id {batch_id} with filename {filename}")
    async with aiohttp.ClientSession() as session:
        subjects = await fetch_subjects(session, batch_id, token)
        if not subjects:
            logger.warning(f"No subjects found for batch_id {batch_id}")
            await update.message.reply_text("No subjects found or request failed. Please check the batch_id or cookies.")
            return

        # Send initial progress message
        progress_message = await update.message.reply_text("Processing... [                    ] 0.0%")

        try:
            total_subjects = len(subjects)
            subject_count = 0
            total_topics = 0
            topic_counts = []
            content_written = False

            # Fetch topic counts for all subjects concurrently
            topic_tasks = [get_topics(session, subject, batch_id, token) for subject in subjects]
            subject_topics = await asyncio.gather(*topic_tasks, return_exceptions=True)
            for topics in subject_topics:
                if isinstance(topics, list):
                    total_topics += len(topics)
                    topic_counts.append(len(topics))
                else:
                    topic_counts.append(0)

            with open(filename, "w", encoding="utf-8") as f:
                for subject, topics, topic_count in zip(subjects, subject_topics, topic_counts):
                    subject_count += 1
                    f.write(f"\nSubject: {subject['subject']} - SubjectId: {subject['subjectId']} - slug: {subject['slug']} - _id: {subject['_id']}\n")
                    content_written = True
                    if not isinstance(topics, list) or not topics:
                        f.write("No topics to process for this subject.\n")
                        # Update progress for subject completion
                        progress = subject_count / total_subjects if total_subjects > 0 else 1
                        await progress_message.edit_text(
                            f"Processing subject {subject_count}/{total_subjects}...\n{create_progress_bar(subject_count, total_subjects)}"
                        )
                        continue

                    # Process topics concurrently
                    topic_tasks = [collect_topic_contents(session, topic, subject, batch_id, token) for topic in topics]
                    topic_contents = await asyncio.gather(*topic_tasks, return_exceptions=True)

                    for idx, content in enumerate(topic_contents):
                        if isinstance(content, str) and content:
                            f.write(content)
                            content_written = True
                        # Update progress less frequently to avoid Telegram rate limits
                        if (idx + 1) % max(1, len(topics) // 5) == 0 or idx == len(topics) - 1:
                            progress = (subject_count - 1 + (idx + 1) / len(topics)) / total_subjects
                            await progress_message.edit_text(
                                f"Processing subject {subject_count}/{total_subjects}, topic {idx + 1}/{len(topics)}...\n{create_progress_bar(subject_count * total_topics + idx + 1, total_subjects * total_topics)}"
                            )
                    f.flush()

            # Check if file is empty
            if not content_written or os.path.getsize(filename) == 0:
                logger.error(f"Generated file {filename} is empty for batch_id {batch_id}")
                await progress_message.edit_text("Error: No content was generated. Please check the batch_id or cookies.")
                if os.path.exists(filename):
                    os.remove(filename)
                return

            # Final progress update
            await progress_message.edit_text("Processing complete! Uploading file...")

            # Send the file
            with open(filename, "rb") as f:
                await update.message.reply_document(document=f, filename=filename)

            # Delete the file
            os.remove(filename)
            await update.message.reply_text(f"File {filename} sent and deleted from storage.")
            logger.info(f"Successfully sent {filename} for batch_id {batch_id}")

        except Exception as e:
            logger.error(f"Error processing batch_id {batch_id}: {str(e)}")
            await progress_message.edit_text(f"Error processing request: {str(e)}")
            if os.path.exists(filename):
                os.remove(filename)

async def main():
    # Load bot token from environment variable with fallback for testing
    bot_token = os.getenv("BOT_TOKEN", "7549640350:AAFp-7vzfhRIo856b-f_gEilKIoeS9KPL5E")
    if not bot_token:
        logger.error("BOT_TOKEN environment variable not set")
        await asyncio.sleep(1)  # Ensure log is written before raising
        raise ValueError("BOT_TOKEN environment variable not set")

    logger.info(f"Bot token loaded: {bot_token[:10]}...")  # Log first 10 chars for security
    application = Application.builder().token(bot_token).build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("batch_id", batch_id))

    # Start the bot
    logger.info("Bot is starting...")
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        # Keep the bot running until interrupted
        await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"Error running bot: {str(e)}")
    finally:
        # Properly shut down the application
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        logger.info("Bot has shut down")

if __name__ == "__main__":
    # Create and manage the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        logger.info("Event loop closed")
