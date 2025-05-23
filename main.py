import aiohttp
import urllib.parse
from bs4 import BeautifulSoup
import os
import asyncio
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ---------- Common headers and cookies ----------
headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "application/json"
}

cookies = {
    "_clck": "1hjjwnc|2|fw3|0|1967",
    "verified_task": "dHJ1ZQ==",
    "countdown_end_time": "MTc0ODA3MTg4NDk1Ng==",
    "auth_token": "cu7oiBffDQbRGx7%2FOhKylmKZYPBubC4Euenu4PkHPj%2FOyu1vuQDaiYALB5VP7gczlwp%2BlqKzYaCiMAuvv4nffM7dWQCTTTNJaNrjLCIxwleQ%2BIfrin5pJuz4juAjlioxrN8d2woRxX%2FUY5y39eYbhASTvLlTplTsH9ktR61S93UECYofiqCH9OO79fnBrc93ahIE3FfqB3hR%2FqMY677%2FVrkxVoP0G56YmxBlIXVnrK1vavK5TnZ%2B9vLBLJTV8lGBAqKL%2Fm4zsXDG0n7qfG0rG9WK2K9AhSIPAqxoH8h%2BpW621TsuKfmk5GXAB8lPSEFfxu4el5G1HQAraS69VGfeP3tC5PQyl%2FvmX5CtxD1Zzli55jLIYLFTXKUgCsHgAfd6iZ%2FhpECaeHeOken3%2FFUS3R14C5rpANjzAglAXSR1lLuqPgNYgQB9EcG8zXs8SBZYTSQom%2FM151PhS23FJ05lG5GGUvwfhYCxfKWqGYy%2B4KDUlxBygcv7VxINx08Br%2FscmCR5K7n%2BDYKc71vLM5LqrBxSyoqvt6rbZwACHh%2FSyRrKebaB7Ype%2FpOEUz%2BhfagTNX1wqAejiv9z%2Fm2BmYPYp04%2BiK0l0abkQYQ5%2FIGpLxpvizqjWxQylWKhvrLejWKMBjivgOpRf9x1Of8tpq8eqI4HTrCL82w2%2F9e7k8wsF4U%3D"
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
                return data['data']['subjects']
            return []
    except Exception:
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
                return data["data"]
            return []
    except Exception:
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
                return data
            return []
    except Exception:
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
                    return extracted['value'] if extracted else None
    except Exception:
        return None

async def collect_topic_contents(session, topic, subject, batch_id, token):
    result = []
    name = topic.get("name", "No Name")
    slug = topic.get("slug", "")
    typeId = topic.get("typeId", "")
    _id = topic.get("_id", "")

    # Fetch videos, notes, and DPPs concurrently
    tasks = [
        get_section(session, slug, typeId, _id, "videos", subject, batch_id, token),
        get_section(session, slug, typeId, _id, "notes", subject, batch_id, token),
        get_section(session, slug, typeId, _id, "DppNotes", subject, batch_id, token)
    ]
    videos, notes, dpps = await asyncio.gather(*tasks, return_exceptions=True)

    # Videos
    if isinstance(videos, list) and videos:
        found_any = False
        video_tasks = [get_video_url(session, video, batch_id) for video in reversed(videos)]
        video_urls = await asyncio.gather(*video_tasks, return_exceptions=True)
        for video, url in zip(reversed(videos), video_urls):
            if isinstance(url, str) and url:
                video_title = video.get('video_title', 'Unknown Title')
                result.append(f"{video_title}: {url}")
                found_any = True
    # Notes
    if isinstance(notes, list) and notes:
        found_any = False
        for note in reversed(notes):
            title = note.get('title', 'Unknown Title')
            download_url = note.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                found_any = True

    # DPPs
    if isinstance(dpps, list) and dpps:
        found_any = False
        for dpp in reversed(dpps):
            title = dpp.get('title', 'Unknown Title')
            download_url = dpp.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                found_any = True
                return "\n".join(result)

def create_progress_bar(progress, total, width=20):
    """Create a text-based progress bar."""
    if total == 0:
        return "[No items to process]"
    filled = int(width * progress // total)
    bar = '🔥' * filled + '🌟' * (width - filled)
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
    async with aiohttp.ClientSession() as session:
        subjects = await fetch_subjects(session, batch_id, token)
        if not subjects:
            await update.message.reply_text("No subjects found or request failed.")
            return

        # Send initial progress message
        progress_message = await update.message.reply_text("Processing... [                    ] 0.0%")

        try:
            total_subjects = len(subjects)
            subject_count = 0
            total_topics = 0
            topic_counts = []

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
                    if not isinstance(topics, list) or not topics:
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
                        if isinstance(content, str):
                            f.write(content)
                        # Update progress less frequently to avoid Telegram rate limits
                        if (idx + 1) % max(1, len(topics) // 5) == 0 or idx == len(topics) - 1:
                            progress = (subject_count - 1 + (idx + 1) / len(topics)) / total_subjects
                            await progress_message.edit_text(
                                f"Processing subject {subject_count}/{total_subjects}, topic {idx + 1}/{len(topics)}...\n{create_progress_bar(subject_count * total_topics + idx + 1, total_subjects * total_topics)}"
                            )
                    f.flush()

            # Final progress update
            await progress_message.edit_text("Processing complete! Uploading file...")

            # Send the file
            with open(filename, "rb") as f:
                await update.message.reply_document(document=f, filename=filename)

            # Delete the file
            os.remove(filename)
            await update.message.reply_text(f"File {filename} sent and deleted from storage.")
        except Exception as e:
            logger.error(f"Error processing batch_id {batch_id}: {str(e)}")
            await progress_message.edit_text(f"Error processing request: {str(e)}")
            if os.path.exists(filename):
                os.remove(filename)

async def main():
    # Replace 'YOUR_BOT_TOKEN' with your actual bot token
    application = Application.builder().token("7549640350:AAFp-7vzfhRIo856b-f_gEilKIoeS9KPL5E").build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("batch_id", batch_id))

    # Start the bot
    print("Bot is running...")
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        # Keep the bot running until interrupted
        await asyncio.Event().wait()
    except Exception as e:
        print(f"Error running bot: {e}")
    finally:
        # Properly shut down the application
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    # Create and manage the event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("Bot stopped by user")
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        print("Event loop closed")
