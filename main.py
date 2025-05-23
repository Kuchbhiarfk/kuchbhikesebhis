import requests
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
    "countdown_end_time": "MTc0Nzk4MTkwNTU3OA==",
    "auth_token": "cu7oiBffDQbRGx7%2FOhKylmKZYPBubC4Euenu4PkHPj%2FOyu1vuQDaiYALB5VP7gczlwp%2BlqKzYaCiMAuvv4nffM7dWQCTTTNJaNrjLCIxwleQ%2BIfrin5pJuz4juAjlioxrN8d2woRxX%2FUY5y39eYbhASTvLlTplTsH9ktR61S93UECYofiqCH9OO79fnBrc93ahIE3FfqB3hR%2FqMY677%2FVrkxVoP0G56YmxBlIXVnrK1vavK5TnZ%2B9vLBLJTV8lGBAqKL%2Fm4zsXDG0n7qfG0rG9WK2K9AhSIPAqxoH8h%2BpW621TsuKfmk5GXAB8lPSEFfxu4el5G1HQAraS69VGfeP3tC5PQyl%2FvmX5CtxD1Zzli55jLIYLFTXKUgCsHgAfd6iZ%2FhpECaeHeOken3%2FFUS3R14C5rpANjzAglAXSR1lLuqPgNYgQB9EcG8zXs8SBZYTSQom%2FM151PhS23FJ05lG5GGUvwfhYCxfKWqGYy%2B4KDUlxBygcv7VxINx08Br%2FscmCR5K7n%2BDYKc71vLM5LqrBxSyoqvt6rbZwACHh%2FSyRrKebaB7Ype%2FpOEUz%2BhfagTNX1wqAejiv9z%2Fm2BmYPYp04%2BiK0l0abkQYQ5%2FIGpLxpvizqjWxQylWKhvrLejWKMBjivgOpRf9x1Of8tpq8eqI4HTrCL82w2%2F9e7k8wsF4U%3D"
}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Welcome! Use /batch_id <batch_id> [-n <filename>.txt] to generate and receive a file with video, DPP, and notes URLs."
    )

def fetch_subjects(batch_id, token):
    url = (
        f"https://streamfiles.eu.org/api/batch_details.php?"
        f"batch_id={batch_id}&token={token}&type=details"
    )
    try:
        response = requests.get(url, headers=headers, cookies=cookies)
        response.raise_for_status()
        data = response.json()
        if data.get('success') and 'data' in data and 'subjects' in data['data']:
            return data['data']['subjects']
        else:
            return []
    except requests.RequestException:
        return []

def get_topics(subject, batch_id, token):
    url = (
        f"https://streamfiles.eu.org/api/batch_details.php?"
        f"batch_id={batch_id}&subject_id={subject['_id']}&token={token}&type=topics&page=1"
    )
    try:
        resp = requests.get(url, headers=headers, cookies=cookies)
        resp.raise_for_status()
        data = resp.json()
        if data.get("success") and isinstance(data.get("data"), list):
            return data["data"]
        else:
            return []
    except Exception:
        return []

def get_section(slug, typeId, _id, section_type, subject, batch_id, token):
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
        resp = requests.get(url, headers=headers, cookies=cookies)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        else:
            return []
    except Exception:
        return []

def get_video_url(video, batch_id):
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
        play_resp = requests.get(play_url, headers=headers, cookies=cookies, timeout=10)
        if play_resp.status_code == 200:
            soup = BeautifulSoup(play_resp.text, 'html.parser')
            input_group = soup.find('div', class_='input-group')
            if input_group:
                extracted = input_group.find('input', {'id': 'video_url'})
                return extracted['value'] if extracted else None
    except Exception:
        return None

def collect_topic_contents(topic, subject, batch_id, token):
    result = []
    name = topic.get("name", "No Name")
    slug = topic.get("slug", "")
    typeId = topic.get("typeId", "")
    _id = topic.get("_id", "")

    # Videos
    videos = get_section(slug, typeId, _id, "videos", subject, batch_id, token)
    if videos:
        found_any = False
        for video in reversed(videos):
            video_title = video.get('video_title', 'Unknown Title')
            real_url = get_video_url(video, batch_id)
            if real_url:
                result.append(f"{video_title}: {real_url}")
                found_any = True

    # Notes
    notes = get_section(slug, typeId, _id, "notes", subject, batch_id, token)
    if notes:
        found_any = False
        for note in reversed(notes):
            title = note.get('title', 'Unknown Title')
            download_url = note.get('download_url')
            if download_url:
                result.append(f"{title}: {download_url}")
                found_any = True

    # DPPs
    dpps = get_section(slug, typeId, _id, "DppNotes", subject, batch_id, token)
    if dpps:
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
    subjects = fetch_subjects(batch_id, token)
    if not subjects:
        await update.message.reply_text("No subjects found or request failed.")
        return

    # Send initial progress message
    progress_message = await update.message.reply_text("Processing... [                    ] 0.0%")

    try:
        total_subjects = len(subjects)
        subject_count = 0
        total_topics = sum(len(get_topics(subject, batch_id, token)) for subject in subjects)
        topic_count = 0

        with open(filename, "w", encoding="utf-8") as f:
            for subject in subjects:
                subject_count += 1
                topics = get_topics(subject, batch_id, token)
                if not topics:
                    # Update progress for subject completion
                    progress = subject_count / total_subjects if total_subjects > 0 else 1
                    await progress_message.edit_text(
                        f"Processing subject {subject_count}/{total_subjects}...\n{create_progress_bar(subject_count, total_subjects)}"
                    )
                    continue

                for topic in topics:
                    topic_count += 1
                    topic_content = collect_topic_contents(topic, subject, batch_id, token)
                    f.write(topic_content)
                    f.flush()

                    # Update progress for topic completion
                    if total_subjects > 0 and total_topics > 0:
                        progress = (subject_count - 1 + topic_count / len(topics)) / total_subjects
                    else:
                        progress = 1
                    await progress_message.edit_text(
                        f"Processing subject {subject_count}/{total_subjects}, topic {topic_count}/{total_topics}...\n{create_progress_bar(subject_count * total_topics + topic_count, total_subjects * total_topics)}"
                    )

                topic_count = 0  # Reset topic count for the next subject

        # Final progress update
        await progress_message.edit_text("Processing complete! Uploading file...")
        
        # Send the file
        with open(filename, "rb") as f:
            await update.message.reply_document(document=f, filename=filename)

        # Delete the file
        os.remove(filename)
        await update.message.reply_text(f"File {filename} sent and deleted from storage.")

    except Exception as e:
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
