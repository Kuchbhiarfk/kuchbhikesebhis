import requests
import urllib.parse
from bs4 import BeautifulSoup
import asyncio
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
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
    "auth_token": "cu7oiBffDQbRGx7%2FOhKylmKZYPBubC4Euenu4PkHPj%2FOyu1vuQDaiYALB5VP7gcznDf2arZE%2FMI1T3zVK6YvKhr5NRrGdctre17tEg4sf2zG%2FZFA%2BwR3mqrLd6HV8snqS9BMH50aC1D14G%2Fz%2B1gMtXXpIH43teFekVPQ0d2HX5WxNuUmnlsiOMpkBJccB4Xj428%2F7qxg5AHFhs%2BfwgdAgn6IY%2BZh5Lpm72ZNigTYMi0Q87giFgAV0pK3FFdE%2Fn%2B%2B2CKM9cTIKvFLZpcasYnTmnwkrxTocARP4GZkd6KjlcyrVItczal568TmZZFvVLKF7%2BAtgQ%2BLty%2Fio1pNhnvPszppVtjetp8H9GnV1A%2FAoVAOqgUmHSA1jForzfYx4K9HnyDfPA14qqiVdSuDmbZeWrB7GUOxlpdKyqa6HrcWNrfKnVy6tT04h9rj8i%2FO2oakF%2F8pPHJ4NyJQG03rkxKHsSjzg2OZ2FcB7mW6Zb31QWMq09YrT5nQ00pqBhbUS4loXEnQDC6ry6LuRSpRj%2FS5jLnh%2FiFDmx7uFK7zm28pO2Gh382DSW5OIfYegF46WQlefcyv144nNKmr3h2YjgxjNoHD0aGqBdnh%2BxdOdqrBAyzqhOPO8E7%2FVN1Jx2qdp9ZtyttCcKfTN6fXdogZqcKGvw5pOhoKpWPnNM8JPQaHROM%3D"
}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_chat.send_message(
        "Welcome! Use /batch_id <batch_id> -n <channel_id> to start the process."
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
            subjects = data['data']['subjects']
            if not subjects:
                print(f"No subjects found for batch_id {batch_id}")
            else:
                print(f"Found {len(subjects)} subjects for batch_id {batch_id}")
            return subjects
        else:
            print(f"No subjects found for batch_id {batch_id}. Response: {data}")
            return []
    except requests.RequestException as e:
        print(f"Error fetching subjects for batch_id {batch_id}: {e}")
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
            if not data["data"]:
                print(f"No topics found for subject {subject['_id']} ({subject['slug']})")
            else:
                print(f"Found {len(data['data'])} topics for subject {subject['_id']} ({subject['slug']})")
            return data["data"]
        else:
            print(f"Invalid topics response for subject {subject['_id']} ({subject['slug']}): {data}")
            return []
    except Exception as e:
        print(f"Error fetching topics for subject {subject['_id']} ({subject['slug']}): {e}")
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
            if not data:
                print(f"No {section_type} found for topic {slug} in subject {subject['slug']}")
            else:
                print(f"Found {len(data)} {section_type} for topic {slug} in subject {subject['slug']}")
            return data
        else:
            print(f"Invalid {section_type} response for topic {slug} in subject {subject['slug']}: {data}")
            return []
    except Exception as e:
        print(f"Error fetching {section_type} for topic {slug} in subject {subject['slug']}: {e}")
        return []

def get_video_url(video, batch_id):
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
    print(f"Attempting to extract video URL for {video_title} at {play_url}")
    try:
        play_resp = requests.get(play_url, headers=headers, cookies=cookies, timeout=15)
        if play_resp.status_code == 200:
            soup = BeautifulSoup(play_resp.text, 'html.parser')
            input_group = soup.find('div', class_='input-group')
            if input_group:
                extracted = input_group.find('input', {'id': 'video_url'})
                if extracted and extracted['value']:
                    print(f"Successfully extracted video URL for {video_title}: {extracted['value']}")
                    return extracted['value']
                else:
                    print(f"No video URL found in input tag for {video_title}")
            else:
                print(f"No input-group div found in play page for {video_title}")
        else:
            print(f"Failed to fetch play page for {video_title}: Status {play_resp.status_code}")
        return None
    except Exception as e:
        print(f"Error extracting video URL for {video_title}: {e}")
        return None

async def collect_topic_contents(topic, subject, batch_id, token, context: ContextTypes.DEFAULT_TYPE, channel_id: str, last_content_type: str):
    name = topic.get("name", "No Name")
    slug = topic.get("slug", "")
    typeId = topic.get("typeId", "")
    _id = topic.get("_id", "")

    print(f"Processing topic {name} (slug: {slug}, id: {_id}) in subject {subject['slug']} (id: {subject['_id']})")

    # Initialize content count
    urls_sent = 0
    current_last_content_type = last_content_type

    # Process videos one by one
    videos = get_section(slug, typeId, _id, "videos", subject, batch_id, token)
    if videos:
        print(f"Found {len(videos)} videos for topic {name}")
        for video in reversed(videos):
            # Fetch video URL
            video_title = video.get('video_title', 'Unknown Title')
            real_url = get_video_url(video, batch_id)
            if real_url:
                content_url = f"{video_title}: {real_url}"
                content_type = 'video'

                # Determine delay based on last content type
                if current_last_content_type is None:
                    delay = 0  # No delay for the first URL
                else:
                    if current_last_content_type == 'video':
                       delay = 480
                    else:
                        delay = 60 if current_last_content_type in ('note', 'dpp') else 480

                if delay > 0:
                    print(f"Waiting {delay} seconds before sending {content_type} URL: {content_url}")
                    await asyncio.sleep(delay)

                # Send the URL to the specified channel
                try:
                    await context.bot.send_message(chat_id=channel_id, text=content_url)
                    print(f"Sent {content_type} URL to channel {channel_id}: {content_url}")
                    urls_sent += 1
                except Exception as e:
                    print(f"Error sending {content_type} URL {content_url} to channel {channel_id}: {e}")
                    await context.bot.send_message(chat_id=channel_id, text=f"Error sending URL: {content_url}")

                current_last_content_type = content_type
                # Wait for delay before fetching the next item
                if current_last_content_type == 'video':
                    await asyncio.sleep(480 if content_type == 'video' else 60)
                else:
                    await asyncio.sleep(60 if content_type in ('note', 'dpp') else 480)
            else:
                print(f"No valid URL extracted for video {video_title} in topic {name}")
    else:
        print(f"No videos found for topic {name}")

    # Process notes one by one
    notes = get_section(slug, typeId, _id, "notes", subject, batch_id, token)
    if notes:
        print(f"Found {len(notes)} notes for topic {name}")
        for note in reversed(notes):
            # Fetch note URL
            title = note.get('title', 'Unknown Title')
            download_url = note.get('download_url')
            if download_url:
                content_url = f"{title}: {download_url}"
                content_type = 'note'

                # Determine delay based on last content type
                if current_last_content_type is None:
                    delay = 0
                else:
                    if current_last_content_type == 'video':
                        delay = 60 if content_type in ('note', 'dpp') else 480
                    else:
                        delay = 60 if current_last_content_type in ('note', 'dpp') else 60

                if delay > 0:
                    print(f"Waiting {delay} seconds before sending {content_type} URL: {content_url}")
                    await asyncio.sleep(delay)

                # Send the URL to the specified channel
                try:
                    await context.bot.send_message(chat_id=channel_id, text=content_url)
                    print(f"Sent {content_type} URL to channel {channel_id}: {content_url}")
                    urls_sent += 1
                except Exception as e:
                    print(f"Error sending {content_type} URL {content_url} to channel {channel_id}: {e}")
                    await context.bot.send_message(chat_id=channel_id, text=f"Error sending URL: {content_url}")

                current_last_content_type = content_type
                # Wait for delay before fetching the next item
                if current_last_content_type == 'video':
                    await asyncio.sleep(480 if content_type == 'video' else 60)
                else:
                    await asyncio.sleep(60 if content_type in ('note', 'dpp') else 480)
            else:
                print(f"No download URL for note {title} in topic {name}")
    else:
        print(f"No notes found for topic {name}")

    # Process DPPs one by one
    dpps = get_section(slug, typeId, _id, "DppNotes", subject, batch_id, token)
    if dpps:
        print(f"Found {len(dpps)} DPPs for topic {name}")
        for dpp in reversed(dpps):
            # Fetch DPP URL
            title = dpp.get('title', 'Unknown Title')
            download_url = dpp.get('download_url')
            if download_url:
                content_url = f"{title}: {download_url}"
                content_type = 'dpp'

                # Determine delay based on last content type
                if current_last_content_type is None:
                    delay = 0
                else:
                    if current_last_content_type == 'video':
                        delay = 60 if content_type in ('note', 'dpp') else 480
                    else:
                        delay = 60 if current_last_content_type in ('note', 'dpp') else 60

                if delay > 0:
                    print(f"Waiting {delay} seconds before sending {content_type} URL: {content_url}")
                    await asyncio.sleep(delay)

                # Send the URL to the specified channel
                try:
                    await context.bot.send_message(chat_id=channel_id, text=content_url)
                    print(f"Sent {content_type} URL to channel {channel_id}: {content_url}")
                    urls_sent += 1
                except Exception as e:
                    print(f"Error sending {content_type} URL {content_url} to channel {channel_id}: {e}")
                    await context.bot.send_message(chat_id=channel_id, text=f"Error sending URL: {content_url}")

                current_last_content_type = content_type
                # Wait for delay before fetching the next item
                if current_last_content_type == 'video':
                    await asyncio.sleep(480 if content_type == 'video' else 60)
                else:
                    await asyncio.sleep(60 if content_type in ('note', 'dpp') else 480)
            else:
                print(f"No download URL for DPP {title} in topic {name}")
    else:
        print(f"No DPPs found for topic {name}")

    if urls_sent == 0:
        print(f"No content (videos, notes, DPPs) collected for topic {name} in subject {subject['slug']}")
        await context.bot.send_message(
            chat_id=channel_id,
            text=f"No content found for topic {name} in subject {subject['slug']}."
        )

    return urls_sent, current_last_content_type  # Return number of URLs sent and last content type

def create_progress_bar(progress, total, width=20):
    if total == 0:
        return "[No items to process]"
    filled = int(width * progress // total)
    bar = '█' * filled + '-' * (width - filled)
    percent = (progress / total) * 100
    return f"[{bar}] {percent:.1f}%"

async def batch_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = context.args
    if len(args) < 3 or args[1] != '-n':
        await update.effective_chat.send_message(
            "Usage: /batch_id <batch_id> -n <channel_id>"
        )
        return

    batch_id = args[0]
    channel_id = args[2]

    # Check if the bot is an admin in the target channel
    try:
        chat = await context.bot.get_chat(channel_id)
        if chat.type != 'channel':
            await update.effective_chat.send_message(
                f"Error: {channel_id} is not a valid channel ID."
            )
            return
        bot_id = context.bot.id
        admins = await chat.get_administrators()
        bot_is_admin = any(admin.user.id == bot_id for admin in admins)
        if not bot_is_admin:
            await update.effective_chat.send_message(
                f"Error: Bot must be an admin in the channel {channel_id} to send messages."
            )
            return
    except Exception as e:
        print(f"Error checking admin status in channel {channel_id}: {e}")
        await update.effective_chat.send_message(
            f"Error: Unable to verify bot admin status in channel {channel_id}. Please ensure the bot is an admin."
        )
        return

    token = cookies["auth_token"]
    subjects = fetch_subjects(batch_id, token)
    if not subjects:
        await context.bot.send_message(
            chat_id=channel_id,
            text="No subjects found or request failed."
        )
        return

    # Store batch_id, channel_id, subjects, and initialize last_content_type in chat_data
    context.chat_data['batch_id'] = batch_id
    context.chat_data['channel_id'] = channel_id
    context.chat_data['subjects'] = subjects
    context.chat_data['awaiting_subject'] = True
    context.chat_data['last_content_type'] = None

    # Create and send subject list to the specified channel
    subject_list = [f"{i} {subj['_id']} - {subj['slug']}" for i, subj in enumerate(subjects, 1)]
    subject_message = "Available subjects:\n" + "\n".join(subject_list) + "\n\nPlease reply with the index or subjectId you want to fetch."
    await context.bot.send_message(chat_id=channel_id, text=subject_message)

async def handle_subject_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.chat_data.get('awaiting_subject', False):
        return

    # Handle message from channel or private chat
    user_input = update.message.text.strip() if update.message else update.channel_post.text.strip()

    subjects = context.chat_data.get('subjects', [])
    batch_id = context.chat_data.get('batch_id')
    channel_id = context.chat_data.get('channel_id')
    token = cookies["auth_token"]
    last_content_type = context.chat_data.get('last_content_type', None)

    # Try to interpret input as index or subjectId
    selected_subject = None
    try:
        index = int(user_input) - 1
        if 0 <= index < len(subjects):
            selected_subject = subjects[index]
    except ValueError:
        for subject in subjects:
            if subject['_id'] == user_input:
                selected_subject = subject
                break

    if not selected_subject:
        await context.bot.send_message(
            chat_id=channel_id,
            text="Invalid input. Please provide a valid index or subjectId."
        )
        return

    # Clear the awaiting state
    context.chat_data['awaiting_subject'] = False

    # Warn about 'notices' subjects
    if selected_subject['slug'].startswith('notices'):
        await context.bot.send_message(
            chat_id=channel_id,
            text=(
                f"Warning: Subject {selected_subject['slug']} appears to be a 'notices' category, "
                "which may not contain videos, notes, or DPPs."
            )
        )

    # Send initial progress message to the specified channel
    progress_message = await context.bot.send_message(
        chat_id=channel_id,
        text="Processing... [                    ] 0.0%"
    )

    try:
        topics = get_topics(selected_subject, batch_id, token)
        if not topics:
            await progress_message.edit_text(
                f"No topics found for subject {selected_subject['slug']} ({selected_subject['_id']}). "
                "This subject may not contain any topics or may be a special category like 'notices'."
            )
            return

        total_topics = len(topics)
        topic_count = 0
        total_urls_sent = 0

        for topic in topics:
            topic_count += 1
            urls_sent, last_content_type = await collect_topic_contents(
                topic, selected_subject, batch_id, token, context, channel_id, last_content_type
            )
            total_urls_sent += urls_sent

            # Update last_content_type in chat_data
            context.chat_data['last_content_type'] = last_content_type

            # Update progress in the specified channel
            progress = topic_count / total_topics if total_topics > 0 else 1
            await progress_message.edit_text(
                f"Processing topic {topic_count}/{total_topics} for subject {selected_subject['slug']}...\n"
                f"{create_progress_bar(topic_count, total_topics)}\n"
                f"Sent {total_urls_sent} URLs."
            )

        if total_urls_sent == 0:
            await progress_message.edit_text(
                f"No content (videos, notes, or DPPs) found for subject {selected_subject['slug']} ({selected_subject['_id']}). "
                "This may be a special category (e.g., 'notices') or have no accessible content."
            )
        else:
            await progress_message.edit_text(
                f"Completed! Sent {total_urls_sent} URLs for subject {selected_subject['slug']}."
            )

    except Exception as e:
        print(f"Exception in handle_subject_selection for subject {selected_subject['slug']}: {e}")
        await progress_message.edit_text(
            f"Error processing request for subject {selected_subject['slug']}: {str(e)}"
        )

async def main():
    # Replace with your actual bot token
    application = Application.builder().token("7624523973:AAHz3VjH0k9qD9DrBnm4tvLsnNGwxcMxIwY").build()

    # Register handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("batch_id", batch_id))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_subject_selection))

    # Start the bot
    print("Bot is running...")
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        await asyncio.Event().wait()
    except Exception as e:
        print(f"Error running bot: {e}")
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
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
