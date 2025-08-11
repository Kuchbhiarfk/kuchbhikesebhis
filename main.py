import requests
from bs4 import BeautifulSoup
import os
import uuid
import asyncio
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ConversationHandler
from telegram import Update

# Conversation states
SUBJECT_URL, SELECT_SUBJECTS = range(2)

# Base URL
base_url = "https://rarestudy.site"

# Global store
subjects = {}
headers = {
        'authority': 'rarestudy.site',
       'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
      'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
     'cache-control': 'no-cache',
     'pragma': 'no-cache',
    'referer': 'https://rarestudy.site/batches',
    'sec-ch-ua': '"Chromium";v="137", "Not/A)Brand";v="24"',
    'sec-ch-ua-arch': '""',
    'sec-ch-ua-bitness': '""',
    'sec-ch-ua-full-version': '"137.0.7337.0"',
    'sec-ch-ua-full-version-list': '"Chromium";v="137.0.7337.0", "Not/A)Brand";v="24.0.0.0"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-model': '"211033MI"',
    'sec-ch-ua-platform': '"Android"',
    'sec-ch-ua-platform-version': '"11.0.0"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36'
}

# Fetch session token from API
def fetch_session_token():
    try:
        resp = requests.get("https://rarekatoken2.vercel.app/token", timeout=10)
        resp.raise_for_status()
        token_data = resp.json()
        if "use_token" in token_data:
            return token_data["use_token"]
    except Exception as e:
        print(f"Error fetching session token: {e}")
    return None

# Fetch URL with retries
def fetch_url(url):
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r

# Extract videos
def fetch_videos(url):
    try:
        r = fetch_url(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        items = soup.find_all('div', class_='item-box')[::-1]
        return [
            f"{i.find('div', class_='item-name').text.strip()} : "
            f"{base_url + i.find('a')['href'] if i.find('a')['href'].startswith('/') else i.find('a')['href']}"
            for i in items
        ]
    except:
        return []

# Extract notes
def fetch_notes(url):
    try:
        r = fetch_url(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        items = soup.find_all('div', class_='item-box')[::-1]
        result = []
        for i in items:
            note_link = i.find('a', class_='note-link')
            if note_link:
                result.append(f"{note_link.text.strip()} : {note_link['href']}")
        return result
    except:
        return []

# Extract DPP
def fetch_dpp(url):
    try:
        r = fetch_url(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        items = soup.find_all('div', class_='item-box')[::-1]
        result = []
        for i in items:
            note_link = i.find('a', class_='note-link')
            if note_link:
                result.append(f"{note_link.text.strip()} : {note_link['href']}")
        return result
    except:
        return []

# Start command
async def start(update: Update, context):
    await update.message.reply_text("Send subject URL with /url <link>")
    return SUBJECT_URL

# Handle URL
async def url_handler(update: Update, context):
    global subjects, headers

    # Get new session token
    session_token = fetch_session_token()
    if not session_token:
        await update.message.reply_text("❌ Failed to get session token.")
        return SUBJECT_URL
    headers['cookie'] = f"cf_clearance=g3z7irdDD_BHTi3MpE6UR1ay4eiXTVG5RkRAMVhKILY-1751948668-1.2.1.1-N72U8xIccTHnfRiJKnZ.6.7mFmGEyNtSKCGzExb012j7Stkj.tPSBic648hLtwqgM.lAlXy0u_JWeAoqL4C3smrGgLTPwHlhVNuf0kxOC5QYDhjj.elN4ZjSoh8doZN1V6BWcl3_eALAXHwzZUwP4Gp9J.fpDzuFCAIonMfPPtVMt4Ib7SiRLoEVsAmP7s6R1XueOqPqYCa9nVygHZBa3MRUsBcwC8SdOEfwy9TiFZE; session={session_token}"

    # Extract URL from message
    parts = update.message.text.split(maxsplit=1)
    if len(parts) < 2:
        await update.message.reply_text("Please provide a valid subject URL.")
        return SUBJECT_URL
    url = parts[1]

    try:
        r = fetch_url(url)
        subjects.clear()

        if 'application/json' in r.headers.get('content-type', ''):
            data = r.json()
            for item in data:
                if item.get('subject') and item.get('url'):
                    subjects[item['subject']] = item['url']
        else:
            soup = BeautifulSoup(r.text, 'html.parser')
            for a in soup.find_all('a'):
                if a.get('href') and a.text.strip():
                    subjects[a.text.strip()] = a.get('href')

        if not subjects:
            await update.message.reply_text("No subjects found.")
            return SUBJECT_URL

        msg = "Available subjects:\n"
        for i, sub in enumerate(subjects.keys(), 1):
            msg += f"{i}. {sub}\n"
        msg += "\nSend subject numbers (comma separated) or 0 for all"
        await update.message.reply_text(msg)

        context.user_data['subject_list'] = list(subjects.items())
        return SELECT_SUBJECTS

    except Exception as e:
        await update.message.reply_text(f"Error fetching subject URL: {e}")
        return SUBJECT_URL

# Fetch a single chapter's content sequentially
def fetch_chapter_content(chap_link):
    try:
        video_page = fetch_url(chap_link)
        soup_chap = BeautifulSoup(video_page.text, 'html.parser')
        video_tag = soup_chap.find('a', class_='video-link') or soup_chap.find('a', href=lambda x: x and '/videos' in x)
        if not video_tag:
            return ""

        videos_url = base_url + video_tag['href'] if video_tag['href'].startswith('/') else video_tag['href']
        notes_url = videos_url.replace('/videos', '/notes')
        dpp_url = videos_url.replace('/videos', '/DppNotes')

        lines = []
        lines.extend(fetch_videos(videos_url))
        lines.extend(fetch_notes(notes_url))
        lines.extend(fetch_dpp(dpp_url))

        return "\n".join([line.strip() for line in lines if line.strip()])
    except:
        return ""

# Handle subject selection
async def select_subjects(update: Update, context):
    try:
        subject_list = context.user_data.get('subject_list', [])
        selection = update.message.text.strip()

        if selection == '0':
            selected_indices = list(range(len(subject_list)))
        else:
            selected_indices = [int(x)-1 for x in selection.split(',')]

        total_subjects = len(selected_indices)
        total_chapters_all = 0

        # Count chapters
        for idx in selected_indices:
            sub_name, sub_path = subject_list[idx]
            chap_url = base_url + sub_path
            r = fetch_url(chap_url)
            soup = BeautifulSoup(r.text, 'html.parser')
            chapters = [(a.text.strip(), base_url + a['href'] if a['href'].startswith('/') else a['href'])
                        for a in soup.find_all('a') if a.get('href')]
            total_chapters_all += len(chapters)

        processed_chapters = 0
        progress_message = await update.message.reply_text("Starting sequential fetch...")

        all_data_parts = []

        for s_idx, idx in enumerate(selected_indices, start=1):
            sub_name, sub_path = subject_list[idx]
            chap_url = base_url + sub_path
            r = fetch_url(chap_url)
            soup = BeautifulSoup(r.text, 'html.parser')
            chapters = [(a.text.strip(), base_url + a['href'] if a['href'].startswith('/') else a['href'])
                        for a in soup.find_all('a') if a.get('href')]

            for c_idx, (chap_name, chap_link) in enumerate(chapters, start=1):
                content = await asyncio.to_thread(fetch_chapter_content, chap_link)
                if content.strip():
                    all_data_parts.append(content)
                processed_chapters += 1
                await progress_message.edit_text(
                    f"Processing: Subject {s_idx}/{total_subjects} - {sub_name}\n"
                    f"Chapter {c_idx}/{len(chapters)}\n"
                    f"Completed: {processed_chapters}/{total_chapters_all} total chapters"
                )

        all_data = "\n".join(all_data_parts).strip()
        filename = f"{uuid.uuid4()}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(all_data)

        with open(filename, "rb") as f:
            await update.message.reply_document(document=f, filename="all_content.txt")
        os.remove(filename)

        await update.message.reply_text("✅ Done. All chapters fetched sequentially.")
        return SUBJECT_URL

    except Exception as e:
        await update.message.reply_text(f"Error processing subjects: {e}")
        return SUBJECT_URL

# Cancel command
async def cancel(update: Update, context):
    await update.message.reply_text("❌ Cancelled.")
    return ConversationHandler.END

# Main
def main():
    bot_token = "7639794663:AAH36AfkV2O8MGddHKWvKKbHtpQPYIOfNzU"  # Replace with your bot token
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app = Application.builder().token(bot_token).build()
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('url', url_handler)],
        states={
            SUBJECT_URL: [CommandHandler('url', url_handler)],
            SELECT_SUBJECTS: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_subjects)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler('start', start))

    loop.run_until_complete(app.run_polling())

if __name__ == "__main__":
    main()
