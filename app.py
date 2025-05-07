from flask import Flask
from main import run_bot

app = Flask(__name__)

@app.route('/')
def index():
    return "Telegram Bot is running!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
