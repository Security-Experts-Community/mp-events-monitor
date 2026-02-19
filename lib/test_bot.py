import json
from pathlib import Path

from telebot import TeleBot, types


def start_work_bot(filename):
    with Path(".bot.json").open("r", encoding="utf-8") as bot_creds:
        bot_info = json.load(bot_creds)

    bot = TeleBot(bot_info["bot_id"])
    tg_chat_id = bot_info["tg_chat_id"]
    # bot.send_message(tg_chat_id, "start xlsx_out.py")
    if filename:
        bot.send_document(tg_chat_id, types.InputFile(filename))

    @bot.message_handler(commands=["start"])
    def start(message):
        pass

    return bot


if __name__ == "__main__":
    bot = start_work_bot("")
    bot.polling(non_stop=False, timeout=5)
