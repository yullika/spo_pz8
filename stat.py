import time
import telebot
import random
import threading
import datetime
import SQLTable as S

# Настройки базы данных
db_config = {
    'user': 'j1007852',
    'password': 'el|N#2}-F8',
    'host': 'srv201-h-st.jino.ru',
    'database': 'j1007852_13423'
}

# Инициализация таблиц
users_table_name = 'users_7'
health_facts_table_name = 'health_facts'
stats_table_name = 'bot_stats'
sql_table_users = S.SQLTable(db_config, users_table_name)
sql_table_health_facts = S.SQLTable(db_config, health_facts_table_name)
sql_table_stats = S.SQLTable(db_config, stats_table_name)

# Инициализация бота
bot = telebot.TeleBot("7215978807:AAHv7CQEs31qHLOKEQ4PYEaFlZebSdUz7oU")


# Загрузка сообщений из файла
def load_messages():
    try:
        with open('facts.txt', 'r', encoding='utf-8') as f:
            return f.readlines()
    except Exception as e:
        print(f"Ошибка при загрузке сообщений: {e}")
        return []


messages = load_messages()

active_chats = {}


# Функция для записи статистики
def log_statistic(user_id, command):
    timestamp = datetime.datetime.now()
    date = timestamp.date()
    sql_table_stats.insert_statistic(user_id, command, date)


# Функция для получения статистики по дням
def get_daily_statistics(date):
    return sql_table_stats.get_statistics_by_date(date)


# Функция для получения статистики по пользователям
def get_user_statistics(user_id):
    return sql_table_stats.get_statistics_by_user(user_id)


# Функция для получения статистики по командам
def get_command_statistics(command):
    return sql_table_stats.get_statistics_by_command(command)


@bot.message_handler(commands=['start'])
def start_message(message):
    chat_id = str(message.chat.id)
    if not sql_table_users.user_exists(chat_id):
        sql_table_users.insert_user(chat_id)
    bot.reply_to(message, "Привет! Я буду отправлять тебе случайные сообщения о здоровье.")

    # Логируем статистику
    log_statistic(chat_id, '/start')

    reminder_thread = threading.Thread(target=send_reminders, args=(chat_id,))
    reminder_thread.start()


@bot.message_handler(commands=['fact'])
def fact_message(message):
    random_fact = sql_table_health_facts.get_random_fact()
    bot.reply_to(message, f'Лови факт о здоровье: {random_fact}')

    # Логируем статистику
    log_statistic(message.chat.id, '/fact')


@bot.message_handler(commands=['help'])
def help_message(message):
    myhelp = "/start - начало работы бота \n/help - помощь \n/fact - факты о здоровье \n/game - игра"
    bot.reply_to(message, f'Я умею:\n{myhelp}')

    # Логируем статистику
    log_statistic(message.chat.id, '/help')


@bot.message_handler(commands=['game'])
def game_message(message):
    bot.reply_to(message, "Привет! Игра начинается! Напиши 'стоп', чтобы остановить.")
    chat_id = message.chat.id
    active_chats[chat_id] = True

    # Логируем статистику
    log_statistic(chat_id, '/game')

    game_thread = threading.Thread(target=stop_game, args=(chat_id,))
    game_thread.start()


def send_reminders(chat_id):
    while True:
        random_message = random.choice(messages).strip()
        bot.send_message(chat_id, random_message)
        time.sleep(3600)


def stop_game(chat_id):
    while active_chats.get(chat_id, False):
        time.sleep(1)  # Проверяем каждую секунду


@bot.message_handler(func=lambda message: True)
def check_for_stop_command(message):
    chat_id = message.chat.id
    if message.text.lower() == 'стоп':
        if chat_id in active_chats:
            active_chats[chat_id] = False  # Останавливаем игру
            bot.send_message(chat_id, "Игра остановлена.")
        else:
            bot.send_message(chat_id, "Игра не была запущена.")

# Запуск бота
if __name__ == '__main__':
    try:
        bot.polling(none_stop=True)
    except Exception as e:
        print(f"Произошла ошибка: {e}")
