import os
import asyncio
from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters


def clean_btn(text: str) -> str:
    if not text:
        return ''
    t = str(text)
    import re
    t = re.sub(r'^[^A-Za-zА-Яа-я0-9]+', '', t)
    t = re.sub(r'[\u00A0\u1680\u2000-\u200A\u202F\u205F\u3000\u200B\s]+', ' ', t)
    t = re.sub(r'[\.,:;!\-–—_/]', '', t)
    t = t.strip().lower()
    return t


def reply_kb() -> ReplyKeyboardMarkup:
    keyboard = [
        ['🔎 Поиск препарата по названию', '🧪 Поиск по д.в.'],
        ['📋 Подбор пестицида', '🧮 Калькулятор расхода препарата'],
        ['ℹ️ Помощь', '📞 Контакты']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False, input_field_placeholder='Выберите действие или введите название...')


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    welcome = '👋 <b>Добро пожаловать!</b>\nЭтот бот поможет быстро подобрать пестицид по вашей культуре и вредному объекту, а также найти препарат по названию. Выберите действие на клавиатуре ниже.'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text=welcome, parse_mode='HTML', reply_markup=reply_kb())


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='📋 Главное меню', reply_markup=reply_kb())


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    help_text = 'ℹ️ <b>Как пользоваться</b>\n• Нажмите "Подбор пестицида" → выберите культуру/цели обработки → выберите вид объекта → выберите вид препарата.\n• Или отправьте название препарата — я подберу ближайшие совпадения.\n• Нажмите "Поиск по д.в." — введите действующее вещество и получите список препаратов.'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text=help_text, parse_mode='HTML', reply_markup=reply_kb())


async def cmd_setcommands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    commands = [
        BotCommand('start', 'Перезапуск / Главное меню'),
        BotCommand('menu', 'Показать клавиатуру меню'),
        BotCommand('reload', 'Обновить данные из таблицы'),
        BotCommand('help', 'Справка по использованию')
    ]
    await context.bot.set_my_commands(commands=commands)
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='Меню команд установлено')


async def cmd_reload(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='Кеш обновлён. Клавиатура обновлена.', parse_mode='HTML', reply_markup=reply_kb())


async def cmd_dbg_on(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    os.environ['DEBUG'] = '1'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='DEBUG=1')


async def cmd_dbg_off(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id if update.effective_chat else None
    os.environ['DEBUG'] = '0'
    if chat_id:
        await context.bot.send_message(chat_id=chat_id, text='DEBUG=0')


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg or not msg.text:
        return
    chat_id = update.effective_chat.id if update.effective_chat else None
    text = msg.text
    btn = clean_btn(text)
    if text == '/start' or text == '/restart':
        await cmd_start(update, context)
        return
    if text == '/menu':
        await cmd_menu(update, context)
        return
    if text == '/help':
        await cmd_help(update, context)
        return
    if text.startswith('/setcommands'):
        await cmd_setcommands(update, context)
        return
    if text.startswith('/reload'):
        await cmd_reload(update, context)
        return
    if text == '/dbg_on':
        await cmd_dbg_on(update, context)
        return
    if text == '/dbg_off':
        await cmd_dbg_off(update, context)
        return
    if btn == 'подбор пестицида':
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='📋 <b>Выберите культуру/цели обработки</b>', parse_mode='HTML')
        return
    if btn == 'поиск препарата по названию':
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='🔎 Введите название препарата текстом. Я учту опечатки и раскладку.', reply_markup=reply_kb())
        return
    if btn == 'поиск по дв':
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='🧪 Введите часть названия действующего вещества (например: "флорасулам" или "д.в. 2,4-д")', reply_markup=reply_kb())
        return
    if btn == 'помощь':
        await cmd_help(update, context)
        return
    if btn == 'контакты':
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text='❌ Контакты не найдены', reply_markup=reply_kb())
        return


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q:
        return
    await q.answer()
    data = q.data or ''
    await q.message.reply_text(f'CB: {data}')


async def main_async():
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        raise RuntimeError('TELEGRAM_TOKEN not set')
    app = ApplicationBuilder().token(token).build()
    app.add_handler(CommandHandler('start', cmd_start))
    app.add_handler(CommandHandler('menu', cmd_menu))
    app.add_handler(CommandHandler('help', cmd_help))
    app.add_handler(CommandHandler('setcommands', cmd_setcommands))
    app.add_handler(CommandHandler('reload', cmd_reload))
    app.add_handler(CommandHandler('dbg_on', cmd_dbg_on))
    app.add_handler(CommandHandler('dbg_off', cmd_dbg_off))
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_message))
    app.add_handler(CallbackQueryHandler(on_callback))
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    await app.updater.idle()
    await app.stop()


if __name__ == '__main__':
    asyncio.run(main_async())
