import os
import re
import random
import logging
import hashlib
from typing import Optional, Dict, Any, List

import aiohttp
from telegram import (
    Update,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    filters,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("detailing-finance-bot")


# =========================
# CONFIG from ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
SCRIPT_URL = os.getenv("SCRIPT_URL", "").strip()

# Список разрешенных user_id через запятую
USER_TG_IDS_STR = os.getenv("USER_TG_IDS", "").strip()
if USER_TG_IDS_STR:
    USER_TG_IDS = [int(x.strip()) for x in USER_TG_IDS_STR.split(",") if x.strip()]
else:
    USER_TG_IDS = []

# ID владельцев (полный доступ)
OWNER_IDS_STR = os.getenv("OWNER_IDS", "").strip()
if OWNER_IDS_STR:
    OWNER_IDS = [int(x.strip()) for x in OWNER_IDS_STR.split(",") if x.strip()]
else:
    OWNER_IDS = []

# Для webhook (Railway)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip()

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not SCRIPT_URL:
    raise RuntimeError("SCRIPT_URL is missing")
if not USER_TG_IDS:
    raise RuntimeError("USER_TG_IDS is missing")


def _default_webhook_path() -> str:
    h = hashlib.sha256(BOT_TOKEN.encode("utf-8")).hexdigest()
    return f"tg/{h[:24]}"


# =========================
# Phrases
# =========================
PH_SAVED_INCOME = [
    "Отлично! ✅ Записал поступление.",
    "Есть! ✅ Зафиксировал.",
    "Принял ✅",
    "Готово ✅",
]

PH_SAVED_EXPENSE = [
    "Записал ✅",
    "Готово ✅",
    "Зафиксировал ✅",
    "Есть ✅",
    "Принял ✅",
]

DENY_TEXT = "Извини, доступ закрыт 🙂"


# =========================
# Conversation states
# =========================
(
    ST_MENU,
    ST_ADD_CHOOSE_TYPE,
    ST_EXP_CATEGORY,
    ST_EXP_PAYMENT_TYPE,
    ST_AMOUNT,
    ST_COMMENT,
    ST_INC_CATEGORY,
    ST_ANALYSIS_PERIOD,
    ST_ANALYSIS_TYPE,
    ST_SPECIAL_REPORTS,
    ST_BALANCE_CHOOSE_TYPE,
    ST_BALANCE_EDIT,
    ST_DEBTS_CHOOSE_TYPE,
    ST_DEBTS_EDIT,
) = range(14)


# =========================
# Helpers: temp messages
# =========================
async def delete_working_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Удалить текущее рабочее сообщение"""
    msg_id = context.user_data.get("working_message_id")
    if msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Couldn't delete message {msg_id}: {e}")
    context.user_data["working_message_id"] = None


# =========================
# Helpers: keyboards
# =========================
def is_allowed(update: Update) -> bool:
    """Проверка доступа - разрешен ли пользователь"""
    user = update.effective_user
    if not user:
        return False
    return user.id in USER_TG_IDS


def is_owner(user_id: int) -> bool:
    """Проверка - является ли пользователь владельцем"""
    return user_id in OWNER_IDS


def kb_main_owner() -> InlineKeyboardMarkup:
    """Главное меню для владельцев"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Внести транзакцию", callback_data="menu:add")],
        [InlineKeyboardButton("📊 Анализ", callback_data="menu:analysis")],
        [InlineKeyboardButton("💰 Сверить баланс", callback_data="menu:balance")],
        [InlineKeyboardButton("💳 Долги", callback_data="menu:debts")],
    ])


def kb_main_employee() -> InlineKeyboardMarkup:
    """Главное меню для сотрудников"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Внести транзакцию", callback_data="menu:add")],
    ])


def kb_choose_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➖ Затраты", callback_data="type:expense")],
        [InlineKeyboardButton("➕ Доход", callback_data="type:income")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:menu")],
    ])


def kb_expense_categories(categories: List[str]) -> InlineKeyboardMarkup:
    """Динамическая клавиатура категорий расходов"""
    rows = []
    row = []
    for i, c in enumerate(categories):
        row.append(InlineKeyboardButton(c, callback_data=f"expcat:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:choose_type")])
    return InlineKeyboardMarkup(rows)


def kb_income_categories(categories: List[str]) -> InlineKeyboardMarkup:
    """Динамическая клавиатура категорий доходов"""
    rows = []
    for i, c in enumerate(categories):
        rows.append([InlineKeyboardButton(c, callback_data=f"inccat:{i}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:choose_type")])
    return InlineKeyboardMarkup(rows)


def kb_payment_types(payment_types: List[str]) -> InlineKeyboardMarkup:
    """Динамическая клавиатура форм оплаты"""
    rows = []
    for i, p in enumerate(payment_types):
        emoji = "💵" if p == "Наличные" else ("📱" if p == "QR код" else "🏢")
        rows.append([InlineKeyboardButton(f"{emoji} {p}", callback_data=f"payment:{i}")])
    return InlineKeyboardMarkup(rows)


def kb_skip_comment() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Пропустить", callback_data="comment:skip")],
    ])


def kb_analysis_periods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Сегодня", callback_data="aperiod:today")],
        [InlineKeyboardButton("📅 Эта неделя", callback_data="aperiod:week")],
        [InlineKeyboardButton("📅 Этот месяц", callback_data="aperiod:month")],
        [InlineKeyboardButton("📅 Этот год", callback_data="aperiod:year")],
        [InlineKeyboardButton("⚙️ Специальные отчеты", callback_data="aperiod:special")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:menu")],
    ])


def kb_analysis_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Поступления", callback_data="atype:income")],
        [InlineKeyboardButton("💸 Затраты", callback_data="atype:expense")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:analysis_periods")],
    ])


def kb_special_reports() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Сравнение месяцев", callback_data="special:compare")],
        [InlineKeyboardButton("💰 Средний чек", callback_data="special:average")],
        [InlineKeyboardButton("📋 Топ категорий затрат", callback_data="special:top")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:analysis_periods")],
    ])


def kb_balance_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💵 Изменить наличные", callback_data="balance:cash")],
        [InlineKeyboardButton("📱 Изменить QR", callback_data="balance:qr")],
        [InlineKeyboardButton("🏢 Изменить БН", callback_data="balance:bn")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:menu")],
    ])


def kb_debts_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Долги передо мной", callback_data="debts_type:owe_me")],
        [InlineKeyboardButton("💳 Мои долги", callback_data="debts_type:i_owe")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:menu")],
    ])


def kb_debts_actions() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Изменить", callback_data="debts:edit")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back:debts_type")],
    ])


# =========================
# Amount parsing
# =========================
def parse_amount(text: str) -> Optional[float]:
    if not text:
        return None
    s0 = text.strip().lower()

    mult = 1.0
    s = re.sub(r"\s+", "", s0)
    if s.endswith("к") or s.endswith("k"):
        mult = 1000.0
        s = s[:-1]

    has_comma = "," in s
    has_dot = "." in s

    if has_comma and has_dot:
        last_comma = s.rfind(",")
        last_dot = s.rfind(".")
        dec_pos = max(last_comma, last_dot)
        int_part = re.sub(r"[.,]", "", s[:dec_pos])
        frac_part = re.sub(r"[.,]", "", s[dec_pos + 1:])
        s = f"{int_part}.{frac_part}"
    elif has_comma and not has_dot:
        s = s.replace(",", ".")
    else:
        pass

    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        val = float(s) * mult
        if val < 0:
            return None
        return round(val, 2)
    except Exception:
        return None


# =========================
# GAS API
# =========================
async def gas_request(payload: Dict[str, Any], user_id: int) -> Dict[str, Any]:
    """Отправить запрос в GAS с указанным user_id"""
    payload = dict(payload)
    payload["user_id"] = user_id

    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(SCRIPT_URL, json=payload) as resp:
            txt = await resp.text()
            try:
                data = await resp.json()
            except Exception:
                logger.error("GAS non-json response: %s", txt)
                raise RuntimeError("GAS вернул не-JSON ответ")
            if not data.get("ok"):
                raise RuntimeError(data.get("error") or "GAS error")
            return data["data"]


def format_transaction(tx: Dict) -> str:
    """Форматировать транзакцию для отображения"""
    type_emoji = "➕" if tx["type"] == "доход" else "➖"
    amount_str = f"{tx['amount']:,.0f} ₽".replace(",", " ")
    
    if tx["type"] == "доход":
        # Доход: ➕ 25 000 ₽ — BMW X5 — QR код
        comment = tx.get("comment", "")
        category = tx.get("category", "")
        return f"{type_emoji} {amount_str} — {comment} — {category}"
    else:
        # Расход: ➖ 5 000 ₽ — Инструменты — Наличные
        category = tx.get("category", "")
        payment_type = tx.get("payment_type", "")
        comment = tx.get("comment", "")
        if comment:
            return f"{type_emoji} {amount_str} — {category} — {payment_type} — {comment}"
        else:
            return f"{type_emoji} {amount_str} — {category} — {payment_type}"


async def main_screen_text_owner(user_id: int) -> str:
    """Получить текст главного экрана для владельца"""
    s = await gas_request({"cmd": "summary_month"}, user_id)
    txs = await gas_request({"cmd": "get_last_transactions", "limit": 5}, user_id)
    
    month = s.get("month_label", "Текущий месяц")
    exp = s.get("expenses", 0)
    inc = s.get("incomes", 0)
    bal_month = s.get("balance_month", 0)
    balances = s.get("balances", {})
    bal_total = s.get("balance_total", 0)
    debts_owe_me = s.get("debts_owe_me", 0)
    debts_i_owe = s.get("debts_i_owe", 0)
    
    text = (
        f"<b>💼 Бизнес</b>\n"
        f"<b>{month}</b>\n\n"
        f"<b>💰 Баланс:</b>\n"
        f"💵 Наличные: <b>{balances.get('cash', 0):,.2f}</b> ₽\n"
        f"📱 QR код: <b>{balances.get('qr', 0):,.2f}</b> ₽\n"
        f"🏢 Безналичные: <b>{balances.get('bn', 0):,.2f}</b> ₽\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💵 Всего: <b>{bal_total:,.2f}</b> ₽\n\n"
        f"➖ Расходы: <b>{exp:,.2f}</b> ₽\n"
        f"➕ Доходы: <b>{inc:,.2f}</b> ₽\n"
        f"🟰 За месяц: <b>{bal_month:,.2f}</b> ₽\n"
        f"💳 Мои долги: <b>{debts_i_owe:,.2f}</b> ₽\n"
        f"💰 Долги передо мной: <b>{debts_owe_me:,.2f}</b> ₽\n"
    ).replace(",", " ")
    
    # Добавляем последние 5 транзакций
    transactions = txs.get("transactions", [])
    if transactions:
        text += "\n<b>📋 Последние 5 операций:</b>\n\n"
        for tx in transactions[:5]:
            text += format_transaction(tx) + "\n"
    
    return text


async def main_screen_text_employee(user_id: int) -> str:
    """Получить текст главного экрана для сотрудника"""
    from datetime import datetime
    
    txs = await gas_request({"cmd": "get_last_transactions", "limit": 10}, user_id)
    
    now = datetime.now()
    date_str = now.strftime("%d %B %Y").replace(
        "January", "января"
    ).replace("February", "февраля").replace("March", "марта").replace(
        "April", "апреля"
    ).replace("May", "мая").replace("June", "июня").replace(
        "July", "июля"
    ).replace("August", "августа").replace("September", "сентября").replace(
        "October", "октября"
    ).replace("November", "ноября").replace("December", "декабря")
    
    text = (
        f"<b>💼 Касса детейлинг-студии</b>\n"
        f"{date_str}\n\n"
        f"<b>📋 Последние 10 операций:</b>\n\n"
    )
    
    transactions = txs.get("transactions", [])
    if transactions:
        for tx in transactions[:10]:
            text += format_transaction(tx) + "\n"
    else:
        text += "Пока нет операций"
    
    return text


async def get_categories(user_id: int) -> Dict[str, Any]:
    """Получить категории"""
    return await gas_request({"cmd": "get_categories"}, user_id)


# =========================
# Handlers
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    context.user_data.clear()
    
    user_id = update.effective_user.id
    
    if is_owner(user_id):
        txt = await main_screen_text_owner(user_id)
        kb = kb_main_owner()
    else:
        txt = await main_screen_text_employee(user_id)
        kb = kb_main_employee()
    
    await update.message.reply_text(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    
    return ST_MENU


async def on_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(DENY_TEXT)
        return ConversationHandler.END

    q = update.callback_query
    await q.answer()
    
    user_id = update.effective_user.id

    if q.data == "menu:add":
        await q.edit_message_text("Окей 🙂 Что вносим?", reply_markup=kb_choose_type())
        context.user_data["working_message_id"] = q.message.message_id
        return ST_ADD_CHOOSE_TYPE

    if q.data == "menu:analysis":
        if not is_owner(user_id):
            await q.answer("Доступ запрещён", show_alert=True)
            return ST_MENU
        await q.edit_message_text("📊 Анализ\n\nВыбери период:", reply_markup=kb_analysis_periods())
        context.user_data["working_message_id"] = q.message.message_id
        return ST_ANALYSIS_PERIOD

    if q.data == "menu:balance":
        if not is_owner(user_id):
            await q.answer("Доступ запрещён", show_alert=True)
            return ST_MENU
        
        balances = await gas_request({"cmd": "get_all_balances"}, user_id)
        
        text = (
            f"<b>💰 Текущие балансы:</b>\n\n"
            f"💵 Наличные: <b>{balances.get('cash', 0):,.2f}</b> ₽\n"
            f"📱 QR код: <b>{balances.get('qr', 0):,.2f}</b> ₽\n"
            f"🏢 Безналичные: <b>{balances.get('bn', 0):,.2f}</b> ₽"
        ).replace(",", " ")
        
        await q.edit_message_text(text, reply_markup=kb_balance_menu(), parse_mode=ParseMode.HTML)
        context.user_data["working_message_id"] = q.message.message_id
        return ST_MENU

    if q.data == "menu:debts":
        if not is_owner(user_id):
            await q.answer("Доступ запрещён", show_alert=True)
            return ST_MENU
        
        await q.edit_message_text("Какие долги?", reply_markup=kb_debts_type())
        context.user_data["working_message_id"] = q.message.message_id
        return ST_DEBTS_CHOOSE_TYPE

    return ST_MENU


async def back_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    user_id = update.effective_user.id

    if q.data == "back:menu":
        await delete_working_message(context, update.effective_chat.id)
        
        if is_owner(user_id):
            txt = await main_screen_text_owner(user_id)
            kb = kb_main_owner()
        else:
            txt = await main_screen_text_employee(user_id)
            kb = kb_main_employee()
        
        await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        return ST_MENU

    if q.data == "back:choose_type":
        await q.edit_message_text("Окей 🙂 Что вносим?", reply_markup=kb_choose_type())
        return ST_ADD_CHOOSE_TYPE

    if q.data == "back:exp_cat":
        user_id = update.effective_user.id
        categories = await get_categories(user_id)
        await q.edit_message_text("На что потратили? 💪", reply_markup=kb_expense_categories(categories["expenses"]))
        return ST_EXP_CATEGORY

    if q.data == "back:analysis_periods":
        await q.edit_message_text("📊 Анализ\n\nВыбери период:", reply_markup=kb_analysis_periods())
        return ST_ANALYSIS_PERIOD

    if q.data == "back:analysis_type":
        period_labels = {
            "today": "Сегодня",
            "week": "Эта неделя",
            "month": "Этот месяц",
            "year": "Этот год"
        }
        period = context.user_data.get("analysis_period", "month")
        period_label = period_labels.get(period, period)
        await q.edit_message_text(f"📊 {period_label}\n\nЧто посмотрим?", reply_markup=kb_analysis_type())
        return ST_ANALYSIS_TYPE

    if q.data == "back:debts_type":
        await q.edit_message_text("Какие долги?", reply_markup=kb_debts_type())
        return ST_DEBTS_CHOOSE_TYPE

    return ST_MENU


# ========== ВНЕСЕНИЕ ТРАНЗАКЦИИ ==========

async def choose_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    user_id = update.effective_user.id
    context.user_data.pop("tx", None)
    context.user_data["tx"] = {}
    
    categories = await get_categories(user_id)

    if q.data == "type:expense":
        context.user_data["categories"] = categories
        await q.edit_message_text("На что потратили? 💪", reply_markup=kb_expense_categories(categories["expenses"]))
        return ST_EXP_CATEGORY

    if q.data == "type:income":
        context.user_data["categories"] = categories
        await q.edit_message_text("Денежки! Откуда? 💰", reply_markup=kb_income_categories(categories["incomes"]))
        return ST_INC_CATEGORY

    return ST_ADD_CHOOSE_TYPE


async def expense_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    categories = context.user_data.get("categories", {}).get("expenses", [])
    idx = int(q.data.split(":")[1])
    cat = categories[idx]

    tx = context.user_data.get("tx", {})
    tx["type"] = "расход"
    tx["category"] = cat
    context.user_data["tx"] = tx

    prompt = "Сколько?\n\nПримеры: <code>2500</code>, <code>2 500</code>, <code>2.500</code>, <code>2500,50</code>, <code>2к</code>"
    await q.edit_message_text(prompt, parse_mode=ParseMode.HTML)
    return ST_AMOUNT


async def income_category(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    categories = context.user_data.get("categories", {}).get("incomes", [])
    idx = int(q.data.split(":")[1])
    cat = categories[idx]

    tx = context.user_data.get("tx", {})
    tx["type"] = "доход"
    tx["category"] = cat
    tx["payment_type"] = cat  # Для доходов категория = форма оплаты
    context.user_data["tx"] = tx

    prompt = "Сколько?\n\nПримеры: <code>2500</code>, <code>2 500</code>, <code>2.500</code>, <code>2500,50</code>, <code>2к</code>"
    await q.edit_message_text(prompt, parse_mode=ParseMode.HTML)
    return ST_AMOUNT


async def amount_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    amt = parse_amount(update.message.text)
    
    try:
        await update.message.delete()
    except Exception:
        pass
    
    if amt is None:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял сумму 🙈\nНапиши, пожалуйста, например: 2500 / 2 500 / 2500,50 / 2к"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_AMOUNT

    tx = context.user_data.get("tx", {})
    tx["amount"] = amt
    context.user_data["tx"] = tx

    work_msg_id = context.user_data.get("working_message_id")
    
    # Для расходов спрашиваем форму оплаты
    if tx.get("type") == "расход":
        categories = context.user_data.get("categories", {})
        payment_types = categories.get("payment_types", [])
        
        if work_msg_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=work_msg_id,
                    text="Откуда списываем?",
                    reply_markup=kb_payment_types(payment_types)
                )
            except Exception:
                pass
        
        return ST_PAYMENT_TYPE
    else:
        # Для доходов сразу спрашиваем комментарий
        if work_msg_id:
            try:
                category = tx.get("category", "")
                if category == "Услуги по БН":
                    text = "Напиши название Юр лица:"
                else:
                    text = "Напиши ФИО клиента или марку авто:"
                
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=work_msg_id,
                    text=text
                )
            except Exception:
                pass
        
        return ST_COMMENT


async def payment_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    categories = context.user_data.get("categories", {})
    payment_types = categories.get("payment_types", [])
    idx = int(q.data.split(":")[1])
    payment_type = payment_types[idx]

    tx = context.user_data.get("tx", {})
    tx["payment_type"] = payment_type
    context.user_data["tx"] = tx

    # Спрашиваем комментарий
    await q.edit_message_text("Добавишь коммент?", reply_markup=kb_skip_comment())
    return ST_COMMENT


async def comment_skip(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    tx = context.user_data.get("tx", {})
    tx["comment"] = ""
    context.user_data["tx"] = tx

    await save_and_finish_(update, context)
    return ST_MENU


async def comment_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    tx = context.user_data.get("tx", {})
    comment_text = (update.message.text or "").strip()
    
    # Проверка для доходов - комментарий обязателен
    if tx.get("type") == "доход" and not comment_text:
        await delete_working_message(context, update.effective_chat.id)
        
        category = tx.get("category", "")
        if category == "Услуги по БН":
            prompt = "Название Юр лица обязательно! Напиши:"
        else:
            prompt = "ФИО или марка авто обязательны! Напиши:"
        
        msg = await update.effective_chat.send_message(prompt)
        context.user_data["working_message_id"] = msg.message_id
        return ST_COMMENT
    
    tx["comment"] = comment_text
    context.user_data["tx"] = tx

    await save_and_finish_(update, context)
    return ST_MENU


async def save_and_finish_(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сохранить транзакцию и показать финальное подтверждение + главный экран"""
    
    await delete_working_message(context, update.effective_chat.id)
    
    user_id = update.effective_user.id
    tx = context.user_data.get("tx", {})
    
    payload = {
        "cmd": "add",
        "type": tx.get("type"),
        "category": tx.get("category"),
        "amount": tx.get("amount"),
        "payment_type": tx.get("payment_type"),
        "comment": tx.get("comment", "")
    }

    try:
        await gas_request(payload, user_id)
    except Exception as e:
        await update.effective_chat.send_message(f"Ошибка: {e}")
        if is_owner(user_id):
            txt = await main_screen_text_owner(user_id)
            kb = kb_main_owner()
        else:
            txt = await main_screen_text_employee(user_id)
            kb = kb_main_employee()
        await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if tx.get("type") == "расход":
        header = random.choice(PH_SAVED_EXPENSE)
        payment_type = tx.get("payment_type", "")
        detail = f"{tx.get('category')} — {tx.get('amount'):,.2f} ₽ — {payment_type}".replace(",", " ")
    else:
        header = random.choice(PH_SAVED_INCOME)
        detail = f"{tx.get('category')} — {tx.get('amount'):,.2f} ₽".replace(",", " ")

    comment = tx.get("comment", "").strip()
    if comment:
        detail += f"\n{comment}"

    await update.effective_chat.send_message(f"{header}\n{detail}")

    if is_owner(user_id):
        txt = await main_screen_text_owner(user_id)
        kb = kb_main_owner()
    else:
        txt = await main_screen_text_employee(user_id)
        kb = kb_main_employee()
    
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)


# ========== АНАЛИЗ ==========

async def analysis_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "aperiod:special":
        await q.edit_message_text("⚙️ Специальные отчеты", reply_markup=kb_special_reports())
        return ST_SPECIAL_REPORTS

    period = q.data.split(":")[1]
    context.user_data["analysis_period"] = period
    
    period_labels = {
        "today": "Сегодня",
        "week": "Эта неделя",
        "month": "Этот месяц",
        "year": "Этот год"
    }
    period_label = period_labels.get(period, period)
    
    await q.edit_message_text(f"📊 {period_label}\n\nЧто посмотрим?", reply_markup=kb_analysis_type())
    return ST_ANALYSIS_TYPE


async def analysis_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id
    period = context.user_data.get("analysis_period", "month")
    atype = q.data.split(":")[1]

    period_labels = {
        "today": "Сегодня",
        "week": "Эта неделя",
        "month": "Этот месяц",
        "year": "Этот год"
    }
    period_label = period_labels.get(period, period)

    await delete_working_message(context, update.effective_chat.id)

    if atype == "income":
        res = await gas_request({"cmd": "analysis_income", "period": period}, user_id)
        
        total = res.get("total", 0)
        by_type = res.get("by_type", {})
        
        text = f"<b>💰 Поступления за {period_label.lower()}</b>\n\n"
        
        if total > 0:
            for payment_type, amount in by_type.items():
                percentage = (amount / total) * 100
                emoji = "💵" if payment_type == "Наличные" else ("📱" if payment_type == "QR код" else "🏢")
                text += f"{emoji} {payment_type}: <b>{amount:,.0f}</b> ₽ ({percentage:.0f}%)\n"
            text += f"━━━━━━━━━━━━━━━━\nИтого: <b>{total:,.0f}</b> ₽"
        else:
            text += "Нет данных"
        
        text = text.replace(",", " ")
        
    else:  # expense
        res = await gas_request({"cmd": "analysis_expense", "period": period}, user_id)
        
        total = res.get("total", 0)
        by_category = res.get("by_category", {})
        
        text = f"<b>💸 Затраты за {period_label.lower()}</b>\n\n"
        
        if total > 0:
            for category, amount in by_category.items():
                text += f"{category}: <b>{amount:,.0f}</b> ₽\n"
            text += f"━━━━━━━━━━━━━━━━\nИтого: <b>{total:,.0f}</b> ₽"
        else:
            text += "Нет данных"
        
        text = text.replace(",", " ")

    await update.effective_chat.send_message(text, parse_mode=ParseMode.HTML)
    
    if is_owner(user_id):
        txt = await main_screen_text_owner(user_id)
        kb = kb_main_owner()
    else:
        txt = await main_screen_text_employee(user_id)
        kb = kb_main_employee()
    
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    
    return ST_MENU


async def special_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id

    await delete_working_message(context, update.effective_chat.id)

    if q.data == "special:compare":
        res = await gas_request({"cmd": "compare_months"}, user_id)
        
        year = res.get("year", 2026)
        months = res.get("months", [])
        
        text = f"<b>📊 Сравнение месяцев ({year})</b>\n\n"
        
        for i, month_data in enumerate(months):
            month_name = month_data.get("month", "")
            incomes = month_data.get("incomes", 0)
            expenses = month_data.get("expenses", 0)
            
            text += f"<b>{month_name}:</b>\n"
            text += f"💰 Выручка: <b>{incomes:,.0f}</b> ₽"
            
            if i > 0:
                prev_incomes = months[i-1].get("incomes", 0)
                if prev_incomes > 0:
                    change = ((incomes - prev_incomes) / prev_incomes) * 100
                    sign = "+" if change >= 0 else ""
                    text += f" ({sign}{change:.0f}%)"
            
            text += f"\n💸 Затраты: <b>{expenses:,.0f}</b> ₽"
            
            if i > 0:
                prev_expenses = months[i-1].get("expenses", 0)
                if prev_expenses > 0:
                    change = ((expenses - prev_expenses) / prev_expenses) * 100
                    sign = "+" if change >= 0 else ""
                    text += f" ({sign}{change:.0f}%)"
            
            text += "\n\n"
        
        text = text.replace(",", " ")

    elif q.data == "special:average":
        res = await gas_request({"cmd": "average_check"}, user_id)
        
        month_data = res.get("month", {})
        year_data = res.get("year", {})
        
        text = "<b>💰 Средний чек</b>\n\n"
        text += f"<b>За {month_data.get('month_label', 'месяц')}:</b>\n"
        text += f"Средний чек: <b>{month_data.get('average', 0):,.0f}</b> ₽\n"
        text += f"Операций: {month_data.get('count', 0)}\n\n"
        text += f"<b>За {year_data.get('year_label', 'год')} год:</b>\n"
        text += f"Средний чек: <b>{year_data.get('average', 0):,.0f}</b> ₽\n"
        text += f"Операций: {year_data.get('count', 0)}"
        
        text = text.replace(",", " ")

    elif q.data == "special:top":
        res = await gas_request({"cmd": "top_expenses"}, user_id)
        
        month_label = res.get("month_label", "месяц")
        total = res.get("total", 0)
        categories = res.get("categories", [])
        
        text = f"<b>📋 Топ категорий затрат ({month_label})</b>\n\n"
        
        if categories:
            for i, cat_data in enumerate(categories, 1):
                category = cat_data.get("category", "")
                amount = cat_data.get("amount", 0)
                text += f"{i}. {category}: <b>{amount:,.0f}</b> ₽\n"
            text += f"━━━━━━━━━━━━━━━━\nИтого: <b>{total:,.0f}</b> ₽"
        else:
            text += "Нет данных"
        
        text = text.replace(",", " ")

    await update.effective_chat.send_message(text, parse_mode=ParseMode.HTML)
    
    if is_owner(user_id):
        txt = await main_screen_text_owner(user_id)
        kb = kb_main_owner()
    else:
        txt = await main_screen_text_employee(user_id)
        kb = kb_main_employee()
    
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    
    return ST_MENU


# ========== БАЛАНС ==========

async def balance_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    payment_type = q.data.split(":")[1]
    context.user_data["balance_payment_type"] = payment_type
    
    labels = {
        "cash": "наличных",
        "qr": "QR счета",
        "bn": "безналичного счета"
    }
    label = labels.get(payment_type, "")

    await q.edit_message_text(
        f"Какой у тебя баланс {label}? 💰\n\n"
        f"Напиши сумму (например: 50000 или 50к)",
        parse_mode=ParseMode.HTML
    )
    context.user_data["working_message_id"] = q.message.message_id
    return ST_BALANCE_EDIT


async def balance_edit_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    user_id = update.effective_user.id
    amt = parse_amount(update.message.text)
    if amt is None or amt < 0:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял сумму 🙈\nНапиши, пожалуйста, например: 50000 / 50 000 / 50к"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_BALANCE_EDIT

    payment_type = context.user_data.get("balance_payment_type", "cash")
    await gas_request({"cmd": "set_balance", "amount": amt, "payment_type": payment_type}, user_id)

    await delete_working_message(context, update.effective_chat.id)

    labels = {
        "cash": "наличных",
        "qr": "QR счета",
        "bn": "безналичного счета"
    }
    label = labels.get(payment_type, "")

    await update.effective_chat.send_message(
        f"Отлично! ✅ Баланс {label} установлен: <b>{amt:,.2f}</b> ₽".replace(",", " "),
        parse_mode=ParseMode.HTML
    )
    
    txt = await main_screen_text_owner(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb_main_owner(), parse_mode=ParseMode.HTML)
    
    return ST_MENU


# ========== ДОЛГИ ==========

async def debts_choose_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    user_id = update.effective_user.id
    debt_type = q.data.split(":")[1]
    context.user_data["debt_type"] = debt_type

    debts = await gas_request({"cmd": "get_debts", "debt_type": debt_type}, user_id)
    debt_amount = debts.get("debts", 0)
    
    debt_label = "Долги передо мной" if debt_type == "owe_me" else "Мои долги"
    
    text = f"{debt_label}:\n<b>{debt_amount:,.2f}</b> ₽".replace(",", " ")
    await q.edit_message_text(text, reply_markup=kb_debts_actions(), parse_mode=ParseMode.HTML)
    
    return ST_DEBTS_CHOOSE_TYPE


async def debts_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    debt_type = context.user_data.get("debt_type", "i_owe")
    debt_label = "долгов передо мной" if debt_type == "owe_me" else "долгов"

    await q.edit_message_text(
        f"Сколько у тебя {debt_label}? 💳\n\n"
        f"Напиши сумму (например: 10000 или 10к)",
        parse_mode=ParseMode.HTML
    )
    context.user_data["working_message_id"] = q.message.message_id
    return ST_DEBTS_EDIT


async def debts_edit_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    user_id = update.effective_user.id
    amt = parse_amount(update.message.text)
    if amt is None or amt < 0:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял сумму 🙈\nНапиши, пожалуйста, например: 10000 / 10 000 / 10к"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_DEBTS_EDIT

    debt_type = context.user_data.get("debt_type", "i_owe")
    
    await gas_request({"cmd": "set_debts", "amount": amt, "debt_type": debt_type}, user_id)

    await delete_working_message(context, update.effective_chat.id)

    debt_label = "Долги передо мной" if debt_type == "owe_me" else "Мои долги"
    
    await update.effective_chat.send_message(
        f"Отлично! ✅ {debt_label} установлены: <b>{amt:,.2f}</b> ₽".replace(",", " "),
        parse_mode=ParseMode.HTML
    )
    
    txt = await main_screen_text_owner(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb_main_owner(), parse_mode=ParseMode.HTML)
    
    return ST_MENU


# ========== HELP & ERROR ==========

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return
    await update.message.reply_text(
        "Кнопки внизу 🙂\n"
        "• Внести транзакцию\n"
        "• Анализ (только для владельцев)\n"
        "• Сверить баланс (только для владельцев)\n"
        "• Долги (только для владельцев)"
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("Ой, что-то пошло не так 🙈 Попробуем ещё раз?")
    except Exception:
        pass


def build_app() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            ST_MENU: [
                CallbackQueryHandler(on_menu, pattern=r"^menu:"),
                CallbackQueryHandler(balance_edit_start, pattern=r"^balance:(cash|qr|bn)$"),
            ],
            ST_ADD_CHOOSE_TYPE: [
                CallbackQueryHandler(choose_type, pattern=r"^type:"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_EXP_CATEGORY: [
                CallbackQueryHandler(expense_category, pattern=r"^expcat:\d+$"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_INC_CATEGORY: [
                CallbackQueryHandler(income_category, pattern=r"^inccat:\d+$"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, amount_received),
            ],
            ST_PAYMENT_TYPE: [
                CallbackQueryHandler(payment_type_selected, pattern=r"^payment:\d+$"),
            ],
            ST_COMMENT: [
                CallbackQueryHandler(comment_skip, pattern=r"^comment:skip$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, comment_received),
            ],
            ST_ANALYSIS_PERIOD: [
                CallbackQueryHandler(analysis_period, pattern=r"^aperiod:"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_ANALYSIS_TYPE: [
                CallbackQueryHandler(analysis_type, pattern=r"^atype:"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_SPECIAL_REPORTS: [
                CallbackQueryHandler(special_reports, pattern=r"^special:"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_BALANCE_EDIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, balance_edit_received),
            ],
            ST_DEBTS_CHOOSE_TYPE: [
                CallbackQueryHandler(debts_choose_type, pattern=r"^debts_type:"),
                CallbackQueryHandler(debts_edit_start, pattern=r"^debts:edit$"),
                CallbackQueryHandler(back_router, pattern=r"^back:"),
            ],
            ST_DEBTS_EDIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debts_edit_received),
            ],
        },
        fallbacks=[CommandHandler("help", cmd_help)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_error_handler(error_handler)
    return app


def run():
    app = build_app()

    if WEBHOOK_URL:
        url_path = WEBHOOK_PATH or _default_webhook_path()
        full_webhook = f"{WEBHOOK_URL.rstrip('/')}/{url_path}"

        logger.info("Starting webhook on 0.0.0.0:%s", PORT)

        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=url_path,
            webhook_url=full_webhook,
            allowed_updates=Update.ALL_TYPES,
        )
    else:
        logger.info("Starting polling")
        app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    run()
```

---

## requirements.txt (тот же):
```
python-telegram-bot[webhooks]==21.6
aiohttp==3.10.10
```
