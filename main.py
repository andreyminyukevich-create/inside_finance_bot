import os
import re
import json
import random
import asyncio
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

BOT_TOKEN   = os.getenv("BOT_TOKEN", "").strip()
SCRIPT_URL  = os.getenv("SCRIPT_URL", "").strip()
WEBHOOK_URL  = os.getenv("WEBHOOK_URL", "").strip()
PORT         = int(os.getenv("PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "").strip()

# Единый аккаунт студии — для всех балансовых операций
STUDIO_ACCOUNT_ID = int(os.getenv("STUDIO_ACCOUNT_ID", "419675968"))

def _parse_ids(env_var: str) -> List[int]:
    val = os.getenv(env_var, "").strip()
    return [int(x.strip()) for x in val.split(",") if x.strip()] if val else []

OWNER_IDS = _parse_ids("OWNER_IDS")
ADMIN_IDS = _parse_ids("ADMIN_IDS")

# Общий allowlist
USER_TG_IDS = list(set(OWNER_IDS + ADMIN_IDS))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is missing")
if not SCRIPT_URL:
    raise RuntimeError("SCRIPT_URL is missing")
if not USER_TG_IDS:
    raise RuntimeError("OWNER_IDS и ADMIN_IDS не заданы")


def _default_webhook_path() -> str:
    h = hashlib.sha256(BOT_TOKEN.encode("utf-8")).hexdigest()
    return f"tg/{h[:24]}"


# ========================================
# РОЛИ
# ========================================
def get_role(user_id: int) -> str:
    """Возвращает 'owner' | 'admin'"""
    if user_id in OWNER_IDS:
        return "owner"
    return "admin"


def is_owner(user_id: int) -> bool:
    return user_id in OWNER_IDS


def is_allowed(update: Update) -> bool:
    user = update.effective_user
    if not user:
        return False
    return user.id in USER_TG_IDS


# ========================================
# ФРАЗЫ
# ========================================
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

# ========================================
# СОСТОЯНИЯ
# ========================================
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
    ST_DEBTS_SELECT,
    ST_DEBTS_AMOUNT,
    ST_DEBTS_ADD_NAME,
    ST_DEBTS_ADD_AMOUNT,
    ST_DEBTS_ADD_PAYMENT,
    ST_DEBTS_ADD_COMMENT,
    ST_FILM_CLIENT,
    ST_FILM_METERS,
    ST_FILM_AMOUNT,
    ST_FILM_PAYMENT,
) = range(22)


# ========================================
# ВСПОМОГАТЕЛЬНЫЕ
# ========================================
async def delete_working_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    msg_id = context.user_data.get("working_message_id")
    if msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"Couldn't delete message {msg_id}: {e}")
    context.user_data["working_message_id"] = None


# ========================================
# КЛАВИАТУРЫ
# ========================================
def kb_main_owner() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Внести транзакцию",    callback_data="menu:add")],
        [InlineKeyboardButton("📦 Продал пленку",         callback_data="menu:film")],
        [InlineKeyboardButton("💰 Долги перед Inside",    callback_data="menu:debts_owe_us")],
        [InlineKeyboardButton("💳 Долги Inside",          callback_data="menu:debts_we_owe")],
        [InlineKeyboardButton("📊 Анализ",               callback_data="menu:analysis")],
        [InlineKeyboardButton("⚙️ Корректировать баланс", callback_data="menu:balance")],
    ])


def kb_main_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Внести транзакцию", callback_data="menu:add")],
        [InlineKeyboardButton("📦 Продал пленку",     callback_data="menu:film")],
        [InlineKeyboardButton("💰 Долги перед Inside", callback_data="menu:debts_owe_us")],
    ])


def kb_choose_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("➖ Затраты",  callback_data="type:expense")],
        [InlineKeyboardButton("➕ Доход",    callback_data="type:income")],
        [InlineKeyboardButton("⬅️ Назад",    callback_data="back:menu")],
    ])


def kb_expense_categories(categories: List[str]) -> InlineKeyboardMarkup:
    rows, row = [], []
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
    rows = [[InlineKeyboardButton(c, callback_data=f"inccat:{i}")] for i, c in enumerate(categories)]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="back:choose_type")])
    return InlineKeyboardMarkup(rows)


def kb_payment_types(payment_types: List[str]) -> InlineKeyboardMarkup:
    rows = []
    for i, p in enumerate(payment_types):
        emoji = "💵" if p == "Наличные" else "🏢"
        rows.append([InlineKeyboardButton(f"{emoji} {p}", callback_data=f"payment:{i}")])
    return InlineKeyboardMarkup(rows)


def kb_skip_comment() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Пропустить", callback_data="comment:skip")],
    ])


def kb_analysis_periods() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Сегодня",              callback_data="aperiod:today")],
        [InlineKeyboardButton("📅 Эта неделя",           callback_data="aperiod:week")],
        [InlineKeyboardButton("📅 Этот месяц",           callback_data="aperiod:month")],
        [InlineKeyboardButton("📅 Этот год",             callback_data="aperiod:year")],
        [InlineKeyboardButton("⚙️ Специальные отчеты",  callback_data="aperiod:special")],
        [InlineKeyboardButton("⬅️ Назад",                callback_data="back:menu")],
    ])


def kb_analysis_type() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Поступления",  callback_data="atype:income")],
        [InlineKeyboardButton("💸 Затраты",      callback_data="atype:expense")],
        [InlineKeyboardButton("⬅️ Назад",        callback_data="back:analysis_periods")],
    ])


def kb_special_reports() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Сравнение месяцев",    callback_data="special:compare")],
        [InlineKeyboardButton("💰 Средний чек",          callback_data="special:average")],
        [InlineKeyboardButton("📋 Топ категорий затрат", callback_data="special:top")],
        [InlineKeyboardButton("⬅️ Назад",                callback_data="back:analysis_periods")],
    ])


def kb_balance_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💵 Корректировать наличные", callback_data="balance:cash")],
        [InlineKeyboardButton("🏢 Корректировать БН",       callback_data="balance:bn")],
        [InlineKeyboardButton("⬅️ Назад",                   callback_data="back:menu")],
    ])


def kb_debtors_list(debtors: List[Dict], owner_mode: bool = True) -> InlineKeyboardMarkup:
    rows = []
    for debtor in debtors:
        name   = debtor["name"]
        amount = f"{debtor['amount']:,.0f}".replace(",", " ")
        rows.append([InlineKeyboardButton(
            f"{name} — {amount} ₽",
            callback_data=f"debtor:{debtor['id']}"
        )])
    if owner_mode:
        rows.append([InlineKeyboardButton("➕ Внести долг", callback_data="debts:add")])
    rows.append([InlineKeyboardButton("⬅️ В главное меню", callback_data="back:menu")])
    return InlineKeyboardMarkup(rows)


def kb_debtor_actions(owner_mode: bool = True) -> InlineKeyboardMarkup:
    rows = []
    if owner_mode:
        rows.append([InlineKeyboardButton("✏️ Изменить сумму", callback_data="debtor:edit")])
        rows.append([InlineKeyboardButton("🗑 Удалить",         callback_data="debtor:delete")])
    # Admin может закрыть долг (изменить сумму)
    else:
        rows.append([InlineKeyboardButton("✏️ Закрыть / изменить", callback_data="debtor:edit")])
    rows.append([InlineKeyboardButton("⬅️ Назад к списку", callback_data="back:debtors_list")])
    return InlineKeyboardMarkup(rows)


def kb_film_payment() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💵 Оплачено наличными", callback_data="film_payment:cash")],
        [InlineKeyboardButton("🏢 Оплачено БН",        callback_data="film_payment:bn")],
        [InlineKeyboardButton("📋 В долг",             callback_data="film_payment:debt")],
        [InlineKeyboardButton("⬅️ Назад",              callback_data="back:menu")],
    ])


def kb_debt_payment() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💵 Наличные",     callback_data="debt_payment:cash")],
        [InlineKeyboardButton("🏢 БН (QR и счёт)", callback_data="debt_payment:bn")],
    ])


# ========================================
# ПАРСИНГ СУММЫ
# ========================================
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
    has_dot   = "." in s
    if has_comma and has_dot:
        last_comma = s.rfind(",")
        last_dot   = s.rfind(".")
        dec_pos    = max(last_comma, last_dot)
        int_part   = re.sub(r"[.,]", "", s[:dec_pos])
        frac_part  = re.sub(r"[.,]", "", s[dec_pos + 1:])
        s = f"{int_part}.{frac_part}"
    elif has_comma and not has_dot:
        s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        val = float(s) * mult
        return None if val < 0 else round(val, 2)
    except Exception:
        return None


# ========================================
# GAS ЗАПРОСЫ (persistent session + 3 retries + 30s timeout)
# ========================================
_gas_session: Optional[aiohttp.ClientSession] = None


async def _get_gas_session() -> aiohttp.ClientSession:
    """Возвращает persistent ClientSession, создавая её при первом вызове."""
    global _gas_session
    if _gas_session is None or _gas_session.closed:
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
        _gas_session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        logger.info("Created new GAS session")
    return _gas_session


async def _close_gas_session():
    """Закрывает GAS session при остановке бота."""
    global _gas_session
    if _gas_session is not None and not _gas_session.closed:
        await _gas_session.close()
        logger.info("Closed GAS session")


async def gas_request(payload: Dict[str, Any], user_id: int) -> Dict[str, Any]:
    """
    Отправляет запрос в GAS с автоматическими ретраями.
    - 3 попытки с задержкой 1.5с / 3с
    - Ретрай на: timeout, 5xx, network errors
    - Без ретрая на: 4xx, GAS вернул ok=false (бизнес-ошибка)
    """
    payload = dict(payload)
    payload["user_id"]    = user_id
    payload["actor_id"]   = user_id
    payload["account_id"] = STUDIO_ACCOUNT_ID

    session = await _get_gas_session()
    max_attempts = 3
    last_error: Optional[str] = None

    for attempt in range(1, max_attempts + 1):
        try:
            async with session.post(SCRIPT_URL, json=payload) as resp:
                # 5xx — повторяем
                if resp.status >= 500:
                    last_error = f"HTTP {resp.status}"
                    logger.warning(
                        "GAS attempt %d/%d failed: %s (cmd=%s)",
                        attempt, max_attempts, last_error, payload.get("cmd")
                    )
                    if attempt < max_attempts:
                        await asyncio.sleep(1.5 * attempt)
                        continue
                    raise RuntimeError(f"GAS вернул {last_error} после {max_attempts} попыток")

                # 4xx — не повторяем, что-то не так с запросом
                if resp.status >= 400:
                    txt = await resp.text()
                    logger.error("GAS HTTP %d (cmd=%s): %s", resp.status, payload.get("cmd"), txt[:500])
                    raise RuntimeError(f"GAS HTTP {resp.status}")

                txt = await resp.text()
                try:
                    data = json.loads(txt)
                except Exception:
                    logger.error("GAS non-json response (cmd=%s): %s", payload.get("cmd"), txt[:500])
                    raise RuntimeError("GAS вернул не-JSON ответ")

                if not data.get("ok"):
                    # Бизнес-ошибка GAS — не ретраим
                    raise RuntimeError(data.get("error") or "GAS error")

                if attempt > 1:
                    logger.info("GAS recovered on attempt %d (cmd=%s)", attempt, payload.get("cmd"))
                return data["data"]

        except asyncio.TimeoutError:
            last_error = "timeout"
            logger.warning(
                "GAS timeout, attempt %d/%d (cmd=%s)",
                attempt, max_attempts, payload.get("cmd")
            )
            if attempt < max_attempts:
                await asyncio.sleep(1.5 * attempt)
                continue
            raise RuntimeError("GAS не ответил за 30 сек (3 попытки)")

        except aiohttp.ClientError as e:
            last_error = str(e)
            logger.warning(
                "GAS network error: %s, attempt %d/%d (cmd=%s)",
                e, attempt, max_attempts, payload.get("cmd")
            )
            if attempt < max_attempts:
                await asyncio.sleep(1.5 * attempt)
                continue
            raise RuntimeError(f"Сетевая ошибка GAS: {e}")

    # Сюда не должны добираться, но на всякий
    raise RuntimeError(last_error or "Unknown GAS error")


# ========================================
# ФОРМАТИРОВАНИЕ ТРАНЗАКЦИЙ
# ========================================
def format_transaction(tx: Dict) -> str:
    type_emoji = "➕" if tx["type"] == "доход" else "➖"
    amount_str = f"{tx['amount']:,.0f} ₽".replace(",", " ")
    if tx["type"] == "доход":
        comment  = tx.get("comment", "")
        category = tx.get("category", "")
        return f"{type_emoji} {amount_str} — {comment} — {category}"
    else:
        category     = tx.get("category", "")
        payment_type = tx.get("payment_type", "")
        comment      = tx.get("comment", "")
        if comment:
            return f"{type_emoji} {amount_str} — {category} — {payment_type} — {comment}"
        return f"{type_emoji} {amount_str} — {category} — {payment_type}"


# ========================================
# ГЛАВНЫЕ ЭКРАНЫ
# ========================================
async def main_screen_text_owner(user_id: int) -> str:
    s = await gas_request({"cmd": "get_main_screen", "view": "owner", "limit": 5}, user_id)

    month      = s.get("month_label", "Текущий месяц")
    exp        = s.get("expenses", 0)
    inc        = s.get("incomes", 0)
    bal_month  = s.get("balance_month", 0)
    balances   = s.get("balances", {})
    bal_total  = s.get("balance_total", 0)
    debts      = s.get("debts", {})

    cash_balance = balances.get("cash", 0)
    bn_balance   = balances.get("bn", 0)

    owe_us_cash = debts.get("owe_us_cash", 0)
    owe_us_bn   = debts.get("owe_us_bn", 0)
    we_owe_cash = debts.get("we_owe_cash", 0)
    we_owe_bn   = debts.get("we_owe_bn", 0)

    cash_with_debts  = cash_balance + owe_us_cash - we_owe_cash
    bn_with_debts    = bn_balance   + owe_us_bn   - we_owe_bn
    total_with_debts = cash_with_debts + bn_with_debts

    text = (
        f"<b>💼 Бизнес</b>\n"
        f"<b>{month}</b>\n\n"
        f"<b>💰 Баланс:</b>\n"
        f"💵 Наличные: <b>{cash_balance:,.2f}</b> ₽\n"
        f"🏢 БН (QR и счёт): <b>{bn_balance:,.2f}</b> ₽\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💵 Всего: <b>{bal_total:,.2f}</b> ₽\n\n"
        f"<b>💰 Баланс с учётом долгов:</b>\n"
        f"💵 Наличные: <b>{cash_with_debts:,.2f}</b> ₽\n"
        f"🏢 БН (QR и счёт): <b>{bn_with_debts:,.2f}</b> ₽\n"
        f"━━━━━━━━━━━━━━━━\n"
        f"💵 Всего: <b>{total_with_debts:,.2f}</b> ₽\n\n"
        f"➖ Расходы: <b>{exp:,.2f}</b> ₽\n"
        f"➕ Доходы: <b>{inc:,.2f}</b> ₽\n"
        f"🟰 За месяц: <b>{bal_month:,.2f}</b> ₽\n"
    ).replace(",", " ")

    transactions = s.get("transactions", [])
    if transactions:
        text += "\n<b>📋 Последние 5 операций:</b>\n\n"
        for tx in transactions[:5]:
            text += format_transaction(tx) + "\n"

    return text


async def main_screen_text_admin(user_id: int) -> str:
    s = await gas_request({"cmd": "get_main_screen", "view": "admin", "limit": 10}, user_id)

    month        = s.get("month_label", "Текущий месяц")
    month_income = s.get("month_income", 0)
    checks_count = s.get("checks_count", 0)

    text = (
        f"<b>💼 Касса детейлинг-студии</b>\n"
        f"<b>{month}</b>\n\n"
        f"🧾 Чеков за месяц: <b>{checks_count}</b>\n"
        f"➕ Оборот: <b>{month_income:,.2f}</b> ₽\n"
    ).replace(",", " ")

    transactions = s.get("transactions", [])
    if transactions:
        text += "\n<b>📋 Последние 10 твоих операций:</b>\n\n"
        for tx in transactions[:10]:
            text += format_transaction(tx) + "\n"
    else:
        text += "\nПока нет операций"

    return text


async def get_main_screen(user_id: int):
    """Возвращает (text, keyboard) в зависимости от роли."""
    if is_owner(user_id):
        txt = await main_screen_text_owner(user_id)
        kb  = kb_main_owner()
    else:
        txt = await main_screen_text_admin(user_id)
        kb  = kb_main_admin()
    return txt, kb


async def get_categories(user_id: int) -> Dict[str, Any]:
    return await gas_request({"cmd": "get_categories"}, user_id)


# ========================================
# HANDLERS: START / MENU
# ========================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    context.user_data.clear()
    user_id = update.effective_user.id
    txt, kb = await get_main_screen(user_id)
    await update.message.reply_text(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


async def on_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(DENY_TEXT)
        return ConversationHandler.END

    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id
    role    = get_role(user_id)

    if q.data == "menu:add":
        await q.edit_message_text("Окей 🙂 Что вносим?", reply_markup=kb_choose_type())
        context.user_data["working_message_id"] = q.message.message_id
        return ST_ADD_CHOOSE_TYPE

    if q.data == "menu:film":
        context.user_data.pop("film", None)
        context.user_data["film"] = {}
        await q.edit_message_text("📦 Продал пленку\n\nКому продали? Напиши имя клиента:")
        context.user_data["working_message_id"] = q.message.message_id
        return ST_FILM_CLIENT

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
            f"<b>⚙️ Корректировать баланс</b>\n\n"
            f"Текущие значения:\n"
            f"💵 Наличные: <b>{balances.get('cash', 0):,.2f}</b> ₽\n"
            f"🏢 БН (QR и счёт): <b>{balances.get('bn', 0):,.2f}</b> ₽\n\n"
            f"Установи новое базовое значение баланса.\n"
            f"Все последующие транзакции будут изменять его."
        ).replace(",", " ")
        await q.edit_message_text(text, reply_markup=kb_balance_menu(), parse_mode=ParseMode.HTML)
        context.user_data["working_message_id"] = q.message.message_id
        return ST_MENU

    if q.data == "menu:debts_owe_us":
        debt_type = "owe_me"
        context.user_data["debt_type"] = debt_type
        debtors = await gas_request({"cmd": "get_debtors_list", "debt_type": debt_type}, user_id)

        text = "<b>💰 Долги перед Inside</b>\n\n"
        if debtors.get("debtors"):
            for d in debtors["debtors"]:
                text += f"• {d['name']}: <b>{d['amount']:,.2f}</b> ₽\n"
            text += f"\n━━━━━━━━━━━━━━━━\nВсего: <b>{debtors.get('total', 0):,.2f}</b> ₽"
        else:
            text += "Список пуст"
        text = text.replace(",", " ")

        owner_mode = is_owner(user_id)
        await q.edit_message_text(
            text,
            reply_markup=kb_debtors_list(debtors.get("debtors", []), owner_mode),
            parse_mode=ParseMode.HTML
        )
        context.user_data["working_message_id"] = q.message.message_id
        return ST_DEBTS_SELECT

    if q.data == "menu:debts_we_owe":
        if not is_owner(user_id):
            await q.answer("Доступ запрещён", show_alert=True)
            return ST_MENU
        debt_type = "i_owe"
        context.user_data["debt_type"] = debt_type
        debtors = await gas_request({"cmd": "get_debtors_list", "debt_type": debt_type}, user_id)

        text = "<b>💳 Долги Inside</b>\n\n"
        if debtors.get("debtors"):
            for d in debtors["debtors"]:
                text += f"• {d['name']}: <b>{d['amount']:,.2f}</b> ₽\n"
            text += f"\n━━━━━━━━━━━━━━━━\nВсего: <b>{debtors.get('total', 0):,.2f}</b> ₽"
        else:
            text += "Список пуст"
        text = text.replace(",", " ")

        await q.edit_message_text(
            text,
            reply_markup=kb_debtors_list(debtors.get("debtors", []), True),
            parse_mode=ParseMode.HTML
        )
        context.user_data["working_message_id"] = q.message.message_id
        return ST_DEBTS_SELECT

    return ST_MENU


# ========================================
# BACK ROUTER
# ========================================
async def back_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    if q.data == "back:menu":
        await delete_working_message(context, update.effective_chat.id)
        txt, kb = await get_main_screen(user_id)
        await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        return ST_MENU

    if q.data == "back:choose_type":
        await q.edit_message_text("Окей 🙂 Что вносим?", reply_markup=kb_choose_type())
        return ST_ADD_CHOOSE_TYPE

    if q.data == "back:exp_cat":
        categories = await get_categories(user_id)
        await q.edit_message_text(
            "На что потратили? 💪",
            reply_markup=kb_expense_categories(categories["expenses"])
        )
        return ST_EXP_CATEGORY

    if q.data == "back:analysis_periods":
        await q.edit_message_text("📊 Анализ\n\nВыбери период:", reply_markup=kb_analysis_periods())
        return ST_ANALYSIS_PERIOD

    if q.data == "back:analysis_type":
        period_labels = {"today": "Сегодня", "week": "Эта неделя", "month": "Этот месяц", "year": "Этот год"}
        period       = context.user_data.get("analysis_period", "month")
        period_label = period_labels.get(period, period)
        await q.edit_message_text(f"📊 {period_label}\n\nЧто посмотрим?", reply_markup=kb_analysis_type())
        return ST_ANALYSIS_TYPE

    if q.data == "back:debtors_list":
        debt_type = context.user_data.get("debt_type", "owe_me")
        debtors   = await gas_request({"cmd": "get_debtors_list", "debt_type": debt_type}, user_id)

        if debt_type == "owe_me":
            text = "<b>💰 Долги перед Inside</b>\n\n"
        else:
            text = "<b>💳 Долги Inside</b>\n\n"

        if debtors.get("debtors"):
            for d in debtors["debtors"]:
                text += f"• {d['name']}: <b>{d['amount']:,.2f}</b> ₽\n"
            text += f"\n━━━━━━━━━━━━━━━━\nВсего: <b>{debtors.get('total', 0):,.2f}</b> ₽"
        else:
            text += "Список пуст"
        text = text.replace(",", " ")

        owner_mode = is_owner(user_id)
        await q.edit_message_text(
            text,
            reply_markup=kb_debtors_list(debtors.get("debtors", []), owner_mode),
            parse_mode=ParseMode.HTML
        )
        return ST_DEBTS_SELECT

    return ST_MENU


# ========================================
# ТРАНЗАКЦИИ
# ========================================
async def choose_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = update.effective_user.id
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
    tx  = context.user_data.get("tx", {})
    tx["type"]     = "расход"
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
    tx  = context.user_data.get("tx", {})
    tx["type"]         = "доход"
    tx["category"]     = cat
    tx["payment_type"] = cat
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

    if tx.get("type") == "расход":
        categories    = context.user_data.get("categories", {})
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
        return ST_EXP_PAYMENT_TYPE
    else:
        if work_msg_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=update.effective_chat.id,
                    message_id=work_msg_id,
                    text="Напиши ФИО клиента или марку авто:"
                )
            except Exception:
                pass
        return ST_COMMENT


async def payment_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    categories    = context.user_data.get("categories", {})
    payment_types = categories.get("payment_types", [])
    idx           = int(q.data.split(":")[1])
    payment_type  = payment_types[idx]
    tx = context.user_data.get("tx", {})
    tx["payment_type"] = payment_type
    context.user_data["tx"] = tx
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

    tx           = context.user_data.get("tx", {})
    comment_text = (update.message.text or "").strip()

    if tx.get("type") == "доход" and not comment_text:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message("ФИО или марка авто обязательны! Напиши:")
        context.user_data["working_message_id"] = msg.message_id
        return ST_COMMENT

    tx["comment"] = comment_text
    context.user_data["tx"] = tx
    await save_and_finish_(update, context)
    return ST_MENU


async def save_and_finish_(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await delete_working_message(context, update.effective_chat.id)
    user_id = update.effective_user.id
    tx      = context.user_data.get("tx", {})

    payload = {
        "cmd":          "add",
        "type":         tx.get("type"),
        "category":     tx.get("category"),
        "amount":       tx.get("amount"),
        "payment_type": tx.get("payment_type"),
        "comment":      tx.get("comment", "")
    }

    try:
        await gas_request(payload, user_id)
    except Exception as e:
        await update.effective_chat.send_message(f"Ошибка: {e}")
        txt, kb = await get_main_screen(user_id)
        await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        return

    if tx.get("type") == "расход":
        header       = random.choice(PH_SAVED_EXPENSE)
        payment_type = tx.get("payment_type", "")
        detail       = f"{tx.get('category')} — {tx.get('amount'):,.2f} ₽ — {payment_type}".replace(",", " ")
    else:
        header = random.choice(PH_SAVED_INCOME)
        detail = f"{tx.get('category')} — {tx.get('amount'):,.2f} ₽".replace(",", " ")

    comment = tx.get("comment", "").strip()
    if comment:
        detail += f"\n{comment}"

    await update.effective_chat.send_message(f"{header}\n{detail}")

    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)


# ========================================
# АНАЛИЗ (только owner)
# ========================================
async def analysis_period(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    if q.data == "aperiod:special":
        await q.edit_message_text("⚙️ Специальные отчеты", reply_markup=kb_special_reports())
        return ST_SPECIAL_REPORTS

    period = q.data.split(":")[1]
    context.user_data["analysis_period"] = period
    period_labels = {"today": "Сегодня", "week": "Эта неделя", "month": "Этот месяц", "year": "Этот год"}
    period_label  = period_labels.get(period, period)
    await q.edit_message_text(f"📊 {period_label}\n\nЧто посмотрим?", reply_markup=kb_analysis_type())
    return ST_ANALYSIS_TYPE


async def analysis_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    if not is_owner(user_id):
        await q.answer("Доступ запрещён", show_alert=True)
        return ST_MENU

    period       = context.user_data.get("analysis_period", "month")
    atype        = q.data.split(":")[1]
    period_labels = {"today": "Сегодня", "week": "Эта неделя", "month": "Этот месяц", "year": "Этот год"}
    period_label  = period_labels.get(period, period)

    await delete_working_message(context, update.effective_chat.id)

    if atype == "income":
        res    = await gas_request({"cmd": "analysis_income", "period": period}, user_id)
        total  = res.get("total", 0)
        by_type = res.get("by_type", {})
        text   = f"<b>💰 Поступления за {period_label.lower()}</b>\n\n"
        if total > 0:
            for ptype, amount in by_type.items():
                percentage = (amount / total) * 100
                emoji = "💵" if ptype == "Наличные" else "🏢"
                text += f"{emoji} {ptype}: <b>{amount:,.0f}</b> ₽ ({percentage:.0f}%)\n"
            text += f"━━━━━━━━━━━━━━━━\nИтого: <b>{total:,.0f}</b> ₽"
        else:
            text += "Нет данных"
        text = text.replace(",", " ")
    else:
        res         = await gas_request({"cmd": "analysis_expense", "period": period}, user_id)
        total       = res.get("total", 0)
        by_category = res.get("by_category", {})
        text        = f"<b>💸 Затраты за {period_label.lower()}</b>\n\n"
        if total > 0:
            for cat, amount in by_category.items():
                text += f"{cat}: <b>{amount:,.0f}</b> ₽\n"
            text += f"━━━━━━━━━━━━━━━━\nИтого: <b>{total:,.0f}</b> ₽"
        else:
            text += "Нет данных"
        text = text.replace(",", " ")

    await update.effective_chat.send_message(text, parse_mode=ParseMode.HTML)
    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


async def special_reports(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    if not is_owner(user_id):
        await q.answer("Доступ запрещён", show_alert=True)
        return ST_MENU

    await delete_working_message(context, update.effective_chat.id)

    if q.data == "special:compare":
        res    = await gas_request({"cmd": "compare_months"}, user_id)
        year   = res.get("year", 2026)
        months = res.get("months", [])
        text   = f"<b>📊 Сравнение месяцев ({year})</b>\n\n"
        for i, month_data in enumerate(months):
            month_name = month_data.get("month", "")
            incomes    = month_data.get("incomes", 0)
            expenses   = month_data.get("expenses", 0)
            text += f"<b>{month_name}:</b>\n"
            text += f"💰 Выручка: <b>{incomes:,.0f}</b> ₽"
            if i > 0:
                prev_inc = months[i - 1].get("incomes", 0)
                if prev_inc > 0:
                    change = ((incomes - prev_inc) / prev_inc) * 100
                    text += f" ({'+' if change >= 0 else ''}{change:.0f}%)"
            text += f"\n💸 Затраты: <b>{expenses:,.0f}</b> ₽"
            if i > 0:
                prev_exp = months[i - 1].get("expenses", 0)
                if prev_exp > 0:
                    change = ((expenses - prev_exp) / prev_exp) * 100
                    text += f" ({'+' if change >= 0 else ''}{change:.0f}%)"
            text += "\n\n"
        text = text.replace(",", " ")

    elif q.data == "special:average":
        res        = await gas_request({"cmd": "average_check"}, user_id)
        month_data = res.get("month", {})
        year_data  = res.get("year", {})
        text = "<b>💰 Средний чек</b>\n\n"
        text += f"<b>За {month_data.get('month_label', 'месяц')}:</b>\n"
        text += f"Средний чек: <b>{month_data.get('average', 0):,.0f}</b> ₽\n"
        text += f"Операций: {month_data.get('count', 0)}\n\n"
        text += f"<b>За {year_data.get('year_label', 'год')} год:</b>\n"
        text += f"Средний чек: <b>{year_data.get('average', 0):,.0f}</b> ₽\n"
        text += f"Операций: {year_data.get('count', 0)}"
        text = text.replace(",", " ")

    elif q.data == "special:top":
        res        = await gas_request({"cmd": "top_expenses"}, user_id)
        month_label = res.get("month_label", "месяц")
        total      = res.get("total", 0)
        categories = res.get("categories", [])
        text = f"<b>📋 Топ категорий затрат ({month_label})</b>\n\n"
        if categories:
            for i, cat_data in enumerate(categories, 1):
                text += f"{i}. {cat_data.get('category', '')}: <b>{cat_data.get('amount', 0):,.0f}</b> ₽\n"
            text += f"━━━━━━━━━━━━━━━━\nИтого: <b>{total:,.0f}</b> ₽"
        else:
            text += "Нет данных"
        text = text.replace(",", " ")

    else:
        text = "Неизвестный отчёт"

    await update.effective_chat.send_message(text, parse_mode=ParseMode.HTML)
    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


# ========================================
# БАЛАНС (только owner)
# ========================================
async def balance_edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    if not is_owner(user_id):
        await q.answer("Доступ запрещён", show_alert=True)
        return ST_MENU

    payment_type = q.data.split(":")[1]
    context.user_data["balance_payment_type"] = payment_type
    labels = {"cash": "наличных", "bn": "БН"}
    label  = labels.get(payment_type, "")

    await q.edit_message_text(
        f"⚙️ <b>Корректировка баланса {label}</b>\n\n"
        f"Введи новое значение баланса:\n"
        f"(например: 102000 или 102к)\n\n"
        f"С этого момента все транзакции будут\n"
        f"изменять именно это значение.",
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

    if not is_owner(user_id):
        await update.effective_chat.send_message("Доступ запрещён")
        return ST_MENU

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

    labels = {"cash": "наличных", "bn": "БН счета"}
    label  = labels.get(payment_type, "")
    await update.effective_chat.send_message(
        f"Отлично! ✅ Баланс {label} установлен: <b>{amt:,.2f}</b> ₽".replace(",", " "),
        parse_mode=ParseMode.HTML
    )

    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


# ========================================
# ДОЛГИ
# ========================================
async def debts_select_debtor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    if q.data == "debts:add":
        if not is_owner(user_id):
            await q.answer("Доступ запрещён", show_alert=True)
            return ST_DEBTS_SELECT
        debt_type  = context.user_data.get("debt_type", "owe_me")
        debt_label = "Долги перед Inside" if debt_type == "owe_me" else "Долги Inside"
        await q.edit_message_text(
            f"<b>{debt_label}</b>\n\n➕ Внести новый долг\n\nВведи имя должника:",
            parse_mode=ParseMode.HTML
        )
        context.user_data["working_message_id"] = q.message.message_id
        return ST_DEBTS_ADD_NAME

    debtor_id = int(q.data.split(":")[1])
    context.user_data["debtor_id"] = debtor_id

    debt_type = context.user_data.get("debt_type", "owe_me")
    debtors   = await gas_request({"cmd": "get_debtors_list", "debt_type": debt_type}, user_id)

    debtor = next((d for d in debtors.get("debtors", []) if d["id"] == debtor_id), None)
    if not debtor:
        await q.answer("Должник не найден", show_alert=True)
        return ST_DEBTS_SELECT

    context.user_data["debtor_name"] = debtor["name"]
    text = (
        f"<b>{debtor['name']}</b>\n"
        f"Текущий долг: <b>{debtor['amount']:,.2f}</b> ₽\n\n"
    ).replace(",", " ")
    text += "Выбери действие:"

    owner_mode = is_owner(user_id)
    await q.edit_message_text(text, reply_markup=kb_debtor_actions(owner_mode), parse_mode=ParseMode.HTML)
    return ST_DEBTS_SELECT


async def debtor_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q       = update.callback_query
    await q.answer()
    user_id = update.effective_user.id

    if q.data == "debtor:edit":
        debtor_name = context.user_data.get("debtor_name", "Должник")
        await q.edit_message_text(
            f"<b>{debtor_name}</b>\n\nВведи новую сумму долга:\n(или 0 чтобы закрыть)",
            parse_mode=ParseMode.HTML
        )
        context.user_data["working_message_id"] = q.message.message_id
        return ST_DEBTS_AMOUNT

    if q.data == "debtor:delete":
        if not is_owner(user_id):
            await q.answer("Доступ запрещён", show_alert=True)
            return ST_DEBTS_SELECT
        debtor_id = context.user_data.get("debtor_id")
        await gas_request({"cmd": "delete_debtor", "debtor_id": debtor_id}, user_id)
        await delete_working_message(context, update.effective_chat.id)
        await update.effective_chat.send_message("✅ Должник удалён")
        txt, kb = await get_main_screen(user_id)
        await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
        return ST_MENU

    return ST_DEBTS_SELECT


async def debts_amount_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    user_id = update.effective_user.id
    amt     = parse_amount(update.message.text)

    if amt is None or amt < 0:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял сумму 🙈\nНапиши, пожалуйста, например: 10000 / 10 000 / 10к или 0"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_DEBTS_AMOUNT

    debtor_id = context.user_data.get("debtor_id")
    await gas_request({"cmd": "update_debtor", "debtor_id": debtor_id, "amount": amt}, user_id)
    await delete_working_message(context, update.effective_chat.id)

    if amt == 0:
        await update.effective_chat.send_message("✅ Долг закрыт")
    else:
        debtor_name = context.user_data.get("debtor_name", "Должник")
        await update.effective_chat.send_message(
            f"✅ Обновлено!\n<b>{debtor_name}</b>: {amt:,.2f} ₽".replace(",", " "),
            parse_mode=ParseMode.HTML
        )

    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


async def debts_add_name_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    debtor_name = (update.message.text or "").strip()
    if not debtor_name:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message("Имя обязательно! Напиши:")
        context.user_data["working_message_id"] = msg.message_id
        return ST_DEBTS_ADD_NAME

    context.user_data["new_debtor_name"] = debtor_name
    work_msg_id = context.user_data.get("working_message_id")
    if work_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=work_msg_id,
                text=f"<b>{debtor_name}</b>\n\nВведи сумму долга:\n\nПримеры: <code>5000</code>, <code>5 000</code>, <code>5к</code>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    return ST_DEBTS_ADD_AMOUNT


async def debts_add_amount_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    amt = parse_amount(update.message.text)
    if amt is None or amt <= 0:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял сумму 🙈\nНапиши, пожалуйста, например: 5000 / 5 000 / 5к"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_DEBTS_ADD_AMOUNT

    context.user_data["new_debtor_amount"] = amt
    debtor_name = context.user_data.get("new_debtor_name", "")
    work_msg_id = context.user_data.get("working_message_id")

    if work_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=work_msg_id,
                text=f"<b>{debtor_name}</b>\nСумма: {amt:,.0f} ₽\n\nВыбери форму оплаты:".replace(",", " "),
                reply_markup=kb_debt_payment(),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    return ST_DEBTS_ADD_PAYMENT


async def debts_add_payment_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    payment_choice = q.data.split(":")[1]
    payment_type   = "Наличные" if payment_choice == "cash" else "БН (QR и счёт)"
    context.user_data["new_debtor_payment"] = payment_type

    debtor_name = context.user_data.get("new_debtor_name", "")
    amount      = context.user_data.get("new_debtor_amount", 0)

    await q.edit_message_text(
        f"<b>{debtor_name}</b>\n"
        f"Сумма: {amount:,.0f} ₽\n"
        f"Форма оплаты: {payment_type}\n\n"
        f"Добавить комментарий?\n(или напиши <code>-</code> чтобы пропустить)".replace(",", " "),
        parse_mode=ParseMode.HTML
    )
    context.user_data["working_message_id"] = q.message.message_id
    return ST_DEBTS_ADD_COMMENT


async def debts_add_comment_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    user_id     = update.effective_user.id
    comment     = (update.message.text or "").strip()
    if comment == "-":
        comment = ""

    debtor_name  = context.user_data.get("new_debtor_name", "")
    amount       = context.user_data.get("new_debtor_amount", 0)
    debt_type    = context.user_data.get("debt_type", "owe_me")
    payment_type = context.user_data.get("new_debtor_payment", "Наличные")

    await delete_working_message(context, update.effective_chat.id)

    payload = {
        "cmd":          "add_debtor",
        "debtor_name":  debtor_name,
        "amount":       amount,
        "debt_type":    debt_type,
        "comment":      comment,
        "payment_type": payment_type
    }

    try:
        await gas_request(payload, user_id)
        await update.effective_chat.send_message(
            f"✅ Долг записан!\n<b>{debtor_name}</b>: {amount:,.0f} ₽".replace(",", " "),
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        await update.effective_chat.send_message(f"Ошибка: {e}")

    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


# ========================================
# ПРОДАЖА ПЛЕНКИ
# ========================================
async def film_client_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    client_name = (update.message.text or "").strip()
    if not client_name:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message("Имя клиента обязательно! Напиши:")
        context.user_data["working_message_id"] = msg.message_id
        return ST_FILM_CLIENT

    film = context.user_data.get("film", {})
    film["client"] = client_name
    context.user_data["film"] = film

    work_msg_id = context.user_data.get("working_message_id")
    if work_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=work_msg_id,
                text="Сколько метров пленки?"
            )
        except Exception:
            pass
    return ST_FILM_METERS


async def film_meters_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    meters_text = (update.message.text or "").strip()
    try:
        meters = float(meters_text.replace(",", "."))
        if meters <= 0:
            raise ValueError
    except Exception:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял количество 🙈\nНапиши, пожалуйста, например: 5 или 5.5"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_FILM_METERS

    film = context.user_data.get("film", {})
    film["meters"] = meters
    context.user_data["film"] = film

    work_msg_id = context.user_data.get("working_message_id")
    if work_msg_id:
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=work_msg_id,
                text="Какая сумма?\n\nПримеры: <code>2500</code>, <code>2 500</code>, <code>2к</code>",
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    return ST_FILM_AMOUNT


async def film_amount_received(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return ConversationHandler.END

    try:
        await update.message.delete()
    except Exception:
        pass

    amt = parse_amount(update.message.text)
    if amt is None or amt <= 0:
        await delete_working_message(context, update.effective_chat.id)
        msg = await update.effective_chat.send_message(
            "Не понял сумму 🙈\nНапиши, пожалуйста, например: 2500 / 2 500 / 2к"
        )
        context.user_data["working_message_id"] = msg.message_id
        return ST_FILM_AMOUNT

    film = context.user_data.get("film", {})
    film["amount"] = amt
    context.user_data["film"] = film

    work_msg_id = context.user_data.get("working_message_id")
    if work_msg_id:
        try:
            client = film.get("client", "")
            meters = film.get("meters", 0)
            text = (
                f"📦 Продал пленку\n\n"
                f"Клиент: <b>{client}</b>\n"
                f"Метров: <b>{meters}</b>\n"
                f"Сумма: <b>{amt:,.0f}</b> ₽\n\n"
                f"Как оплатили?"
            ).replace(",", " ")
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=work_msg_id,
                text=text,
                reply_markup=kb_film_payment(),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass
    return ST_FILM_PAYMENT


async def film_payment_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id        = update.effective_user.id
    payment_choice = q.data.split(":")[1]

    film   = context.user_data.get("film", {})
    client = film.get("client", "")
    meters = film.get("meters", 0)
    amount = film.get("amount", 0)

    await delete_working_message(context, update.effective_chat.id)

    if payment_choice == "debt":
        comment = f"Пленка {meters} м"
        payload = {
            "cmd":          "add_debtor",
            "debtor_name":  client,
            "amount":       amount,
            "debt_type":    "owe_me",
            "comment":      comment,
            "payment_type": "Наличные",
            "allow_any":    True   # разрешаем admin добавлять долг через пленку
        }
        try:
            await gas_request(payload, user_id)
            await update.effective_chat.send_message(
                f"✅ Записано в долг!\n"
                f"<b>{client}</b>: {amount:,.0f} ₽\n"
                f"({meters} м пленки)".replace(",", " "),
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            await update.effective_chat.send_message(f"Ошибка: {e}")
    else:
        category     = "Наличные"       if payment_choice == "cash" else "БН (QR и счёт)"
        payment_type = "Наличные"       if payment_choice == "cash" else "БН (QR и счёт)"
        comment      = f"{client} - Пленка {meters} м"
        payload = {
            "cmd":          "add",
            "type":         "доход",
            "category":     category,
            "amount":       amount,
            "payment_type": payment_type,
            "comment":      comment
        }
        try:
            await gas_request(payload, user_id)
            emoji = "💵" if payment_choice == "cash" else "🏢"
            await update.effective_chat.send_message(
                f"✅ Записано!\n"
                f"{emoji} {category} — {amount:,.0f} ₽\n"
                f"{client} — {meters} м пленки".replace(",", " ")
            )
        except Exception as e:
            await update.effective_chat.send_message(f"Ошибка: {e}")

    txt, kb = await get_main_screen(user_id)
    await update.effective_chat.send_message(txt, reply_markup=kb, parse_mode=ParseMode.HTML)
    return ST_MENU


# ========================================
# HELP / ERROR
# ========================================
async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_allowed(update):
        await update.message.reply_text(DENY_TEXT)
        return
    await update.message.reply_text("Кнопки внизу 🙂\nЕсли что-то не работает — напиши /start")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("Ой, что-то пошло не так 🙈 Попробуем ещё раз?")
    except Exception:
        pass


# ========================================
# BUILD APP
# ========================================
def build_app() -> Application:
    app = ApplicationBuilder().token(BOT_TOKEN).post_shutdown(_on_shutdown).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            ST_MENU: [
                CallbackQueryHandler(on_menu,          pattern=r"^menu:"),
                CallbackQueryHandler(balance_edit_start, pattern=r"^balance:(cash|bn)$"),
            ],
            ST_ADD_CHOOSE_TYPE: [
                CallbackQueryHandler(choose_type,  pattern=r"^type:"),
                CallbackQueryHandler(back_router,  pattern=r"^back:"),
            ],
            ST_EXP_CATEGORY: [
                CallbackQueryHandler(expense_category, pattern=r"^expcat:\d+$"),
                CallbackQueryHandler(back_router,      pattern=r"^back:"),
            ],
            ST_INC_CATEGORY: [
                CallbackQueryHandler(income_category, pattern=r"^inccat:\d+$"),
                CallbackQueryHandler(back_router,     pattern=r"^back:"),
            ],
            ST_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, amount_received),
            ],
            ST_EXP_PAYMENT_TYPE: [
                CallbackQueryHandler(payment_type_selected, pattern=r"^payment:\d+$"),
            ],
            ST_COMMENT: [
                CallbackQueryHandler(comment_skip,   pattern=r"^comment:skip$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, comment_received),
            ],
            ST_ANALYSIS_PERIOD: [
                CallbackQueryHandler(analysis_period, pattern=r"^aperiod:"),
                CallbackQueryHandler(back_router,     pattern=r"^back:"),
            ],
            ST_ANALYSIS_TYPE: [
                CallbackQueryHandler(analysis_type, pattern=r"^atype:"),
                CallbackQueryHandler(back_router,   pattern=r"^back:"),
            ],
            ST_SPECIAL_REPORTS: [
                CallbackQueryHandler(special_reports, pattern=r"^special:"),
                CallbackQueryHandler(back_router,     pattern=r"^back:"),
            ],
            ST_BALANCE_EDIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, balance_edit_received),
            ],
            ST_DEBTS_SELECT: [
                CallbackQueryHandler(debts_select_debtor, pattern=r"^debtor:\d+$"),
                CallbackQueryHandler(debts_select_debtor, pattern=r"^debts:add$"),
                CallbackQueryHandler(debtor_action,       pattern=r"^debtor:(edit|delete)$"),
                CallbackQueryHandler(back_router,         pattern=r"^back:"),
            ],
            ST_DEBTS_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debts_amount_received),
            ],
            ST_DEBTS_ADD_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debts_add_name_received),
            ],
            ST_DEBTS_ADD_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debts_add_amount_received),
            ],
            ST_DEBTS_ADD_PAYMENT: [
                CallbackQueryHandler(debts_add_payment_selected, pattern=r"^debt_payment:"),
            ],
            ST_DEBTS_ADD_COMMENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, debts_add_comment_received),
            ],
            ST_FILM_CLIENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, film_client_received),
            ],
            ST_FILM_METERS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, film_meters_received),
            ],
            ST_FILM_AMOUNT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, film_amount_received),
            ],
            ST_FILM_PAYMENT: [
                CallbackQueryHandler(film_payment_selected, pattern=r"^film_payment:"),
                CallbackQueryHandler(back_router,           pattern=r"^back:"),
            ],
        },
        fallbacks=[CommandHandler("help", cmd_help)],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_error_handler(error_handler)
    return app


async def _on_shutdown(app: Application) -> None:
    """Graceful shutdown: закрываем persistent GAS session."""
    await _close_gas_session()


def run():
    app = build_app()
    if WEBHOOK_URL:
        url_path     = WEBHOOK_PATH or _default_webhook_path()
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
