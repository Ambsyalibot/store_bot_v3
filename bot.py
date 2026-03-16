
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

BOT_TOKEN = "8760390856:AAFAvOOPMpnpGwYFlhUaYxsXl_SpUXHtI9M"
ADMIN_ID = 7007160064
SUPPORT_USERNAME = "Dmaardone"

DB_NAME = "store_v3.db"
RESERVE_MINUTES = 5

SHAM_CASH_NAME = "AMB"
SHAM_CASH_NUMBER = "8940269bc88c8dbf09425b45917ec4c5"
BINANCE_ID = "1121624869"

DEFAULT_CATEGORIES = [
    "حسابات شات جي بي تي",
    "حسابات ببجي موبايل",
    "حسابات فيسبوك",
    "حسابات بايبال",
    "حسابات قوقل",
    "حسابات نتفلكس",
    "شحن رصيد MTN",
    "شحن رصيد سيرياتيل",
    "شحن ببجي موبايل",
    "شحن فري فاير",
    "إعلانات ممولة فيسبوك",
    "تزويد متابعين فيسبوك",
]

logging.basicConfig(level=logging.INFO)

if not BOT_TOKEN or BOT_TOKEN == "PUT_BOT_TOKEN_HERE":
    raise RuntimeError("ضع BOT_TOKEN الحقيقي داخل bot.py")

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher(storage=MemoryStorage())

class AddCategoryState(StatesGroup):
    name = State()

class AddProductStates(StatesGroup):
    name = State()
    category = State()
    price = State()
    description = State()
    stock = State()

class AddStockStates(StatesGroup):
    product_id = State()
    stock = State()

class EditProductStates(StatesGroup):
    product_id = State()
    field = State()
    value = State()

class DeleteProductStates(StatesGroup):
    product_id = State()

class BroadcastStates(StatesGroup):
    text = State()

class BuyStates(StatesGroup):
    quantity = State()
    waiting_payment_proof = State()

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID

def support_url() -> str:
    return f"https://t.me/{SUPPORT_USERNAME}"

def status_label(status: str) -> str:
    mapping = {
        "reserved": "محجوز",
        "review": "بانتظار مراجعة الأدمن",
        "approved": "تم التسليم",
        "rejected": "مرفوض",
        "expired": "انتهت المهلة",
        "cancelled": "ملغي",
    }
    return mapping.get(status, status)

def safe_int(text: str) -> Optional[int]:
    try:
        return int(str(text).strip())
    except Exception:
        return None

def format_price(value) -> str:
    try:
        num = float(value)
        if num.is_integer():
            return str(int(num))
        return f"{num:.2f}"
    except Exception:
        return str(value)

async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            description TEXT DEFAULT '',
            is_active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(category_id) REFERENCES categories(id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS stock_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            secret_data TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'available',
            reserved_by INTEGER,
            reserved_until TEXT,
            order_id INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            total_price REAL NOT NULL DEFAULT 0,
            payment_method TEXT,
            payment_proof_file_id TEXT,
            status TEXT NOT NULL DEFAULT 'reserved',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
        """)
        for cat in DEFAULT_CATEGORIES:
            await db.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (cat,))
        await db.commit()

async def register_user(message: Message):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            """
            INSERT INTO users (user_id, username, full_name)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name
            """,
            (message.from_user.id, message.from_user.username, message.from_user.full_name),
        )
        await db.commit()

async def get_categories():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT id, name FROM categories WHERE is_active=1 ORDER BY id ASC")
        return await cur.fetchall()

async def create_category(name: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT INTO categories (name) VALUES (?)", (name,))
        await db.commit()

async def create_product(category_id: int, name: str, price: float, description: str):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "INSERT INTO products (category_id, name, price, description) VALUES (?, ?, ?, ?)",
            (category_id, name, price, description),
        )
        await db.commit()
        return cur.lastrowid

async def add_stock_lines(product_id: int, lines: list[str]):
    async with aiosqlite.connect(DB_NAME) as db:
        for line in lines:
            await db.execute(
                "INSERT INTO stock_items (product_id, secret_data) VALUES (?, ?)",
                (product_id, line),
            )
        await db.commit()

async def get_all_products(only_available: bool = False):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT p.id, c.name, p.name, p.price, p.description, p.is_active,
                   COALESCE(SUM(CASE WHEN s.status='available' THEN 1 ELSE 0 END), 0) AS available_count
            FROM products p
            JOIN categories c ON c.id = p.category_id
            LEFT JOIN stock_items s ON s.product_id = p.id
            GROUP BY p.id
            ORDER BY p.id DESC
            """
        )
        rows = await cur.fetchall()
        if only_available:
            rows = [r for r in rows if r[6] > 0 and r[5] == 1]
        return rows

async def get_products_by_category(category_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT p.id, p.name, p.price, p.description, p.is_active,
                   COALESCE(SUM(CASE WHEN s.status='available' THEN 1 ELSE 0 END), 0) AS available_count
            FROM products p
            LEFT JOIN stock_items s ON s.product_id = p.id
            WHERE p.category_id = ?
            GROUP BY p.id
            ORDER BY p.id DESC
            """,
            (category_id,),
        )
        return await cur.fetchall()

async def get_product(product_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT p.id, c.name, p.name, p.price, p.description, p.is_active,
                   COALESCE(SUM(CASE WHEN s.status='available' THEN 1 ELSE 0 END), 0) AS available_count
            FROM products p
            JOIN categories c ON c.id = p.category_id
            LEFT JOIN stock_items s ON s.product_id = p.id
            WHERE p.id = ?
            GROUP BY p.id
            """,
            (product_id,),
        )
        return await cur.fetchone()

async def update_product_field(product_id: int, field_name: str, value):
    allowed = {"name", "price", "description", "is_active"}
    if field_name not in allowed:
        raise ValueError("حقل غير مسموح")
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(f"UPDATE products SET {field_name} = ? WHERE id = ?", (value, product_id))
        await db.commit()

async def delete_product(product_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM stock_items WHERE product_id = ?", (product_id,))
        await db.execute("DELETE FROM orders WHERE product_id = ?", (product_id,))
        await db.execute("DELETE FROM products WHERE id = ?", (product_id,))
        await db.commit()

async def create_order_with_quantity(user_id: int, product_id: int, quantity: int, total_price: float) -> Optional[int]:
    reserve_until = (datetime.utcnow() + timedelta(minutes=RESERVE_MINUTES)).isoformat()
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            "SELECT id FROM stock_items WHERE product_id = ? AND status = 'available' ORDER BY id ASC LIMIT ?",
            (product_id, quantity),
        )
        rows = await cur.fetchall()
        if len(rows) < quantity:
            return None
        cur = await db.execute(
            "INSERT INTO orders (user_id, product_id, quantity, total_price, status) VALUES (?, ?, ?, ?, 'reserved')",
            (user_id, product_id, quantity, total_price),
        )
        order_id = cur.lastrowid
        for (stock_id,) in rows:
            await db.execute(
                "UPDATE stock_items SET status='reserved', reserved_by=?, reserved_until=?, order_id=? WHERE id=?",
                (user_id, reserve_until, order_id, stock_id),
            )
        await db.commit()
        return order_id

async def set_order_payment_method(order_id: int, payment_method: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE orders SET payment_method=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (payment_method, order_id),
        )
        await db.commit()

async def set_order_proof(order_id: int, file_id: str):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute(
            "UPDATE orders SET payment_proof_file_id=?, status='review', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (file_id, order_id),
        )
        await db.commit()

async def get_user_orders(user_id: int, delivered_only: bool = False):
    async with aiosqlite.connect(DB_NAME) as db:
        q = """
        SELECT o.id, p.name, o.quantity, o.total_price, o.payment_method, o.status, o.created_at
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.user_id = ?
        """
        q += " AND o.status = 'approved'" if delivered_only else " AND o.status != 'approved'"
        q += " ORDER BY o.id DESC LIMIT 30"
        cur = await db.execute(q, (user_id,))
        return await cur.fetchall()

async def get_new_orders():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT o.id, u.username, u.full_name, p.name, o.quantity, o.total_price,
                   o.payment_method, o.payment_proof_file_id, o.status, o.created_at
            FROM orders o
            JOIN users u ON u.user_id = o.user_id
            JOIN products p ON p.id = o.product_id
            WHERE o.status IN ('review')
            ORDER BY o.id DESC LIMIT 30
            """
        )
        return await cur.fetchall()

async def get_cancelled_orders():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT o.id, u.full_name, p.name, o.quantity, o.total_price, o.status, o.created_at
            FROM orders o
            JOIN users u ON u.user_id = o.user_id
            JOIN products p ON p.id = o.product_id
            WHERE o.status IN ('rejected', 'expired', 'cancelled')
            ORDER BY o.id DESC LIMIT 30
            """
        )
        return await cur.fetchall()

async def get_order(order_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute(
            """
            SELECT o.id, o.user_id, o.product_id, o.quantity, o.total_price,
                   o.payment_method, o.payment_proof_file_id, o.status, p.name
            FROM orders o
            JOIN products p ON p.id = o.product_id
            WHERE o.id = ?
            """,
            (order_id,),
        )
        return await cur.fetchone()

async def get_order_stock_items(order_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT id, secret_data FROM stock_items WHERE order_id=? ORDER BY id ASC", (order_id,))
        return await cur.fetchall()

async def approve_order(order_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT user_id FROM orders WHERE id=?", (order_id,))
        row = await cur.fetchone()
        if not row:
            return None
        user_id = row[0]
        await db.execute("UPDATE orders SET status='approved', updated_at=CURRENT_TIMESTAMP WHERE id=?", (order_id,))
        await db.execute("UPDATE stock_items SET status='sold' WHERE order_id=?", (order_id,))
        await db.commit()
        return user_id

async def reject_order(order_id: int):
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT user_id FROM orders WHERE id=?", (order_id,))
        row = await cur.fetchone()
        if not row:
            return None
        user_id = row[0]
        await db.execute("UPDATE orders SET status='rejected', updated_at=CURRENT_TIMESTAMP WHERE id=?", (order_id,))
        await db.execute(
            "UPDATE stock_items SET status='available', reserved_by=NULL, reserved_until=NULL, order_id=NULL WHERE order_id=?",
            (order_id,),
        )
        await db.commit()
        return user_id

async def expire_old_reservations():
    while True:
        now_iso = datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_NAME) as db:
            cur = await db.execute(
                """
                SELECT DISTINCT o.id, o.user_id
                FROM orders o
                JOIN stock_items s ON s.order_id = o.id
                WHERE o.status = 'reserved' AND s.status='reserved' AND s.reserved_until <= ?
                """,
                (now_iso,),
            )
            rows = await cur.fetchall()
            for order_id, user_id in rows:
                await db.execute("UPDATE orders SET status='expired', updated_at=CURRENT_TIMESTAMP WHERE id=?", (order_id,))
                await db.execute(
                    "UPDATE stock_items SET status='available', reserved_by=NULL, reserved_until=NULL, order_id=NULL WHERE order_id=?",
                    (order_id,),
                )
                try:
                    await bot.send_message(user_id, "⏰ انتهت مهلة الحجز، وتم إرجاع الكمية إلى المخزون.")
                except Exception:
                    pass
            await db.commit()
        await asyncio.sleep(20)

async def get_all_users_ids():
    async with aiosqlite.connect(DB_NAME) as db:
        cur = await db.execute("SELECT user_id FROM users")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

def user_main_menu(is_admin_user: bool = False):
    rows = [
        [InlineKeyboardButton(text="🛍️ المتجر", callback_data="menu_store")],
        [InlineKeyboardButton(text="📦 المنتجات المتاحة", callback_data="menu_available")],
        [InlineKeyboardButton(text="🧾 طلباتي", callback_data="menu_orders")],
        [InlineKeyboardButton(text="💳 مشترياتي السابقة", callback_data="menu_history")],
        [InlineKeyboardButton(text="💬 الدعم", url=support_url())],
    ]
    if is_admin_user:
        rows.append([InlineKeyboardButton(text="🔐 لوحة الأدمن", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def back_home_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 الرئيسية", callback_data="back_home")]])

def admin_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📥 الطلبات الجديدة", callback_data="admin_new_orders")],
            [InlineKeyboardButton(text="❌ الطلبات الملغية", callback_data="admin_cancelled_orders")],
            [InlineKeyboardButton(text="📦 إدارة المنتجات", callback_data="admin_products")],
            [InlineKeyboardButton(text="📢 رسالة جماعية", callback_data="admin_broadcast")],
            [InlineKeyboardButton(text="👥 المستخدمون", callback_data="admin_users")],
            [InlineKeyboardButton(text="🔙 الرئيسية", callback_data="back_home")],
        ]
    )

def admin_products_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ إضافة قسم", callback_data="admin_add_category")],
            [InlineKeyboardButton(text="➕ إضافة منتج", callback_data="admin_add_product")],
            [InlineKeyboardButton(text="📥 إضافة مخزون", callback_data="admin_add_stock")],
            [InlineKeyboardButton(text="📋 عرض المنتجات", callback_data="admin_list_products")],
            [InlineKeyboardButton(text="✏️ تعديل منتج", callback_data="admin_edit_product")],
            [InlineKeyboardButton(text="🗑 حذف منتج", callback_data="admin_delete_product")],
            [InlineKeyboardButton(text="🔙 لوحة الأدمن", callback_data="admin_panel")],
        ]
    )

async def categories_kb(prefix: str):
    cats = await get_categories()
    rows = [[InlineKeyboardButton(text=name, callback_data=f"{prefix}_{cat_id}")] for cat_id, name in cats]
    rows.append([InlineKeyboardButton(text="🔙 الرئيسية", callback_data="back_home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def payment_methods_kb(order_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💚 Sham Cash", callback_data=f"paymethod_sham_{order_id}")],
            [InlineKeyboardButton(text="🟡 Binance", callback_data=f"paymethod_binance_{order_id}")],
            [InlineKeyboardButton(text="🔙 الرئيسية", callback_data="back_home")],
        ]
    )

def approve_reject_kb(order_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text=f"✅ قبول #{order_id}", callback_data=f"approve_{order_id}"),
            InlineKeyboardButton(text=f"❌ رفض #{order_id}", callback_data=f"reject_{order_id}"),
        ]]
    )

@dp.message(CommandStart())
async def cmd_start(message: Message):
    await register_user(message)
    await message.answer("✨ أهلاً بك في متجر AMB ✨\n\nاختر من الأزرار التالية:", reply_markup=user_main_menu(is_admin(message.from_user.id)))

@dp.callback_query(F.data == "back_home")
async def back_home(callback: CallbackQuery):
    await callback.message.edit_text("🏠 الرئيسية", reply_markup=user_main_menu(is_admin(callback.from_user.id)))
    await callback.answer()

@dp.callback_query(F.data == "menu_store")
async def menu_store(callback: CallbackQuery):
    await callback.message.edit_text("🛍️ اختر القسم:", reply_markup=await categories_kb("cat"))
    await callback.answer()

@dp.callback_query(F.data == "menu_available")
async def menu_available(callback: CallbackQuery):
    rows = await get_all_products(only_available=True)
    if not rows:
        await callback.message.edit_text("لا توجد منتجات متاحة الآن.", reply_markup=back_home_kb())
        await callback.answer()
        return
    text = "📦 المنتجات المتاحة:\n\n"
    for pid, cat_name, name, price, desc, is_active, count in rows[:30]:
        text += f"• <b>{name}</b> — {format_price(price)} — المتوفر: {count}\n"
    await callback.message.edit_text(text, reply_markup=back_home_kb())
    await callback.answer()

@dp.callback_query(F.data == "menu_orders")
async def menu_orders(callback: CallbackQuery):
    rows = await get_user_orders(callback.from_user.id, delivered_only=False)
    if not rows:
        await callback.message.edit_text("لا توجد طلبات حالية.", reply_markup=back_home_kb())
        await callback.answer()
        return
    text = "🧾 طلباتي:\n\n"
    for oid, name, quantity, total_price, payment_method, status, created_at in rows:
        pm = payment_method or "غير محددة"
        text += f"#{oid} | {name} | العدد: {quantity} | الإجمالي: {format_price(total_price)} | {pm} | {status_label(status)}\n"
    await callback.message.edit_text(text, reply_markup=back_home_kb())
    await callback.answer()

@dp.callback_query(F.data == "menu_history")
async def menu_history(callback: CallbackQuery):
    rows = await get_user_orders(callback.from_user.id, delivered_only=True)
    if not rows:
        await callback.message.edit_text("لا توجد مشتريات سابقة بعد.", reply_markup=back_home_kb())
        await callback.answer()
        return
    text = "💳 مشترياتي السابقة:\n\n"
    for oid, name, quantity, total_price, payment_method, status, created_at in rows:
        text += f"#{oid} | {name} | العدد: {quantity} | الإجمالي: {format_price(total_price)}\n"
    await callback.message.edit_text(text, reply_markup=back_home_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("cat_"))
async def category_products(callback: CallbackQuery):
    category_id = int(callback.data.split("_")[1])
    products = await get_products_by_category(category_id)
    visible = [p for p in products if p[4] == 1]
    if not visible:
        await callback.message.edit_text("لا توجد منتجات في هذا القسم الآن.", reply_markup=await categories_kb("cat"))
        await callback.answer()
        return
    rows = []
    for pid, name, price, desc, is_active, count in visible:
        rows.append([InlineKeyboardButton(text=f"{name} — {format_price(price)}", callback_data=f"product_{pid}")])
    rows.append([InlineKeyboardButton(text="🔙 الأقسام", callback_data="menu_store")])
    await callback.message.edit_text("اختر المنتج:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

@dp.callback_query(F.data.startswith("product_"))
async def product_page(callback: CallbackQuery, state: FSMContext):
    product_id = int(callback.data.split("_")[1])
    product = await get_product(product_id)
    if not product:
        await callback.answer("المنتج غير موجود", show_alert=True)
        return
    pid, cat_name, name, price, description, is_active, available_count = product
    if not is_active:
        await callback.answer("المنتج مخفي حالياً", show_alert=True)
        return
    text = (
        f"<b>{name}</b>\n"
        f"القسم: {cat_name}\n"
        f"السعر للوحدة: {format_price(price)}\n"
        f"الوصف: {description or 'لا يوجد وصف'}\n"
        f"المتوفر: {available_count}"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛒 شراء", callback_data=f"buy_{product_id}")],
            [InlineKeyboardButton(text="🔙 رجوع", callback_data="menu_store")],
        ]
    )
    await state.update_data(selected_product_id=product_id)
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("buy_"))
async def buy_product(callback: CallbackQuery, state: FSMContext):
    product_id = int(callback.data.split("_")[1])
    product = await get_product(product_id)
    if not product:
        await callback.answer("المنتج غير موجود", show_alert=True)
        return
    await state.update_data(selected_product_id=product_id)
    await state.set_state(BuyStates.quantity)
    await callback.message.edit_text("كم عدد الحسابات / الوحدات التي تريدها؟ أرسل رقمًا فقط.")
    await callback.answer()

@dp.message(BuyStates.quantity)
async def receive_quantity(message: Message, state: FSMContext):
    quantity = safe_int(message.text)
    if not quantity or quantity <= 0:
        await message.answer("أرسل رقمًا صحيحًا أكبر من 0.")
        return
    data = await state.get_data()
    product_id = data.get("selected_product_id")
    product = await get_product(product_id)
    if not product:
        await state.clear()
        await message.answer("المنتج غير موجود.")
        return
    pid, cat_name, name, price, description, is_active, available_count = product
    if quantity > available_count:
        await message.answer(f"الكمية المطلوبة غير متوفرة. المتوفر حاليًا: {available_count}")
        return
    total_price = float(price) * quantity
    order_id = await create_order_with_quantity(message.from_user.id, product_id, quantity, total_price)
    if not order_id:
        await message.answer("تعذر حجز الكمية المطلوبة. جرّب مرة أخرى.")
        return
    await state.clear()
    text = (
        f"✅ تم إنشاء الطلب #{order_id}\n\n"
        f"المنتج: {name}\n"
        f"الكمية: {quantity}\n"
        f"سعر الوحدة: {format_price(price)}\n"
        f"الإجمالي: {format_price(total_price)}\n\n"
        f"اختر طريقة الدفع:\n"
        f"⏰ مهلة الحجز: {RESERVE_MINUTES} دقائق"
    )
    await message.answer(text, reply_markup=payment_methods_kb(order_id))

@dp.callback_query(F.data.startswith("paymethod_"))
async def choose_payment_method(callback: CallbackQuery, state: FSMContext):
    _, method, order_id_str = callback.data.split("_")
    order_id = int(order_id_str)
    await set_order_payment_method(order_id, method)
    await state.update_data(waiting_order_id=order_id)
    await state.set_state(BuyStates.waiting_payment_proof)
    order = await get_order(order_id)
    if not order:
        await callback.answer("الطلب غير موجود", show_alert=True)
        return
    _, _, _, quantity, total_price, payment_method, _, _, product_name = order
    if method == "sham":
        text = (
            f"💚 <b>الدفع عبر Sham Cash</b>\n\n"
            f"المنتج: {product_name}\n"
            f"الكمية: {quantity}\n"
            f"الإجمالي: {format_price(total_price)}\n\n"
            f"الاسم: {SHAM_CASH_NAME}\n"
            f"رقم الحساب:\n<code>{SHAM_CASH_NUMBER}</code>\n\n"
            f"بعد التحويل أرسل لقطة شاشة لإثبات الدفع الآن."
        )
    else:
        text = (
            f"🟡 <b>الدفع عبر Binance</b>\n\n"
            f"المنتج: {product_name}\n"
            f"الكمية: {quantity}\n"
            f"الإجمالي: {format_price(total_price)}\n\n"
            f"Binance ID:\n<code>{BINANCE_ID}</code>\n\n"
            f"بعد التحويل أرسل لقطة شاشة لإثبات الدفع الآن."
        )
    await callback.message.edit_text(text, reply_markup=back_home_kb())
    await callback.answer()

@dp.message(BuyStates.waiting_payment_proof, F.photo)
async def receive_payment_proof(message: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data.get("waiting_order_id")
    if not order_id:
        await state.clear()
        await message.answer("لا يوجد طلب مرتبط بهذه الصورة.")
        return
    photo = message.photo[-1]
    await set_order_proof(order_id, photo.file_id)
    order = await get_order(order_id)
    if not order:
        await state.clear()
        await message.answer("الطلب غير موجود.")
        return
    _, _, _, quantity, total_price, payment_method, _, _, product_name = order
    caption = (
        f"📥 <b>طلب جديد للمراجعة</b>\n\n"
        f"رقم الطلب: #{order_id}\n"
        f"المنتج: {product_name}\n"
        f"الكمية: {quantity}\n"
        f"الإجمالي: {format_price(total_price)}\n"
        f"طريقة الدفع: {payment_method}\n"
        f"المستخدم: {message.from_user.full_name}\n"
        f"المعرف: {message.from_user.id}"
    )
    try:
        await bot.send_photo(ADMIN_ID, photo.file_id, caption=caption, reply_markup=approve_reject_kb(order_id))
    except Exception:
        await bot.send_message(ADMIN_ID, caption, reply_markup=approve_reject_kb(order_id))
    await state.clear()
    await message.answer("✅ تم استلام صورة الدفع. طلبك الآن بانتظار مراجعة الأدمن.", reply_markup=back_home_kb())

@dp.message(BuyStates.waiting_payment_proof)
async def waiting_proof_non_photo(message: Message):
    await message.answer("أرسل صورة لقطة الشاشة فقط لإثبات الدفع.")

@dp.message(Command("admin"))
async def cmd_admin(message: Message):
    if not is_admin(message.from_user.id):
        await message.answer("غير مصرح")
        return
    await message.answer("🔐 لوحة الأدمن", reply_markup=admin_menu())

@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("غير مصرح", show_alert=True)
        return
    await callback.message.edit_text("🔐 لوحة الأدمن", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_products")
async def admin_products(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    await callback.message.edit_text("📦 إدارة المنتجات", reply_markup=admin_products_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_add_category")
async def admin_add_category(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AddCategoryState.name)
    await callback.message.edit_text("أرسل اسم القسم الجديد:")
    await callback.answer()

@dp.message(AddCategoryState.name)
async def add_category_message(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    name = message.text.strip()
    try:
        await create_category(name)
        await message.answer("✅ تم إضافة القسم بنجاح.", reply_markup=admin_products_menu())
    except Exception:
        await message.answer("⚠️ هذا القسم موجود مسبقًا أو الاسم غير صالح.", reply_markup=admin_products_menu())
    await state.clear()

@dp.callback_query(F.data == "admin_add_product")
async def admin_add_product(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AddProductStates.name)
    await callback.message.edit_text("أرسل اسم المنتج:")
    await callback.answer()

@dp.message(AddProductStates.name)
async def add_product_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AddProductStates.category)
    await message.answer("اختر القسم:", reply_markup=await categories_kb("addcat"))

@dp.callback_query(F.data.startswith("addcat_"), AddProductStates.category)
async def add_product_category(callback: CallbackQuery, state: FSMContext):
    category_id = int(callback.data.split("_")[1])
    await state.update_data(category_id=category_id)
    await state.set_state(AddProductStates.price)
    await callback.message.edit_text("أرسل السعر للوحدة:")
    await callback.answer()

@dp.message(AddProductStates.price)
async def add_product_price(message: Message, state: FSMContext):
    try:
        price = float(message.text.strip())
    except Exception:
        await message.answer("أرسل سعرًا صحيحًا.")
        return
    await state.update_data(price=price)
    await state.set_state(AddProductStates.description)
    await message.answer("أرسل الوصف:")

@dp.message(AddProductStates.description)
async def add_product_desc(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await state.set_state(AddProductStates.stock)
    await message.answer("أرسل المخزون، كل سطر بهذا الشكل:\nemail@example.com:password")

@dp.message(AddProductStates.stock)
async def add_product_stock(message: Message, state: FSMContext):
    data = await state.get_data()
    lines = [line.strip() for line in message.text.splitlines() if line.strip()]
    if not lines:
        await message.answer("المخزون فارغ. أرسل سطرًا واحدًا على الأقل.")
        return
    product_id = await create_product(
        category_id=data["category_id"],
        name=data["name"],
        price=data["price"],
        description=data["description"],
    )
    await add_stock_lines(product_id, lines)
    await state.clear()
    await message.answer(f"✅ تم إضافة المنتج بنجاح\nID: {product_id}\nعدد عناصر المخزون: {len(lines)}", reply_markup=admin_products_menu())

@dp.callback_query(F.data == "admin_add_stock")
async def admin_add_stock(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AddStockStates.product_id)
    await callback.message.edit_text("أرسل ID المنتج الذي تريد إضافة مخزون له:")
    await callback.answer()

@dp.message(AddStockStates.product_id)
async def add_stock_product_id(message: Message, state: FSMContext):
    product_id = safe_int(message.text)
    if not product_id:
        await message.answer("أرسل رقم ID صحيح.")
        return
    product = await get_product(product_id)
    if not product:
        await message.answer("المنتج غير موجود.")
        return
    await state.update_data(product_id=product_id)
    await state.set_state(AddStockStates.stock)
    await message.answer("أرسل المخزون الجديد، كل سطر بهذا الشكل:\nemail@example.com:password")

@dp.message(AddStockStates.stock)
async def add_stock_lines_message(message: Message, state: FSMContext):
    data = await state.get_data()
    lines = [line.strip() for line in message.text.splitlines() if line.strip()]
    if not lines:
        await message.answer("المخزون فارغ.")
        return
    await add_stock_lines(data["product_id"], lines)
    await state.clear()
    await message.answer(f"✅ تم إضافة {len(lines)} عنصر مخزون.", reply_markup=admin_products_menu())

@dp.callback_query(F.data == "admin_list_products")
async def admin_list_products(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    rows = await get_all_products(False)
    if not rows:
        await callback.message.edit_text("لا توجد منتجات بعد.", reply_markup=admin_products_menu())
        await callback.answer()
        return
    text = "📋 المنتجات:\n\n"
    for pid, cat_name, name, price, desc, is_active, count in rows[:40]:
        text += (
            f"ID {pid} | {name}\n"
            f"القسم: {cat_name}\n"
            f"السعر: {format_price(price)}\n"
            f"الحالة: {'ظاهر' if is_active else 'مخفي'}\n"
            f"المتوفر: {count}\n\n"
        )
    await callback.message.edit_text(text, reply_markup=admin_products_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_edit_product")
async def admin_edit_product(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(EditProductStates.product_id)
    await callback.message.edit_text("أرسل ID المنتج الذي تريد تعديله:")
    await callback.answer()

@dp.message(EditProductStates.product_id)
async def edit_product_id(message: Message, state: FSMContext):
    product_id = safe_int(message.text)
    if not product_id:
        await message.answer("أرسل رقم ID صحيح.")
        return
    product = await get_product(product_id)
    if not product:
        await message.answer("المنتج غير موجود.")
        return
    await state.update_data(product_id=product_id)
    await state.set_state(EditProductStates.field)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="الاسم", callback_data="editf_name")],
            [InlineKeyboardButton(text="السعر", callback_data="editf_price")],
            [InlineKeyboardButton(text="الوصف", callback_data="editf_description")],
            [InlineKeyboardButton(text="إخفاء / إظهار", callback_data="editf_toggle")],
        ]
    )
    await message.answer("اختر الحقل الذي تريد تعديله:", reply_markup=kb)

@dp.callback_query(F.data.startswith("editf_"), EditProductStates.field)
async def edit_product_field(callback: CallbackQuery, state: FSMContext):
    field = callback.data.split("_", 1)[1]
    data = await state.get_data()
    product_id = data["product_id"]
    if field == "toggle":
        product = await get_product(product_id)
        new_value = 0 if product[5] == 1 else 1
        await update_product_field(product_id, "is_active", new_value)
        await state.clear()
        await callback.message.edit_text("✅ تم تغيير حالة المنتج بنجاح.", reply_markup=admin_products_menu())
        await callback.answer()
        return
    await state.update_data(field=field)
    await state.set_state(EditProductStates.value)
    await callback.message.edit_text("أرسل القيمة الجديدة:")
    await callback.answer()

@dp.message(EditProductStates.value)
async def edit_product_value(message: Message, state: FSMContext):
    data = await state.get_data()
    value = message.text.strip()
    if data["field"] == "price":
        try:
            value = float(value)
        except Exception:
            await message.answer("أرسل سعرًا صحيحًا.")
            return
    await update_product_field(data["product_id"], data["field"], value)
    await state.clear()
    await message.answer("✅ تم تعديل المنتج بنجاح.", reply_markup=admin_products_menu())

@dp.callback_query(F.data == "admin_delete_product")
async def admin_delete_product(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(DeleteProductStates.product_id)
    await callback.message.edit_text("أرسل ID المنتج الذي تريد حذفه نهائيًا:")
    await callback.answer()

@dp.message(DeleteProductStates.product_id)
async def delete_product_message(message: Message, state: FSMContext):
    product_id = safe_int(message.text)
    if not product_id:
        await message.answer("أرسل رقم ID صحيح.")
        return
    product = await get_product(product_id)
    if not product:
        await message.answer("المنتج غير موجود.")
        return
    await delete_product(product_id)
    await state.clear()
    await message.answer("✅ تم حذف المنتج نهائيًا.", reply_markup=admin_products_menu())

@dp.callback_query(F.data == "admin_new_orders")
async def admin_new_orders(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    orders = await get_new_orders()
    if not orders:
        await callback.message.edit_text("لا توجد طلبات جديدة.", reply_markup=admin_menu())
        await callback.answer()
        return
    text = "📥 الطلبات الجديدة:\n\n"
    rows = []
    for oid, username, full_name, product_name, quantity, total_price, payment_method, proof_file_id, status, created_at in orders[:10]:
        text += f"#{oid} | {product_name} | العدد: {quantity} | الإجمالي: {format_price(total_price)} | {payment_method} | {status_label(status)}\n"
        rows.append([
            InlineKeyboardButton(text=f"✅ قبول #{oid}", callback_data=f"approve_{oid}"),
            InlineKeyboardButton(text=f"❌ رفض #{oid}", callback_data=f"reject_{oid}"),
        ])
    rows.append([InlineKeyboardButton(text="🔙 لوحة الأدمن", callback_data="admin_panel")])
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

@dp.callback_query(F.data.startswith("approve_"))
async def approve_order_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    if not order:
        await callback.answer("الطلب غير موجود", show_alert=True)
        return
    _, user_id, product_id, quantity, total_price, payment_method, payment_proof_file_id, status, product_name = order
    items = await get_order_stock_items(order_id)
    await approve_order(order_id)
    secrets = "\n".join([f"<code>{secret}</code>" for _, secret in items])
    try:
        await bot.send_message(
            user_id,
            f"✅ تم قبول طلبك #{order_id}\n\n"
            f"المنتج: {product_name}\n"
            f"الكمية: {quantity}\n"
            f"الإجمالي: {format_price(total_price)}\n\n"
            f"بيانات التسليم:\n{secrets}"
        )
    except Exception:
        pass
    await callback.answer("تم قبول الطلب")
    await admin_new_orders(callback)

@dp.callback_query(F.data.startswith("reject_"))
async def reject_order_cb(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    order_id = int(callback.data.split("_")[1])
    order = await get_order(order_id)
    if not order:
        await callback.answer("الطلب غير موجود", show_alert=True)
        return
    _, user_id, product_id, quantity, total_price, payment_method, payment_proof_file_id, status, product_name = order
    await reject_order(order_id)
    try:
        await bot.send_message(
            user_id,
            f"❌ تم رفض طلبك #{order_id}\n"
            f"تم إرجاع الكمية إلى المخزون.\n"
            f"إذا كانت لديك مشكلة، تواصل مع الدعم:\n{support_url()}"
        )
    except Exception:
        pass
    await callback.answer("تم رفض الطلب")
    await admin_new_orders(callback)

@dp.callback_query(F.data == "admin_cancelled_orders")
async def admin_cancelled_orders(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    orders = await get_cancelled_orders()
    if not orders:
        await callback.message.edit_text("لا توجد طلبات ملغية.", reply_markup=admin_menu())
        await callback.answer()
        return
    text = "❌ الطلبات الملغية:\n\n"
    for oid, full_name, product_name, quantity, total_price, status, created_at in orders:
        text += f"#{oid} | {product_name} | العدد: {quantity} | الإجمالي: {format_price(total_price)} | {full_name} | {status_label(status)}\n"
    await callback.message.edit_text(text, reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    ids = await get_all_users_ids()
    await callback.message.edit_text(f"👥 عدد مستخدمي البوت: {len(ids)}", reply_markup=admin_menu())
    await callback.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(BroadcastStates.text)
    await callback.message.edit_text("أرسل الرسالة الجماعية الآن:")
    await callback.answer()

@dp.message(BroadcastStates.text)
async def do_broadcast(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    ids = await get_all_users_ids()
    sent = 0
    for uid in ids:
        try:
            await bot.send_message(uid, message.text)
            sent += 1
        except Exception:
            pass
    await state.clear()
    await message.answer(f"✅ تم إرسال الرسالة إلى {sent} مستخدم.", reply_markup=admin_menu())

async def main():
    await init_db()
    asyncio.create_task(expire_old_reservations())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
