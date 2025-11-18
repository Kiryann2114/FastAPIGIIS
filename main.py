import uvicorn
import sqlite3
import asyncio
import hashlib
import os
from pydantic import BaseModel
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import requests
from bs4 import BeautifulSoup
from typing import Optional
import aiohttp
from asyncio import to_thread
import time
from datetime import datetime


# === Настройка БД с поддержкой асинхронного доступа ===
# Создаем соединение с БД
def get_db_connection():
    conn = sqlite3.connect("DBUin.db", check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    return conn


# === Создаём таблицы при старте, если их нет ===
def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS UINs
                   (
                       UIN
                       TEXT
                       PRIMARY
                       KEY,
                       status
                       TEXT
                       DEFAULT
                       'Проверка',
                       cheker
                       INTEGER
                       DEFAULT
                       -
                       1,
                       last_checked
                       TEXT
                       DEFAULT
                       '2000-01-01 00:00:00'
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS account
                   (
                       login
                       TEXT
                       PRIMARY
                       KEY,
                       password
                       TEXT
                   )
                   ''')
    # Добавим тестового пользователя: login=test, password=test
    cursor.execute("INSERT OR IGNORE INTO account (login, password) VALUES (?, ?)",
                   ("admin", hashlib.sha256("h6mCbIA0GN".encode()).hexdigest()))
    conn.commit()
    conn.close()


init_db()


# === Загрузка прокси и хэширование содержимого ===
def load_proxies():
    try:
        if not os.path.exists("proxy.txt"):
            return []
        with open("proxy.txt", "r", encoding="utf-8") as file:
            proxies = [line.strip() for line in file if line.strip()]
        return proxies
    except Exception as e:
        print(f"Ошибка при загрузке прокси: {e}")
        return []


def get_proxy_hash():
    try:
        if not os.path.exists("proxy.txt"):
            return ""
        with open("proxy.txt", "rb") as f:
            return hashlib.md5(f.read()).hexdigest()
    except Exception as e:
        print(f"Ошибка хэширования proxy.txt: {e}")
        return ""


# === Основные функции работы с UIN ===
def SetUIN(Uins):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for UIN in Uins:
            cursor.execute("SELECT COUNT(*) FROM UINs WHERE UIN = ?", (UIN,))
            if cursor.fetchone()[0] > 0:
                cursor.execute("UPDATE UINs SET status = 'проверка', last_checked = ? WHERE UIN = ?",
                               (current_time, UIN))
            else:
                cursor.execute("INSERT INTO UINs (UIN, last_checked) VALUES (?, ?)",
                               (UIN, current_time))
        conn.commit()
        conn.close()
        return "Данные успешно загружены"
    except Exception as e:
        print(f"Ошибка при добавлении UIN: {e}")
        return "Не удалось загрузить данные"


def DeleteUIN(Uins):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for UIN in Uins:
            cursor.execute("DELETE FROM UINs WHERE UIN = ?", (UIN,))
        conn.commit()
        conn.close()
        return "Данные успешно удалены"
    except Exception as e:
        print(f"Ошибка при удалении UIN: {e}")
        return "Не удалось удалить данные"


def GetUIN(Uins):
    conn = get_db_connection()
    cursor = conn.cursor()
    arr_uin = []
    for uin in Uins:
        cursor.execute("SELECT UIN, status FROM UINs WHERE UIN = ?", (uin,))
        result = cursor.fetchone()
        if result:
            arr_uin.append({'uin': result[0], 'status': result[1]})
    conn.close()
    return arr_uin


def GetUINStatus():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT UIN, status FROM UINs WHERE status != 'проверка'")
    result = [{'uin': row[0], 'status': row[1]} for row in cursor.fetchall()]
    conn.close()
    return result


def GetAllUINs():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT UIN, status FROM UINs")
    result = [{'uin': row[0], 'status': row[1]} for row in cursor.fetchall()]
    conn.close()
    return result


# === Hash и авторизация ===
def hash_password(password):
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def check_user(login, password):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM account WHERE login = ? AND password = ?",
                   (login, hash_password(password)))
    result = cursor.fetchone()[0] > 0
    conn.close()
    return result


def get_uin_status_from_db(uin):
    """Получить статус UIN из БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM UINs WHERE UIN = ?", (uin,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None


def update_uin_status(uin, status):
    """Обновить статус UIN в БД"""
    conn = get_db_connection()
    cursor = conn.cursor()
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("UPDATE UINs SET status = ?, last_checked = ? WHERE UIN = ?",
                   (status, current_time, uin))
    conn.commit()
    conn.close()


def get_uins_for_checking_batch(limit=100, offset=0):
    """Получить батч UINов для проверки (исключая 'Продан') с пагинацией"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
                   SELECT UIN
                   FROM UINs
                   WHERE status != 'Продан'
                   ORDER BY last_checked ASC
                       LIMIT ?
                   OFFSET ?
                   ''', (limit, offset))
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return result


def get_total_uins_count():
    """Получить общее количество UINов для проверки"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM UINs WHERE status != 'Продан'")
    result = cursor.fetchone()[0]
    conn.close()
    return result


# === Воркер: проверяет UIN через очередь, с прокси или без ===
async def worker(worker_id: int, proxies: list, queue: asyncio.Queue):
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'ru,en;q=0.9',
        'cache-control': 'no-cache',
        'connection': 'keep-alive',
        'content-type': 'application/x-www-form-urlencoded',
        'host': 'probpalata.gov.ru',
        'origin': 'https://probpalata.gov.ru',
        'pragma': 'no-cache',
        'referer': 'https://probpalata.gov.ru/',
        'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "YaBrowser";v="25.8", "Yowser";v="2.5"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36'
    }

    # Структура для хранения состояния прокси
    class ProxyState:
        def __init__(self, proxy_str, cooldown_until=0):
            self.proxy_str = proxy_str
            self.request_count = 0
            self.cooldown_until = cooldown_until  # Время до которого прокси отдыхает
            self.last_used = 0

        def can_use(self, current_time):
            return current_time >= self.cooldown_until

        def mark_used(self, current_time):
            self.request_count += 1
            self.last_used = current_time
            if self.request_count >= 20:
                self.cooldown_until = current_time + 50  # Отдых 50 секунд
                self.request_count = 0
                print(
                    f"Прокси {self.get_ip()} уходит на отдых до {time.strftime('%H:%M:%S', time.localtime(self.cooldown_until))}")

        def get_ip(self):
            try:
                ip_port = self.proxy_str.split("@")[1]
                return ip_port.split(":")[0]
            except:
                return self.proxy_str

    # Инициализация состояний прокси
    proxy_states = [ProxyState(proxy_str) for proxy_str in proxies]
    has_proxies = len(proxies) > 0

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            try:
                uin = await queue.get()
                if uin is None:
                    queue.task_done()
                    break

                # ПРОВЕРЯЕМ ТЕКУЩИЙ СТАТУС UIN ПЕРЕД ОБРАБОТКОЙ
                current_status = await to_thread(get_uin_status_from_db, uin)

                if current_status and current_status == 'Продан':
                    print(f"Воркер {worker_id}: UIN {uin} уже продан, пропускаем")
                    queue.task_done()
                    continue

                print(f"Воркер {worker_id}: проверяю UIN {uin}")
                data = {'action': 'check', 'uin': uin}

                # Выбор доступного прокси
                req_proxy = None
                current_time = asyncio.get_event_loop().time()

                if has_proxies:
                    # Ищем доступный прокси (не в коoldауне)
                    available_proxies = [p for p in proxy_states if p.can_use(current_time)]

                    if available_proxies:
                        # Выбираем прокси, который дольше не использовался
                        available_proxies.sort(key=lambda x: x.last_used)
                        current_proxy = available_proxies[0]

                        # Форматируем прокси для aiohttp
                        try:
                            user_pass, ip_port = current_proxy.proxy_str.split("@")
                            user, password = user_pass.split(":")
                            ip, port = ip_port.split(":")
                            req_proxy = f"http://{user}:{password}@{ip}:{port}"
                        except Exception as e:
                            print(f"Ошибка парсинга прокси: {e}")
                            req_proxy = None

                        # Помечаем прокси как использованный
                        current_proxy.mark_used(current_time)
                        print(
                            f"Воркер {worker_id}: использует прокси {current_proxy.get_ip()} (запрос {current_proxy.request_count}/20)")
                    else:
                        # Все прокси в коoldауне - ждем
                        print(f"Воркер {worker_id}: все прокси в режиме отдыха, ожидание...")
                        await asyncio.sleep(5)
                        await queue.put(uin)
                        queue.task_done()
                        continue
                else:
                    # Режим без прокси
                    req_proxy = None

                try:
                    async with session.post(
                            "https://probpalata.gov.ru/check-uin/",
                            data=data,
                            timeout=aiohttp.ClientTimeout(total=10),
                            proxy=req_proxy
                    ) as response:
                        if response.status != 200:
                            print(f"Воркер {worker_id}: HTTP {response.status} для UIN {uin} → повтор")

                            # ПРОВЕРЯЕМ СТАТУС ПЕРЕД ПОВТОРНЫМ ДОБАВЛЕНИЕМ
                            current_status = await to_thread(get_uin_status_from_db, uin)
                            if not current_status or current_status != 'Продан':
                                await asyncio.sleep(2)
                                await queue.put(uin)
                            else:
                                print(f"Воркер {worker_id}: UIN {uin} стал проданным, не добавляем в очередь")

                            queue.task_done()
                            continue

                        text = await response.text()
                        soup = BeautifulSoup(text, 'html.parser')
                        values = [p.text.strip() for p in soup.find_all('p', class_='check-result-row__value') if
                                  p.text.strip()]

                        status = "НеПродан"
                        if len(values) >= 6:
                            status = "Продан" if values[5] == "Продано" else "НеПродан"

                        # Обновление БД в отдельном потоке
                        await to_thread(update_uin_status, uin, status)
                        print(f"Воркер {worker_id}: UIN {uin} — статус '{status}'")

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    print(f"Воркер {worker_id}: Ошибка запроса для UIN {uin}: {e} → повтор...")

                    # ПРОВЕРЯЕМ СТАТУС ПЕРЕД ПОВТОРНЫМ ДОБАВЛЕНИЕМ
                    current_status = await to_thread(get_uin_status_from_db, uin)

                    if not current_status or current_status != 'Продан':
                        # При ошибке сбрасываем счетчик запросов для текущего прокси
                        if has_proxies and available_proxies:
                            current_proxy.request_count = 0
                            current_proxy.cooldown_until = current_time + 30  # Отдых 30 сек при ошибке
                            print(f"Прокси {current_proxy.get_ip()} отправлен на отдых из-за ошибки")

                        await asyncio.sleep(2)
                        await queue.put(uin)
                    else:
                        print(f"Воркер {worker_id}: UIN {uin} стал проданным, не добавляем в очередь")

                    queue.task_done()
                    continue

                except Exception as e:
                    print(f"Воркер {worker_id}: Ошибка обработки UIN {uin}: {e}")
                    queue.task_done()

            except Exception as e:
                print(f"Воркер {worker_id}: Критическая ошибка: {e}")
                queue.task_done()


# === Глобальные переменные для управления ===
chek_uins_task: Optional[asyncio.Task] = None
shutdown_event: Optional[asyncio.Event] = None
proxy_hash: str = ""
current_batch_offset = 0
BATCH_SIZE = 100


# === Основной процесс проверки UIN ===
async def chek_uins(shutdown: asyncio.Event):
    global proxy_hash, current_batch_offset
    queue = asyncio.Queue()
    tasks = []

    while not shutdown.is_set():
        current_proxies = await to_thread(load_proxies)
        current_hash = await to_thread(get_proxy_hash)

        if current_hash != proxy_hash:
            print("Обнаружены изменения в proxy.txt — перезапуск воркеров...")

            for _ in range(len(tasks)):
                await queue.put(None)
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            tasks.clear()

            num_workers = max(1, len(current_proxies) // 2) if current_proxies else 1
            proxy_pairs = []
            if current_proxies:
                for i in range(0, len(current_proxies), 2):
                    pair = current_proxies[i:i + 2]
                    if len(pair) == 1:
                        pair.append(current_proxies[i])
                    proxy_pairs.append(pair)
            else:
                proxy_pairs = [[]] * num_workers

            tasks = []
            for i in range(num_workers):
                worker_proxies = proxy_pairs[i] if i < len(proxy_pairs) else []
                task = asyncio.create_task(worker(i, worker_proxies, queue))
                tasks.append(task)

            proxy_hash = current_hash
            print(f"Перезапущено {len(tasks)} воркеров с {len(current_proxies)} прокси")

        try:
            # Если очередь почти пустая, загружаем следующую партию UIN
            if queue.qsize() < BATCH_SIZE // 2:
                uins = await to_thread(get_uins_for_checking_batch, BATCH_SIZE, current_batch_offset)

                if uins:
                    print(f"Загружаем батч UINов: {len(uins)} записей, offset={current_batch_offset}")
                    for uin in uins:
                        await queue.put(uin)

                    current_batch_offset += len(uins)
                else:
                    # Если дошли до конца, начинаем сначала
                    print("Достигнут конец базы UINов, начинаем сначала...")
                    current_batch_offset = 0

        except Exception as e:
            print(f"Ошибка при чтении UIN из БД: {e}")

        await asyncio.sleep(5)

    for _ in range(len(tasks)):
        await queue.put(None)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    print("Все воркеры остановлены.")


# === FastAPI ===
class ModelGet(BaseModel):
    UINs: list[str]
    login: str
    password: str


# === Lifespan: управление жизненным циклом приложения ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    global chek_uins_task, shutdown_event
    shutdown_event = asyncio.Event()
    chek_uins_task = asyncio.create_task(chek_uins(shutdown_event))

    yield

    shutdown_event.set()
    if chek_uins_task:
        await chek_uins_task


app = FastAPI(lifespan=lifespan)


@app.post("/api/SetUIN")
async def APISetUIN(body: ModelGet):
    if check_user(body.login, body.password):
        return SetUIN(body.UINs)
    else:
        return 505


@app.post("/api/DeleteUIN")
async def APIDeleteUIN(body: ModelGet):
    if check_user(body.login, body.password):
        return DeleteUIN(body.UINs)
    else:
        return 505


@app.post("/api/GetUIN")
async def APIGetUIN(body: ModelGet):
    if check_user(body.login, body.password):
        return GetUIN(body.UINs)
    else:
        return 505


@app.get("/api/GetUINStatus")
async def APIGetUINStatus():
    return GetUINStatus()


@app.get("/api/GetAllUINs")
async def APIGetAllUINs():
    return GetAllUINs()


#if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host="0.0.0.0",
        port=8000,
        reload=True
    )