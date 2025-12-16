import uvicorn
import sqlite3
import asyncio
import hashlib
import os
from pydantic import BaseModel
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
from bs4 import BeautifulSoup
from typing import Optional
import aiohttp
from asyncio import to_thread
import time
from datetime import datetime
import re

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
                       UIN TEXT PRIMARY KEY,
                       status TEXT DEFAULT 'Проверка',
                       cheker INTEGER DEFAULT -1,
                       last_checked TEXT DEFAULT '2000-01-01 00:00:00',
                       date_sales TEXT DEFAULT NULL
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS account
                   (
                       login TEXT  PRIMARY KEY,
                       password TEXT
                   )
                   ''')
    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS Sales
                   (
                       UIN TEXT PRIMARY KEY,
                       date_sales TEXT DEFAULT 'Проверка'
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


def SetSales(Uins):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for UIN in Uins:
            cursor.execute("SELECT COUNT(*) FROM Sales WHERE UIN = ?", (UIN,))
            if cursor.fetchone()[0] > 0:
                cursor.execute("UPDATE Sales SET date_sales = 'Проверка' WHERE UIN = ?", (UIN,))
            else:
                cursor.execute("INSERT INTO Sales (UIN, date_sales) VALUES (?, ?)", (UIN, 'Проверка'))
        conn.commit()
        conn.close()
        return "Данные успешно загружены"
    except Exception as e:
        print(f"Ошибка при добавлении UIN: {e}")
        return "Не удалось загрузить данные"


def GetSalesDate():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT UIN, date_sales FROM Sales")
    result = [{'uin': row[0], 'date': row[1]} for row in cursor.fetchall()]
    conn.close()
    return result


def DeleteSales(Uins):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        for UIN in Uins:
            cursor.execute("DELETE FROM Sales WHERE UIN = ?", (UIN,))
        conn.commit()
        conn.close()
        return "Данные успешно удалены"
    except Exception as e:
        print(f"Ошибка при удалении UIN: {e}")
        return "Не удалось удалить данные"


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


def get_uins_for_checking_batch(limit=100):
    """Получить батч UINов для проверки (исключая 'Продан'), сортируя по времени последней проверки"""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT UIN
        FROM UINs
        WHERE status != 'Продан'
        ORDER BY last_checked ASC
        LIMIT ?
    ''', (limit,))
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return result


# === Sales: функции работы с датой продажи ===
def get_sales_uins_for_checking_batch(limit=100):
    """Получить батч UINов из Sales, где date_sales = 'Проверка'."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT UIN
        FROM Sales
        WHERE date_sales = 'Проверка'
        LIMIT ?
    ''', (limit,))
    result = [row[0] for row in cursor.fetchall()]
    conn.close()
    return result


def update_sales_date(uin: str, sale_date: str):
    """Обновить дату продажи в Sales для UIN."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Sales SET date_sales = ? WHERE UIN = ?", (sale_date, uin))
    conn.commit()
    conn.close()


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
                        target = soup.find('p', class_='check-result-row__value', string=lambda s: s and 'Продано' in s)
                        status = "Продан" if target else "НеПродан"

                        # Обновление БД в отдельном потоке
                        await to_thread(update_uin_status, uin, status)
                        print(f"Воркер {worker_id}: UIN {uin} — статус '{status}'")

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    print(f"Воркер {worker_id}: Ошибка запроса для UIN {uin}: {e} → повтор...")

                    # ПРОВЕРЯЕМ СТАТУС ПЕРЕД ПОВТОРНЫМ ДОБАВЛЕНИЕМ
                    current_status = await to_thread(get_uin_status_from_db, uin)

                    if not current_status or current_status != 'Продан':
                        # При ошибке сбрасываем счетчик запросов для текущего прокси
                        if has_proxies and 'current_proxy' in locals():
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
chek_sales_task: Optional[asyncio.Task] = None
shutdown_event: Optional[asyncio.Event] = None
proxy_hash: str = ""
BATCH_SIZE = 100


# === Sales Proxy Pool (10 использований -> отдых 30с; ошибка -> отдых 30с) ===
class SalesProxyState:
    def __init__(self, proxy_str: str, cooldown_until: float = 0):
        self.proxy_str = proxy_str
        self.use_count = 0
        self.cooldown_until = cooldown_until
        self.last_used = 0.0

    def can_use(self, now: float) -> bool:
        return now >= self.cooldown_until

    def mark_used(self, now: float):
        self.use_count += 1
        self.last_used = now
        if self.use_count >= 10:
            # 10 кругов (использований) — отдых 30 секунд
            self.cooldown_until = now + 30
            self.use_count = 0
            print(
                f"[Sales] Прокси {self.get_ip()} уходит на отдых до {time.strftime('%H:%M:%S', time.localtime(self.cooldown_until))}"
            )

    def mark_error(self, now: float):
        # Любая ошибка — отдых 30 секунд, повтор с другим прокси
        self.cooldown_until = now + 30
        self.use_count = 0
        print(
            f"[Sales] Прокси {self.get_ip()} отправлен на отдых из-за ошибки до {time.strftime('%H:%M:%S', time.localtime(self.cooldown_until))}"
        )

    def get_proxy_config(self) -> tuple[Optional[str], Optional[aiohttp.BasicAuth]]:
        """
        Возвращает (proxy_url, proxy_auth) для aiohttp.
        Важно: многие HTTP-прокси не принимают user:pass в URL и рвут соединение.
        Поэтому proxy_url формируем как http://ip:port, а логин/пароль передаем через proxy_auth.
        """
        try:
            user_pass, ip_port = self.proxy_str.split("@")
            user, password = user_pass.split(":", 1)
            ip, port = ip_port.split(":")
            proxy_url = f"http://{ip}:{port}"
            proxy_auth = aiohttp.BasicAuth(user, password)
            return proxy_url, proxy_auth
        except Exception:
            return None, None

    def get_ip(self) -> str:
        try:
            ip_port = self.proxy_str.split("@")[1]
            return ip_port.split(":")[0]
        except Exception:
            return self.proxy_str


class SalesProxyPool:
    def __init__(self):
        self._states_by_proxy: dict[str, SalesProxyState] = {}
        self._order: list[str] = []

    def refresh(self, proxy_list: list[str]):
        new_set = set(proxy_list)
        # удалить отсутствующие
        for old in list(self._states_by_proxy.keys()):
            if old not in new_set:
                del self._states_by_proxy[old]
        # добавить новые
        for p in proxy_list:
            if p not in self._states_by_proxy:
                self._states_by_proxy[p] = SalesProxyState(p)
        self._order = [p for p in proxy_list if p in self._states_by_proxy]

    def acquire(self, now: float) -> tuple[Optional[SalesProxyState], Optional[float]]:
        """Вернуть (proxy_state, wait_seconds). Если все на отдыхе — wait_seconds до ближайшего освобождения."""
        if not self._order:
            return None, None
        available = [self._states_by_proxy[p] for p in self._order if self._states_by_proxy[p].can_use(now)]
        if available:
            available.sort(key=lambda x: x.last_used)
            return available[0], 0.0
        soonest = min(self._states_by_proxy[p].cooldown_until for p in self._order)
        return None, max(0.0, soonest - now)


async def fetch_sales_date_from_giis(
    uin: str,
    session: aiohttp.ClientSession,
    proxy: Optional[str],
    proxy_auth: Optional[aiohttp.BasicAuth],
) -> Optional[str]:
    """
    1) GET https://dmdk.ru/ -> достать sessid из <input type="hidden" name="sessid" value="...">
    2) POST https://dmdk.ru/local/templates/dmdk_new/services_ajax/ajax.php
       data: sessid=<...>, type="jewel", id=<uin>
    Возвращает дату продажи в формате dd.mm.yyyy (строка) или None если дата не найдена.
    """
    if not proxy:
        raise ValueError("[Sales] proxy is required but was not provided/parsed")

    timeout = aiohttp.ClientTimeout(total=30)
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "accept-language": "ru,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    }

    # Step 1: get sessid
    async with session.get(
        "https://dmdk.ru/",
        proxy=proxy,
        proxy_auth=proxy_auth,
        timeout=timeout,
        headers=headers,
    ) as r:
        if r.status != 200:
            raise aiohttp.ClientResponseError(
                request_info=r.request_info,
                history=r.history,
                status=r.status,
                message=f"GET / returned {r.status}",
                headers=r.headers,
            )
        html = await r.text()

    soup = BeautifulSoup(html, "html.parser")
    sessid_input = soup.find("input", {"type": "hidden", "name": "sessid"})
    sessid = sessid_input.get("value").strip() if sessid_input and sessid_input.get("value") else ""
    if not sessid:
        raise ValueError("[Sales] sessid not found in dmdk.ru HTML")

    # Step 2: POST ajax
    post_url = "https://dmdk.ru/local/templates/dmdk_new/services_ajax/ajax.php"
    data = {"sessid": sessid, "type": "jewel", "id": uin}

    post_headers = dict(headers)
    post_headers.update(
        {
            "origin": "https://dmdk.ru",
            "x-requested-with": "XMLHttpRequest",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
    )

    async with session.post(
        post_url,
        data=data,
        proxy=proxy,
        proxy_auth=proxy_auth,
        timeout=timeout,
        headers=post_headers,
    ) as r:
        if r.status != 200:
            raise aiohttp.ClientResponseError(
                request_info=r.request_info,
                history=r.history,
                status=r.status,
                message=f"POST ajax returned {r.status}",
                headers=r.headers,
            )
        result_html = await r.text()

    # Parse: find "Дата продажи" and extract dd.mm.yyyy nearby
    soup = BeautifulSoup(result_html, "html.parser")
    label = soup.find("span", string=lambda s: s and "Дата продажи" in s)
    if not label:
        # fallback: regex on whole response
        m = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", result_html)
        return m.group(0) if m else None

    container = label.find_parent("div", class_=lambda c: c and "row" in c) or label.parent
    text = container.get_text(" ", strip=True) if container else soup.get_text(" ", strip=True)
    m = re.search(r"\b\d{2}\.\d{2}\.\d{4}\b", text)
    return m.group(0) if m else None


# === Основной процесс проверки UIN ===
async def chek_uins(shutdown: asyncio.Event):
    global proxy_hash
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
                uins = await to_thread(get_uins_for_checking_batch, BATCH_SIZE)

                if uins:
                    print(f"Загружаем батч UINов: {len(uins)} записей")
                    for uin in uins:
                        await queue.put(uin)
                else:
                    print("Нет UINов для проверки (все, возможно, проданы)")
                    await asyncio.sleep(10)

        except Exception as e:
            print(f"Ошибка при чтении UIN из БД: {e}")

        await asyncio.sleep(5)

    for _ in range(len(tasks)):
        await queue.put(None)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    print("Все воркеры остановлены.")


async def chek_sales_dates(shutdown: asyncio.Event):
    """
    Фоновая задача:
    - читает Sales, где date_sales='Проверка'
    - для каждого UIN делает до 3 попыток запроса к ГИИС через разные прокси
    - если все прокси на отдыхе — ждёт первого освободившегося
    - при успехе пишет дату продажи в Sales.date_sales
    """
    pool = SalesProxyPool()
    local_proxy_hash = ""
    SALES_BATCH_SIZE = 100

    async with aiohttp.ClientSession() as session:
        while not shutdown.is_set():
            # Подхватываем изменения proxy.txt (и при первом запуске тоже)
            try:
                current_hash = await to_thread(get_proxy_hash)
                if current_hash != local_proxy_hash:
                    current_proxies = await to_thread(load_proxies)
                    pool.refresh(current_proxies)
                    local_proxy_hash = current_hash
                    print(f"[Sales] Обновлён список прокси: {len(current_proxies)} шт.")
            except Exception as e:
                print(f"[Sales] Ошибка обновления прокси: {e}")

            # Берём UIN'ы на проверку
            try:
                uins = await to_thread(get_sales_uins_for_checking_batch, SALES_BATCH_SIZE)
            except Exception as e:
                print(f"[Sales] Ошибка чтения Sales из БД: {e}")
                await asyncio.sleep(5)
                continue

            if not uins:
                await asyncio.sleep(5)
                continue

            for uin in uins:
                if shutdown.is_set():
                    break

                success = False
                attempts_made = 0
                while attempts_made < 3 and not shutdown.is_set():
                    # Берём доступный прокси или ждём ближайший
                    proxy_state = None
                    req_proxy = None
                    req_proxy_auth = None

                    while not shutdown.is_set():
                        now = asyncio.get_event_loop().time()
                        proxy_state, wait_seconds = pool.acquire(now)

                        if proxy_state is not None:
                            # форматируем прокси, и только если он валиден — считаем "круг" (использование)
                            req_proxy, req_proxy_auth = proxy_state.get_proxy_config()
                            if not req_proxy:
                                proxy_state.mark_error(now)
                                print(f"[Sales] UIN {uin} — прокси не распарсился, отправлен на отдых, берём другой")
                                proxy_state = None
                                req_proxy = None
                                req_proxy_auth = None
                                continue

                            proxy_state.mark_used(now)
                            break

                        # Нет прокси вообще
                        if wait_seconds is None:
                            print("[Sales] Прокси не настроены (proxy.txt пустой). Ожидание 10 сек...")
                            await asyncio.sleep(10)
                            continue

                        # Все на отдыхе — ждём первого освободившегося
                        wait_seconds = max(0.5, float(wait_seconds))
                        await asyncio.sleep(wait_seconds)

                    if shutdown.is_set() or not req_proxy:
                        break

                    attempts_made += 1
                    proxy_ip = proxy_state.get_ip() if proxy_state else "unknown"
                    print(f"[Sales] UIN {uin} — попытка {attempts_made}/3 через прокси {proxy_ip}")

                    try:
                        sale_date = await fetch_sales_date_from_giis(uin, session, req_proxy, req_proxy_auth)
                        if sale_date:
                            await to_thread(update_sales_date, uin, sale_date)
                            print(f"[Sales] UIN {uin} — дата продажи '{sale_date}' записана в БД")
                            success = True
                            break
                        else:
                            print(f"[Sales] UIN {uin} — попытка {attempts_made}/3: дата не получена")
                    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                        now = asyncio.get_event_loop().time()
                        if proxy_state is not None:
                            proxy_state.mark_error(now)
                        print(f"[Sales] UIN {uin} — попытка {attempts_made}/3: ошибка запроса: {e}")
                        continue
                    except Exception as e:
                        now = asyncio.get_event_loop().time()
                        if proxy_state is not None:
                            proxy_state.mark_error(now)
                        print(f"[Sales] UIN {uin} — попытка {attempts_made}/3: ошибка обработки: {e}")
                        continue

                if not success:
                    # Оставляем date_sales='Проверка' — повторим в следующем цикле
                    pass

            await asyncio.sleep(1)


# === FastAPI ===
class ModelGet(BaseModel):
    UINs: list[str]
    login: str
    password: str


# === Lifespan: управление жизненным циклом приложения ===
@asynccontextmanager
async def lifespan(app: FastAPI):
    global chek_uins_task, chek_sales_task, shutdown_event
    shutdown_event = asyncio.Event()
    chek_uins_task = asyncio.create_task(chek_uins(shutdown_event))
    chek_sales_task = asyncio.create_task(chek_sales_dates(shutdown_event))

    yield

    shutdown_event.set()
    tasks = [t for t in [chek_uins_task, chek_sales_task] if t]
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


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

@app.get("/api/SetSalesDate")
async def APISetSalesDate(body: ModelGet):
    if check_user(body.login, body.password):
        return SetSales(body.UINs)
    else:
        return 505

@app.get("/api/DeleteSalesDate")
async def APIDeleteSalesDate(body: ModelGet):
    if check_user(body.login, body.password):
        return DeleteSales(body.UINs)
    else:
        return 505

@app.get("/api/GetSalesDate")
async def APIGetSalesDate():
    return GetSalesDate()

# Запуск сервера
#if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host="0.0.0.0",
        port=8000,
        reload=True
    )