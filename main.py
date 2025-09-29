import uvicorn
import sqlite3
from pydantic import BaseModel
from fastapi import FastAPI
import asyncio
from contextlib import asynccontextmanager
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import hashlib

conn = sqlite3.connect("DBUin.db")
cursor = conn.cursor()


def setup_driver():
    """Настройка драйвера для сервера Ubuntu"""
    chrome_options = Options()

    # Основные опции для сервера
    chrome_options.add_argument("--headless=new")  # Новый headless режим
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")  # Ускорит работу
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")

    # Опции для обхода защиты
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    # User agent
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Скрываем автоматизацию
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return driver
    except Exception as e:
        print(f"Ошибка создания драйвера: {e}")
        raise


def SetUIN(Uins):
    try:
        for UIN in Uins:
            cursor.execute(f"SELECT COUNT(*) FROM UINs WHERE UIN = {UIN}")
            if cursor.fetchone()[0] > 0:
                cursor.execute(
                    f"UPDATE UINs SET UIN = '{UIN}', status = false WHERE UIN = '{UIN}'")
            else:
                cursor.execute(
                    f"INSERT INTO UINs (UIN) VALUES ('{UIN}')")
        conn.commit()
        return "Данные успешно загружены"
    except:
        return "Не удалось загрузить данные"


def DeleteUIN(Uins):
    try:
        for UIN in Uins:
            cursor.execute(f"DELETE FROM UINs WHERE UIN = '{UIN}' and status = 1")
        conn.commit()
        return "Данные успешно удалены"
    except:
        return "Не удалось удалить данные"


def GetUINStatus():
    cursor.execute(f'SELECT UIN FROM UINs WHERE status = 1')
    all_uin = cursor.fetchall()
    arr_uin = []
    for uin in all_uin:
        arr_uin.append(uin[0])
    return arr_uin


async def check_uin_with_selenium(uin):
    """Проверка UIN с использованием Selenium"""
    driver = None
    try:
        driver = setup_driver()

        # Переходим на страницу
        driver.get("https://probpalata.gov.ru/check-uin")

        # Ждем загрузки страницы и находим поле для ввода UIN
        wait = WebDriverWait(driver, 10)

        # Ищем поле ввода UIN (возможно, нужно уточнить селектор)
        uin_input = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='uin']"))
        )

        # Вводим UIN
        uin_input.clear()
        uin_input.send_keys(uin)

        # Находим и нажимаем кнопку проверки (возможно, нужно уточнить селектор)
        check_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit'][class='button form-row__button']"))
        )
        check_button.click()

        # Ждем загрузки результатов
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".check-result-row__value"))
        )

        # Парсим результаты
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        data = [p.text.strip() for p in soup.find_all('p', class_='check-result-row__value') if p.text.strip()]

        if len(data) >= 5:
            if data[5] == "Продано":
                cursor.execute(f"SELECT COUNT(*) FROM UINs WHERE UIN = {uin}")
                if cursor.fetchone()[0] > 0:
                    cursor.execute(
                        f"UPDATE UINs SET UIN = '{uin}', status = true WHERE UIN = '{uin}'")
                    conn.commit()
                print(f"Статус UIN {uin}: Продано")
                return True
            else:
                print(f"Статус UIN {uin}: Не Продано")
                return False
        else:
            print(f"Не удалось проверить UIN: {uin}")
            return False

    except Exception as e:
        print(f"Ошибка при проверке UIN {uin}: {e}")
        return False
    finally:
        if driver:
            driver.quit()


async def chek_uins():
    """Основная функция проверки UIN"""
    while True:
        try:
            print("Получаю данные из БД")
            cursor.execute("SELECT UIN FROM UINs WHERE status = false")
            uins = cursor.fetchall()

            for uin in uins:
                uin = uin[0]
                await check_uin_with_selenium(uin)
                await asyncio.sleep(1)  # Пауза между проверками

        except Exception as e:
            print(f"Ошибка в обработке UIN: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(chek_uins())
    yield


app = FastAPI(lifespan=lifespan)


def hash_password(password):
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def check_user(login, password):
    cursor.execute(f"SELECT COUNT(*) FROM account WHERE login = '{login}' and password = '{hash_password(password)}'")
    if cursor.fetchone()[0] > 0:
        return True
    else:
        return False


class ModelGet(BaseModel):
    UINs: list[str]
    login: str
    password: str


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


@app.get("/api/GetUINStatus")
async def APIGetUINStatus():
    return GetUINStatus()


#if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host="0.0.0.0",
        port=8000,
        reload=True
    )