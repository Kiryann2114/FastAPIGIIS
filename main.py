import uvicorn
import sqlite3
from pydantic import BaseModel
from fastapi import FastAPI, BackgroundTasks
import asyncio
from contextlib import asynccontextmanager
import requests
from bs4 import BeautifulSoup
import hashlib




conn = sqlite3.connect("DBUin.db")
cursor = conn.cursor()

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


headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


async def chek_uins():
    while True:
        try:
            print("Получаю данные из БД")
            cursor.execute("SELECT UIN FROM UINs WHERE status = false")
            uins = cursor.fetchall()
            for uin in uins:
                uin = uin[0]
                print(f"Проверяю UIN: {uin}")
                response = requests.get(f"https://probpalata.gov.ru/check-uin/?action=check&uin={uin}",
                                        headers=headers, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                data = [p.text.strip() for p in soup.find_all('p', class_='check-result-row__value')if p.text.strip()]
                print(data)
                print(response)
                await asyncio.sleep(10)
                if len(data) >= 5:
                    if data[5] == "Продано":
                        cursor.execute(f"SELECT COUNT(*) FROM UINs WHERE UIN = {uin}")
                        if cursor.fetchone()[0] > 0:
                            cursor.execute(
                                f"UPDATE UINs SET UIN = '{uin}', status = true WHERE UIN = '{uin}'")
                            conn.commit()
                        print(f"Статус UIN {uin}: Продано")
                    else:
                        print(f"Статус UIN {uin}: Не Продано")
                else:
                    print(f"Не получилось проверить. Пропускаю UIN: {uin}")
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