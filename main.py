from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from clickhouse_driver import Client

CLICK_HOUSE_HOST = '188.225.27.153'
CLICK_HOUSE_USER = 'default'
CLICK_HOUSE_PASSWORD = ''
CLICK_HOUSE_DB = 'default'

client = Client(CLICK_HOUSE_HOST,
                user=CLICK_HOUSE_USER,
                password=CLICK_HOUSE_PASSWORD,
                database=CLICK_HOUSE_DB) # коннект к ClickHouse

app = FastAPI()


class Item(BaseModel):
    data: str


def parse_data(data):
    data_types = [int, str, str, float, float, float, float, float, int, int, str, int, str, str]

    all_data = []

    for line in data.splitlines():
        data_parts = line.split(',')
        converted_data = [data_type(data_part) for data_type, data_part in zip(data_types, data_parts)]
        all_data.append(converted_data)

    return all_data


@app.post("/")
async def demo_basic_auth_username(item: Item):
    try:
        # SQL-запрос для вставки данных
        cnt = client.execute("INSERT INTO raw_data (*) VALUES",
                       parse_data(item.data))
        return {
            "message": "Item added successfully",
            "count": cnt
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")