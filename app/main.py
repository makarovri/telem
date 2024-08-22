from fastapi import FastAPI, Request, HTTPException
from clickhouse_driver import Client
import json

client = Client("clickhouse",
                user="default",
                password="",
                database="arnavi") # подключение к ClickHouse

app = FastAPI()

@app.post("/")
async def handler(request: Request):
    try:
        json_bytes = await request.body()

        json_data = json.loads(json_bytes)

        data_to_insert = []
        for i in json_data:
            data_to_insert.append(i.values())
        
        # print(data_to_insert)
        cnt = client.execute(f"INSERT INTO flespi_data (*) VALUES", data_to_insert)

        return {"data_to_insert":"OK"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")
