from fastapi import FastAPI, HTTPException, Request
from clickhouse_driver import Client
from fastapi.responses import JSONResponse

client = Client('clickhouse',
                user='default',
                password='',
                database='arnavi') # подключение к ClickHouse

app = FastAPI()


@app.post("/")
async def root(request: list):
    try:
        # print(body)

        json_data = json.loads(body)

        for i in json_data:
            # SQL-запрос для вставки данных
            cnt = client.execute(f"INSERT INTO flespi_daata {tuple(i.keys())} VALUES", i.values())
        
        return JSONResponse(content={"request_body": body})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")
        
