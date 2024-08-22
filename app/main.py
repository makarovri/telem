from fastapi import FastAPI, HTTPException

app = FastAPI()


@app.post("/")
async def insert_data(item):
    try:
        print(item)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")
