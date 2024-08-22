from fastapi import FastAPI

app = FastAPI()


@app.get("/")
async def home():
    print("YES")
    return {"data": "Hello World"}
