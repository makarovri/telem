from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI()


@app.post("/")
async def root(request: list):
    try:
        print(body.decode())
        return JSONResponse(content={"request_body": body})
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")
