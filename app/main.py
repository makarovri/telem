from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    data: str

@app.post("/")
async def insert_data(item: Item):
    try:
        print(item.data)
        
        return {
            "message": "Item added successfully"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")
