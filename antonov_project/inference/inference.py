from fastapi import FastAPI, UploadFile, File
import random, logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [inference] %(message)s"
)
logger = logging.getLogger("inference")

app = FastAPI()

@app.post("/infer")
async def infer(file: UploadFile = File(...)):
    data = await file.read()
    boxes = [
        {
          "x1": random.randint(0, 100),
          "y1": random.randint(0, 100),
          "x2": random.randint(100, 200),
          "y2": random.randint(100, 200),
          "confidence": round(random.random(), 2)
        }
        for _ in range(2)
    ]
    return {"boxes": boxes}
