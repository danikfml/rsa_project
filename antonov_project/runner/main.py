import os, cv2, json, asyncio, logging, time
from pathlib import Path
import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, errors as kerr

LVL = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LVL,
                    format="%(asctime)s %(levelname)s [runner] %(message)s")
for lib in ("httpx", "httpcore.http11"): logging.getLogger(lib).setLevel(logging.WARNING)

KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INFER_URL = os.getenv("INFERENCE_URL", "http://inference:8000/infer")
HB_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "5"))
STRIDE = int(os.getenv("FRAME_STRIDE", "3"))
DEF_VIDEO = "/app/videos/sample_video.mp4"

_tasks = {}


async def _k(cls, *a, **kw):
    back = 1
    while True:
        c = cls(*a, **kw)
        try:
            await c.start(); return c
        except kerr.KafkaConnectionError:
            await c.stop();
            await asyncio.sleep(back);
            back = min(back * 2, 15)


async def send_hb(prod, sid):
    while True:
        await prod.send("heartbeat", json.dumps({"scenario_id": sid}).encode())
        await asyncio.sleep(HB_INTERVAL)


async def loop(sid: int, vid: str):
    if not Path(vid).exists(): vid = DEF_VIDEO
    cap = cv2.VideoCapture(vid)
    if not cap.isOpened(): logging.error("cannot open %s", vid); return
    prod = await _k(AIOKafkaProducer, bootstrap_servers=KAFKA_BOOT)
    await prod.send_and_wait("runner_states", json.dumps({"scenario_id": sid, "state": "active"}).encode())
    hb_task = asyncio.create_task(send_hb(prod, sid))

    async with httpx.AsyncClient(timeout=5) as cli:
        fid = 0
        try:
            while True:
                ok, frame = cap.read()
                if not ok: cap.set(cv2.CAP_PROP_POS_FRAMES, 0); continue
                if fid % STRIDE == 0:
                    _, buf = cv2.imencode(".jpg", frame)
                    boxes = []
                    try:
                        r = await cli.post(INFER_URL,
                                           files={"file": ("f.jpg", buf.tobytes(), "image/jpeg")})
                        boxes = r.json().get("boxes", [])
                    except Exception as e:
                        logging.warning("infer err: %s", e)
                    await prod.send("predictions", json.dumps({
                        "scenario_id": sid, "frame_id": fid, "boxes": boxes}).encode())
                fid += 1;
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            pass
        finally:
            hb_task.cancel();
            await hb_task
            await prod.send_and_wait("runner_states", json.dumps({"scenario_id": sid, "state": "inactive"}).encode())
            cap.release();
            await prod.stop()


async def bus():
    cons = await _k(AIOKafkaConsumer, "runner_cmd",
                    bootstrap_servers=KAFKA_BOOT, group_id="runner")
    try:
        async for m in cons:
            c = json.loads(m.value);
            sid, act = c["scenario_id"], c["action"]
            vp = c.get("video_path", DEF_VIDEO)
            if act == "init_startup" and sid not in _tasks:
                logging.info("START sid=%s", sid)
                _tasks[sid] = asyncio.create_task(loop(sid, vp))
            elif act == "init_shutdown" and sid in _tasks:
                logging.info("STOP sid=%s", sid)
                _tasks[sid].cancel();
                await asyncio.gather(_tasks[sid], return_exceptions=True)
                _tasks.pop(sid, None)
    finally:
        await cons.stop()


async def main():
    while True:
        try:
            await bus()
        except Exception as e:
            logging.error("bus error %s retry", e);
            await asyncio.sleep(5)


if __name__ == "__main__": asyncio.run(main())
