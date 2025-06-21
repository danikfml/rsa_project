import os, asyncio, json, logging
from typing import Dict, Optional
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, errors as kerr
from aiokafka.errors import NodeNotReadyError
from sqlalchemy import (
    Column, Integer, String, Text, Boolean, DateTime,
    select, update
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(level=LOGLEVEL, format="%(asctime)s %(levelname)s [api] %(message)s")

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./api.db")
KAFKA_BOOT = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

Base = declarative_base()
engine = create_async_engine(DATABASE_URL, echo=False, future=True)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class Scenario(Base):
    __tablename__ = "scenarios"
    id = Column(Integer, primary_key=True)
    state = Column(String, nullable=False)
    video_path = Column(String, nullable=False)


class Outbox(Base):
    __tablename__ = "outbox"
    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    sent = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)


class ScenarioCreate(BaseModel):
    video_path: str


class ScenarioChange(BaseModel):
    action: str


class ScenarioInfo(BaseModel):
    id: int
    state: str
    video_path: str


async def _retry_kafka(cls, *args, **kwargs):
    backoff = 1
    while True:
        client = cls(*args, **kwargs)
        try:
            await client.start()
            logging.info("%s started", cls.__name__)
            return client
        except kerr.KafkaConnectionError as e:
            logging.warning("KafkaConnectionError: %s; retry in %ss", e, backoff)
            await client.stop()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 15)


producer: Optional[AIOKafkaProducer] = None
tasks: list[asyncio.Task] = []
PRED_CACHE: Dict[str, list] = {}

app = FastAPI()


@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    global producer
    producer = await _retry_kafka(AIOKafkaProducer, bootstrap_servers=KAFKA_BOOT)
    tasks.extend([
        asyncio.create_task(_outbox_sender()),
        asyncio.create_task(_states_listener()),
        asyncio.create_task(_predictions_listener())
    ])


@app.on_event("shutdown")
async def on_shutdown():
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if producer:
        await producer.stop()


@app.post("/scenario/", response_model=ScenarioInfo)
async def create_scenario(body: ScenarioCreate):
    async with Session() as session:
        scen = Scenario(state="inactive", video_path=body.video_path)
        session.add(scen)
        await session.commit()
        await session.refresh(scen)
        logging.info("Created scenario id=%s", scen.id)
        return ScenarioInfo(id=scen.id, state=scen.state, video_path=scen.video_path)


@app.post("/scenario/{sid}", response_model=ScenarioInfo)
async def change_state(sid: int, body: ScenarioChange):
    if body.action not in ("init_startup", "init_shutdown"):
        raise HTTPException(400, "Invalid action")
    async with Session() as session:
        scen = await session.get(Scenario, sid)
        if not scen:
            raise HTTPException(404, "Scenario not found")
        session.add(Outbox(
            topic="commands",
            message=json.dumps({
                "scenario_id": sid,
                "action": body.action,
                "video_path": scen.video_path
            })
        ))
        await session.commit()
        logging.info("Enqueued command %s for scenario %s", body.action, sid)
        return ScenarioInfo(id=scen.id, state=scen.state, video_path=scen.video_path)


@app.get("/scenario/{sid}", response_model=ScenarioInfo)
async def get_status(sid: int):
    async with Session() as session:
        scen = await session.get(Scenario, sid)
        if not scen:
            raise HTTPException(404, "Scenario not found")
        return ScenarioInfo(id=scen.id, state=scen.state, video_path=scen.video_path)


@app.get("/prediction/{sid}")
async def get_prediction(sid: int):
    return PRED_CACHE.get(str(sid), [])


async def _outbox_sender():
    global producer
    while True:
        async with Session() as session:
            pending = (await session.execute(
                select(Outbox).where(Outbox.sent.is_(False))
            )).scalars().all()
            if pending:
                logging.info("Outbox: %d pending", len(pending))
            for row in pending:
                try:
                    await producer.send_and_wait(row.topic, row.message.encode())
                    row.sent = True
                    session.add(row)
                    await session.commit()
                    logging.info("Outbox sent id=%s topic=%s", row.id, row.topic)
                except (kerr.KafkaConnectionError, NodeNotReadyError) as e:
                    logging.warning("Outbox send failed (%s); recreating producer", e)
                    await producer.stop()
                    producer = await _retry_kafka(AIOKafkaProducer, bootstrap_servers=KAFKA_BOOT)
                    break
                except Exception as e:
                    logging.error("Outbox unexpected error id=%s: %s", row.id, e)
                    break
        await asyncio.sleep(1)


async def _states_listener():
    cons = await _retry_kafka(
        AIOKafkaConsumer, "states",
        bootstrap_servers=KAFKA_BOOT, group_id="api-states",
        auto_offset_reset="earliest"
    )
    try:
        async for msg in cons:
            data = json.loads(msg.value)
            async with Session() as session:
                await session.execute(
                    update(Scenario)
                    .where(Scenario.id == data["scenario_id"])
                    .values(state=data["state"])
                )
                await session.commit()
                logging.info("State update: %s → %s", data["scenario_id"], data["state"])
    finally:
        await cons.stop()


async def _predictions_listener():
    cons = await _retry_kafka(
        AIOKafkaConsumer, "predictions",
        bootstrap_servers=KAFKA_BOOT, group_id="api-pred",
        auto_offset_reset="earliest"
    )
    try:
        async for msg in cons:
            pl = json.loads(msg.value)
            sid = str(pl["scenario_id"])
            PRED_CACHE.setdefault(sid, []).append(pl)
    finally:
        await cons.stop()
