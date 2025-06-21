import os
import asyncio
import json
import logging
from datetime import datetime, timedelta

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, errors as kerr
from sqlalchemy import (
    Column, Integer, String, Boolean, Text, DateTime, select
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker

# ────────── Logging ──────────
LOGLEVEL = os.getenv("LOGLEVEL", "INFO").upper()
logging.basicConfig(
    level=LOGLEVEL,
    format="%(asctime)s %(levelname)s [orch] %(message)s"
)

DB_URL = os.getenv("ORCH_DB_URL", "sqlite+aiosqlite:///./orchestrator.db")
KAFKA_ADDR = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HB_TIMEOUT = int(os.getenv("HEARTBEAT_TIMEOUT", "10"))

Base = declarative_base()
engine = create_async_engine(DB_URL, echo=False, future=True)
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class Outbox(Base):
    __tablename__ = "outbox"
    id = Column(Integer, primary_key=True)
    topic = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    sent = Column(Boolean, default=False)
    created = Column(DateTime, default=datetime.utcnow)


class Scenario(Base):
    __tablename__ = "scenarios"
    id = Column(Integer, primary_key=True)
    state = Column(String, nullable=False)
    last_hb = Column(DateTime)
    video_path = Column(String, nullable=False)


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


async def kafka_alive() -> bool:
    host, port = KAFKA_ADDR.split(":")
    try:
        reader, writer = await asyncio.open_connection(host, int(port))
        writer.close()
        await writer.wait_closed()
        return True
    except:
        return False


async def outbox_sender():
    producer = await _retry_kafka(AIOKafkaProducer, bootstrap_servers=KAFKA_ADDR)
    try:
        while True:
            async with Session() as session:
                rows = (await session.execute(
                    select(Outbox).where(Outbox.sent.is_(False))
                )).scalars().all()
                if rows:
                    logging.info("Outbox: %d pending", len(rows))
                for row in rows:
                    try:
                        await producer.send_and_wait(row.topic, row.message.encode())
                        row.sent = True
                        session.add(row)
                        await session.commit()
                        logging.debug("Outbox sent id=%s topic=%s", row.id, row.topic)
                    except Exception as e:
                        logging.warning("Outbox send failed (%s), will retry", e)
                        break
            await asyncio.sleep(1)
    finally:
        await producer.stop()


async def commands_consumer():
    consumer = await _retry_kafka(
        AIOKafkaConsumer, "commands",
        bootstrap_servers=KAFKA_ADDR,
        group_id="orch-commands",
        auto_offset_reset="earliest"
    )
    try:
        async for msg in consumer:
            cmd = json.loads(msg.value)
            sid, action, vp = cmd["scenario_id"], cmd["action"], cmd.get("video_path", "")
            async with Session() as session:
                scen = await session.get(Scenario, sid)
                if not scen:
                    scen = Scenario(id=sid, state="inactive", video_path=vp)

                if action == "init_startup" and scen.state == "inactive":
                    scen.state = "in_startup_processing"
                    scen.last_hb = datetime.utcnow()
                    session.add(scen)
                    session.add_all([
                        Outbox(topic="states", message=json.dumps({"scenario_id": sid, "state": scen.state})),
                        Outbox(topic="runner_cmd", message=json.dumps(cmd))
                    ])
                    await session.commit()
                    logging.info("init_startup → %s for sid=%s", scen.state, sid)

                elif action == "init_shutdown" and scen.state == "active":
                    inter = "in_shutdown_processing"
                    final = "inactive"
                    scen.state = final
                    scen.last_hb = datetime.utcnow()
                    session.add(scen)
                    session.add_all([
                        Outbox(topic="states", message=json.dumps({"scenario_id": sid, "state": inter})),
                        Outbox(topic="runner_cmd", message=json.dumps(cmd)),
                        Outbox(topic="states", message=json.dumps({"scenario_id": sid, "state": final}))
                    ])
                    await session.commit()
                    logging.info("init_shutdown → %s → %s for sid=%s", inter, final, sid)

                else:
                    logging.info("Ignored %s for sid=%s in state=%s", action, sid, scen.state)
    finally:
        await consumer.stop()


async def runner_state_consumer():
    consumer = await _retry_kafka(
        AIOKafkaConsumer, "runner_states",
        bootstrap_servers=KAFKA_ADDR,
        group_id="orch-runner-states",
        auto_offset_reset="earliest"
    )
    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            sid, st = data["scenario_id"], data["state"]
            async with Session() as session:
                scen = await session.get(Scenario, sid)
                if not scen:
                    continue
                scen.state = st
                if st == "active":
                    scen.last_hb = datetime.utcnow()
                session.add(scen)
                session.add(Outbox(topic="states", message=json.dumps({"scenario_id": sid, "state": st})))
                await session.commit()
                logging.info("Runner reported sid=%s → %s", sid, st)
    finally:
        await consumer.stop()


async def heartbeat_consumer():
    consumer = await _retry_kafka(
        AIOKafkaConsumer, "heartbeat",
        bootstrap_servers=KAFKA_ADDR,
        group_id="orch-heartbeat",
        auto_offset_reset="earliest"
    )
    try:
        async for msg in consumer:
            hb = json.loads(msg.value)
            sid = hb["scenario_id"]
            async with Session() as session:
                scen = await session.get(Scenario, sid)
                if scen:
                    scen.last_hb = datetime.utcnow()
                    session.add(scen)
                    await session.commit()
                    logging.debug("Heartbeat sid=%s", sid)
    finally:
        await consumer.stop()


async def watchdog():
    while True:
        if not await kafka_alive():
            logging.debug("Kafka down, skipping watchdog")
            await asyncio.sleep(HB_TIMEOUT / 2)
            continue

        async with Session() as session:
            now = datetime.utcnow()
            for scen in (await session.execute(select(Scenario))).scalars().all():
                if scen.state == "active" and scen.last_hb and \
                        (now - scen.last_hb) > timedelta(seconds=HB_TIMEOUT):
                    logging.warning("Lost hb for sid=%s → enqueue restart", scen.id)
                    shutdown = {"scenario_id": scen.id, "action": "init_shutdown", "video_path": scen.video_path}
                    startup = {"scenario_id": scen.id, "action": "init_startup", "video_path": scen.video_path}
                    session.add_all([
                        Outbox(topic="runner_cmd", message=json.dumps(shutdown)),
                        Outbox(topic="runner_cmd", message=json.dumps(startup))
                    ])
                    await session.commit()
                    logging.info("Watchdog enqueued shutdown/startup for sid=%s", scen.id)
        await asyncio.sleep(HB_TIMEOUT / 2)


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    await asyncio.gather(
        outbox_sender(),
        commands_consumer(),
        runner_state_consumer(),
        heartbeat_consumer(),
        watchdog()
    )


if __name__ == "__main__":
    asyncio.run(main())
