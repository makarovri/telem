from fastapi import FastAPI, Request, HTTPException
from clickhouse_driver import Client
import json

client = Client("clickhouse",
                user="default",
                password="",
                database="arnavi") # подключение к ClickHouse

columns = [
    "name",
    "ain.4",
    "battery.voltage",
    "can.abs.failure.indicator.status",
    "can.adblue.level",
    "can.airbag.indicator.status",
    "can.axle.weight.5",
    "can.battery.indicator.status",
    "can.car.closed.remote.status",
    "can.check.engine.indicator.status",
    "can.coolant.level.low.indicator.status",
    "can.driver.seatbelt.indicator.status",
    "can.dynamic.ignition.status",
    "can.electronic.power.control.status",
    "can.engine.ignition.status",
    "can.engine.load.level",
    "can.engine.motorhours",
    "can.engine.rpm",
    "can.engine.status",
    "can.engine.temperature",
    "can.esp.indicator.status",
    "can.front.left.door.status",
    "can.front.right.door.status",
    "can.fuel.consumed",
    "can.fuel.level",
    "can.fuel.level.low.indicator.status",
    "can.glow.plug.indicator.status",
    "can.handbrake.indicator.status",
    "can.handbrake.status",
    "can.high.beam.status",
    "can.hood.status",
    "can.ignition.key.status",
    "can.lights.failure.indicator.status",
    "can.log.number",
    "can.low.beam.status",
    "can.module.sleep.mode",
    "can.movement.state.bitmask",
    "can.oil.pressure.indicator.status",
    "can.parking.lights.status",
    "can.passenger.seatbelt.indicator.status",
    "can.pedal.brake.status",
    "can.rear.left.door.status",
    "can.rear.right.door.status",
    "can.soot.filter.indicator.status",
    "can.stop.indicator.status",
    "can.throttle.pedal.level",
    "can.tire.pressure.low.status",
    "can.trunk.status",
    "can.vehicle.mileage",
    "can.vehicle.speed",
    "can.warning.indicator.status",
    "can.wear.brake.pads.indicator.status",
    "can.webasto.status",
    "car.remote.control.state",
    "channel.id",
    "device.id",
    "device.name",
    "device.type.id",
    "din",
    "engine.ignition.status",
    "external.powersource.voltage",
    "factory.alarm.actuated.status",
    "gsm.mcc",
    "gsm.mnc",
    "gsm.signal.level",
    "ident",
    "peer",
    "position.altitude",
    "position.direction",
    "position.latitude",
    "position.longitude",
    "position.satellites",
    "position.speed",
    "protocol.id",
    "server.timestamp",
    "timestamp",
    "vehicle.mileage"
]

app = FastAPI()

@app.post("/")
async def handler(request: Request):
    try:
        json_bytes = await request.body()

        json_data = json.loads(json_bytes)

        data_to_insert = []
        for i in json_data:
            for col in columns:
                if col in list(i.keys()):
                    msg_array.append(i.get(col))
                else:
                    msg_array.append(None)
            data_to_insert.append(msg_array)
        
        print(data_to_insert)
        cnt = client.execute(f"INSERT INTO flespi_data (*) VALUES", data_to_insert, types_check=True)

        return {"data_to_insert":"OK"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error inserting item: {e}")
