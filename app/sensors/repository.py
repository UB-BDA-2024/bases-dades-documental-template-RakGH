from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional

from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)#, latitude=sensor.latitude, longitude=sensor.longitude
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = data
    data_str = f"{db_sensordata.temperature}/{db_sensordata.humidity}/{db_sensordata.battery_level}/{db_sensordata.last_seen}"
    return redis._client.set(sensor_id, data_str)


def get_data(redis: Session, sensor_id: int, sensor_name: str) -> schemas.Sensor:
    #db_sensordata = data
    data_str = redis._client.get(sensor_id)

    db_sensordata = data_str.decode().split("/")
    return {
        "id": sensor_id,
        "name": sensor_name,
        "temperature": float(db_sensordata[0]),
        "humidity": float(db_sensordata[1]),
        "battery_level": float(db_sensordata[2]),
        "last_seen": db_sensordata[3]
    }

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor