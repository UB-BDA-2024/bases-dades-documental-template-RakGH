from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import json
from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: Session) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    mongodb_client.getDatabase('sensors')
    mycol = mongodb_client.getCollection(str(db_sensor.id))
    mydoc = {
    "latitude": sensor.latitude,
    "longitude": sensor.longitude,
    "type": sensor.type,
    "mac_address": sensor.mac_address
    }
    mycol.insert_one(mydoc)
    return db_sensor

def record_data(redis: Session, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    db_sensordata = json.dumps(data.dict())
    #data_str = f"{db_sensordata.temperature}/{db_sensordata.humidity}/{db_sensordata.battery_level}/{db_sensordata.last_seen}"
    return redis._client.set(sensor_id, db_sensordata)


def get_data(redis: Session, sensor_id: int, sensor_name: str) -> schemas.Sensor:
    #db_sensordata = data
    data_str = redis._client.get(sensor_id)

    db_sensordata = data_str.decode().split("/")
    return {
        "id": sensor_id,
        "name": sensor_name
        
        #"temperature": float(db_sensordata[0]),
        #"humidity": float(db_sensordata[1]),
        #"battery_level": float(db_sensordata[2]),
        #"last_seen": db_sensordata[3]
        
    }

def delete_sensor(db: Session, sensor_id: int, mongodb_client: Session):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    mongodb_client.getDatabase('sensors')
    mycol = mongodb_client.getCollection(str(sensor_id))
    mycol.drop()
    return db_sensor