from datetime import datetime
from typing import Optional

import sqlalchemy.orm
from sqlalchemy import Column, Integer, String, create_engine, TIMESTAMP, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import SingletonThreadPool

from config import time_format, db_path

engine = create_engine(db_path, echo=True, poolclass=SingletonThreadPool, connect_args={"check_same_thread": False})

Base = declarative_base()

Session = sessionmaker(bind=engine)
session: sqlalchemy.orm.Session = Session()


class Temperature(Base):
    __tablename__ = 'temperature'
    id = Column(Integer, primary_key=True)
    date = Column(TIMESTAMP, default=datetime.now)
    device = Column(String)
    temperature = Column(Float)

    def __init__(self, device: str, temperature: float, date: Optional[datetime] = None):
        if date:
            self.date = date
        self.device = device
        self.temperature = temperature

    def __repr__(self):
        return f"<Temperature('{self.device}', '{self.date}', '{self.temperature}')>"

    def serialize(self) -> dict:
        return {'device': self.device,
                'time': self.date.strftime(time_format),
                'value': self.temperature}


class Humidity(Base):
    __tablename__ = 'humidity'
    id = Column(Integer, primary_key=True)
    date = Column(TIMESTAMP, default=datetime.now)
    device = Column(String)
    humidity = Column(Float)

    def __init__(self, device: str, humidity: float, date: Optional[datetime] = None):
        if date:
            self.date = date
        self.device = device
        self.humidity = humidity

    def __repr__(self):
        return f"<Humidity('{self.device}', '{self.date}', '{self.humidity}')>"

    def serialize(self) -> dict:
        return {'device': self.device,
                'time': self.date.strftime(time_format),
                'value': self.humidity}


class CarbonicGas(Base):
    __tablename__ = 'carbonic_gas'
    id = Column(Integer, primary_key=True)
    date = Column(TIMESTAMP, default=datetime.now)
    device = Column(String)
    carbonic_gas = Column(Float)

    def __init__(self, device: str, carbonic_gas: int, date: Optional[datetime] = None):
        if date:
            self.date = date
        self.device = device
        self.carbonic_gas = carbonic_gas

    def __repr__(self):
        return f"<Carbonic gas('{self.device}', '{self.date}', '{self.carbonic_gas}')>"

    def serialize(self) -> dict:
        return {'device': self.device,
                'time': self.date.strftime(time_format),
                'value': self.carbonic_gas}


class Pressure(Base):
    __tablename__ = 'atmospheric_pressure'
    id = Column(Integer, primary_key=True)
    date = Column(TIMESTAMP, default=datetime.now)
    device = Column(String)
    pressure = Column(Float)

    def __init__(self, device: str, atmospheric_pressure: float, date: Optional[datetime] = None):
        if date:
            self.date = date
        self.device = device
        self.pressure = atmospheric_pressure

    def __repr__(self):
        return f"<Atmospheric pressure('{self.device}', '{self.date}', '{self.pressure}')>"

    def serialize(self) -> dict:
        return {'device': self.device,
                'time': self.date.strftime(time_format),
                'value': self.pressure}


# Создание таблицы
Base.metadata.create_all(engine)


def write_to_database(device_id: str, parameter: str, value: float | int, date: Optional[datetime] = None) -> None:
    """
    Запись значения в базу данных
    :param device_id: идентификатор девайса
    :param parameter: имя параметра
    :param value: значение параметра
    """

    if parameter == 'temperature':
        o = Temperature(device_id, value, date)
    elif parameter == 'humidity':
        o = Humidity(device_id, value, date)
    elif parameter == 'carbonic_gas':
        o = CarbonicGas(device_id, value, date)
    elif parameter == 'pressure':
        o = Pressure(device_id, value, date)
    else:
        raise ValueError()
    session.add(o)
    session.commit()
    return o.id


def read_last_from_database(parameter: str):
    """
    Чтение последней записи из базы данных
    :param parameter: имя параметра
    """
    if parameter == 'temperature':
        q = session.query(Temperature)[-1]
    elif parameter == 'humidity':
        q = session.query(Humidity)[-1]
    elif parameter == 'carbonic_gas':
        q = session.query(CarbonicGas)[-1]
    elif parameter == 'pressure':
        q = session.query(Pressure)[-1]
    else:
        raise ValueError()
    return q


if __name__ == '__main__':
    print(read_last_from_database('temperature'))
