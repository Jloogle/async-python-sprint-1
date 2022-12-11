from typing import List

from pydantic import BaseModel


class DetailWeather(BaseModel):
    hour: str
    temp: int
    condition: str


class Weather(BaseModel):
    date: str
    hours: List[DetailWeather]


class Province(BaseModel):
    id: int
    name: str


class GeoObject(BaseModel):
    province: Province


class ForecastModel(BaseModel):
    geo_object: GeoObject
    forecasts: List[Weather]


class CityData(BaseModel):
    city: str
    date: str
    avg_temp: float
    condition_hours: int
