from typing import List

from pydantic import BaseModel


class DetailWeather(BaseModel):
    hour: str
    temp: int
    condition: str


class Weather(BaseModel):
    date: str
    hours: List[DetailWeather]


class ForecastModel(BaseModel):
    forecasts: List[Weather]