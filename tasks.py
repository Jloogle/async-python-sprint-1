import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Process
from statistics import mean
from typing import List

import pandas as pd

from api_client import YandexWeatherAPI
from base_models import DetailWeather, ForecastModel
from utils import CITIES, GOOD_WEATHER, MAX_HOUR, MIN_HOUR

logger = logging.getLogger()

logging.basicConfig(
    filename='application-log.log',
    filemode='w',
    format='%(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class DataFetchingTask:
    """Получение данных через API"""
    @staticmethod
    def fetch(city):
        return ForecastModel.parse_obj(YandexWeatherAPI().get_forecasting(city))


class DataCalculationTask(Process):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    @staticmethod
    def check_condition(condition):
        return condition in GOOD_WEATHER

    @staticmethod
    def _calc_temp_and_condition(hours: List[DetailWeather]):
        temp = []
        count_good_hours = 0
        if len(hours) == 0:
            raise ValueError
        for hour in hours:
            if MIN_HOUR < int(hour.hour) <= MAX_HOUR:
                temp.append(hour.temp)
                if DataCalculationTask.check_condition(hour.condition):
                    count_good_hours += 1
        return {
            'avg_temp': mean(temp),
            'good_hours': count_good_hours
        }

    @staticmethod
    def _calculate(city):
        all_temp = []
        all_good_hours = []
        temp = dict()
        weather = dict()
        data_city = DataFetchingTask.fetch(city)
        for data in data_city.forecasts:
            try:
                day_result = (
                    DataCalculationTask._calc_temp_and_condition(data.hours)
                )
                all_temp.append(day_result['avg_temp'])
                all_good_hours.append(day_result['good_hours'])
                temp[data.date] = day_result['avg_temp']
                weather[data.date] = day_result['good_hours']
            except (ZeroDivisionError, ValueError):
                logger.error(
                    msg=f'Недостаточно данных по городу "{city}" '
                        f'на дату "{data.date}". Дата исключена')
                continue
            temp['Среднее'] = mean(all_temp)
            weather['Среднее'] = mean(all_good_hours)
        return [
            {'Город/день': city, 'Погода': 'Температура, среднее', **temp},
            {'Погода': 'Без осадков, часов', **weather}
        ]

    def run(self):
        with ThreadPoolExecutor() as pool:
            future = pool.map(self._calculate, CITIES.keys())
            for item in future:
                self.queue.put(item)
                logger.info(f'В очередь добавлен "{item}"')


class DataAggregationTask(Process):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def run(self):
        df_lists = []
        while True:
            if self.queue.empty():
                logger.info('Очередь пуста')
                df = pd.DataFrame.from_dict(df_lists)
                df.to_csv('result.csv')
                logger.info('Результат сохранен в csv файл')
                break
            item = self.queue.get()
            df_lists.extend(item)
            logger.info(f'"{item}" получен из очереди')


class DataAnalyzingTask:
    @staticmethod
    def analyzing():
        df = pd.read_csv('result.csv')
        df = (df.merge(df.groupby('Город/день')
                       .agg({'Среднее': 'sum'})
                       .rename(columns={'Среднее': 'Рейтинг'}),
                       on='Город/день'))
        df['Рейтинг'] = df['Рейтинг'].rank(ascending=False)
        min_rating = df['Рейтинг'] == df['Рейтинг'].min()
        result = list(df[min_rating]['Город/день'].unique())
        logger.info(f'Анализ данных прошел успешно, результат: "{result}"')
        print('Рекомендовано отправиться в такие города, '
              f'как: {", ".join(result)}')
