import csv
from collections import defaultdict
from multiprocessing import Process, Queue
from statistics import mean
from typing import List
from concurrent.futures import ThreadPoolExecutor

from api_client import YandexWeatherAPI
from base_models import CityData, DetailWeather, ForecastModel
from utils import (AVG_STR, AVG_TEMP, CONDITION, COUNT_HOURS, GOOD_WEATHER,
                   MAX_HOUR, MIN_HOUR, logger, CITIES)


class DataFetchingTask:
    """Получение данных через API"""
    def __init__(self):
        self.api = YandexWeatherAPI()

    def fetch(self, city):
        try:
            response = self.api.get_forecasting(city)
            parsed = ForecastModel.parse_obj(response)
            logger.info(
                msg=f'Данные о городе {city}, получены успешно'
            )
            return parsed
        except Exception as e:
            logger.exception(
                msg=f'Получение данных о городе {city},'
                    f' произошло с ошибкой {e}, операция остановлена.'
            )
            raise e

    def run(self):
        with ThreadPoolExecutor() as executor:
            futures = []
            for city in CITIES.keys():
                futures.append(executor.submit(self.fetch, city=city))
            return futures


class DataCalculationTask(Process):
    """Обработка данных полученных от API"""
    def __init__(self, queue: Queue, future_res: List[ForecastModel]):
        super().__init__()
        self.queue = queue
        self.future_res = future_res

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

    def run(self):
        for date in self.future_res:
            for date_info in date.forecasts:
                try:
                    day_result = self._calc_temp_and_condition(
                        date_info.hours
                    )
                    data = CityData(
                        city=date.geo_object.province.name,
                        date=date_info.date,
                        avg_temp=day_result['avg_temp'],
                        condition_hours=day_result['good_hours']
                    )
                    self.queue.put(data)
                except (ZeroDivisionError, ValueError):
                    logger.error(
                        msg=f'Недостаточно данных по городу '
                            f'"{date.geo_object.province.name}" '
                            f'на дату "{date_info.date}". Дата исключена')
                    continue
            logger.info(
                msg='Подсчет данных для города: '
                    f'{date.geo_object.province.name}, завершен'
            )


class DataAggregationTask:
    """Группировка подсчитанных данных о городах"""
    def __init__(self, queue):
        self.queue = queue

    def group_by_city(self):
        group_data = dict()
        while not self.queue.empty():
            item = self.queue.get()
            if item.city not in group_data:
                group_data[item.city] = defaultdict(dict)
                group_data[item.city]['sum'][AVG_TEMP] = 0
                group_data[item.city]['sum'][CONDITION] = 0

            date = item.date
            group_data[item.city][date][AVG_TEMP] = int(
                item.avg_temp)
            group_data[item.city][date][CONDITION] = int(
                item.condition_hours
            )

            group_data[item.city]['sum'][AVG_TEMP] += item.avg_temp
            group_data[item.city]['sum'][CONDITION] += item.condition_hours
        return group_data

    @staticmethod
    def count_points(info):
        return int(info[AVG_TEMP] * 100 + info[CONDITION])

    def count_avg_and_rating(self, group_data):
        rating = defaultdict(list)
        for city, info in group_data.items():
            group_data[city][AVG_STR][AVG_TEMP] = (
                    group_data[city]['sum'][AVG_TEMP] / COUNT_HOURS
            )
            group_data[city][AVG_STR][CONDITION] = (
                    group_data[city]['sum'][CONDITION] / COUNT_HOURS
            )
            group_data[city].pop('sum')

            points = self.count_points(group_data[city][AVG_STR])
            rating[points].append(city)
        return group_data, rating

    def group_table_ordered_by_points(self, group_data, rating) -> list[dict]:
        result = list()
        index = 1

        for points in sorted(rating, reverse=True):
            for city in rating[points]:
                avg = dict()
                good_hours = dict()
                for k, v in group_data[city].items():
                    avg[k] = round(v[AVG_TEMP], 1)
                    good_hours[k] = round(v[CONDITION], 1)
                result.append(
                    {
                        'Город/день': city,
                        '': AVG_TEMP,
                        **avg,
                        'Рейтинг': index,
                    }
                )
                result.append(
                    {
                        'Город/день': None,
                        '': CONDITION,
                        **good_hours,
                        'Рейтинг': None,
                    }
                )
            index += 1
        return result

    def run(self):
        data, rating = self.count_avg_and_rating(self.group_by_city())
        return self.group_table_ordered_by_points(
            data, rating
        )


class DataAnalyzingTask:
    """Анализ сгруппированных данных"""
    def __init__(self, data):
        self.data = data

    def run(self):
        count = 0
        city_rating = []
        data = self.data
        with open('result.csv', 'w') as f:
            writer = csv.DictWriter(f, fieldnames=list(data[0].keys()))
            writer.writeheader()
            writer.writerows(data)
        for city in data:
            if city['Рейтинг'] == 1:
                city_rating.append(city['Город/день'])
                count += 1
        if count == 1:
            msg = ('Рекомендовано отправиться в город, как: '
                   f'{city_rating[0]}')
            print(msg)
            logger.info(msg=msg)
        else:
            msg = ('Рекомендовано отправиться в такие города,'
                   f' как:{", ".join(city_rating)}')
            print(msg)
            logger.info(msg=msg)

        logger.info('Анализ данных прошел успешно')
