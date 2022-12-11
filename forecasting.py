from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue

from tasks import (DataAggregationTask, DataAnalyzingTask,
                   DataCalculationTask, DataFetchingTask)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    queue = Queue()
    with ThreadPoolExecutor() as pool:
        future = pool.map(DataFetchingTask, CITIES)

    for i in future:
        DataCalculationTask(i.run(), queue).run()
    data, rating = DataAggregationTask(queue).run()
    analyzer = DataAnalyzingTask(data, rating)
    analyzer.run()


if __name__ == "__main__":
    forecast_weather()
