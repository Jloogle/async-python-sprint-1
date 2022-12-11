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
    q = DataFetchingTask()
    future = q.run()
    future_res = [f.result() for f in future]
    process_producer = DataCalculationTask(queue, future_res)
    process_producer.start()
    process_producer.join()
    a = DataAggregationTask(queue)
    result = a.run()
    DataAnalyzingTask(result).run()


if __name__ == "__main__":
    forecast_weather()
