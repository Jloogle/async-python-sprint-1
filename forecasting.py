import multiprocessing

from tasks import DataAggregationTask, DataAnalyzingTask, DataCalculationTask


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    queue = multiprocessing.Queue()
    process_producer = DataCalculationTask(queue)
    process_consumer = DataAggregationTask(queue)
    process_producer.start()
    process_producer.join()
    process_consumer.start()
    process_consumer.join()
    DataAnalyzingTask().analyzing()


if __name__ == "__main__":
    forecast_weather()
