from multiprocessing import Queue

from tasks import (DataAggregationTask, DataAnalyzingTask,
                   DataCalculationTask, DataFetchingTask)


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
    # Николай, добрый вечер, не могу найти вас в пачке.
    # Хотел спросить, а то запутался окончательно, правильно ли так? Если нет,
    # то не могли бы вы скинуть ссылку на документацию, где может описано
    # подробнее чем в теории?


if __name__ == "__main__":
    forecast_weather()
