import unittest

from tasks import DataFetchingTask


class TestDataFetching(unittest.TestCase):
    def test_data_fetching_task_success(self):
        task = DataFetchingTask()
        task_fetch = task.fetch('MOSCOW')
        self.assertTrue(len(task_fetch.forecasts) == 5)
        self.assertTrue(task_fetch.forecasts[0].date == '2022-05-26')
        self.assertTrue(len(task_fetch.forecasts[0].hours) == 24)

    def test_data_fetching_task_unknown_city(self):
        self.assertRaises(Exception, DataFetchingTask().fetch, 'KHIMKI')
