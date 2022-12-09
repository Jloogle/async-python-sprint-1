import unittest

from tasks import DataCalculationTask, DataFetchingTask


class TestDataFetching(unittest.TestCase):
    def test_data_fetching_task_success(self):
        task = DataFetchingTask.fetch('MOSCOW')
        self.assertTrue(len(task.forecasts) == 5)
        self.assertTrue(task.forecasts[0].date == '2022-05-26')
        self.assertTrue(len(task.forecasts[0].hours) == 24)

    def test_data_fetching_task_unknown_city(self):
        self.assertRaises(Exception, DataFetchingTask.fetch, 'KHIMKI')


class TestDataCalculation(unittest.TestCase):
    def test_data_calculation_task_success(self):
        task = DataCalculationTask._calculate('MOSCOW')
        self.assertEqual(task[0]['Город/день'], 'MOSCOW')
        self.assertIsNotNone(task[0].get('2022-05-26'))
        #avg_temp
        self.assertEqual(task[0].get('2022-05-26'), 18)
        #count_condition_hours
        self.assertEqual(task[1].get('2022-05-26'), 3)
