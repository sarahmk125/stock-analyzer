import logging

from app.lib.data_retriever import DataRetriever


class Manager(object):
    def __init__(self):
        self.data_retriever = DataRetriever()

    def runner(self, custom_stock=None, custom_period_start=None, custom_period_end=None):
        print('[Manager] Running task...')
        if custom_stock and custom_period_start:
            self.data_retriever.custom_retrieve(stock=custom_stock, period1=custom_period_start, period2=custom_period_end)
        else:
            self.data_retriever.run_stocks_from_sheet()
