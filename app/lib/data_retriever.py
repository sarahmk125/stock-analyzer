import pandas as pd
import requests
import io
import logging
import app.constants as constants
import dateutil.relativedelta as relativedelta

from datetime import datetime, time, timedelta
from time import sleep
from app.lib.google_sheet_integration import GoogleSheetIntegration


class DataRetriever(object):
    def __init__(self):
        self.google_sheet_integration = GoogleSheetIntegration()

    def _get_current_datetime(self):
        return datetime.combine(datetime.today(), time.min)

    def _get_epoch(self, input_datetime):
        return int(input_datetime.timestamp())

    def _get_current_epoch(self):
        return self._get_epoch(self._get_current_datetime())

    def _get_previous_epoch_days(self, days, period_end):
        return self._get_epoch(period_end - timedelta(days=days))

    def _get_previous_epoch_months(self, months, period_end):
        return self._get_epoch(period_end - relativedelta.relativedelta(months=months))

    def _get_previous_epoch_years(self, years, period_end):
        return self._get_epoch(period_end - relativedelta.relativedelta(years=years))

    def custom_retrieve(self, stock, period1, period2=None, interval='1d', events='history'):
        print(f'[DataRetriever] Getting data for: stock {stock}, {period1} to {period2}, interval: {interval}, events: {events}...')
        # Clean stock name
        stock = stock.strip()

        if not period2:
            period2 = self._get_current_epoch()

        url = f'{constants.YAHOO_BASE_URL}{stock}?period1={period1}&period2={period2}&interval={interval}&events={events}'
        response = requests.get(url)

        if not response.ok:
            # Retry one time due to page not found
            print('[DataRetriever] Failure occurred, waiting 10 seconds then retrying once...')
            sleep(10)

            response = requests.get(url)
            if not response.ok:
                raise Exception(f'[DataRetriever] Yahoo request error on: Getting data for: stock {stock}, {period1} to {period2}, interval: {interval}. Response: {response.text}')

        data = response.content.decode('utf8')
        df_history = pd.read_csv(io.StringIO(data))
        return df_history
    
    def _calculate_adj_close_percent(self, df_history):
        adj_close_list = df_history['Adj Close'].tolist()

        # NOTE: calculating last close under assumption array is in ascending date order.
        # This is true now, but might want a test around this later.
        period1_close = adj_close_list[0]
        yesterday_close = adj_close_list[len(adj_close_list) - 1]
        diff_percent = round((yesterday_close - period1_close) / period1_close * 100.0, 2)
        return diff_percent

    def run_stocks_from_sheet(self):
        print(f'[DataRetriever] Getting stock data from sheet...')
        df_sheet = self.google_sheet_integration.sheet_to_df(
            spreadsheet=constants.GOOGLE_SPREADSHEET_ID,
            worksheet=constants.GOOGLE_SPREADSHEET_TAB)
        
        # Get 7 days, 1M, 3M, 1Y, 5Y changes; variables needed for all
        ticks = list(df_sheet['ticker'])
        current_dateime = self._get_current_datetime()
        period2 = self._get_epoch(current_dateime)

        # NOTE: this can be more efficient and parallelized, but doing it simply for now.
        # For each tick (stock), get the 5 date fields necessary and add.
        # Also, we could get all 5 years of data then do the calculations to limit api calls; unnecessary right now.
        week_list = []
        month_list = []
        three_month_list = []
        one_year_list = []
        five_year_list = []

        for tick in ticks:
            print(f'[DataRetriever] Processing tick {tick}...')
            # 7D
            period1 = self._get_previous_epoch_days(7, current_dateime)
            df_history = self.custom_retrieve(tick, period1, period2)
            week_list.append(self._calculate_adj_close_percent(df_history))

            # 1M
            period1 = self._get_previous_epoch_months(1, current_dateime)
            df_history = self.custom_retrieve(tick, period1, period2)
            month_list.append(self._calculate_adj_close_percent(df_history))

            # 3M
            period1 = self._get_previous_epoch_months(3, current_dateime)
            df_history = self.custom_retrieve(tick, period1, period2)
            three_month_list.append(self._calculate_adj_close_percent(df_history))

            # 1Y
            period1 = self._get_previous_epoch_years(1, current_dateime)
            df_history = self.custom_retrieve(tick, period1, period2)
            one_year_list.append(self._calculate_adj_close_percent(df_history))

            # 5Y
            period1 = self._get_previous_epoch_years(5, current_dateime)
            df_history = self.custom_retrieve(tick, period1, period2)
            five_year_list.append(self._calculate_adj_close_percent(df_history))
        
        df_sheet['week_change'] = week_list
        df_sheet['month_change'] = month_list
        df_sheet['three_month_change'] = three_month_list
        df_sheet['year_change'] = one_year_list
        df_sheet['five_year_change'] = five_year_list

        print('[DataRetriever] Writing to sheet...')
        self.google_sheet_integration.df_to_sheet(
            constants.GOOGLE_SPREADSHEET_ID,
            constants.GOOGLE_SPREADSHEET_TAB,
            df_sheet)
