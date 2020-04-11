import pandas as pd
import requests
import io
import logging
import app.constants as constants
import dateutil.relativedelta as relativedelta

from datetime import datetime, time, timedelta


class DataRetriever(object):
    def __init__(self):
        pass

    def _get_current_epoch(self):
        midnight = datetime.combine(datetime.today(), time.min)
        return int(midnight.timestamp())

    def _get_previous_epoch_days(self, days, period_end):
        period_start = period_end - timedelta(days=days)
        return int(period_start.timestamp())

    def _get_previous_epoch_months(self, months, period_end):
        period_start = period_end - relativedelta.relativedelta(months=months)
        return int(period_start.timestamp())

    def _get_previous_epoch_years(self, years, period_end):
        period_start = period_end - relativedelta.relativedelta(years=years)
        return int(period_start.timestamp())

    def custom_retrieve(self, stock, period1, period2=None, interval='1d', events='history'):
        logging.info(f'[DataRetriever] Getting data for: stock {stock}, {period1} to {period2}, interval: {interval}, events: {events}...')
        url = f'{constants.YAHOO_BASE_URL}{stock}?period1={period1}&period2={period2}&interval={interval}&events={events}'
        response = requests.get(url)

        if not response.ok:
            raise Exception(f'[DataRetriever] Yahoo request error. Response: {response.text}')

        data = response.content.decode('utf8')
        df_history = pd.read_csv(io.StringIO(data))
        return df_history
    
    def run_stocks_from_sheet(self):
        pass
