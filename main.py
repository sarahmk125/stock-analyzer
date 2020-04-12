import argparse
from app.lib.manager import Manager


def runner(custom_stock=None, custom_period_start=None, custom_period_end=None):
    Manager().runner(custom_stock=custom_stock, custom_period_start=custom_period_start, custom_period_end=custom_period_end)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p1', '--period1', dest='period1', required=False, help='Custom start period.')
    parser.add_argument('-p2', '--period2', dest='period2', required=False, help='Custom end period.')
    parser.add_argument('-s', '--stock', dest='stock', required=False, help='Custom stock.')

    args = parser.parse_args()
    runner(custom_stock=args.stock, custom_period_start=args.period1, custom_period_end=args.period2)