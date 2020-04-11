import argparse
from app.lib.manager import Manager


def runner():
    Manager().runner()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p1', '--period1', dest='period1', required=False, help='Custom start period.')
    parser.add_argument('-p2', '--period2', dest='period2', required=False, help='Custom end period.')
    parser.add_argument('-s', '--stock', dest='stock', required=False, help='Custom stock.')

    args = parser.parse_args()
    runner()