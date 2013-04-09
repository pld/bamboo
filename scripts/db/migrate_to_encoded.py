import argparse

from pybamboo.connection import Connection
from pybamboo.dataset import Dataset


DEV_BAMBOO_URL = 'http://dev.bamboo.io/'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', help='The dataset ID to migrate')
    args = parser.parse_args()


def main():
    dataset_url = "http://bamboo.io/datasets/%s.csv" % args.dataset

    dev_connect = Connection(url=DEV_BAMBOO_URL)

    dataset = Dataset(url=dataset_url, connection=dev_connect)
    print dataset.id


main()
