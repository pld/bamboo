import argparse

from pybamboo.dataset import Dataset


BAMBOO_DEV_URL = 'http://dev.bamboo.io/'


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dataset', help='The dataset ID to migrate')
    args = parser.parse_args()
    main()


def main():
    dataset_url = "%sdatasets/%s.csv" % (BAMBOO_DEV_URL, args.dataset)

    dataset = Dataset(url=dataset_url)
    print dataset.id
