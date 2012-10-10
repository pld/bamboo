import json

from pandas import concat, Series

from bamboo.models.dataset import Dataset
from bamboo.lib.io import import_dataset
from bamboo.lib.utils import call_async


class MergeError(Exception):
    """
    For errors while merging datasets.
    """
    pass


def merge_dataset_ids(dataset_ids):
    # try to get each of the datasets
    dataset_ids = json.loads(dataset_ids)
    result = None

    datasets = [Dataset.find_one(dataset_id) for dataset_id in dataset_ids]
    new_dframe = _merge_datasets(datasets)

    # save the resulting dframe as a new dataset
    new_dataset = Dataset()
    new_dataset.save()
    call_async(import_dataset, new_dataset, new_dataset, dframe=new_dframe)

    # store the child dataset ID with each parent
    for dataset in datasets:
        dataset.add_merged_dataset(new_dataset)

    # return the new dataset ID
    return new_dataset


def _merge_datasets(datasets):
    """
    Merge two or more datasets.  Raises a MergeError if less than 2 datasets
    are provided.
    """
    if len(datasets) < 2:
        raise MergeError(
            'merge requires 2 datasets (found %s)' % len(datasets))

    dframes = []
    for dataset in datasets:
        dframes.append(dataset.dframe().add_parent_column(dataset.dataset_id))

    return concat(dframes, ignore_index=True)
