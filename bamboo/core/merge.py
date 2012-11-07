import simplejson as json

from celery.task import task
from pandas import concat

from bamboo.models.dataset import Dataset
from bamboo.lib.io import import_dataset
from bamboo.lib.utils import call_async


class MergeError(Exception):
    """For errors while merging datasets."""
    pass


def merge_dataset_ids(dataset_ids):
    """Load a JSON array of dataset IDs and start a background merge task."""
    new_dataset = Dataset()
    new_dataset.save()
    dataset_ids = json.loads(dataset_ids)

    call_async(_merge_datasets_task, new_dataset, dataset_ids)

    return new_dataset


@task
def _merge_datasets_task(new_dataset, dataset_ids):
    """Merge datasets specified by dataset_ids.

    Args:

    - new_dataset: The dataset store the merged dataset in.
    - dataset_ids: A list of IDs to merge into *new_dataset*.

    Raises:
        MergeError: If less than 2 datasets are provided.
    """
    datasets = [Dataset.find_one(dataset_id) for dataset_id in dataset_ids]

    if len(datasets) < 2:
        raise MergeError(
            'merge requires 2 datasets (found %s)' % len(datasets))

    # check that all datasets are in a 'ready' state
    if any([not Dataset.find_one(dataset.dataset_id).is_ready for dataset
            in datasets]):
        raise _merge_datasets_task.retry(countdown=1)

    new_dframe = _merge_datasets(datasets)

    # save the resulting dframe as a new dataset
    import_dataset(new_dataset, dframe=new_dframe)

    # store the child dataset ID with each parent
    for dataset in datasets:
        dataset.add_merged_dataset(new_dataset)


def _merge_datasets(datasets):
    """Merge two or more datasets."""
    dframes = []

    for dataset in datasets:
        dframes.append(dataset.dframe().add_parent_column(dataset.dataset_id))

    return concat(dframes, ignore_index=True)
