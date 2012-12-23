import simplejson as json

from celery.task import task
from pandas import concat

from bamboo.models.dataset import Dataset
from bamboo.lib.io import import_dataset
from bamboo.lib.async import call_async


class MergeError(Exception):
    """For errors while merging datasets."""
    pass


def merge_dataset_ids(dataset_ids):
    """Load a JSON array of dataset IDs and start a background merge task.

    :param dataset_ids: An array of dataset IDs to merge.

    :raises: `MergeError` if less than 2 datasets are provided. If a dataset
        cannot be found for a dataset ID it is ignored. Therefore if 2 dataset
        IDs are provided and one of them is bad an error is raised.  However,
        if three dataset IDs are provided and one of them is bad, an error is
        not raised.
    """
    dataset_ids = json.loads(dataset_ids)
    datasets = [Dataset.find_one(dataset_id) for dataset_id in dataset_ids]
    datasets = [dataset for dataset in datasets if dataset.record]

    if len(datasets) < 2:
        raise MergeError(
            'merge requires 2 datasets (found %s)' % len(datasets))

    new_dataset = Dataset()
    new_dataset.save()

    call_async(_merge_datasets_task, new_dataset, datasets)

    return new_dataset


@task
def _merge_datasets_task(new_dataset, datasets):
    """Merge datasets specified by dataset_ids.

    :param new_dataset: The dataset store the merged dataset in.
    :param dataset_ids: A list of IDs to merge into `new_dataset`.
    """
    # check that all datasets are in a 'ready' state
    if any([not dataset.is_ready for dataset in datasets]):
        [dataset.reload() for dataset in datasets]
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
