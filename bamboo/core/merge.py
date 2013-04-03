from celery.task import task
from pandas import concat

from bamboo.core.frame import BambooFrame
from bamboo.lib.async import call_async
from bamboo.models.dataset import Dataset


class MergeError(Exception):
    """For errors while merging datasets."""
    pass


def merge_dataset_ids(dataset_ids, mapping):
    """Load a JSON array of dataset IDs and start a background merge task.

    :param dataset_ids: An array of dataset IDs to merge.

    :raises: `MergeError` if less than 2 datasets are provided. If a dataset
        cannot be found for a dataset ID it is ignored. Therefore if 2 dataset
        IDs are provided and one of them is bad an error is raised.  However,
        if three dataset IDs are provided and one of them is bad, an error is
        not raised.
    """
    datasets = [Dataset.find_one(dataset_id) for dataset_id in dataset_ids]
    datasets = [dataset for dataset in datasets if dataset.record]

    if len(datasets) < 2:
        raise MergeError(
            'merge requires 2 datasets (found %s)' % len(datasets))

    new_dataset = Dataset.create()

    call_async(__merge_datasets_task, new_dataset, datasets, mapping)

    return new_dataset


@task(default_retry_delay=2, ignore_result=True)
def __merge_datasets_task(new_dataset, datasets, mapping):
    """Merge datasets specified by dataset_ids.

    :param new_dataset: The dataset store the merged dataset in.
    :param dataset_ids: A list of IDs to merge into `new_dataset`.
    """
    # check that all datasets are in a 'ready' state
    while any([not dataset.record_ready for dataset in datasets]):
        [dataset.reload() for dataset in datasets]
        raise __merge_datasets_task.retry(countdown=1)

    new_dframe = __merge_datasets(datasets, mapping)

    # save the resulting dframe as a new dataset
    new_dataset.save_observations(new_dframe)

    # store the child dataset ID with each parent
    for dataset in datasets:
        dataset.add_merged_dataset(mapping, new_dataset)


def __merge_datasets(datasets, mapping):
    """Merge two or more datasets."""
    dframes = []

    if not mapping:
        mapping = {}

    for dataset in datasets:
        dframe = dataset.dframe()
        column_map = mapping.get(dataset.dataset_id)

        if column_map:
            dframe = BambooFrame(dframe.rename(columns=column_map))

        dframe = dframe.add_parent_column(dataset.dataset_id)
        dframes.append(dframe)

    return concat(dframes, ignore_index=True)
