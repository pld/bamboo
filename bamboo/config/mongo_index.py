from pymongo import Connection, ASCENDING


def ensure_indexing():
    """Ensure that bamboo models are indexed."""
    connection = Connection('localhost', 27017)
    db = connection.bamboo_dev
    calculations = db.calculations
    datasets = db.datasets
    observations = db.observations
    datasets.ensure_index([
        ("BAMBOO_RESERVED_KEY_dataset_id", ASCENDING),
        ("BAMBOO_RESERVED_KEY_dataset_observation_id", ASCENDING)])
    observations.ensure_index([
        ("BAMBOO_RESERVED_KEY_dataset_observation_id", ASCENDING)])
    calculations.ensure_index([("BAMBOO_RESERVED_KEY_dataset_id", ASCENDING)])


if __name__ == "__main__":
    ensure_indexing()
