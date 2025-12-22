from enum import StrEnum


class BeaconEnvironment(StrEnum):
    """Environment options according to the Beacon specification."""
    prod = "prod"
    test = "test"
    dev = "dev"
    staging = "staging"
