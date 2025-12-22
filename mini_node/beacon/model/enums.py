from enum import StrEnum


class EntityType(StrEnum):
    """Beacon entities that are expected to be configured in beacon-common.yaml.

    Entity type information is included in Beacon responses.

    Note that not all entities are included – just the one that are supported.
    If a new entry is added to the configuration file, its entity ID must be
    also added here.
    """
    INFO = "info"
    CONFIGURATION = "configuration"
    ENTRY_TYPES = "entryTypes"
    MAP = "map"
    FILTERING_TERM = "filteringTerm"
    DATASET = "dataset"
    GENOMIC_VARIANT = "genomicVariant"
    INDIVIDUAL = "individual"


class Granularity(StrEnum):
    """Beacon response granularity options."""
    boolean = "boolean"
    count = "count"
    record = "record"
