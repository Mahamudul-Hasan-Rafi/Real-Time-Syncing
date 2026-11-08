"""Every exception the service raises on purpose, in one hierarchy.

    CdcEsSyncError
    ├── ConfigError                 config.yaml or an index file is wrong (exit code 2)
    │   └── MappingError            an index definition is inconsistent with itself or its SQL
    │       └── ScanError           the document SQL cannot be read by the scanner
    ├── InvalidEventError           a Kafka message is not a usable Debezium change event   -> dead letter
    ├── MissingKeyError             an event lacks the columns that locate its documents     -> dead letter
    ├── DocumentRejectedError       a row cannot become a valid document                     -> dead letter
    └── ShutdownRequested           SIGINT/SIGTERM interrupted a wait; unwinds to the main loop
"""
from __future__ import annotations


class CdcEsSyncError(Exception):
    """Base class of every error raised on purpose by cdc-es-sync."""


class ConfigError(CdcEsSyncError):
    """The configuration cannot be loaded or is invalid."""


class MappingError(ConfigError):
    """An index definition (field map, keys, lookups) is inconsistent."""


class ScanError(MappingError):
    """The document SQL has a shape the scanner does not understand."""


class InvalidEventError(CdcEsSyncError):
    """A Kafka message is not a usable Debezium change event."""


class MissingKeyError(CdcEsSyncError):
    """An event does not carry the columns the mapping needs, usually a missing supplemental log group."""


class DocumentRejectedError(CdcEsSyncError):
    """The row cannot become a valid document. It goes to the dead letter topic."""


class ShutdownRequested(CdcEsSyncError):  # noqa: N818 (a signal, not an error)
    """A stop was requested while waiting. Uncommitted events are re-read on the next start."""
