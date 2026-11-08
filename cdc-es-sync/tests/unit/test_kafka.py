"""Consumer lag as reported by the cdc_consumer_lag metric."""
from cdc_es_sync.connectors.kafka import partition_lag


def test_lag_from_the_fetch_position():
    assert partition_lag(low=0, high=100, position=90, committed=-1) == 10


def test_idle_partition_after_restart_uses_the_committed_offset():
    # nothing fetched since the assignment (position unknown): the group is fully caught up, lag is 0, not 100
    assert partition_lag(low=0, high=100, position=-1001, committed=100) == 0


def test_idle_partition_with_unread_events_counts_from_the_committed_offset():
    assert partition_lag(low=0, high=100, position=-1001, committed=95) == 5


def test_group_that_never_committed_starts_at_the_beginning():
    assert partition_lag(low=40, high=100, position=-1001, committed=-1001) == 60


def test_lag_is_never_negative():
    assert partition_lag(low=0, high=10, position=12, committed=-1) == 0
