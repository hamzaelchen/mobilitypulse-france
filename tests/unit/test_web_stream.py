"""SSE broadcaster: what is pushed, when, and how slow clients and database outages behave."""

import asyncio

from web.stream import Broadcaster


class Source:
    """Scriptable state reader: set .state, or .error to simulate an outage."""

    def __init__(self, state):
        self.state = state
        self.error = None
        self.reads = 0

    def __call__(self):
        self.reads += 1
        if self.error:
            raise self.error
        return self.state


def state(bikes=10, freshness="live", stations=None, priorities=None):
    stations = stations if stations is not None else {"1": {"station_id": "1", "bikes": 3}}
    return {
        "kpis": {"bikes": bikes, "freshness": freshness, "cause": None, "server_time": "t"},
        "stations": stations,
        "priorities": priorities or [],
        "transfers": [],
    }


def drain(subscription):
    out = []
    while not subscription.queue.empty():
        out.append(subscription.queue.get_nowait())
    return out


def run(coro):
    return asyncio.run(coro)


def test_a_new_subscriber_gets_the_full_snapshot_without_waiting():
    async def scenario():
        broadcaster = Broadcaster(Source(state()))
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        ((name, data),) = drain(sub)
        assert name == "snapshot" and data["stations"] and data["kpis"]["bikes"] == 10

    run(scenario())


def test_nothing_is_pushed_while_nothing_changes():
    async def scenario():
        source = Source(state())
        broadcaster = Broadcaster(source)
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        drain(sub)
        await broadcaster.tick()
        await broadcaster.tick()
        assert drain(sub) == [] and source.reads == 3

    run(scenario())


def test_only_the_changed_stations_are_pushed():
    async def scenario():
        source = Source(
            state(
                stations={
                    "1": {"station_id": "1", "bikes": 3},
                    "2": {"station_id": "2", "bikes": 5},
                }
            )
        )
        broadcaster = Broadcaster(source)
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        drain(sub)
        source.state = state(
            bikes=9,
            stations={"1": {"station_id": "1", "bikes": 2}, "2": {"station_id": "2", "bikes": 5}},
        )
        await broadcaster.tick()
        ((name, data),) = drain(sub)
        assert name == "update" and data["kpis"]["bikes"] == 9
        assert data["changed"] == [{"station_id": "1", "bikes": 2}] and data["removed"] == []
        assert "priorities" not in data  # unchanged lists are not resent

    run(scenario())


def test_a_changed_priority_list_is_pushed():
    async def scenario():
        source = Source(state(priorities=[{"station_id": "1", "rank": 1}]))
        broadcaster = Broadcaster(source)
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        drain(sub)
        source.state = state(priorities=[{"station_id": "2", "rank": 1}])
        await broadcaster.tick()
        ((_, data),) = drain(sub)
        assert data["priorities"] == [{"station_id": "2", "rank": 1}]

    run(scenario())


def test_a_freshness_change_alone_is_pushed():
    async def scenario():
        source = Source(state(freshness="live"))
        broadcaster = Broadcaster(source)
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        drain(sub)
        source.state = state(freshness="stale")
        await broadcaster.tick()
        ((name, data),) = drain(sub)
        assert name == "update" and data["kpis"]["freshness"] == "stale"
        assert data["changed"] == []

    run(scenario())


def test_a_database_outage_is_announced_once_then_the_full_state_comes_back():
    async def scenario():
        source = Source(state())
        broadcaster = Broadcaster(source)
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        drain(sub)

        source.error = RuntimeError("db down")
        await broadcaster.tick()
        await broadcaster.tick()
        events = drain(sub)
        assert [name for name, _ in events] == ["status"]
        assert events[0][1] == {"freshness": "unknown", "cause": "database_unavailable"}

        source.error = None
        await broadcaster.tick()
        ((name, data),) = drain(sub)
        assert name == "snapshot" and data["stations"]  # resync, not a partial diff

    run(scenario())


def test_a_slow_client_is_resynchronised_instead_of_blocking_the_others():
    async def scenario():
        source = Source(state(bikes=1))
        broadcaster = Broadcaster(source, queue_size=2)
        await broadcaster.tick()
        slow, fast = broadcaster.subscribe(), broadcaster.subscribe()
        drain(slow), drain(fast)
        for bikes in (2, 3, 4, 5):
            source.state = state(bikes=bikes)
            await broadcaster.tick()
            drain(fast)
        events = drain(slow)
        assert any(name == "snapshot" for name, _ in events)  # it was resynchronised
        assert events[-1][1]["kpis"]["bikes"] == 5  # and ends on the current state

    run(scenario())


def test_unsubscribed_clients_receive_nothing_and_idle_means_no_reads():
    async def scenario():
        source = Source(state())
        broadcaster = Broadcaster(source, interval=0.01)
        sub = broadcaster.subscribe()
        broadcaster.unsubscribe(sub)
        assert broadcaster.subscribers == 0
        task = asyncio.create_task(broadcaster.run())
        await asyncio.sleep(0.05)
        task.cancel()
        assert source.reads == 0  # nobody is listening: the database is left alone

    run(scenario())


def test_polling_runs_while_somebody_listens():
    async def scenario():
        source = Source(state())
        broadcaster = Broadcaster(source, interval=0.01)
        sub = broadcaster.subscribe()
        task = asyncio.create_task(broadcaster.run())
        await asyncio.sleep(0.1)
        task.cancel()
        assert source.reads >= 2
        assert any(name == "snapshot" for name, _ in drain(sub))

    run(scenario())


def test_ages_that_grow_with_the_clock_do_not_trigger_updates():
    async def scenario():
        source = Source(state())
        source.state["kpis"].update(
            data_age_minutes=0.1, data_age_seconds=6, heartbeat_age_seconds=1
        )
        broadcaster = Broadcaster(source)
        await broadcaster.tick()
        sub = broadcaster.subscribe()
        drain(sub)
        source.state = state()
        source.state["kpis"].update(
            data_age_minutes=0.2, data_age_seconds=12, heartbeat_age_seconds=2
        )
        await broadcaster.tick()
        assert drain(sub) == []

    run(scenario())
