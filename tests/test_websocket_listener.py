import pytest

from trio_binance.ws.trio_stream import TrioApiManager


class DummySocket:
    def __init__(self, manager, path, messages):
        self._messages = list(messages)
        self._manager = manager
        self._path = path

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def recv(self):
        if not self._messages:
            return None
        return self._messages.pop(0)


@pytest.mark.trio
async def test_start_listener_receives_and_calls_callback(monkeypatch):
    received = []

    async def cb(msg):
        received.append(msg)

    mgr = TrioApiManager()
    socket = DummySocket(mgr, "/dummy", [{"e": "test", "msg": "hello"}])

    # run the listener directly (it will exit when socket returns None)
    await mgr.start_listener(socket, "/dummy", cb)

    assert received == [{"e": "test", "msg": "hello"}]
