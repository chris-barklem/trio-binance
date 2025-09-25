# trio-binance

A Trio-first port of python-binance — a lightweight async Binance API client adapted to Trio and httpx.

This repository provides an incremental migration from an asyncio-based client to a Trio-friendly implementation. It includes a Trio-native `TrioApiManager`, Trio-friendly helpers, and compatibility shims to make the migration practical.

This repository contains a port of the popular `python-binance` client to Trio.

Credits
- Original project: https://github.com/sammchardy/python-binance

## Highlights

- Async HTTP client using `httpx.AsyncClient` (works under AnyIO/Trio)
- Trio-friendly websocket support and a `TrioApiManager` for running listeners
- Integration and unit tests (use `TRIO_BINANCE_INTEGRATION=1` to enable live network tests)


## Installation

Recommended: create a virtual environment and install the project in editable mode with dev dependencies.

```bash
python -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .[dev]
```

If you only need runtime deps install:

```bash
.venv/bin/python -m pip install -r requirements.txt
```


## Quick usage examples

All examples below assume you are running inside a Trio context (either via `trio.run(...)` or a pytest-trio test).

### HTTP: get symbol info (BTCUSDC)

```python
import trio
from trio_binance.async_client import AsyncClient

async def fetch_symbol():
		client = await AsyncClient.create()
		info = await client.get_symbol_info('BTCUSDC')
		print(info)
		await client.close_connection()

trio.run(fetch_symbol)
```

This prints the full symbol dictionary (filters, precisions, order types, etc.).

### Websocket: receive a single depth update (BTCUSDC)

For quick local debugging we include small integration tests in `tests/` that connect to Binance public streams and print the first message. To run them:

```bash
# Enable integration tests
TRIO_BINANCE_INTEGRATION=1 \
	.venv/bin/python -m pytest tests/test_ws_depth_bids_asks.py::test_depth_socket_btcusdc_bidask -q -s
```

This test uses a short-lived background thread + asyncio websocket to ensure the stream connects cleanly in diverse environments and prints a single depth update.

### Websocket: receive a kline (1m) update

```bash
TRIO_BINANCE_INTEGRATION=1 \
	.venv/bin/python -m pytest tests/test_ws_klines_btcusdc_1m.py::test_kline_socket_btcusdc_1m -q -s
```

The test prints one kline message for `BTCUSDC` and exits.


## Tests

Run the unit test suite (integration tests are skipped by default):

```bash
.venv/bin/python -m pytest -q
```

Integration tests (use live network) are marked with the `integration` marker and are skipped unless you set an environment variable:

```bash
TRIO_BINANCE_INTEGRATION=1 .venv/bin/python -m pytest -k integration -s
```

If you want CI to run integration tests create a separate workflow or job with network access and set `TRIO_BINANCE_INTEGRATION=1` for that job.


## Notes about SSL and local environments

Some environments (corporate proxies, custom certs) may cause SSL validation failures when connecting to public websockets. The integration tests currently accept disabling certificate verification in the test thread to ensure local debugging works; this is a test-only convenience and is not used in library production code.

If you prefer secure default behavior, I can change the tests to make SSL verification optional via an env var (recommended for CI) and restore strict verification by default.


## Development & Contributing

- Follow the repo style and run unit tests locally.
- If you add integration tests, mark them with `@pytest.mark.integration` and add the `TRIO_BINANCE_INTEGRATION` guard.
- Please run `pytest -q` before opening PRs.


## Roadmap / Next Work

- Replace temporary test harnesses with tests that exercise the library's `ReconnectingWebsocket` and `TrioApiManager` directly.
- Add a mock websocket server for deterministic CI tests.
- Harden reconnection/backpressure behavior and add more robust telemetry (metrics/logging).


## License & Credits

This project forks work from `sammchardy/python-binance` and preserves the original license and credits. See the upstream repository for full attribution.

---