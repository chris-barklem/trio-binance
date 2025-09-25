import os
import pytest

from trio_binance.async_client import AsyncClient

pytestmark = pytest.mark.integration


@pytest.mark.trio
@pytest.mark.integration
async def test_get_exchange_info_btcusdc():
    """Integration test: call get_exchange_info and verify BTCUSDC is present.

    This test runs only when TRIO_BINANCE_INTEGRATION=1 in the environment.
    """
    if os.environ.get("TRIO_BINANCE_INTEGRATION") != "1":
        pytest.skip("Integration tests disabled. Set TRIO_BINANCE_INTEGRATION=1 to enable.")

    client = await AsyncClient.create()
    info = await client.get_exchange_info()
    assert isinstance(info, dict)
    symbols = info.get("symbols", [])
    btcusdc = None
    for s in symbols:
        if s.get("symbol") == "BTCUSDC":
            btcusdc = s
            break
    if btcusdc is None:
        pytest.fail("BTCUSDC not found in exchange info")

    # Print full symbol info for visibility in integration runs
    print("BTCUSDC symbol info:", btcusdc)


