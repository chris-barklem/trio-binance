import pytest
import httpx

from trio_binance.async_client import AsyncClient


@pytest.mark.trio
async def test_init_session_creates_httpx_client():
    """Ensure AsyncClient._init_session returns an httpx.AsyncClient instance
    without performing any network calls.
    """
    client = AsyncClient()
    session = client._init_session()
    assert isinstance(session, httpx.AsyncClient)
