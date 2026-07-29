"""
Shared pytest fixtures for NEM Market Watch tests.

Requires a running PostgreSQL instance for the database-backed tests.
Set DATABASE_URL to your test database — NEVER a live/shared one: the
test_db fixture TRUNCATEs every table it seeds before each test.
"""
import pytest
import pytest_asyncio
from pathlib import Path
from datetime import datetime
import pandas as pd
import sys
import os
import httpx

# Add the backend directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

from app.database import NEMDatabase
from tests import db_seed as seed

SCHEMA_SQL = (Path(__file__).parent / 'schema.sql').read_text()

_TABLES_TO_TRUNCATE = (
    "dispatch_data, dispatch_data_hourly, price_data, generator_info, "
    "pdpasa_data, stpasa_data, price_setter_data, bid_day_offer, bid_per_offer, "
    "predispatch_price, predispatch_interconnector, predispatch_constraint"
)


# ============================================================================
# Database Fixtures
# ============================================================================

@pytest_asyncio.fixture
async def test_db():
    """Create a database for testing.

    Requires DATABASE_URL environment variable pointing to a PostgreSQL database.
    Applies the tests-only schema mirror (see tests/schema.sql — nem-ingestion
    owns the real DDL) and truncates every table before each test.
    """
    db_url = os.getenv('DATABASE_URL')

    if not db_url:
        pytest.skip("DATABASE_URL environment variable not set. Set it to run database tests.")

    db = NEMDatabase(db_url)
    await db.initialize()

    async with db._pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)
        await conn.execute(f"TRUNCATE {_TABLES_TO_TRUNCATE} RESTART IDENTITY CASCADE")

    yield db
    await db.close()


@pytest_asyncio.fixture
async def populated_db(test_db):
    """Database with sample data pre-loaded"""
    # Insert sample dispatch data
    dispatch_df = pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'BAYSW1',
            'scadavalue': 350.5,
            'uigf': 0.0,
            'totalcleared': 350.0,
            'ramprate': 0.0,
            'availability': 400.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'duid': 'AGLHAL',
            'scadavalue': 94.2,
            'uigf': 0.0,
            'totalcleared': 94.0,
            'ramprate': 0.0,
            'availability': 95.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),  # Same as other records
            'duid': 'ARWF1',
            'scadavalue': 185.0,
            'uigf': 0.0,
            'totalcleared': 185.0,
            'ramprate': 0.0,
            'availability': 200.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        },
    ])
    await seed.insert_dispatch_data(test_db, dispatch_df)

    # Insert sample price data
    price_df = pd.DataFrame([
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'VIC',
            'price': 72.30,
            'totaldemand': 5200.0,
            'price_type': 'DISPATCH'
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 90.00,
            'totaldemand': 7500.0,
            'price_type': 'TRADING'
        },
        {
            'settlementdate': datetime(2025, 1, 15, 10, 30),
            'region': 'NSW',
            'price': 88.00,
            'totaldemand': 7400.0,
            'price_type': 'PUBLIC'
        },
        {
            'settlementdate': datetime(2025, 1, 12, 12, 0),
            'region': 'NSW',
            'price': 75.00,
            'totaldemand': 7200.0,
            'price_type': 'PUBLIC'
        },
    ])
    await seed.insert_price_data(test_db, price_df)

    # Insert sample generator info
    await seed.update_generator_info(test_db, [
        {
            'duid': 'BAYSW1',
            'station_name': 'Bayswater',
            'region': 'NSW',
            'fuel_source': 'Coal',
            'technology_type': 'Steam',
            'capacity_mw': 660
        },
        {
            'duid': 'AGLHAL',
            'station_name': 'Hallett',
            'region': 'SA',
            'fuel_source': 'Wind',
            'technology_type': 'Wind',
            'capacity_mw': 95
        },
        {
            'duid': 'ARWF1',
            'station_name': 'Ararat Wind Farm',
            'region': 'VIC',
            'fuel_source': 'Wind',
            'technology_type': 'Wind',
            'capacity_mw': 240
        },
    ])

    return test_db


# ============================================================================
# Async API Client Fixtures
# ============================================================================

@pytest_asyncio.fixture
async def async_client(populated_db):
    """
    Async HTTP client for integration tests.

    Uses httpx.AsyncClient with ASGITransport to run the FastAPI app
    on the same event loop as the database, avoiding event loop conflicts.
    """
    from app.main import app
    import app.main as main_module

    # Set the database on the main module (same event loop)
    main_module.db = populated_db

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test"
    ) as client:
        yield client

    # Clean up
    main_module.db = None
