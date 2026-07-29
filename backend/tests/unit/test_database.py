"""
Unit tests for NEMDatabase (PostgreSQL, read-only).

Requires DATABASE_URL environment variable set to a PostgreSQL database.
Write-path setup for these tests uses tests/db_seed.py helpers directly
(NEMDatabase itself has no insert_* methods — nem-ingestion owns writes).
"""
import pytest
import pandas as pd
from datetime import datetime, date, timedelta

from tests import db_seed as seed


class TestDispatchQueries:
    """Tests for dispatch data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data(self, populated_db):
        """Test retrieving latest dispatch data"""
        result = await populated_db.get_latest_dispatch_data()

        # Should only return records from the most recent timestamp
        assert len(result) > 0
        # All records should have the same (max) timestamp
        assert result['settlementdate'].nunique() == 1

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data_with_limit(self, populated_db):
        """Test limit parameter"""
        result = await populated_db.get_latest_dispatch_data(limit=1)
        assert len(result) <= 1

    @pytest.mark.asyncio
    async def test_get_latest_dispatch_data_empty(self, test_db):
        """Test empty database returns empty DataFrame"""
        result = await test_db.get_latest_dispatch_data()
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_dispatch_data_by_date_range(self, populated_db):
        """Test date range query"""
        start = datetime(2025, 1, 15, 10, 0)
        end = datetime(2025, 1, 15, 11, 0)

        result = await populated_db.get_dispatch_data_by_date_range(start, end)
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_get_dispatch_data_by_date_range_with_duid(self, populated_db):
        """Test date range query with DUID filter"""
        start = datetime(2025, 1, 15, 10, 0)
        end = datetime(2025, 1, 15, 11, 0)

        result = await populated_db.get_dispatch_data_by_date_range(start, end, duid='BAYSW1')
        if len(result) > 0:
            assert all(result['duid'] == 'BAYSW1')

    @pytest.mark.asyncio
    async def test_get_unique_duids(self, populated_db):
        """Test getting unique DUIDs"""
        duids = await populated_db.get_unique_duids()
        assert isinstance(duids, list)
        assert 'BAYSW1' in duids


class TestPriceQueries:
    """Tests for price data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_prices(self, populated_db):
        """Test retrieving latest prices by type"""
        dispatch = await populated_db.get_latest_prices('DISPATCH')
        trading = await populated_db.get_latest_prices('TRADING')

        assert len(dispatch) > 0
        assert len(trading) > 0

    @pytest.mark.asyncio
    async def test_get_latest_prices_filters_by_type(self, populated_db):
        """Test that price type filter works"""
        dispatch = await populated_db.get_latest_prices('DISPATCH')
        if len(dispatch) > 0:
            assert all(dispatch['price_type'] == 'DISPATCH')

    @pytest.mark.asyncio
    async def test_get_price_history(self, populated_db):
        """Test price history query"""
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)

        result = await populated_db.get_price_history(start, end, price_type='DISPATCH')
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_get_price_history_with_region(self, populated_db):
        """Test price history with region filter"""
        start = datetime(2025, 1, 1)
        end = datetime(2025, 1, 31)

        result = await populated_db.get_price_history(start, end, region='NSW', price_type='DISPATCH')
        if len(result) > 0:
            assert all(result['region'] == 'NSW')


class TestGeneratorInfo:
    """Tests for generator info query methods"""

    @pytest.mark.asyncio
    async def test_get_generators_by_region_fuel(self, populated_db):
        """Test filtered generator query"""
        result = await populated_db.get_generators_by_region_fuel(region='NSW')

        if len(result) > 0:
            # All should be from NSW
            assert all(result['region'] == 'NSW')


class TestRegionQueries:
    """Tests for region-specific queries"""

    @pytest.mark.asyncio
    async def test_get_region_fuel_mix(self, populated_db):
        """Test region fuel mix query"""
        result = await populated_db.get_region_fuel_mix('NSW')

        if len(result) > 0:
            # Should have percentage column
            assert 'percentage' in result.columns
            assert 'fuel_source' in result.columns

    @pytest.mark.asyncio
    async def test_get_region_price_history(self, populated_db):
        """Test region price history query"""
        # This uses relative time, so may not return data depending on test data
        result = await populated_db.get_region_price_history('NSW', hours=24)

        # Should be a DataFrame (possibly empty)
        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_get_region_summary(self, populated_db):
        """Test region summary query"""
        result = await populated_db.get_region_summary('NSW')

        assert isinstance(result, dict)
        assert 'region' in result
        assert result['region'] == 'NSW'
        assert 'generator_count' in result


class TestDataSummary:
    """Tests for data summary methods"""

    @pytest.mark.asyncio
    async def test_get_data_summary(self, populated_db):
        """Test data summary query"""
        result = await populated_db.get_data_summary()

        assert isinstance(result, dict)
        assert 'total_records' in result
        assert 'unique_duids' in result
        assert 'fuel_breakdown' in result

    @pytest.mark.asyncio
    async def test_get_data_coverage(self, populated_db):
        """Test data coverage query"""
        result = await populated_db.get_data_coverage('price_data')

        assert isinstance(result, dict)
        assert 'total_records' in result
        assert 'days_with_data' in result


class TestGenerationByFuelType:
    """Tests for generation aggregation queries"""

    @pytest.mark.asyncio
    async def test_get_generation_by_fuel_type(self, populated_db):
        """Test generation by fuel type aggregation"""
        start = datetime(2025, 1, 15, 10, 0)
        end = datetime(2025, 1, 15, 11, 0)

        result = await populated_db.get_generation_by_fuel_type(start, end)

        if len(result) > 0:
            assert 'fuel_source' in result.columns
            assert 'total_generation' in result.columns
            assert 'unit_count' in result.columns


class TestGetMergedPriceHistory:
    """Tests for get_merged_price_history method (MERGED price type)"""

    @pytest.mark.asyncio
    async def test_returns_public_when_no_dispatch(self, test_db):
        """When only PUBLIC data exists, return PUBLIC with source_type"""
        # Insert only PUBLIC data
        public_df = pd.DataFrame([
            {
                'settlementdate': datetime.now() - timedelta(hours=1),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'PUBLIC'
            }
        ])
        await seed.insert_price_data(test_db, public_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert len(result) == 1
        assert 'source_type' in result.columns
        assert result.iloc[0]['source_type'] == 'PUBLIC'

    @pytest.mark.asyncio
    async def test_returns_dispatch_when_no_public(self, test_db):
        """When only DISPATCH data exists, return DISPATCH with source_type"""
        # Insert only DISPATCH data
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': datetime.now() - timedelta(hours=1),
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7600.0,
                'price_type': 'DISPATCH'
            }
        ])
        await seed.insert_price_data(test_db, dispatch_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert len(result) == 1
        assert 'source_type' in result.columns
        assert result.iloc[0]['source_type'] == 'DISPATCH'

    @pytest.mark.asyncio
    async def test_returns_empty_dataframe_when_no_data(self, test_db):
        """Should return empty DataFrame when no data exists"""
        result = await test_db.get_merged_price_history('NSW', hours=24)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_merges_public_and_dispatch_data(self, test_db):
        """When both PUBLIC and DISPATCH data exist, PUBLIC takes priority"""
        base_time = datetime.now()

        # Insert PUBLIC data at time T
        public_df = pd.DataFrame([
            {
                'settlementdate': base_time - timedelta(hours=1),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'PUBLIC'
            }
        ])
        await seed.insert_price_data(test_db, public_df)

        # Insert DISPATCH data at time T (same timestamp) and T+5min (different timestamp)
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': base_time - timedelta(hours=1),  # Same as PUBLIC
                'region': 'NSW',
                'price': 90.00,
                'totaldemand': 7600.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': base_time - timedelta(minutes=55),  # Different timestamp
                'region': 'NSW',
                'price': 92.00,
                'totaldemand': 7700.0,
                'price_type': 'DISPATCH'
            }
        ])
        await seed.insert_price_data(test_db, dispatch_df)

        result = await test_db.get_merged_price_history('NSW', hours=24)

        # Should have 2 records: PUBLIC at T and DISPATCH at T+5min
        assert len(result) == 2
        # PUBLIC should be used for the overlapping timestamp
        public_row = result[result['source_type'] == 'PUBLIC']
        assert len(public_row) == 1
        assert public_row.iloc[0]['price'] == 85.50
        # DISPATCH fills in the gap
        dispatch_row = result[result['source_type'] == 'DISPATCH']
        assert len(dispatch_row) == 1
        assert dispatch_row.iloc[0]['price'] == 92.00


class TestAggregatedPriceHistory:
    """Tests for get_aggregated_price_history method"""

    @pytest.mark.asyncio
    async def test_returns_non_aggregated_for_short_hours(self, test_db):
        """When hours is small, should return non-aggregated data (via get_merged_price_history)"""
        # Insert some price data
        price_df = pd.DataFrame([
            {
                'settlementdate': datetime.now() - timedelta(hours=1),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'PUBLIC'
            }
        ])
        await seed.insert_price_data(test_db, price_df)

        # With small hours, should use merged (non-aggregated) path
        result = await test_db.get_aggregated_price_history('NSW', hours=6, aggregation_minutes=30)

        assert isinstance(result, pd.DataFrame)

    @pytest.mark.asyncio
    async def test_returns_aggregated_data_for_long_hours(self, test_db):
        """When hours is large, should return aggregated data"""
        base_time = datetime.now()

        # Insert multiple price points
        price_data = []
        for i in range(10):
            price_data.append({
                'settlementdate': base_time - timedelta(hours=i),
                'region': 'NSW',
                'price': 80.0 + i,
                'totaldemand': 7000.0 + i * 100,
                'price_type': 'PUBLIC'
            })

        price_df = pd.DataFrame(price_data)
        await seed.insert_price_data(test_db, price_df)

        # Request aggregated data with 60-minute buckets
        result = await test_db.get_aggregated_price_history('NSW', hours=12, aggregation_minutes=60)

        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            assert 'source_type' in result.columns

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_data(self, test_db):
        """Should return empty DataFrame when no data exists"""
        result = await test_db.get_aggregated_price_history('NSW', hours=24, aggregation_minutes=60)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0


class TestPASAQueries:
    """Tests for PASA data query methods"""

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa(self, test_db):
        """Test retrieving latest PDPASA data for a region"""
        # Insert PDPASA data for multiple runs
        df1 = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand50': 7500.0,
            'lorcondition': 0
        }])
        await seed.insert_pdpasa_data(test_db, df1)

        df2 = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 30),  # Later run
            'interval_datetime': datetime(2025, 1, 15, 11, 0),
            'regionid': 'NSW1',
            'demand50': 7600.0,
            'lorcondition': 1
        }])
        await seed.insert_pdpasa_data(test_db, df2)

        result = await test_db.get_latest_pdpasa('NSW1')

        # Should return data from the latest run only
        assert len(result) == 1
        assert result[0]['demand50'] == 7600.0
        assert result[0]['lorcondition'] == 1

    @pytest.mark.asyncio
    async def test_get_latest_pdpasa_empty(self, test_db):
        """Test retrieving PDPASA for region with no data"""
        result = await test_db.get_latest_pdpasa('NOREGION')
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_get_latest_stpasa(self, test_db):
        """Test retrieving latest STPASA data for a region"""
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 6, 0),
            'interval_datetime': datetime(2025, 1, 16, 0, 0),
            'regionid': 'QLD1',
            'demand50': 6500.0,
            'lorcondition': 0
        }])
        await seed.insert_stpasa_data(test_db, df)

        result = await test_db.get_latest_stpasa('QLD1')

        assert len(result) == 1
        assert result[0]['demand50'] == 6500.0

    @pytest.mark.asyncio
    async def test_get_latest_stpasa_empty(self, test_db):
        """Test retrieving STPASA for region with no data"""
        result = await test_db.get_latest_stpasa('NOREGION')
        assert len(result) == 0


class TestExportMethods:
    """Tests for export methods used in CSV downloads"""

    @pytest.mark.asyncio
    async def test_get_unique_fuel_sources(self, test_db):
        """Test getting unique fuel sources from generator_info"""
        # Insert some generator info with different fuel sources
        generators = [
            {'duid': 'GEN1', 'station_name': 'Coal Station', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 500},
            {'duid': 'GEN2', 'station_name': 'Wind Farm', 'region': 'VIC',
             'fuel_source': 'Wind', 'technology_type': 'Wind Turbine', 'capacity_mw': 200},
            {'duid': 'GEN3', 'station_name': 'Solar Farm', 'region': 'QLD',
             'fuel_source': 'Solar', 'technology_type': 'Photovoltaic', 'capacity_mw': 150},
        ]
        await seed.update_generator_info(test_db, generators)

        result = await test_db.get_unique_fuel_sources()

        assert isinstance(result, list)
        assert 'Coal' in result
        assert 'Wind' in result
        assert 'Solar' in result

    @pytest.mark.asyncio
    async def test_get_unique_fuel_sources_empty(self, test_db):
        """Test getting unique fuel sources when no generators exist"""
        result = await test_db.get_unique_fuel_sources()
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_export_price_data(self, test_db):
        """Test exporting price data as DataFrame"""
        # Insert price data
        price_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 30),
                'region': 'VIC',
                'price': 90.00,
                'totaldemand': 5000.0,
                'price_type': 'DISPATCH'
            }
        ])
        await seed.insert_price_data(test_db, price_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_price_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert 'settlementdate' in result.columns
        assert 'region' in result.columns
        assert 'price' in result.columns

    @pytest.mark.asyncio
    async def test_export_price_data_with_region_filter(self, test_db):
        """Test exporting price data filtered by region"""
        # Insert price data for multiple regions
        price_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'region': 'NSW',
                'price': 85.50,
                'totaldemand': 7500.0,
                'price_type': 'DISPATCH'
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'region': 'VIC',
                'price': 90.00,
                'totaldemand': 5000.0,
                'price_type': 'DISPATCH'
            }
        ])
        await seed.insert_price_data(test_db, price_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_price_data(start, end, regions=['NSW'])

        assert len(result) == 1
        assert result.iloc[0]['region'] == 'NSW'

    @pytest.mark.asyncio
    async def test_export_price_data_empty(self, test_db):
        """Test exporting price data when no data exists"""
        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_price_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert 'settlementdate' in result.columns

    @pytest.mark.asyncio
    async def test_export_generation_data(self, test_db):
        """Test exporting generation data as DataFrame"""
        # Insert generator info
        generators = [
            {'duid': 'GEN1', 'station_name': 'Test Station', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 500}
        ]
        await seed.update_generator_info(test_db, generators)

        # Insert dispatch data
        dispatch_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 0),
            'duid': 'GEN1',
            'scadavalue': 400.0,
            'uigf': 0.0,
            'totalcleared': 400.0,
            'ramprate': 0.0,
            'availability': 500.0,
            'raise1sec': 0.0,
            'lower1sec': 0.0
        }])
        await seed.insert_dispatch_data(test_db, dispatch_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_generation_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'generation_mw' in result.columns
        assert 'station_name' in result.columns

    @pytest.mark.asyncio
    async def test_export_generation_data_with_filters(self, test_db):
        """Test exporting generation data with region and fuel source filters"""
        # Insert generator info
        generators = [
            {'duid': 'GEN1', 'station_name': 'Coal Station', 'region': 'NSW',
             'fuel_source': 'Coal', 'technology_type': 'Steam', 'capacity_mw': 500},
            {'duid': 'GEN2', 'station_name': 'Wind Farm', 'region': 'VIC',
             'fuel_source': 'Wind', 'technology_type': 'Wind Turbine', 'capacity_mw': 200}
        ]
        await seed.update_generator_info(test_db, generators)

        # Insert dispatch data for both generators
        dispatch_df = pd.DataFrame([
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'duid': 'GEN1',
                'scadavalue': 400.0,
                'uigf': 0.0,
                'totalcleared': 400.0,
                'ramprate': 0.0,
                'availability': 500.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            },
            {
                'settlementdate': datetime(2025, 1, 15, 10, 0),
                'duid': 'GEN2',
                'scadavalue': 150.0,
                'uigf': 0.0,
                'totalcleared': 150.0,
                'ramprate': 0.0,
                'availability': 200.0,
                'raise1sec': 0.0,
                'lower1sec': 0.0
            }
        ])
        await seed.insert_dispatch_data(test_db, dispatch_df)

        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        # Filter by region
        result = await test_db.export_generation_data(start, end, regions=['NSW'])
        assert len(result) == 1
        assert result.iloc[0]['region'] == 'NSW'

        # Filter by fuel source
        result = await test_db.export_generation_data(start, end, fuel_sources=['Wind'])
        assert len(result) == 1
        assert result.iloc[0]['fuel_source'] == 'Wind'

    @pytest.mark.asyncio
    async def test_export_generation_data_empty(self, test_db):
        """Test exporting generation data when no data exists"""
        start = datetime(2025, 1, 15, 0, 0)
        end = datetime(2025, 1, 15, 23, 59)

        result = await test_db.export_generation_data(start, end)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_pdpasa(self, test_db):
        """Test exporting latest PDPASA data as DataFrame"""
        # Insert PDPASA data
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 10, 0),
            'interval_datetime': datetime(2025, 1, 15, 10, 30),
            'regionid': 'NSW1',
            'demand10': 7200.0,
            'demand50': 7500.0,
            'demand90': 7800.0,
            'reservereq': 1500.0,
            'capacityreq': 9000.0,
            'aggregatecapacityavailable': 10500.0,
            'aggregatepasaavailability': 10000.0,
            'surplusreserve': 1500.0,
            'lorcondition': 0,
            'calculatedlor1level': 2000.0,
            'calculatedlor2level': 1500.0
        }])
        await seed.insert_pdpasa_data(test_db, df)

        result = await test_db.export_latest_pasa_data('pdpasa')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert 'run_datetime' in result.columns
        assert 'interval_datetime' in result.columns
        assert 'regionid' in result.columns

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_stpasa(self, test_db):
        """Test exporting latest STPASA data as DataFrame"""
        # Insert STPASA data
        df = pd.DataFrame([{
            'run_datetime': datetime(2025, 1, 15, 6, 0),
            'interval_datetime': datetime(2025, 1, 16, 0, 0),
            'regionid': 'VIC1',
            'demand10': 4500.0,
            'demand50': 4900.0,
            'demand90': 5300.0,
            'reservereq': 1200.0,
            'capacityreq': 6100.0,
            'aggregatecapacityavailable': 7200.0,
            'aggregatepasaavailability': 6900.0,
            'surplusreserve': 1100.0,
            'lorcondition': 0,
            'calculatedlor1level': 1800.0,
            'calculatedlor2level': 1400.0
        }])
        await seed.insert_stpasa_data(test_db, df)

        result = await test_db.export_latest_pasa_data('stpasa')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_with_region_filter(self, test_db):
        """Test exporting PASA data with region filter"""
        # Insert PDPASA data for multiple regions
        df = pd.DataFrame([
            {
                'run_datetime': datetime(2025, 1, 15, 10, 0),
                'interval_datetime': datetime(2025, 1, 15, 10, 30),
                'regionid': 'NSW1',
                'demand50': 7500.0
            },
            {
                'run_datetime': datetime(2025, 1, 15, 10, 0),
                'interval_datetime': datetime(2025, 1, 15, 10, 30),
                'regionid': 'VIC1',
                'demand50': 5000.0
            }
        ])
        await seed.insert_pdpasa_data(test_db, df)

        # Filter by region (should auto-convert NSW to NSW1)
        result = await test_db.export_latest_pasa_data('pdpasa', regions=['NSW'])

        assert len(result) == 1
        assert result.iloc[0]['regionid'] == 'NSW1'

    @pytest.mark.asyncio
    async def test_export_latest_pasa_data_empty(self, test_db):
        """Test exporting PASA data when no data exists"""
        result = await test_db.export_latest_pasa_data('pdpasa')

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert 'run_datetime' in result.columns

    @pytest.mark.asyncio
    async def test_get_export_data_ranges(self, test_db):
        """Test getting export data ranges"""
        # Insert some data
        price_df = pd.DataFrame([{
            'settlementdate': datetime(2025, 1, 15, 10, 0),
            'region': 'NSW',
            'price': 85.50,
            'totaldemand': 7500.0,
            'price_type': 'DISPATCH'
        }])
        await seed.insert_price_data(test_db, price_df)

        result = await test_db.get_export_data_ranges()

        assert isinstance(result, dict)
        assert 'prices' in result
        assert 'generation' in result
        assert 'pasa' in result
        assert 'pdpasa' in result['pasa']
        assert 'stpasa' in result['pasa']
        assert 'earliest_date' in result['prices']
        assert 'latest_date' in result['prices']

    @pytest.mark.asyncio
    async def test_get_export_data_ranges_empty(self, test_db):
        """Test getting export data ranges when no data exists"""
        result = await test_db.get_export_data_ranges()

        assert isinstance(result, dict)
        assert result['prices']['earliest_date'] is None
        assert result['prices']['latest_date'] is None
        assert result['pasa']['pdpasa']['earliest_date'] is None
        assert result['pasa']['stpasa']['earliest_date'] is None


class TestGetBidBandsForDuid:
    """Tests for get_bid_bands_for_duid method"""

    @pytest.mark.asyncio
    async def test_get_bid_bands_success(self, test_db):
        """Test fetching combined bid band data."""
        # Insert day offer
        day_df = pd.DataFrame([{
            'settlementdate': pd.Timestamp('2026-02-21'),
            'duid': 'BAYSW1',
            'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
            'priceband1': -987.0, 'priceband2': 0.0, 'priceband3': 30.0,
            'priceband4': 50.0, 'priceband5': 100.0, 'priceband6': 300.0,
            'priceband7': 1000.0, 'priceband8': 5000.0, 'priceband9': 10000.0,
            'priceband10': 15000.0,
            'minimumload': 200.0,
            't1': 3.0, 't2': 3.0, 't3': 3.0, 't4': 3.0,
        }])
        await seed.insert_bid_day_offer(test_db, day_df)

        # Insert per-offer intervals
        per_df = pd.DataFrame([
            {
                'settlementdate': pd.Timestamp('2026-02-21 00:05:00'),
                'duid': 'BAYSW1',
                'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
                'bandavail1': 100.0, 'bandavail2': 50.0, 'bandavail3': 200.0,
                'bandavail4': 0.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
                'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
                'bandavail10': 0.0,
                'maxavail': 660.0, 'fixedload': 0.0,
                'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
            },
            {
                'settlementdate': pd.Timestamp('2026-02-21 00:10:00'),
                'duid': 'BAYSW1',
                'offerdate': pd.Timestamp('2026-02-20 12:00:00'),
                'bandavail1': 120.0, 'bandavail2': 40.0, 'bandavail3': 180.0,
                'bandavail4': 10.0, 'bandavail5': 0.0, 'bandavail6': 0.0,
                'bandavail7': 0.0, 'bandavail8': 0.0, 'bandavail9': 0.0,
                'bandavail10': 0.0,
                'maxavail': 660.0, 'fixedload': 0.0,
                'rocup': 5.0, 'rocdown': 5.0, 'pasaavailability': 660.0,
            },
        ])
        await seed.insert_bid_per_offer(test_db, per_df)

        result = await test_db.get_bid_bands_for_duid('BAYSW1', date(2026, 2, 21))

        assert len(result) == 2
        assert result[0]['priceband1'] == -987.0
        assert result[0]['bandavail1'] == 100.0
        assert result[1]['bandavail1'] == 120.0
        assert result[0]['minimumload'] == 200.0

    @pytest.mark.asyncio
    async def test_get_bid_bands_empty(self, test_db):
        """Test fetching bids for nonexistent DUID."""
        result = await test_db.get_bid_bands_for_duid('NONEXIST', date(2026, 2, 21))
        assert len(result) == 0


class TestSearchDuids:
    """Tests for search_duids method"""

    @pytest.mark.asyncio
    async def test_search_duids_by_duid(self, test_db):
        """Test searching by DUID prefix."""
        await seed.update_generator_info(test_db, [
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
            {'duid': 'BAYSW2', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
            {'duid': 'LOYS1', 'station_name': 'Loy Yang A', 'region': 'VIC1',
             'fuel_source': 'Brown Coal', 'technology_type': 'Steam', 'capacity_mw': 560.0},
        ])

        results = await test_db.search_duids('BAYSW')
        assert len(results) == 2
        assert results[0]['duid'] == 'BAYSW1'
        assert results[1]['duid'] == 'BAYSW2'

    @pytest.mark.asyncio
    async def test_search_duids_by_station_name(self, test_db):
        """Test searching by station name."""
        await seed.update_generator_info(test_db, [
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
        ])

        results = await test_db.search_duids('Bayswater')
        assert len(results) == 1
        assert results[0]['duid'] == 'BAYSW1'

    @pytest.mark.asyncio
    async def test_search_duids_no_results(self, test_db):
        """Test search with no matches."""
        results = await test_db.search_duids('ZZZZZ')
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_search_duids_case_insensitive(self, test_db):
        """Test that search is case-insensitive."""
        await seed.update_generator_info(test_db, [
            {'duid': 'BAYSW1', 'station_name': 'Bayswater', 'region': 'NSW1',
             'fuel_source': 'Black Coal', 'technology_type': 'Steam', 'capacity_mw': 660.0},
        ])

        results = await test_db.search_duids('baysw')
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_search_duids_limit(self, test_db):
        """Test search respects limit."""
        await seed.update_generator_info(test_db, [
            {'duid': f'TEST{i}', 'station_name': 'TestStation', 'region': 'NSW1',
             'fuel_source': 'Gas', 'technology_type': 'OCGT', 'capacity_mw': 100.0}
            for i in range(5)
        ])

        results = await test_db.search_duids('TEST', limit=2)
        assert len(results) == 2
