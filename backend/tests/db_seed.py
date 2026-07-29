"""Test-only data seeding helpers.

nem-ingestion owns every write path in production; this module exists only
so pytest fixtures can populate the tests-only schema (see tests/schema.sql)
with rows to read back. These functions mirror the pre-split insert_* methods
but are standalone (never part of the shipped app.database.NEMDatabase) so
runtime code never imports write helpers.
"""

import pandas as pd
from typing import Any, Dict, List


async def insert_dispatch_data(db, df: pd.DataFrame) -> int:
    """Insert dispatch data from DataFrame."""
    if df.empty:
        return 0

    numeric = df[['scadavalue', 'uigf', 'totalcleared', 'ramprate',
                  'availability', 'raise1sec', 'lower1sec']].astype(object)
    numeric = numeric.where(pd.notna(numeric), None)

    records = []
    for (_, row), values in zip(df.iterrows(), numeric.itertuples(index=False)):
        records.append((
            row['settlementdate'].to_pydatetime() if hasattr(row['settlementdate'], 'to_pydatetime') else row['settlementdate'],
            row['duid'],
            *values
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO dispatch_data
            (settlementdate, duid, scadavalue, uigf, totalcleared, ramprate, availability, raise1sec, lower1sec)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (settlementdate, duid) DO UPDATE SET
                scadavalue = EXCLUDED.scadavalue,
                uigf = EXCLUDED.uigf,
                totalcleared = EXCLUDED.totalcleared,
                ramprate = EXCLUDED.ramprate,
                availability = EXCLUDED.availability,
                raise1sec = EXCLUDED.raise1sec,
                lower1sec = EXCLUDED.lower1sec
        """, records)

    return len(records)


async def insert_price_data(db, df: pd.DataFrame) -> int:
    """Insert price data from DataFrame."""
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        records.append((
            row['settlementdate'].to_pydatetime() if hasattr(row['settlementdate'], 'to_pydatetime') else row['settlementdate'],
            row['region'],
            row['price'],
            row['totaldemand'],
            row['price_type']
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO price_data
            (settlementdate, region, price, totaldemand, price_type)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (settlementdate, region, price_type) DO UPDATE SET
                price = EXCLUDED.price,
                totaldemand = EXCLUDED.totaldemand
        """, records)

    return len(records)


async def update_generator_info(db, generator_data: List[Dict[str, Any]]):
    """Update generator information."""
    records = []
    for gen in generator_data:
        records.append((
            gen['duid'],
            gen.get('station_name'),
            gen.get('region'),
            gen.get('fuel_source'),
            gen.get('technology_type'),
            gen.get('capacity_mw')
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO generator_info
            (duid, station_name, region, fuel_source, technology_type, capacity_mw)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (duid) DO UPDATE SET
                station_name = EXCLUDED.station_name,
                region = EXCLUDED.region,
                fuel_source = EXCLUDED.fuel_source,
                technology_type = EXCLUDED.technology_type,
                capacity_mw = EXCLUDED.capacity_mw,
                updated_at = CURRENT_TIMESTAMP
        """, records)


async def insert_pdpasa_data(db, df: pd.DataFrame) -> int:
    """Insert PDPASA data from DataFrame."""
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        records.append((
            row['run_datetime'].to_pydatetime() if hasattr(row['run_datetime'], 'to_pydatetime') else row['run_datetime'],
            row['interval_datetime'].to_pydatetime() if hasattr(row['interval_datetime'], 'to_pydatetime') else row['interval_datetime'],
            row['regionid'],
            row.get('demand10'),
            row.get('demand50'),
            row.get('demand90'),
            row.get('reservereq'),
            row.get('capacityreq'),
            row.get('aggregatecapacityavailable'),
            row.get('aggregatepasaavailability'),
            row.get('surplusreserve'),
            row.get('lorcondition'),
            row.get('calculatedlor1level'),
            row.get('calculatedlor2level')
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO pdpasa_data
            (run_datetime, interval_datetime, regionid, demand10, demand50, demand90,
             reservereq, capacityreq, aggregatecapacityavailable, aggregatepasaavailability,
             surplusreserve, lorcondition, calculatedlor1level, calculatedlor2level)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (run_datetime, interval_datetime, regionid) DO UPDATE SET
                demand10 = EXCLUDED.demand10,
                demand50 = EXCLUDED.demand50,
                demand90 = EXCLUDED.demand90,
                reservereq = EXCLUDED.reservereq,
                capacityreq = EXCLUDED.capacityreq,
                aggregatecapacityavailable = EXCLUDED.aggregatecapacityavailable,
                aggregatepasaavailability = EXCLUDED.aggregatepasaavailability,
                surplusreserve = EXCLUDED.surplusreserve,
                lorcondition = EXCLUDED.lorcondition,
                calculatedlor1level = EXCLUDED.calculatedlor1level,
                calculatedlor2level = EXCLUDED.calculatedlor2level
        """, records)

    return len(records)


async def insert_stpasa_data(db, df: pd.DataFrame) -> int:
    """Insert STPASA data from DataFrame."""
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        records.append((
            row['run_datetime'].to_pydatetime() if hasattr(row['run_datetime'], 'to_pydatetime') else row['run_datetime'],
            row['interval_datetime'].to_pydatetime() if hasattr(row['interval_datetime'], 'to_pydatetime') else row['interval_datetime'],
            row['regionid'],
            row.get('demand10'),
            row.get('demand50'),
            row.get('demand90'),
            row.get('reservereq'),
            row.get('capacityreq'),
            row.get('aggregatecapacityavailable'),
            row.get('aggregatepasaavailability'),
            row.get('surplusreserve'),
            row.get('lorcondition'),
            row.get('calculatedlor1level'),
            row.get('calculatedlor2level')
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO stpasa_data
            (run_datetime, interval_datetime, regionid, demand10, demand50, demand90,
             reservereq, capacityreq, aggregatecapacityavailable, aggregatepasaavailability,
             surplusreserve, lorcondition, calculatedlor1level, calculatedlor2level)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (run_datetime, interval_datetime, regionid) DO UPDATE SET
                demand10 = EXCLUDED.demand10,
                demand50 = EXCLUDED.demand50,
                demand90 = EXCLUDED.demand90,
                reservereq = EXCLUDED.reservereq,
                capacityreq = EXCLUDED.capacityreq,
                aggregatecapacityavailable = EXCLUDED.aggregatecapacityavailable,
                aggregatepasaavailability = EXCLUDED.aggregatepasaavailability,
                surplusreserve = EXCLUDED.surplusreserve,
                lorcondition = EXCLUDED.lorcondition,
                calculatedlor1level = EXCLUDED.calculatedlor1level,
                calculatedlor2level = EXCLUDED.calculatedlor2level
        """, records)

    return len(records)


async def insert_bid_day_offer(db, df: pd.DataFrame) -> int:
    """Insert BIDDAYOFFER data from DataFrame."""
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        sd = row['settlementdate']
        sd = sd.to_pydatetime() if hasattr(sd, 'to_pydatetime') else sd
        sd_date = sd.date() if hasattr(sd, 'date') else sd

        od = row['offerdate']
        od = od.to_pydatetime() if hasattr(od, 'to_pydatetime') else od

        records.append((
            sd_date,
            row['duid'],
            od,
            row.get('priceband1'), row.get('priceband2'), row.get('priceband3'),
            row.get('priceband4'), row.get('priceband5'), row.get('priceband6'),
            row.get('priceband7'), row.get('priceband8'), row.get('priceband9'),
            row.get('priceband10'),
            row.get('minimumload'),
            row.get('t1'), row.get('t2'), row.get('t3'), row.get('t4'),
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO bid_day_offer
            (settlementdate, duid, offerdate,
             priceband1, priceband2, priceband3, priceband4, priceband5,
             priceband6, priceband7, priceband8, priceband9, priceband10,
             minimumload, t1, t2, t3, t4)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (settlementdate, duid, offerdate) DO UPDATE SET
                priceband1 = EXCLUDED.priceband1, priceband2 = EXCLUDED.priceband2,
                priceband3 = EXCLUDED.priceband3, priceband4 = EXCLUDED.priceband4,
                priceband5 = EXCLUDED.priceband5, priceband6 = EXCLUDED.priceband6,
                priceband7 = EXCLUDED.priceband7, priceband8 = EXCLUDED.priceband8,
                priceband9 = EXCLUDED.priceband9, priceband10 = EXCLUDED.priceband10,
                minimumload = EXCLUDED.minimumload,
                t1 = EXCLUDED.t1, t2 = EXCLUDED.t2, t3 = EXCLUDED.t3, t4 = EXCLUDED.t4
        """, records)

    return len(records)


async def insert_bid_per_offer(db, df: pd.DataFrame) -> int:
    """Insert BIDPEROFFER data from DataFrame."""
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        sd = row['settlementdate']
        sd = sd.to_pydatetime() if hasattr(sd, 'to_pydatetime') else sd

        od = row['offerdate']
        od = od.to_pydatetime() if hasattr(od, 'to_pydatetime') else od

        records.append((
            sd,
            row['duid'],
            od,
            row.get('bandavail1'), row.get('bandavail2'), row.get('bandavail3'),
            row.get('bandavail4'), row.get('bandavail5'), row.get('bandavail6'),
            row.get('bandavail7'), row.get('bandavail8'), row.get('bandavail9'),
            row.get('bandavail10'),
            row.get('maxavail'),
            row.get('fixedload'),
            row.get('rocup'),
            row.get('rocdown'),
            row.get('pasaavailability'),
        ))

    async with db._pool.acquire() as conn:
        await conn.executemany("""
            INSERT INTO bid_per_offer
            (settlementdate, duid, offerdate,
             bandavail1, bandavail2, bandavail3, bandavail4, bandavail5,
             bandavail6, bandavail7, bandavail8, bandavail9, bandavail10,
             maxavail, fixedload, rocup, rocdown, pasaavailability)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (settlementdate, duid, offerdate) DO UPDATE SET
                bandavail1 = EXCLUDED.bandavail1, bandavail2 = EXCLUDED.bandavail2,
                bandavail3 = EXCLUDED.bandavail3, bandavail4 = EXCLUDED.bandavail4,
                bandavail5 = EXCLUDED.bandavail5, bandavail6 = EXCLUDED.bandavail6,
                bandavail7 = EXCLUDED.bandavail7, bandavail8 = EXCLUDED.bandavail8,
                bandavail9 = EXCLUDED.bandavail9, bandavail10 = EXCLUDED.bandavail10,
                maxavail = EXCLUDED.maxavail, fixedload = EXCLUDED.fixedload,
                rocup = EXCLUDED.rocup, rocdown = EXCLUDED.rocdown,
                pasaavailability = EXCLUDED.pasaavailability
        """, records)

    return len(records)
