import math
from datetime import datetime, date, timedelta

from app.config import Config
from app.logger import logger
from app.parquet import generate_parquet_files
from app.utils import get_markets, RunSettings
from app.tardis_client import booksbuilder


async def get_runs():
    config = Config()

    if config.archive_dates[0] == "yesterday":
        dates = [date.today() - timedelta(days=1)]
        all_markets = await get_markets(config, expiry=date.today())
        markets = [
            market
            for market in all_markets
            if market.enabled or market.expiry.date() >= dates[-1]
        ]

    else:
        all_markets = await get_markets(config)
        dates = sorted(
            [datetime.strptime(dt, "%Y-%m-%d").date() for dt in config.archive_dates]
        )
        markets = [
            market
            for market in all_markets
            if market.enabled or market.expiry.date() >= dates[-1]
        ]

    if config.markets_filter[0] != "all":
        markets = [
            market for market in markets if market.market in config.markets_filter
        ]

    runs = []

    for dt_str in config.archive_dates:
        dt = (
            date.today() - timedelta(days=1)
            if dt_str == "yesterday"
            else datetime.strptime(dt_str, "%Y-%m-%d").date()
        )
        for market in markets:
            # TODO: check expiry and error if expired
            runs.append(RunSettings(dt, market))

    logger.info(f"Will run: {', '.join([str(setting) for setting in runs])}")
    return runs


async def get_book(settings):
    dt = [
        datetime.combine(settings.date, datetime.min.time()),
        datetime.combine(settings.date, datetime.max.time()),
    ]

    orderbooks = await booksbuilder([settings.market], [dt])
    return orderbooks


async def process_mktdata(runs):
    success = 0
    errors = 0

    for settings in runs:

        logger.info(f"Starting {settings} raw files generation.")

        if not settings.market.instrument:
            logger.warn(
                f"Warning: {settings.market.market} does not have a local instrument mapping."
            )
            logger.info("--")
            continue

        orderbooks = await get_book(settings)
        if len(orderbooks) != 1:
            logger.error(
                f"Error for {settings}: received invalid number of books: {len(orderbooks)}. Expected 1."
            )
            errors += 1
            logger.info("--")
            continue

        try:
            raw_files = await generate_parquet_files(
                list(orderbooks.values())[0], settings
            )
        except Exception as e:
            logger.error(f"Error generating raw files for {settings}")
            logger.error(e)
            errors += 1
            logger.info("--")
            continue
        else:
            logger.info(f"Generated raw files for {settings}")
            success += 1

    logger.info("========================================================")
    logger.info(f"summary: success={success} errors={errors}")
    logger.info("========================================================")

    if errors > 0:
        exit(1)


async def main():
    runs = await get_runs()
    await process_mktdata(runs)
