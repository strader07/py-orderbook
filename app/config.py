import logging
import os

class Config:
    exchanges = None
    markets_filter = None
    archive_dates = None
    markets_csv = None
    
    def __init__(self):
        level = logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO"))
        logging.basicConfig(
            level=level,
            format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        logging.info(f"Set logging level to {level}")

        EXCHANGES="gate-io"
        # MARKETS_FILTER="JNTBTC"
        MARKETS_FILTER="JNT_USDT"
        ARCHIVE_DATES="2020-08-05"
        #ARCHIVE_DATES="yesterday"

        self.exchanges = EXCHANGES.split(",")
        self.markets_filter = MARKETS_FILTER.split(",")
        self.archive_dates = ARCHIVE_DATES.split(",")
        self.markets_csv = None

        print(f"exchanges={self.exchanges} | markets_filter={self.markets_filter} | archive_dates={self.archive_dates}")
