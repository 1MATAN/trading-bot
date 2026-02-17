#!/usr/bin/env python
"""Standalone script to download 15-sec bars from IBKR."""
import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-35s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
)

from simulation.ibkr_data_downloader import download_15s_data

results = asyncio.run(download_15s_data())
print(f'\nTotal downloaded: {len(results)} events')
