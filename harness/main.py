
import config.config as config
import asyncio
import mq.mq as mq

# TODO. why can't my editor find this import? 
from harness.harness import Harness

from functools import partial

import logging

async def main() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d  %(name)-18s  %(message)s",
        datefmt="%H:%M:%S",
    )

    # this config also configures logging settings. 
    harnessConfig = config.load_config()

    harness = Harness(harnessConfig)

    await harness.start() # start harness service. 


if __name__ == "__main__":
    asyncio.run(main())
