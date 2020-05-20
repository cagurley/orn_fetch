"""
Fetcher main
"""

import comevo
import datetime as dt
import os
from support import log, plog
from time import sleep


def main():
    try:
        log(f'Fetcher booted from disk at {dt.datetime.now().astimezone()}')
        while True:
            current = dt.datetime.now().astimezone()
            next_start = current + dt.timedelta(hours=1)
            plog(f'Cycle begun at {current}')
            comevo.fetch(current)
            current = dt.datetime.now().astimezone()
            sleep_interval = (next_start - current).seconds
            if sleep_interval > 0:
                plog(f'Cycle ended and sleep begun at {current}')
                print(f'Next cycle will begin at {next_start}')
                sleep(sleep_interval)
            else:
                plog('Cycle took longer than one hour to complete')
            if os.name == 'nt':
                os.system('cls')
            else:
                os.system('clear')
    except KeyboardInterrupt:
        plog(f'Fetcher terminated via keyboard interrupt at {dt.datetime.now().astimezone()}')
    finally:
        return None


if __name__ == '__main__':
    main()
