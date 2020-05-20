"""
Shared supporting code
"""

import re
from pathlib import Path


class MissingResponseError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        return None


def get_valid_filename(s):
    s = str(s).strip().replace(" ", "_")
    return re.sub(r"[^-\w.]", "", s)


def log(line):
    try:
        with open(Path.cwd().joinpath('fetch.log'), 'a') as log:
            line += '\n'
            log.write(line)
    except OSError:
        print('Log file could not be accessed.')
    finally:
        return None


def plog(line):
    print(line)
    log(line)
    return None


def validate_keys(src, keys=tuple()):
    """
    Return whether keys in src exactly match supplied keys argument.

    src should be a Container, keys should be an Iterable
    """
    for key in keys:
        if key not in src:
            return False
    for key in src:
        if key not in keys:
            return False
    return True
