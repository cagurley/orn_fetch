"""
Based on a sample script provided by NRCCUA:
https://github.com/nrccua/file_export_sample
"""

import datetime as dt
import json
import os
import pyodbc
import requests
import sqlite3
from pathlib import PurePath
from support import *

if 'HOME' in os.environ:
    PATH = PurePath(os.environ['HOME']).joinpath('.orn_fetch')
else:
    PATH = PurePath(os.environ['HOMEDRIVE'] + os.environ['HOMEPATH']).joinpath('.orn_fetch')
with open(PATH.joinpath('groups.json')) as f:
    GDATA = json.load(f)


def fetch(current, last):
    """
    :param current: An aware datetime to be saved after a successful fetch
    :param last: An aware datetime string of the form '%Y-%m-%dT%H:%M:%S%z' read from file
    :return: A list of strings equal to the names of newly written data files or None if not successful
    """
    try:
        path = PATH.joinpath('api.json')
        with open(path) as file:
            hdata = json.load(file)
        if not validate_keys(hdata, ('auth_url',
                                     'data_url',
                                     'dest_dir',
                                     'username',
                                     'api_key')):
            raise KeyError('JSON file malformed; provide necessary header data exactly.')
    except (KeyError, OSError, json.JSONDecodeError) as e:
        plog(repr(e))
        return None
    else:
        try:
            # Login and authorization
            log(f"Authorization request {hdata['auth_url']} begun")
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            payload = {'grant_type': 'password', 'username': hdata['username'], 'password': hdata['api_key']}
            auth = requests.post(hdata['auth_url'], headers=headers, data=payload).json()
            headers['Authorization'] = ' '.join([auth['token_type'], auth['access_token']])

            # Request data
            start = dt.datetime.strftime(dt.datetime.strptime(last, '%Y-%m-%dT%H:%M:%S%z'), '%Y-%m-%dT%H:%M:%S')
            downloads = list()
            for group in GDATA:
                for module in GDATA[group]['modules']:
                    log(f"Request for data begun for module {module}")
                    endpoint = hdata['data_url'].replace('{module}', module)
                    params = {
                        'start': start,
                        'timeZone': 'Eastern'
                    }
                    response = requests.get(f"{endpoint}", headers=headers, params=params)
                    if response.ok:
                        data = response.json()['data']
                        if data:
                            if not isinstance(data, list):
                                data = [data]
                            download_path = PurePath(hdata['dest_dir']).joinpath(f"comevo_{group}_{module}_{current.strftime('%Y%m%d%H%M%S')}.json")
                            print(f'Writing file to `{download_path}`.')
                            with open(download_path, 'w') as f:
                                json.dump({'data': data}, f, indent=2)
                            log(f'Download to `{download_path}` successful')
                            downloads.append((group, download_path))
                        else:
                            log('No records returned; no file created')
                    else:
                        raise requests.RequestException(
                            f"There was an error retrieving data with status code {response.status_code}.\n{response.content}")
            return downloads
        except (OSError,
                json.JSONDecodeError,
                requests.RequestException,
                requests.ConnectionError,
                requests.HTTPError,
                requests.URLRequired,
                requests.TooManyRedirects,
                requests.ConnectTimeout,
                requests.ReadTimeout,
                requests.Timeout,
                MissingResponseError) as e:
            plog(repr(e))
            return None


def init(current):
    """
    Setup local database and retrieve data from SQL Server database

    :param current: An aware datetime to be saved after a successful fetch
    :return: A three-tuple comprised of the connection, cursor, and string filename for the locally initiated SQLite database
    """
    try:
        path = PATH.joinpath('connect.json')
        with open(path) as file:
            hdata = json.load(file)
        if not validate_keys(hdata, ('driver',
                                     'host',
                                     'database',
                                     'user',
                                     'password')):
            raise KeyError('JSON file malformed; provide necessary header data exactly.')
    except (KeyError, OSError, json.JSONDecodeError) as e:
        plog(repr(e))
    else:
        try:
            # Setup local database
            log('Local database initiation begun')
            localdb = f"temp_{current.strftime('%Y%m%d%H%M%S')}.db"
            lconn = sqlite3.connect(localdb)
            lcur = lconn.cursor()

            lcur.execute('DROP TABLE IF EXISTS main')
            lconn.commit()
            lcur.execute("""CREATE TABLE main (id text, grp text, val text)""")
            lconn.commit()
            lcur.execute('CREATE INDEX m ON main (id, grp, val)')
            lconn.commit()

            # Retrieve data from SQL Server database
            log('External database reference begun')
            with pyodbc.connect(driver=hdata['driver'],
                                server=hdata['host'],
                                database=hdata['database'],
                                uid=hdata['user'],
                                pwd=hdata['password']) as conn:
                with conn.cursor() as cur:
                    for group in GDATA:
                        cur.execute(f"""select dbo.toGuidString(a.[id]) as [id], ? as [grp], dv.[value] as [val]
from [application] as a
inner join [device] as dv on a.[person] = dv.[record] and dv.[type] = 'campus_email' and dv.[rank] = 1
where exists (select * from [checklist] where a.[id] = [record] and [active] = 1 and [template] in ({', '.join(['?'] * len(GDATA[group]['checklists']))}))
and a.[person] not in (select [record] from [tag] where [tag] = 'test')
order by 2, 1""", (group, *GDATA[group]['checklists']))
                        fc = 0
                        while True:
                            rows = cur.fetchmany(500)
                            if not rows:
                                break
                            lcur.executemany('INSERT INTO main VALUES (?, ?, ?)', rows)
                            lconn.commit()
                            fc += 1
            conn.close()

            # Return local database connection, cursor, and filename
            return lconn, lcur, localdb
        except pyodbc.DatabaseError as e:
            conn.close()
            plog(repr(e))
        except (OSError, sqlite3.DatabaseError) as e:
            plog(repr(e))
    return None


def replace(pairs, cur):
    """
    Read and replace file data

    :param pairs: Iterable of two-tuples of the form (GROUP, PATH), where GROUP is the group name and PATH is a corresponding data file
    :param cur: An open cursor to the local SQLite database
    :return: None
    """
    try:
        log('Replacing file values for downloaded files')
        for (group, path) in pairs:
            with open(path) as f:
                data = json.load(f)
                for index, entry in enumerate(data['data']):
                    cur.execute("select [id] from [main] where [grp] = ? and [val] = ?", (group, entry['attributes']['organizationIdValue']))
                    result = cur.fetchone()
                    if result:
                        data['data'][index]['attributes']['organizationIdValue'] = result[0]
            with open(path, 'w') as f:
                json.dump(data, f, indent=2)
            log(f'File values replaced for `{path}`')
    except (OSError, sqlite3.DatabaseError) as e:
        plog(repr(e))
    return None


def end(conn, cur, path):
    """
    Cleanup local database

    :param conn: A open connection to the local SQLite database
    :param cur: An open cursor to the local SQLite database
    :param path: The path to the local SQLite database
    :return: None
    """
    try:
        log(f'Local database clean-up begun')
        cur.execute('DROP TABLE IF EXISTS main')
        conn.commit()
        cur.close()
        conn.close()
        os.remove(path)
    except (OSError, sqlite3.DatabaseError) as e:
        plog(repr(e))
    return None
