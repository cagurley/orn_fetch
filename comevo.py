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


def fetch(current, last):
    """
    :param current: An aware datetime to be saved after a successful fetch
    :param last: An aware datetime string of the form '%Y-%m-%dT%H:%M:%S%z' read from file
    :return: a string equal to the name of the newly written data file or None if not successful
    """
    try:
        if 'HOME' in os.environ:
            hpath = PurePath(os.environ['HOME'])
        else:
            hpath = PurePath(os.environ['HOMEDRIVE'] + os.environ['HOMEPATH'])
        hpath = hpath.joinpath('.orn_fetch', 'api.json')
        with open(hpath) as file:
            hdata = json.load(file)
        if not validate_keys(hdata, ('auth_url',
                                     'data_url',
                                     'module',
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
            log(f"Request for data begun")
            endpoint = hdata['data_url'].replace('{module}', hdata['module'])
            params = {
                'start': dt.datetime.strftime(dt.datetime.strptime(last, '%Y-%m-%dT%H:%M:%S%z'), '%Y-%m-%dT%H:%M:%S'),
                'timeZone': 'Eastern'
            }
            response = requests.get(f"{endpoint}", headers=headers, params=params)
            if response.ok:
                data = response.json()['data']
                if data:
                    if not isinstance(data, list):
                        data = [data]
                    download_path = PurePath(hdata['dest_dir']).joinpath(f"comevo_{hdata['module']}_{current.strftime('%Y%m%d%H%M%S')}.json")
                    print(f'Writing file to `{download_path}`.')
                    with open(download_path, 'w') as f:
                        json.dump({'data': data}, f, indent=2)
                    log(f'Download to `{download_path}` successful')
                    return download_path
                else:
                    log('No records returned; no file created')
                    return None
            else:
                raise requests.RequestException(
                    f"There was an error retrieving data with status code {response.status_code}.\n{response.content}")
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


def replace(current, filepath):
    """
    :param current: An aware datetime to be saved after a successful fetch
    :param filepath: Path to downloaded data file for value replacement
    :return: None
    """
    if filepath:
        try:
            if 'HOME' in os.environ:
                hpath = PurePath(os.environ['HOME'])
            else:
                hpath = PurePath(os.environ['HOMEDRIVE'] + os.environ['HOMEPATH'])
            hpath = hpath.joinpath('.orn_fetch', 'connect.json')
            with open(hpath) as file:
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
                localdb = f"temp_{current.strftime('%Y%m%d%H%M%S')}.db"
                lconn = sqlite3.connect(localdb)

                # Setup local database
                lcur = lconn.cursor()
                lcur.execute('DROP TABLE IF EXISTS main')
                lconn.commit()
                lcur.execute("""CREATE TABLE main (id text, val text)""")
                lconn.commit()
                lcur.execute('CREATE INDEX m ON main (id, val)')
                lconn.commit()

                # Retrieve data from SQL Server database
                log('Database reference begun')
                with pyodbc.connect(driver=hdata['driver'],
                                    server=hdata['host'],
                                    database=hdata['database'],
                                    uid=hdata['user'],
                                    pwd=hdata['password']) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""select dbo.toGuidString(fr.[record]) as [id], dv.[value] as [val]
    from [form.response] as fr
    inner join [form] as f on fr.[form] = f.[id] and f.[parent] = '8a0187e0-ee53-4ce3-9133-754715f33e9b'
    inner join [application] as a on fr.[record] = a.[id]
    inner join [device] as dv on a.[person] = dv.[record] and dv.[type] = 'campus_email' and dv.[rank] = 1
    where fr.[status] is null
    and a.[person] not in (select [record] from [tag] where [tag] = 'test')
    order by 2, 1""")
                        fc = 0
                        while True:
                            rows = cur.fetchmany(500)
                            if not rows:
                                break
                            lcur.executemany('INSERT INTO main VALUES (?, ?)', rows)
                            lconn.commit()
                            fc += 1
                conn.close()

                # Read and replace file data
                with open(filepath) as f:
                    data = json.load(f)
                    for index, entry in enumerate(data['data']):
                        lcur.execute("select [id] from [main] where [val] = ?", [entry['attributes']['organizationIdValue']])
                        result = lcur.fetchone()
                        if result:
                            data['data'][index]['attributes']['organizationIdValue'] = result[0]
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=2)
                log('File values replaced')

                # Cleanup local database
                lcur.execute('DROP TABLE IF EXISTS main')
                lconn.commit()
            except pyodbc.DatabaseError as e:
                conn.close()
                plog(repr(e))
            except (OSError, sqlite3.DatabaseError) as e:
                plog(repr(e))
            finally:
                lconn.rollback()
                lcur.close()
                lconn.close()
                os.remove(localdb)
    return None