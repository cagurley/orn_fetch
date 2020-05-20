"""
Based on a sample script provided by NRCCUA:
https://github.com/nrccua/file_export_sample
"""

import datetime as dt
import json
import os
import requests
from pathlib import PurePath
from support import *


def fetch(current):
    """
    :param current: An aware datetime to be saved after a successful fetch
    :return: None
    """
    try:
        with open('last.dat') as file:
            last = file.readline().strip()

        if 'HOME' in os.environ:
            hpath = PurePath(os.environ['HOME'])
        else:
            hpath = PurePath(os.environ['HOMEDRIVE'] + os.environ['HOMEPATH'])
        hpath = hpath.joinpath('.orn_fetch', 'comevo.json')
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
    else:
        try:
            # Login and authorization
            log(f"Authorization request {hdata['auth_url']} begun")
            headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
            payload = {'grant_type': 'password', 'username': hdata['username'], 'password': hdata['api_key']}
            auth = requests.post(hdata['auth_url'], headers=headers, data=payload).json()
            headers['Authorization'] = ' '.join([auth['token_type'], auth['access_token']])

            # Request data
            # session = requests.Session()
            # session.headers.update(headers)
            log(f"Request for data begun")
            endpoint = hdata['data_url'].replace('{module}', hdata['module'])
            response = requests.get(f"{endpoint}", headers=headers)
            # for file in files_to_download:
                # # Get the file name from the url, unescape it, and then replace whitespace with underscore
                # filename = urlparse(file)
                # filename = get_valid_filename(unquote(PurePath(filename.path).name))
                # download_path = PurePath(hdata['dest_dir']).joinpath(filename)
                # plog(f"Downloading file from url {file}")
                # DON'T USE THE SESSION HERE
                # download_response = requests.get(file, allow_redirects=True, stream=True)
            if response.ok:
                download_path = PurePath(hdata['dest_dir']).joinpath(f"comevo_{hdata['module']}_{current.strftime('%Y%m%d%H%M%S')}.json")
                print(f'Writing file to `{download_path}`.')
                with open(download_path, 'w') as f:
                    f.write(json.dumps(response.json()['data'], indent=2))
                    # for chunk in response.iter_content(chunk_size=1024):
                    #     if chunk:
                    #         f.write(chunk)
                log(f'Download to `{download_path}` successful')
            else:
                raise requests.RequestException(
                    f"There was an error retrieving data with status code {response.status_code}.\n{response.content}")
            # files_to_download = []
            # if response_json['files']:
            #     download_url = f"{hdata['url']}/file"
            #     for file in response_json['files']:
            #         download_response_json = session.post(download_url,
            #                                               data=json.dumps(payload),
            #                                               params={'filename': file['fileName']}).json()
            #         if "fileUrl" in download_response_json:
            #             files_to_download.append(download_response_json["fileUrl"])

            # Download files
            # if len(files_to_download) == 0:
            #     log('No undelivered files to download found')
            # else:
            #     for file in files_to_download:
            #         # Get the file name from the url, unescape it, and then replace whitespace with underscore
            #         filename = urlparse(file)
            #         filename = get_valid_filename(unquote(PurePath(filename.path).name))
            #         download_path = PurePath(hdata['dest_dir']).joinpath(filename)
            #         plog(f"Downloading file from url {file}")
            #         # DON'T USE THE SESSION HERE
            #         download_response = requests.get(file, allow_redirects=True, stream=True)
            #         if download_response.ok:
            #             print(f"Writing file to {download_path}.")
            #             with open(download_path, "wb") as f:
            #                 for chunk in download_response.iter_content(chunk_size=1024):
            #                     if chunk:
            #                         f.write(chunk)
            #             log(f'Download to {download_path} successful')
            #         else:
            #             raise requests.RequestException(f"There was an error retrieving {file} with status code {download_response.status_code}.\n{download_response.content}")

            with open('last.dat', 'w') as file:
                file.write(dt.datetime.strftime(current, '%Y-%m-%dT%H:%M:%S%z') + '\n')
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
        # finally:
        #     session.close()
    finally:
        return None


# if __name__ == '__main__':
#     fetch(dt.datetime.now().astimezone())
