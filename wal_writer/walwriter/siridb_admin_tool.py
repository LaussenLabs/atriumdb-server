#
# AtriumDB is a timeseries database software designed to best handle the unique
# features and challenges that arise from clinical waveform data.
#
# Copyright (c) 2025 The Hospital for Sick Children.
#
# This file is part of AtriumDB 
# (see atriumdb.io).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import requests
from requests.auth import HTTPBasicAuth


class SiriDBAdmin:

    def __init__(self, host: str = None, port: int = 9080):
        self.host = 'localhost' if host is None else host
        self.port = port

    def new_database(self, db_name: str, time_precision: str = 'ns', buffer_size: int = 8192, duration_num: str = '4d',
                     duration_log: str = '4d', username: str = 'sa', password: str = 'siri',):

        url = 'http://' + self.host + ":" + str(self.port) + "/new-database"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "dbname": db_name,
            "time_precision": time_precision,
            "buffer_size": buffer_size,
            "duration_num": duration_num,
            "duration_log": duration_log
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def new_account(self, new_account_name: str, new_account_password: str, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/new-account"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "account": new_account_name,
            "password": new_account_password
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def change_password(self, account_name: str, new_password: str, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/change-password"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "account": account_name,
            "password": new_password
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def drop_account(self, account_name: str, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/drop-account"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "account": account_name,
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def new_pool(self, db_name: str, db_user: str, db_password: str, host: str, port: int,
                 username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/new-pool"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "dbname": db_name,
            "username": db_user,
            "password": db_password,
            "host": host,
            "port": port
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def new_replica(self, db_name: str, db_user: str, db_password: str, host: str, port: int, pool: int = 0,
                    username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/new-replica"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "dbname": db_name,
            "username": db_user,
            "password": db_password,
            "host": host,
            "port": port,
            "pool": pool
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def drop_database(self, db_name: str, ignore_offline: bool = False, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/drop-database"
        headers = {'Content-Type': 'application/json'}
        auth = HTTPBasicAuth(username=username, password=password)

        payload = {
            "database": db_name,
            "ignore_offline": ignore_offline
        }

        resp = requests.post(url=url, json=payload, headers=headers, auth=auth)
        return resp.text

    def get_version(self, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/get-version"
        headers = {'Content-Type': 'application/json'}

        auth = HTTPBasicAuth(username=username, password=password)
        resp = requests.get(url=url, headers=headers, auth=auth)

        return resp.text

    def get_accounts(self, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/get-accounts"
        headers = {'Content-Type': 'application/json'}

        auth = HTTPBasicAuth(username=username, password=password)
        resp = requests.get(url=url, headers=headers, auth=auth)

        return resp.text

    def get_databases(self, username: str = 'sa', password: str = 'siri'):

        url = 'http://' + self.host + ":" + str(self.port) + "/get-databases"
        headers = {'Content-Type': 'application/json'}

        auth = HTTPBasicAuth(username=username, password=password)
        resp = requests.get(url=url, headers=headers, auth=auth)

        return resp.text


if __name__ == '__main__':
    admin = SiriDBAdmin()

