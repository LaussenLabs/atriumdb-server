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
import yaml
from pathlib import Path
import ast


class Config:
    def __init__(self):
        # load config and secrets files into the config object's attributes
        self.load_config("config.yaml")
        self.load_config("secrets.yaml")

        # validate that the connection named in config file exists and that it has at least one of its required fields.
        # If you just check for name it could have been named properly in the secrets file but not in the config
        if hasattr(self, self.svc_wal_writer['metadb_connection']) and "type" in getattr(self, self.svc_wal_writer['metadb_connection']):
            # get metadata database connection
            self.svc_wal_writer['metadb_connection'] = getattr(self, self.svc_wal_writer['metadb_connection'])
            if 'username' not in self.svc_wal_writer['metadb_connection'] or 'password' not in self.svc_wal_writer['metadb_connection']:
                raise ValueError(f"Username or password not found for metadb_connection name specified in config. "
                                 f"Please specify a username and password in secrets.yaml file under the same name as in the config."
                                 f" See secrets_example.yaml for an example.")
        else:
            raise ValueError(f"Metadata connection name '{self.svc_wal_writer['metadb_connection']}' not found in config file. "
                             f"For an example see example_config.yaml under the header 'metadb'. Replace 'metadb' with "
                             f"'{self.svc_wal_writer['metadb_connection']}' and fill fields with your connection info.")

        # set connection parameters if the database type is not sqlite
        if self.svc_wal_writer['metadb_connection']['type'] == "sqlite":
            self.CONNECTION_PARAMS = None
        else:
            self.CONNECTION_PARAMS = {'host': self.svc_wal_writer['metadb_connection']['host'],
                                      'user': self.svc_wal_writer['metadb_connection']['username'],
                                      'password': self.svc_wal_writer['metadb_connection']['password'],
                                      'database': self.svc_wal_writer['metadb_connection']['db_name'],
                                      'port': self.svc_wal_writer['metadb_connection']['port']}
        # parse siridb connections if siri is enabled
        if self.svc_wal_writer['enable_siri']:
            self.siridb['hosts'] = [ast.literal_eval(conn) for conn in self.siridb['hosts']]

    def load_config(self, file_name):
        stream = open(f"/{file_name}", 'r')
        data = yaml.load(stream, Loader=yaml.FullLoader)
        config_keys = data.keys()
        # keys (k) will be the non-indented headers such as metadb, svc_tsc_gen ect
        for k in config_keys:
            # check if an attribute with that name has already been loaded
            if hasattr(self, k):
                # if it has, append the new values to the dictionary
                for subkey, value in data.get(k).items():
                    getattr(self, k)[subkey] = value
            else:
                # if it hasn't then add the attribute and the sub headers as a dictionary
                setattr(self, k, data.get(k))


config = Config()
