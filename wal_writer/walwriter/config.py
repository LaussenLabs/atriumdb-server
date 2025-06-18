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
import os
import ast

class ConfigurationError(Exception):
    pass

class Config:
    def __init__(self):
        # load config and secrets files into the config object's attributes
        self.load_config("config.yaml")
        self.load_config("secrets.yaml", fallback_to_env=True)

        # validate that the connection named in config file exists and that it has at least one of its required fields.
        # If you just check for name it could have been named properly in the secrets file but not in the config
        try:
            wal_writer_cfg = self.svc_wal_writer
        except AttributeError:
            raise ConfigurationError("Missing 'svc_wal_writer' in config.yaml.")
        
        if hasattr(self, wal_writer_cfg['metadb_connection']) and "type" in getattr(self, wal_writer_cfg['metadb_connection']):
            # get metadata database connection
            wal_writer_cfg['metadb_connection'] = getattr(self, wal_writer_cfg['metadb_connection'])
            if 'username' not in wal_writer_cfg['metadb_connection'] or 'password' not in wal_writer_cfg['metadb_connection']:
                raise ValueError(f"Username or password not found for metadb_connection name specified in config. "
                                 f"Please specify a username and password in secrets.yaml file under the same name as in the config."
                                 f" See secrets_example.yaml for an example.")
        else:
            raise ValueError(f"Metadata connection name '{wal_writer_cfg['metadb_connection']}' not found in config file. "
                             f"For an example see example_config.yaml under the header 'metadb'. Replace 'metadb' with "
                             f"'{wal_writer_cfg['metadb_connection']}' and fill fields with your connection info.")

        # set connection parameters if the database type is not sqlite
        if wal_writer_cfg['metadb_connection']['type'] == "sqlite":
            self.CONNECTION_PARAMS = None
        else:
            self.CONNECTION_PARAMS = {'host': wal_writer_cfg['metadb_connection']['host'],
                                      'user': wal_writer_cfg['metadb_connection']['username'],
                                      'password': wal_writer_cfg['metadb_connection']['password'],
                                      'database': wal_writer_cfg['metadb_connection']['db_name'],
                                      'port': wal_writer_cfg['metadb_connection']['port']}
        # parse siridb connections if siri is enabled
        if wal_writer_cfg['enable_siri']:
            self.siridb['hosts'] = [ast.literal_eval(conn) for conn in self.siridb['hosts']]

    def load_config(self, file_name, fallback_to_env=False):
        path = Path(f"/" + file_name)
        yaml_data = {}

        # load yaml if provided
        if path.exists():
            with path.open("r") as stream:
                try:
                    yaml_data = yaml.load(stream, Loader=yaml.FullLoader) or {}
                except yaml.YAMLError as e:
                    raise ConfigurationError(f"Failed to parse YAML file '{file_name}': {e}")

            config_keys = yaml_data.keys()
            # keys (k) will be the non-indented headers such as metadb, svc_tsc_gen ect
            for k in config_keys:
                # check if an attribute with that name has already been loaded
                if hasattr(self, k):
                    # if it has, append the new values to the dictionary
                    for subkey, value in yaml_data.get(k).items():
                        getattr(self, k)[subkey] = value
                else:
                    # if it hasn't then add the attribute and the sub headers as a dictionary
                    setattr(self, k, yaml_data.get(k))

        # if no yaml provided, try environment variables if requested
        elif fallback_to_env:
            print(f"[config] {file_name} not found. Falling back to environment variables.", flush=True)
            # iterate through environment variables prefixing *_USER or *_PASSWORD
            env_vars = os.environ
            for section_name in vars(self):
                section = getattr(self, section_name)
                if not isinstance(section, dict):
                    continue

                # use 'type' as env prefix if available, otherwise fallback to section name
                # e.g. for MARIADB_USER this will be saved under METADB_USER if the metadb type
                # in config is set to mariadb, else it will be saved under MARIADB_USER
                env_prefix = section.get("type", section_name).upper()

                user_key = f"{env_prefix}_USER"
                pass_key = f"{env_prefix}_PASSWORD"

                loaded = []
                if user_key in env_vars:
                    section["username"] = env_vars[user_key]
                    loaded.append("username")
                if pass_key in env_vars:
                    section["password"] = env_vars[pass_key]
                    loaded.append("password")

                if loaded:
                    setattr(self, section_name, section)
                    print(f"[config] Loaded {', '.join(loaded)} from env into '{section_name}'", flush=True)
        else:
            raise ConfigurationError(f"Missing required config file: {file_name}")


config = Config()
