import yaml
from pathlib import Path


class Config:
    def __init__(self):
        # load config and secrets files into the config object's attributes
        self.load_config("config.yaml")
        self.load_config("secrets.yaml")

        # validate that the connection named in config file exists and that it has at least one of its required fields.
        # If you just check for name it could have been named properly in the secrets file but not in the config
        if hasattr(self, self.svc_tsc_gen['metadb_connection']) and "type" in getattr(self, self.svc_tsc_gen['metadb_connection']):
            # get metadata database connection
            self.svc_tsc_gen['metadb_connection'] = getattr(self, self.svc_tsc_gen['metadb_connection'])
            if 'username' not in self.svc_tsc_gen['metadb_connection'] or 'password' not in self.svc_tsc_gen['metadb_connection']:
                raise ValueError(f"Username or password not found for metadb_connection name specified in config. "
                                 f"Please specify a username and password in secrets.yaml file under the same name as in the config."
                                 f" See secrets_example.yaml for an example.")
        else:
            raise ValueError(f"Metadata connection name '{self.svc_tsc_gen['metadb_connection']}' not found in config file. "
                             f"For an example see example_config.yaml under the header 'metadb'. Replace 'metadb' with "
                             f"'{self.svc_tsc_gen['metadb_connection']}' and fill fields with your connection info.")


        self.svc_tsc_gen['max_workers'] = None if self.svc_tsc_gen['max_workers'] == 'None' else int(self.svc_tsc_gen['max_workers'])

        # set connection parameters if the database type is not sqlite
        if self.svc_tsc_gen['metadb_connection']['type'] == "sqlite":
            self.CONNECTION_PARAMS = None
        else:
            self.CONNECTION_PARAMS = {'host': self.svc_tsc_gen['metadb_connection']['host'],
                                      'user': self.svc_tsc_gen['metadb_connection']['username'],
                                      'password': self.svc_tsc_gen['metadb_connection']['password'],
                                      'database': self.svc_tsc_gen['metadb_connection']['db_name'],
                                      'port': self.svc_tsc_gen['metadb_connection']['port']}

    def load_config(self, file_name):
        stream = open(Path(__file__).parent.parent / file_name, 'r')
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
