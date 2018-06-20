'''
Module handling vendor software configurations
'''
import json

class Configuration:
    
    def __init__(self, config_file):
        '''
        initialise with a config json file
        '''
        self.config_file = config_file
        with open(config_file, 'r') as fin:
            txt = fin.read()
        self.config = json.loads(txt)
        
    @property
    def connection_string(self):
        '''
        return the connection string as string
        '''
        return self.config['connection_string'].format(config=self.config)
    
    def update_config_file(self, server=None, db_name=None, password=None, user=None):
        '''
        update the configuration json file given 
        server, db_name, or password
        '''
        if server:
            self.config['server'] = server
        if db_name:
            self.config['db_name'] = db_name
        if password:
            self.config['password'] = password
        if user:
            self.config['user'] = user
        
        try:
            with open(self.config_file, 'w') as fout:
                fout.write(json.dumps(self.config))
        except PermissionError as e:
            print(e)
        except Exception as e:
            print(e)

        return True