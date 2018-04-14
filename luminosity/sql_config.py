'''
Module handling vendor software configurations
'''
import json

class Configuration:
    
    def __init__(self, config_file):
        self.config_file = config_file
        with open(config_file, 'r') as fin:
            txt = fin.read()
        self.config = json.loads(txt)
        
    @property
    def connection_string(self):
        return self.config['connection_string'].format(config=self.config)
    
    def update_config_file(self, **kwargs):
        for key in kwargs.keys():
            if key in self.config.keys():
                self.config[key] = kwargs[key]
            else:
                raise ValueError('Configuration parameter/key error!')
        
        try:
            with open(self.config_file, 'w') as fout:
                fout.write(json.dumps(self.config))
        except PermissionError as e:
            print(e)
        except Exception as e:
            print(e)

        return True