import pypyodbc
from .sql_config import Configuration

class SqlConnect:
    '''
    Connect to SQL database
    '''
    def __init__(self, config_file):
        self.__conf = Configuration(config_file)
        self.__open_connection()
            
    def open(self):
        '''
        reopen database connection
        '''
        #check if already open
        try:
            #already open
            self.connection.cursor()
        except Exception as e:
            #already closed
            if e.__class__ == pypyodbc.ProgrammingError and \
               e.args[0] == 'HY000':
                #open the connection again
                self.__open_connection()
            else:
                #pass on any other exceptions
                raise e
                
        return True
        
            
    def __open_connection(self):
        '''
        helper function
        '''
        try:
            self.connection = pypyodbc.connect(self.__conf.connection_string)
        except pypyodbc.DatabaseError as e:
            error_code, error_msg = e.args
            if error_code == '28000':
                print('Username or password problem, failed to login.')
            elif error_code == '08001':
                print('Server connection problem, not found')
            else:
                raise e
        except pypyodbc.Error as e:
            error_code, error_msg = e.args
            if error_code == 'IM002':
                print('Data source name problem')
            else:
                raise e
        except pypyodbc.ProgrammingError as e:
            error_code, error_msg = e.args
            if error_code == '42000':
                print('The database name in connection string is not found')
            else:
                raise e
        except Exception as e:
            raise e            
            
            
            
    def close(self):
        '''
        close connection
        '''
        try:
            #close open connection
            self.connection.close()
        except Exception as e:
            #already closed
            if e.__class__ == pypyodbc.ProgrammingError and \
               e.args[0] == 'HY000':
               pass
            #pass on any other exceptions
            else:
                raise e
                
        return True
                
                
        