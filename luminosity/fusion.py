'''
OneLambda Fusion access module
'''
import json
import pypyodbc
import pandas as pd


# ## Todo
# ### 1. a few different ways of outputting? Luminosity style? csv? Excel with formula?
# ### 2. output to a database (sqlite?)
# ### 3. db -> new/overwrite/append etc.
# ### 4. testing


def read_fusion_connection_json(settings_json='fusion_settings.json'):
    '''
    read the connection setting file
    '''
    config = json.loads(open(settings_json, 'r').read())
    return config



def fusion_connection_string(config):
    '''create a connection string from config (dict)'''
    return config['connection_string'].format(server=config['server'],
                                             db_name=config['db_name'],
                                             password=config['password'])


def update_fusion_connection_json(settings_json='fusion_settings.json', **kwargs):
    '''
    update the connection setting file
    '''
    config = read_fusion_connection_json(settings_json)
    
    server = kwargs.get('server', None)
    db_name = kwargs.get('db_name', None)
    password = kwargs.get('password', None)
    
    if server:
        config['server'] = server
    if db_name:
        config['db_name'] = db_name
    if password:
        config['password'] = password
    try:
        with open(settings_json, 'w') as fout:
            fout.write(json.dumps(config))
    except PermissionError as e:
        print(e)
    except Exception as e:
        print(e)


def connect_to_fusion(connection_string):
    try:
        return pypyodbc.connect(connection_string)
    except Exception as e:
        print(e)
        return None


def read_data_from_fusion(patient_local_id, **kwargs):
    '''
    extract bead values from fusion given a unique patient_local_id
    '''
    #Luminex kit types
    kittype = kwargs.get('kittype', '').upper()
    if kittype == 'LSM':
        kittype = 't.CatalogID like \'LSM%\' and'
    elif kittype == 'SAB':
        ab_class = kwargs.get('_class', '_')
        if ab_class in [1, 2, '1', '2']:
            kittype = f't.CatalogID like \'LS{ab_class}A%\' and'
        else:
            kittype = f't.CatalogID like \'LS_A%\' and'    
    elif kittype == 'PRA':
        ab_class = kwargs.get('_class', '_')
        if ab_class in [1, 2, '1', '2']:
            kittype = f't.CatalogID like \'LS{ab_class}PRA%\' and'
        else:
            kittype = f't.CatalogID like \'LS_PRA%\' and'
            
    #limit the date range of the results
    session_date = kwargs.get('date_range', '')
    if session_date:
        session_date = f'and t.AddDT >= \'{session_date[0]}\' and t.AddDT < \'{session_date[1]}\''
    
    sql = f'''
        select  

                p.patientid as patient_id
                ,t.trayidname as session_name
                ,s.sampleidname as sample_name      
                ,t.catalogid as catalog_id
                ,d.beadid as bead_id
                ,replace(
                    replace(
                        convert(nvarchar(100), pd.specabbr), '-,',''
                        ), ',-', ''
                    ) as ab_sero
                ,replace(
                    replace(
                        convert(nvarchar(100), pd.specificity), '-,', ''
                        ), ',-', ''
                    ) as ab_mol
                ,d.rawdata as raw_value
                ,d.normalvalue as baseline_value

        from 

            patient as p

                join sample as s on s.PatientID = p.PatientID
                join well as w on w.SampleID = s.SampleID
                join tray as t on t.TrayID = w.TrayID
                join well_detail as d on d.WellID = w.WellID
                join product_detail as pd on pd.BeadID = d.BeadID 
                                         and t.CatalogID = pd.CatalogID
        where 
            {kittype}
            p.patientid ='{patient_local_id}'
            {session_date}
            
        order by 

            s.shipmentdt

        '''
        
    return pd.read_sql_query(sql, con=con)


con = connect_to_fusion(fusion_connection_string(read_fusion_connection_json()))


# d = read_data_from_fusion(patient_local_id=7766
#                       ,_class=2
#                       ,date_range=['2016-01-01', '2017-04-01']
#                       ,kittype='sab')

