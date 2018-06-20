from sql_db import SqlConnect
import pandas as pd
import os

matchit_db = SqlConnect('matchit_settings.json')
fusion_db = SqlConnect('fusion_settings.json')

def close_both_dbs():
    matchit_db.close()
    fusion_db.close()

def read_matchitdata(session_id, sample_id, matchitdb):
    '''
        execute the store procedure in matchIT to extract
        bead (antigen) and adjustn (AD-BG MFI) values
    '''
    mexec = ('exec dbo.TT_Get_Adj_BG_MFI' \
             ' \'{session_id}\', \'{sample_id}\' \
             ').format(session_id=session_id, sample_id=sample_id)
    md = pd.read_sql_query(mexec, con=matchitdb.connection)
    #todo : sort allele to guarantee that alpha is always first
    gmd = md.groupby(['bead', 'adjustn']).apply(lambda x: '-'.join(x.allele))
    gmd = gmd.reset_index()
    gmd = gmd.iloc[:, 1:].copy()
    gmd.columns = ['lifematch', 'allele']
    gmd.set_index('allele', inplace=True)
    return gmd
    
def allele_concat(row):
    '''
        helper function to concat DQA-DQB and DPA-DPB
        for fusion data
    '''
    if row.alpha != '-':
        return f'{row.alpha}-{row.beta}'
    else:
        return row.beta   
    
def resplit_allele(allele):
    '''
        helper function to split DQA-DQB and DPA-DPB
        or put a Null value to DRA field
    '''
    alleles = allele.split('-')
    if len(alleles) > 1:
        return alleles
    else:
        return [None, alleles[0]]
    
    
def locus_order(x):
    '''
    helper function: assign a sorting order for each locus
    '''
    sort_orders = {'A' : 0, 'B' : 1, 'C' : 2,
                     'DRB1' : 3, 'DRB5' : 4, 'DRB3' : 5, 'DRB4' : 6,
                     'DQB1' : 7, 'DPB1' : 8}
    
    for locus in sort_orders.keys():
        if x.startswith(locus):
            return sort_orders[locus]
        
        
def read_fusiondata(session_id, sample_id, fusiondb):
    '''
        execute the store procedure in fusion to extract
        bead (antigen) and normalvalue (normalised MFI) values
    '''
    fexec = ('exec dbo.tt_Get_Normal_MFI' \
             ' \'{session_id}\', \'{sample_id}\' \
             ').format(session_id=session_id, sample_id=sample_id)
    fd = pd.read_sql_query(fexec, con=fusiondb.connection)
    fd['allele'] = fd.apply(allele_concat, axis=1)
    gfd = fd[['allele', 'normalvalue']].copy()
    gfd.columns = ['allele', 'labscreen']
    gfd.set_index('allele', inplace=True)
    return gfd


def export_data_csv(msession_id, fsession_id, msample_id, fsample_id, file_path=None, return_data=False):
    '''
        merge two data sets and sort them and export to csv file
    '''
    #concat data sets
    d =  pd.concat([read_matchitdata(msession_id, msample_id, matchit_db), 
                   read_fusiondata(fsession_id, fsample_id, fusion_db)], 
                   axis=1)
    
    #reset index to data column
    d.reset_index(inplace=True)
    #resplit the alleles into a list
    d['temp'] = d['index'].apply(resplit_allele)
    #assign the list in 'temp' to alpha/beta
    d['alpha'] = d['temp'].apply(lambda x: x[0])
    d['beta'] = d['temp'].apply(lambda x: x[1])
    
    #assign a sorting order
    d['locus_order'] = d['beta'].apply(locus_order)
    
    #sort inplace
    d.sort_values(by=['locus_order', 'beta', 'alpha'], inplace=True)
    
    #only keep the relevant data
    d = d[['alpha', 'beta', 'lifematch', 'labscreen']]
    
    #export to csv removing index
    csv_file = msession_id +'_' + msample_id + '.csv'
    if file_path:
        csv_file = os.path.join(file_path, csv_file)
        
    d.to_csv(csv_file, index=False)
    
    #export data for debug or other purposes
    if return_data:
        return d