##############################################################################
##                                                                          ##
##  Luminosity - A Python Module for Reading and Writing Luminex CSV Files  ##
##                 By Mian Chen [mianchen(at)gmail(dot)com]                 ##
##                        v0.2 2016-03-10                                   ##
##                                                                          ##
##                   Release Under: Creative Common License                 ##
##                     Attribution-NonCommercial-ShareAlike                 ##
##                                 CC BY-NC-SA                              ##
##                                                                          ##
##############################################################################

# Tested under CPython 3.4, pandas 0.17

import pandas as pd
import csv, enum
from pandas import Series, DataFrame

pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_colwidth', 20)

__version__ = '0.2.1'


METAINFO = enum.Enum('METAINFO', 
                     ['Program','Build','Date','SN','Session','Operator',
                      'TemplateID','TemplateName','TemplateVersion',
                      'TemplateDescription','TemplateDevelopingCompany',
                      'TemplateAuthor','SampleVolume','DDGate','SampleTimeout',
                      'BatchAuthor','BatchStartTime','BatchStopTime',
                      'BatchDescription','BatchComment'])

class Luminosity:
    
    def __init__(self, filename):
        
        self.FileName = filename
        
        with open(filename, 'r') as f:
            #open the file and convert into a list in memory
            self.__data = list(csv.reader(f.readlines()))
            
            #find the where every "DataType:" chunck locates
            self.__dataTypes = self.__locateDataTypes()
            
            #split header from the first row of "DataType:"
            split_point = self.__dataTypes.min()
            self.__header = self.__data[:split_point]
            
            #read data into a multi index dataframe
            self.__data = self.__readData()
            
            #get meta data
            self.__meta = self.__metaInfo()
   

    ######################### PRIVATE METHODS #####################

    def __locateDataTypes(self):
        '''
        find out how many "DataType:" sections are in the csv
        file and return where they are as a panda Series 
        '''
        names = []
        locs = []
        for i, x in enumerate(self.__data):
            if x and 'DataType:' in x[0]:
                names.append(x[1])
                locs.append(i)
        return Series(locs, index=names)
      
    def __locateCalCon(self):
        '''
        locate the position of cal and con bead data if exist
        '''
        d = {}
        for i, x in enumerate(self.__header):
            if x and 'CALInfo:' in x[0]:
                d['CAL'] = i
            elif x and 'CONInfo:' in x[0]:
                d['CON'] = i
        return d
    
    def __calConInfo(self, calcon):
        '''
        return cal or con info if exist
        '''
        try:
            #get the position of cal/con
            n = self.__locateCalCon()[calcon]
            
            #turn into a dataframe
            #n=location, n+1=header, n+2,n+3 = values
            df = DataFrame(self.__header[n+2:n+4], 
                           columns = self.__header[n+1],
                           dtype = float)
            #reset index
            df.set_index(['ProductName'], inplace=True)
        except:
            df = DataFrame()
        return df
    
    def __readData(self, DataTypes=None):
        '''
        return sections of bead data in a dictionary of dataframes
        each dataframe contains one section of the bead data belongs
        to a datatype. dataType can be the datatype name (str) or 
        list like object containing all of the wanted names
        '''
        data = []
        names = []
        
        #if DataType specified use it to restrict return results
        if DataTypes:
            #slice the self.__datatypes Series
            #if a string is given
            if isinstance(DataTypes, str):
                dtList = self.__dataTypes[[DataTypes]]
            #if a list is given
            else:
                dtList = self.__dataTypes[DataTypes]

        #otherwise use the full Series
        else:
            dtList = self.__dataTypes
        
        #iterate through the selected datatypes
        for x in dtList.index:
            #infer 'count' and 'trimmed count' data to int
            #this step is necessary for OneLambda HLA Fusion to work
            if 'Count' in x:
                dtype = int
            #infer all other numerical strings to float
            else:
                dtype = float
                
            names.append(x)    #name of data
            loc = dtList[x]    #starting location of data
            data.append(self.__readDataLoc(loc, dtype=dtype))
        return pd.concat(data, keys=names)
 
    def __oneDataBlock(self):
        '''
        read the first data block
        '''
        return self.__data.loc[self.__dataTypes.index[0]]
   
    def __readDataLoc(self, dtLoc, dtype=None):
        '''
        return a dataframe of data chunck for a given datatype
        '''
        hRow = dtLoc + 1                  #position of the header row
        fRow = dtLoc + 2                  #first row of the chunck of data
        lRow = dtLoc + 2 + self.SampleNum #last row
        
        #convert and store data into dataDict using dataTypeName as key
        df = DataFrame(self.__data[fRow:lRow],
                       columns = self.__data[hRow],
                       dtype = dtype) 
            
        #set Location column as index
        df.set_index('Location', inplace=True)
        return df
   
    def __metaInfo(self):
        '''
        MetaInfo property return a dict containing all meta info
        '''
        d = {}
        for x in METAINFO:
            d[x.name] = self.__getMetaInfo(x.name)
        return d
    
    def __getMetaInfo(self, Name=None):
        '''
        get the meta info for a given Name
        '''
        if Name == None:
            return ''
        elif Name == 'Date':
            date = ' '.join(self.RawMeta.loc[Name, 'Value':'Extra'].values)
            return date
        elif Name in self.RawMeta.index:
            return self.RawMeta.loc[Name, 'Value']
        else:
            return ''

    def __rebuildMeta(self):
        '''
        reconstruct the first part of meta data down until
        CAL and CON info if exists
        '''
        text = ''
        for x in METAINFO:
            if x.name not in ['Program', 'Date']:
                text += ('\"{}\",\"{}\"\r\n'
                         .format(x.name, self.__meta[x.name]))
            elif x.name == 'Date':
                text += ('\"{}\",\"{d[0]}\", "{d[1]}\"\r\n\r\n'
                         .format(x.name, d = self.__meta[x.name].split(' ')))
            elif x.name == 'Program':
                text += ('\"{}\","{}\",809\r\n'
                         .format(x.name, self.__meta[x.name]))
        return text
        
    def __rebuildCalcon(self):
        '''
        reconstruct CAL and CON info if exists
        '''
        text = ''
        if not self.CalInfo.empty:
            text += '\"CALInfo:\"\r\n'
            text += self.CalInfo.to_csv(line_terminator='\r\n', 
                                        quoting=csv.QUOTE_ALL, 
                                        quotechar='"')
        if not self.ConInfo.empty:
            text += '\"CONInfo:\"\r\n'
            text += self.ConInfo.to_csv(line_terminator='\r\n', 
                                        quoting=csv.QUOTE_ALL, 
                                        quotechar='"')
        return text
        
    def __rebuildMisc(self):
        '''
        Rebuild the last bit of meta info before the actual results
        '''
        return ('''\"AssayLotInfo:\"\r\n'''
                '''<No assay standards or controls found>'''
                '''\r\n\r\n\r\n\r\n\r\n\"Samples\",\"{}\",'''
                '''\"Min Events\",\"0\"\r\n\"Results\"'''
                '''\r\n\r\n'''.format(self.SampleNum))
  
    def __metaCSV(self):
        '''
        return csv string of meta data ready to be saved into a file
        '''
        return self.__rebuildMeta() + \
               self.__rebuildCalcon() + self.__rebuildMisc()
        
    def __dataCSV(self):
        text = ''
        for x in self.__data.index.levels[0]:
            text += '\"DataType:\",\"{}\"\r\n'.format(x)
            text += self.__data.loc[x].to_csv(line_terminator='\r\n', 
                                              quoting=csv.QUOTE_ALL,
                                              quotechar='"') 
            text +='\r\n'
        return text

    ########################### PROPERTIES ###########################
    
    @property
    def Data(self):
        '''
        return a dataframe with all data blocks
        '''
        return self.__data
    
    @property
    def DataTypes(self):
        '''
        list of captured data types
        '''
        return self.__dataTypes.index
    
    @property
    def SampleNum(self):
        '''
        return the calculated number of samples in the run
        the ["Samples","##","Min Events","##"] row is ignored
        '''
        #the number of rows difference between consecutive data blocks
        diff = self.__dataTypes.diff()
        #len == 2 : only 1 consistent value + NaN
        #len > 2 : more than 1 consistent value + NaN
        #len < 2 : the data appear to be blank: all NaN
        if len(set(diff)) == 2:
            return int(diff[1]) - 3
        else:
            raise ValueError('Inconsistent number of samples '
                             'in each data block!')
    
    @property
    def Samples(self):
        '''
        return all sample names
        '''
        return self.__oneDataBlock()['Sample']
    
    @property
    def Beads(self):
        '''
        return all bead labels/names
        '''
        return self.__oneDataBlock().columns[1:-2] 
    
    @property
    def RawMeta(self):
        '''
        Return meta data in a dict
        '''       
        #convert header into a pandas dataframe
        df = DataFrame(self.__header)
        df.set_index(0, inplace=True)
        df.index.names = ['Meta_Info']
        df.rename(columns={1: 'Value', 2:'Extra'}, inplace=True)
        return df

    @property
    def Meta(self):
        '''
        return meta data in a dict
        '''
        return self.__meta

      
        
        
    @property
    def CalInfo(self):
        '''
        return a df containing CAL1/CAL2 info
        '''
        return self.__calConInfo('CAL')
    
    @property
    def ConInfo(self):
        '''
        return a df containing CON1/CON2 info
        '''        
        return self.__calConInfo('CON')

    
    ######################### METHODS ###################################
    
    def GetMeta(self, Name=None):
        '''
        return meta data when given a name or METAINFO enum
        '''
        if Name and isinstance(Name, METAINFO):
            return self.__meta[Name.name]
        elif Name and isinstance(Name, str):
            return self.__meta[Name]
        else:
            return ''
    
    def UpdateWith(self, from_obj, from_loc, to_loc):
        '''
        update the current Luminosity object with one sample data 
        from a different Luminosity object
        Usage:
        UpdateWith(sourceObject, 'source sample label', 'target lable')
        UpdateWith(sourceObject, [list of labels], [list of targets])
        '''
        #convert str into list like object
        if isinstance(from_loc, str) and isinstance(to_loc, str):
            from_loc = [from_loc]
            to_loc = [to_loc]
        
        try:
            #sanity check before updating
            if (self.Beads == from_obj.Beads).all() \
                and (self.DataTypes == from_obj.DataTypes).all():
                   
                sdata = from_obj.Data
                
                #back up data before updating
                backupData = self.__data.copy()
                
                #updating...
                for floc, tloc in zip(from_loc, to_loc):
                    temp = sdata.xs(floc, level = 1,
                                    drop_level = False)
                    
                    #set the source location index same as target for update
                    locations = temp.index.levels[1].values
                    n = list(locations).index(floc)
                    locations[n] = tloc
                    
                    #update the backup of this dataframe from source
                    backupData.update(temp)
                    
                    #reverse the source location index back to previous value
                    #NB: when the index of a new copy of a dataframe changes
                    #the original corresponding index in the old dataframe
                    #also changes.
                    locations[n] = floc
                
                #if no exception, update the data of this object
                self.__data = backupData
                return True
            
            else:
                raise ValueError
        except ValueError:
            msg = ('Update Failed: <{}> do not have identical bead '
                   'labels and/or data types to this object!'
                   .format(from_obj.FileName))
            
            return False, msg
    
    def Output(self, FileName=None):
        '''
        output (write) the data as a fully and correctly formatted
        Luminex csv file to a given file name. if no name is given
        write to the current folder using the current filename_new.csv
        as output destination file name.
        returns true/false and the output path depending on success
        '''
        if FileName:
            file = FileName
        else:
            file = self.FileName.replace('.csv', '_new.csv')
        text = self.__metaCSV() + self.__dataCSV()
        try: 
            with open(file, 'w', newline='') as f:
                f.write(text)
            return True, file
        except:
            return False, file

