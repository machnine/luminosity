##############################################################################
##                                                                          ##
##  Luminosity - A Python Module for Reading and Writing Luminex CSV Files  ##
##                 By Mian Chen [mianchen(at)gmail(dot)com]                 ##
##                        v0.2.3 2018-11-16                                 ##
##                                                                          ##
##                   Release Under: Creative Common License                 ##
##                     Attribution-NonCommercial-ShareAlike                 ##
##                                 CC BY-NC-SA                              ##
##                                                                          ##
##############################################################################

# Tested under CPython 3.4, pandas 0.18


    ############################ CHANGE LOG #######################
'''
    * Updated many Docstrings to nicer formats and with more details
    * Correct most of the warnings raised by PyLint
    * Added Mergewith() method to merge a new csv file with the current object
    * Added self.__get_sample_num(), changed sample_num property to just return
      the value of self.__sample_num private property
    * Add 'sanity check' to prevent merging data from different lots/kits
'''

import csv
import enum
import pandas as pd
from pandas import Series, DataFrame

pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', 20)
pd.set_option('display.max_colwidth', 20)

METAINFO = enum.Enum('METAINFO',
                     ['Program', 'Build', 'Date', 'SN', 'Session', 'Operator',
                      'TemplateID', 'TemplateName', 'TemplateVersion',
                      'TemplateDescription', 'TemplateDevelopingCompany',
                      'TemplateAuthor', 'SampleVolume', 'DDGate', 'SampleTimeout',
                      'BatchAuthor', 'BatchStartTime', 'BatchStopTime',
                      'BatchDescription', 'BatchComment'])

__version__ = '0.2.3'

class Luminosity:

    __doc__ = ('''
            Luminosity class represents a Luminex csv data file. 

            Parameter:
            ---------
                filename  : The full path to the csv file

            Properties:
            ----------
                data      : Unfiltered data in a DataFrame minus the meta data
                datatypes : All collected data types
                sample_num : Number of samples
                samples   : Sample names and locations in a Series
                beads     : Microbead labels in a list
                RawMeta   : Raw unprocessed metadata
                cal_info   : CAL beads readings
                con_info   : CON beads reading

            Methods:
            --------
                get_neta()    : Return meta when given a name
                update_from() : Update selected rows with new data
                merge_with()  : Merge current file with another csv file
                output()      : Write the changes to a csv file

            Exampls:
            --------
                See individual Docstrings for examples.
                
        ''')

    def __init__(self, filename):

        self.file_name = filename

        with open(filename, 'r') as handle:
            #open the file and convert into a list in memory
            self.__csv_data = list(csv.reader(handle.readlines()))

            #find the where every "DataType:" chunck locates
            self.__datatypes = self.__locate_data_types()

            #get number of samples
            self.__sample_num = self.__get_sample_num()

            #split header from the first row of "DataType:"
            split_point = self.__datatypes.min()
            self.__header = self.__csv_data[:split_point]

            #read data into a multi index dataframe
            self.__data = self.__read_data()

            #get meta data
            self.__meta = self.__meta_info()

    ######################### PRIVATE METHODS #####################

    def __locate_data_types(self):
        '''
            Find out how many "DataType:" sections are in the csv
            file and return where they are as a panda Series
        '''
        names = []
        locs = []
        for i, row in enumerate(self.__csv_data):
            if row and 'DataType:' in row[0]:
                names.append(row[1])
                locs.append(i)
        return Series(locs, index=names)



    def __locate_calcon(self):
        '''
            Locate the position of cal and con bead data if exist
        '''
        pos = {}
        for i, row in enumerate(self.__header):
            if row and 'CALInfo:' in row[0]:
                pos['CAL'] = i
            elif row and 'CONInfo:' in row[0]:
                pos['CON'] = i
        return pos

    def __calcon_info(self, calcon):
        '''
            Return cal or con info if exist
        '''
        try:
            #get the position of cal/con
            pos = self.__locate_calcon()[calcon]

            #turn into a dataframe
            #pos=location, pos+1=header, pos+2,pos+3 = values
            dframe = DataFrame(self.__header[pos+2:pos+4], \
                           columns=self.__header[pos+1], \
                           dtype=float)
            #reset index
            dframe.set_index(['ProductName'], inplace=True)
        except Exception:
            dframe = DataFrame()
        return dframe

    def __read_data(self):
        '''
            Return bead data dictionary in a dataframe with
            'DataType','Location' and 'Sample' as MultiIndex
        '''
        data = []
        dtnames = []

        dt_list = self.__datatypes

        #iterate through the datatypes
        for idx in dt_list.index:
            #infer 'count' and 'trimmed count' data to int
            #this step is necessary for OneLambda HLA Fusion to work
            if 'Count' in idx:
                dtype = int
            #infer all other numerical strings to float
            else:
                dtype = float
            dtnames.append(idx)   #name of data type
            loc = dt_list[idx]     #starting location of data
            data.append(self.__read_data_loc(loc, dtype=dtype))
        return pd.concat(data, keys=dtnames)

    def __one_data_block(self):
        '''
            Read the first data block in the csv file
        '''
        return self.__data.loc[self.__datatypes.index[0]]

    def __read_data_loc(self, dlocation, dtype=None):
        '''
            Return a dataframe of data chunck for a given datatype
        '''
        header_row = dlocation + 1
        first_row = dlocation + 2
        last_row = dlocation + 2 + self.__sample_num

        #convert and store data into dataDict using dataTypeName as key
        dframe = DataFrame(self.__csv_data[first_row:last_row], \
                       columns=self.__csv_data[header_row], \
                       dtype=dtype)

        #set Location column as index
        dframe.set_index('Location', inplace=True)
        return dframe

    def __get_sample_num(self):
        '''
            Return the calculated number of samples in the run
            the ["Samples","##","Min Events","##"] row is ignored
        '''
        #the number of rows difference between consecutive data blocks
        diff = self.__datatypes.diff()
        #len == 2 : only 1 consistent value + NaN
        #len > 2 : more than 1 consistent value + NaN
        #len < 2 : the data appear to be blank: all NaN
        if len(set(diff)) == 2:
            return int(diff[1]) - 3
        else:
            raise ValueError('Inconsistent number of samples '
                             'in each data block!')

    def __meta_info(self):
        '''
            Return a dict containing all meta info
        '''
        data_dict = {}
        for meta in METAINFO:
            data_dict[meta.name] = self.__get_meta_info(meta.name)
        return data_dict

    def __get_meta_info(self, name=None):
        '''
            Get the meta info for a given name
        '''
        raw_meta = self.__raw_meta()

        if name is None:
            return ''
        elif name == 'Date':
            date = ' '.join(raw_meta.loc[name, 'Value':'Extra'].values)
            return date
        elif name in raw_meta.index:
            return raw_meta.loc[name, 'Value']
        else:
            return ''



    def __raw_meta(self):
        '''
            Return meta data in a dict
        '''
        #convert header into a pandas dataframe
        dframe = DataFrame(self.__header)
        dframe.set_index(0, inplace=True)
        dframe.index.names = ['Meta_Info']
        dframe.rename(columns={1: 'Value', 2:'Extra'}, inplace=True)
        return dframe

    def __rebuild_meta(self):
        '''
            Reconstruct the first part of meta data down until
            CAL and CON info if exists
        '''
        text = ''
        for item in METAINFO:
            if item.name not in ['Program', 'Date']:
                text += ('\"{}\",\"{}\"\r\n'
                         .format(item.name, self.__meta[item.name]))
            elif item.name == 'Date':
                text += ('\"{}\",\"{d[0]}\", "{d[1]}\"\r\n\r\n'
                         .format(item.name, d=self.__meta[item.name].split(' ')))
            elif item.name == 'Program':
                text += ('\"{}\","{}\",809\r\n'
                         .format(item.name, self.__meta[item.name]))
        return text



    def __rebuild_calcon(self):
        '''
            Reconstruct CAL and CON info if exists
        '''
        text = ''
        if not self.cal_info.empty:
            text += '\"CALInfo:\"\r\n'
            text += self.cal_info.to_csv(line_terminator='\r\n', \
                                        quoting=csv.QUOTE_ALL, \
                                        quotechar='"')
        if not self.con_info.empty:
            text += '\"CONInfo:\"\r\n'
            text += self.con_info.to_csv(line_terminator='\r\n', \
                                        quoting=csv.QUOTE_ALL, \
                                        quotechar='"')
        return text



    def __rebuild_misc(self):
        '''
            Rebuild the last bit of meta info before the actual results
        '''
        return ('''\"AssayLotInfo:\"\r\n'''
                '''<No assay standards or controls found>'''
                '''\r\n\r\n\r\n\r\n\r\n\"Samples\",\"{}\",'''
                '''\"Min Events\",\"0\"\r\n\"Results\"'''
                '''\r\n\r\n'''.format(self.__sample_num))

    def __metacsv(self):
        '''
            Return csv string of meta data ready to be saved into a file
        '''
        return self.__rebuild_meta() +  self.__rebuild_calcon() + self.__rebuild_misc()



    def __data_csv(self):
        '''
            Reconstruct the main body of csv with data for all datatypes
        '''
        text = ''
        for idx in self.__data.index.levels[0]:
            text += '\"DataType:\",\"{}\"\r\n'.format(idx)
            text += self.__data.loc[idx].to_csv(line_terminator='\r\n', \
                                              quoting=csv.QUOTE_ALL, \
                                              quotechar='"')
            text += '\r\n'
        return text


    def __mod_loc(self, loc, delta):
        '''
           Helper function to shift location index by given delta
        '''
        location = loc.split(' ')
        try:
            location[0] = str(int(location[0]) + delta)
        except Exception:
            pass
        return ' '.join(location)

    def __mod_index(self, dframe, delta):
        '''
            Helper function returns a modifed MultiIndex object from df
            delta = number of positions that the 2nd level index is to be shifted
            Example:
            --------
            Original df.index:
                MultiIndex(levels=[['Median', 'Result', 'Count' ... 'Avg Result'],
                ['1 (H2)','2 (A3)', ...'7 (F3)','8 (G3)','9 (H3)']], ...)
                  ^        ^            ^        ^        ^

            modIndex(df, delta=10) returns:
                MultiIndex(levels=[['Median', 'Result', 'Count' ... 'Avg Result'],
                ['11 (H2)', '12 (A3)', ... '17 (F3)', '18 (G3)', '19 (H3)']], ...)
                  ^           ^             ^           ^          ^
        '''
        new_index = []
        for idx in dframe.index.tolist():
            new_index.append((idx[0], self.__mod_loc(idx[1], delta)))
        return pd.MultiIndex.from_tuples(new_index, names=dframe.index.names)

    ########################### PROPERTIES ###########################

    @property
    def data(self):
        '''
        return a dataframe with all data blocks
        '''
        return self.__data

    @property
    def datatypes(self):
        '''
            List of captured data types
        '''
        return self.__datatypes.index.tolist()

    @property
    def sample_num(self):
        '''
            Number of samples
        '''
        return self.__sample_num

    @property
    def samples(self):
        '''
        return all sample names and locations in a dataframe
        '''
        dblock = self.__one_data_block()
        return dblock['Sample']

    @property
    def beads(self):
        '''
            Return all bead labels/names
        '''
        return self.__one_data_block().columns[1:-2].tolist()

    @property
    def meta(self):
        '''
            Return meta data in a dict
        '''
        return self.__meta


    @property
    def cal_info(self):
        '''
            Return a df containing CAL1/CAL2 info
        '''
        return self.__calcon_info('CAL')

    @property
    def con_info(self):
        '''
            Return a df containing CON1/CON2 info
        '''
        return self.__calcon_info('CON')

    @property
    def loc_dict(self):
        '''
            Return a dictionary using well location as key and order a value
            e.g.  '1 (H1)' --> {'(H1)': '1'}
            TO DO: this does not work with locations not in this format
                   make something fit other formats
        '''
        return {x.split(' ')[1]:x.split(' ')[0] for x in self.samples.index}


    ######################### METHODS ###################################

    def get_meta(self, name=None):
        '''
            Return meta data when given a name or METAINFO enum
            Example:
            --------
            GetMeta('SN') and GetMeta(METAINFO.SN) returns
            the serial number of the Luminex machine
        '''
        if name and isinstance(name, METAINFO):
            return self.__meta[name.name]
        elif name and isinstance(name, str):
            return self.__meta[name]
        else:
            return ''

    def __update_loc(self, source, excluded:list=[]):
        excluded = [x.replace('(','').replace(')', '') for x in excluded]
        source_locs = source.loc_dict
        target_locs = self.loc_dict
        s_locs = []
        t_locs = []
        for k, v in source_locs.items():
            if k.replace('(','').replace(')','') not in excluded:
                s_locs.append(f'{v} {k}')
                t_locs.append(f'{target_locs[k]} {k}')
        return s_locs, t_locs

    def update_with_exclusion(self, from_obj, excluded_locs: list, ignorecheck=False):        
       self.update_from(from_obj, *self.__update_loc(from_obj, excluded_locs), ignorecheck=ignorecheck)


    def update_from(self, from_obj, from_loc, to_loc, ignorecheck=False):
        '''
            Update the current Luminosity object with one sample data
            from a different Luminosity object
            Example:
            --------
            UpdateWith(sourceObject, 'source sample label', 'target lable')
            UpdateWith(sourceObject, [list of labels], [list of targets])
        '''
        #sanity check
        if not ignorecheck:
            assert self.__meta['TemplateName'] == from_obj.meta['TemplateName'], \
                    'Attempted to merge data generated by different templates!' \
                    'The ignorecheck flag must be set to True for this to proceed.'

        #convert str into list like object
        if isinstance(from_loc, str) and isinstance(to_loc, str):
            from_loc = [from_loc]
            to_loc = [to_loc]

        try:
            #sanity check before updating
            if (Series(self.beads) == Series(from_obj.beads)).all() \
                and (Series(self.datatypes) == Series(from_obj.datatypes)).all():

                sdata = from_obj.data

                #back up data before updating
                backup_data = self.__data.copy()

                #updating...
                for floc, tloc in zip(from_loc, to_loc):
                    temp = sdata.xs(floc, level=1,
                                    drop_level=False)

                    #set the source location index same as target for update
                    locations = temp.index.levels[1].values
                    pos = list(locations).index(floc)
                    locations[pos] = tloc

                    #update the backup of this dataframe from source
                    backup_data.update(temp)

                    #reverse the source location index back to previous value
                    #NB: when the index of a new copy of a dataframe changes
                    #the original corresponding index in the old dataframe
                    #also changes.
                    locations[pos] = floc

                #if no exception, update the data of this object
                self.__data = backup_data
                return True

            else:
                raise ValueError
        except ValueError:
            msg = ('Update Failed: <{}> do not have identical bead '
                   'labels and/or data types to this object!'
                   .format(from_obj.FileName))

            return False, msg



    def merge_with(self, from_obj, updateloc=True, ignorecheck=False):
        '''
            Merge/concatenate data from another csv file
            updateloc = True:
                Modify the 'Location' index of from_obj
            updateloc = False:
                Leave the 'Location' indices unchanged

            ignorecheck = False:
                Check that both objects are generated by the same template

            Example:
            --------
            csvobj1.MergeWith(csvobj2, updateloc=True, ignorecheck=False)
        '''
        if not ignorecheck:
            assert self.__meta['TemplateName'] == from_obj.meta['TemplateName'], \
                 'Attempted to merge data generated by different templates!' \
                 'The ignorecheck flag must be set to True for this to proceed.'

        totalnum = self.sample_num + from_obj.sample_num

        if updateloc:
            from_obj.data.index = self.__mod_index(from_obj.data, \
								  delta=self.sample_num)
        self.__sample_num = totalnum
        self.__data = pd.concat([self.__data, from_obj.data])


    def output(self, filename=None):
        '''
            Outputs(writes) the data as a fully and correctly formatted
            Luminex csv file to a given file name. if no name is given
            write to the current folder using the current filename_new.csv
            as output destination file name.
            returns true/false and the output path depending on success
        '''
        if filename:
            file = filename
        else:
            file = self.file_name.replace('.csv', '_new.csv')
        text = self.__metacsv() + self.__data_csv()
        try:
            with open(file, 'w', newline='') as handle:
                handle.write(text)
            return True, file
        except Exception:
            return False, file
