﻿"""
Luminosity
Python module for reading and writing Luminex csv files
Tested under offical CPython 3.4 environment
"""
import csv, collections, datetime

__version__ = '0.1a'

class Reader(object):
    """
    Luminosity Reader class
    reads raw Luminex csv files into easy-to-manipulate variables
    """

    def __init__(self, csvfilename):
        """
        Reader class constructor
        """
        self.__csvfilename = csvfilename
        self.__header, self.__results = self.__readcsv(self.__csvfilename)

    def __readcsv(self, csvfilename):
        """
        Private method to split csv file into header and results
        """
        try:
            with open(csvfilename, 'r') as file:
                tempdata = [row for row in csv.reader(file) if row]
        except FileNotFoundError:
            print('%s => File cannot be found!'%csvfilename)
            return [], []
        except UnicodeDecodeError:
            print('%s => Invalid file format!'%csvfilename)
            return [], []
        except Exception as err:
            print('%s => %s'%(csvfilename, err))
            return [], []
        else:
            try:
                splitpoint = next(index for index, _ in enumerate(tempdata)
                                  if 'Samples' and 'Min Events' in _)
            except StopIteration:
                print('%s => Invalid Luminex csv file.'%csvfilename)
                return [], []
            else:
                return tempdata[ :splitpoint + 1], tempdata[splitpoint + 2: ]

    @property
    def datatypes(self):
        """
        All collected datatypes and parameters in a list[]
        """
        Datatype = collections.namedtuple('Datatype', ['name', 'index'])
        try:
            return [Datatype(row[1], index) 
                    for index, row in enumerate(self.__results) 
                    if 'DataType:' in row]
        except Exception:
            print('Error getting datatypes from %s.'%self.__csvfilename)
            return []

    @property
    def datatype_names(self):
        """list of all capture data type names"""
        return [x.name for x in self.datatypes]

    @property
    def beadnames(self):
        """
        Returns all beads (probes) names in the run
        """
        #gets the 'Location','Sample', ....., 'Notes' row
        try:
            tempcols = next((row for row in self.__results 
                         if 'Location' and 'Sample' and 'Notes' in row))
            #gets only the probe names, replace space with '_' for namedtuples
            return [probe.replace(' ', '_') for probe in tempcols[2:-2]]
        except StopIteration:
            print('No "Location", "Sample" found in : %s, check file format.'
                  %self.__csvfilename)
            return []
        except Exception:
            return []
        
    @property
    def samples(self):
        """
        Returns all sample names tested in the run in named tuples
        collections.namedtuple('Samplename', ['index', 'location', 'name'])
        """
        Samplename = collections.namedtuple('Samplename', 
                                            ['index', 'location', 'name'])
        top = self.datatypes[0].index + 2  # start of datatype[0] data
        end = self.datatypes[1].index      # end of the datatype[0] data
        samplenamelist = []
        for sample in self.__results[top : end]:
            idx, loc = self.__idxloc(sample[0])
            if idx:
                samplenamelist.append(Samplename(idx, loc, sample[1]))
        return samplenamelist
    @property
    def samplenames(self):
        """list of sample names"""
        return [x.name for x in self.samples]
    
    def samplename(self, index = None, location = None):
        """
        Returns a the name of a sample of a give index or location
        """
        temp1 = None
        temp2 = None
        if location is None:
            for s in self.samples:
                if index == s.index:
                    return s.name
        elif index is None:
            for s in self.samples:
                if location == s.location:
                    return s.name
        else:
            for s in self.samples:
                if index == s.index:
                    temp1 = s
                if location == s.location:
                    temp2 = s               
                if temp1.index == temp2.index:
                    return temp1.name
                else:
                    raise ValueError('index and location refer to different sample')
    
    def __idxloc(self, locstr):
        """helper function to return index and location"""
        #group(1) = index / group(2) = location
        pattern = r'(\d{1,2}\s?)(?:\(([A-H]\d{1,2})\))?'
        matches = csv.re.search(pattern, locstr)
        idx = None
        loc = None
        if matches:
            if matches.group(2):
                idx = int(matches.group(1))
                loc = matches.group(2)
            elif matches.group(1):
                idx = int(matches.group(1))
        return (idx, loc)
    
    def __beaddatatype(self, datatype = None):
        """
        helper function return section(s) of data of given datatypes
        in the form of str, tuple or list
        """
        # get the list of datatype names
        dlist = self.__validatedataparam(datatype, 
                                        (dt.name for dt in self.datatypes))
        for item in dlist:
            for idx, dtype in enumerate(self.datatypes):
                if item == dtype.name:
                    try:
                        # first to penultimate datatypes
                        yield(self.__results[dtype.index + 2: self.datatypes[idx + 1].index])
                    except:
                        # last datatype
                        yield(self.__results[dtype.index + 2:])

    def __validatedataparam(self, param, params):
        """helper function to validate input"""
        # convert to 1 tuple if is str
        if isinstance(param, (str, int)):
            param = (param, )

        # if not None, check if is a subset of params
        if param == None:   # if no entry, return whole list
            return params
        elif isinstance(param, (list, tuple)) and \
                    set(param).issubset(set(params)):
            return param
        else: # if not a subset or None return an empty tuple
            return ()

    def beaddata(self, datatype = None, sampleindices = None):
        """
        return a generator of data of given datatype and sample indices
        e.g.
        Reader.beaddata(datatype = 'Result') 
                returns the result section of all samples
        Reader.beaddata(datatype = 'Result', sampleindices=(1,)) 
                returns the result section of samples 1
        Reader.beaddata(sampleindices=(1,3,5)) 
                returns all result section of samples 1, 3 and 5
        """

        sampleList = self.__validatedataparam(sampleindices,
                                         (sample.index for sample in self.samples))
        for dataSection in self.__beaddatatype(datatype):
            for idx in sampleList:
                yield([self.__tryparsefloat(x) for x in dataSection[idx - 1]])

    def __tryparsefloat(self, string):
        """
        helper function returns float value of the string
        otherwise return the untouch string
        """
        try:
            return float(string)
        except ValueError:
            return string

    @property
    def header_params(self):
        """header parameters"""
        return [x for x in dir(self.header)
                if not x.startswith('_')]
        
    
    @property
    def header(self):
        """header property stores all meta data"""
        return self.__Header(self.__header)


    class __Header(object):
        """
        Header class to store meta data
        """
        def __init__(self, headerlist):
            self.__header = headerlist

        @property
        def Program(self):
            """ Program used to generate the Luminex csv file """
            try:
                return next((row[1] for row in self.__header if 'Program' in row))
            except Exception:
                return ''
        @property
        def Build(self):
            """
             Program version
            """
            try:
                return next((row[1] for row in self.__header if 'Build' in row))
            except Exception:
                return ''

        @property
        def datetime(self):
            """
             Date when data was generated
            """
            try:
                for row in self.__header:
                    if row[0] == 'Date' and ':' in row[2]:
                        return datetime.datetime.strptime(row[1] + ' ' + row[2], 
                                                          '%d/%m/%Y %H:%M:%S')
            except Exception:
                return ''


        @property
        def SN(self):
            """
             Luminex machine SN number
            """
            try:
                return next((row[1] for row in self.__header
                             if 'SN' in row))
            except Exception:
                return ''

        @property
        def Session(self):
            """
             Luminex session number
            """
            try:
                return next((row[1] for row in self.__header
                             if 'Session' in row))
            except Exception:
                return ''

        @property
        def Operator(self):
            """
             Operator
            """
            try:
                op = next((row[1] for row in self.__header
                             if 'Operator' in row))
                if op:
                    return op
                else:
                    return self.batch_author
            except Exception:
                return ''

        @property
        def TemplateID(self):
            """
             Template ID
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateID' in row))
            except Exception:
                return ''

        @property
        def TemplateName(self):
            """
             Template name
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateName' in row))
            except Exception:
                return ''

        @property
        def TemplateVersion(self):
            """
             Template version
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateVersion' in row))
            except Exception:
                return ''

        @property
        def TemplateDescription(self):
            """
             Template description
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateDescription' in row))
            except Exception:
                return ''

        @property
        def TemplateDevelopingCompany(self):
            """
             Template developing company
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateDevelopingCompany' in row))
            except Exception:
                return ''

        @property
        def TemplateAuthor(self):
            """
             Template author
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateAuthor' in row))
            except Exception:
                return ''

        @property
        def DDGate(self):
            """
             DDGate as set by the template
            """
            try:
                return next((row[1] for row in self.__header
                             if 'DDGate' in row))
            except Exception:
                return ''

        @property
        def SampleTimeout(self):
            """
             Sample timeout as set by the template
            """
            try:
                return next((row[1] for row in self.__header
                             if 'SampleTimeout' in row))
            except Exception:
                return ''

        @property
        def BatchAuthor(self):
            """
             Author of the multibatch if the session is part of a 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchAuthor' in row))
            except Exception:
                return ''

        @property
        def BatchStartTime(self):
            """
             Run start time of the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchStartTime' in row))
            except Exception:
                return ''

        @property
        def BatchStopTime(self):
            """
             Run stop time of the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchStopTime' in row))
            except Exception:
                return ''

        @property
        def BatchDescription(self):
            """
             Description of the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchDescription' in row))
            except Exception:
                return ''

        @property
        def BatchComment(self):
            """
             User comments in the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchComment' in row))
            except Exception:
                return ''
           
        @property
        def SampleVolume(self):
            """
             Sample volume as set by the template
            """
            try:
                return next((row[1] for row in self.__header
                             if 'SampleVolume' in row))
            except Exception:
                return ''

        @property
        def Min_Events(self):
            """
             Minimum events in a run
            """
            try:
                return int(next((row[3] for row in self.__header
                                 if 'Samples' and 'Min Events' in row)))
            except Exception:
                return 0


        @property
        def AssayLotInfo(self):
            """
             Assay standards or controls info
            """
            try:
                head = next((self.__header.index(row) + 1 for row in self.__header
                             if 'AssayLotInfo:' in row))
                tail = next((self.__header.index(row) for row in self.__header
                             if 'Samples' and 'Min Events' in row))
                return self.__header[head : tail]
            except Exception:
                return ''
        
        def __calconproc(self, data):
            if len(data) > 0 :
                return data[0]
            else:
                return data

        @property
        def CAL1(self):
            """
             CAL1 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Classification Calibrator' in row])

        @property
        def CAL2(self):
            """
             CAL2 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Reporter Calibrator' in row])

        @property
        def CON1(self):
            """
             CON1 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Classification Control' in row])

        @property
        def CON2(self):
            """
             CON2 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Reporter Control' in row])
