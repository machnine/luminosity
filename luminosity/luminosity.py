"""
Luminosity
Python module for reading and writing Luminex csv files
Tested under offical CPython 3.4 environment
"""

import csv, collections, datetime

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
            print('{0} => File cannot be found!'.format(csvfilename))
            return [], []
        except UnicodeDecodeError:
            print('{0} => Invalid file format!'.format(csvfilename))
            return [], []
        except Exception as err:
            print('{0} => {1}'.format(csvfilename, err))
            return [], []
        else:
            try:
                splitpoint = next(index for index, _ in enumerate(tempdata)
                                  if 'Samples' and 'Min Events' in _)
            except StopIteration:
                print('{0} => Invalid Luminex csv file.'.format(csvfilename))
                return [], []
            else:
                return tempdata[ :splitpoint + 1], tempdata[splitpoint + 2: ]

    @property
    def datatypes(self):
        """
        All collected datatypes in a list[]
        """
        Datatype = collections.namedtuple('Datatype', ['name', 'index'])
        try:
            return [Datatype(row[1], index) 
                    for index, row in enumerate(self.__results) 
                    if 'DataType:' in row]
        except Exception:
            print('Error getting datatypes from {}'.format(self.__csvfilename))
            return []

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
            print('No "Location", "Sample" found in : {}, check file format.'
                  .format(self.__csvfilename))
            return []
        except Exception:
            return []
        
    @property
    def samplenames(self):
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
                                         (sample.index for sample in self.samplenames))
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
        def program(self):
            """ Program used to generate the Luminex csv file """
            try:
                return next((row[1] for row in self.__header if 'Program' in row))
            except Exception:
                return ''
        @property
        def build(self):
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
        def machine_sn(self):
            """
             Luminex machine SN number
            """
            try:
                return next((row[1] for row in self.__header
                             if 'SN' in row))
            except Exception:
                return ''

        @property
        def session(self):
            """
             Luminex session number
            """
            try:
                return next((row[1] for row in self.__header
                             if 'Session' in row))
            except Exception:
                return ''

        @property
        def operator(self):
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
        def template_id(self):
            """
             Template ID
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateID' in row))
            except Exception:
                return ''

        @property
        def template_name(self):
            """
             Template name
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateName' in row))
            except Exception:
                return ''

        @property
        def template_version(self):
            """
             Template version
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateVersion' in row))
            except Exception:
                return ''

        @property
        def template_description(self):
            """
             Template description
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateDescription' in row))
            except Exception:
                return ''

        @property
        def template_developing_company(self):
            """
             Template developing company
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateDevelopingCompany' in row))
            except Exception:
                return ''

        @property
        def template_author(self):
            """
             Template author
            """
            try:
                return next((row[1] for row in self.__header
                             if 'TemplateAuthor' in row))
            except Exception:
                return ''

        @property
        def ddgate(self):
            """
             DDGate as set by the template
            """
            try:
                return next((row[1] for row in self.__header
                             if 'DDGate' in row))
            except Exception:
                return ''

        @property
        def sample_timeout(self):
            """
             Sample timeout as set by the template
            """
            try:
                return next((row[1] for row in self.__header
                             if 'SampleTimeout' in row))
            except Exception:
                return ''

        @property
        def batch_author(self):
            """
             Author of the multibatch if the session is part of a 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchAuthor' in row))
            except Exception:
                return ''

        @property
        def batch_starttime(self):
            """
             Run start time of the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchStartTime' in row))
            except Exception:
                return ''

        @property
        def batch_stoptime(self):
            """
             Run stop time of the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchStopTime' in row))
            except Exception:
                return ''

        @property
        def batch_description(self):
            """
             Description of the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchDescription' in row))
            except Exception:
                return ''

        @property
        def batch_comment(self):
            """
             User comments in the 'Multibatch'
            """
            try:
                return next((row[1] for row in self.__header
                             if 'BatchComment' in row))
            except Exception:
                return ''
           
        @property
        def sample_volume(self):
            """
             Sample volume as set by the template
            """
            try:
                return next((row[1] for row in self.__header
                             if 'SampleVolume' in row))
            except Exception:
                return ''

        @property
        def min_events(self):
            """
             Minimum events in a run
            """
            try:
                return int(next((row[3] for row in self.__header
                                 if 'Samples' and 'Min Events' in row)))
            except Exception:
                return 0


        @property
        def assay_lotinfo(self):
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
        def cal1(self):
            """
             CAL1 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Classification Calibrator' in row])

        @property
        def cal2(self):
            """
             CAL2 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Reporter Calibrator' in row])

        @property
        def con1(self):
            """
             CON1 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Classification Control' in row])

        @property
        def con2(self):
            """
             CON2 beads reading
            """
            return self.__calconproc([row for row in self.__header
                    if 'Reporter Control' in row])
