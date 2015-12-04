import datetime

class Header(object):
    """
    header class to store meta data
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