import os
from datetime import datetime, timezone
import numpy as np
from collections import OrderedDict
import logging


from HSTB.drivers import kmall, par3, sbet, svp
from HSTB.kluster import monitor


supported_mbes = ['.all', '.kmall']
supported_sbet = ['.out', '.sbet', '.smrmsg']  # people keep mixing up these extensions, so just check for the nav/smrmsg in both
supported_export_log = ['.txt', '.log']
supported_svp = ['.svp']
all_extensions = list(np.concatenate([supported_mbes, supported_sbet, supported_export_log, supported_svp]))


class LoggerClass:
    """
    Basic class for logging.  Include a logging.logger instance to use that, or set silent to true to disable print
    messages entirely.  Use of Logger will trump silent.
    """

    def __init__(self, silent=False, logger=None):
        self.silent = silent
        self.logger = logger

    def print_msg(self, msg: str, loglvl: int = logging.INFO):
        """
        Either print to console, print using logger, or do not print at all, if self.silent = True

        Parameters
        ----------
        msg
            message contents as string
        loglvl
            one of the logging enum values, logging.info or logging.warning as example
        """

        if self.logger is not None:
            if not isinstance(loglvl, int):
                raise ValueError('Log level must be an int (see logging enum), found {}'.format(loglvl))
            self.logger.log(loglvl, msg)
        elif self.silent:
            pass
        else:
            print(msg)


class FqprIntel(LoggerClass):
    """
    Kluster intelligence module.

    Take in a file, gather the file level information, determine where in the project it needs to go (based on things
    like serial number and time of file).  The project contains converted data by system/sector/frequency etc.

    From there determine how to proceed.  Is the user ready to convert and the incoming file is a multibeam file?  Go
    ahead and convert the line to the appropriate Fqpr instance within the project (appending to or overwriting the
    existing data).  Does the user want to go ahead and process all the way to georeference on adding new lines?
    Run the full processing for that line.

    If the incoming file is an SBET, run import ppnav for all applicable data.  Re-run georeference after.

    If the incoming file is an SVP, run svcorrect on the cast on the nearest in time data that would apply.
    """

    def __init__(self, project=None, **kwargs):
        super().__init__(**kwargs)
        self.project = project
        self.multibeam_intel = MultibeamModule(silent=self.silent, logger=self.logger)
        self.nav_intel = NavigationModule(silent=self.silent, logger=self.logger)
        self.naverror_intel = NavErrorModule(silent=self.silent, logger=self.logger)
        self.navlog_intel = NavExportLogModule(silent=self.silent, logger=self.logger)
        self.svp_intel = SvpModule(silent=self.silent, logger=self.logger)

        self.unique_id = 0
        self.monitors = {}

    def return_intel_modules(self):
        """
        Return a list of all intel modules

        Returns
        -------
        list
            list of all intel modules
        """

        intel_modules = []
        for attr in vars(self):
            potential_module = self.__getattribute__(attr)
            if isinstance(potential_module, IntelModule):
                intel_modules.append(potential_module)
        return intel_modules

    def start_folder_monitor(self, folderpath, is_recursive=True):
        """
        Create a new DirectoryMonitor object for the provided folder path.  Automatically start the monitoring
        and store it as an attribute in the monitors dictionary

        Parameters
        ----------
        folderpath
            absolute folder path to the directory we want to monitor
        is_recursive
            if True, search subdirectories as well
        """

        folderpath = os.path.normpath(folderpath)
        if os.path.isdir(folderpath):
            self.stop_folder_monitor(folderpath)
            # you have to recreate the DirectoryMonitor object, there is no restart
            self.monitors[folderpath] = monitor.DirectoryMonitor(folderpath, is_recursive)
            self.monitors[folderpath].bind_to(self.handle_monitor_event)
            self.monitors[folderpath].start()
            print('now monitoring {}'.format(folderpath))
        else:
            print('Unable to start monitoring, path provided is not a valid directory: {}'.format(folderpath))

    def stop_folder_monitor(self, folderpath):
        """
        Stop and remove the monitor object for the given folderpath

        Parameters
        ----------
        folderpath
            absolute folder path to the directory we want to monitor
        """

        folderpath = os.path.normpath(folderpath)
        if folderpath in self.monitors:
            self.monitors[folderpath].stop()
            self.monitors.pop(folderpath)
            print('no longer monitoring {}'.format(folderpath))

    def handle_monitor_event(self, filepath, file_event):
        """
        Direct the events from the directory monitoring object to either adding a new file or removing an existing file

        Parameters
        ----------
        filepath
            absolute file path to the file that came from the directory monitoring object
        file_event
            one of 'created', 'deleted'
        """

        if file_event == 'created':
            self.add_file(filepath)
        else:
            self.remove_file(filepath)

    def add_file(self, infile: str):
        """
        Starting point for FqprIntel, adding a file to the class which then adds it to one of the intel objects.

        We check to see if the file is in one of the approved file extension lists, or check in a more comprehensive way
        (see sbet.is_sbet) before adding.

        Parameters
        ----------
        infile
            full file path to the new file

        Returns
        -------
        str
            the updated_type that matches this file
        OrderedDict
            attributes associated with one of the gather_xxxx functions
        """

        infile = os.path.normpath(infile)
        fileext = os.path.splitext(infile)[1]
        updated_type = ''
        new_data = None
        if fileext in supported_mbes:
            new_data = gather_multibeam_info(infile)
            new_data['unique_id'] = self.unique_id
            self.unique_id += 1
            added = self.multibeam_intel.add_dict(new_data)
            if added:
                updated_type = 'multibeam'
        elif fileext in supported_svp:
            new_data = gather_svp_info(infile)
            new_data['unique_id'] = self.unique_id
            self.unique_id += 1
            added = self.svp_intel.add_dict(new_data)
            if added:
                updated_type = 'svp'
        elif fileext in supported_sbet:
            if sbet.is_sbet(infile):
                new_data = gather_navfile_info(infile)
                new_data['unique_id'] = self.unique_id
                self.unique_id += 1
                added = self.nav_intel.add_dict(new_data)
                self.match_navigation_files()
                if added:
                    updated_type = 'navigation'
            elif sbet.is_smrmsg(infile):
                new_data = gather_naverrorfile_info(infile)
                new_data['unique_id'] = self.unique_id
                self.unique_id += 1
                added = self.naverror_intel.add_dict(new_data)
                self.match_navigation_files()
                if added:
                    updated_type = 'naverror'
        elif fileext in supported_export_log:
            new_data = gather_exportlogfile_info(infile)
            if new_data is not None:
                new_data['unique_id'] = self.unique_id
                self.unique_id += 1
                added = self.navlog_intel.add_dict(new_data)
                self.match_navigation_files()
                if added:
                    updated_type = 'navlog'
        else:
            self.print_msg('File is not of a supported type: {}'.format(infile), logging.ERROR)
        return updated_type, new_data

    def remove_file(self, infile: str):
        """
        Remove this file from any intelligence modules

        Parameters
        ----------
        infile
            full file path to the file

        Returns
        -------
        str
            the updated_type that matches this file
        int
            unique id as integer for the file removed
        """

        updated_type = ''
        uid = None
        if infile in self.multibeam_intel.file_paths:
            uid = self.multibeam_intel.remove_file(infile)
            updated_type = 'multibeam'
        elif infile in self.svp_intel.file_paths:
            uid = self.svp_intel.remove_file(infile)
            updated_type = 'svp'
        elif infile in self.nav_intel.file_paths:
            uid = self.nav_intel.remove_file(infile)
            updated_type = 'navigation'
        elif infile in self.naverror_intel.file_paths:
            uid = self.naverror_intel.remove_file(infile)
            updated_type = 'naverror'
        elif infile in self.navlog_intel.file_paths:
            uid = self.navlog_intel.remove_file(infile)
            updated_type = 'navlog'
        return updated_type, uid

    def match_navigation_files(self):
        pass


class IntelModule(LoggerClass):
    """
    Base module for the intelligence modules.  Intelligence modules are classes that contain a specific kind of file,
    and have methods that are useful for that file type to determine which files go together and which files should be
    processed in to which date/time/sonarmodel container.

    Contains the attribution that all other extended modules share as well as the basic adding/removing functionality.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.added_data = []  # each added OrderedDict are appended to self.added_data
        self.file_paths = []  # list of all added filepaths ['C:\\data_dir\\fil.kmall']
        self.file_path = {}  # {'fil.kmall': 'C:\\data_dir\\fil.kmall'}
        self.file_name = {}  # {'C:\\data_dir\\fil.kmall': 'fil.kmall'}
        self.unique_id_reverse = {}  # {0: 'C:\\data_dir\\fil.kmall'}
        self.type = {}  # {'C:\\data_dir\\fil.kmall': 'kongsberg_kmall'}
        self.time_added = {}  # {'C:\\data_dir\\fil.kmall': datetime.datetime(2020, 11, 19, 15, 35, 2, 44724, tzinfo=datetime.timezone.utc)}
        self.last_modified_time_utc = {}  # {'C:\\data_dir\\fil.kmall': datetime.datetime(2020, 9, 10, 13, 16, 54, 96522, tzinfo=datetime.timezone.utc)}
        self.created_time_utc = {}  # {'C:\\data_dir\\fil.kmall': datetime.datetime(2020, 11, 19, 15, 35, 1, 899690, tzinfo=datetime.timezone.utc)}
        self.file_size_kb = {}  # {'C:\\data_dir\\fil.kmall': 33106.004}
        self.unique_id = {}  # {'C:\\data_dir\\fil.kmall': 0}

    def _check_files_same_size(self, attributes: OrderedDict):
        """
        Take in the new attribution for a potential newly added file (see add_dict) and see if this attribution matches
        an existing file by file name and file size.  We assume that if those match, this attribution is a duplicate.

        Have to check file names to ensure that we catch files that are added once from one location and are then moved
        to another location (the file path will change but the file name and size will be the same)

        Parameters
        ----------
        attributes
            attributes for the incoming file, see one of the gather_xxxx functions outside of the class

        Returns
        -------
        bool
            if True, this is a duplicate set of attribution
        """

        new_file_name = os.path.split(attributes['file_path'])[1]
        new_file_size = attributes['file_size_kb']
        if new_file_size in list(self.file_size_kb.values()):
            old_file_paths = list(self.file_size_kb.keys())
            for fpth in old_file_paths:
                if self.file_size_kb[fpth] == new_file_size and os.path.split(fpth)[1] == new_file_name:
                    return True
        return False

    def add_dict(self, attributes: OrderedDict):
        """
        Add an incoming dictionary to the intelligence module, if it is not in there already and is a valid set

        Parameters
        ----------
        attributes
            attributes for the incoming file, see one of the gather_xxxx functions outside of the class
        """

        if 'file_path' in list(attributes.keys()):
            norm_filepath = os.path.normpath(attributes['file_path'])
            filename = os.path.split(norm_filepath)[1]
            if norm_filepath not in self.file_paths and not self._check_files_same_size(attributes):
                self.added_data.append(attributes)
                self.file_paths.append(norm_filepath)
                self.file_path[filename] = norm_filepath
                self.file_name[norm_filepath] = filename
                self.unique_id_reverse[attributes['unique_id']] = norm_filepath
                attributes['file_name'] = filename
                for ky, val in attributes.items():
                    if ky != 'file_path':  # we store file paths in a separate attribute
                        try:
                            attr = self.__getattribute__(ky)
                            attr[norm_filepath] = val
                            self.__setattr__(ky, attr)
                        except AttributeError:  # attributes key not in this class
                            self.print_msg('{} is not an attribute of this module'.format(ky), logging.WARNING)
                self.print_msg('File {} added as {}'.format(norm_filepath, attributes['type']))
                return True
            else:
                self.print_msg('Input data dictionary describes a file that already exists in Kluster Intelligence: {}'.format(attributes['file_path']), logging.ERROR)
                return False
        else:
            raise ValueError('Input data dictionary does not have a file_path key, found {}'.format(list(attributes.keys())))

    def remove_file(self, filepath: str):
        """
        remove the provided filepath from the intelligence module, returns the unique id for this file so that we can
        update the GUI if necessary

        Parameters
        ----------
        filepath
            absolute file path for the file to remove

        Returns
        -------
        int
            unique id for the file we removed, FqprIntel is generating the unique id
        """

        norm_filepath = os.path.normpath(filepath)
        if norm_filepath in self.file_paths:
            uid = self.unique_id[norm_filepath]
            self.file_paths.remove(filepath)
            for ky in vars(self):
                attr = self.__getattribute__(ky)
                if isinstance(attr, dict):
                    if filepath in list(attr.keys()):
                        attr.pop(filepath)
                        self.__setattr__(ky, attr)
            self.print_msg('File {} removed'.format(filepath), logging.INFO)
            return uid
        else:
            self.print_msg('File {} is not in this module'.format(filepath))
            return '', ''


class MultibeamModule(IntelModule):
    """
    IntelModule specific for multibeam files, with multibeam specific attribution
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.data_start_time_utc = {}
        self.data_end_time_utc = {}
        self.primary_serial_number = {}
        self.secondary_serial_number = {}
        self.sonar_model_number = {}


class NavigationModule(IntelModule):
    """
    IntelModule specific for post processed navigation (SBET) files, with SBET specific attribution
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.weekly_seconds_start = {}
        self.weekly_seconds_end = {}


class NavErrorModule(IntelModule):
    """
    IntelModule specific for post processed nav error (SMRMSG) files, with SMRMSG specific attribution
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.weekly_seconds_start = {}
        self.weekly_seconds_end = {}
        self.matching_sbet = {}


class NavExportLogModule(IntelModule):
    """
    IntelModule specific for sbet export log files, with log file specific attribution
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.mission_date = {}
        self.datum = {}
        self.ellipsoid = {}
        self.matching_sbet = {}
        self.input_sbet_file = {}
        self.exported_sbet_file = {}
        self.sample_rate_hertz = {}


class SvpModule(IntelModule):
    """
    IntelModule specific for caris svp files, with svp file specific attribution
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.profiles = {}
        self.number_of_profiles = {}
        self.number_of_layers = {}
        self.julian_day = {}
        self.time_utc = {}
        self.latitude = {}
        self.longitude = {}
        self.source_epsg = {}
        self.utm_zone = {}
        self.utm_hemisphere = {}


def gather_basic_file_info(filename: str):
    """
    Build out the basic file metadata that can be gathered from any file on the file system.

    Parameters
    ----------
    filename
        full file path to a file

    Returns
    -------
    dict
        basic file attributes as dict
    """

    if not os.path.exists(filename):
        raise EnvironmentError('{} does not exist'.format(filename))
    elif not os.path.isfile(filename):
        raise EnvironmentError('{} is not a file'.format(filename))
    stat_blob = os.stat(filename)
    last_modified_time = datetime.fromtimestamp(stat_blob.st_mtime, tz=timezone.utc)
    created_time = datetime.fromtimestamp(stat_blob.st_ctime, tz=timezone.utc)
    filesize = np.around(stat_blob.st_size / 1024, 3)  # size in kB
    time_added = datetime.now(tz=timezone.utc)
    return {'file_path': filename, 'last_modified_time_utc': last_modified_time,
            'created_time_utc': created_time, 'file_size_kb': filesize, 'time_added': time_added}


def gather_multibeam_info(multibeam_file: str):
    """
    fast method to read info from a multibeam file without reading the whole file.  Supports .all and .kmall files

    the secondary serial number will be zero for all systems except dual head.  Dual head records the secondary head
    serial number (starboard head) as the secondary serial number.  For non dual head systems, the primary serial
    number is all that is needed.

    Parameters
    ----------
    multibeam_file
        file path to a multibeam file

    Returns
    -------
    OrderedDict
        dictionary object with all metadata related to the provided multibeam file
    """

    basic = gather_basic_file_info(multibeam_file)
    fileext = os.path.splitext(multibeam_file)[1]
    if fileext == '.all':
        mtype = 'kongsberg_all'
        aread = par3.AllRead(multibeam_file)
        start_end = aread.fast_read_start_end_time()
        serialnums = aread.fast_read_serial_number()
    elif fileext == '.kmall':
        mtype = 'kongsberg_kmall'
        km = kmall.kmall(multibeam_file)
        start_end = km.fast_read_start_end_time()
        serialnums = km.fast_read_serial_number()
    else:
        raise IOError('File ({}) is not a valid multibeam file'.format(multibeam_file))
    info_data = OrderedDict({'file_path': basic['file_path'], 'type': mtype,
                             'data_start_time_utc': datetime.fromtimestamp(start_end[0], tz=timezone.utc),
                             'data_end_time_utc': datetime.fromtimestamp(start_end[1], tz=timezone.utc),
                             'primary_serial_number': serialnums[0],
                             'secondary_serial_number': serialnums[1], 'sonar_model_number': serialnums[2],
                             'last_modified_time_utc': basic['last_modified_time_utc'],
                             'created_time_utc': basic['created_time_utc'], 'file_size_kb': basic['file_size_kb'],
                             'time_added': basic['time_added']})
    return info_data


def gather_navfile_info(ppnav_file: str):
    """
    Gather the file information from the provided post_processed_navigation file

    Currently only supports POSPac sbet files

    Parameters
    ----------
    ppnav_file
        full file path to the post processed navigation file

    Returns
    -------
    OrderedDict
        dictionary object with all metadata related to the provided processed navigation file
    """

    basic = gather_basic_file_info(ppnav_file)
    tms = sbet.sbet_fast_read_start_end_time(ppnav_file)
    if tms is None:
        raise IOError('File ({}) is not a valid postprocessed navigation file'.format(ppnav_file))
    mtype = 'POSPac sbet'
    info_data = OrderedDict({'file_path': basic['file_path'], 'type': mtype,
                             'weekly_seconds_start': tms[0], 'weekly_seconds_end': tms[1],
                             'last_modified_time_utc': basic['last_modified_time_utc'],
                             'created_time_utc': basic['created_time_utc'], 'file_size_kb': basic['file_size_kb'],
                             'time_added': basic['time_added']})
    return info_data


def gather_naverrorfile_info(pperror_file: str):
    """
    Gather the file information from the provided post_processed_navigation file

    Currently only supports POSPac smrmsg files

    Parameters
    ----------
    pperror_file
        full file path to the post processed navigation file

    Returns
    -------
    OrderedDict
        dictionary object with all metadata related to the provided processed navigation file
    """

    basic = gather_basic_file_info(pperror_file)
    tms = sbet.smrmsg_fast_read_start_end_time(pperror_file)
    if tms is None:
        raise IOError('File ({}) is not a valid postprocessed error file'.format(pperror_file))
    mtype = 'POSPac smrmsg'
    info_data = OrderedDict({'file_path': basic['file_path'], 'type': mtype,
                             'weekly_seconds_start': tms[0], 'weekly_seconds_end': tms[1],
                             'last_modified_time_utc': basic['last_modified_time_utc'],
                             'created_time_utc': basic['created_time_utc'], 'file_size_kb': basic['file_size_kb'],
                             'time_added': basic['time_added']})
    return info_data


def gather_exportlogfile_info(exportlog_file: str):
    """
    Gather the file information from the provided navigation log file

    Currently only supports POSPac export log files

    Parameters
    ----------
    exportlog_file
        full file path to the nav log file

    Returns
    -------
    OrderedDict
        dictionary object with all metadata related to the provided processed navigation file
    """

    basic = gather_basic_file_info(exportlog_file)
    loginfo = sbet.get_export_info_from_log(exportlog_file)
    if loginfo is not None:
        info_data = OrderedDict({'file_path': basic['file_path'], 'input_sbet_file': loginfo['input_sbet_file'],
                                 'exported_sbet_file': loginfo['exported_sbet_file'],
                                 'sample_rate_hertz': loginfo['sample_rate_hertz'], 'type': 'sbet_export_log',
                                 'mission_date': loginfo['mission_date'], 'datum': loginfo['datum'],
                                 'ellipsoid': loginfo['ellipsoid'], 'last_modified_time_utc': basic['last_modified_time_utc'],
                                 'created_time_utc': basic['created_time_utc'], 'file_size_kb': basic['file_size_kb'],
                                 'time_added': basic['time_added']})
    else:
        return None
    return info_data


def gather_svp_info(svp_file: str):
    """
    read the provided svp file and generate a dictionary of attributes associated with the file

    Parameters
    ----------
    svp_file
        full filepath to a svp file

    Returns
    -------
    OrderedDict
        dictionary object with all metadata related to the provided svp file
    """

    basic = gather_basic_file_info(svp_file)
    svp_object = svp.CarisSvp(svp_file)
    svp_dict = svp_object.return_dict()
    formatted_time_utc = [datetime.fromtimestamp(tm, tz=timezone.utc) for tm in svp_dict['svp_time_utc']]
    info_data = OrderedDict({'file_path': basic['file_path'], 'type': 'caris_svp', 'profiles': svp_dict['profiles'],
                             'number_of_profiles': svp_dict['number_of_profiles'],
                             'number_of_layers': svp_dict['number_of_layers'],
                             'julian_day': svp_dict['svp_julian_day'], 'time_utc': formatted_time_utc,
                             'latitude': svp_dict['latitude'], 'longitude': svp_dict['longitude'],
                             'source_epsg': svp_dict['source_epsg'], 'utm_zone': svp_dict['utm_zone'],
                             'utm_hemisphere': svp_dict['utm_hemisphere'],
                             'last_modified_time_utc': basic['last_modified_time_utc'],
                             'created_time_utc': basic['created_time_utc'], 'file_size_kb': basic['file_size_kb'],
                             'time_added': basic['time_added']})
    return info_data
