import os
import glob
import time
import shutil
import logging
import pydicom
from pubsub import pub
from retry import retry
from pathlib import Path
from datetime import datetime
from pydicom.errors import InvalidDicomError
from watchdog.observers.polling import PollingObserver
from watchdog.events import PatternMatchingEventHandler

logger = logging.getLogger(__name__)

class DicomWatcher:
    def __init__(self, directory, modality):
        self._directory = directory
        self._modality = modality
        self._inode = None
        self._observer = PollingObserver(timeout=.01)
        self._observer.schedule(
            DicomHandler(modality, ignore_directories=True),
            directory
        )

    def start(self):
        logger.info(f'starting dicom watcher on {self._directory}')
        logger.debug(f'hitting reset')
        pub.sendMessage('reset')
        self._inode = os.stat(self._directory).st_ino
        self._observer.start()

    def join(self):
        self._observer.join()

    def stop(self):
        logger.info(f'stopping dicom watcher on {self._directory}')
        self._observer.stop()
        # check if this the same directory
        inode = os.stat(self._directory).st_ino
        if inode == self._inode:
            try:
                logger.info(f'removing {self._directory}')
                shutil.rmtree(self._directory)
                logger.info(f'successfully removed {self._directory}')
            except FileNotFoundError:
                logger.info(f'{self._directory} does not exist, moving on')
                pass
        else:
            logger.info(f'not going to remove new directory with the same name {self._directory}')

class DicomHandler(PatternMatchingEventHandler):
    def __init__(self, modality, **kwargs):
        super().__init__(**kwargs)
        self._modality = modality

    def on_created(self, event):
        path = Path(event.src_path)
        try:
            self.file_size = -1
            if not path.exists():
                logger.info(f'file {path} no longer exists')
                return
            ds = self.read_dicom(path)
            cic = ds.get((0x0008, 0x9208))
            if cic and cic.value == 'PHASE':
                logger.info(f'deleting phase file {path}')
                path.unlink()
                return
            self.check_series(ds, path)
            path = self.construct_path(path, ds)
            logger.info(f'publishing message to parent-proc topic with ds={path}')
            pub.sendMessage('parent-proc', ds=ds, path=path, modality=self._modality)
        except InvalidDicomError as e:
            logger.info(f'not a dicom file {path}')
        except FileNotFoundError as e:
            logger.warning(f'file not found: {e}')
            pass
        except Exception as e:
            logger.info(f'An unexpected error occurred: {e}')
            logger.exception(e, exc_info=True)


    @retry((IOError, InvalidDicomError), delay=.01, backoff=1.5, max_delay=1.5, tries=15)
    def read_dicom(self, dicom):
        """
        Checking the file size is necessary when mounted over a samba share.
        the scanner writes dicoms as they come (even if they are incomplete)
        This method ensures the entire dicom is written before being processed
        """
        new_file_size = dicom.stat().st_size
        if self.file_size != new_file_size:
            logger.info(f'file size was {self.file_size}')
            self.file_size = new_file_size
            logger.info(f'file size is now {self.file_size}')
            raise IOError
        return pydicom.dcmread(dicom, stop_before_pixels=False)

    def check_series(self, ds, old_path):
        series_number = ds.get('SeriesNumber', 'UNKNOWN SERIES')
        instance_number = ds.get('InstanceNumber', 'UNKNOWN INSTANCE')
        series_uid = ds.get('SeriesInstanceUID', 'UNKNOWN SERIES UID')
        if not hasattr(self, 'first_dcm_series'):
            logger.info(f'found first series instance uid {series_uid} (series={series_number}, instance={instance_number})')
            self.first_dcm_series = ds.SeriesInstanceUID
            self.first_dcm_study = ds.StudyInstanceUID
            return

        if self.first_dcm_series != series_uid:
            logger.info(f'found new series instance uid {series_uid} (series={series_number}, instance={instance_number})')
            self.trigger_reset(ds, old_path)
            self.first_dcm_series = ds.SeriesInstanceUID
            self.first_dcm_study = ds.StudyInstanceUID

    def trigger_reset(self, ds, old_path):
        study_name = self.first_dcm_study
        series_name = self.first_dcm_series
        dicom_parent = old_path.parent
        new_path_no_dicom = Path.joinpath(dicom_parent, study_name)#, series_name)
        logger.debug(f'path to remove: {new_path_no_dicom}')
        shutil.rmtree(new_path_no_dicom)
        self.clean_parent(dicom_parent)
        logger.debug('making it out of clean_parent method')
        pub.sendMessage('reset')

    def clean_parent(self, path):
        logger.debug(f'cleaning target dir: {path}')
        for file in glob.glob(f'{path}/*.dcm'):
            logger.debug(f'removing {file}')
            os.remove(file)

    def construct_path(self, old_path, ds):
        study_name = ds.StudyInstanceUID
        series_name = ds.SeriesInstanceUID
        dicom_filename = old_path.name
        dicom_parent = old_path.parent

        new_path_no_dicom = Path.joinpath(dicom_parent, study_name, series_name)

        logger.info(f'moving file from {old_path} to {new_path_no_dicom}')

        os.makedirs(new_path_no_dicom, exist_ok=True)

        try:
            shutil.move(old_path, new_path_no_dicom)
        except shutil.Error:
            pass
        
        new_path_with_dicom = Path.joinpath(dicom_parent, study_name, series_name, dicom_filename)

        return str(new_path_with_dicom)
