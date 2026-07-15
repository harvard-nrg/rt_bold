import os
import sys
import pdb
import time
import math
import json
import shutil
import psutil
import logging
import datetime
import threading
import numpy as np
from pubsub import pub
from pathlib import Path
from scanbuddy.proc.snr import SNR
from sortedcontainers import SortedDict
from scanbuddy.proc.fdata import ExtractFdata
from scanbuddy.proc.converter import Converter

logger = logging.getLogger(__name__)

class BoldProcessor:
    def __init__(self, config, debug_display=False):
        self.reset()
        self._config = config
        self._debug_display = self._config.find_one('$.app.debug_display', default=debug_display)
        pub.subscribe(self.reset, 'reset')
        pub.subscribe(self.listener, 'bold-proc')

    def reset(self):
        self._instances = SortedDict()
        self._snr_instances = SortedDict()
        self.make_arrays_zero('reset')
        pub.sendMessage('plot_snr', snr_metric=str(0.0))
        logger.debug('received message to reset')

    def listener(self, ds, path, modality):
        logger.info('inside of the bold-proc topic')
        self._modality = modality
        key = int(ds.InstanceNumber)

        ### Check if the data is multi-echo
        is_multi_echo, is_TE2 = self.check_echo(ds)
        if is_multi_echo is True and is_TE2 is False:
            # if it is multi-echo but wrong echo-time, delete file and return. do not process
            os.remove(path)
            return
        if is_multi_echo:
            key = self.get_new_key(key)
            logger.info(f'new multi-echo key: {key}')

        # check if the dicom is a 'noise' volume
        if self.check_noise(ds, key): return
        self._instances[key] = {
            'path': path,
            'volreg': None,
            'nii_path': None
        }
        self._snr_instances[key] = {
            'path': path,
            'fdata_array': None,
            'mask_threshold': None,
            'mask': None
        }
        logger.debug('current state of instances')
        logger.debug(json.dumps(self._instances, default=list, indent=2))

        logger.info('instantiating and running converter')
        converter = Converter()
        converter.run(self._instances[key], modality, key)

        tasks = self.check_volreg(key)
        logger.debug('publishing message to volreg topic with the following tasks')
        logger.debug(json.dumps(tasks, indent=2))

        pub.sendMessage('volreg', tasks=tasks, modality=modality)
        logger.debug(f'publishing message to params topic')
        pub.sendMessage('params', ds=ds, modality=modality)

        logger.debug(f'after volreg')
        logger.debug(json.dumps(self._instances, indent=2))
        project = ds.get('StudyDescription', '[STUDY]')
        session = ds.get('PatientID', '[PATIENT]')
        scandesc = ds.get('SeriesDescription', '[SERIES]')
        scannum = ds.get('SeriesNumber', '[NUMBER]')
        subtitle_string = f'{project} • {session} • {scandesc} • {scannum}'
        if key < 6:
            logger.info(f'Scan info: Project: {project}, Session: {session}, Series: {scandesc}, Scan Number: {scannum}, Date & Time: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')        
        num_vols = ds[(0x0020, 0x0105)].value
        if self._debug_display:
            pub.sendMessage('plot', instances=self._instances, subtitle_string=subtitle_string)
        elif num_vols == key:
            pub.sendMessage('plot', instances=self._instances, subtitle_string=subtitle_string)

        fdata_tasks = self.check_fdata(key)

        fdata_extraction = ExtractFdata()
        nii_path = self._instances[key]['nii_path']
        fdata_extraction.do(nii_path, fdata_tasks)

        snr_thread = threading.Thread(
            target=pub.sendMessage,
            args=('snr',),
            kwargs={'key': key, 'snr_instances': self._snr_instances, 'ds': ds}
        )
        snr_thread.start()

    def check_volreg(self, key):
        tasks = list()
        current = self._instances[key]

        i = self._instances.bisect_left(key)

        try:
            left_index = max(0, i - 1)
            left = self._instances.values()[left_index]
            logger.debug(f'to the left of {current["path"]} is {left["path"]}')
            tasks.append((current, left))
        except IndexError:
            pass

        try:
            right_index = i + 1
            right = self._instances.values()[right_index]
            logger.debug(f'to the right of {current["path"]} is {right["path"]}')
            tasks.append((right, current))
        except IndexError:
            pass

        return tasks

    def get_new_key(self, instance_number):
        return ((instance_number - 2) // 4) + 1

    def check_fdata(self, key):
        tasks = list()

        current_idx = self._snr_instances.bisect_left(key)

        try:
            value = self._snr_instances.values()[current_idx]
            tasks.append(value)
        except IndexError:
            pass

        return tasks

    def make_arrays_zero(self, moment='end'):
        if moment == 'end':
            logger.info('freeing up RAM from snr arrays')
        else:
            logger.debug('making sure snr arrays are deallocated')
        self._fdata_array = None
        self._slice_intensity_means = None

    def check_noise(self, ds, vol_number):
        '''
        Checks the dicom header for 'NOISE' in the Siemens
        private header. If the string 'NOISE' is present,
        do not process that dicom.
        '''
        sequence = ds[(0x5200, 0x9230)][0]

        try:
            siemens_private_tag = sequence[(0x0021, 0x11fe)][0]
            scan_string = str(siemens_private_tag[(0x0021, 0x1175)].value)
        except KeyError:
            siemens_private_tag = sequence[(0x0021, 0x10fe)][0]
            scan_string = str(siemens_private_tag[(0x0021, 0x1075)].value)
            
        if 'NOISE' in scan_string:
            logger.info(f'Volume {vol_number} found to be a noise volume. Not processing.')
            return True
        return False


    def check_echo(self, ds):
        '''
        This method will check for the string 'TE' in 
        the siemens private data tag. If 'TE' exists in that
        tag it means the scan is multi-echo. If it is multi-echo
        we are only interested in the second echo or 'TE2'
        Return False if 'TE2' is not found. Return True if 
        'TE2' is found or no reference to 'TE' is found
        '''
        sequence = ds[(0x5200, 0x9230)][0]
        try:
            siemens_private_tag = sequence[(0x0021, 0x11fe)][0]
            scan_string = str(siemens_private_tag[(0x0021, 0x1175)].value)
        except KeyError:
            siemens_private_tag = sequence[(0x0021, 0x10fe)][0]
            scan_string = str(siemens_private_tag[(0x0021, 0x1075)].value)
        if 'TE2' in scan_string:
            logger.info('multi-echo scan detected')
            logger.info(f'using 2nd echo time: {self.get_echo_time(ds)}')
            return True, True
        elif 'TE' not in scan_string:
            logger.info('single echo scan detected')
            return False, False
        else:
            logger.info('multi-echo scan found, wrong echo time, deleting file and moving on')
            return True, False

    def get_echo_time(self, ds):
        sequence = ds[(0x5200, 0x9230)][0]
        echo_sequence_item = sequence[(0x0018, 0x9114)][0]
        return echo_sequence_item[(0x0018, 0x9082)].value

