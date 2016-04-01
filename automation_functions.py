"""Functions shared between history_utils and workflow_runner --retry_failed."""

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.client import ConnectionError
from bioblend.galaxy.ftpfiles import FTPFilesClient
from requests.packages import urllib3
from ConfigParser import SafeConfigParser
from ConfigParser import NoOptionError
from multiprocessing import Pool
from functools import partial
import sys
import os
import fnmatch
import os.path
import re
import json
import argparse
import textwrap
import tarfile
import logging
import collections

class HistoryInfo():
    history = {}
    status = {}

    def __init__(self, hist, hist_status):
        self.history = hist
        self.status = hist_status

def read_all_histories(results_base_path, logger):
    '''
    Validate and parse AllHistories.json file. Return python representation of file.
    '''
    all_history_json_filename = os.path.join(results_base_path, "All_Histories.json")
    if not os.path.isfile(all_history_json_filename):
        logger.error("")
        logger.error("ERROR: Could not find All_Histories.json file in %s.", results_base_path)
        logger.error("\tDid you use the workflow_runner.py to launch automated Galaxy NGS analysis? " + \
              "Check your configuration.ini file to make sure it has configued the correct output directory to inspect. ")
        logger.error("")
        logger.error("\t** Note: All_Histories.json is a file automatically generated when the workflow_runner.py script is invoked to launch workflows.")
        logger.error("")
        sys.exit(30)

    all_history_json = open(all_history_json_filename, "rb")
    try:
        histories = json.load(all_history_json)
    except:
        logger.error("ERROR: Could not load any history records from All_Histories.json file in %s.", args.all_history_dir)
        logger.error("\tDid you use the workflow_runner.py to launch the automated Galaxy NGS analysis? Possibly the workflow_runner has not yet completed?")
        logger.error("")
        sys.exit(31)
    logger.info("Loading List of Histories From : %s", all_history_json.name)
    return histories


def get_history_status(histories, history_client, logger):
    '''
    Parse history info to create list of histories that are successful, running, failed, etc.
    '''
    all_successful = []
    all_running = []
    all_failed = []
    all_except = []
    all_waiting = []
    upload_history = None

    for h in histories:
        # Example h_status object structures
        # {'state_details': {u'discarded': 0,
        #                    u'ok': 9,
        #                    u'failed_metadata': 0,
        #                    u'upload': 0,
        #                    u'paused': 0,
        #                    u'running': 0,
        #                    u'setting_metadata': 0,
        #                    u'error': 0,
        #                    u'new': 0,
        #                    u'queued': 0,
        #                    u'empty': 0},
        #  'state': u'ok',
        #  'percent_complete': 100}
        # {'state_details': {u'discarded': 0, u'ok': 1, u'failed_metadata': 0, u'upload': 0, u'paused': 0, u'running': 1, u'setting_metadata': 0, u'error': 0, u'new': 0, u'queued': 30, u'empty': 0}, 'state': u'running', 'percent_complete': 3}
        # {'state_details': {u'discarded': 0, u'ok': 1, u'failed_metadata': 0, u'upload': 0, u'paused': 0, u'running': 0, u'setting_metadata': 0, u'error': 0, u'new': 0, u'queued': 31, u'empty': 0}, 'state': u'queued', 'percent_complete': 3}
        # {'state_details': {u'discarded': 0, u'ok': 9, u'failed_metadata': 0, u'upload': 0, u'paused': 0, u'running': 0, u'setting_metadata': 0, u'error': 0, u'new': 0, u'queued': 0, u'empty': 0}, 'state': u'ok', 'percent_complete': 100}
        # {'state_details': {u'discarded': 0, u'ok': 1, u'failed_metadata': 0, u'upload': 0, u'paused': 0, u'running': 1, u'setting_metadata': 0, u'error': 0, u'new': 0, u'queued': 30, u'empty': 0}, 'state': u'running', 'percent_complete': 3}
        # {'state_details': {u'discarded': 0, u'ok': 1, u'failed_metadata': 0, u'upload': 0, u'paused': 0, u'running': 0, u'setting_metadata': 0, u'error': 0, u'new': 0, u'queued': 31, u'empty': 0}, 'state': u'queued', 'percent_complete': 3}
        try:
            h_status = history_client.get_status(h['id'])
        except Exception as e:
            logger.exception(e)
            all_except.append(h)
            continue

        history_info = HistoryInfo(h, h_status)

        if str(h['upload_history']).lower() in ('true', 'yes'):
            upload_history = history_info
        else:
            if h_status['state'] == 'ok':
                all_successful.append(history_info)
            elif h_status['state'] == 'running':
                all_running.append(history_info)
            elif h_status['state'] == 'queued':
                all_waiting.append(history_info)
            else:
                all_failed.append(history_info)
    return all_successful, all_running, all_failed, all_except, all_waiting, upload_history
