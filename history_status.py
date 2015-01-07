#!/usr/bin/python
__author__ = 'eablck'

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.client import ConnectionError
from ConfigParser import SafeConfigParser
from requests.packages import urllib3
import sys
import os
import json


class HistoryInfo():
    history = {}
    status = {}

    def __init__(self, hist, hist_status):
        self.history = hist
        self.status = hist_status


def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api


def get_total_datasets(history):
    total_datasets = 0
    if history is not None:
        total_datasets = sum(history['state_details'].itervalues())
    return total_datasets


def has_failures(history):
    has_failures = False
    total_falures = 0
    if history is not None:
        has_failures += history['state_details']['error']
        has_failures += history['state_details']['failed_metadata']
        has_failures += history['state_details']['']


##########################################################################
# Main Logic Starts Here
##########################################################################

# Disable Warnings. Without this a warning such as the following is generated:
# /Library/Python/2.7/site-packages/requests/packages/urllib3/connectionpool.py:734:
# InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
# See: https://urllib3.readthedocs.org/en/latest/security.html
# At some point we might want to allow this warning instead, or fix up the
# cause.
urllib3.disable_warnings()

parser = SafeConfigParser()

if len(sys.argv) == 2:
    if sys.argv[1].endswith('.ini'):
        parser.read(sys.argv[1])
    else:
        print "You passed %s I need a .ini file" % (sys.argv[1],)
        sys.exit(1)
else:
    parser.read('configuration.ini')

# Get the configuration parmaters
api_key = get_api_key(parser.get('Globals', 'api_file'))
galaxy_host = parser.get('Globals', 'galaxy_host')
output_dir = parser.get('Globals', 'output_dir')

# Get a connection to Galaxy and a client to interact with Histories
galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
historyClient = HistoryClient(galaxyInstance)

print ""
print "Locating Histories To Query Status ... "

# Open up the serialized History file that was saved for the batch run
all_history_json_filename = os.path.join(output_dir, "All_Histories.json")
if not os.path.isfile(all_history_json_filename):
    print ""
    print "ERROR: Could not find All_Histories.json file in " + output_dir + "."
    print "       Did you use the workflow_runner.py to launch automated Galaxy NGS analysis? " + \
        "Check your configuration.ini file to make sure it has configued the correct output directory to inspect. "
    print ""
    print "       ** Note: All_Histories.json is a file automatically generated when the workflow_runner.py script is invoked to launch workflows."
    print ""
    sys.exit(2)

all_history_json = open(all_history_json_filename, "rb")
histories = json.load(all_history_json)

print "Loading List of Histories From : " + all_history_json.name
print ""

all_successful = []
all_running = []
all_failed = []
all_except = []

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
        h_status = historyClient.get_status(h['id'])
    except:
        all_except.append(h)
        continue

    history_info = HistoryInfo(h, h_status)

    if h_status['state'] == 'ok':
        all_successful.append(history_info)
    elif h_status['state'] == 'running':
        all_running.append(history_info)
    else:
        all_failed.append(history_info)

num_histories = len(histories)
print "TOTAL Number of Histories: " + str(num_histories) + " ( RUNNING = " + str(len(all_running)) + ", FAILED? = " + str(len(all_failed)) + ", NOT_FOUND = " + str(len(all_except)) + ", COMPLETED (OK) = " + str(len(all_successful)) + " )"



# print h['name'] + " => FINISHED.  ALL Steps completed SUCCESSFULLY."


sys.exit()
