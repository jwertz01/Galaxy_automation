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
import argparse
import textwrap
import subprocess


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


def get_total_datasets(history_status):
    total_datasets = 0
    if history_status is not None:
        total_datasets = sum(history_status['state_details'].itervalues())
    return total_datasets


def has_failures(history):
    has_failures = False
    total_falures = 0
    if history is not None:
        has_failures += history['state_details']['error']
        has_failures += history['state_details']['failed_metadata']
        has_failures += history['state_details']['']

    if total_falures > 0:
        has_failures = True 

    return has_failures


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

arg_parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     usage='%(prog)s all_history_dir {check_status, download, delete} [OPTIONS]',
                                     description='Helper utilities to assist with histories generated by automated NGS Galaxy analyses initiated via the Galaxy Automation workflow_runner.py script',
                                     epilog=textwrap.dedent('''\
                                        Examples:

                                        # Download histories for batch23.  They will be downloaded and extracted into the same batch23 directory that containst he All_Histories.json file.
                                        #        Histories that are successfully downloaded will be deleted (-d)
                                        history_utils.py /Users/annblack/GalaxyAutomation/fastqs/batch23 download -d -i configuration.eablck.ini

                                        # Download histories for batch23, but place results into new directory (will be created if does not exist)
                                        #        Histories that are successfully downloaded will be deleted (-d)
                                        history_utils.py /Users/annblack/GalaxyAutomation/fastqs/batch23 download -d -o /Users/annblack/GalaxyAutomation/fastqs/batch23/results -ini configuration.eablck.ini

                                        # Query Status about histories launched via the Galaxy Automation workflow_runner.py script
                                        history_utils.py /Users/annblack/GalaxyAutomation/fastqs/batch23 check_status -ini configuration.eablck.ini

                                        # Delete all COMPLETED (OK) Galaxy Histories, any history that is failed or in progress will NOT be deleted.
                                        history_utils.py /Users/annblack/GalaxyAutomation/fastqs/batch23 delete -ini configuration.eablck.ini

                                        '''))
arg_parser.add_argument('all_history_dir', help="directory which contains the All_Histories.json file (generated by the Galaxy Automation workflow_runner.py script)")
arg_parser.add_argument('action', choices=['check_status', 'download', 'delete'], help="history helper utility to run")
arg_parser.add_argument('-d', '--delete_post_download', action='store_true', help="delete histories after they are successfully downloaded")
arg_parser.add_argument('-oo', '--overwrite_existing_downloads', action='store_true', help="remove existing history download files and re-download", default=False)
arg_parser.add_argument('-o', '--output_dir', help="directory to download and extract history results into", default='$ALL_HISTORY_DIR/download')
arg_parser.add_argument('-i', '--ini', help="configuration ini file to load", default='configuration.ini')
arg_parser.add_argument('-f', '--delete_failed_histories', action='store_true', help="delete failed histories in addition to the completed (ok) histories")

args = arg_parser.parse_args()


config_parser = SafeConfigParser()

if args.ini == 'configuration.ini':
    print ""
    print "A configuration file was not specified when running the command.  Will look for a default file \'configuration.ini\' to load configuration from."
    print ""
    arg_parser.print_help()
    if not os.path.isfile(args.ini):
        print ""
        print "ERROR!"
        print ""
        print "The configuration file, %s, does not exist. Create a configuration ini file and try again. Exiting." % (args.ini)
        print ""
        sys.exit(4)
        
    config_parser.read('configuration.ini')
elif args.ini.endswith('.ini'):
    if not os.path.isfile(args.ini):
        print ""
        print "ERROR!"
        print ""
        print "The configuration file, %s, does not exist. Create a configuration ini file and try again. Exiting." % (args.ini)
        print ""
        arg_parser.print_help()
        print ""
        sys.exit(5)
    config_parser.read(args.ini)
else:
    print "The configuration ini file must end with .ini, the file specified was %s", (args.ini)

'''
if len(sys.argv) >= 2:
    if sys.argv[1].endswith('.ini'):
        config_parser.read(sys.argv[1])
    else:
        print "You passed %s I need a .ini file" % (sys.argv[1],)
        sys.exit(1)
else:
    print ""
    print "A configuration file was not specified when running the command.  Will look for a default file \'configuration.ini\' to load configuration from."
    config_parser.read('configuration.ini')
'''

# Get the configuration parmaters
api_key = get_api_key(config_parser.get('Globals', 'api_file'))
galaxy_host = config_parser.get('Globals', 'galaxy_host')
output_dir = config_parser.get('Globals', 'output_dir')
delete_histories = config_parser.get('Globals', 'delete_post_download')

# Get a connection to Galaxy and a client to interact with Histories
galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
historyClient = HistoryClient(galaxyInstance)

print ""
print "Locating Histories To Perform Action, %s, On ... " % (args.action)

# Open up the serialized History file that was saved for the batch run
all_history_json_filename = os.path.join(args.all_history_dir, "All_Histories.json")
if not os.path.isfile(all_history_json_filename):
    print ""
    print "ERROR: Could not find All_Histories.json file in %s." % (args.all_history_dir)
    print "       Did you use the workflow_runner.py to launch automated Galaxy NGS analysis? " + \
          "Check your configuration.ini file to make sure it has configued the correct output directory to inspect. "
    print ""
    print "       ** Note: All_Histories.json is a file automatically generated when the workflow_runner.py script is invoked to launch workflows."
    print ""
    sys.exit(2)

all_history_json = open(all_history_json_filename, "rb")
try:
    histories = json.load(all_history_json)
except:
    print "ERROR: Could not load any history records from All_Histories.json file in %s." % (args.all_history_dir)
    print "       Did you use the workflow_runner.py to launch the automated Galaxy NGS analysis? Possibly the workflow_runner has not yet completed?"
    print ""
    sys.exit(2)

print "Loading List of Histories From : " + all_history_json.name
print ""

all_successful = []
all_running = []
all_failed = []
all_except = []
all_waiting = []

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

    if h['upload_history']:
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

num_histories = len(histories)

if args.action == "check_status":
    print "TOTAL Number of Histories: " + str(num_histories) + " ( RUNNING = " + str(len(all_running)) + ", FAILED? = " + str(len(all_failed)) + ", NOT_FOUND = " + str(len(all_except)) + ", COMPLETED (OK) = " + str(len(all_successful)) + ", WAITING TO RUN = " + str(len(all_waiting)) + " )"
    print ""
    print "UPLOAD HISTORY:"
    print "\tHISTORY_NAME => %s , REPORTED STATUS => %s , TOTAL_DATASETS => %s" % (upload_history.history['name'], upload_history.status['state'], get_total_datasets(upload_history.status))
    print ""

    if len(all_except) > 0:
        print ""
        print "EXCEPTIONS OCCURRED! Unable to retreive status for " + str(len(all_except)) + " of " + str(num_histories) + " histories."
        print ""
        print "\tErrors where thrown by Galaxy when attempting communication about the following histories." 
        print "\tIn your browser, please check that Galaxy can be connected to, the Galaxy URL provided in the configuration file is accurate,"
        print "\tand that the following histories exist in your 'Saved Histories' list:"
        print ""
        for h in all_except:
            print "\t\tHISTORY_NAME => "+h['name']

    if len(all_failed) > 0:
        print ""
        print "FAILURES OCCURRED! NGS Analysis Failed for " + str(len(all_failed)) + " of " + str(num_histories) + " histories."
        print ""
        print "\tFailures occurred when attempting the analyze the following samples/histories."
        print "\tIn your browser, please inspect the following histories and take necessary actions to re-run the NGS analysis:"
        print ""
        for h_info in all_failed:
            print "\t\tHISTORY_NAME => " + h_info.history['name'] + " , REPORTED STATUS => " + h_info.status['state']

    if len(all_waiting) > 0:
        print ""
        print "WAITING TO RUN. NGS Analysis has not yet started for " + str(len(all_waiting)) + " of " + str(num_histories)
        print ""
        for h_info in all_waiting:
            print "\t\tHISTORY_NAME => " + h_info.history['name'] + " , REPORTED STATUS => " + h_info.status['state']

    if len(all_running) > 0:
        print ""
        print "ACTIVELY RUNNING. NGS Analysis is currently underway for " + str(len(all_running)) + " of " + str(num_histories)
        print ""
        for h_info in all_running:
            print "\t\tHISTORY_NAME => " + h_info.history['name'] + " , PERCENT COMPLETED => " + str(h_info.status['percent_complete'])

    if len(all_successful) > 0:
        print ""
        print "COMPLETED.  NGS Analysis is currently completed for " + str(len(all_successful)) + " of " + str(num_histories)
        print ""
        print "\tThe following samples/histories have completed NGS analysis ran.  The output will still need to be inspected to ensure analysis accuracy and interpretation:"
        print ""
        for h_info in all_successful:
            print "\t\tHISTORY_NAME => " + h_info.history['name'] + " , PERCENT COMPLETED => " + str(h_info.status['percent_complete'])

    print ""
    print "All status has been retrieved. This command can be re-ran as needed."
    print ""
elif args.action == "download":
    print "DOWNLOADING COMPLETED HISTORIES:"
    if args.output_dir.startswith('$'):
        output_dir = os.path.join(args.all_history_dir, "download")
    else:
        output_dir = args.output_dir

    if not os.path.exists(output_dir):
        print "Output directory %s not found. Creating ..." % (output_dir)
        os.makedirs(output_dir)

    if len(all_successful) == 0:
        print ""
        print "None of the Histories have completed successfully.  Run again later, or use the check_status option to view analysis progress. Exiting."
        print ""
        sys.exit()
    elif num_histories != (len(all_successful) + 1):
        print ""
        print "Not all histories have completed successfully.  Will only download successfully completed analysis histories."
        print "Downloading %s of %s Histories" % (len(all_successful), num_histories)
        print ""
    else:
        print ""
        print "Downloading %s Completed Histories (skipping the Upload History, %s)" % (len(all_successful), upload_history.history['name'])
        print ""

    for h_info in all_successful:
        # create a sub-directory with the history name
        h_output_dir = os.path.join(output_dir, h_info.history['name'])
        if not os.path.exists(h_output_dir):
            os.makedirs(h_output_dir)
        history_gz_name = os.path.join(h_output_dir, h_info.history['name'] + ".tar.gz")
        
        if os.path.exists(history_gz_name):
            if args.overwrite_existing_downloads:
                print "\tHistory, %s, is already downloaded. Will overwrite." % (h_info.history['name'])
            else:
                print "\tHistory, %s, is already downloaded to %s. Skipping." % (h_info.history['name'], history_gz_name)
                continue

        history_gz = open(history_gz_name, 'wb')
        print "\tDownloading %s to %s" % (h_info.history['name'], history_gz_name) 
        jeha_id = historyClient.export_history(h_info.history['id'], wait=True)
        print "\t\tExported History from Galaxy, now Downloading and Saving to %s." % (h_output_dir)
        # create a new file for saving history
        historyClient.download_history(h_info.history['id'], jeha_id, history_gz)
        download_success = True
        history_dataset_names = []
        try:
            subprocess.check_call(["tar", "-xzvf", history_gz_name, "-C", h_output_dir])
 #           history_tar = tarfile.open(history_gz_name, 'r:gz')
#            history_dataset_names = history_tar.getmembers()
        except Exception as inst:
            download_success = False
            print ""
            print type(inst)
            print inst.args
            print inst
            print ""
        #check the file to make sure it seems sound.
        if download_success and (len(history_dataset_names) >= 0):
            download_success = True
        else:
            download_success = False
            print "\t\tERROR! Downloaded history file, %s, does not appear to be a valid tar archive file, or does not have an expected number of datasets (has %s). Please remove the file and try again." % (history_gz_name, len(history_dataset_names))
            print ""
            continue

        print "\t\tDownload complete. Extracting history archive file."
        history_tar.extractall(h_output_dir)
        

        if args.delete_post_download and download_success:
            print "\t\tDeleting history, %s, in Galaxy." % h_info.history['name']
            print ""
            historyClient.delete_dataset(h_info.history['id'])

elif args.action == "delete":
    #do something here
    print "DELETING ALL COMPLETED HISTORIES."
    print ""
    if len(all_successful) == 0:
        if args.delete_failed_histories:
            if len(all_failed) == 0:
                print ""
            print "No completed or failed Histories to delete - Nothing to do.  Run again later, or use the check_status option to view analysis progress. Exiting."
            print ""
            sys.exit()                
        else:
            print ""
            print "No completed Histories to delete - Nothing to do.  Run again later, or use the check_status option to view analysis progress. Exiting."
            print ""
            sys.exit()

    delete_upload_history = False
    if args.delete_failed_histories:
        if num_histories != (len(all_successful) + len(all_failed) + 1):
            print ""
            print "Will ignor histories that are in progress, or can not determine status from."
            print "Will ignor Upload history, %s, since there are still running analyses." % (upload_history.history['name'])
            print ""
        else:
            delete_upload_history = True
    else:
        if num_histories != (len(all_successful) + 1):
            print ""
            print "Not all histories have completed successfully.  Will only delete successfully completed analysis histories. Upload History, %s, will not be deleted until all histories are completed." % (upload_history.history['name'])
            print "Deleting %s of %s Histories" % (len(all_successful), num_histories)
            print ""
        else:
            print ""
            print "Deleting %s Completed Histories AND the Upload History, %s" % (len(all_successful), upload_history.history['name'])
            print ""
            delete_upload_history = True

    for h_info in all_successful:
        print "\t\tDeleting Completed (OK) history, %s, in Galaxy." % h_info.history['name']
        print ""
        historyClient.delete_dataset(h_info.history['id'])

    if args.delete_failed_histories:
        for h_info in all_failed:
            print "\t\tDeleting Completed (OK) history, %s, in Galaxy." % h_info.history['name']
            print ""
            historyClient.delete_dataset(h_info.history['id'])


sys.exit()
