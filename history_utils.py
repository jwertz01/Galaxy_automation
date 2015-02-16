#!/usr/bin/python
__author__ = 'eablck'

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.client import ConnectionError
from ConfigParser import SafeConfigParser
from ConfigParser import NoOptionError
from requests.packages import urllib3
from multiprocessing import Pool
import sys
import os
import json
import argparse
import textwrap
import tarfile
import logging


class HistoryAlreadyDownloadedException(Exception):
    '''
    Exception for indicating that the history has previously been downloaded.
    '''
    pass

class MaxLevelFilter(logging.Filter):
    '''
    Logging Filter for seperating messages between stdout and stderr
    Filters (lets through) all messages with level <= LEVEL
    '''
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno <= self.level 

class HistoryInfo():
    history = {}
    status = {}

    def __init__(self, hist, hist_status):
        self.history = hist
        self.status = hist_status

def _str2bool(val):
    '''
    Helper function to change configuration parms in config.ini into Boolean

    type: val: string
    param val: The value to convert into Boolean

    return Boolean interpretation of val
    '''
    return str(val).lower() in ("yes", "true")

def _get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api

def _get_total_datasets(history_status):
    total_datasets = 0
    if history_status is not None:
        total_datasets = sum(history_status['state_details'].itervalues())
    return total_datasets

def _has_failures(history):
    has_failures = False
    total_falures = 0
    if history is not None:
        has_failures += history['state_details']['error']
        has_failures += history['state_details']['failed_metadata']
        has_failures += history['state_details']['']

    if total_falures > 0:
        has_failures = True 

    return has_failures

def _delete(history_client, num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful, delete_failed_histories, purge_histories):
    logger = logging.getLogger(LOGGER_NAME)

    logger.info("DELETING HISTORIES...")
    if len(all_successful) == 0:
        if delete_failed_histories:
            if len(all_failed) == 0:
                logger.warning("\tNo completed or failed Histories to delete - Nothing to do.  Run again later, or use the check_status option to view analysis progress. Exiting.")
                return 5
        else:
            logger.warning("\tNo completed Histories to delete - Nothing to do.  Run again later, or use the check_status option to view analysis progress. Exiting.")
            return 6

    delete_upload_history = False
    if delete_failed_histories:
        if num_histories != (len(all_successful) + len(all_failed) + 1):
            logger.warning("\tWill ignor histories that are in progress, or can not determine status from.")
            logger.warning("\tWill ignor Upload history, %s, since there are still running analyses.", upload_history.history['name'])
        else:
            delete_upload_history = True
    else:
        if num_histories != (len(all_successful) + 1):
            logger.warning("\tNot all histories have completed successfully.  Will only delete successfully completed analysis histories. Upload History, %s, will not be deleted until all histories are completed.", upload_history.history['name'])
            logger.info("Deleting %s SUCCESSFULLY Completed Histories of %s total Histories", len(all_successful), num_histories)
        else:
            logger.info("Deleting %s Completed Histories AND the Upload History, %s", len(all_successful), upload_history.history['name'])
            delete_upload_history = True

    if (len(all_successful) > 0):
        logger.info("\tDeleting Completed (SUCCESSFULLY - OK) Histories:")
        for h_info in all_successful:
            history_client.delete_history(h_info.history['id'], purge_histories)
            logger.info("\t\tHISTORY %s DELETED. Purged? %s", h_info.history['name'], purge_histories)

    if (delete_failed_histories and (len(all_failed) > 0)):
        logger.info("\tDeleting FAILED Histories:")
        for h_info in all_failed:
            history_client.delete_history(h_info.history['id'])
            logger.info("\t\tHISTORY %s DELETED. Purged? %s", h_info.history['name'], purge_histories)

    if delete_upload_history is True:
        logger.info("\tAll Result Histories Deleted, Deleting Upload History: %s", upload_history.history['name'])
        history_client.delete_history(upload_history.history['id'])
        logger.info("\t\tHISTORY %s DELETED. Purged? %s", upload_history.history['name'], purge_histories)
    else:
        logger.warning("\tIGNORING the following Histories because they are Running, Queued, or Can not be Reached for some reason:")
        all_ignored = all_running + all_waiting
        if not delete_failed_histories:
            all_ignored += all_failed
        for h_info in all_ignored:
            logger.warning("\t\tHISTORY_NAME => \'%s\' : HISTORY_STATUS => %s", h_info.history['name'], h_info.status['state'])
        for h in all_except:
            logger.warning("\t\tHISTORY_NAME => \'%s\' : HISTORY_STATUS => Unknown", h['name'])

def _post_dl_callback(h_info):
    logger = logging.getLogger(LOGGER_NAME)

    logger.info("DOWNLOADED History: %s", h_info.history['name'])
    logger.info("\tHistory Archive File: %s", h_info.history['download_gz'])
    logger.info("\tHistory download Directory: %s", h_info.history['download_dir'])
    logger.info("\tDeleted History After Download? %s", str(h_info.history['deleted']))
    return

def _download_history(galaxy_host, api_key, h_output_dir, h_info, force_overwrite, delete_post_download, purge):

    try:
        galaxy_instance = GalaxyInstance(galaxy_host, key=api_key)
        history_client = HistoryClient(galaxy_instance)

        if not os.path.exists(h_output_dir):
            os.makedirs(h_output_dir)
        history_gz_name = os.path.join(h_output_dir, h_info.history['name'] + ".tar.gz")
        h_info.history['download_gz'] = history_gz_name
        h_info.history['download_dir'] = h_output_dir
        
        if os.path.exists(history_gz_name):
            if not force_overwrite:
                err_msg = "HISTORY ALREADY DOWNLOADED - SKIPPING: History %s is already downloaded and located here: %s." % (h_info.history['name'], history_gz_name)
                raise HistoryAlreadyDownloadedException(err_msg)

        history_gz = open(history_gz_name, 'wb')
        jeha_id = history_client.export_history(h_info.history['id'], wait=True)

        # create a new file for saving history
        history_client.download_history(h_info.history['id'], jeha_id, history_gz)
        history_gz.flush()
        history_gz.close()

        download_success = True
        history_dataset_names = []
        try:
            history_tar = tarfile.open(history_gz_name, 'r:gz')
            history_dataset_names = history_tar.getmembers()
        except Exception as inst:
            download_success = False
            err_msg = "Failure occurred during history dowload, %s. Error: %s. Args: %s. Message: %s" % (h_info.history['name'],type(inst), inst.args, inst)
            raise RuntimeError(err_msg)
        #check the file to make sure it seems sound.
        if download_success and (len(history_dataset_names) >= 10):
            download_success = True
        else:
            download_success = False
            err_msg = "ERROR! Downloaded history file, %s, does not appear to be a valid tar archive file, or does not have an expected number of datasets (has %s). Please remove the file and try again." % (history_gz_name, len(history_dataset_names))
            raise RuntimeError(err_msg)

        history_tar.extractall(h_output_dir)

        if (delete_post_download is True) and download_success:
            h_info.history['deleted'] = True
            history_client.delete_history(h_info.history['id'], purge=purge)
        else:
            h_info.history['deleted'] = False

        history_tar.close()
    except (RuntimeError, HistoryAlreadyDownloadedException):
        raise
    except Exception as inst:
        # Wrappering exception to make sure the exception is pickable.  HTTPError would cause UnpickleableErrors and hang process workers during pool join
        err_msg = "Error (type: %s) occurred when downloading History, %s." % (type(inst), h_info.history['name'])
        raise RuntimeError(err_msg)

    return h_info

def _report_status(num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful):
    '''
    Reports status for the histories found in the All_Histories.json file
    '''
    logger = logging.getLogger(LOGGER_NAME)

    logger.info("TOTAL Number of Histories: " + str(num_histories) + " ( RUNNING = " + str(len(all_running)) + ", FAILED? = " + str(len(all_failed)) + ", NOT_FOUND = " + str(len(all_except)) + ", COMPLETED (OK) = " + str(len(all_successful)) + ", WAITING TO RUN = " + str(len(all_waiting)) + " )")
    logger.info("")
    logger.info("UPLOAD HISTORY:")
    if upload_history is not None:
        logger.info("\tHISTORY_NAME => %s , REPORTED STATUS => %s , TOTAL_DATASETS => %s", upload_history.history['name'], upload_history.status['state'], _get_total_datasets(upload_history.status))
    else:
        logger.error("\tUnable to retreive status for the upload_history.")
    logger.info("")

    if len(all_except) > 0:
        logger.error("")
        logger.error("EXCEPTIONS OCCURRED! Unable to retreive status for " + str(len(all_except)) + " of " + str(num_histories) + " histories.")
        logger.error("\tErrors where thrown by Galaxy when attempting communication about the following histories.")
        logger.error("\tIn your browser, please check that Galaxy can be connected to, the Galaxy URL provided in the configuration file is accurate,")
        logger.error("\tand that the following histories exist in your 'Saved Histories' list in the Galaxy UI:")
        for h in all_except:
            logger.error("\t\tHISTORY_NAME => %s", h['name'])

    if len(all_failed) > 0:
        logger.info("")
        logger.error("FAILURES OCCURRED! NGS Analysis Failed for %s of %s histories.", str(len(all_failed)), str(num_histories))
        logger.error("\tFailures occurred when attempting the analyze the following samples/histories.")
        logger.error("\tIn your browser, please inspect the following histories and take necessary actions to re-run the NGS analysis:")
        for h_info in all_failed:
            logger.error("\t\tHISTORY_NAME => %s , REPORTED STATUS => %s", h_info.history['name'], h_info.status['state'])

    if len(all_waiting) > 0:
        logger.info("")
        logger.info("WAITING TO RUN. NGS Analysis has not yet started for %s of %s", str(len(all_waiting)), str(num_histories))
        for h_info in all_waiting:
            logger.info("\t\tHISTORY_NAME => %s , REPORTED STATUS => %s", h_info.history['name'], h_info.status['state'])

    if len(all_running) > 0:
        logger.info("")
        logger.info("ACTIVELY RUNNING. NGS Analysis is currently underway for %s of %s", str(len(all_running)), str(num_histories))
        for h_info in all_running:
            logger.info("\t\tHISTORY_NAME => %s , PERCENT COMPLETED => %s", h_info.history['name'], str(h_info.status['percent_complete']))

    if len(all_successful) > 0:
        logger.info("")
        logger.info("COMPLETED.  NGS Analysis is currently completed for %s of %s", str(len(all_successful)), str(num_histories))
        logger.info("\tThe following samples/histories have completed NGS analysis ran.  The output will still need to be inspected to ensure analysis accuracy and interpretation:")
        for h_info in all_successful:
            logger.info("\t\tHISTORY_NAME => %s, PERCENT COMPLETED => %s", h_info.history['name'], str(h_info.status['percent_complete']))

    logger.info("")
    logger.info("All status has been retrieved. This command can be re-ran as needed.")
    logger.info("")

def _download(galaxy_host, api_key, num_processes, root_output_dir, use_sample_result_dir, num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful, force_overwrite, delete_post_download, purge):

    logger = logging.getLogger(LOGGER_NAME)

    logger.info("DOWNLOADING COMPLETED HISTORIES:")

    if len(all_successful) == 0:
        logger.warning("\tNone of the Histories have completed successfully.  Run again later, or use the check_status option to view analysis progress. Exiting.")
        return 0
    elif num_histories != (len(all_successful) + 1):
        logger.warning("\tNot all histories have completed successfully.  Will only download SUCCESSFULLY completed analysis histories.")
        logger.warning("\tThe following Histories will NOT be downloaded since they are FAILED, or QUEUED (WAITING), or RUNNING:")
        for h_info in all_failed:
            logger.warning("\t\tHISTORY_NAME => \'%s\' : HISTORY_STATUS => %s", h_info.history['name'], h_info.status['state'])
        for h_info in all_running:
            logger.warning("\t\tHISTORY_NAME => \'%s\' : HISTORY_STATUS => %s", h_info.history['name'], h_info.status['state'])
        for h_info in all_waiting:
            logger.warning("\t\tHISTORY_NAME => \'%s\' : HISTORY_STATUS => %s", h_info.history['name'], h_info.status['state'])
        for h in all_except:
            logger.warning("\t\tHISTORY_NAME => \'%s\' : HISTORY_STATUS => Unknown", h['name'])
        logger.warning("\tDownloading %s SUCCESSFULLY Completed Histories out of %s total Histories:", len(all_successful), num_histories)
    else:
        logger.info("\tDownloading %s SUCCESSFULLY Completed Histories (skipping the Upload History: %s)", len(all_successful), upload_history.history['name'])

    pool = Pool(processes=int(num_processes))

    dl_results = {}
    for h_info in all_successful:
        # create a sub-directory with the history name
        if use_sample_result_dir is False:
            h_output_dir = os.path.join(root_output_dir, (os.path.join(h_info.history['name'], "download")))
        else:
            h_output_dir = os.path.join(h_info.history['sample_result_dir'], "download")

        result = pool.apply_async(_download_history, args=[galaxy_host, api_key, h_output_dir, h_info, force_overwrite, delete_post_download, purge], callback=_post_dl_callback)
        dl_results[h_info.history['name']] = result

    #should be all done with processing.... this will block until all work is done
    pool.close()
    pool.join()

    # lets check the sucessfullness of the runs
    for history_name in dl_results.keys():
        history_result = dl_results[history_name]
        if not history_result.successful():
            try:
                history_result.get()
            except HistoryAlreadyDownloadedException as e_already_download:
                logger.warning("\t" + e_already_download.message)
            except Exception as inst:
                logger.error("HISTORY DOWNLOAD ERROR! HISTORY NAME = %s", history_name)
                logger.exception(inst)

def _get_argparser():
    '''
    Configure an argument parser to process any arguments passed into the main method.

    :return: Configured parsrer for processing arguments supplied to main method (for example from command line) :argparse.ArgumentParser: argparse ArgumentParser object
    '''
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
    arg_parser.add_argument('-o', '--output_dir', help="directory to download and extract history results into. Default will be the root $ALL_HISTORY_DIR and each of the sample specific results directories generated when invoking workflow_runner.py", default='$ALL_HISTORY_DIR')
    arg_parser.add_argument('-i', '--ini', help="configuration ini file to load", default='configuration.ini')
    arg_parser.add_argument('-f', '--delete_failed_histories', action='store_true', help="delete failed histories in addition to the completed (ok) histories. Only used with action = \'delete\'")

    return arg_parser

def _parse_ini(args):
    '''
    Load all configuration information from the ini file

    :type args: argparse.Namespace
    :param args: Program arguments specified via command line

    :return: A primed :SafeConfigParser: object ready to get configuration parameters from :ConfigParser.SafeConfigParser:
    '''
    logger = logging.getLogger(LOGGER_NAME)

    config_parser = SafeConfigParser()

    if args.ini == 'configuration.ini':
        logger.info("A configuration file was not specified when running the command.  Will look for a default file \'configuration.ini\' to load configuration from.")
        if not os.path.isfile(args.ini):
            raise RuntimeError("The configuration file, %s, does not exist. Create a configuration ini file and try again. Exiting." % (args.ini))
            
        config_parser.read('configuration.ini')
    elif args.ini.endswith('.ini'):
        if not os.path.isfile(args.ini):
            raise RuntimeError("The configuration file, %s, does not exist. Create a configuration ini file and try again. Exiting." % (args.ini))
        config_parser.read(args.ini)
    else:
        raise RuntimeError("The configuration ini file must end with .ini, the file specified was %s", (args.ini))

    return config_parser

##########################################################################
# Main Logic Starts Here
##########################################################################
def main(argv=None):

    arg_parser = _get_argparser()
    if argv is None:
        args = arg_parser.parse_args()
    else:
        args = arg_parser.parse_args(argv)

    use_sample_result_dir = True
    if args.output_dir.startswith('$'):
        output_dir = args.all_history_dir
    else:
        output_dir = args.output_dir
        use_sample_result_dir = False

    if not os.path.exists(output_dir):
        print "Output directory %s not found. Creating ..." % (output_dir)
        os.makedirs(output_dir)


    ## Build log file.
    ## We will have a main logger to system out
    ## A global log file
    ## And a log per each sample in the sample directory
    ## configure console logging

    runlog_filename = os.path.join(output_dir, "History_Utils.log")
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=runlog_filename,
                    filemode='ab')
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)

    no_date_formatter = logging.Formatter('%(levelname)s\t%(name)s\t%(message)s')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(MaxLevelFilter(logging.INFO))
    stdout_handler.setFormatter(no_date_formatter)

    stderr_hander = logging.StreamHandler(sys.stderr)
    stderr_hander.setLevel(logging.WARNING)
    stderr_hander.setFormatter(no_date_formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_hander)

    try:
        config_parser = _parse_ini(args)
    except RuntimeError as inst:
        logger.error(inst.message)
        logger.info("HINT: How to run the history_utils tool:")
        arg_parser.print_usage()
        return 1

    # Get the configuration parmaters
    try:
        api_key = _get_api_key(config_parser.get('Globals', 'api_file'))
        galaxy_host = config_parser.get('Globals', 'galaxy_host')
        delete_histories = _str2bool(config_parser.get('Globals', 'delete_post_download'))
        if args.delete_post_download is not None:
            delete_histories = args.delete_post_download
        purge_histories = _str2bool(config_parser.get('Globals', 'purge'))
    except NoOptionError as e:
        logger.error("Problem occured when reading configuration from %s. Please verify properly configured key names and values.", args.ini)
        logger.exception(e)
        return 7

    # Get a connection to Galaxy and a client to interact with Histories
    galaxy_instance = GalaxyInstance(galaxy_host, key=api_key)
    history_client = HistoryClient(galaxy_instance)

    logger.info("")
    logger.info("Locating Histories To Perform Action, %s, On ... ", args.action)

    # Open up the serialized History file that was saved for the batch run
    all_history_json_filename = os.path.join(args.all_history_dir, "All_Histories.json")
    if not os.path.isfile(all_history_json_filename):
        logger.error("")
        logger.error("ERROR: Could not find All_Histories.json file in %s.", args.all_history_dir)
        logger.error("\tDid you use the workflow_runner.py to launch automated Galaxy NGS analysis? " + \
              "Check your configuration.ini file to make sure it has configued the correct output directory to inspect. ")
        logger.error("")
        logger.error("\t** Note: All_Histories.json is a file automatically generated when the workflow_runner.py script is invoked to launch workflows.")
        logger.error("")
        return 2

    all_history_json = open(all_history_json_filename, "rb")
    try:
        histories = json.load(all_history_json)
    except:
        logger.error("ERROR: Could not load any history records from All_Histories.json file in %s.", args.all_history_dir)
        logger.error("\tDid you use the workflow_runner.py to launch the automated Galaxy NGS analysis? Possibly the workflow_runner has not yet completed?")
        logger.error("")
        return 3

    logger.info("Loading List of Histories From : %s", all_history_json.name)

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

    num_histories = len(histories)

    if args.action == "check_status":

        _report_status(num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful)

    elif args.action == "download":

        num_processes = config_parser.get('Globals', 'num_processes')

        _download(galaxy_host, api_key, num_processes, output_dir, use_sample_result_dir, num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful, args.overwrite_existing_downloads, delete_histories, purge_histories)

     
    elif args.action == "delete":

        #def _delete(history_client, num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful, delete_failed_histories, purge_histories):
        _delete(history_client, num_histories, upload_history, all_except, all_failed, all_waiting, all_running, all_successful, args.delete_failed_histories, purge_histories)

    return 0

# Disable Warnings. Without this a warning such as the following is generated:
# /Library/Python/2.7/site-packages/requests/packages/urllib3/connectionpool.py:734:
# InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
# See: https://urllib3.readthedocs.org/en/latest/security.html
# At some point we might want to allow this warning instead, or fix up the
# cause.
urllib3.disable_warnings()

LOGGER_NAME = 'history_utils'

if __name__ == "__main__":
    sys.exit(main())

