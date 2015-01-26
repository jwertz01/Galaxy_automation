#!/usr/bin/python
__author__ = 'tbair,eablck'

# Needs python 2.7 or higher for functionality

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.datasets import DatasetClient
from bioblend.galaxy.client import ConnectionError
from requests.packages import urllib3
from ConfigParser import SafeConfigParser
from multiprocessing import Pool
from functools import partial
import sys
import os
import fnmatch
import os.path
import re
import time
import json
import argparse
import textwrap
import logging
import collections

class MaxLevelFilter(logging.Filter):
    '''Filters (lets through) all messages with level <= LEVEL'''
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno <= self.level 

def _get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api


def _parse_sample_name(file_path, file_name_re):
    sample_name = file_name_re.match(os.path.basename(file_path))
    if sample_name is not None:
        return sample_name.group(1)
    else:
        return 'Unable_to_parse'


def _setup_base_datamap(workflow, library_list_mapping, library_datasets, upload_dataset_map):
    '''
        Creates the data map that the bioblend WorkflowClient needs to invoke the workflow_label

        :type workflow: returned JSON object from WorkflowClient.show_workflow
        :param workflow: a description of the workflow and its iputs as a JSON object

        :type library_list_mapping: dict 
        :param library_list_mapping: a map of workflow input names to galaxy library dataset ids 

        :type library_datasets: list 
        :param library_datasets: a list of the library datasets that were imported into the upload history

        :type upload_list_mapping: dict 
        :param upload_list_mapping: a map of workflow input names to files/datasets that should have been uploaded from local filesystem

        :type r1_id: str 
        :param r1_id: the galaxy dataset id of the READ1 local file that was uploaded to the upload history

        :type r2_id: str
        :param r2_id: the galaxy dataset id of the READ2 local file that was uploaded to the upload history

        :return: a dataset map depicting the mapping of workflow inputs to galaxy dataset ids, formatted as needed by 
                 bioblend WorkflowClient run_workflow
    '''
    datamap = {}
    for w_input in workflow['inputs'].keys():
        data_set = []
        workflow_label = workflow['inputs'][w_input]['label']
        # find mapping if present
        if workflow_label in library_list_mapping:
            if not workflow_label in UPLOAD_TYPE_GLOBALS:
                data_set = {
                    'id': library_datasets[workflow_label]['id'], 'hda_ldda': library_datasets[workflow_label]['hda_ldda']}
        elif workflow_label in upload_dataset_map:
            data_set = {
                    'id': upload_dataset_map[workflow_label]['id'], 'hda_ldda': 'hda'}
        else:
            labels = ""
            for wf_input in workflow['inputs'].keys():
                labels += "%s, " % (workflow['inputs'][wf_input]['label'],)
            raise RuntimeError("Workflow requesting '%s' unsure what to assign. Choices I have: %s. Workflow would like the following inputs please adjust your configuration.ini file: (%s). WorkflowConfiguration Problem - unknown requested input. Adjust and validate configuration.ini file.", workflow_label, ",".join(library_list_mapping.keys().join(upload_list_mapping)), labels)
        datamap[w_input] = {'id': data_set['id'], 'src': data_set['hda_ldda']}
    return datamap


def _import_library_datasets(history_client, upload_history, library_dataset_list, default_lib):
    library_datasets = {}
    for data in library_dataset_list:
        key, value = data.split(':')
        library_file_dataset = history_client.upload_dataset_from_library(
            upload_history['id'], value)
        # Lets make the datasets un-deleted. Unclear why they have them marked
        # as deleted when they import.
        # From code inspection - I think the status code returned
        # is the HTTP status codes
        status_code = history_client.update_dataset(
            upload_history['id'], library_file_dataset['id'], deleted=False)
        library_file_dataset['library_name'] = default_lib
        if status_code != 200:
            raise RuntimeError("Unable to mark the imported library dataset as un-deleted. Galaxy communication error?")
        library_datasets[key] = library_file_dataset
    return library_datasets


def _get_notes(history, workflow, library_list_mapping, library_datasets, upload_dataset_map, upload_input_files_map):
    # return a string that describes this particular setup
    notes = []
    notes.append("Original Input Directory: " + history['sample_dir'])
    notes.append("Results Directory: " + history['result_dir'])
    notes.append("Upload History Name: " + history['upload_history_name'])
    notes.append("Result History Name: " + history['name'])

    notes.append(
        "Workflow Name (id): " + workflow['name'] + " (" + workflow['id'] + ")")
    notes.append("Workflow Runtime Inputs >> ")
    for wf_input in workflow['inputs'].keys():
        dataset_name = ""
        dataset_id = ""
        dataset_file = ""
        workflow_label = workflow['inputs'][wf_input]['label']
        if workflow_label in library_list_mapping:
            if not workflow_label in CONFIG_GLOBALS:
                dataset_name = library_datasets[workflow_label]['name']
                dataset_id = library_datasets[workflow_label]['id']
                dataset_lib = library_datasets[workflow_label]['library_name']
                notes.append(
                    workflow_label + "("+wf_input + ") => " + dataset_lib + os.path.sep + dataset_name + " (" + dataset_id + ")")
        elif workflow_label in upload_dataset_map:
            # This would need to be augmented if there are other types of
            # uploads needed
            dataset = upload_dataset_map[workflow_label]
            dataset_file = upload_input_files_map[workflow_label]
            dataset_name = dataset['name']
            dataset_id = dataset['id']
            notes.append(
                workflow_label + "("+wf_input + ") => " + dataset_name + " (" + dataset_id + ") " + dataset_file)
    return notes


def _changePath(old_path):
    # get where to write and root
    new_path = old_path.replace(args.input_dir, output_dir)
    return new_path

def _upload_input_files(upload_wf_input_files_map, history_client, tool_client):
    upload_dataset_map = {}

    return upload_dataset_map

def _get_all_upload_files(root_path, upload_list_mapping, config_parser):
    '''
    Locates all the files that should be uploaded and groups them together according to workflow inputs.

    :type: root_path: str
    :param root_path: The root input directory as specified as a main argument

    :type: upload_list_mapping: dict 
    :param: upload_list_mapping: a mapping of workflow input names to the local file patterns that should be found and uploaded to Galaxy 

    :type: config_parser: SafeConfigParser
    :param config_parser: The configuration parser for loading configuration from the ini file

    :return list of dict objects that contain a mapping of workflow_input_name to local_file_name for each workflow to be ran

    '''
    file_name_re = re.compile(config_parser.get('Globals', 'sample_re'))

    upload_file_tuple_list = []
    file_list = []
    read1_file_list = None

    upload_inputname_list = upload_list_mapping.keys()

    for upload_input_name in upload_inputname_list:
        upload_type = upload_list_mapping[upload_input_name]
        if upload_type in UPLOAD_TYPE_GLOBALS:
            if upload_type == UPLOAD_TYPE_READ1:
                file_list = _get_files(root_path, config_parser.get('Globals', 'READ1_re'))
                read1_file_list = file_list
            elif upload_type == UPLOAD_TYPE_READ2:
                if read1_file_list is None:
                    raise RuntimeError("Cound not find any READ1 files.  If READ2 files are configured as workflow inputs (in config.ini), the READ1 inputs must be listed first int he upload_input_ids.")
                else:
                    file_list = []
                    for read1_file in read1_file_list:
                        r2_file = _get_r2_file(read1_file, config_parser)
                        file_list.append(r2_file)

        else:
            # assume it is a regular expression - generic
            file_list = _get_files(root_path, upload_type)

        for input_file in file_list:
            index = file_list.index(input_file)
            try:
                wf_upload_input_file_map = upload_file_tuple_list[index]
            except IndexError:
                wf_upload_input_file_map = collections.OrderedDict()
                upload_file_tuple_list.append(wf_upload_input_file_map)
            wf_upload_input_file_map[upload_input_name] = input_file

    return upload_file_tuple_list

def _get_r2_file(r1_file, config_parser):
    read1_sub_re = config_parser.get('Globals', 'READ1_sub_re')
    read2_sub_re = config_parser.get('Globals', 'READ2_sub_re')

    input_dir_path = os.path.dirname(r1_file) + os.path.sep
    r1_sub_pattern = re.compile(read1_sub_re, re.IGNORECASE)
    r2_file = r1_sub_pattern.sub(read2_sub_re, r1_file)
    if not os.path.exists(r1_file):
        raise RuntimeError("%s R1 file Not Found", r1_file)
    if not os.path.exists(r2_file):
        raise RuntimeError("%s R2 file Not Found", r1_file)

    return r2_file

def _get_files(root_path, file_match_re):
    # Eventually might want to get more than just fastq files to upload.
    # This would require some type of alignment by sample id, or in a common directory,
    # or being passed in a fully qualified file name or someting else.
    # But for now....
    matches = []
    for root, dirnames, filenames in os.walk(root_path):
        for filename in fnmatch.filter(filenames, file_match_re):
            matches.append(os.path.join(root, filename))
    return matches

def _post_wf_run(history, all_histories):
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Workflow launch successful for sample: %s", history['sample_name'])
    logger.info("\tDetails of the workflow invocation:")
    notes = history['notes']
    for note in notes:
        logger.info("\t\t%s", note)

    all_histories.append(history)


def _launch_workflow(galaxy_host, api_key, workflow, upload_history, upload_input_files_map, genome, library_list_mapping, library_datasets, sample_name, result_dir ):

    # Create a log file specific for this sample
    sample_result_dir = os.path.join(result_dir, sample_name)
    if not os.path.exists(sample_result_dir):
        os.makedirs(sample_result_dir)

    # Clean up any existing file handlers (this is due to multi-processor threads)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    tab_formatter = '\t\t\t\t'
    runlog_filename = os.path.join(sample_result_dir, sample_name+"_Workflow_Runner.log")
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=runlog_filename,
                    filemode='wb')

    local_logger = logging.getLogger("workflow_runner_"+sample_name)


    try:
        galaxy_instance = GalaxyInstance(galaxy_host, key=api_key)
        history_client = HistoryClient(galaxy_instance)
        tool_client = ToolClient(galaxy_instance)
        workflow_client = WorkflowClient(galaxy_instance)

        sample_dir = os.path.dirname(upload_input_files_map.values()[0])

        local_logger.info("Launching workflow for sample: %s", sample_name)
        history = history_client.create_history(sample_name)
        history['upload_history'] = False
        history['sample_name'] = sample_name
        history['sample_dir'] = sample_dir
        history['upload_history_name'] = upload_history['name']
        history['result_dir'] = result_dir

        # upload all the files
        local_logger.info("Uploading input files for workflow: %s", workflow['name'])
        upload_dataset_map = {}
        for wf_inputname in upload_input_files_map.keys():
            upload_file = upload_input_files_map[wf_inputname]
            input_upload_out = tool_client.upload_file(
                upload_file, upload_history['id'], file_type='fastqsanger', dbkey=genome)
            local_logger.info("\tUploaded: %s => %s", wf_inputname, upload_file)
            input_dataset = input_upload_out['outputs'][0]
            upload_dataset_map[wf_inputname] = input_dataset

        data_map = _setup_base_datamap(
            workflow, library_list_mapping, library_datasets, upload_dataset_map)

        # Have files in place need to set up workflow
        # Based on example at
        # http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
        notes = _get_notes(history, workflow, library_list_mapping, library_datasets, upload_dataset_map, upload_input_files_map)
        local_logger.info("Details of the workflow invocation:")
        for note in notes:
            local_logger.info("\t%s", note)
        history['notes'] = notes
        workflow_notes = ",".join(notes)
        rep_params = {
            'SAMPLE_ID': sample_name, 'WORKFLOW_NOTES': workflow_notes}
    #   Keeping the following lines of code commented.  It might be in future versions of Galaxy this is how
    #   nested parameters need to be set.  See forum: https://github.com/afgane/bioblend/issues/71
    #   Until then, while running our hoemgrown patched Galaxy runtime, we will use the json mechanism to show nested parameters.
    #        params = {'toolshed.g2.bx.psu.edu/repos/devteam/bwa_wrappers/bwa_wrapper/1.2.3': {'params|readGroup|rgid': sampleName,
    #                                                                                          'params|readGroup|rglb': sampleName,
    #                                                                                          'params|readGroup|rgsm': sampleName},
    #                  'annotation_v2_wrapper': {'input_notes': workflow_notes}}
        params = {'toolshed.g2.bx.psu.edu/repos/devteam/bwa_wrappers/bwa_wrapper/1.2.3': {'params': {'readGroup': {'rgid': sample_name,
                                                                                                                   'rglb': sample_name,
                                                                                                                   'rgsm': sample_name}}},
                  'annotation_v2_wrapper': {'report_selector': {'input_notes': workflow_notes}}}
        rwf = workflow_client.run_workflow(workflow['id'],
                                          data_map,
                                          params=params,
                                          history_id=history['id'],
                                          replacement_params=rep_params,
                                          import_inputs_to_history=False)
        local_logger.info("Workflow has been initiated, the resulting history object will be logged in All_Histories.json.")
    except Exception as inst:
        local_logger.error("Unexpected Error occurred: %s : %s : %s", inst.__class__.__name__, inst.args, inst.message)
        local_logger.exception(inst)
        raise RuntimeError("Error occurred with processing Sample, %s, of type %s.  Review sample log file for more information: %s", sample_name, type(inst), runlog_filename)
    finally:
        logging.shutdown()

    return history

def _get_argparser():
    '''
    Configure an argument parser to process any arguments passed into the main method.

    :return: Configured parsrer for processing arguments supplied to main method (for example from command line) :argparse.ArgumentParser: argparse ArgumentParser object
    '''
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         usage='%(prog)s input_dir [OPTIONS]',
                                         description=textwrap.dedent('''\
                                                    Automatically launch Galaxy workflows, including uploading input files
                                                    and importing of additional input files from Galaxy shared data libraries.

                                                    Each workflow ran will have resulting datasets sent to its own Galaxy history.

                                                    This tool will output a run log for each workflow launched, in addition to a history log
                                                    (All_Histories.json) that will be used by history_utils.py for taking additional
                                                    actions on the generated result histories from each workflow launch.

                                                    Galaxy connection and workflow confirguration information is loaded from configuration.ini file.
                                            '''),
                                         epilog=textwrap.dedent('''\
                                            Examples:

                                            # Launch workflows for all input files found under '/Users/annblack/GalaxyAutomation/fastqs/batch23'
                                            # Resulting runtime and history log files will be stored under '/Users/annblack/GalaxyAutomation/fastqs/batch23/results'
                                            workflow_runner.py /Users/annblack/GalaxyAutomation/fastqs/batch23 -i configuration.eablck.ini

                                            # Launch workflows for all input files found under '/Users/annblack/GalaxyAutomation/fastqs/batch23'
                                            # Output the resulting runtime and history log files under '/Users/annblack/Results/batch23/results'
                                            workflow_runner.py /Users/annblack/GalaxyAutomation/fastqs/batch23 -o /Users/annblack/Results/batch23/results -i configuration.eablck.ini

                                            '''))
    arg_parser.add_argument('input_dir', help="directory which contains the workflow input files (subfolders will be traversed) that need to be uploaded.  IE, the files used to match to upload_input_ids (configuration.ini)")
    arg_parser.add_argument('-o', '--output_dir', help="directory to save logs into as well as history log file (All_Histories.json, used by the history_utils.py tool) Recommended to also be a directory you would like to download workflow results into. Directory will be created if it does not exist. Default will be $INPUT_DIR/results", default='$INPUT_DIR/results')
    arg_parser.add_argument('-i', '--ini', help="configuration ini file to load", default='configuration.ini')

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
# Main Runner Logic Starts Here
##########################################################################
def main(argv=None):

    ## Build log file.
    ## We will have a main logger to system out
    ## A global log file
    ## And a log per each sample in the sample directory
    ## configure console logging

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    no_date_formatter = logging.Formatter('%(levelname)s\t%(name)s\t%(message)s')
    date_formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
    tab_formatter = '\t\t\t\t'

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(MaxLevelFilter(logging.INFO))
    stdout_handler.setFormatter(no_date_formatter)

    stderr_hander = logging.StreamHandler(sys.stderr)
    stderr_hander.setLevel(logging.WARNING)
    stderr_hander.setFormatter(no_date_formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_hander)

    arg_parser = _get_argparser()
    if argv is None:
        args = arg_parser.parse_args()
    else:
        args = arg_parser.parse_args(argv)


    try:
        config_parser = _parse_ini(args)
    except RuntimeError as inst:
        logger.error(inst.message)
        logger.info("HINT: How to run the workflow_runner tool:")
        arg_parser.print_usage()
        return 1



    if args.output_dir.startswith('$'):
        output_dir = os.path.join(args.input_dir, "results")
    else:
        output_dir = args.output_dir

    if not os.path.exists(output_dir):
        logger.info("Output directory %s not found. Creating ...", output_dir)
        os.makedirs(output_dir)

    # Create a new run log file.
    runlog_filename = os.path.join(output_dir, "Workflow_Runner.log")
    runlog_handler = logging.FileHandler(runlog_filename, mode="wb")
    runlog_handler.setFormatter(date_formatter)
    runlog_handler.setLevel(logging.DEBUG)
    logger.info("Logging to : %s", runlog_filename)
    logger.addHandler(runlog_handler)

    # This will be the serialized history record - to be created when files are found.
    all_history_json = None

    try:

        # Get the configuration parmaters
        api_key = _get_api_key(config_parser.get('Globals', 'api_file'))
        galaxy_host = config_parser.get('Globals', 'galaxy_host')
        file_name_re = re.compile(config_parser.get('Globals', 'sample_re'))
        read1_re = config_parser.get('Globals', 'READ1_re')
        read1_sub_re = config_parser.get('Globals', 'READ1_sub_re')
        read2_sub_re = config_parser.get('Globals', 'READ2_sub_re')
        library_input_ids = ''.join(ch for ch in config_parser.get('Globals', 'library_input_ids') if ch != '\n')
        library_dataset_list = library_input_ids.split(',')
        upload_input_ids = ''.join(ch for ch in config_parser.get('Globals', 'upload_input_ids') if ch != '\n')
        upload_dataset_list = upload_input_ids.split(',')
        genome = config_parser.get('Globals', 'genome')
        default_lib = config_parser.get('Globals', 'default_lib')
        workflow_id = config_parser.get('Globals', 'workflow_id')
        num_processes = config_parser.get('Globals', 'num_processes')

        pool = Pool(processes=int(num_processes))

        galaxy_instance = GalaxyInstance(galaxy_host, key=api_key)
        history_client = HistoryClient(galaxy_instance)
        tool_client = ToolClient(galaxy_instance)
        workflow_client = WorkflowClient(galaxy_instance)
        dataSet_client = DatasetClient(galaxy_instance)

        # Start to officially log into the results directory
        logger.info("")
        logger.info("Locating input files.  Searching the following directory (and child directories): \n%s%s", tab_formatter, args.input_dir)

        library_list_mapping = {}
        upload_list_mapping = {}
        for data in library_dataset_list:
            key, value = data.split(':')
            library_list_mapping[key] = value
        for data in upload_dataset_list:
            key, value = data.split(':')
            upload_list_mapping[key] = value

        upload_wf_input_files_list = _get_all_upload_files(args.input_dir, upload_list_mapping, config_parser)

        #files = _get_files(args.input_dir, read1_re)

        if len(upload_wf_input_files_list) == 0:
            logger.warning("Not able to find any input files looked in %s", args.input_dir)
        else:
            # Input files have been found, lets try to prep for running workflows.

            # First lets open up some output files.
            # Lets store these in the output directory for now.  Possibly move at a
            # later time?

            # All_Histories.json will store serialized history objets for every history (inlucding upload history)
            # such that history status script can be run and give status updates about all histories involved
            # for the batch.
            all_history_json = open(
                os.path.join(output_dir, "All_Histories.json"), "wb")

            all_histories = []

            try:
                workflow = workflow_client.show_workflow(workflow_id)
                workflow_input_keys = workflow['inputs'].keys()
            except ConnectionError:
                logger.error("Error in retrieving Workflow Information from Galaxy.  Please verfiy the workflow id and Galaxy URL provided in the configuration file.")
                return 5
            except ValueError:
                logger.error("Error in retrieving Workflow information from Galaxy.  Please verify your API Key is accurate.")
                return 6
            assert len(workflow_input_keys) == len(
                upload_dataset_list) + len(library_dataset_list)

            # Files have been found.  Lets create a common history and use it to
            # upload all inputs and all data library input files.  A new history
            # will be created for each sample workflow run to store only results.
            normalized_input_dir = args.input_dir
            if args.input_dir.endswith(os.path.sep):
                normalized_input_dir = normalized_input_dir[:-1]
            batch_name = os.path.basename(normalized_input_dir)
            logger.info("All input files will be uploaded/imported into the Galaxy history: %s", batch_name)
            upload_history = history_client.create_history(batch_name)
            upload_history['upload_history'] = True
            all_histories.append(upload_history)
            library_datasets = _import_library_datasets(history_client, upload_history, library_dataset_list, default_lib)

            wf_results = {}

            # Upload the files needed for the workflow
            for upload_wf_input_files_map in upload_wf_input_files_list:
                # upload the local files and launch a workflow.
                sample_name = _parse_sample_name(upload_wf_input_files_map.values()[0], file_name_re)
                logger.info("Preparing Galaxy to run a workflow for: %s", sample_name)
                logger.info("\tThe following input files will be uploaded: ")
                for wf_input_name in upload_wf_input_files_map.keys():
                    logger.info("\t%s => %s", wf_input_name, upload_wf_input_files_map[wf_input_name])

                new_post_wf_run = partial(_post_wf_run, all_histories=all_histories)
                result = pool.apply_async(_launch_workflow, args=[galaxy_host, api_key, workflow, upload_history, upload_wf_input_files_map, genome, library_list_mapping, library_datasets, sample_name, output_dir], callback=new_post_wf_run)
                wf_results[sample_name] = result

            #should be all done with processing.... this will block until all work is done
            pool.close()
            pool.join()

            # lets check the sucessfullness of the runs
            for sample in wf_results.keys():
                sample_result = wf_results[sample]
                if not sample_result.successful():
                    logger.error("WORKFLOW INITIATION ERROR! SAMPLE = %s", sample)
                    try:
                        sample_result.get()
                    except Exception as inst:
                        logger.error("Unexpected Error occurred: %s , %s ", type(inst), inst)
                        logger.exception(inst)

            all_history_json.write(json.dumps(all_histories))

            logger.info("")
            logger.info("Number of samples found: %s", str(len(upload_wf_input_files_list)))
            logger.info("Workflow, %s, has been launched for all samples.", workflow['name'])
            logger.info("You can view history status by invoking the following command:")
            logger.info("%s>> python history_status.py <my_result_dir> --ini <myConfiguration.ini>", tab_formatter)
            logger.info("")
            logger.info("A log of all samples processed and their inputs can be found here: %s", runlog_filename)
            logger.info("")

        return 0

    except Exception as inst:
        logger.error("Unexpected Error occurred: %s : %s", type(inst), inst.message)
        logger.exception(inst)
        return 23
    finally:
        if all_history_json is not None:
            try:
                all_history_json.flush()
                all_history_json.close()
            except Exception as inst:
                logger.error("ERROR saving the history log file: %s.  This may cause problems when trying to use history_utils.py.", all_history_json.name)
                logger.error("ERROR Information: type = %s, message = %s ", type(inst), inst)
        logging.shutdown()


# Disable Warnings. Without this a warning such as the following is generated:
# /Library/Python/2.7/site-packages/requests/packages/urllib3/connectionpool.py:734:
# InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
# See: https://urllib3.readthedocs.org/en/latest/security.html
# At some point we might want to allow this warning instead, or fix up the
# cause.
urllib3.disable_warnings()

CONFIG_GLOBALS = ["RUN_SUMMARY", "SAMPLE_RE", "BATCH_NAME"]
UPLOAD_TYPE_READ1 = "READ1"
UPLOAD_TYPE_READ2 = "READ2"
UPLOAD_TYPE_GLOBALS = [UPLOAD_TYPE_READ1, UPLOAD_TYPE_READ2]

LOGGER_NAME = 'workflow_runner'

if __name__ == "__main__":
    sys.exit(main())

