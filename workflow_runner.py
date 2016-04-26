#!/usr/bin/python
__author__ = 'tbair,eablck,jwertz'

# Needs python 2.7 or higher for functionality

from automation_functions import *

class MaxLevelFilter(logging.Filter):
    '''
    Logging Filter for seperating messages between stdout and stderr
    Filters (lets through) all messages with level <= LEVEL
    '''
    def __init__(self, level):
        self.level = level

    def filter(self, record):
        return record.levelno <= self.level

def _get_api_key(file_name):
    '''
    Reads the api key from file and returns

    :type file_path: string
    :param file_path: the qualified path name of the file containing the api key

    :return string: the user api key string to use when connecting to galaxy instance

    '''
    file_handle = open(file_name)
    api = file_handle.readline().strip('\n')
    return api

def _parse_sample_name(file_path, file_name_re):
    '''
    Strips a sample name out of a file name

    :type file_path: string
    :param file_path: the full file name to strip sample name out of

    :type file_name_re: re Regular Expression Object (compiled via re.compile() from a regular expression pattern)
    :param file_name_re: The regular expression object used to pull out the sample name from the first group found.

    :return string: return the sample name or 'Unable_to_parse' if it is unable to retrieve a sample name from the file

    '''

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

    :type library_datasets: list of Galaxy DataSet JSON objects
    :param library_datasets: a list of the library datasets that were imported into the upload history

    :type upload_dataset_map: dict of String:Galaxy DataSet JSON objects
    :param upload_dataset_map: a map/dict of workflow input names to files/datasets that should have been uploaded from local filesystem

    :return: a dataset map depicting the mapping of workflow inputs to galaxy dataset ids, formatted as needed bys
             bioblend WorkflowClient run_workflow
    '''
    datamap = {}
    for w_input in workflow['inputs'].keys():
        data_set = []
        workflow_label = workflow['inputs'][w_input]['label']
        # find mapping if present
        if workflow_label in library_list_mapping:
            if workflow_label not in UPLOAD_TYPE_GLOBALS:
                data_set = {'id': library_datasets[workflow_label]['id'], 'hda_ldda': library_datasets[workflow_label]['hda_ldda']}
        elif workflow_label in upload_dataset_map:
            data_set = {'id': upload_dataset_map[workflow_label]['id'], 'hda_ldda': 'hda'}
        else:
            labels = ""
            for wf_input in workflow['inputs'].keys():
                labels += "%s, " % (workflow['inputs'][wf_input]['label'],)
            raise RuntimeError("Workflow requesting \'%s\' unsure what to assign. Choices I have: %s. Workflow would like the following inputs please adjust your configuration.ini file: (%s). WorkflowConfiguration Problem - unknown requested input. Adjust and validate configuration.ini file." % (workflow_label, ",".join(library_list_mapping.keys() + upload_dataset_map.keys()), labels))
        datamap[w_input] = {'id': data_set['id'], 'src': data_set['hda_ldda']}
    return datamap

def _import_library_datasets(history_client, upload_history, library_dataset_list, default_lib):
    '''
    Import a set of datasets from a Galaxy shared library into a specified Galaxy history

    :type history_client: bioblend.HistoryClient
    :param history_client: the Galaxy history client object to use for taking operations on Galaxy histories remotely

    :type upload_history: the Galaxy history to import the library datasets into
    :param upload_history: Galaxy JSON history object - expected to have an 'id' attribute

    :type library_dataset_list: list
    :param library_dataset_list: a list of of <workflow_input_name>:<library_dataset_galaxy_id> tuples
        read from the configuration ini.  The library dataset galaxy ids are used to identify, and locate the
        library dataset files to import into the upload_history

    :type default_lib: string
    :param default_lib: the default Galaxy Shared Data library as configured in the config.ini.  This is
        only used for outputting log messages.  It is assumed the library dataset ids specfied are located in
        this Galaxy shared library.

    :return dict: a map of workflow intput names (key) to galaxy DataSet JSON objects (value) returned from
        the Galaxy HistoryClient from the dataset import to the history from the library.

    '''
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
    '''
    Builds up a set of audit log messages about the automated workflow run for this specific sample.
    The notes are used in the logging of workflow automation run logs as well as what gets passed into
    the workflow if RUN_SUMMARY is specified in the configuration.ini file

    :type history: bioblend Galaxy History JSON object returned from HistoryClient during history creation
    :param history: Galaxy history JSON object (generated and returned during history creation through HistoryClient)

    :type workflow: bioblend Galaxy Workflow JSON object
    :param workflow: Galaxy worfklow JSON object (genearted and returned during WorkflowClient.show_workflow)

    :type library_list_mapping: dict of String to Galaxy DataSet JSON objects
    :param library_list_mapping: a map of workflow input names to galaxy library dataset JSON objects returned during HistoryClient upload

    :type library_datasets: list of Galaxy DataSet JSON objects
    :param library_datasets: a list of the library datasets that were imported into the upload history

    :type upload_dataset_map: dict of String:Galaxy DataSet JSON objects
    :param upload_dataset_map: a map/dict of workflow input names to files/datasets that should have been uploaded from local filesystem

    :type upload_input_files_map: dict of String:FilePaths
    :param upload_input_files_map: a map/dict of workflow input names to file paths of locally uploaded workflow inputs from files

    :return list of strings.  Each item in the notes is a different audit message about the workflow run.

    '''
    # return a string that describes this particular setup
    notes = []
    notes.append("Original Input Directory: " + history['sample_dir'])
    notes.append("Batch Results Directory: " + history['result_dir'])
    notes.append("Sample Results Directory: " + history['sample_result_dir'])
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
            if workflow_label not in CONFIG_GLOBALS:
                dataset_name = library_datasets[workflow_label]['name']
                dataset_id = library_datasets[workflow_label]['id']
                dataset_lib = library_datasets[workflow_label]['library_name']
                notes.append(
                    workflow_label + "(" + wf_input + ") => " + dataset_lib + os.path.sep + dataset_name + " (" + dataset_id + ")")
        elif workflow_label in upload_dataset_map:
            # This would need to be augmented if there are other types of
            # uploads needed
            dataset = upload_dataset_map[workflow_label]
            if upload_input_files_map:
                dataset_file = upload_input_files_map[workflow_label]
            else:
                dataset_file = ''
            dataset_name = dataset['name']
            dataset_id = dataset['id']
            notes.append(
                workflow_label + "(" + wf_input + ") => " + dataset_name + " (" + dataset_id + ") " + dataset_file)
    return notes

def _get_all_upload_files(root_path, upload_list_mapping, config_parser, upload_protocol, galaxy_instance):
    '''
    Locates all the files that should be uploaded and groups them together according to workflow inputs.

    :type: root_path: str
    :param root_path: The root input directory as specified as a main argument

    :type: upload_list_mapping: dict
    :param: upload_list_mapping: a mapping of workflow input names to the local file patterns that should be found and uploaded to Galaxy

    :type: config_parser: SafeConfigParser
    :param config_parser: The configuration parser for loading configuration from the ini file

    :type: upload_protocol: string
    :param upload_protocol: whether to upload files via HTTP or FTP

    :type: galaxy_instance: bioblend.galaxy.GalaxyInstance
    :param galaxy_instance: A base representation of an instance of Galaxy, identified by a URL and a user API key

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
                file_list = _get_files(root_path, config_parser.get('Globals', 'READ1_re'), upload_protocol, galaxy_instance)
                read1_file_list = file_list
            elif upload_type == UPLOAD_TYPE_READ2:
                if read1_file_list is None:
                    raise RuntimeError("Cound not find any READ1 files.  If READ2 files are configured as workflow inputs (in config.ini), the READ1 inputs must be listed first in the upload_input_ids.")
                else:
                    file_list = []
                    read2_file_list = _get_files(root_path, re.sub('R1', 'R2', config_parser.get('Globals', 'READ1_re')), upload_protocol, galaxy_instance)
                    for read1_file in read1_file_list:
                        r2_file = _get_r2_file(read1_file, config_parser, upload_protocol, read1_file_list + read2_file_list)
                        file_list.append(r2_file)

        else:
            # assume it is a regular expression - generic
            file_list = _get_files(root_path, upload_type, upload_protocol, galaxy_instance)

        for input_file in file_list:
            index = file_list.index(input_file)

            try:
                wf_upload_input_file_map = upload_file_tuple_list[index]
            except IndexError:
                wf_upload_input_file_map = collections.OrderedDict()
                upload_file_tuple_list.append(wf_upload_input_file_map)
            wf_upload_input_file_map[upload_input_name] = input_file

    return upload_file_tuple_list

def _get_r2_file(r1_file, config_parser, upload_protocol, file_list):
    '''
    Given the forward read fastq file name, compute what the reverse read fastq file name should be.

    :type: r1_file: String - qualified file name
    :param r1_file: The fully qualified forward read file name to use to base the reverse read file name from

    :type: config_parser: SafeConfigParser
    :param config_parser: The configuration parser for loading configuration from the ini file

    :type: upload_protocol: string
    :param upload_protocol: whether to upload files via HTTP or FTP

    :type: file_list: list
    :param file_list: list of R1 and R2 files in local or FTP directory (depending on upload_protocol)

    :return String - fully qualified file name for the reverse read file
    '''
    read1_sub_re = config_parser.get('Globals', 'READ1_sub_re')
    read2_sub_re = config_parser.get('Globals', 'READ2_sub_re')

    r1_sub_pattern = re.compile(read1_sub_re, re.IGNORECASE)
    r2_file = r1_sub_pattern.sub(read2_sub_re, r1_file)

    if upload_protocol == "http":
        if not os.path.exists(r1_file):
            raise RuntimeError("%s R1 file Not Found" % r1_file)
        if not os.path.exists(r2_file):
            raise RuntimeError("%s R2 file Not Found" % r1_file)
    else:  # ftp
        if r1_file not in file_list:
            raise RuntimeError("%s R1 file Not Found" % r1_file)
        if r2_file not in file_list:
            raise RuntimeError("%s R2 file Not Found" % r1_file)

    return r2_file

def _get_files(root_path, file_match_re, upload_protocol, galaxy_instance):
    '''
    Traverse all files under the root directory (including sub-directories) and build a list
    of files whose name match the specified compiled regular expression

    :type: root_path: str
    :param root_path: The root input directory as specified as a main argument

    :type file_match_re: re Regular Expression Object (compiled via re.compile() from a regular expression pattern)
    :param file_match_re: The regular expression object used to match filenames to find.

    :type: upload_protocol: string
    :param upload_protocol: whether to upload files via HTTP or FTP

    :type: galaxy_instance: bioblend.galaxy.GalaxyInstance
    :param galaxy_instance: A base representation of an instance of Galaxy, identified by a URL and a user API key

    :return list of file names that match the regular expression under the root directory

    '''
    # Eventually might want to get more than just fastq files to upload.
    # This would require some type of alignment by sample id, or in a common directory,
    # or being passed in a fully qualified file name or someting else.
    # But for now....
    matches = []
    if upload_protocol == "http":
        for root, dirnames, filenames in os.walk(root_path, followlinks=True):
            for filename in fnmatch.filter(filenames, file_match_re):
                matches.append(os.path.join(root, filename))
    else:  # ftp
        ftp_client = FTPFilesClient(galaxy_instance)
        filenames = [z['path'] for z in ftp_client.get_ftp_files()]
        matches = fnmatch.filter(filenames, file_match_re)
    return matches

def _post_wf_run(history, all_histories):
    '''
    This is a callback method which gets invoked when _launch_workflow completes successfully.
    It will log information into the global workflow_run log file and also add the resulting
    history JSON object to the all_histories list for serialization to All_Histories.json file.

    '''
    logger = logging.getLogger(LOGGER_NAME)
    logger.info("Workflow launch successful for sample: %s" % history['sample_name'])
    logger.info("\tDetails of the workflow invocation:")
    notes = history['notes']
    for note in notes:
        logger.info("\t\t%s" % note)

    all_histories.append(history)

def _launch_workflow(galaxy_host, api_key, workflow, upload_history, upload_input_files_map, genome, library_list_mapping, library_datasets, sample_name, result_dir, upload_protocol, retry_failed, failed_sample_to_run):
    '''
    Launches a workflow in Galaxy.  Assumed that this function is thread safe - can be run leveraging multiprocessing python logic asyncronously.

    :type: galaxy_host: string
    :param galaxy_host: The connection URL/address to the galaxy server as configured in the configuration.ini file

    :type: api_key: string
    :param api_key: user Galaxy api string for user authentication in Galaxy communication

    :type workflow: bioblend Galaxy Workflow JSON object
    :param workflow: Galaxy worfklow JSON object (genearted and returned during WorkflowClient.show_workflow)

    :type: upload_history: bioblend Galaxy History JSON object
    :param upload_history: Galaxy history JSON object (generated and returned during HistoryClient.create_history)

    :type upload_input_files_map: dict of String:FilePaths
    :param upload_input_files_map: a map/dict of workflow input names to file paths of locally uploaded workflow inputs from files

    :type: genome: string
    :param genome: the dbkey (genome) the input files are associated with and will be set when the files are uploaded through the ToolClient

    :type library_list_mapping: dict of String to Galaxy DataSet JSON objects
    :param library_list_mapping: a map of workflow input names to galaxy library dataset JSON objects returned during HistoryClient upload

    :type library_datasets: list of Galaxy DataSet JSON objects
    :param library_datasets: a list of the library datasets that were imported into the upload history

    :type: sample_name: string
    :param sample_name: The sample name

    :type: result_dir: string
    :param result_dir: The directory to place results into (log files and serialized JSON history objects)

    :type: upload_protocol: string
    :param upload_protocol: whether to upload files via HTTP or FTP

    :return bioblend History JSON object which will hold all the workflow results for the history.
            The history JSON object will also be augmented with the following additional data:

            key: upload_history
            value: whether or not this was an upload history (vs. a results history)

            key: sample_name
            value: the name of the sample ran

            key: sample_dir
            value: the directory name/path where the sample input files were locally found and uploaded from

            key: upload_history_name
            value: the name of the history containing the uploaded files for this workflow run

            key: result_dir
            value: the parent directory which contains the results (logs, json) for this batch run

            key: sample_result_dir
            value: the sample level directory which contains the log for this specific sample run

    '''
    # Create a log file specific for this sample
    sample_result_dir = os.path.join(result_dir, sample_name)
    if not os.path.exists(sample_result_dir):
        os.makedirs(sample_result_dir)

    # Clean up any existing file handlers (this is due to multi-processor threads)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    tab_formatter = '\t\t\t\t'
    runlog_filename = os.path.join(sample_result_dir, sample_name + "_Workflow_Runner.log")
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s',
                    datefmt='%m-%d %H:%M',
                    filename=runlog_filename,
                    filemode='wb')

    local_logger = logging.getLogger("workflow_runner_" + sample_name)

    try:
        galaxy_instance = GalaxyInstance(galaxy_host, key=api_key)
        history_client = HistoryClient(galaxy_instance)
        tool_client = ToolClient(galaxy_instance)
        workflow_client = WorkflowClient(galaxy_instance)

        if upload_input_files_map is not None:
            sample_dir = os.path.dirname(upload_input_files_map.values()[0])
        else:
            sample_dir = ''

        local_logger.info("Launching workflow for sample: %s" % sample_name)
        history = history_client.create_history(sample_name)
        # Update the history with annotation and tags
        annotation_str = "Generated by workflow automation. Workflow name = %s. Upload history= %s" % (workflow['name'], upload_history['name'])
        status_code = history_client.update_history(history['id'], annotation=annotation_str, tags=[upload_history['name']])
        local_logger.info("Updated History. Added annotation (%s) and tags (%s).  Return code: %s" % (annotation_str, upload_history['name'], str(status_code)))
        history['upload_history'] = False
        history['sample_name'] = sample_name
        history['sample_dir'] = sample_dir
        history['upload_history_name'] = upload_history['name']
        history['result_dir'] = result_dir
        history['sample_result_dir'] = sample_result_dir
        history['library_datasets'] = library_datasets

        # upload all the files
        if not retry_failed:
            local_logger.info("Uploading input files for workflow: %s" % workflow['name'])
            upload_dataset_map = {}
            for wf_inputname in upload_input_files_map.keys():
                upload_file = upload_input_files_map[wf_inputname]
                if upload_protocol == "http":
                    input_upload_out = tool_client.upload_file(
                        upload_file, upload_history['id'], file_type='fastqsanger', dbkey=genome)
                else:
                    input_upload_out = tool_client.upload_from_ftp(
                        upload_file, upload_history['id'], file_type='fastqsanger', dbkey=genome)
                local_logger.info("\tUploaded: %s => %s" % (wf_inputname, upload_file))
                input_dataset = input_upload_out['outputs'][0]
                upload_dataset_map[wf_inputname] = input_dataset

            data_map = _setup_base_datamap(
                workflow, library_list_mapping, library_datasets, upload_dataset_map)
        else:
            data_map = failed_sample_to_run.history['input_map']
            upload_dataset_map = failed_sample_to_run.history['upload_dataset_map']

        history['input_map'] = data_map
        history['upload_dataset_map'] = upload_dataset_map

        # Have files in place need to set up workflow
        # Based on example at
        # http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
        notes = _get_notes(history, workflow, library_list_mapping, library_datasets, upload_dataset_map, upload_input_files_map)
        local_logger.info("Details of the workflow invocation:")
        for note in notes:
            local_logger.info("\t%s" % note)
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
                  'annotation_v2_wrapper': {'report_selector': {'input_notes': workflow['name']}}}
        rwf = workflow_client.run_workflow(workflow['id'],
                                          data_map,
                                          params=params,
                                          history_id=history['id'],
                                          replacement_params=rep_params,
                                          import_inputs_to_history=False)
        local_logger.info("Workflow has been initiated, the resulting history object will be logged in All_Histories.json.")
    except Exception as inst:
        local_logger.error("Unexpected Error occurred: %s : %s : %s" % (inst.__class__.__name__, inst.args, inst.message))
        local_logger.exception(inst)
        # Wrappering exception to make sure the exception is pickable.  HTTPError would cause UnpickleableErrors and hang process workers during pool join
        error_msg = "Error (type: %s) occurred when processing Sample, %s.  Review sample log file for more information: %s" % (type(inst), sample_name, runlog_filename)
        raise RuntimeError(error_msg)
    finally:
        logging.shutdown()

    return history

def _get_argparser():
    '''
    Configure an argument parser to process any arguments passed into the main method.

    :return: Configured parsrer for processing arguments supplied to main method (for example from command line) :argparse.ArgumentParser: argparse ArgumentParser object
    '''
    arg_parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                         # usage='%(prog)s [OPTIONS]',
                                         description=textwrap.dedent('''\
                                                    Automatically launch Galaxy workflows, including uploading input files
                                                    and importing of additional input files from Galaxy shared data libraries.

                                                    Each workflow ran will have resulting datasets sent to its own Galaxy history.

                                                    This tool will output a run log for each workflow launched, in addition to a history log
                                                    (All_Histories.json) that will be used by history_utils.py for taking additional
                                                    actions on the generated result histories from each workflow launch.

                                                    Galaxy connection and workflow configuration information is loaded from configuration.ini file.
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
    arg_parser.add_argument('input_dir', help="directory which contains the workflow input files (subfolders will be traversed) that need to be uploaded if uploading over http.  Batch name to use if uploading from ftp. if --retry_failed, this the results directory containing All_Histories.json from a previous automation run.")
    arg_parser.add_argument('-o', '--output_dir', help="directory to save logs into as well as history log file (All_Histories.json, used by the history_utils.py tool) Recommended to also be a directory you would like to download workflow results into. Directory will be created if it does not exist. Default will be $INPUT_DIR/results", default='$INPUT_DIR/results')
    arg_parser.add_argument('-i', '--ini', help="configuration ini file to load", default='configuration.ini')
    arg_parser.add_argument('-r', '--retry_failed', action="store_true", help="whether to re-run a failed workflow instead of running new files", default=False)
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
        raise RuntimeError("The configuration ini file must end with .ini, the file specified was %s" % (args.ini))

    return config_parser


##########################################################################
# Main Runner Logic Starts Here
##########################################################################
def main(argv=None):

    # Build log file.
    # We will have a main logger to system out
    # A global log file
    # And a log per each sample in the sample directory
    # configure console logging

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)
    no_date_formatter = logging.Formatter('%(levelname)s\t%(name)s\t%(message)s')
    date_formatter = logging.Formatter('%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s')
    tab_formatter = '\t\t\t\t'

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.addFilter(MaxLevelFilter(logging.INFO))
    stdout_handler.setFormatter(no_date_formatter)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.WARNING)
    stderr_handler.setFormatter(no_date_formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    arg_parser = _get_argparser()
    args = arg_parser.parse_args(argv)

    try:
        config_parser = _parse_ini(args)
    except RuntimeError as inst:
        logger.error(inst.message)
        logger.info("HINT: How to run the workflow_runner tool:")
        arg_parser.print_usage()
        return 1

    # Default output_dir is $INPUT_DIR/results
    if args.output_dir.startswith('$'):
        if args.input_dir:
            output_dir = os.path.join(args.input_dir, "results")
        else:
            output_dir = "results"
    else:
        output_dir = args.output_dir

    if not os.path.exists(output_dir):
        logger.info("Output directory %s not found. Creating ..." % output_dir)
        os.makedirs(output_dir)

    # Create a new run log file.
    runlog_filename = os.path.join(output_dir, "Workflow_Runner.log")
    runlog_handler = logging.FileHandler(runlog_filename, mode="wb")
    runlog_handler.setFormatter(date_formatter)
    runlog_handler.setLevel(logging.DEBUG)
    logger.info("Logging to : %s" % runlog_filename)
    logger.addHandler(runlog_handler)

    # This will be the serialized history record - to be created when files are found.
    all_history_json = None

    try:

        # Get the configuration parmaters
        api_key = _get_api_key(config_parser.get('Globals', 'api_file'))
        galaxy_host = config_parser.get('Globals', 'galaxy_host')
        file_name_re = re.compile(config_parser.get('Globals', 'sample_re'))
        library_input_ids = config_parser.get('Globals', 'library_input_ids').replace('\n', '')
        library_dataset_list = library_input_ids.split(',')
        upload_input_ids = config_parser.get('Globals', 'upload_input_ids').replace('\n', '')
        upload_dataset_list = upload_input_ids.split(',')
        genome = config_parser.get('Globals', 'genome')
        default_lib = config_parser.get('Globals', 'default_lib')
        workflow_id = config_parser.get('Globals', 'workflow_id')
        num_processes = config_parser.get('Globals', 'num_processes')
        upload_protocol = config_parser.get('Globals', 'upload_protocol').lower()

        # Check upload protocol
        if not args.retry_failed:
            accepted_protocols = ["http", "ftp"]
            default_protocol = "http"
            if not upload_protocol:
                upload_protocol = default_protocol
                logger.info("Upload protocol not specified. Using default: %s" % default_protocol)
            if upload_protocol not in accepted_protocols:
                logger.error("Unrecognized upload protocol: %s. Please specify one of the following values: %s" % (upload_protocol, ', '.join(accepted_protocols)))
                return 7
            if upload_protocol == "http" and args.input_dir is None:
                arg_parser.print_usage()
                logger.error("Input directory argument required if upload protocol is http.")
                return 8
            if upload_protocol == "ftp" and args.input_dir is None:
                logger.error("Batch name must be specified if upload protocol is ftp. (Batch name is used to name the upload Galaxy history and tag all result histories for easy lookup.)")
                return 9

        pool = Pool(processes=int(num_processes))

        galaxy_instance = GalaxyInstance(galaxy_host, key=api_key)
        history_client = HistoryClient(galaxy_instance)
        workflow_client = WorkflowClient(galaxy_instance)

        failed_samples_to_run = []  # list of histories
        if args.retry_failed:
            histories = read_all_histories(args.input_dir, logger)
            (
                all_successful, all_running, all_failed, all_except, all_waiting,
                upload_history
            ) = get_history_status(histories, history_client, logger)
            if not all_failed:
                logger.error("Could not find any failed directories to retry.")
                return 10
            failed_samples_to_run = all_failed
            logger.info("Found %d failed samples to rerun: %s" % (len(failed_samples_to_run), ', '.join([z.history['name'] for z in failed_samples_to_run])))

        # Start to officially log into the results directory
        logger.info("")
        input_dir = args.input_dir if (upload_protocol == "http") else "[FTP directory] " + args.input_dir
        logger.info("Locating input files.  Searching the following directory (and child directories): \n%s%s" % (tab_formatter, input_dir))
        # Put library and upload config into dicts
        library_list_mapping = {}  # {filename: ID}
        upload_list_mapping = {}          # {upload input name: upload type}
        # upload input name is just a label ("R1 FastQ" or "R2 Fastq") to use as dict key
        # upload type is READ1 or READ2 or other regex. READ1 and READ2 correspond to READ*_re in config
        for data in library_dataset_list:
            key, value = data.split(':')
            library_list_mapping[key] = value
        for data in upload_dataset_list:
            key, value = data.split(':')
            upload_list_mapping[key] = value

        if not args.retry_failed:
            upload_wf_input_files_list = _get_all_upload_files(args.input_dir, upload_list_mapping, config_parser, upload_protocol, galaxy_instance)

        # files = _get_files(args.input_dir, read1_re)
        if not args.retry_failed and len(upload_wf_input_files_list) == 0:
            if upload_protocol == "http":
                logger.warning("Not able to find any input files. Looked in %s" % args.input_dir)
            else:
                # ftp
                logger.warning("Not able to find any input files uploaded via FTP.")
        else:
            # Input files have been found, lets try to prep for running workflows.

            # First lets open up some output files.
            # Lets store these in the output directory for now.  Possibly move at a
            # later time?

            failed_samples = open(
                os.path.join(output_dir, "Failed_Sample_Workflows.out"), "wb")

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
            if not args.retry_failed:
                if upload_protocol == "http":
                    dir_arg = args.input_dir
                else:
                    sample_names = []
                    for upload_wf_input_files_map in upload_wf_input_files_list:
                        sample_name = _parse_sample_name(upload_wf_input_files_map.values()[0], file_name_re)
                        sample_names.append(sample_name)
                    dir_arg = "|".join(sample_names)

                normalized_input_dir = dir_arg
                if dir_arg.endswith(os.path.sep):
                    normalized_input_dir = normalized_input_dir[:-1]

                if args.input_dir is None:
                    batch_name = "Input_files:%s" % os.path.basename(normalized_input_dir)
                elif upload_protocol == "http":
                    batch_name = os.path.basename(normalized_input_dir)
                else:
                    batch_name = args.input_dir

                logger.info("All input files will be uploaded/imported into the Galaxy history: %s" % batch_name)
                upload_history = history_client.create_history(batch_name)
                upload_history['upload_history'] = True
                all_histories.append(upload_history)
                library_datasets = _import_library_datasets(history_client, upload_history, library_dataset_list, default_lib)

            wf_results = {}
            # Upload the files needed for the workflow
            if not args.retry_failed:
                for upload_wf_input_files_map in upload_wf_input_files_list:
                    # upload the local files and launch a workflow.
                    sample_name = _parse_sample_name(upload_wf_input_files_map.values()[0], file_name_re)
                    logger.info("Preparing Galaxy to run a workflow for: %s" % sample_name)
                    logger.info("\tThe following input files will be uploaded: ")
                    for wf_input_name in upload_wf_input_files_map.keys():
                        logger.info("\t%s => %s" % (wf_input_name, upload_wf_input_files_map[wf_input_name]))

                    new_post_wf_run = partial(_post_wf_run, all_histories=all_histories)
                    result = pool.apply_async(_launch_workflow, args=[galaxy_host, api_key, workflow, upload_history, upload_wf_input_files_map, genome, library_list_mapping, library_datasets, sample_name, output_dir, upload_protocol, args.retry_failed, None], callback=new_post_wf_run)
                    wf_results[sample_name] = result
            else:
                for s in failed_samples_to_run:
                    upload_history = s.history
                    sample_name = "%s_retry" % s.history['name']
                    input_map = s.history['input_map']
                    library_datasets = s.history['library_datasets']
                    orig_input_dir = [z for z in s.history['notes'] if ('Original Input Directory:' in z)]
                    orig_input_dir = orig_input_dir[0].replace('Original Input Directory:', '').strip()
                    upload_wf_input_files_list = _get_all_upload_files(orig_input_dir, upload_list_mapping, config_parser, upload_protocol, galaxy_instance)
                    if upload_wf_input_files_list:
                        upload_wf_input_files_map = [z for z in upload_wf_input_files_list if os.path.basename(z[upload_dataset_list[0].split(':')[0]]).startswith('%s_' % s.history['name'])]
                        assert len(upload_wf_input_files_map) == 1
                        upload_wf_input_files_map = upload_wf_input_files_map[0]
                    else:
                        upload_wf_input_files_map = None
                    new_post_wf_run = partial(_post_wf_run, all_histories=all_histories)
                    result = pool.apply_async(_launch_workflow, args=[galaxy_host, api_key, workflow, upload_history, upload_wf_input_files_map, genome, library_list_mapping, library_datasets, sample_name, output_dir, upload_protocol, args.retry_failed, s], callback=new_post_wf_run)
                    wf_results[sample_name] = result
            # should be all done with processing.... this will block until all work is done
            pool.close()
            pool.join()

            # lets check the sucessfullness of the runs
            for sample in wf_results.keys():
                sample_result = wf_results[sample]
                if not sample_result.successful():
                    logger.error("WORKFLOW INITIATION ERROR! SAMPLE = %s" % sample)
                    try:
                        sample_result.get()
                    except Exception as inst:
                        # Write it out to failed log - need for retry.
                        failed_samples.write(sample + "\n")
                        logger.error("Unexpected Error occurred: %s , %s " % (type(inst), inst))
                        logger.exception(inst)

            all_history_json.write(json.dumps(all_histories))

            logger.info("")
            logger.info("Number of samples found: %s" % str(len(upload_wf_input_files_list)))
            logger.info("Workflow, %s, has been launched for all samples." % workflow['name'])
            logger.info("You can view history status by invoking the following command:")
            logger.info("%s>> python history_utils.py <my_result_dir> check_status --ini <myConfiguration.ini>" % tab_formatter)
            logger.info("")
            logger.info("A log of all samples processed and their inputs can be found here: %s" % runlog_filename)
            logger.info("")

        return 0

    except Exception as inst:
        logger.error("Unexpected Error occurred: %s : %s" % (type(inst), inst.message))
        logger.exception(inst)
        return 23
    finally:
        if all_history_json is not None:
            try:
                all_history_json.flush()
                all_history_json.close()
                failed_samples.flush()
                failed_samples.close()
            except Exception as inst:
                logger.error("ERROR saving the history log file: %s.  This may cause problems when trying to use history_utils.py." % all_history_json.name)
                logger.error("ERROR Information: type = %s, message = %s " % (type(inst), inst))
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
