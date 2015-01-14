#!/usr/bin/python
__author__ = 'tbair,eablck'


from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.datasets import DatasetClient
from bioblend.galaxy.client import ConnectionError
from requests.packages import urllib3
from ConfigParser import SafeConfigParser
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


def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api


def parse_sample_name(file_path):
    sampleName = file_name_re.match(os.path.basename(file_path))
    if sampleName is not None:
        return sampleName.group(1)
    else:
        return 'Unable_to_parse'


def setup_base_datamap(current_history, run_history, r1_id, r2_id):
    dm = {}
    for w in workflow_input_keys:
        data_set = []
        global workflow_label
        workflow_label = workflow['inputs'][w]['label']
        # find mapping if present
        if workflow_label in library_list_mapping:
            global library_datasets
            if not workflow_label in known_globals:
                data_set = dataSetClient.show_dataset(
                    dataset_id=library_datasets[workflow_label]['id'])
        elif workflow_label in upload_list_mapping:
            # This would need to be augmented if there are other types of
            # uploads needed
            if upload_list_mapping[workflow_label].startswith('READ1'):
                data_set = {
                    'id': r1_id, 'name': upload_list_mapping[workflow_label], 'hda_ldda': 'hda'}
            elif upload_list_mapping[workflow_label].startswith('READ2'):
                data_set = {
                    'id': r2_id, 'name': upload_list_mapping[workflow_label], 'hda_ldda': 'hda'}
        else:
            print "Workflow requesting '%s' unsure what to assign. Choices I have: %s" % (workflow_label, ",".join(library_list_mapping.keys().join(upload_list_mapping)))
            labels = ""
            for x in workflow_input_keys:
                labels += "%s: ," % (workflow['inputs'][x]['label'],)
            print "Workflow would like the following inputs please adjust your config.ini file %s" % (labels)
            sys.exit(1)
        dm[w] = {'id': data_set['id'], 'src': data_set['hda_ldda']}
    return dm


def import_library_datasets(current_history):
    library_datasets = {}
    for data in library_dataset_list:
        key, value = data.split(':')
        library_file_dataset = historyClient.upload_dataset_from_library(
            current_history, value)
        # Lets make the datasets un-deleted. Unclear why they have them marked
        # as deleted when they import.
        status_code = historyClient.update_dataset(
            current_history, library_file_dataset['id'], deleted=False)
        library_datasets[key] = library_file_dataset
    return library_datasets


def getNotes():
    # return a string that describes this particular setup
    notes = []
    notes.append("Original Input Directory: " + sample_dir)
    notes.append("Results Directory: " + result_dir)
    notes.append("Upload History Name: " + upload_history['name'])
    notes.append("History Name: " + history['name'])

    notes.append(
        "Workflow Name (id): " + workflow['name'] + " (" + workflow_id + ")")
    notes.append("Workflow Runtime Inputs >> ")
    for w in workflow_input_keys:
        dataset_name = ""
        dataset_id = ""
        dataset_file = ""
        workflow_label = workflow['inputs'][w]['label']
        if workflow_label in library_list_mapping:
            if not workflow_label in known_globals:
                dataset_name = library_datasets[workflow_label]['name']
                dataset_id = library_datasets[workflow_label]['id']
                notes.append(
                    w + ": " + default_lib + os.path.sep + dataset_name + " (" + dataset_id + ")")
        elif workflow_label in upload_list_mapping:
            # This would need to be augmented if there are other types of
            # uploads needed
            if upload_list_mapping[workflow_label].startswith('READ1'):
                dataset = dataSetClient.show_dataset(READ1['id'])
                dataset_name = dataset['name']
                dataset_id = READ1['id']
                dataset_file = R1_file
            elif upload_list_mapping[workflow_label].startswith('READ2'):
                dataset = dataSetClient.show_dataset(READ2['id'])
                dataset_name = dataset['name']
                dataset_id = READ2['id']
                dataset_file = R2_file
            notes.append(
                w + ": " + dataset_name + " (" + dataset_id + ") " + dataset_file)
    return notes


def changePath(old_path):
    # get where to write and root
    new_path = old_path.replace(args.input_dir, output_dir)
    return new_path


def get_files(root_path):
    # Eventually might want to get more than just fastq files to upload.
    # This would require some type of alignment by sample id, or in a common directory,
    # or being passed in a fully qualified file name or someting else.
    # But for now....
    matches = []
    for root, dirnames, filenames in os.walk(root_path):
        for filename in fnmatch.filter(filenames, '*R1*fastq.gz'):
            matches.append(os.path.join(root, filename))
    return matches

##########################################################################
# Main Runner Logic Starts Here
##########################################################################
def main(args=None):
    if args is None:
        args = sys.argv
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

    args = arg_parser.parse_args(args)

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

    if args.output_dir.startswith('$'):
        output_dir = os.path.join(args.input_dir, "results")
    else:
        output_dir = args.output_dir

    if not os.path.exists(output_dir):
        print "Output directory %s not found. Creating ..." % (output_dir)
        os.makedirs(output_dir)


    # Get the configuration parmaters
    api_key = get_api_key(config_parser.get('Globals', 'api_file'))
    galaxy_host = config_parser.get('Globals', 'galaxy_host')
    file_name_re = re.compile(config_parser.get('Globals', 'sample_re'))
    library_input_ids = ''.join(ch for ch in config_parser.get('Globals', 'library_input_ids') if ch != '\n')
    library_dataset_list = library_input_ids.split(',')
    upload_input_ids = ''.join(ch for ch in config_parser.get('Globals', 'upload_input_ids') if ch != '\n')
    upload_dataset_list = upload_input_ids.split(',')
    genome = config_parser.get('Globals', 'genome')
    default_lib = config_parser.get('Globals', 'default_lib')
    workflow_id = config_parser.get('Globals', 'workflow_id')

    galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
    historyClient = HistoryClient(galaxyInstance)
    toolClient = ToolClient(galaxyInstance)
    workflowClient = WorkflowClient(galaxyInstance)
    dataSetClient = DatasetClient(galaxyInstance)

    workflow_label = ""

    print ""
    print "Locating fastq files.  Searching the following directory (and child directories): "
    print "\t" + args.input_dir
    files = get_files(args.input_dir)
    if len(files) == 0:
        print "Not able to find any fastq files looked in %s" % (args.input_dir)
    else:
        # Fastq files have been found, lets try to prep for running workflows.

        # First lets open up some output files.
        # Lets store these in the output directory for now.  Possibly move at a
        # later time?

        # Run log will contain a history of files uploaded and the workflow kicked
        # off (including parameters)
        run_log = open(os.path.join(output_dir, "GalaxyAutomationRun.log"), "wb")
        # Result_History.json will store serialized history objects for each sample's history that contains
        # workflow results specific to that history.  This will be used to
        # automate downloads.
        result_history_json = open(
            os.path.join(output_dir, "Result_Histories.json"), "wb")
        # All_Histories.json will store serialized history objets for every history (inlucding upload history)
        # such that history status script can be run and give status updates about all histories involved
        # for the batch.
        all_history_json = open(
            os.path.join(output_dir, "All_Histories.json"), "wb")

        result_histories = []
        all_histories = []

        library_list_mapping = {}
        upload_list_mapping = {}
        for data in library_dataset_list:
            key, value = data.split(':')
            library_list_mapping[key] = value
        for data in upload_dataset_list:
            key, value = data.split(':')
            upload_list_mapping[key] = value

        try:
            workflow = workflowClient.show_workflow(workflow_id)
            workflow_input_keys = workflow['inputs'].keys()
        except ConnectionError:
            print "Error in retrieving Workflow Information from Galaxy.  Please verfiy the workflow id and Galaxy URL provided in the configuration file."
            sys.exit()
        except ValueError:
            print "Error in retrieving Workflow information from Galaxy.  Please verify your API Key is accurate."
            sys.exit()
        assert len(workflow_input_keys) == len(
            upload_dataset_list) + len(library_dataset_list)

        # Files have been found.  Lets create a common history and use it to
        # upload all inputs and all data library input files.  A new history
        # will be created for each sample workflow run to store only results.
        batchName = os.path.basename(args.input_dir)
        upload_history = historyClient.create_history(batchName)
        upload_history['upload_history'] = True
        all_histories.append(upload_history)
        library_datasets = import_library_datasets(upload_history['id'])

        # Upload the files needed for the workflow, and kick it off
        print "Located " + str(len(files)) + " R1 fastq files for processing (will find pair R2 file):"
        print "\t" + "\n\t".join(files)
        files_to_keep = {}
        for R1_file in files:
            input_dir_path = os.path.dirname(R1_file) + os.path.sep
            R2_file = R1_file.replace('R1', 'R2')
            if not os.path.exists(R1_file):
                print "%s File Not Found" % (R1_file, )
                raise Exception
            if not os.path.exists(R2_file):
                print "%s R2 file Not Found" % (R1_file, )
                raise Exception
            sampleName = parse_sample_name(R1_file)
            sample_dir = os.path.basename(os.path.dirname(R1_file))
            result_dir = os.path.join(input_dir_path, "results")

            print ""
            print "Uploading Sample : " + sampleName
            print "\tR1 File: " + R1_file
            print "\tR2 File: " + R2_file

            history = historyClient.create_history(sampleName)
            history['upload_history'] = False
        
            all_histories.append(history)
            result_histories.append(history)
            R1 = toolClient.upload_file(
                R1_file, upload_history['id'], file_type='fastqsanger', dbkey=genome)
            R2 = toolClient.upload_file(
                R2_file, upload_history['id'], file_type='fastqsanger', dbkey=genome)
            READ1 = R1['outputs'][0]
            READ2 = R2['outputs'][0]

            data_map = setup_base_datamap(
                upload_history['id'], history['id'], R1['outputs'][0]['id'], R2['outputs'][0]['id'])

            # Have files in place need to set up workflow
            # Based on example at
            # http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
            notes = getNotes()
            print ""
            print "Launching Workflow: "
            print "\t" + "\n\t".join(notes)
            workflow_notes = ",".join(notes)
            rep_params = {
                'SAMPLE_ID': sampleName, 'WORKFLOW_NOTES': workflow_notes}
    #   Keeping the following lines of code commented.  It might be in future versions of Galaxy this is how
    #   nested parameters need to be set.  See forum: https://github.com/afgane/bioblend/issues/71
    #   Until then, while running our hoemgrown patched Galaxy runtime, we will use the json mechanism to show nested parameters.
    #        params = {'toolshed.g2.bx.psu.edu/repos/devteam/bwa_wrappers/bwa_wrapper/1.2.3': {'params|readGroup|rgid': sampleName,
    #                                                                                          'params|readGroup|rglb': sampleName,
    #                                                                                          'params|readGroup|rgsm': sampleName},
    #                  'annotation_v2_wrapper': {'input_notes': workflow_notes}}
            params = {'toolshed.g2.bx.psu.edu/repos/devteam/bwa_wrappers/bwa_wrapper/1.2.3': {'params': {'readGroup': {'rgid': sampleName,
                                                                                                                       'rglb': sampleName,
                                                                                                                       'rgsm': sampleName}}},
                      'annotation_v2_wrapper': {'report_selector': {'input_notes': workflow_notes}}}
            rwf = workflowClient.run_workflow(workflow_id,
                                              data_map,
                                              params=params,
                                              history_id=history['id'],
                                              replacement_params=rep_params,
                                              import_inputs_to_history=False)
            run_log.write("Workflow Automation for Sample: " + sampleName + "\n")
            for n in notes:
                run_log.write("\t" + n + "\n")
            for output in rwf['outputs']:
                data_set = dataSetClient.show_dataset(dataset_id=output,)
                if sampleName == data_set['name'].split('.')[0]:
                    if not input_dir_path in files_to_keep:
                        files_to_keep[input_dir_path] = []
                    files_to_keep[input_dir_path].append(output)

    all_history_json.write(json.dumps(all_histories))
    result_history_json.write(json.dumps(result_histories))
    try:
        result_history_json.flush()
        result_history_json.close()
    except Exception as inst:
        print "ERROR saving the history log file: %s.  This may cause problems when trying to use history_utils.py." % (result_history_json.name)
        print "ERROR Information: type = %s, message = %s " % (type(inst), inst)

    try:
        all_history_json.flush()
        all_history_json.close()
    except Exception as inst:
        print "ERROR saving the history log file: %s.  This may cause problems when trying to use history_utils.py." % (all_history_json.name)
        print "ERROR Information: type = %s, message = %s " % (type(inst), inst)

    try:
        run_log.flush()
        run_log.close()
    except Exception as inst:
        print "ERROR saving the run log file: %s.  The Run log file contains a log of the workflow automation steps taken." % (run_log.name)
        print "ERROR Information: type = %s, message = %s " % (type(inst), inst)

    print ""
    print "Number of samples found: " + str(len(files))
    print "Workflow, " + workflow_label + ", has been launched for all samples."
    print "You can view history status by invoking the following command:"
    print "\t>> python history_status.py <myConfiguration.ini>"
    print ""
    print "A log of all samples processed and their inputs can be found here:" + str(run_log)
    print ""

# Disable Warnings. Without this a warning such as the following is generated:
# /Library/Python/2.7/site-packages/requests/packages/urllib3/connectionpool.py:734:
# InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
# See: https://urllib3.readthedocs.org/en/latest/security.html
# At some point we might want to allow this warning instead, or fix up the
# cause.
urllib3.disable_warnings()

known_globals = ["RUN_SUMMARY", "SAMPLE_RE", "BATCH_NAME"]

if __name__ == "__main__":
    sys.exit(main())

