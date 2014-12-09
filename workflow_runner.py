#!/usr/bin/python
__author__ = 'tbair'


from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.datasets import DatasetClient
import sys
import os
import fnmatch
import os.path
from ConfigParser import SafeConfigParser
import re
import time
import json


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
 #   oto_wf_id = parser.get('Globals', 'oto_wf_id')
 #   library_dataset_list = parser.get('Globals', 'library_input_ids').split(',')
 #   upload_dataset_list = parser.get('Globals', 'upload_input_ids').split(',')
 #   library_list_mapping = {}
 #   upload_list_mapping = {}
 #   for data in library_dataset_list:
 #       key, value = data.split(':')
 #       library_list_mapping[key] = value
 #   for data in upload_dataset_list:
 #       key, value = data.split(':')
 #       upload_list_mapping[key] = value
 #   workflow = workflowClient.show_workflow(oto_wf_id)
 #   workflow_input_keys = workflow['inputs'].keys()
 #   assert len(workflow_input_keys) == len(upload_dataset_list) + len(library_dataset_list)

    dm = {}
    for w in workflow_input_keys:
        data_set = []
        workflow_label = workflow['inputs'][w]['label']
        # find mapping if present
        if workflow_label in library_list_mapping:
            global library_datasets
            if not workflow_label in known_globals:
                data_set = dataSetClient.show_dataset(dataset_id=library_datasets[key]['id'])
        elif workflow_label in upload_list_mapping:
            # This would need to be augmented if there are other types of uploads needed
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
#        if list_mapping.has_key(workflow_label):
#            if not list_mapping[workflow_label].startswith('read'):
#                prep = historyClient.upload_dataset_from_library(
#                    current_history, list_mapping[workflow_label])
#                data_set = dataSetClient.show_dataset(dataset_id=prep['id'])
#            elif list_mapping[workflow_label].startswith('read1'):
#                data_set = {
#                    'id': r1_id, 'name': list_mapping[workflow_label], 'hda_ldda': 'hda'}
#            elif list_mapping[workflow_label].startswith('read2'):
#                data_set = {
#                    'id': r2_id, 'name': list_mapping[workflow_label], 'hda_ldda': 'hda'}
#            else:
#                print "mapping error "
#                sys.exit(1)
#        else:
#            print "Workflow requesting '%s' unsure what to assign. Choices I have: %s" % (workflow_label, ",".join(list_mapping.keys()))
#            labels = ""
#            for x in workflow_input_keys:
#                labels += "%s: ," % (workflow['inputs'][x]['label'],)
#            print "Workflow would like the following inputs please adjust your config.ini file %s" % (labels)
#            sys.exit(1)
        dm[w] = {'id': data_set['id'], 'src': data_set['hda_ldda']}
    return dm


def import_library_datasets(current_history):
    library_datasets = {}
    for data in library_dataset_list:
        key, value = data.split(':')
        library_file_dataset = historyClient.upload_dataset_from_library(
            current_history, value)
        library_datasets[key] = library_file_dataset
    return library_datasets


def getNotes():
    # return a string that describes this particular setup
    notes = []
    notes.append("Original Input Directory: " + sample_dir)
    notes.append("Results Directory: " + result_dir)
    notes.append("Upload History Name: " + upload_history['name'])
    notes.append("History Name: " + upload_history['name'])

    notes.append("Workflow Name (id): " + workflow['name'] + " (" + oto_wf_id + ")")
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
                notes.append(w + ": " + default_lib + os.path.sep + dataset_name + " (" + dataset_id + ")")
        elif workflow_label in upload_list_mapping:
            # This would need to be augmented if there are other types of uploads needed
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
            notes.append(w + " : " + dataset_name + " (" + dataset_id + ") " + dataset_file)

#    files = []
#    for i in inputs:
#        name, file = i.split(':')
#        files.append(file)
#    notes.append("Files:" + ",".join(files))
#    notes.append("Workflow_id:" + oto_wf_id)
#    #workflow = workflowClient.show_workflow(oto_wf_id)
#    # notes.append(",".join(workflow))
#    return ":".join(notes)
    return notes


def changePath(old_path):
    # get where to write and root
    new_path = old_path.replace(fastq_root, output_dir)
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


# Main Runner Logic Starts Here

known_globals = ["RUN_SUMMARY", "SAMPLE_RE"]

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
file_name_re = re.compile(parser.get('Globals', 'SAMPLE_RE'))
library_dataset_list = parser.get('Globals', 'library_input_ids').split(',')
upload_dataset_list = parser.get('Globals', 'upload_input_ids').split(',')
fastq_root = parser.get('Globals', 'fastq_dir')
output_dir = parser.get('Globals', 'output_dir')
genome = parser.get('Globals', 'genome')
default_lib = parser.get('Globals', 'default_lib')
oto_wf_id = parser.get('Globals', 'oto_wf_id')


galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
historyClient = HistoryClient(galaxyInstance)
toolClient = ToolClient(galaxyInstance)
workflowClient = WorkflowClient(galaxyInstance)
dataSetClient = DatasetClient(galaxyInstance)

files = get_files(fastq_root)
if len(files) == 0:
    print "Not able to find any fastq files looked in %s" % (fastq_root)
else:

    # Files have been found.  Lets create a common history and use it to
    # upload.

    # First lets open up some output files.
    # Lets store these in the output directory for now.  Possibly move at a later time?

    run_log = open(os.path.join(output_dir, "GalaxyAutomation.log"), "wb")
    history_json = open(os.path.join(output_dir, "Histories.json"), "wb")

    library_list_mapping = {}
    upload_list_mapping = {}
    for data in library_dataset_list:
        key, value = data.split(':')
        library_list_mapping[key] = value
    for data in upload_dataset_list:
        key, value = data.split(':')
        upload_list_mapping[key] = value
    workflow = workflowClient.show_workflow(oto_wf_id)
    workflow_input_keys = workflow['inputs'].keys()
    assert len(workflow_input_keys) == len(upload_dataset_list) + len(library_dataset_list)

    batchName = os.path.basename(fastq_root)
    upload_history = historyClient.create_history(batchName)
    library_datasets = import_library_datasets(upload_history)

    # Upload the files needed for the workflow, and kick it off
    print "Found fastq files running workflow for the following files (R2's will be added)"
    print ",".join(files)
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

        print "Running %s and %s with name %s" % (R1_file, R2_file, sampleName)
        history = historyClient.create_history(sampleName)
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
        print "\n".join(notes)
        rep_params = {'SAMPLE_ID': sampleName, 'WORKFLOW_NOTES': ",".join(notes)}
        print sampleName
        params = {}
        print data_map
        rwf = workflowClient.run_workflow(oto_wf_id,
                                          data_map, params=params, history_id=history[
                                              'id'],
                                          replacement_params=rep_params,
                                          import_inputs_to_history=False)
        history_json.write(json.dumps(history))
        run_log.write("Workflow Automation for Sample: " + "sampleName" + "\n")
        for n in notes:
            run_log.write("\t" + n + "\n")
        for output in rwf['outputs']:
            data_set = dataSetClient.show_dataset(dataset_id=output,)
            if sampleName == data_set['name'].split('.')[0]:
                if not input_dir_path in files_to_keep:
                    files_to_keep[input_dir_path] = []
                files_to_keep[input_dir_path].append(output)
sys.exit()  # put in to cope with the lack of the download api working
timestr = time.strftime("%Y%m%d-%H%M%S")
fh = open(timestr, 'w')
for path in files_to_keep:
    for id in files_to_keep[path]:
        fh.write("%s:%s\n" % (path, id))
fh.close()
for path in files_to_keep:
    retrieved = []
    while len(retrieved) != len(files_to_keep[path]):
        for output in files_to_keep[path]:
            data_set = dataSetClient.show_dataset(dataset_id=output,)
            if data_set['state'] == 'ok':
                new_path = changePath(path)
                new_path = os.path.join(new_path, data_set['name'])
                print data_set
                print new_path
                try:
                    download = dataSetClient.download_dataset(
                        output, file_path=new_path, use_default_filename=False, wait_for_completion=True, maxwait=600)
                    print download
                    retrieved.append(data_set['id'])
                except Exception as e:
                    print "%s" % (e,)
    time.sleep(120)
os.remove(timestr)
