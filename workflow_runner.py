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
import copy


def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api


def parse_sample_name(file_path):
    sampleName = file_name_re.match(os.path.basename(file_path))
    if sampleName != None:
        return sampleName.group(1)
    else:
        return 'Unable_to_parse'


def setup_base_datamap():
    oto_wf_id = parser.get('Globals','oto_wf_id')
    dataset_list = parser.get('Globals', 'input_ids').split(',')
    list_mapping = {}
    for data in dataset_list:
            key,value = data.split(':')
            list_mapping[key]=value
    workflow = workflowClient.show_workflow(oto_wf_id)
    workflow_input_keys = workflow['inputs'].keys()
    assert len(workflow_input_keys) == len(dataset_list)
    dm = {}
    for w in workflow_input_keys:
        workflow_label = workflow['inputs'][w]['label']
        #find mapping if present
        if list_mapping.has_key(workflow_label):
            if not list_mapping[workflow_label].startswith('read'):
                data_set = dataSetClient.show_dataset(dataset_id=list_mapping[workflow_label],)
            else:
                data_set = {'id': list_mapping[workflow_label], 'name': list_mapping[workflow_label], 'hda_ldda': 'hda'}
        else:
            print "Workflow requesting '%s' unsure what to assign. Choices I have: %s" % (workflow_label, ",".join(list_mapping.keys()))
            labels = ""
            for x in workflow_input_keys:
                labels += "%s: ," %(workflow['inputs'][x]['label'],)
            print "Workflow would like the following inputs please adjust your config.ini file %s" % (labels)
            sys.exit(1)
        dm[w]={'id':data_set['id'],'src':data_set['hda_ldda']}
    return dm

def get_files(root_path):
    matches = []
    for root, dirnames, filenames in os.walk(root_path):
      for filename in fnmatch.filter(filenames, '*R1*fastq.gz'):
          matches.append(os.path.join(root, filename))
    return matches
parser = SafeConfigParser()

if len(sys.argv) == 2:
    if sys.argv[1].endswith('.ini'):
        parser.read(sys.argv[1])
    else:
        print "You passed %s I need a .ini file" %(sys.argv[1],)
        sys.exit(1)
else:
    parser.read('configuration.ini')
api_key = get_api_key(parser.get('Globals', 'api_file'))
galaxy_host = parser.get('Globals', 'galaxy_host')

file_name_re = re.compile(parser.get('Globals', 'sample_re'))



galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
historyClient = HistoryClient(galaxyInstance)
toolClient = ToolClient(galaxyInstance)
workflowClient = WorkflowClient(galaxyInstance)
dataSetClient = DatasetClient(galaxyInstance)

generic_data_map = setup_base_datamap()
files = get_files(parser.get('Globals','fastq_dir'))
if len(files) == 0:
        print "Not able to find any fastq files looked in %s" %(parser.get('Globals', 'fastq_dir'))
else:
    print "Found fastq files running workflow for the following files (R2's will be added)"
    print ",".join(files)
    files_to_keep = {}
    for R1 in files:
        input_dir_path = os.path.dirname(R1)
        data_map = copy.deepcopy(generic_data_map)
        R2 = R1.replace('R1','R2')
        if not os.path.exists(R1):
            print "%s File Not Found" % (R1, )
            raise Exception
        if not os.path.exists(R2):
            print "%s R2 file Not Found" % (R1, )
            raise Exception
        sampleName = parse_sample_name(R1)
        print "Running %s and %s with name %s" %(R1,R2,sampleName)
        history = historyClient.create_history(sampleName)
        print data_map
        R1 = toolClient.upload_file(R1, history['id'], file_type='fastqsanger',dbkey=parser.get('Globals','genome'))
        R2 = toolClient.upload_file(R2, history['id'], file_type='fastqsanger',dbkey=parser.get('Globals','genome'))

        for d in data_map.keys():
                if data_map[d]['id'] == 'read1':
                    data_map[d]['id'] = R1['outputs'][0]['id']
                elif data_map[d]['id'] == 'read2':
                    data_map[d]['id'] = R2['outputs'][0]['id']

        # Have files in place need to set up workflow
        # Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
        rep_params = {'SAMPLE_ID': sampleName}
        print sampleName
        params = {}
        rwf = workflowClient.run_workflow(parser.get('Globals','oto_wf_id'),
                                          data_map, params=params, history_id=history['id'],
                                          replacement_params=rep_params,import_inputs_to_history=True)
        for output in rwf['outputs']:
            data_set = dataSetClient.show_dataset(dataset_id=output,)
            if sampleName == data_set['name'].split('.')[0]:
                if not files_to_keep.has_key(input_dir_path):
                    files_to_keep[input_dir_path]=[]
                files_to_keep[input_dir_path].append(output)
timestr = time.strftime("%Y%m%d-%H%M%S")
fh = open(timestr, 'w')
for path in files_to_keep:
    for id in files_to_keep[path]:
        fh.write("%s:%s\n" % (path,id))
fh.close()
for path in files_to_keep:
    retrieved = []
    while len(retrieved) != len(files_to_keep[path]):
        for output in files_to_keep[path]:
            data_set = dataSetClient.show_dataset(dataset_id=output,)
            if data_set['state'] == 'ok':
                #download = dataSetClient.download_dataset(output,file_path=path,use_default_filename=True,wait_for_completion=True,maxwait=600)
                retrieved.append("test")
    time.sleep(120)
os.remove(timestr)