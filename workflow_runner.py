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
import glob


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
    workflow = workflowClient.show_workflow(oto_wf_id)
    workflow_input_keys = workflow['inputs'].keys()
    assert len(workflow_input_keys) == len(dataset_list)
    dm = {}
    for d,w in zip(dataset_list,workflow_input_keys):
        if not d.startswith('read'):
            data_set = dataSetClient.show_dataset(dataset_id=d)
        else:
            data_set = {'id': d, 'name': d, 'hda_ldda': 'hda'}
        print "Workflow requesting %s assigning %s:%s " % (workflow['inputs'][w]['label'], data_set['name'], data_set['id'])
        dm[w]={'id':data_set['id'],'src':data_set['hda_ldda']}
    return dm

def get_files(root_path):
    matches = []
    for root, dirnames, filenames in os.walk(root_path):
      for filename in fnmatch.filter(filenames, '*R1*fastq.gz'):
          matches.append(os.path.join(root, filename))
parser = SafeConfigParser()
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
    for R1 in files:
        data_map = generic_data_map
        R2 = R1.replace('R1','R2')
        if not os.path.exists(R1):
            print "%s File Not Found" % (R1, )
            raise Exception
        if not os.path.exists(R2):
            print "%s File Not Found" % (R1, )
            raise Exception
        sampleName = parse_sample_name(R1)
        print "Running %s and %s with name %s" %(R1,R2,sampleName)
        history = historyClient.create_history(sampleName)
        R1 = toolClient.upload_file(R1, history['id'], file_type='fastqsanger')
        R2 = toolClient.upload_file(R2, history['id'], file_type='fastqsanger')
        for d in data_map.keys():
                if data_map[d]['id'] == 'read1':
                    data_map[d]['id'] = R1['outputs'][0]['id']
                elif data_map[d]['id'] == 'read2':
                    data_map[d]['id'] = R2['outputs'][0]['id']

        # Have files in place need to set up workflow
        # Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow

        rep_params = {'SAMPLE_ID': sampleName}
        params = {}
        rwf = workflowClient.run_workflow(parser.get('Globals','oto_wf_id'),
                                          data_map, params=params, history_id=history['id'],
                                          replacement_params=rep_params)
        print rwf
