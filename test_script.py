#!/usr/bin/python
__author__ = 'tbair'

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.datasets import DatasetClient
import sys
import os.path
from ConfigParser import SafeConfigParser
import re


def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api


parser = SafeConfigParser()
parser.read('configuration.ini')
api_key = get_api_key(parser.get('Globals', 'api_file'))
galaxy_host = parser.get('Globals', 'galaxy_host')
oto_wf_id = parser.get('Globals', 'oto_wf_id')
platform_design = parser.get('Globals', 'platform_design')
known_variants = parser.get('Globals', 'known_variants')
oto_custom_data = parser.get('Globals', 'oto_custom_data')
report_template = parser.get('Globals', 'report_template')
custom_quality_metrics = parser.get('Globals', 'custom_quality_metrics')
file_name_re = re.compile(parser.get('Globals', 'sample_re'))
R1 = sys.argv[1]
R2 = sys.argv[2]

try:
    sampleName = file_name_re.match(os.path.basename(R1)).group(1)
except ArithmeticError:
    sampleName = 'Unable_to_parse'

galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
historyClient = HistoryClient(galaxyInstance)
toolClient = ToolClient(galaxyInstance)
workflowClient = WorkflowClient(galaxyInstance)
dataSetClient = DatasetClient(galaxyInstance)
workflow = workflowClient.show_workflow(oto_wf_id)
history = historyClient.create_history(sampleName)

R1 = toolClient.upload_file(R1, history['id'], file_type='fastqsanger')
R2 = toolClient.upload_file(R2, history['id'], file_type='fastqsanger')

# Have files in place need to set up workflow
# Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow

datamap = {
    workflow['inputs'].keys()[0]: {'id': custom_quality_metrics, 'src': 'hda'},
    workflow['inputs'].keys()[1]: {'id': report_template, 'src': 'hda'},
    workflow['inputs'].keys()[2]: {'id': R1['outputs'][0]['id'], 'src': 'hda'},
    workflow['inputs'].keys()[3]: {'id': R2['outputs'][0]['id'], 'src': 'hda'},
    workflow['inputs'].keys()[4]: {'id': oto_custom_data, 'src': 'hda'},
    workflow['inputs'].keys()[5]: {'id': known_variants, 'src': 'hda'},
    workflow['inputs'].keys()[6]: {'id': platform_design, 'src': 'hda'},
}
params = {}
rep_params = {'SAMPLE_ID': sampleName}
rwf = workflowClient.run_workflow(workflow['id'],
                                  datamap, params=params, history_id=history['id'],
                                  replacement_params=rep_params)
print rwf
