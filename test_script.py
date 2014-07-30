#!/usr/bin/python
__author__ = 'tbair'

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.datasets import DatasetClient
import time
import sys
import os.path

#api_key = '293b01c35e9be1202446de4c0f4193b9' #galaxy.hpc api key
api_key = 'c4ec958ebfbb56a2148f264fb89af7d5 ' #neon-galaxy.hpc api key
#galaxy_host = 'https://galaxy.hpc.uiowa.edu' #real Galaxy
galaxy_host ='https://neon-galaxy.hpc.uiowa.edu:44300'
#oto_wf_id = 'c50592c2d5f1990d' #real oto workflow
oto_wf_id = 'eeba014900a7fb69' #neon oto workflow
platform_design = '4297a1e53d8b35a4'
known_variants = '0f299008fdceea36'
oto_custom_data = '792cdd29e54db84f'
report_template = 'f843e8d542196e75'
custom_quality_metrics = '38b6e00a3fe71a99'
R1 = sys.argv[1]
R2 = sys.argv[2]
try:
    sampleName = os.path.basename(R1).split('_')[0]
except:
    sampleName = 'Unable_to_parse'

galaxyInstance = GalaxyInstance(galaxy_host,key=api_key)
historyClient = HistoryClient(galaxyInstance)
toolClient = ToolClient(galaxyInstance)
workflowClient = WorkflowClient(galaxyInstance)
dataSetClient = DatasetClient(galaxyInstance)


workflow = workflowClient.show_workflow(oto_wf_id)
history = historyClient.create_history(sampleName)


R1 = toolClient.upload_file(R1,history['id'],file_type='fastqsanger')
R2 = toolClient.upload_file(R2,history['id'],file_type='fastqsanger')
#Have files in place need to set up workflow
# Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
print workflow['inputs']
#datamap  = {workflow['inputs'].keys()[0]: {'id': R1['outputs'][0]['id'], 'src': 'hda'}} #simple datamap for testing
datamap  = {workflow['inputs'].keys()[0]: {'id': platform_design, 'src': 'hda'},
            workflow['inputs'].keys()[1]: {'id': known_variants, 'src': 'hda'},
            workflow['inputs'].keys()[2]: {'id': R1['outputs'][0]['id'], 'src': 'hda'},
            workflow['inputs'].keys()[3]: {'id': oto_custom_data, 'src': 'hda'},
            workflow['inputs'].keys()[4]: {'id': R2['outputs'][0]['id'], 'src': 'hda'},
            workflow['inputs'].keys()[5]: {'id': custom_quality_metrics, 'src': 'hda'},
            }
params = {}
rep_params = {'SAMPLE_ID':sampleName}
rwf = workflowClient.run_workflow(workflow['id'],datamap,params=params,history_id=history['id'],replacement_params=rep_params)
print rwf
