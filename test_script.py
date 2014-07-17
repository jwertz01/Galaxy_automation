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

api_key = '293b01c35e9be1202446de4c0f4193b9' #galaxy.hpc api key
#api_key = '0157d09c8a28b689702443facb93369f' #neon-galaxy.hpc api key
galaxy_host = 'https://galaxy.hpc.uiowa.edu' #real Galaxy
#galaxy_host ='https://neon-galaxy.hpc.uiowa.edu:44302'
oto_wf_id = 'c50592c2d5f1990d' #real oto workflow
#oto_wf_id = '7576cb7f105ca501' #simple workflow for testing
platform_design = '808d5b008d7b5283'
known_variants = 'd631f0584da64e36'
oto_custom_data = '6cc40868de00e622'
report_template = '1492d625c8ec5538'
custom_quality_metrics = '682e7654ec211682'
R1 = sys.argv[1]
R2 = sys.argv[2]
sampleName = 'samName'

galaxyInstance = GalaxyInstance(galaxy_host,key=api_key)
historyClient = HistoryClient(galaxyInstance)
toolClient = ToolClient(galaxyInstance)
workflowClient = WorkflowClient(galaxyInstance)
dataSetClient = DatasetClient(galaxyInstance)


workflow = workflowClient.show_workflow(oto_wf_id)
history = historyClient.create_history(sampleName)


R1 = toolClient.upload_file(R1,history['id'],file_type='fastqsanger')
#R2 = toolClient.upload_file(R2,history['id'],file_type='fastqsanger')
#Have files in place need to set up workflow
# Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
#datamap = dict()
#datamap['14080'] = {'src':'ldda', 'id': platform_design}
#datamap['14081'] = {'src':'ldda','id': known_variants}
#datamap['14082'] = {'src':'ldda','id': R1['id']}
#datamap['14083'] = {'src':'ldda','id': oto_custom_data}
#datamap['14084'] = {'src':'ldda','id': report_template}
#datamap['14085'] = {'src':'ldda','id': R2['id']}
#datamap['14086'] = {'src':'ldda','id': custom_quality_metrics
print workflow['inputs']
#datamap  = {workflow['inputs'].keys()[0]: {'id': R1['outputs'][0]['id'], 'src': 'hda'}} #simple datamap for testing
datamap  = {workflow['inputs'].keys()[0]: {'id': platform_design, 'src': 'ld'},
            workflow['inputs'].keys()[1]: {'id': known_variants, 'src': 'ld'},
            workflow['inputs'].keys()[2]: {'id': R1['outputs'][0]['id'], 'src': 'hda'},
            workflow['inputs'].keys()[3]: {'id': oto_custom_data, 'src': 'ld'},
            workflow['inputs'].keys()[4]: {'id': R2['outputs'][0]['id'], 'src': 'hda'},
            workflow['inputs'].keys()[5]: {'id': custom_quality_metrics, 'src': 'ld'},
            }
params = {}
rep_params = {}
rwf = workflowClient.run_workflow(workflow['id'],datamap,params=params,history_id=history['id'],replacement_params=rep_params)
print rwf
