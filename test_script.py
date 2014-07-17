#!/usr/bin/python
__author__ = 'tbair'

from bioblend.galaxy import GalaxyInstance, HistoryClient, ToolClient, WorkflowClient, DatasetClient
import time
import sys
import os.path

#api_key = '293b01c35e9be1202446de4c0f4193b9' #galaxy.hpc api key
api_key = '0157d09c8a28b689702443facb93369f' #neon-galaxy.hpc api key
#galaxy_host = 'https://galaxy.hpc.uiowa.edu' #real Galaxy
galaxy_host ='https://neon-galaxy.hpc.uiowa.edu:44302'
#oto_wf_id = 'c50592c2d5f1990d' #real oto workflow
oto_wf_id = 'eeba014900a7fb69'
#oto_data_library = '97fa6d9fa130e652' #tbair lib on galaxy.hpc
oto_data_library = 'd254ca9cd14fb3b5'
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


oto_wf = gi.workflows.get_workflows(oto_wf_id)
history = historyClient(sampleName)
workflow = workflowClient.show_workflow(oto_wf_id)


# create folder for today
todays_folder = time.strftime("%d_%m_%Y")
todays_folder =  todays_folder #add a forward slash for galaxy
if len(dataSetClient. get_folders(library_id=oto_data_library, name= "/" + todays_folder)) == 0:
    gi.libraries.create_folder(library_id=oto_data_library, folder_name=todays_folder)
folder_id = gi.libraries.get_folders(library_id=oto_data_library, name="/" + todays_folder)
lib_contents = gi.libraries.show_library(library_id=oto_data_library ,contents=True)

# galaxy_r1_name = os.path.basename(R1) # uncomment for unix
assert isinstance(R1, str)
galaxy_r1_name = R1.split("\\")[-1]
galaxy_r1_name = galaxy_r1_name.replace('.gz','')
# galaxy_r2_name = os.path.basename(R2) # uncomment for unix
assert isinstance(R2, str)
galaxy_r2_name = R2.split("\\")[-1]
galaxy_r2_name = galaxy_r1_name.replace('.gz','')

for lc in lib_contents:
        if lc.has_key('name'):
            if galaxy_r1_name == os.path.basename(lc['name']):
                R1 = lc
            if galaxy_r2_name == os.path.basename(lc['name']):
                R2 = lc
if  type(R1) == str: # dict would indicate file already there
    print "R1 file not already found ...uploading"
    R1 = gi.libraries.upload_file_from_local_path(library_id=oto_data_library, file_local_path=R1, folder_id=folder_id[0]['id'], file_type='fastqsanger')[0]
if type(R2) == str: # dict would indicate file already there
    print "R2 file not already found...uploading"
    R2 = gi.libraries.upload_file_from_local_path(library_id=oto_data_library, file_local_path=R2, folder_id=folder_id[0]['id'], file_type='fastqsanger')[0]

#Have files in place need to set up workflow
# Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
datamap = dict()
datamap['14080'] = {'src':'ldda', 'id': platform_design}
datamap['14081'] = {'src':'ldda','id': known_variants}
datamap['14082'] = {'src':'ldda','id': R1['id']}
datamap['14083'] = {'src':'ldda','id': oto_custom_data}
datamap['14084'] = {'src':'ldda','id': report_template}
datamap['14085'] = {'src':'ldda','id': R2['id']}
datamap['14086'] = {'src':'ldda','id': custom_quality_metrics}
datamap = {wf['inputs'].keys()[0]: {'id': R1['id'], 'src':'hda'}}
params = {'Show beginning1':{'param':'lineNum','value':10}}
print gi.url
for wf_inputs in wf['inputs']:
    if not datamap.has_key(wf_inputs):
        print "missing workflow parameter %s " %(wf_inputs,)
# move data into workflow
print gi.workflows.export_workflow_json(wf['id'])
mv_dataset = gi.histories.upload_dataset_from_library(history_id=history['id'], lib_dataset_id=R1['id'])
rwf = gi.workflows.run_workflow(oto_wf, datamap, history_id=history['id'],params=params)
print rwf
