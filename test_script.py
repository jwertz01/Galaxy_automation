#!/usr/bin/python
__author__ = 'tbair'

from bioblend.galaxy import GalaxyInstance
import time
import sys
import os.path

api_key = '293b01c35e9be1202446de4c0f4193b9'
galaxy_host = 'https://galaxy.hpc.uiowa.edu'
oto_wf_id = '7576cb7f105ca501'
oto_wf_id = 'c50592c2d5f1990d'
oto_data_library = '97fa6d9fa130e652'
platform_design = '808d5b008d7b5283'
known_variants = 'd631f0584da64e36'
oto_custom_data = '6cc40868de00e622'
report_template = '1492d625c8ec5538'
custom_quality_metrics = '682e7654ec211682'
R1 = sys.argv[1]
R2 = sys.argv[2]
sampleName = 'samName'

gi = GalaxyInstance(galaxy_host,key=api_key)
oto_wf = gi.workflows.get_workflows(oto_wf_id)
wf = gi.workflows.show_workflow(oto_wf_id)
#print wf['inputs']
# create folder for today
todays_folder = time.strftime("%d_%m_%Y")
todays_folder =  todays_folder #add a forward slash for galaxy
if len(gi.libraries.get_folders(library_id=oto_data_library, name= "/" + todays_folder)) == 0:
    gi.libraries.create_folder(library_id=oto_data_library, folder_name=todays_folder)
folder_id = gi.libraries.get_folders(library_id=oto_data_library, name="/" + todays_folder)
lib_contents = gi.libraries.show_library(library_id=oto_data_library ,contents=True)
for lc in lib_contents:
        if lc.has_key('name'):
            if lc['name'] == os.path.basename(R1):
                R1 = lc
            if lc['name'] == os.path.basename(R2):
                R2 = lc
if type(R1) != dict: # dict would indicate file already there
    R1 = gi.libraries.upload_file_from_local_path(library_id=oto_data_library, file_local_path=sys.argv[1], folder_id=folder_id[0]['id'], file_type='fastqsanger')
if type(R2) != dict: # dict would indicte file already there
    R2 = gi.libraries.upload_file_from_local_path(library_id=oto_data_library, file_local_path=sys.argv[2], folder_id=folder_id[0]['id'], file_type='fastqsanger')

#Have files in place need to set up workflow
# Based on example at http://bioblend.readthedocs.org/en/latest/api_docs/galaxy/docs.html#run-a-workflow
datamap = dict()
datamap['14080'] = {'src':'ld', 'id': platform_design}
datamap['14081'] = {'src':'ld','id': known_variants}
datamap['14082'] = {'src':'ld','id': R1[0]['id']}
datamap['14083'] = {'src':'ld','id': oto_custom_data}
datamap['14084'] = {'src':'ld','id': report_template}
datamap['14085'] = {'src':'ld','id': R2[0]['id']}
datamap['14086'] = {'src':'ld','id': custom_quality_metrics}

params = dict()
params['SAMPLE_ID']=sampleName
for wf_inputs in wf['inputs']:
    if datamap.has_key(wf_inputs.keys()[0]) == False:
        print "missing workflow parameter %s " %(wf_inputs,)

rwf = gi.workflows.run_workflow(workflow_id=oto_wf, history_name=sampleName, dataset_map=datamap, replacement_params=params)
print rwf
