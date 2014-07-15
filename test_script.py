#!/usr/bin/python
__author__ = 'tbair'

from bioblend.galaxy import GalaxyInstance
import time
import sys

api_key = '293b01c35e9be1202446de4c0f4193b9'
galaxy_host = 'https://galaxy.hpc.uiowa.edu'
oto_wf_id = '7576cb7f105ca501'
oto_wf_id = 'c50592c2d5f1990d'
oto_data_library = '97fa6d9fa130e652'
gi = GalaxyInstance(galaxy_host,key=api_key)
oto_wf = gi.workflows.get_workflows(oto_wf_id)
wf = gi.workflows.show_workflow(oto_wf_id)
print wf['inputs']
# create folder for today
todays_folder = time.strftime("%d_%m_%Y")
todays_folder = "/" + todays_folder #add a forward slash for galaxy
if len(gi.libraries.get_folders(library_id=oto_data_library, name=todays_folder)) == 0:
    gi.libraries.create_folder(library_id=oto_data_library, folder_name=todays_folder)
folder_id = gi.libraries.get_folders(library_id=oto_data_library, name=todays_folder)
# folder_id[0]['id']
gi.libraries.upload_file_from_local_path(library_id=oto_data_library, file_local_path=sys.argv[1], folder_id=folder_id[0]['id'], file_type='fastqsanger')
