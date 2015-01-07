#!/usr/bin/python
__author__ = 'tbair,eablck'
import sys

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.libraries import LibraryClient
from bioblend.galaxy.workflows import WorkflowClient
from ConfigParser import SafeConfigParser

def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api

parser = SafeConfigParser()

if len(sys.argv) >= 2:
    if sys.argv[1].endswith('.ini'):
        parser.read(sys.argv[1])
    else:
        print "You passed %s I need a .ini file" %(sys.argv[1],)
        sys.exit(1)
else:
    parser.read('configuration.ini')

api_key = get_api_key(parser.get('Globals', 'api_file'))


galaxy_host = parser.get('Globals', 'galaxy_host')
galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
libraryInstance = LibraryClient(galaxyInstance)
libs = libraryInstance.get_libraries(name=parser.get('Globals','default_lib'))
details = libraryInstance.get_folders(library_id=libs[0]['id'])
folder = libraryInstance.show_library(library_id=libs[0]['id'],contents=True)
for f in folder[1:]:
    print "%s:%s" % (f['name'],f['id'])

workflowClient = WorkflowClient(galaxyInstance)
wf = workflowClient.get_workflows()
for w in wf:
    print w['id']
    print w['name']
    print w['owner']
    workflow = workflowClient.show_workflow(w['id'])
    workflow_input_keys = workflow['inputs'].keys()
    for wk in workflow_input_keys:
        print workflow['inputs'][wk]['label']
        print wk

