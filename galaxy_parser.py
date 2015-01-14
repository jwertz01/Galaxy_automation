#!/usr/bin/python
__author__ = 'tbair,eablck'
import sys

from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.libraries import LibraryClient
from bioblend.galaxy.workflows import WorkflowClient
from ConfigParser import SafeConfigParser
from requests.packages import urllib3


def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api

# Disable Warnings. Without this a warning such as the following is generated:
# /Library/Python/2.7/site-packages/requests/packages/urllib3/connectionpool.py:734:
# InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly advised.
# See: https://urllib3.readthedocs.org/en/latest/security.html
# At some point we might want to allow this warning instead, or fix up the
# cause.
urllib3.disable_warnings()

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

print ""
print "This Galaxy Parser Utility is for help in customizing 'library_input_ids' and 'workflow_id' section of the configuration ini file necessary to run Galaxy Automation."
print "It will look up the file names and their Galaxy IDs for the default_library configured in the configuration.ini file"
print "The Ids returned should be used to match the appropriate library files in Galaxy to the required Galaxy workflow inputs"
print ""
print "library_input_ids should be formatted as \'my_workflow_input_name:my_matching_library_file_galaxy_id\'"
print ""

workflowClient = WorkflowClient(galaxyInstance)
wf = workflowClient.get_workflows()
print ""
print "WORKFLOW INFORMATION:"
for w in wf:
    print "\tWORKFLOW_NAME => \'%s\' : WORKFLOW_ID => %s : WORKFLOW_OWNER => %s" % (w['name'], w['id'], w['owner'])
    print "\tWORKFLOW_INPUTS:"
    workflow = workflowClient.show_workflow(w['id'])
    workflow_input_keys = workflow['inputs'].keys()
    for wk in workflow_input_keys:
        print "\t\tINPUT_NAME => \'%s\' : INPUT_ID => %s" % (workflow['inputs'][wk]['label'], wk)
        
print ""
libs = libraryInstance.get_libraries(name=parser.get('Globals','default_lib'))
details = libraryInstance.get_folders(library_id=libs[0]['id'])
folder = libraryInstance.show_library(library_id=libs[0]['id'],contents=True)
print "LISTING ALL FILES (AND THEIR GALAXY IDs) FOR LIBRARY \'%s\':" % parser.get('Globals','default_lib')
for f in folder[1:]:
    print "\tGALAXY_LIBRARY_FILE_NAME => \'%s\':GALAXY_LIBRARY_FILE_ID => %s" % (f['name'],f['id'])

print ""
sys.exit()
