
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
import sys
import os
import fnmatch
import os.path
from ConfigParser import SafeConfigParser
import re
import time
import json
from requests.packages import urllib3


def get_api_key(file_name):
    fh = open(file_name)
    api = fh.readline().strip('\n')
    return api

############################################################################################
# Main Runner Logic Starts Here
############################################################################################
urllib3.disable_warnings()

parser = SafeConfigParser()

if len(sys.argv) == 2:
    if sys.argv[1].endswith('.ini'):
        parser.read(sys.argv[1])
    else:
        print "You passed %s I need a .ini file" % (sys.argv[1],)
        sys.exit(1)
else:
    parser.read('configuration.ini')

# Get the configuration parmaters
api_key = get_api_key(parser.get('Globals', 'api_file'))
galaxy_host = parser.get('Globals', 'galaxy_host')
output_dir = parser.get('Globals', 'output_dir')


# Test - write out all histories, then load
galaxyInstance = GalaxyInstance(galaxy_host, key=api_key)
historyClient = HistoryClient(galaxyInstance)
histories = historyClient.get_histories()

# histories_json = open(os.path.join(output_dir, "Histories_test.json"), "wb")
# histories_json.write(json.dumps(histories))
# histories_json.flush()
# histories_json.close()

# Lets load up all the histories



all_history_json = open(os.path.join(output_dir, "All_Histories.json"), "rb")
histories = json.load(all_history_json)

for h in histories:
	print h['id'] 
	print historyClient.get_status(h['id'])

sys.exit()



