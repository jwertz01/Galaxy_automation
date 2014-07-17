
import sys
import os
from bioblend.galaxy import GalaxyInstance
from bioblend.galaxy.histories import HistoryClient
from bioblend.galaxy.tools import ToolClient
from bioblend.galaxy.workflows import WorkflowClient
from bioblend.galaxy.datasets import DatasetClient

#Execute workflow from the command line.
#Example calls:
#python run_workflow.py input output
#python run_workflow.py '/Users/pro/Documents/sandbox/data/reads/e_coli_1000.fq' '/Users/pro/Documents/sandbox/data/reads/ecoli.bed'

GALAXY_URL = 'https://neon-galaxy.hpc.uiowa.edu:44302'
API_KEY = '0157d09c8a28b689702443facb93369f'
WORKFLOW_ID = 'eeba014900a7fb69'
TOOL_ID_IN_GALAXY = 'Show beginning1'

def findDatasedIdByExtention(datasetClient, output, ext):
    id = ''
    for datasetId in output['outputs']:
        dataset = datasetClient.show_dataset(datasetId)
        if dataset['file_ext'] == ext:
            id = datasetId
            break
    return id
def downloadDataset(datasetClient, datasetId, outpath):
    if datasetId != '':
        datasetClient.download_dataset(datasetId, outpath, False, True)
    else:
        print 'Dataset id %s not found. Fail to download dataset to % s.' % (datasetId, outpath)

def main():
    try:
        input_path = sys.argv[1]
        output_path = sys.argv[2]

        galaxyInstance = GalaxyInstance(GALAXY_URL, key=API_KEY)
        historyClient = HistoryClient(galaxyInstance)
        toolClient = ToolClient(galaxyInstance)
        workflowClient = WorkflowClient(galaxyInstance)
        datasetClient = DatasetClient(galaxyInstance)

        history = historyClient.create_history('tmp')
        #uploadedFile = toolClient.upload_file(input_path, history['id'] )

        workflow = workflowClient.show_workflow(WORKFLOW_ID)
        dataset_map  = {workflow['inputs'].keys()[0]: {'id': 'd254ca9cd14fb3b5', 'src': 'ldda'}}
        params = {TOOL_ID_IN_GALAXY: {'param': 'reference_genome', 'value': 'hg19'}}
        output = workflowClient.run_workflow(WORKFLOW_ID, dataset_map, params, history['id'])

        downloadDataset(datasetClient, findDatasedIdByExtention(datasetClient, output, 'bed'), output_path)
        #delete history
        historyClient.delete_history(history['id'])
        #if galaxy instance support dataset purging
        #historyClient.delete_history(history['id'], True)

    except IndexError:
        print 'usage: %s key url workflow_id history step=src=dataset_id' % os.path.basename(sys.argv[0])
        sys.exit(1)

if __name__ == '__main__':
    main()