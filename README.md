This script is dependant on biobase just do a pip install biobase and that should work

Note that you must create and populate a file called api.key (unless you change the name in the configuration.ini file) The api.key file should have your galaxy api key first line, nothing else

Also in the configuration.ini file you will need to put the workflow id and an ordered list of datasets needed for the workflow.

The script will complain if the workflow is is asking for more inputs than you are giving, it will also parrot off what the workflow is requesting along with the name of the file to try and help match things up

To run script just call the script it will look in the directory to find all possible R1*.fastq.gz files, construct the likely R2 pair name and then run. It does not wait for completion.

The sample name is constructed by looking at the first part of the file name you can modify by changing the regular expression in the configuration.ini file

