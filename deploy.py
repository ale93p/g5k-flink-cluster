import subprocess
from os import environ
from sys import stdout, stderr
from configparser import ConfigParser
from shlex import split as commandSplit
from shutil import copyfile
from time import sleep


### Read configuration file ###

config = ConfigParser()
config.read('cluster.conf')

g5kConfig = config['g5k']
deployImg = str(g5kConfig['deploy.image.name'])
userName = str(g5kConfig['user.name'])
oarFile = str(g5kConfig['oar.file.location'])
multiCluster = str(g5kConfig['multi.cluster']) in "yes"

flinkConfig = config['flink']
flinkVersion = str(flinkConfig['flink.version'])

ansibleConfig = config['ansible']
inventoryPath = str(ansibleConfig['inventory.file.path'])
playbookPath = str(ansibleConfig['playbook.file.path'])

### Variables ###

flinkConfYaml = 'flink-conf.yaml'
slavesConf = 'slaves'
mastersConf = 'masters'

### Obtain cluster's nodes list ###
print("OARFILE:",oarFile)
if oarFile == 'default':
    try:
        oarFile = environ.get('OAR_NODE_FILE')
    except KeyError:
        print("ERROR: nodefile not found")
        exit()
else:
    oarFile = oarFile.replace('~', environ.get('HOME'))

print("OARFILE:",oarFile)
with open(oarFile) as file:
    clusterNodes = [line.strip() for line in file]

clusterNodes = list(set(clusterNodes))
nodesNbr = len(clusterNodes)


print("Your cluster is composed by {} nodes: {}".format(nodesNbr, clusterNodes))

### Deploy image through kadeploy in g5k ###

kadeployCommad = 'kadeploy3 -f {} -a {}.env -k'.format(oarFile, deployImg)
if multiCluster:
    kadeployCommad = kadeployCommad + " --multi-server"
print(kadeployCommad)
kadeployArgs = commandSplit(kadeployCommad)
kadeployProcess = subprocess.Popen(kadeployArgs, stderr=stderr, stdout=stdout)
kadeployProcess.communicate()

### Create ansible host file ###

with open(inventoryPath, 'w') as inventoryFile:
    inventoryFile.write('[all:vars]\n')
    inventoryFile.write('flink_dir="~/flink-{}/"\n'.format(flinkVersion))
    inventoryFile.write('flink_bin_dir="~/flink-{}/bin/"\n'.format(flinkVersion))
    inventoryFile.write('flink_conf_dir="~/flink-{}/conf/"\n'.format(flinkVersion))
    inventoryFile.write('\n')
    inventoryFile.write('[jobmanager]\n')
    inventoryFile.write(str(clusterNodes[0]) + '\n')
    inventoryFile.write('\n')
    inventoryFile.write('[taskmanager]\n')
    for i in range(1, nodesNbr):
        inventoryFile.write(str(clusterNodes[i]) + '\n')

with open(mastersConf, 'w') as masterFile:
    masterFile.write('{}:8081'.format(clusterNodes[0]))

with open(slavesConf, 'w') as slavesFile:
    for i in range(1, nodesNbr):
        slavesFile.write("{}\n".format(clusterNodes[i]))

### Create flink configuration file to be deployed on host ###

with open(flinkConfYaml, 'r') as flinkConf:
    flinkConfData = flinkConf.readlines()

for i in range(0, len(flinkConfData)):
    if flinkConfData[i].find('jobmanager.rpc.address') >= 0:
        print(flinkConfData[i].find('jobmanager.rpc.address'), flinkConfData[i])
        flinkConfData[i] = 'jobmanager.rpc.address: {}\n'.format(clusterNodes[0])
        print(i, flinkConfData[i])
        break

with open(flinkConfYaml, 'w') as flinkConf:
    flinkConf.writelines(flinkConfData)

### Copy nimbus configurations locally ###

copyfile(flinkConfYaml, environ.get('HOME') + '/flink-{}/conf/flink-conf.yaml'.format(flinkVersion)) # this will allow deploying topologies from g5k frontend


### Run ansible playbook ###

ansibleCommand = environ.get('HOME') + '/.local/bin/ansible-playbook -i {} {} --extra-vars "flink_yaml={} masters={} slaves={}"'.format(inventoryPath, playbookPath, flinkConfYaml, mastersConf, slavesConf)

print(ansibleCommand)
ansibleArgs = commandSplit(ansibleCommand)
ansibleProcess = subprocess.Popen(ansibleArgs, stderr=stderr, stdout=stdout)
ansibleProcess.communicate()

### Print ssh tunnel to run on host
print()
print()
print("**** ssh tunnel ***")
print("ssh {}@access.grid5000.fr -N -L8081:{}:8081".format(userName ,clusterNodes[0]))
