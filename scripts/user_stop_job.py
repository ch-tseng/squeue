from datetime import datetime
import os, sys
from libQueue import SQ

def job_name_info(job):
    tmp = job.replace('.job','')
    infos = tmp.split('_')
    host = infos[0]
    job_id = infos[-1:][0]
    gpu_id = 'none'

    if(len(infos)>2):
        gpu_id = infos[1].replace('gpu','')

    job_path = os.path.join(qhost.base_path, 'jobs', 'running', job)
    paras = qhost.get_job_requirements(job_path, keyPara=['owner','execute_start_time','job_path'])

    return (host,job_id,gpu_id,paras[0],paras[1],paras[2])

qhost = SQ()
runnings = qhost.running_job_list()
print("There are {} jobs in running:".format(len(runnings)))
print('')
print("{:20s} {:20s} {:8s} {:12s} {:20s} {:20s}".format('job_id','exec_host','gpu_id', 'owner', 'start_time', 'job_path' ))
print("{:20s} {:20s} {:8s} {:12s} {:20s} {:20s}".format('----------','----------','------', '----------','-----------------','---------------------------------------------------'))

job_running = {}
for x in runnings:
    (exechost,jobid,gpuid,job_owner,job_start_time, job_path) = job_name_info(x)
    start_time = job_start_time.strftime("%Y/%m/%d %H:%M:%S")
    print("{:20s} {:20s} {:8s} {:12s} {:20s} {:20s}".format(jobid, exechost, gpuid, job_owner, start_time,  job_path ))
    if qhost.username == job_owner:
        job_running.update( {jobid:x} )

jid = input('please keyin the Job ID you want to stop: '.format(len(runnings)))

if jid not in job_running:
    print("job {} is not in running or not submitted by you, cannot stop it.".format(jid))
    sys.exit()

job_path = os.path.join(qhost.base_path, 'jobs', 'running', job_running[jid])
with open(job_path, 'r') as f:
    file = f.read()

print('')
if '[False]' in file:
    print('The job {} will be stop, please wait.'.format(jid))
    file = file.replace('[False]', '[True]')

    with open(job_path, 'w') as f:
        f.write(file)

else:
    if '[True]' in file:
        print('The job {} has beed set to stop?'.format(jid))
    else:
        print("There is no stop function tag in the job file, please inform MIS.")
