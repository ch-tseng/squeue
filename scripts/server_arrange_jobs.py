import os
from libQueue import SQ
from configparser import ConfigParser
from datetime import datetime
from datetime import timedelta
import shutil

cfg = ConfigParser()
cfg.read("config.ini",encoding="utf-8")
base_path = cfg.get('global', 'base_path')

def sort_value(d, index):
    return sorted(d.items(), key=lambda x: x[1][index], reverse=True)

qhost = SQ()
jobs_waiting = qhost.waiting_job_list()

with open( os.path.join(base_path, 'jobs', 'arrange.info'),'w' ) as f:
    f.write('')

for job in jobs_waiting:
    job_path = os.path.join(base_path, 'jobs', 'waiting', job )

    [submit_time, job_exec_path,when_to_execute,run_after_submit_min,esti_execute_hours,specified_time_run,want_finish_before_date, \
        specified_host, reqs_cpus, reqs_ram, reqs_gram] = \
        qhost.get_job_requirements(job_path, ['submit_time', 'job_path', 'when_to_execute', 'run_after_submit_min', 'esti_execute_hours', 'specified_time_run', \
        'want_finish_before_date', 'specified_host', 'reqs_cpus', 'reqs_ram', 'reqs_gram' ])

    if when_to_execute in [0,1,2]:
        if specified_time_run == 'none':
            real_time_job = submit_time + timedelta(minutes=run_after_submit_min)
        else:
            real_time_job = specified_time_run

    elif when_to_execute in [3]:  #指定由系統安排的, 先放在最晚看到結果時間, 往前推預估執行時間
        real_time_job = want_finish_before_date - timedelta(minutes=esti_execute_hours*60 )

    passed_seconds = datetime.now().timestamp() - real_time_job.timestamp()

    if passed_seconds>0:
        if specified_host != 'none':
            exec_hosts = [specified_host]

        else:
            exec_hosts = qhost.can_hosts_basic(job.split('.')[0])
            print('assign the job:', job_exec_path)
            print(exec_hosts)

        if(len(exec_hosts)>0):
            first_host = list(exec_hosts)[0]
            print('assign {} to launch.'.format(first_host))
            shutil.move(job_path, os.path.join(base_path, 'jobs', 'queue', first_host+'_'+job) )
        else:
            print('no hosts can run the job', job)

    else:
        desc_jobs = "{} will start to run on {} \n".format(job, real_time_job.strftime("%Y/%m/%d %H:%M:%S"))
        with open( os.path.join(base_path, 'jobs', 'arrange.info'),'a' ) as f:
            f.write(desc_jobs)
        #print('ignore the job:', job_exec_path)
