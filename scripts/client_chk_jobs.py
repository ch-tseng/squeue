from libCMDs import HOSTINFO
import subprocess
import os, time, sys
import shutil
from datetime import datetime

def update_jobfile():
    global update_time

    with open(in_running_path,'r') as f:
        job_content = f.read()

    now = datetime.now()
    exec_time = now.strftime("%Y/%m/%d %H:%M:%S")

    if process_id is None:
        job_content = job_content.replace('{execute_host}', host_info.hostname)
        job_content = job_content.replace('{execute_start_time}', exec_time)
        job_content = job_content.replace('{execute_status}', 'running')
        job_content = job_content.replace('{execute_log}', exec_log_path)

    else:
        job_content = job_content.replace('{process_id}', str(process_id))
        job_content = job_content.replace('{execute_live_time}', exec_time)

    with open(in_running_path,'w') as f:
        f.write(job_content)

    update_time = time.time()

def finish_jobfile():
    with open(in_running_path,'r') as f:
        job_content = f.read()

    now = datetime.now()
    f_time = now.strftime("%Y/%m/%d %H:%M:%S")
    job_content = job_content.replace('{execute_end_time}', f_time)
    job_content = job_content.replace('running', 'finished')

    with open(in_running_path,'w') as f:
        f.write(job_content)



host_info = HOSTINFO()

jobs = host_info.check_jobs()
if not len(jobs)>0:
    sys.exit(0)

process_id = None
update_time = time.time()
for job in jobs[:1]:
    [job_file, exe_path, owner, job_tags, notify_line, notify_email, notify_sms] = job

    job_id = job_file.split('.')[0]
    job_path = os.path.join( host_info.base_path, 'jobs', 'queue', job_file)
    in_running_path = os.path.join( host_info.base_path, 'jobs', 'running', job_file)
    finished_path = os.path.join( host_info.base_path, 'jobs', 'finished', job_file)
    failed_path = os.path.join( host_info.base_path, 'jobs', 'failed', job_file)
    exec_log_path = os.path.join( host_info.base_path, 'jobs', 'logs', job_id+'.log')

    shutil.move(job_path, in_running_path)
    update_jobfile()

    #run_bg(exe_path)
    #proc = subprocess.Popen("nohup {} >/dev/null 2>&1 &".format(exe_path), shell=True)
    proc = subprocess.Popen("{} >{} 2>&1".format(exe_path,exec_log_path), shell=True)
    while proc.poll() == None:
        if process_id is None:
            process_id = proc.pid
            update_jobfile()

        elif time.time()-update_time > 60:
            update_jobfile()

        time.sleep(1)

    finish_jobfile()
    shutil.move(in_running_path, finished_path)

    #將本job tag name 寫到jobs/logs/tag_finished 中, 通知該tag已結束
    if len(job_tags.strip())>0:
        now = datetime.now()
        f_time = now.strftime("%Y/%m/%d %H:%M:%S")
        with open( os.path.join(host_info.base_path, 'jobs', 'logs', 'tag_finished.info'),'a' ) as f:
            f.write("{}|{}|{} \n".format(job_tags, job_id, f_time))

