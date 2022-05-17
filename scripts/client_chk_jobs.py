from libCMDs import HOSTINFO
import subprocess
import os, time, sys
import shutil
from configparser import ConfigParser
from datetime import datetime
from libLineAPI import LINEBOT

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
        stop_running = ""

    else:
        cfg = ConfigParser()
        cfg.read(in_running_path, encoding="utf-8")
        last_exec_time = cfg.get("status", "execute_live_time")
        stop_running = cfg.get("command", "stop_running")
        user_stop_time = cfg.get("command", "user_stop_time")

        job_content = job_content.replace('{process_id}', str(process_id))
        job_content = job_content.replace(last_exec_time, exec_time)

    #檢查是否有[True] 停止job的動作，未來需要加上switch to the user動作
    if stop_running == '[True]':
        job_content = job_content.replace('[True]', '[False]')
        job_content = job_content.replace(user_stop_time, exec_time)

        if notify_line == 1:
            lineNotify.callWebLine(host_info.username, 'Job-Queue通知', '您的JOB:{}已經手動停止執行。'.format(job_id))

    with open(in_running_path,'w') as f:
        f.write(job_content)

    if stop_running == '[True]':
        shutil.move( in_running_path, finished_path)
        host_info.kill_process(process_id)

    update_time = time.time()

def finish_jobfile():
    with open(in_running_path,'r') as f:
        job_content = f.read()

    now = datetime.now()
    f_time = now.strftime("%Y/%m/%d %H:%M:%S")
    job_content = job_content.replace('{execute_end_time}', f_time)
    #job_content = job_content.replace('running', 'finished')

    with open(in_running_path,'w') as f:
        f.write(job_content)



host_info = HOSTINFO()
lineNotify = LINEBOT()

jobs = host_info.check_jobs()
print('jobs', jobs)

if not len(jobs)>0:
    sys.exit(0)

process_id = None
update_time = time.time()
notify_line, notify_email, notify_sms = 0, 0, 0
for job in jobs[:1]:
    [job_file, exe_path, owner, job_tags, notify_line, notify_email, notify_sms] = job

    job_id = job_file.split('.')[0]
    job_path = os.path.join( host_info.base_path, 'jobs', 'queue', job_file)
    in_running_path = os.path.join( host_info.base_path, 'jobs', 'running', job_file)
    finished_path = os.path.join( host_info.base_path, 'jobs', 'finished', job_file)
    failed_path = os.path.join( host_info.base_path, 'jobs', 'failed', job_file)
    exec_log_path = os.path.join( host_info.base_path, 'jobs', 'logs', job_id+'.log')

    tmp_exec_path = None
    if '_gpu' in job_file:
        gpu_id = job_file.split('_')[1].replace('gpu','').strip()
        darknet_port = str(int(gpu_id) + 8090)
        #print('GPU Interface', gpu_id)
        with open(exe_path,'r') as f:
            exec_cmd = f.read()

        exec_cmd = exec_cmd.replace('{GPU}', gpu_id)
        exec_cmd = exec_cmd.replace('{DARKNET_PORT}', darknet_port)
        #print('exec_cmd', exec_cmd)
        tmp_exec_path = os.path.join(host_info.base_path, 'hosts', host_info.hostname, 'exec_gpu_shell.sh')
        with open( tmp_exec_path, 'w') as f:
            f.write(exec_cmd)

    shutil.move(job_path, in_running_path)
    update_jobfile()

    if notify_line == 1:
        lineNotify.callWebLine(host_info.username, 'Job-Queue通知', '您的JOB:{}已經開始在{}執行。'.format(job_id, host_info.hostname))

    if tmp_exec_path is None:
        proc = subprocess.Popen("{} >{} 2>&1".format(exe_path,exec_log_path), shell=True)
    else:
        proc = subprocess.Popen("{} >{} 2>&1".format(tmp_exec_path,exec_log_path), shell=True)

    time.sleep(1)
    while proc.poll() == None:
        if process_id is None:
            process_id = proc.pid
            update_jobfile()

        elif time.time()-update_time > 30:
            update_jobfile()

        sys.stdout.flush()
        time.sleep(1)

    finish_jobfile()
    shutil.move(in_running_path, finished_path)

    if notify_line == 1:
        lineNotify.callWebLine(host_info.username, 'Job-Queue通知', '您被安排在{}的JOB:{}，目前已經執行結束。'.format(host_info.hostname, job_id))

    #將本job tag name 寫到jobs/logs/tag_finished 中, 通知該tag已結束
    if len(job_tags.strip())>0:
        now = datetime.now()
        f_time = now.strftime("%Y/%m/%d %H:%M:%S")
        with open( os.path.join(host_info.base_path, 'jobs', 'logs', 'tag_finished.info'),'a' ) as f:
            f.write("{}|{}|{} \n".format(job_tags, job_id, f_time))

