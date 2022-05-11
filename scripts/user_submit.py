import sys, os, time
import subprocess
from datetime import datetime
from libQueue import SQ
from libLineAPI import LINEBOT

def exec_cmd(cmd_txt, timeout=1):
    results = subprocess.run(
        cmd_txt, shell=True, universal_newlines=True, check=True, \
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=timeout)

    exec_code = results.returncode
    if exec_code != 0:
        print("[Error] exec_cmd() --> execute command error, rtn code is {}, command:{}".format(exec_code,cmd_txt))
        sys.exit("exit the program.")

    return results.stdout

qhost = SQ()
lineNotify = LINEBOT()

base_path = qhost.base_path

now = datetime.now()
submit_time = now.strftime("%Y/%m/%d %H:%M:%S")

owner = qhost.username

cmd = ''' hostname '''
rtn = exec_cmd(cmd, timeout=1)
if rtn is not None:
    submit_from = rtn.strip()

print('=====================JoB Submit==============================')
print('owner:{}'.format(owner))
print('hostname:{}'.format(submit_from))
print('')

#q1 --------------------------------------------------------
job_path = None
while job_path is None:
    txt = input("Your job's path: ")
    if len(txt)>6:
        if os.path.exists(txt) is True:
            job_path = txt

job_tags = None
while job_tags is None:
    txt = input("Tags for this job. (empty, or use , to separate) ")
    if len(txt) > 0:
        job_tags = txt
    else:
        job_tags = ''

#q2 --------------------------------------------------------
yn_specified_host = None
while yn_specified_host is None:
    txt = input("Do you want to execute the job on the specified workstation? (y/n) ")
    if txt in ['Y','N','y','n']:
        yn_specified_host = txt.lower()

if yn_specified_host == 'y':
    specified_host = None
    qhost.update_host_list()
    hosts = ','.join(qhost.host_list)
    while specified_host is None:
        print("workstations: {}".format(hosts))
        txt = input("Which workstation you want to assign to execute? ")
        if txt.upper() in qhost.host_list:
            specified_host = txt.upper()

if yn_specified_host == 'n':
    specified_host = 'none'

    #q3 --------------------------------------------------------
    reqs_cpus = None
    while reqs_cpus is None:
        txt = input("How many CPU cores will this job use? (0 -->not sure)  ")
        if txt.isdigit():
            if int(txt)<=64 and int(txt)>=0:
                reqs_cpus = txt

    #q4 --------------------------------------------------------
    reqs_ram = None
    while reqs_ram is None:
        txt = input("How much memory will this job use? (GB, 0 -->not sure)  ")
        if txt.isdigit():
            if int(txt)<=256 and int(txt)>=0:
                reqs_ram = str(int(txt)*1000)

    #q5 --------------------------------------------------------
    reqs_gram = None
    while reqs_gram is None:
        txt = input("How much GPU RAM will this job use? (GB, 0 -->not sure)  ")
        if txt.isdigit():
            if int(txt)<=32 and int(txt)>=0:
                reqs_gram = str(int(txt)*1000)

else:
    reqs_cpus = '0'
    reqs_ram = '0'
    reqs_gram = '0'

#q6 --------------------------------------------------------
when_to_execute = None
while when_to_execute is None:
    print("When do you want this job to execute?")
    txt = input("  (0:immediatelly, 1:after X hours, 2:on specified time, 3:let Queue-System decide, 4:After a job is finished) ")
    if txt in ['0', '1','2', '3', '4']:
        when_to_execute = txt

if when_to_execute == '0':
    specified_time_run = 'none'
    run_after_submit_min = '0'
    esti_execute_hours = '0'
    wait_for_tag = ''
    want_finish_before_date = submit_time

elif when_to_execute == '1':
    specified_time_run = 'none'
    esti_execute_hours = '0'
    wait_for_tag = ''
    want_finish_before_date = submit_time

    run_after_submit_min = None
    while run_after_submit_min is None:
        txt = input("How many hours after submit you want for this job start to run? (1~24 hrs) ")
        if txt.isdigit():
            if int(txt)<=24 and int(txt)>0:
                run_after_submit_min = str(int(txt)*60)

elif when_to_execute == '2':
    run_after_submit_min = '0'
    esti_execute_hours = '0'
    wait_for_tag = ''
    want_finish_before_date = submit_time

    specified_time_run = None
    while specified_time_run is None:
        txt = input("Please give the date & time for the job to launch. (yyyy/mm/dd hh:mm): ")
        tmp1 = txt.split('/')
        if (len(tmp1)==3 and len(tmp1[0])==4 and len(tmp1[1])==2 and len(tmp1[2])==8):
            try:
                tmp2 = len(txt.split(' ')[1].split(':'))
                if tmp2 == 2:
                    specified_time_run = txt + ':00'
            except:
                continue

elif when_to_execute == '3':
    specified_time_run = 'none'
    run_after_submit_min = '0'
    wait_for_tag = ''

    esti_execute_hours = None
    while esti_execute_hours is None:
        txt = input("How many hours will this job take? ( hours, 0:not sure )  ")
        if txt.isdigit():
            if int(txt)>=0:
                esti_execute_hours = txt

    want_finish_before_date = None
    while want_finish_before_date is None:
        txt = input("On which date/time would you like to see the results of your job? (yyyy/mm/dd hh:mm) ")
        tmp1 = txt.split('/')
        if (len(tmp1)==3 and len(tmp1[0])==4 and len(tmp1[1])==2 and len(tmp1[2])==8):
            try:
                tmp2 = len(txt.split(' ')[1].split(':'))
                if tmp2 == 2:
                    want_finish_before_date = txt + ':00'
            except:
                continue

elif when_to_execute == '4':
    specified_time_run = 'none'
    run_after_submit_min = '0'
    waitting_tag = 'none'
    esti_execute_hours = '0'
    want_finish_before_date = submit_time

    wait_for_tag = None
    while wait_for_tag is None:
        txt = input("Please input the Tag for another job, your job will start to run while the job with the tag is finished. ")
        if len(txt)>0:
            wait_for_tag = txt

#q10 --------------------------------------------------------
notify_line = None
while notify_line is None:
    txt = input("Do you want Line notify for the job's status? ( 0:no, 1:yes )  ")
    if txt in ['0','1']:
        notify_line = txt

#q11 -------------------------------------------------------
notify_email = None
while notify_email is None:
    txt = input("Do you want email notify for the job's status? ( 0:no, 1:yes )  ")
    if txt in ['0','1']:
        notify_email = txt

#q12 -------------------------------------------------------
notify_sms = None
while notify_sms is None:
    txt = input("Do you want SMS notify for the job's status? ( 0:no, 1:yes )  ")
    if txt in ['0','1']:
        notify_sms = txt

jobid = str(time.time()).split('.')[0]
if not os.path.exists(os.path.join(base_path, 'jobs', 'waiting')):
    os.makedirs(os.path.join(base_path, 'jobs', 'waiting'))

with open( os.path.join(base_path, 'scripts', 'data','job.empty'), 'r') as f:
    jobs_empty = f.read()

#now = datetime.now()
#submit_time = now.strftime("%Y/%m/%d %H:%M:%S")
jobs_empty = jobs_empty.replace('{owner}', owner)
jobs_empty = jobs_empty.replace('{specified_host}', specified_host)
jobs_empty = jobs_empty.replace('{notify_line}', notify_line)
jobs_empty = jobs_empty.replace('{notify_email}', notify_email)
jobs_empty = jobs_empty.replace('{notify_sms}', notify_sms)
jobs_empty = jobs_empty.replace('{submit_from}', submit_from)
jobs_empty = jobs_empty.replace('{submit_time}', submit_time)
jobs_empty = jobs_empty.replace('{job_path}', job_path)
jobs_empty = jobs_empty.replace('{when_to_execute}', when_to_execute)
jobs_empty = jobs_empty.replace('{specified_time_run}', specified_time_run)
jobs_empty = jobs_empty.replace('{run_after_submit_min}', run_after_submit_min)
jobs_empty = jobs_empty.replace('{esti_execute_hours}', esti_execute_hours)
jobs_empty = jobs_empty.replace('{want_finish_before_date}', want_finish_before_date)
jobs_empty = jobs_empty.replace('{reqs_cpus}', reqs_cpus)
jobs_empty = jobs_empty.replace('{reqs_ram}', reqs_ram)
jobs_empty = jobs_empty.replace('{reqs_gram}', reqs_gram)
jobs_empty = jobs_empty.replace('{job_tags}', job_tags)
jobs_empty = jobs_empty.replace('{wait_for_tag}', wait_for_tag)

with open( os.path.join(base_path, 'jobs', 'waiting', jobid+'.job'),'w' ) as f:
    f.write(jobs_empty)

print('')
print('-----------------------------------------------------------------')
print('            your job id is: {}'.format(jobid))
print('-----------------------------------------------------------------')
print('')

txt = 'Your job {} will start '.format(jobid)

if when_to_execute == '0':
    txt = 'Your job {} will start '.format(jobid)
    txt += "immediatelly."

elif when_to_execute == '1':
    txt = 'Your job {} will start '.format(jobid)
    txt += "after {} minutes.".format(run_after_submit_min)

elif when_to_execute == '2':
    txt = 'Your job {} will start '.format(jobid)
    txt += "on {}".format(specified_time_run)

elif when_to_execute == '3':
    txt = "Your job's estimated execution time is {} hours,".format(esti_execute_hours)
    txt += "and you want the job to be finished before {}".format(want_finish_before_date)
    txt += "Queue-System will arrange your job to run."

txt += '\n \n'
if yn_specified_host == 'y':
    txt += "The workstation for the job is {} which you assigned. \n".format(specified_host)
    print(txt)

else:
    txt += "Queue system will assign an workstation for you. \n"
    txt += "Currently, these are the workstations which meet your requirements."
    print(txt)

    can_hosts = qhost.can_hosts_basic( jobid )

    if len(can_hosts)>0:
        txt = 'There '
        if len(can_hosts)==1:
            txt += 'is 1 workstation '
        else:
            txt += 'are {} workstations '.format(len(can_hosts))

        txt += 'currently meet your requirements. '
        print(txt)
        #print(', '.join(can_hosts))
        print('')

        header = qhost.status_header()
        print(header)
        for i, host in enumerate(can_hosts):
            host = host.split('_')[0]
            s_path = os.path.join(base_path, 'hosts', host, 'hardware.status')
            if os.path.exists(s_path):
                with open(s_path, 'r') as f:
                    print(f.read())

    else:
        txt = 'There are currently no workstations that meet your requirements.'
        txt += 'Your job will be placed in queue, and waiting to be executed.'
        print(txt)

if notify_line == '1':
    #lineNotify.line_notify('you submitted a job \n'+jobs_empty, img_path=None)
    lineNotify.callWebLine(owner, 'JOB:{} submit'.format(jobid), jobs_empty)
