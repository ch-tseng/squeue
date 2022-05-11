from libQueue import SQ
import os
from datetime import datetime

qhost = SQ()

def print_wattings():
    path = os.path.join(qhost.base_path, 'jobs', 'waiting')
    flist = os.listdir(path)
    flist.sort()
    job_list = []
    #header = "{:15s} {:12s} {:12s} {:20s} {:12s} {:8s} {:8s} {:8s} \n".format('---------------', '------------', \
    #    '------------', '--------------------', '------------', '--------', '--------', '--------')
    header =  "{:15s} {:12s} {:12s} {:20s} {:12s} {:8s} {:8s} {:8s} {:12s}\n".format(' job', ' owner', ' from', \
        ' submit-time', ' spec-host', ' r-cpu', ' r-ram', ' r-gram', 'Tags')
    #header += "{:15s} {:12s} {:12s} {:20s} {:12s} {:8s} {:8s} {:8s}".format('-------------', '----------', \
    #    '----------', '--------------------', '---------------', '--------', '--------', '--------')
    header += "{:15s} {:12s} {:12s} {:20s} {:12s} {:8s} {:8s} {:8s} {:12s}".format('---------------', '------------', \
        '------------', '--------------------', '------------', '--------', '--------', '--------', '------------')
    print(header)

    for j in flist:
        filename, file_extension = os.path.splitext(j)
        file_extension = file_extension.lower()

        if(file_extension.lower() == '.job'):
            job_path = os.path.join(path, j)
            job_requ = qhost.get_job_requirements(job_path, \
                ['owner','submit_from','submit_time','reqs_cpus','reqs_ram','reqs_gram','specified_host','job_path','job_tags'])

            if job_requ is not None:
                job_id = j.split('.')[0]

            else:
                job_id = '[err]'+j.split('.')[0]
                job_requ = ['    ','    ','      ',0,0,0,'   ','  ']

            [owner,submit_from,submit_time,reqs_cpus,reqs_ram,reqs_gram,execute_host,job_path,job_tags] = job_requ
            submit_time = submit_time.strftime("%Y/%m/%d %H:%M:%S")
            txt = "{:15s} {:12s} {:12s} {:20s} {:12s} {:8d} {:8d} {:8d} {:12s} \n".format(\
                job_id, owner,submit_from,submit_time,execute_host,int(reqs_cpus),int(reqs_ram),int(reqs_gram),job_tags)
            txt += '                job:{} \n'.format(job_path)

            print(txt)
            print('')

def print_queue():
    path = os.path.join(qhost.base_path, 'jobs', 'queue')
    flist = os.listdir(path)
    flist.sort()
    job_list = []
    header =  "{:15s} {:12s} {:12s} {:20s} {:12s} {:8s} {:8s} {:8s} {:12s} {:12s}\n".format(' job', ' owner', ' from', \
        ' submit-time', ' spec-host', ' r-cpu', ' r-ram', ' r-gram', 'Tags', 'exec-host')
    header += "{:15s} {:12s} {:12s} {:20s} {:12s} {:8s} {:8s} {:8s} {:12s} {:12s}".format('---------------', '------------', \
        '------------', '--------------------', '------------', '--------', '--------', '--------', '------------', '------------')
    print(header)

    for j in flist:
        filename, file_extension = os.path.splitext(j)
        file_extension = file_extension.lower()

        if(file_extension.lower() == '.job'):
            job_path = os.path.join(path, j)
            job_requ = qhost.get_job_requirements(job_path, \
                ['owner','submit_from','submit_time','reqs_cpus','reqs_ram','reqs_gram','specified_host','job_path', 'job_tags'])

            if job_requ is not None:
                tmp = j.split('.')[0].split('_')
                if len(tmp)>2:
                    host_exec = tmp[0]
                    gpu_id = tmp[1]
                    job_id = tmp[2] +  '({})'.format(gpu_id)
                else:
                    host_exec = tmp[0]
                    job_id = tmp[1]

            else:
                job_id = '[err]'+j.split('.')[0]
                job_requ = ['    ','    ','      ',0,0,0,'   ','  ']

            [owner,submit_from,submit_time,reqs_cpus,reqs_ram,reqs_gram,execute_host,job_path,job_tags] = job_requ
            submit_time = submit_time.strftime("%Y/%m/%d %H:%M:%S")
            txt = "{:15s} {:12s} {:12s} {:20s} {:12s} {:8d} {:8d} {:8d} {:12s} {:12s}\n".format(\
                job_id, owner,submit_from,submit_time,execute_host,int(reqs_cpus),int(reqs_ram),int(reqs_gram),job_tags,host_exec)
            txt += '                job:{} \n'.format(job_path)

            print(txt)

    with open( os.path.join(qhost.base_path, 'jobs', 'arrange.info'), 'r') as f:
        print(f.read())

def print_running():
    path = os.path.join(qhost.base_path, 'jobs', 'running')
    flist = os.listdir(path)
    flist.sort()
    job_list = []
    header =  "{:15s} {:12s} {:20s} {:20s} {:12s} {:12s} {:12s} {:50s} \n".format(' job', ' owner', ' submit-time', \
        'start-time', ' exec-host', ' process-id', 'Tags', ' log-file')
    header += "{:15s} {:12s} {:20s} {:20s} {:12s} {:12s} {:12s} {:50s} ".format('---------------', '------------', \
        '--------------------', '--------------------', '------------', '------------', '------------', '-------------------------')
    print(header)

    for j in flist:
        filename, file_extension = os.path.splitext(j)
        file_extension = file_extension.lower()

        if(file_extension.lower() == '.job'):
            job_path = os.path.join(path, j)
            job_requ = qhost.get_job_requirements(job_path, \
                ['owner','submit_time','execute_start_time','execute_host','process_id','execute_log','job_path', 'job_tags'])

            if job_requ is not None:
                tmp = j.split('.')[0].split('_')
                if len(tmp)>2:
                    host_exec = tmp[0]
                    gpu_id = tmp[1]
                    job_id = tmp[2] +  '({})'.format(gpu_id)

                else:
                    job_id = tmp[1]

            else:
                job_id = '[err]'+j.split('.')[0]
                job_requ = ['    ','    ','      ',0,0,0,'   ','  ']

            [owner,submit_time,execute_start_time,execute_host,process_id,execute_log,job_path,job_tags] = job_requ
            submit_time = submit_time.strftime("%Y/%m/%d %H:%M:%S")
            execute_start_time = execute_start_time.strftime("%Y/%m/%d %H:%M:%S")
            #print(execute_start_time)
            #execute_start_time = execute_start_time.strftime("%Y/%m/%d %H:%M:%S")
            txt = "{:15s} {:12s} {:20s} {:20s} {:12s} {:12s} {:12s} {:50s} ".format(\
                job_id, owner,submit_time,execute_start_time,execute_host,process_id,job_tags,execute_log)
            txt += '                job:{} \n'.format(job_path)

            print(txt)


print('[Waitting]')
print_wattings()
print('')
print('')

print('[Queue]')
print_queue()
print('')
print('')

print('[Running]')
print_running()
