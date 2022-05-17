import sys, os
from datetime import datetime
import time
from configparser import ConfigParser
import ast
import glob
import re
import getpass

class SQ:
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("config.ini",encoding="utf-8")

        self.th_seen_seconds = cfg.getint('global', 'th_seen_seconds')
        self.base_path = cfg.get('global', 'base_path')
        self.th_cpu_busy = cfg.getint('global', 'th_cpu_busy')

        if os.path.exists('/etc/centos-release'):
            self.ubuntu = False
        else:
            self.ubuntu = True

        self.username = getpass.getuser()

    def modification_date(self, file_path):
        t = os.path.getmtime(file_path)
        return time.time() - t, datetime.fromtimestamp(t)

    def update_host_list(self):
        hosts = os.listdir( os.path.join(self.base_path,'hosts'))
        hosts.sort()
        self.host_list = hosts

    def split_host_id(self, name):
        match = re.match(r"([a-z]+)([0-9]+)", name, re.I)
        if match:
            items = match.groups()
        else:
            items = []

        return items

    def resource_job_reserve(self, host_name):
        queue_path = os.path.join(self.base_path,'jobs', 'queue')
        running_path = os.path.join(self.base_path,'jobs', 'running')

        t_cpu, t_ram, gpu_gram = 0, 0, {}
        for job_file in os.listdir(queue_path):
            path_job_file = os.path.join(queue_path, job_file)
            if os.path.isfile(path_job_file):
                if job_file.split('_')[0] == host_name:
                    r_cpu, r_ram, r_gram = self.get_job_requirements(path_job_file)
                    t_cpu += r_cpu
                    t_ram += r_ram
                    if 'gpu' in job_file.split('_')[1]:
                        gpu_id = int(job_file.split('_')[1].replace('gpu',''))
                        if gpu_id in gpu_gram:
                            t_gram = gpu_gram[gpu_id]
                        else:
                            t_gram = 0

                        t_gram += r_gram
                        gpu_gram.update( {gpu_id:t_gram} )

        for job_file in os.listdir(running_path):
            path_job_file = os.path.join(running_path, job_file)
            if os.path.isfile(path_job_file):
                if job_file.split('_')[0] == host_name:
                    r_cpu, r_ram, r_gram = self.get_job_requirements(path_job_file)
                    t_cpu += r_cpu
                    t_ram += r_ram
                    if 'gpu' in job_file.split('_')[1]:
                        gpu_id = int(job_file.split('_')[1].replace('gpu',''))
                        if gpu_id in gpu_gram:
                            t_gram = gpu_gram[gpu_id]
                        else:
                            t_gram = 0

                        t_gram += r_gram
                        gpu_gram.update( {gpu_id:t_gram} )


        return t_cpu, t_ram, gpu_gram

    def update_host_data(self):
        self.hosts_qlist = ''
        hosts_status = {}

        self.update_host_list()
        host_path_list = os.listdir( os.path.join(self.base_path,'hosts'))

        if self.ubuntu is False:
            host_path_list.sort(key=self.split_host_id)

        for host in host_path_list:
            host_path = os.path.join(self.base_path,'hosts',host)
            if os.path.isdir(host_path):
                status_path = os.path.join(self.base_path,'hosts',host,'hardware.status')
                if os.path.exists(status_path):
                    passed_seconds, last_update_time = self.modification_date(status_path)

                    if passed_seconds<self.th_seen_seconds:
                        with open(status_path, 'r') as f:
                            fread = f.read()

                            if self.ubuntu is False: fread += '\n'

                            self.hosts_qlist += fread
                            lines = self.hosts_qlist.split('\n')
                            for line in lines:
                                datas = line.split()
                                if len(datas) == 10:
                                    #                             cores, loading, ram_available, grams
                                    if datas[9] == 'x':
                                        gpu_ram = 0
                                    else:
                                        gpu_ram = int(datas[9])

                                    sdata = [ int(datas[1]), float(datas[2]), int(datas[5]), [gpu_ram] ]
                                elif len(datas)>0:
                                    sdata[3].append( float(datas[2]) )

                            hosts_status.update( {host:sdata} )

        self.hosts_status = hosts_status

    def status_header(self):
        header = '{:20s} {:4s} {:8s} {:10s} {:10s} {:10s} {:5s} {:10s} {:10s} {:10s}\n'.format( \
            'host', 'cpus','loading','total-ram','ram-used','ram-avail','gpus','total-gram','used-gram','free-gram')
        header += '{:20s} {:4s} {:8s} {:10s} {:10s} {:10s} {:5s} {:10s} {:10s} {:10s}'.format( \
            '------------','----','-------','---------','---------','---------','------','----------','----------','----------')
        return header


    def get_status(self):
        #cpu-cores  cpu-loading     memory-total    memory-used     memory-avail    gpus    gram-total      gram-used       gram-free
        header = self.status_header()
        print(header)

        self.update_host_data()
        print(self.hosts_qlist)

    def get_execute_host(self, r_ram, r_cpu, r_gram):
        self.update_host_data()

        host_list = self.hosts_status
        avil_host_list = {}
        for host in host_list:
            cores = host_list[host][0]
            cpu_loading = host_list[host][1]
            ram_avil = host_list[host][2]
            gram_avil_list = host_list[host][3]

            #需扣除queue, running中, job所需要的resource
            d_cpu, d_ram, d_gram_planned = self.resource_job_reserve(host_name=host)
            print('主機{}, queue, running 中的jobs, 要保留CPU:{}, RAM:{}, GRAM:{}'.format(host,d_cpu,d_ram,d_gram_planned))
            ram_avil = ram_avil - d_cpu
            cores = cores - d_cpu

            #print(cpu_loading, (ram_avil,r_ram), (cores,r_cpu))
            if cpu_loading<self.th_cpu_busy and (ram_avil>=r_ram) and (cores>=r_cpu):
                if r_gram>0:
                    for i, g in enumerate(gram_avil_list):
                        if len(d_gram_planned)>0 and i in d_gram_planned:
                            d_gram = d_gram_planned[i]
                        else:
                            d_gram = 0

                        g = g - d_gram
                        if g>r_gram:
                            avil_host_list.update( { host+'_gpu'+str(i):[cpu_loading,cores,ram_avil,gram_avil_list] } )

                else:
                    avil_host_list.update( {host:[cpu_loading,cores,ram_avil,gram_avil_list] } )

        return dict(sorted(avil_host_list.items(), key=lambda x: x[1][0]))

    def get_para_type(self, v):
        rtn = []
        if v in ['owner','notify_line','notify_email','notify_sms']:
            t1 = 'user'

        elif v in ['submit_from','submit_time','job_path','when_to_execute','run_after_submit_min','esti_execute_hours', \
                   'specified_time_run', 'want_finish_before_date', 'job_tags', 'wait_for_tag']:
            t1 = 'job'

        elif v in ['specified_host','reqs_cpus','reqs_ram','reqs_gram']:
            t1 = 'host'

        elif v in ['execute_host','process_id','execute_start_time','execute_live_time','execute_end_time','execute_status','execute_log']:
            t1 = 'status'

        elif v in ['stop_running', 'user_stop_time']:
            t1 = 'command'

        else:
            return None

        if v in ['owner', 'submit_from','submit_time','job_path','specified_host','execute_host','process_id','execute_start_time',\
                 'execute_live_time','execute_end_time','execute_status','execute_log', 'specified_time_run', 'want_finish_before_date',\
                 'job_tags', 'wait_for_tag', 'stop_running', 'user_stop_time']:
            t2 = 's'  #string
        elif v in ['when_to_execute', 'notify_line', 'notify_email', 'notify_sms', 'run_after_submit_min', 'esti_execute_hours', \
                   'reqs_cpus','reqs_ram','reqs_gram']:
            t2 = 'i' #integer
        else:
            return None

        return (t1,t2)

    def get_job_requirements(self, job_path, keyPara=[]):
        cfg_job = ConfigParser()

        if True:
        #try:
            cfg_job.read(job_path, encoding="utf-8")

            if len(keyPara)>0:
                rtn = []
                for key in keyPara:
                    (t1,t2) = self.get_para_type(key)
                    #print('t1,t2,key', t1,t2,key)
                    if t2 == 'i':
                        data = cfg_job.getint(t1, key)
                    elif t2 == 's':
                        data = cfg_job.get(t1, key)

                    if key in ['submit_time', 'execute_start_time', 'execute_live_time', 'execute_end_time', 'specified_time_run', \
                               'want_finish_before_date', 'user_stop_time']:
                        try:
                            data = datetime.strptime(data, '%Y/%m/%d %H:%M:%S')
                        except:
                            pass

                    rtn.append(data)

                return rtn

            else:
                r_cpu = cfg_job.getint('host', 'reqs_cpus')
                r_ram = cfg_job.getint('host', 'reqs_ram')
                r_gram = cfg_job.getint('host', 'reqs_gram')

                return (r_cpu,r_ram,r_gram)

        #except:
        #    print('[Warning] {} cannot load, please review the job file.'.format(job_path))
        #    return None


    def can_hosts_basic(self, jobid):
        path = os.path.join(self.base_path, 'jobs', 'waiting')
        cfg_job = ConfigParser()
        job_list = os.listdir(path)

        exec_host = []
        for j in job_list:
            filename, file_extension = os.path.splitext(j)
            file_extension = file_extension.lower()

            if(file_extension.lower() == '.job' and jobid==filename):
                job_path = os.path.join(path, j)

                try:
                    job_requ = self.get_job_requirements(job_path)

                    if job_requ is not None:
                        r_cpu = job_requ[0]
                        r_ram = job_requ[1]
                        r_gram = job_requ[2]

                except:
                    print('[Warning] {} cannot load, please review the job file.'.format(job_path))
                    continue

                exec_host = self.get_execute_host(r_ram,r_cpu,r_gram)

        return exec_host
                #print( jobid, 'exec_host', exec_host)

    def waiting_job_list(self):
        path = os.path.join(self.base_path, 'jobs', 'waiting')
        flist = os.listdir(path)
        flist.sort()
        job_list = []
        for j in flist:
            filename, file_extension = os.path.splitext(j)
            file_extension = file_extension.lower()

            if(file_extension.lower() == '.job'):
                job_path = os.path.join(path, j)
                job_requ = self.get_job_requirements(job_path)
                if job_requ is not None:
                    job_list.append(j)

        return job_list

    def running_job_list(self):
        path = os.path.join(self.base_path, 'jobs', 'running')
        flist = os.listdir(path)
        flist.sort()
        job_list = []
        for j in flist:
            filename, file_extension = os.path.splitext(j)
            file_extension = file_extension.lower()

            if(file_extension.lower() == '.job'):
                job_path = os.path.join(path, j)
                job_requ = self.get_job_requirements(job_path)
                if job_requ is not None:
                    job_list.append(j)

        return job_list

    def queueing_job_list(self):
        path = os.path.join(self.base_path, 'jobs', 'queue')
        flist = os.listdir(path)
        flist.sort()
        job_list = []
        for j in flist:
            filename, file_extension = os.path.splitext(j)
            file_extension = file_extension.lower()

            if(file_extension.lower() == '.job'):
                job_path = os.path.join(path, j)
                job_requ = self.get_job_requirements(job_path)
                if job_requ is not None:
                    job_list.append(j)

        return job_list


    def job_name_2_id(self, job):
        tmp = job.replace('.job','')
        tmp = tmp.split('_')[-1:]
        return tmp

    def get_job_content(self, job_id):
        path = os.path.join(self.base_path, 'jobs')

        for folder in ['waiting','queue','running','failed','finished']:
            folder_path = os.path.join(path,folder)
            files = os.listdir(folder_path)
            for f in files:
                filename, file_extension = os.path.splitext(f)
                if file_extension == '.job':
                    if job_id in filename:
                        job_file_path = os.path.join(folder_path, f)
                        print('     ======================================================')
                        print('       The status for the job {} is {}.'.format(job_id,folder))
                        print('     ======================================================')
                        print('')
                        print('--------------------------------------------------------------------')
                        with open(job_file_path, 'r') as f:
                            print(f.read())

                        print('--------------------------------------------------------------------')
