import sys, os
from datetime import datetime
import time
from configparser import ConfigParser
import ast
import glob

class SQ:
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("config.ini",encoding="utf-8")

        self.th_seen_seconds = cfg.getint('global', 'th_seen_seconds')
        self.base_path = cfg.get('global', 'base_path')

    def modification_date(self, file_path):
        t = os.path.getmtime(file_path)
        return time.time() - t, datetime.fromtimestamp(t)

    def update_host_list(self):
        hosts = os.listdir( os.path.join(self.base_path,'hosts'))
        hosts.sort()
        self.host_list = hosts

    def update_host_data(self):
        self.hosts_qlist = ''
        hosts_status = {}

        #hosts = os.listdir( os.path.join(self.base_path,'hosts'))
        #hosts.sort()
        self.update_host_list()
        hosts = self.host_list
        for host in os.listdir( os.path.join(self.base_path,'hosts')):
            host_path = os.path.join(self.base_path,'hosts',host)
            if os.path.isdir(host_path):
                status_path = os.path.join(self.base_path,'hosts',host,'hardware.status')
                if os.path.exists(status_path):
                    passed_seconds, last_update_time = self.modification_date(status_path)

                    if passed_seconds<self.th_seen_seconds:
                        with open(status_path, 'r') as f:
                            self.hosts_qlist += f.read()
                            lines = self.hosts_qlist.split('\n')
                            for line in lines:
                                datas = line.split()
                                if len(datas) == 10:
                                    #                             cores, loading, ram_available, grams
                                    sdata = [ int(datas[1]), float(datas[2]), int(datas[5]), [int(datas[9])] ]
                                elif len(datas)>0:
                                    sdata[3].append( int(datas[2]) )

                            hosts_status.update( {host:sdata} )

        self.hosts_status = hosts_status

    def status_header(self):
        header = '{:10s} {:4s} {:8s} {:10s} {:10s} {:10s} {:5s} {:10s} {:10s} {:10s}\n'.format( \
            'host', 'cpus','loading','total-ram','ram-used','ram-avail','gpus','total-gram','used-gram','free-gram')
        header += '{:10s} {:4s} {:8s} {:10s} {:10s} {:10s} {:5s} {:10s} {:10s} {:10s}'.format( \
            '-------','----','-------','---------','---------','---------','------','----------','----------','----------')
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

            #print(cpu_loading, (ram_avil,r_ram), (cores,r_cpu))
            if cpu_loading<50 and (ram_avil>=r_ram) and (cores>=r_cpu):
                if r_gram>0:
                    for i, g in enumerate(gram_avil_list):
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

        else:
            return None

        if v in ['owner', 'submit_from','submit_time','job_path','specified_host','execute_host','process_id','execute_start_time',\
                 'execute_live_time','execute_end_time','execute_status','execute_log', 'specified_time_run', 'want_finish_before_date',\
                 'job_tags', 'wait_for_tag']:
            t2 = 's'  #string
        elif v in ['when_to_execute', 'notify_line', 'notify_email', 'notify_sms', 'run_after_submit_min', 'esti_execute_hours', \
                   'reqs_cpus','reqs_ram','reqs_gram']:
            t2 = 'i' #integer
        else:
            return None

        return (t1,t2)

    def get_job_requirements(self, job_path, keyPara=[]):
        cfg_job = ConfigParser()

        #if True:
        try:
            cfg_job.read(job_path, encoding="utf-8")

            if len(keyPara)>0:
                rtn = []
                for key in keyPara:
                    (t1,t2) = self.get_para_type(key)
                    if t2 == 'i':
                        data = cfg_job.getint(t1, key)
                    elif t2 == 's':
                        data = cfg_job.get(t1, key)

                    if key in ['submit_time', 'execute_start_time', 'execute_live_time', 'execute_end_time', 'specified_time_run', \
                               'want_finish_before_date']:
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

        except:
            print('[Warning] {} cannot load, please review the job file.'.format(job_path))
            return None


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
