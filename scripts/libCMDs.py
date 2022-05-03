import subprocess
import sys, os
from configparser import ConfigParser
import ast
import re

class HOSTINFO:
    def __init__(self):
        cfg = ConfigParser()
        cfg.read("/home/chtseng/client.ini",encoding="utf-8")

        self.base_path = cfg.get('global', 'base_path')
        self.hostname = cfg.get('global', 'hostname')
        self.host_path = os.path.join(self.base_path, 'hosts', self.hostname )

    def exec_cmd(self, cmd_txt, timeout=1):
        results = subprocess.run(
            cmd_txt, shell=True, universal_newlines=True, check=True, \
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8", timeout=timeout)

        exec_code = results.returncode
        if exec_code != 0:
            print("[Error] exec_cmd() --> execute command error, rtn code is {}, command:{}".format(exec_code,cmd_txt))
            sys.exit("exit the program.")

        return results.stdout

    def err_exit(self, def_name, info):
        print("[Error] {} --> {}".format(def_name, info))
        sys.exit("exit the program.")

    def get_cpu_list(self):
        cmd = '''cat /proc/cpuinfo | grep processor | wc -l '''
        rtn = self.exec_cmd(cmd, timeout=1)
        if rtn is not None:
            cpu_count = int(rtn)
        else:
            err_exit('get_cpu_list()', 'get /proc/cpuinfo is None')

        cmd = '''cat /proc/cpuinfo | grep "model name" '''
        rtn = self.exec_cmd(cmd, timeout=1)
        rtn = rtn.replace('\t','').replace('model name:','').strip()
        rtn = rtn.split('\n')

        if cpu_count != len(rtn):
            err_exit('get_cpu_list()', '/proc/cpuinfo and /proc/cpuinfo are not matched.')

        return rtn

    def get_gpu_list(self):
        cmd = ''' nvidia-smi -q | grep "Product Name" '''
        rtn = self.exec_cmd(cmd, timeout=1)
        if rtn is not None:
            gpu_count = len(rtn)
            rtn = rtn.split('\n')
            lists = []
            for r in rtn:
                if len( r.strip() ) > 0:
                    lists.append(r.split(':')[1].strip())

            cmd = ''' nvidia-smi -q | grep -A 4 "FB Memory Usage" '''
            rtn = self.exec_cmd(cmd, timeout=3)
            if rtn is not None:
                rtn = rtn.replace('\n', '').replace('--','')
                list1 = rtn.split('FB Memory Usage')
                lists_ram, lists_1 = [], []
                for l in list1:
                    if len(l.strip())>0:
                        lists_1.append(l)
                        lists_2 = []  #4 values: Total,(Reserved),Used,Free
                        for ll in l.split('MiB'):
                            ll = ll.strip()
                            if len(ll)>0 and (ll[:4] in ['Tota','Used','Free']):
                                lists_2.append( int(ll.split(':')[1].strip()) )

                        lists_ram.append(lists_2)

            else:
                err_exit('get_gpu_list()', 'get info from nvidia-smi -q got error.')

        else:
            err_exit('get_gpu_list()', 'get info from nvidia-smi -q  got error.')

        return lists, lists_ram

    def get_memory_info(self):
        cmd = ''' free | grep "Mem:" '''  #total        used        free      shared  buff/cache   available
        rtn = self.exec_cmd(cmd, timeout=1)
        if rtn is not None:
            rtn =rtn.replace('Mem:', '').replace('\n','')
            rtn = rtn.split(' ')
            lists = []
            for r in rtn:
                if len(r.strip())>0:
                    lists.append( int(int(r)/1000))

            if len(lists)==6:
                #return total, used, available
                return lists[0], lists[1], lists[5]
            else:
                err_exit('get_memory_info()', 'get info from free cmd, columns are not equal to 5')

    def get_cpu_loading(self):
        cmd = ''' grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$4+$5)} END {print usage}' '''
        rtn = self.exec_cmd(cmd, timeout=1)
        if rtn is not None:
            return float(rtn)
        else:
            return None

    def log_status(self, data, type):
        with open(os.path.join(self.host_path, type+'.status'),'w') as f:
            f.write(data)

    def punch(self, ptype):
        if ptype == 'hardware':
            if not os.path.exists( os.path.join(self.host_path)):
                os.makedirs( os.path.join(self.host_path)  )

            #format cpu,ram,gpu,gram,hdisk
            cpu_list = self.get_cpu_list()
            #print('cpu_count:{}, cpu_list:{}'.format( len(cpu_list), cpu_list))

            gpu_list, gpu_ram_info = self.get_gpu_list()
            #print('gpu_count:{}, gpu_list:{}, ram:{}'.format( len(gpu_list), gpu_list, gpu_ram_info))
            ram_total, ram_used, ram_avail = self.get_memory_info()
            cpu_loading = self.get_cpu_loading()
            #print('cpu_loading', cpu_loading)

            #cpu-cores	cpu-loading	memory-total	memory-used	memory-avail	gpus	gram-total	gram-used	gram-free
            base_logtxt = '{:10s} {:4d} {:7.1f} {:10d} {:10d} {:10d} {:5d}'.format(self.hostname,len(cpu_list), cpu_loading, ram_total, ram_used, ram_avail, len(gpu_list))
            if len(gpu_list)>0:
                for id, gpu in enumerate(gpu_list):
                    if id == 0:
                        logtxt = base_logtxt + '{:10d} {:10d} {:10d}\n'.format(gpu_ram_info[id][0], gpu_ram_info[id][1], gpu_ram_info[id][2])
                    else:
                        logtxt += "".join(' ' for x in range(0,len(base_logtxt))) + '{:10d} {:10d} {:10d}\n'.format(gpu_ram_info[id][0], gpu_ram_info[id][1], gpu_ram_info[id][2])
            else:
                logtxt = '{:10s} {:4d} {:7.1f} {:10d} {:10d} {:10d} {:5d} {:10s} {:10s} {:10s}'.format(len(cpu_list), cpu_loading, ram_total, ram_used, ram_avail, 0, 'x', 'x', 'x')

            self.log_status(logtxt, 'hardware')
            print(logtxt)

    def get_para_type(self, v):
        rtn = []
        if v in ['owner','notify_line','notify_email','notify_sms']:
            t1 = 'user'

        elif v in ['submit_from','submit_time','job_path','run_after_submit_min','esti_execute_hours','want_finish_after_hours']:
            t1 = 'job'

        elif v in ['reqs_cpus','reqs_ram','reqs_gram']:
            t1 = 'host'

        else:
            return None

        if v in ['owner', 'submit_from','submit_time','job_path']:
            t2 = 's'  #string
        elif v in ['notify_line', 'notify_email', 'notify_sms', 'run_after_submit_min', 'esti_execute_hours','want_finish_after_hours', 'reqs_cpus','reqs_ram','reqs_gram']:
            t2 = 'i' #integer
        else:
            return None

        return (t1,t2)

    def get_job_requirements(self, job_path, keyPara=[]):
        cfg_job = ConfigParser()

        if True:
            cfg_job.read(job_path, encoding="utf-8")

            if len(keyPara)>0:
                rtn = []
                for key in keyPara:
                    (t1,t2) = self.get_para_type(key)
                    if t2 == 'i':
                        data = cfg_job.getint(t1, key)
                    elif t2 == 's':
                        data = cfg_job.get(t1, key)

                    if key == 'submit_time':
                        data = datetime.strptime(data, '%Y/%m/%d %H:%M:%S')

                    rtn.append(data)

                return rtn

            else:
                r_cpu = cfg_job.getint('host', 'reqs_cpus')
                r_ram = cfg_job.getint('host', 'reqs_ram')
                r_gram = cfg_job.getint('host', 'reqs_gram')

                return (r_cpu,r_ram,r_gram)

    def check_jobs(self):
        job_waits = []
        check_path = os.path.join(self.base_path, 'jobs', 'queue')
        files = os.listdir(check_path)
        for f in files:
            job_path = os.path.join(self.base_path, 'jobs', 'queue', f)

            if os.path.isfile(job_path):
                if f.split('_')[0] == self.hostname:
                    [exe_path, owner, notify_line, notify_email, notify_sms] = \
                        self.get_job_requirements(job_path, ['job_path','owner','notify_line','notify_email','notify_sms'])

                    #print(owner)
                    job_waits.append([f, exe_path, owner, notify_line, notify_email, notify_sms])

        return job_waits
