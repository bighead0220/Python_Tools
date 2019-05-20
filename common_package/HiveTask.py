#!/usr/bin/env python3
#===============================================================================
#
#         FILE: HiveTask.py
#
#        USAGE: ---
#
#  DESCRIPTION:
#
#      OPTIONS: ---
# REQUIREMENTS: ---
#         BUGS: ---
#        NOTES: ---
#       AUTHOR: liuxiaoze
#      COMPANY: JD
#      VERSION: 5.0
#      CREATED: 2017年1月19日
#     REVISION: ---
#     REVISION: ---
#    SRC_TABLE: ---
#    TGT_TABLE: --- 
#===============================================================================
import os
import sys
import datetime
import subprocess
import random
from Calendar import *

class HiveTask:

    def __init__(self,  log_lev = 'INFO', ignore_error = False, shark = False):
        os.chdir(os.path.abspath(os.path.dirname(sys.argv[0])))
        self.edw_dir = '../'
        self.shark = shark
        for i in range(10):
            if os.path.exists(self.edw_dir+'common') and os.path.exists(self.edw_dir+'etl'):
                break
            else:
                self.edw_dir += '../'
        self.date_today   = datetime.datetime.now().date()
        date_yesterday    = self.date_today - datetime.timedelta(days=1)
        self.tmpdict = {}
        for i in sys.argv[1:]:
            i = i.strip()
            if i.isalpha() and i.lower() == 'explain':
                self.tmpdict['explain'] = 'explain'
            else:
                get_date = i.replace('-','')[:8]
                if get_date.isdigit() and 'data_day' not in self.tmpdict:
                    self.data_day = datetime.date(int(get_date[:4]), int(get_date[4:6]), int(get_date[6:]))
                    self.tmpdict['data_day'] = self.data_day
        if 'data_day' not  in self.tmpdict:
            self.data_day = date_yesterday 
            self.calendar = Calendar(self.data_day)
        else:
            self.calendar = Calendar(self.data_day)
        self.data_day_int = self.data_day.strftime("%Y%m%d")
        self.data_day_str = self.data_day.isoformat()
        self.ignore_error = ignore_error
        self.log_name     = os.path.splitext( os.path.split( sys.argv[0])[1])[0]
        self.UDF_JAR      = os.getenv('UDF_JAR')
        self.username     = os.environ['USER']
        from ddlogging import DDLogging
        from hadoop_connector import HadoopConnector 
    
        log_place_dir = os.getenv('BI_LOG_DIR')
        if log_place_dir is None:
            log_place_dir = '/tmp/'+self.username+'_LOG/'
        log_place_dir += '/' + self.date_today.strftime("%Y%m%d")
        if not os.path.isdir(log_place_dir):
            try:
                os.makedirs(log_place_dir)
            except:
                pass
        mylog = DDLogging( self.log_name, log_place_dir )
        mylog.set_level(log_lev)
        self.log = mylog.get_logger()
        self.log.info('日志位置：'+ log_place_dir)
        self.hc = HadoopConnector(self.log)

    def exec_sql(self, schema_name, sql, table_name = '', lzo_compress = False, lzo_index_path = None, merge_flag = False, merge_part_dir = [],  min_size = 128*1024*1024):
        try:
            #self.hc.run_sql_in_hive(schema_name, sql, lzo_compress = lzo_compress)

            res = self.__run_sql_in_hive(db = schema_name, sql = sql, isLzop = lzo_compress)
            if res[0] != 0:
                raise Exception('Please Check SQL...')

            if res[1] == '0':
                self.log.warning('You get a NULL table...')    

            if lzo_compress and lzo_index_path != None:
                self.CreateIndex(db = schema_name, table = table_name, path = lzo_index_path) 

            if merge_flag:
                self.merge_small_file(db = schema_name, table = table_name, partition = merge_part_dir,  min_size = min_size) 

            if schema_name == 'fdm':
                parse = self.__parse_table(db = schema_name, table = table_name)
                table_path = '/'+parse['Location'].replace('//','#').split('/',1)[1]+'/'
                from check_compare import check_compare
                check_compare(table_path)

        except Exception as e:
            if not self.ignore_error:
                raise e
            self.log.error(str(e))

    def __run_sql_in_hive(self, db, sql, isLzop=False):
        import re
        #sql  = sql.strip().replace('\\', '\\\\').replace('"', '\\"')
        if 'explain' in self.tmpdict:
            sql = 'EXPLAIN '+sql.replace(';', ';\nEXPLAIN ')
            sql = re.sub('EXPLAIN *\n*set', 'set', sql) 
        hql = '\nset hive.exec.dynamic.partition=true;\nset hive.exec.dynamic.partition.mode=nonstrict;\n'
        if isLzop:
            hql += '''set mapred.output.compress=true;
                      set hive.exec.compress.output=true;
                      set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
                   '''
        hql += '''USE %s;\n%s''' % ( db, sql)
        if self.shark:
            res = self.run_shell_cmd('shark -e"%s"' % hql)
        else:
            res = self.run_shell_cmd('hive -e"%s"' % hql)
        totalwrite = '0'    
        for num in res[1].split('\n'):
            if num.find('HDFS Read:') != -1:
                num = num.strip().split()
                change = num[num.index('HDFS'):-1]
                self.changeNumbers = change
                self.log.debug(' '.join(change))
                totalwrite = str(change[-1])
        return [res[0], totalwrite]

    def merge_small_file(self, db, table, partition = [], min_size = 128*1024*1024):
        hadoop    = os.environ.get("HADOOP_HOME")
        streaming = hadoop+'/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar'
        dirs      = []
        dirs_size = []
        parse     = self.__parse_table(db = db,table = table)
        local     = '/'+parse['Location'].replace('//','#').split('/',1)[1]
        part      = ['']
        isLzop    = False
        lzop      = ''
        if parse['InputFormat'].endswith('LzoTextInputFormat'):
            isLzop = True 
            lzop   = '-D mapred.output.compress=true -D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec'
        
        if partition == []:
            if parse['PARTITION'] != None:
                part  = self.__partitions(db = db, table = table)
        else:
            if parse['PARTITION'] != None:
                part = []
                allpartitions  = self.__partitions(db = db, table = table)
                for i in [i.strip() for i in partition if i.strip() != '']:
                    if i.endswith('*'):
                        part.append(i)
                        continue
                    for h in allpartitions:
                        if h.startswith(i):
                            part.append(h)
        self.log.info(part)
    
        for partition in part:
            dir = local+'/' + partition
            dir = dir.rstrip('/*')
            if partition.endswith('*'):
                dirs = [dir+'/*']
            else:
                result = self.run_shell_cmd('hadoop fs -du %s' % dir)
                for line in result[1].split('\n'):
                    line = line.strip().split()
                    if len(line) == 2 and line[0].isdigit() and not line[1].endswith('.index') and not line[1].endswith('_SUCCESS') and not line[1].endswith('_logs'):
                        file_size,file_dir = line
                        if int(file_size) < min_size:
                            dirs_size.append(int(file_size))
                            dirs.append('/'+file_dir.replace('//','#').split('/',1)[1])
                if len(dirs) < 2:
                    dirs, dirs_size   = [], []
                    continue
            self.log.info( dirs )
            random_num = hex(random.randint(0xF00000, 0xFFFFFF))
            Output     = '/user/'+self.username+'/warehouse/tmp/sqltmp/' + table + '_' + random_num + '/'
            total_size = sum(dirs_size)
            numReduce  = int(total_size/(1024*1024*250))
            if numReduce == 0:
                numReduce += 1
            self.log.info('Sum small file size is %sB/250MB = %s' % (total_size, numReduce))
            if [ i.strip() for i in dirs if i.strip() != ''] != []:
                jobcmd = "%s/bin/hadoop jar %s %s -D mapred.job.priority=VERY_HIGH \
                -input %s \
                -output %s \
                -numReduceTasks '%s'  \
                -mapper 'cat'  \
                -reducer 'cat'" % (hadoop, streaming,  lzop, ' '.join(dirs), Output, numReduce)
                try:result = self.run_shell_cmd('hadoop fs -rmr -skipTrash  %s' % Output)
                except:pass
                result = self.run_shell_cmd(jobcmd) 
                results = [i.strip() for i in result[1].split('\n') if i.strip() != '']
                self.log.info(results[-1])
                if results[-1] == 'Streaming Command Failed!':
                    self.log.info('Merge Small File Field!')
                    raise
                dirs   = [ i+'*' for i in dirs ]
                try:result = self.run_shell_cmd('hadoop fs -rmr -skipTrash %s' % ' '.join(dirs)) 
                except:pass
                if isLzop:
                    self.hc.lzop_index( path = Output) 
                result = self.run_shell_cmd('hadoop fs -mv %s* %s' % (Output, dir))
            dirs, dirs_size   = [], [] 
            try:result  = self.run_shell_cmd('hadoop fs -rmr -skipTrash %s' % Output)
            except:pass

    def CreateIndex(self, db, table, path = 'Normal'):
        parse      = self.__parse_table(db = db, table = table)
        table_path = '/'+parse['Location'].replace('//','#').split('/',1)[1]+'/'
    
        if not isinstance(path,list):
            if path.lower() == 'normal':
                indexpath = table_path + 'dt=' + self.data_day_str
            elif path == '':
                indexpath = table_path
            else:
                indexpath = path
            self.log.info('Index Path:' + indexpath)
            self.hc.lzop_index( path = indexpath)
        else:
            for partition in path:
                if not  partition.startswith('/'):
                    indexpath = table_path+partition
                else:
                    indexpath = partition
                self.log.info('Index Path:' + indexpath)
                self.hc.lzop_index( path = indexpath)

    def __check_sql(self, sql):
        hive_sql = []
        hql = sql.strip().split('\n')
        for q in hql:
            if q.startswith('--'):
                continue
            hive_sql.append(q)
        return '\n'.join(hive_sql)

    def run_shell_cmd(self, shellcmd, encoding='utf8'):
        sql = self.__check_sql(shellcmd)
        flag = True
        for f in ['DESC FORMATTED', 'SHOW PARTITIONS']:
            if f in sql.upper():
                flag = False
        if flag:
            self.log.info(sql)
        res = subprocess.Popen(sql, shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        results = []
        while True:
            line = res.stdout.readline().decode(encoding).strip()
            if line == '' and res.poll() is not None:
                break
            else:
                results.append(line)
                if flag:
                    self.log.info(line)
        ReturnCode = res.returncode
        if ReturnCode != 0:
            raise Exception('\n'.join(results))
        return [ReturnCode, '\n'.join(results)]
     
    def __parse_table(self, db, table):
        desc_src   = "hive -e'use %s;desc formatted %s'" % (db,table)
        table_info = self.run_shell_cmd(desc_src)

        if table_info[0] != 0:
            if table_info[0] != None:
                raise Exception(self.__class__.__name__+": ReturnCode %s\n" % table_info[0] + table_info[1])
        if table_info[1].strip().split('\n')[-1].startswith('FAILED:'):
            raise Exception(self.__class__.__name__+':DB->%s OR TABLE->%s MYBE NOT FOUND !' % (db,table))
        table_desc = table_info[1].split('OK',1)[1].split('#')
        if len(table_desc) < 2:
            raise Exception(self.__class__.__name__+":Not Find Table %s" % table)

        table_dict = {}
        table_dict.update(( ('TableName', table), ('TableDB', db) ))
        if table_desc[1].strip().startswith('col_name'):
            lines = table_desc[1].strip().split('\n')[1:]
            table_dict['TABLE'] = []
            for line in lines:
                line = line.strip().split()
                if len(line) == 3:
                    line = line[:-1]
                    table_dict['TABLE'].append(' '.join(line))
        else:
             raise Exception(self.__class__.__name__+"SrcTable Fieldes is null")

        if table_desc[2].strip() == 'Partition Information':
            partition = table_desc[3].strip().split('\n')[1:]
            table_dict['PARTITION'] = []
            for line in partition:
                line = line.strip().split()
                if len(line) == 3:
                    line = line[:-1]
                    table_dict['PARTITION'].append(' '.join(line))
        else:
            table_dict['PARTITION'] = None

        for Line in table_info[1].split('\n'):
            Line = Line.strip()
            line = Line.split()

            if Line.startswith('Location:'):
                if len(line) > 1:
                    table_dict[line[0][:-1]] = line[1]
                else:
                    table_dict['Location'] = ''

            elif Line.startswith('field.delim'):
                if len(line) > 1:
                    table_dict[line[0]] = line[1].replace('\\t','\t').replace('\\n','\n')
                else:
                    table_dict['field.delim'] = ''

            elif Line.startswith('line.delim'):
                if len(line) > 1:
                    table_dict[line[0]] = line[1].replace('\\t','\t').replace('\\n','\n')
                else:
                    table_dict['line.delim'] = ''

            elif Line.startswith('Table Type:'):
                if len(line) > 2:
                    table_dict[''.join((line[0],line[1][:-1]))] = line[2]
                else:
                    table_dict['TableType'] = ''

            elif Line.startswith('colelction.delim'):
                if len(line) > 1:
                    table_dict[line[0]] = line[1]

            elif Line.startswith('InputFormat:'):
                if len(line) > 1:
                    table_dict[line[0][:-1]] = line[1]
                else:
                    table_dict['InputFormat'] = ''

            elif Line.startswith('OutputFormat:'):
                if len(line) > 1:
                    table_dict[line[0][:-1]] = line[1]
                else:
                    table_dict['OutputFormat'] = ''

        return table_dict    

    def oneday(self, days, sep='-'): 
        someday = (self.data_day + datetime.timedelta(days=int(days))).strftime('%Y%m%d')        
        return someday.replace('',sep.strip())

    def __partitions(self,db,table):
        rc = self.run_shell_cmd("hive -e'use %s;SHOW PARTITIONS %s'" % (db,table))
        if rc[0] != 0:
            if rc[0] != None:
                self.log.info('show paritions %s' % rc[1])
                raise Exception(self.__class__.__name__+": ReturnCode %s\n" % rc[0] + rc[1])
        if rc[1].strip().split('\n')[-1].startswith('FAILED:'):
            self.log.info('%s.%s Has No PARTITITON !' % (db,table))
            partition_parse = []
        else:
            partition_parse = rc[1].strip().split('OK')[-1].split('\n')[:-1]
            partition_parse = [i.strip()  for i in partition_parse if i.strip() != '']
        return partition_parse

