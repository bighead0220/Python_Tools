#!/usr/bin/env python3
"""
USAGE: 
AUTHOR: 
CREATED AT: {{create_time}}
Dependent-{{dependent}}
"""

import sys
import time


sys.path.append('common/')

from datetime_util import DateTimeUtil
from HiveTask import HiveTask


class {{table_name}}:
    def __init__(self, jobDate, hiveTask):
        self.db_name = 'app'
        self.location_prefix = '/user/mart_vdp/' + self.db_name + '/vdp_base/'
        self.ht = hiveTask
        self.jobDate = jobDate
        self.yesterday = DateTimeUtil.oneday_by_date(jobDate, -1, '%Y-%m-%d')
        self.before_yesterday = DateTimeUtil.oneday_by_date(jobDate, -2, '%Y-%m-%d')
        self.three_days_ago = DateTimeUtil.oneday_by_date(jobDate, -3, '%Y-%m-%d')
        self.last_month_start = DateTimeUtil.oneday_by_date(jobDate[0:8] + '01', -1, '%Y-%m-%d')[0:8] + '01'
        self.last_month_end = DateTimeUtil.oneday_by_date(jobDate[0:8] + '01', -1, '%Y-%m-%d')
        print('数据日期：' + self.yesterday)

    def run_sql(self):
        sql = """
        create external table if not exists {{table_name}}
        (
              {{fields_name}}
         )
        comment '{{comment}}'
        partitioned by ({{partition}})
        ROW FORMAT delimited fields terminated by '\t'         
        lines terminated by '\n'
        STORED AS orc
        location '""" + self.location_prefix + """{{table_name}}';"""
        sql += """
            set mapred.output.compress=true;
            set hive.exec.compress.output=true;
            set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
            set io.compression.codecs=com.hadoop.compression.lzo.LzopCodec;
            add jar common/Hive_Vdp_Udf_Jar.jar;
						create temporary function skuType as 'com.jd.vdp.udf.ConvertSkuDataTypeUtil';

						insert overwrite table {{table_name}} partition ()
						{{sql_content}}
            """
        self.ht.exec_sql(schema_name=self.db_name, sql=sql)
		
   

		

    def do_job(self):
      self.run_sql()
    

    def run(self):
        self.do_job()


def main():
    ht = HiveTask()
    jobDate = DateTimeUtil.get_format_today('%Y-%m-%d')
    if len(sys.argv) == 2:
        jobDate = sys.argv[1]
        main_job = {{table_name}}(jobDate, ht)
        main_job.run()
    elif len(sys.argv) == 3:
        sDate = sys.argv[1]
        eDate = sys.argv[2]
        datediff = DateTimeUtil.datediff(sDate, eDate, '%Y-%m-%d')
        for i in range(0, datediff+1):
            jobDate = DateTimeUtil.oneday_by_date(sDate, i, '%Y-%m-%d')
            main_job = {{table_name}}(jobDate, ht)
            main_job.run()
    else:
            main_job = {{table_name}}(jobDate, ht)
            main_job.run()

if __name__ == '__main__':
    main()

