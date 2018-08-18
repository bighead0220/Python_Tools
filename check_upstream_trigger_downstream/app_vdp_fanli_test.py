#!/usr/bin/env python3                                                                                           
"""                                                                                                              
USAGE: 返利                                                                       
AUTHOR: liuxiaoze                                                                                             
CREATED AT: 2017年7月3日15:59:51                                                           
"""                                                                                                              
                                                                                                                 
import sys                                                                                                       
import time   
import calendar
#sys.path.append('/home/mart_vdp/task/edw/etl/app/bin/common/')                                                  
#sys.path.append('/home/mart_vdp/task/edw/etl/app/bin/vc/common/')                                               
sys.path.append('common/')                                                                                       
                                                                                                                 
from datetime_util import DateTimeUtil                                                                           
from HiveTask import HiveTask                                                                                    
                                                                                                                 
class FANLI:                                                                                            
                                                                                                                 
    def __init__(self, jobDate, hiveTask):                                                                       
        self.db_name = 'app'                                                                                     
        self.location_prefix = '/user/mart_vdp/' + self.db_name + '/vdp_base/'                                   
        self.ht = hiveTask
        self.yesterday = DateTimeUtil.oneday_by_date(jobDate, -1, '%Y-%m-%d')   
        self.end_day=str(calendar.monthrange(int(self.yesterday[0:4]),int(self.yesterday[5:7]))[1])
        self.month_start = DateTimeUtil.oneday_by_date(jobDate[0:8] + '01',-1, '%Y-%m-%d')[0:8]+'01'
        self.month_end = DateTimeUtil.oneday_by_date(jobDate[0:8] + '01',-1, '%Y-%m-%d')
        #self.month_end = jobDate[0:8] + self.end_day
        print('数据日期：' + self.yesterday)                                                                          
    
                                                                                           
    def run_sql(self):                                                                                                                                                                                            
        sql = """
        create table if not exists app_vdp_fanli_test                                                      
        ( 
           sku_id                     string     comment 'skuid'
					,period                     string     comment '受益期间'
					,supplier_code              string     comment '供应商代码'
					,item_first_cate_cd         string     comment '一级品类id'
					,item_first_cate_name       string     comment '一级品类名称'
					,item_second_cate_cd        string     comment '二级品类id'
					,item_second_cate_name      string     comment '二级品类名称'
					,item_third_cate_cd         string     comment '三级品类id'
					,item_third_cate_name       string     comment '三级品类名称'
					,apportion_amount						double		 comment '返利'
         )                                                                                                       
        comment '返利'                                                                                        
        partitioned by (dt string)                                                                               
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'                                    
        WITH SERDEPROPERTIES (                                                                                   
        'field.delim'='\t'                                                                                       
        ) STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'                                 
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'                                
        location '""" + self.location_prefix + """app_vdp_fanli_test';"""                                                                                                                                                 
        sql += """                                                                                         
            set mapred.output.compress=true;                                                                     
            set hive.exec.compress.output=true;                                                                  
            set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;                            
            set io.compression.codecs=com.hadoop.compression.lzo.LzopCodec;
            INSERT overwrite table app_vdp_fanli_test partition (dt='""" + self.month_end + """')
						select 
									 sku_id            
					,period                    
					,supplier_code             
					,item_first_cate_cd        
					,item_first_cate_name      
					,item_second_cate_cd       
					,item_second_cate_name     
					,item_third_cate_cd        
					,item_third_cate_name      
					,apportion_amount						
						from app.app_vdp_fanli
						where dt='2017-11-30'
						;                     
            """
        self.ht.exec_sql(schema_name=self.db_name, sql=sql)
              
    def do_job(self):
        """
        执行job
        """
        self.run_sql()


    def run(self):
        self.do_job()

def main():
    ht = HiveTask()
    jobDate = DateTimeUtil.get_format_today('%Y-%m-%d')
    if len(sys.argv)==2:
        jobDate=sys.argv[1]
    main_job = FANLI(jobDate, ht)
    main_job.run()

if __name__ == '__main__':
    main()

