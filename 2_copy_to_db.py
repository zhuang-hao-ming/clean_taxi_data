
from time import time
from query import copy_day_log_table_from_file, make_day_gps_log_table_idx

def copy_all_to_db(file_name='D:/data/09/01/20160901_001-utf.txt', table_name='gps_log_9_1'):
    begin_tick = time()
    pre_tick = time()
    
    
    copy_day_log_table_from_file(file_name, table_name)

    print('create day_log_table: consume {}; {} has elapsed'.format(time() - pre_tick, time() - begin_tick))
    pre_tick = time()

    make_day_gps_log_table_idx(table_name)
    print('create day_log_table idx: consume {}; {} has elapsed'.format(time() - pre_tick, time() - begin_tick))
    pre_tick = time()


if __name__ == '__main__':

    day_list = list(range(1, 31+1))
    day_list.remove(7)
    for day in day_list:
        
        # 将gps记录文件导入数据库
        file_name = 'F:/taxi/shenzhen_2009_5_taxi_data/XXYGPSDATA/2009/5/{}/all.csv'.format(day)
        table_name = 'gps_log_5_{}'.format(day)

        print(file_name)
        print(table_name)
        copy_all_to_db(file_name, table_name)

        
    
    