
from time import time
import shutil
from tqdm import tqdm


'''
因为在数据库中建立的表，是以天为单位来组织的，所以
将每个小时的文件合并为一个大文件，再一次导入数据库。

一个小时一个小时的insert速度比较慢

'''

def merge_day(day):
    # r无法处理直接跟在引号前面的\ ???
    base_name = r'F:\taxi\shenzhen_2009_5_taxi_data\XXYGPSDATA\2009\5\{}'.format(day)

    output_file = base_name + r'\all.csv'

    input_file_list = [base_name + r'\{}.txt'.format(hour) for hour in range(24)]

    print(output_file)
    print(input_file_list)

    begin_tick = time()

    with open(output_file,'wb') as wfd:
        for f in input_file_list:
            with open(f,'rb') as fd:
                shutil.copyfileobj(fd, wfd, 1024*1024*10)

    print(time() - begin_tick)


day_list = list(range(1, 31+1))
day_list.remove(7)

for day in tqdm(day_list):
    merge_day(day)

