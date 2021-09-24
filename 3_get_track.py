# -*- encoding: utf-8 -*-

'''
从原始的gps记录中提取行程，把连续的一段 on_service 当作一段行程
'''

import time
from collections import namedtuple, defaultdict

import psycopg2
import numpy as np
import fiona
from shapely.ops import unary_union
from shapely.geometry import shape, Point

import matplotlib.path as mpltPath

from config import config

'''
从原始的gps记录中提取出od track

notes:
---------------
1. 读入原始数据
2. 保留在深圳市内的记录
3. 提取od track
4. 过滤时长<180s的od track

是否应该进一步过滤异常值？如何过滤？



'''




def get_raw_log_data(day, hour=18):
    '''从数据库读取od数据

    Parameters:
    ----------
    day : int
        9月的日期
    hour : int
        小时{0-23}

    Returns:
    ---------
    rows : list
        [ (log_time, car_id, on_service, x, y, v), ...]
    '''
    sql = '''select log_time, car_id, on_service, ST_X(geom), ST_Y(geom), velocity from gps_log_5_{day} where EXTRACT(HOUR FROM log_time) = {hour} order by car_id, log_time;'''.format(day=day, hour=hour)
    
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()        
        cur.execute(sql)        
        rows = cur.fetchall()
        cur.close()
        return rows
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def create_track_table(day, hour):
    '''
    在数据库中创建OD track表


    '''
    
    sql = '''
    drop table if exists gps_log_{day}_{hour}_track;
    create table gps_log_{day}_{hour}_track
    (
        uuid SERIAL,
        track_id integer,
        log_time timestamp,
        car_id text,
        x float,
        y float,
        v float
    );    
    '''.format(day=day, hour=hour)
    # print(sql)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()  






def insert_track(day, hour, recs):

    

    '''
    将OD track插入数据库
    
    '''
    begin_tick = time.time()
    table_name = 'gps_log_{day}_{hour}_track'.format(day=day, hour=hour)
    sql = '''
        INSERT INTO {}(track_id, log_time, car_id, x, y, v) VALUES(%s, %s, %s, %s, %s, %s)
    '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, recs)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
    

    print('insert elapse {}'.format(time.time() - begin_tick))






def get_od_track(day, hour):
    '''
    从原始gps记录中获取OD track

    Prameters:
    ---------
    day : int
        日期
    hour : int
        小时
    border_path : Path
        限定polygon的path
    
    Notes:
    --------
    
    
    '''
    
    LogTuple = namedtuple('LogTuple', ['log_time', 'car_id', 'on_service', 'x', 'y', 'v'])

    id_logs_dict = defaultdict(list)


    rows = get_raw_log_data(day, hour)

    
    for idx, row in enumerate(rows):
        log_tuple = LogTuple(*row)
        id_logs_dict[log_tuple.car_id].append(log_tuple)
        
    del rows

    
    track_list = []

    for car_id, logs in id_logs_dict.items():        
        track = []
        for log in logs:
            if not log.on_service:
                if len(track) > 0:
                    track_list.append(track)
                    track = []
            else:
                track.append(log)
        if len(track) > 0:
            track_list.append(track)
        

    recs = []
    
    for idx, track in enumerate(track_list):
        if len(track) < 3:
            continue

        begin_time = track[0].log_time
        end_time = track[-1].log_time
        travel_time = (end_time-begin_time).total_seconds()

        if travel_time < 180:
            continue

        for log in track:
            # 插入数据库
            recs.append((idx, log.log_time, log.car_id, log.x, log.y, log.v))

    create_track_table(day, hour)
    insert_track(day, hour, recs)
    


        
if __name__ == '__main__':

    # border = read_border_polygon()
    # border_path = mpltPath.Path(border.exterior.coords)

    
    day_list = list(range(1, 31+1))
    day_list.remove(7)
    
    for day in day_list:
        for hour in range(0, 23+1):
            begin_tick = time.time()            
            get_od_track(day, hour)
            print('day: {} , hour: {} elapse {}'.format(day, hour, time.time() - begin_tick))