# -*- encoding: utf-8 -*-

'''

这个程序的目的是读取“按照每小时存储的原始出租车数据”，转换成开始位置，结束位置，开始时间，结束时间，行程距离用来存储

** 已经执行过了，不用再执行一遍了

'''

import time
from datetime import datetime
from collections import namedtuple, defaultdict

import numpy as np
import psycopg2

from tqdm import tqdm

from config import config

def insert_od(recs, table_name):

    '''
    将OD插入数据库
    
    '''
    begin_tick = time.time()
    
    sql = '''
        insert into {}(begin_x, begin_y, end_x, end_y, begin_log_time, end_log_time, distance) values(%s, %s, %s, %s, %s, %s, %s);
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
    '''从数据库读取od track数据

    Parameters:
    ----------
    day : int
        9月的日期
    hour : int
        小时{0-23}

    Returns:
    ---------
    rows : list
        [ (track_id, x, y, log_time), ...]
    '''
    sql = '''select track_id, st_x(geom) as x, st_y(geom) as y, log_time from gps_log_{day}_{hour}_track order by track_id, uuid;'''.format(day=day, hour=hour)
    
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

def read_od_from_db(day, hour):
    '''从数据库中读取并处理出day,hour的od,然后插入数据库


    notes:
    --------
    [
        
        (x1, y1, x2, y2, begin_time, distance, travel_time)
        ...
        ...
    ]

    '''

    TrackRec = namedtuple('TrackRec', ['track_id', 'x', 'y', 'log_time'])
    track_id_recs = defaultdict(list)

    rows = get_od_track(day, hour)
    for row in tqdm(rows):
        track_rec = TrackRec(*row)
        track_id_recs[track_rec.track_id].append(track_rec)
    
    results = []
    out_cnt = 0
    for track_id, recs in track_id_recs.items():
        distance = 0
        begin_rec = recs[0]
        end_rec = recs[-1]

        pre_pnt = np.array([begin_rec.x, begin_rec.y])
        for rec in recs[1:]:
            pnt = np.array([rec.x, rec.y])            
            distance += np.linalg.norm(pnt-pre_pnt)
            pre_pnt= pnt
        
        travel_time = (end_rec.log_time - begin_rec.log_time).total_seconds()

        v = distance / travel_time # m/s


        if v < 1 or v > 22.22:
            continue
            out_cnt +=1


        results.append((begin_rec.x, begin_rec.y, end_rec.x, end_rec.y, begin_rec.log_time, end_rec.log_time, distance))

    print(out_cnt)
    insert_od(results, 'od_2016')

    

def main():
    begin_tick = time.time()
    
    day_list = list(range(1, 30+1))


    for day in day_list:
        for hour in range(0, 23+1):
            read_od_from_db(day, hour)
            print('elapse time {} {} {}'.format(time.time() - begin_tick, day, hour))
            

if __name__ == '__main__':
    main()