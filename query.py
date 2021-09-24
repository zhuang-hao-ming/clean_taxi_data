from config import config
import psycopg2


def copy_day_log_table_from_file(file_name, day_log_table_name):
    sql = '''
    select copy_day_log_table_from_file(%s, %s)        
    '''
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (file_name,day_log_table_name))
        conn.commit()
        cur.close()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

def make_day_gps_log_table_idx(table_name):
    sql = '''
    select make_gps_log_table_idx(%s)
    '''
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(sql, (table_name,))
        conn.commit()
        cur.close()
        return True
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return False
    finally:
        if conn is not None:
            conn.close()

def insert_od_data(od_rec_list, table_name):
    '''将定位到格网的od记录插入数据库

    Parameters:
    ----------
    od_rec_list : list
        元组列表

    '''
    sql = '''
        INSERT INTO {}(o_id, d_id, pickup_time, dropoff_time, distance) VALUES(%s, %s, %s, %s, %s);
    '''.format(table_name)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.executemany(sql, od_rec_list)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
