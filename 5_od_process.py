# -*- encoding: utf-8 -*-


'''
将数据库中基于坐标的od转换成基于block id或者grid id的od记录

** 一般来说执行这个就可以

'''

import time
from collections import namedtuple

from config import config
import psycopg2
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

import fiona

from query import insert_od_data

from tqdm import tqdm


def read_fishnet(shp_path):
    '''读入fishnet数据， 构建R树和坐标-id字典

    Parameter:
    ---------
    shp_path : str
        shapefile文件路径
    
    Returns:
    -------
    rtree : STRtree
        使用格网构建的rtree
    coord_id_dict: dict
        以格网中心为键，格网id为值的字典

    Nots:
    -------

    使用fiona读入shp文件，对于每一个feature字典， 从geometry键获得geojson， 然后转换为shapely几何对象，
    因为shapely的rtree是一个静态的rtree，需要提前知道所有的几何对象，所以先将对象保存在list中，最后构建rtree。

    获得每个几何对象的重心的wkt作为几何体的键（可以知道，每个格网的重心必然不同），以重心为键id为值构建字典

    '''
    c = fiona.open(shp_path)

    coord_id_dict = {}
    geom_list = []
    
    for feature in c:
        geometry = feature['geometry']
        geom = shape(geometry)
        geom_list.append(geom)    

        coord_key = geom.centroid.wkt
        assert(coord_key not in coord_id_dict)
        coord_id_dict[coord_key] = feature['properties']['block_id']

    c.close()

    rtree = STRtree(geom_list)

    return rtree, coord_id_dict

def query_grid_id(fishnet_rtree, coord_id_dict, pnt):
    '''查询出发点或者目的点所在的格网id

    Parameters:
    ------------
    fishnet_rtree : STRtree
        格网rtree
    coord_id_dict : dict
        以格网中心wkt为键格网id为值的字典
    pnt : geometry
        要查询的点几何体
    
    Returns:
    ----------
    fishnet_id : integer
        格网id
    None
        点没有落入任何一个格网中
    



    '''
    candidate_geoms = fishnet_rtree.query(pnt)
    

    true_candidate_geoms = []
    for candidate in candidate_geoms:
        if candidate.intersects(pnt):
            true_candidate_geoms.append(candidate)

    # assert(len(true_candidate_geoms) == 1 or len(true_candidate_geoms) == 0)

    if len(true_candidate_geoms) == 1:
        coord_key = true_candidate_geoms[0].centroid.wkt
        assert(coord_key in coord_id_dict)
        fishnet_id = coord_id_dict[coord_key]
        return fishnet_id        
    else:
        return None



def get_raw_od_data(table_name, limit, offset):
    '''从数据库读取od数据
    Returns:
    ---------
    rows : list
        [ (begin_x, begin_y, end_x, end_y, begin_log_time, distance, travel_time), ...]
    '''
    sql = '''select begin_x, begin_y, end_x, end_y, begin_log_time, end_log_time, distance from {table_name} limit {limit} offset {offset};'''.format(table_name=table_name, limit=limit, offset=offset)
    print(sql)
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





def main():

    fishnet_rtree, coord_id_dict = read_fishnet(r'C:\ex_2020\predict_poi_ratio\shp\shenzhen_street_block_v2\street_block_2_32649.shp')

    begin_tick = time.time()
    limit = 1000000
    offset = 0
    # 读od数据
    cnt = 0
    while True:
        print('limit: {} offset: {}'.format(limit, offset))

        rows = get_raw_od_data('od_2016', limit, offset)
        if len(rows) == 0:
            break
        ODRec = namedtuple('ODRec', ['begin_x', 'begin_y', 'end_x', 'end_y', 'begin_log_time', 'end_log_time', 'distance'])
        recs = []
        

        for row in tqdm(rows):
            od_rec = ODRec(*row)

            # 定位起点的grid id
            o_grid_id = query_grid_id(fishnet_rtree, coord_id_dict, Point(od_rec.begin_x, od_rec.begin_y))
            # 定位终点的grid id
            d_grid_id = query_grid_id(fishnet_rtree, coord_id_dict, Point(od_rec.end_x, od_rec.end_y))

            if o_grid_id is not None and d_grid_id is not None and o_grid_id != d_grid_id:

                distance = od_rec.distance / 1000
                # travel_time = od_rec.travel_time / 60
                # # 计算travel time rate
                # travel_time_rate = travel_time / distance
                # 保存结果
                recs.append((o_grid_id, d_grid_id, od_rec.begin_log_time, od_rec.end_log_time, distance))
                cnt += 1
                
            if len(recs) > 100000:
                print(cnt)
                # 插入数据库
                insert_od_data(recs, 'block_od_2016')
                recs=[]
        if len(recs) > 0:
            insert_od_data(recs, 'block_od_2016')
            
        offset += limit
    
    
    
    
    
    
    print('elapse {}'.format(time.time() - begin_tick))

if __name__ == '__main__':
    main()
