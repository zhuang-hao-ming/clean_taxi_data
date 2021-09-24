

--- copy_day_log_table_from_file
--- @param file_name {{VARCHAR}} 文本文件名
--- @param table_name {{VARCHAR}} 数据表名
--- 1. 建立内部临时表
--- 2. 使用copy将文本数据导入临时表
--- 3. 从临时表中做查询处理建立 day log table
CREATE OR REPLACE FUNCTION copy_day_log_table_from_file(file_name VARCHAR, table_name VARCHAR)
RETURNS VOID
AS 
$$
DECLARE
	copy_command VARCHAR;
	create_table_command VARCHAR;
BEGIN
	DROP TABLE IF EXISTS __gps_log_table; --- 内部使用的辅助表
	CREATE TABLE __gps_log_table
 	(
 	    
 	    log_date TEXT,
 	    log_time TEXT,
 	    unknown_1 TEXT, -- 未知
 	    car_id TEXT, -- 出租车id
 	    x FLOAT,
 	    y FLOAT,
 	    velocity FLOAT, -- 速度 m/s
 	    direction SMALLINT, -- 方向角
 	    on_service BOOLEAN, -- 是否载客
 	    unknown_2 TEXT    -- log是否有效
 	);
 	copy_command := format('COPY __gps_log_table FROM %L WITH (FORMAT csv);', file_name);
 	
 	EXECUTE copy_command; --- 将文本文件copy入数据库

	
	create_table_command := format('
	CREATE TABLE %I AS
	SELECT to_timestamp(log_date || lpad(log_time, 6, ''0''), ''YYYYMMDDHH24MISS'') as log_time,
		car_id,
		velocity,
		direction,
		on_service,
		ST_Transform(ST_GeomFromText(''POINT('' || x || '' '' || y || '')'', 4326), 32649) as geom 
	FROM __gps_log_table
	WHERE x > 113 AND x < 115 AND y > 22 AND y < 23;
	', table_name);
	
	EXECUTE format('DROP TABLE IF EXISTS %I;', table_name); --- 如果数据表已经存在则删除
	EXECUTE create_table_command; --- 从inner table创建新数据表
	
	
	

END;
$$
LANGUAGE plpgsql;



--- make day gps log table index
--- @param table_name {{varchar}} 数据表名
--- 清理day log table包括
--- 1. 在log_time上建立索引
--- 2. 基于log_time索引做空间聚集
--- 3. 重新统计查询计划信息
--- 表的大小约4千万，以上操作耗时约5分钟，后面会对该表做24次基于log_time的查询建立hour log table
--- 分析，这个索引是否有收益？ 没有收益
CREATE OR REPLACE FUNCTION make_gps_log_table_idx(table_name VARCHAR)
RETURNS VOID
AS 
$$
DECLARE
	idx_name VARCHAR := table_name || '_idx';
BEGIN
    SET maintenance_work_mem TO '2047MB'; -- 调大maintenance_work_mem，可以加快建立索引的速度
    EXECUTE format('CREATE INDEX %I ON %I(log_time);', idx_name, table_name);
    EXECUTE format('CLUSTER %I USING %I;', table_name, idx_name);
    EXECUTE format('ANALYZE %I;', table_name);
    SET maintenance_work_mem TO '16MB'; -- 恢复maintenance
END;
$$
LANGUAGE plpgsql;


select copy_day_log_table_from_file('F:/taxi/shenzhen_2009_5_taxi_data/XXYGPSDATA/2009/5/14/all.csv', 'gps_log_5_14')

SELECT COUNT(*) FROM gps_log_5_14 LIMIT 1;


explain select log_time, car_id, on_service, ST_X(geom), ST_Y(geom), velocity from gps_log_5_11 where EXTRACT(HOUR FROM log_time) = 8 order by car_id, log_time;
explain select log_time, car_id, on_service, ST_X(geom), ST_Y(geom), velocity from gps_log_5_11 where EXTRACT(HOUR FROM log_time) = 8;
analyze gps_log_5_11;




drop table if exists od_2009;
create table od_2009
(
    begin_x float,
    begin_y float,
    end_x float,
    end_y float,
    begin_log_time timestamp,
    end_log_time timestamp,
    distance float
)
 

select count(*) from od_2009;

drop table if exists od_2009_process;
create table od_2009_process
(
    o_id int,
    d_id int,
    pickup_time timestamp,
    dropoff_time timestamp,
    distance float
)

select count(*) from od_2009_process;





 
