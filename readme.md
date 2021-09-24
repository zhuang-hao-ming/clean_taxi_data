## 清理出租车数据的脚本

这个仓库存储了关于清理原始出租车数据的一些脚本。包括数据的入库，行程的提取，od的构建

01. config.py

连接数据库的配置文件

02. query.py

一些操作数据库的函数

03. main.sql

sql函数

1. merge_files.py

合并按照小时组织的原始txt文件为按照天组织的txt文件，这一步的目的是为了减少将数据复制到数据库的时间

2. copy_to_db.py

将按照天组织的txt文件复制到数据库中建表和索引

3. get_track.py

从原始的gpd记录中提取连续的一段 on_service当作行程

4. get_od_table.py

从行程中提取起点坐标，终点坐标，起点时间，终点时间，行程距离构建od记录

5. od_process.py

将坐标形式的od转换为 block id或者grid id形式的