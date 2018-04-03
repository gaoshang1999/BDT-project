--IncidntNum,Category,Descript,DayOfWeek,Date,Time,PdDistrict,Resolution,Address,X,Y,Location,PdId
create table pdi ( 
		incidntnum  String, 
		category  String,
		descript   String,
		dayofweek   String,
		date_   String,
		time_  String,
		pddistrict  String,
		resolution  String, 
		address  String,
		x DECIMAL(30,20),  
		y DECIMAL(30,20),  
		location String, 
		pdid String
)
COMMENT 'Police Department Incidents'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
; 


LOAD DATA LOCAL INPATH '/home/cloudera/SF_Police_Department_Incidents.csv' OVERWRITE INTO TABLE pdi;


 

--sql 1
--select count(*) from pdi;
--2184234

--sql 2
--select count(*) from  (select distinct category from pdi) t;
--39

--sql 3
--select  month(to_date(date_, 'MM/dd/yyyy')) as mon, count(*)  from pdi  group by  month(to_date(date_, 'MM/dd/yyyy')) order by 1;
--12

--sql 4
--select  split(time_, ':')[0]  , count(*)  from pdi  group by split(time_, ':')[0] order by 1;
--24

--sql 5
--select substr(time_, 0, 4)||'0' as ten_minutes, count(*) from pdi group by substr(time_, 0, 4)||'0' order by 1
--144

--sql 6
--select DayOfWeek, count(*) from pdi  group by DayOfWeek  order by DayOfWeek ='Monday' desc, DayOfWeek ='Tuesday' desc, DayOfWeek ='Wednesday' desc, DayOfWeek ='Thursday' desc, DayOfWeek ='Friday' desc, DayOfWeek ='Saturday' desc, DayOfWeek ='Sunday' desc
--7

--sql 7
-- select count(*) from pdi p1, pdi p2 where p1.incidntnum = p2.incidntnum;
-- 2184234