
select count(*) from pdi;

select   category, count(*) from pdi group by category order by 2 desc;


select  month(to_date(Date, 'MM/dd/yyyy'))  as mon, count(*)  from pdi  
group by  month(to_date(Date, 'MM/dd/yyyy'))   order by 1;


select  split(Time, ':')[0]  , count(*)  from pdi  
group by split(Time, ':')[0] order by 1;


select substr(Time, 0, 4)||'0' as ten_minutes, count(*) from pdi group by substr(Time, 0, 4)||'0' order by 1 ;


select DayOfWeek, count(*) from pdi  group by DayOfWeek  order by 
DayOfWeek ='Monday' desc, DayOfWeek ='Tuesday' desc, DayOfWeek ='Wednesday' desc, DayOfWeek ='Thursday' desc
, DayOfWeek ='Friday' desc, DayOfWeek ='Saturday' desc, DayOfWeek ='Sunday' desc;



select count(*) from pdi p1, pdi p2 where p1.incidntnum = p2.incidntnum;