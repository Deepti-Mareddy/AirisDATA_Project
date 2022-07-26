
#! /bin/bas

# here all the variables inside the config will be available
source /home/hduser/info_hive.config

hive <<EOF
create database ${database};
use ${database};
create external table ${table1}(id int,name string,rating string,genre string,year int,released_date date,score float,votes int,director string,writer string,star string,country string,budget bigint,gross bigint,company string,runtime int) row format delimited fields terminated by ","stored as textfile;
load data inpath "${path_table1}" into table ${table1};
Select year,genre,count(genre) from ${table1} group by year,genre;
Select * from(select id,name,genre,country,gross,row_number() over( partition by country order by gross desc) r from ${table1}) t where r<=3 ;
select director,star from ${table1} group by director,star having count(star)>5 order by star;
Select company,num,genre from (select company,count(*) as num,genre,rank() over (partition by company order by count(*) desc) as rnk from ${table1} group by company,genre) t where t.rnk=1 order by num;  
create table ${table2}(id int,name string,rating string,genre string,released_date date,score float,votes int,director string,writer string,star string,country string,budget bigint,gross bigint,company string,runtime int)partitioned by (year int) row format delimited fields terminated by ","stored as textfile;
Insert into ${table2} partition(year) select id,name,rating,genre,released_date,score,votes,director,writer,star,country,budget,gross,company,runtime,year from ${table1};




