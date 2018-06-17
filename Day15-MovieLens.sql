Movie Lens::
Step 1:: Prepare the data::

create table movies(id int, name string,genres Array<String>)
row format delimited 
fields terminated by '@'
collection items terminated by '|'
stored as textfile;

in order to load data into this folder we need clean .. change :: to  @ 

create table movies_stg(line string);
load data local inpath '/home/osboxes/Downloads/ml-1m1/movies.dat' into table movies_stg;

select * from movies_stg limit 5;

insert overwrite directory '/hive/practice/movies' select regexp_replace(line,"::",'@') from movies_stg;
--------------------------------------
Ratings Data set:::
UserID::MovieID::Rating::Timestamp

use hivepractice;
create table hivepractice.ratings(uid int, mvid int,rating int, time double)
row format delimited 
fields terminated by '@'
stored as textfile;

select * from hivepractice.ratings;
alter table ratings change time time string;
select * from hivepractice.ratings limit 10;

create table ratings_stg(line string);
load data local inpath '/home/osboxes/Downloads/ml-1m1/ratings.dat' into table ratings_stg;

select * from ratings_stg limit 5;

insert overwrite directory '/hive/practice/ratings' select regexp_replace(line,"::",'@') from ratings_stg;

122  yarn application -list
  123  yarn application -status application_1529246517471_0002
  124  yarn application -kill application_1529246517471_0002
  125  yarn logs -applicationId  application_1529246517471_0002

------------------------------------------
--------------------------------------
Users Data set:::
UserID::Gender::Age::Occupation::Zip-code


use hivepractice;
create table hivepractice.users(uid int, gender string,age int, occupation int,zip int)
row format delimited 
fields terminated by '@'
stored as textfile;

select * from hivepractice.users;
select * from hivepractice.users limit 10;

create table users_stg(line string);
load data local inpath '/home/osboxes/Downloads/ml-1m1/users.dat' into table users_stg;

select * from users_stg limit 5;

insert overwrite directory '/hive/practice/users' select regexp_replace(line,"::",'@') from users_stg;


select * from hivepractice.users;
select * from hivepractice.users limit 10;


---------------------------------------------------------
Q1 Top 10  most viewed movies with names::
---------------------
part-1: ratings data
select count(mvid) from ratings group by mvid order by count(mvid) desc;
select mvid,count(mvid) cnt from ratings group by mvid order by cnt desc limit 10;
f,w,g,h,select,limit,order

select 
r.mvid movie_id,
count(r.mvid) cnt,
m.name as movie_name 
from ratings r 
left join movies m on r.mvid=m.id 
group by r.mvid,m.name order by cnt desc limit 10;

select A.mvid,A.cnt,m.name
from (select mvid,count(mvid) cnt 
from ratings group by mvid order by cnt desc limit 10) A
join movies m on m.id=A.mvid ;

create table q1_res as select A.mvid,A.cnt,m.name
from (select mvid,count(mvid) cnt 
from ratings group by mvid order by cnt desc limit 10) A
join movies m on m.id=A.mvid ;

or else
create table q1_res(mvid int,mvcount int,mvname string)
stored as parquet;

insert overwrite table q1_res 
select A.mvid,A.cnt,m.name
from (select mvid,count(mvid) cnt 
from ratings group by mvid order by cnt desc limit 10) A
join movies m on m.id=A.mvid ;


---------------------------


Q2 -> top 20 rated movies , movie should be rated by 40

take average rating by movieid, condition count(users) >=40

select 
sum(r.rating) ,
count(r.rating),
avg(rating) avgrating,
r.mvid,
m.name
from ratings r left join movies m on r.mvid = m.id 
group by r.mvid ,m.name
having count(r.uid)>40
order by avgrating desc 
limit 10;

select A.* from
	(select 
		sum(r.rating) sum_rating,
		count(r.rating) cnt_rating,
		avg(rating) avgrating,
		r.mvid,
		m.name,
		rank() over (order by avg(rating) desc )  rnk
	from ratings r left join movies m on r.mvid = m.id 

	group by r.mvid ,m.name
	having count(r.uid)>40 
) A
where A.rnk<=20


from where group having select limit order


10 -1 -  1
10 -1 -1 
20 -3 -2
20 -3 -2
20 -3 -2
30 -6 -3
40 -7 -4
50 -8 -5
50 -8 -5
60 -10 -6
  rank, dense_rank



use hivepractice;
create table hivepractice.q2_res as 
select A.* from
	(select 
		sum(r.rating) sum_rating,
		count(r.rating) cnt_rating,
		avg(rating) avgrating,
		r.mvid,
		m.name,
		rank() over (order by avg(rating) desc )  rnk
	from hivepractice.ratings r left join hivepractice.movies m on r.mvid = m.id 

	group by r.mvid ,m.name
	having count(r.uid)>40 
) A
where A.rnk<=20;
