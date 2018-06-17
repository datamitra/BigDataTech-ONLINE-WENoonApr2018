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


---------------------------------------------------------






