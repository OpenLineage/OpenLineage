create table if not exists jdbc_source1 (k int, j1 varchar(5));
create table if not exists jdbc_source2 (k int, j2 varchar(5));
create table if not exists jdbc_source3 (k int, j3 varchar(5));
create table if not exists jdbc_source4 (k int, j4 varchar(5));

insert into jdbc_source1 values (1 , 'ja');
insert into jdbc_source1 values (2 , 'jc');
insert into jdbc_source1 values (3 , 'je');
insert into jdbc_source2 values (1 , 'jb');
insert into jdbc_source2 values (2 , 'jd');
insert into jdbc_source2 values (3 , 'jf');
