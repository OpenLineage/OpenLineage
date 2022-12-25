create user hiveuser with encrypted password 'password';

create database metastore23;
grant all privileges on database metastore23 to hiveuser;

create database metastore31;
grant all privileges on database metastore31 to hiveuser;