/* 
======================================================================================
creates database and schemas
======================================================================================

script puropse:

checks for the database to exist and drops if already exits
creates a database datawarehouse
uses the datawarehouse database
çreates bronze,silve,gold schemas inside datwarehouse

============================================================================================
WARNING::::

this script deletes the pre-existing database if it exists and deltes all its content and schemas
only run the script if all the necessary backup already exists

it creates a new database and adds schemas to it
==============================================================================================
*/


-- use master
use master;

go 

-- checking if database exists
if exists(select 1 from sys.databases where name = 'datawarehouse')
begin
	alter database datawarehouse set single_user with rollback immediate;
	drop database datawarehouse;
end

go
-- create database
create database datawarehouse;

go

-- use datawarehouse
use datawarehouse;

go

-- creating schemas 
create schema bronze;
go
create schema silver;
go
create schema gold;
