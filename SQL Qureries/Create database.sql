create database SSIS_Telecom_DB


use SSIS_Telecom_DB
go


create table fact_transaction (
	id int not null identity (1,1) primary key, -- surrogate key
	transaction_id int not null , 
	imsi varchar(9) not null,
	subscriber_id int,
	tac varchar(8) not null,
	snr varchar(6) not null,
	imei varchar(14) not null,
	cell int not null,
	lac int not null,
	event_type varchar(1) null,
	event_ts datetime not null,
);



create table error_destination_output (
    id int,
    imsi varchar(9),
    imei varchar(14),
    cell int,
    lac int,
    event_type varchar(1),
    event_ts datetime,
	tac varchar(8),
	snr varchar(8),
    ErrorCode int,
    ErrorColumn int
)



select * from fact_transaction

select * from error_destination_output

select * from dim_imsi_reference