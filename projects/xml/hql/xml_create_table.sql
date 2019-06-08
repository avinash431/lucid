create database lucid if not exists;
use lucid;

create  table if not exists xml_temp(line string);

create  table if not exists books_temp(
author string,
title array<string>,
genre array<string>,
price array<string>,
discount array<string>,
publish_date array<string>,
description array<string>)
row format delimited
fields terminated by '|';
 
create  table if not exists books_temp_title(
seqno int,
author string,
title string)
row format delimited
fields terminated by '|';
	   
create  table if not exists books_temp_genre(
seqno int,
author string,
genre  string)
row format delimited
fields terminated by '|';
	   
create table if not exists books_temp_price(
seqno int,
author string,
price string)
row format delimited
fields terminated by '|';

create table if not exists books_temp_discount(
seqno int,
author string,
discount string)
row format delimited
fields terminated by '|';

create table if not exists books_temp_publish_date(
seqno int,
author string,
publish_date string)
row format delimited
fields terminated by '|';

create table if not exists books_temp_description(
seqno int,
author string,
description string)
row format delimited
fields terminated by '|';
