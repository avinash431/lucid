use lucid;

insert overwrite table books_temp
select xpath_string(line, 'book/author'),
       xpath(line, 'book/title/text()'),
       xpath(line, 'book/genre/text()'),
       xpath(line, 'book/price/text()'),
       xpath(line, 'book/price/@discount'),
       xpath(line, 'book/publish_date/text()'),
       xpath(line, 'book/description/text()')
from   xml_temp;

insert overwrite table books_temp_title
select  row_number() over(), author, btitle
from    books_temp
lateral view explode(title) t as btitle;

insert overwrite table books_temp_genre
select  row_number() over(), author, bgenre
from    books_temp
lateral view explode(genre) g as bgenre;

insert overwrite table books_temp_price
select  row_number() over(), author, bprice
from    books_temp
lateral view explode(price) p as bprice;

insert overwrite table books_temp_discount
select  row_number() over(), author, bdiscount
from    books_temp
lateral view explode(discount) d as bdiscount;

insert overwrite table books_temp_publish_date
select  row_number() over(), author, bpublish_date
from    books_temp
lateral view explode(publish_date) pb as bpublish_date;

insert  overwrite table books_temp_description
select  row_number() over(), author, bdescription
from    books_temp
lateral view explode(description) d as bdescription;
