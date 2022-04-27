--------------------------------------
----------context setup---------------
--------------------------------------
use role sysadmin;
use warehouse compute_wh;

create or replace database demo_lab;
use database demo_lab;
use schema demo_lab.public;

---------------------------------------------------------------------
--Demo1 
--laod two type of flat files from diffrent sources into a table vegetable_details
--file1: veggies_root_depth_pipe.txt
--file2: veggies_root_depth_comma_opt_enclosed.csv
--open it in notepad choose the right file format properties
--from WEB_UI load it into the table vegetable_details
---------------------------------------------------------------------

create table demo_lab.public.vegetable_details
(   plant_name varchar(25)
  , root_depth_code varchar(1)    
);

CREATE OR REPLACE FILE FORMAT demo_lab.public.veg_rd_pipe 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = '|' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
    TRIM_SPACE = TRUE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('\\N');
    
CREATE OR REPLACE FILE FORMAT demo_lab.public.veg_rd_comma_opt_enclosed 
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('\\N');
    
--------------------------------------------------------------------
-- Prepare Data to Load --
--------------------------------------------------------------------
-- create external stage with aws public bucket url
-- check the values specified for the properties in a stage
-- list out the files in the external_stage
-- to view data 
-- https://uni-lab-files.s3.us-west-2.amazonaws.com/LU_SOIL_TYPE.tsv 
--------------------------------------------------------------------
create or replace stage demo_lab.public.demo_lab_data_aws_s3_public 
    url = 's3://uni-lab-files'
    --file_format = (type = 'csv', field_delimiter = ',', skip_header = 0) --<optional>
    ;
desc stage demo_lab.public.demo_lab_data_aws_s3_public;

list @demo_lab.public.demo_lab_data_aws_s3_public;

---------------------------------------------------------------------
-- Demo2
-- create a file format and load LU_SOIL_TYPE.tsv file from external stage 
---------------------------------------------------------------------

create or replace file format demo_lab.public.lu_soil_file_tsv_format
  type = 'csv' 
  field_delimiter = '\t'
  skip_header = 1
  ;

create or replace table demo_lab.public.soil_type
(    
    filename varchar
  , file_row_number varchar
  , SOIL_TYPE_ID number	
  , SOIL_TYPE varchar(15)
  , SOIL_DESCRIPTION varchar(75)
);

copy into demo_lab.public.soil_type
from (select metadata$filename, 
             metadata$file_row_number, 
             t.$1, t.$2, t.$3 
             from @demo_lab.public.demo_lab_data_aws_s3_public/LU_SOIL_TYPE.tsv (file_format =>'lu_soil_file_tsv_format') t);
             
select * from demo_lab.public.soil_type;

------------------------------------------------------------------------
--Demo3 
--Loading simple Semistructure data file and query the data
--https://uni-lab-files.s3.us-west-2.amazonaws.com/author_with_header.json
--list @demo_lab.public.demo_lab_data_aws_s3_public/author_with_header.json;
------------------------------------------------------------------------

create or replace table demo_lab.public.author_ingest_json (
 raw_author variant
);             

create or replace FILE FORMAT demo_lab.public.json_file_format
       TYPE = 'JSON' 
       COMPRESSION = 'AUTO' 
       ENABLE_OCTAL = FALSE 
       ALLOW_DUPLICATE = FALSE 
       STRIP_OUTER_ARRAY = FALSE 
       STRIP_NULL_VALUES = FALSE 
       IGNORE_UTF8_ERRORS = FALSE;

copy into demo_lab.public.author_ingest_json 
from @demo_lab.public.demo_lab_data_aws_s3_public
files = ('author_with_header.json') //if we have multiple files to load give file name "," here or use pattren with regex into a single table
file_format =(format_name = json_file_format);

select raw_author from author_ingest_json;

--create or replace view author_details as
select 
 raw_author['AUTHOR_UID']::number as AUTHOR_ID //index based 
,raw_author:FIRST_NAME::STRING as FIRST_NAME //specifying : (colon) // ::(double colon) means type casting or assign data type 
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
from author_ingest_json;

-----------------------------------------------------------
--Demo4
--nested json load
--s3://uni-lab-files/json_book_author_nested.txt
-- to view data 
-- https://uni-lab-files.s3.us-west-2.amazonaws.com/json_book_author_nested.txt 
----------------------------------------------------------
create or replace table demo_lab.public.author_nested_ingest_json (
 raw_nested_book variant
);             

copy into demo_lab.public.author_nested_ingest_json 
from @demo_lab.public.demo_lab_data_aws_s3_public
files = ('json_book_author_nested.txt')
file_format =(format_name = json_file_format);

select 
       raw_nested_book as raw_book_details, 
       raw_nested_book:book_title :: string as bookTitle, 
       raw_nested_book:year_published :: number as Yearof_Publish,
       value:first_name :: string as fName,
       value:middle_name :: string as mName,
       value:last_name :: string as lName
       
       from demo_lab.public.author_nested_ingest_json,
            lateral flatten(input => raw_nested_book:authors);


create or replace table demo_lab.public.book_author_details as 
select 
    -- raw_nested_book as raw_book_details, 
       raw_nested_book:book_title :: string as bookTitle, 
       raw_nested_book:year_published :: number as Yearof_Publish,
       value:first_name :: string as fName,
       value:middle_name :: string as mName,
       value:last_name :: string as lName
       
       from demo_lab.public.author_nested_ingest_json,
            lateral flatten(input => raw_nested_book:authors);

select * from demo_lab.public.book_author_details;

--------------------------------------------------------------------
--Demo_lab check
--------------------------------------------------------------------
select 
  'vegetable_details records count_demo1' as check_,
  (select count(*) from demo_lab.public.vegetable_details) as actual,
  ('42') as expected
union all
select 
  'soil_type records count_demo2' as check_,
  (select count(*) from demo_lab.public.soil_type) as actual,
  ('8') as expected
union all
select 
  'author records count_demo3' as check_,
  (select count(*) from author_ingest_json) as actual,
  ('6') as expected
union all
select 
  'book_author records count_demo4' as check_,    
  (select count(*) from demo_lab.public.book_author_details) as actual,
  ('6') as expected;
  
  
  
 --erase the lab data
 drop database demo_lab;