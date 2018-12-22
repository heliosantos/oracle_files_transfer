-- list packages
select object_name from user_objects where object_type in ('PACKAGE' );


-- list directories
select DIRECTORY_PATH, DIRECTORY_NAME from DBA_DIRECTORIES;


-- get files in directory
select filename, filesize/1024/1024 as size_Mb from table (rdsadmin.rds_file_util.listdir(p_directory => 'TEST_DIRECTORY')) where type = 'file';


-- get content of file
select * from table (rdsadmin.rds_file_util.read_text_file(p_directory => 'TEST_DIRECTORY', p_filename  => 'test.txt'));


-- remove a file
begin 
    UTL_FILE.FREMOVE (location => 'TEST_DIRECTORY', filename => 'test.txt'); 
end;
