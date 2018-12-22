import cx_Oracle
import random
import uuid
import boto3
import argparse

from util import get_update_function


def run():
    args = parse_arguments()
    dirname = args.dirname
    bucket = args.bucket
    key = args.key
    username = args.username
    password = args.password
    connectionString = args.connectionString    
    fname = args.filename or key
    chunksize = args.chunksize
    updateFrequency = args.updateFrequency


    pname = 'file_upload_temp_{}'.format(uuid.uuid4().hex)
    with cx_Oracle.connect(username, password, connectionString) as connection:
        cursor = connection.cursor()

        cursor.execute("""
            create package {} as 
                fh utl_file.file_type;
            end;
            """.format(pname))

        cursor.prepare("""
            BEGIN
                {}.fh :=utl_file.fopen(:dirname, :fname, 'wb', :chunksize);
            END;
            """.format(pname));

        cursor.execute(None, {'dirname': dirname, 'fname': fname, 'chunksize': chunksize})

        cursor.prepare("""
            BEGIN 
                utl_file.put_raw({}.fh,:data, true);
            END;
            """.format(pname));

        s3 = boto3.resource('s3')
        obj = s3.Object(bucket, key)
        f = obj.get()['Body']

        show_status = get_update_function(updateFrequency)
        while True:
            chunk = f.read(chunksize)
            if chunk:
                cursor.execute(None, {'data': chunk})
                show_status(len(chunk))
            else:
                break

        cursor.execute("""
            BEGIN 
                utl_file.fclose({}.fh);
            END;
            """.format(pname))

        cursor.execute("drop package {}".format(pname))



def parse_arguments():
    parser = argparse.ArgumentParser(description='Upload a file to Oracle Server')
    
    parser.add_argument('dirname', help='the destination folder to transfer the file to')
    parser.add_argument('bucket', help='S3 bucket name of the file')
    parser.add_argument('key', help='S3 key name of the file')
    parser.add_argument('username')
    parser.add_argument('password')
    parser.add_argument('connectionString', help='The connection string string to the database')   
    parser.add_argument('--filename')
    parser.add_argument('--chunksize', type=int, default=32767, help='the transfer chunksize in bytes')
    parser.add_argument('--updateFrequency', type=int, default=1, help='The status update frequency (in seconds)')
    args = parser.parse_args()
    return args
    
    
if __name__ == '__main__':
    run()
