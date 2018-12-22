import cx_Oracle
import argparse
import uuid

from util import get_update_function


def run():
    args = parse_arguments()
    dirname = args.dirname
    fname = args.filename
    username = args.username
    password = args.password
    connectionString = args.connectionString
    chunksize = args.chunksize
    updateFrequency = args.updateFrequency

    pname = 'file_upload_temp_{}'.format(uuid.uuid4().hex)
    with cx_Oracle.connect(username, password, connectionString) as connection:
        cursor = connection.cursor()
        
        # create a temporary package that keeps a file descriptor
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


        # splits the file into chunks and sends it to the server
        cursor.prepare("""
            BEGIN 
                utl_file.put_raw({}.fh,:data, true);
            END;
            """.format(pname));
        
        show_status = get_update_function(updateFrequency)
        with open(fname, "rb") as f:
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

        # delete the temporary package
        cursor.execute("drop package {}".format(pname))


def parse_arguments():
    parser = argparse.ArgumentParser(description='Upload a file to Oracle Server')
    
    parser.add_argument('dirname', help='the destination folder to transfer the file to')
    parser.add_argument('filename', help='a local file to transfer to the destination folder')
    parser.add_argument('username')
    parser.add_argument('password')
    parser.add_argument('connectionString', help='The connection string string to the database')   
    parser.add_argument('--chunksize', type=int, default=32767, help='the transfer chunksize in bytes')
    parser.add_argument('--updateFrequency', type=int, default=1, help='The status update frequency (in seconds)')
    args = parser.parse_args()
    return args
    
    
if __name__ == '__main__':
    run()
