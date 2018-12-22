import cx_Oracle
import uuid
import argparse

from util import get_update_function


def run():
    args = parse_arguments()
    dirname = args.dirname
    fname = args.filename
    username = args.username
    password = args.password
    connectionString = args.connectionString
    chunksize = args.chunksize
    saveAs = args.saveAs or fname
    updateFrequency = args.updateFrequency

    with cx_Oracle.connect(username, password, connectionString) as connection:
            
        pname = 'file_upload_temp_{}'.format(uuid.uuid4().hex)
        cursor = connection.cursor()

        cursor.execute("""
        create package {pname} as

            TYPE chunk_record IS RECORD(
               chunk raw({chunksize})
            );

            TYPE chunk_table IS TABLE OF chunk_record;

            function get_chunks return chunk_table PIPELINED;
        end;
        """.format(pname=pname, chunksize=chunksize))

        cursor.execute("""
            CREATE PACKAGE BODY {pname} AS

                FUNCTION get_chunks
                    RETURN chunk_table PIPELINED
                IS
                    rec chunk_record;
                    fh utl_file.file_type;
                    t RAW({chunksize});
                BEGIN

                    fh := utl_file.fopen('{dirname}', '{fname}', 'rb', {chunksize});
                    utl_file.get_raw(fh, t, {chunksize});

                    while utl_raw.length(t) > 0 loop

                        SELECT t INTO rec FROM DUAL;
                        PIPE ROW (rec);
                        utl_file.get_raw(fh, t, {chunksize});
                    end loop;

                    RETURN;
                END get_chunks;
            END;
            """.format(pname=pname, chunksize=chunksize, dirname=dirname, fname=fname))

        cursor.execute("select * from table({}.get_chunks)".format(pname))


        with open(saveAs, 'wb') as outfile:
            show_status = get_update_function(updateFrequency)
            for chunk in cursor:
                outfile.write(chunk[0])
                show_status(len(chunk[0]))
        cursor.execute("drop package {}".format(pname))

    
def parse_arguments():
    parser = argparse.ArgumentParser(description='Upload a file to Oracle Server')
    
    parser.add_argument('dirname', help='the destination folder to transfer the file to')
    parser.add_argument('filename', help='a local file to transfer to the destination folder')
    parser.add_argument('username')
    parser.add_argument('password')
    parser.add_argument('connectionString', help='The connection string string to the database')    
    parser.add_argument('--chunksize', type=int, default=2000, help='the transfer chunksize in bytes')
    parser.add_argument('--saveAs')
    parser.add_argument('--updateFrequency', type=int, default=1, help='The status update frequency (in seconds)')
    args = parser.parse_args()
    return args
    
    
if __name__ == '__main__':
    run()
