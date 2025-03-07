import time
import argparse
from helpers.connection import conn
from helpers.utils import print_rows
from helpers.utils import print_rows_to_file
from helpers.utils import print_command_to_file
from helpers.utils import make_csv

#  search_role is need?
def display_info(search_type, search_value ,search_role): 
    # TODO
    try:
        cur = conn.cursor()
        
        cur.execute("SET search_path to s_2020030819")

        if search_type == 'asc' :
            sql = """
            SELECT 
            pa.p_id, 
            p_name, 
            pae.role,
            m_name,
            STRING_AGG(DISTINCT casting, ', ') AS casting
            FROM participant pa
            LEFT OUTER JOIN participate pae on pa.p_id = pae.p_id 
            LEFT OUTER JOIN movie m on pae.m_id = m.m_id
            WHERE pae.role ILIKE %s
            GROUP BY pa.p_id, p_name, pae.role, m_name
            LIMIT %s

            """
            cur.execute(sql, [search_role, search_value])

        elif search_type == 'movie' :
            sql = """
            SELECT 
            pa.p_id, 
            p_name, 
            pae.role, 
            STRING_AGG(DISTINCT casting, ', ') AS casting
            FROM participant pa, participate pae, movie m
            WHERE pa.p_id = pae.p_id
            AND m.m_id = pae.m_id
            AND m.m_id = %s
            AND pae.role ILIKE %s
            GROUP BY pa.p_id, p_name, pae.role
            """
            cur.execute(sql, [search_value, search_role])

        

        else :
            print("can't search by", search_type)
            return False

        rows = cur.fetchall()
        if not rows:
            print("No results found.")
            return False
        else:
            column_names = [desc[0] for desc in cur.description]
            #
            #print_rows_to_file(column_names, rows)
            #make_csv(column_names, rows)
            #
            print_rows(column_names, rows)
            return True

    except Exception as err:
        print(err)
    
    finally:
        cur.close()
    # end
    pass


def main(args):
    # TODO
    if args.command == "info":
        if args.asc:
            display_info('asc', args.asc[0], args.asc[1])
        elif args.movie:
            display_info('movie', args.movie[0], args.movie[1])
        
    else :
        print("Error: query command error.")
        
    
if __name__ == "__main__":
    #
    #print_command_to_file()
    #
    start = time.time()
    parser = argparse.ArgumentParser(description = """
    how to use
    1. info [-a(all) / -o(one)] value role
    2. ...
    3. ...
    """, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', 
        help='select one of query types [info, ...]')

    #info
    parser_info = subparsers.add_parser('info', help='Display participant associated to genre info')
    group_info = parser_info.add_mutually_exclusive_group(required=True)
    # TODO
    group_info.add_argument('-a', dest='asc',  type=str, help='c_id of customer entity', nargs= 2)
    group_info.add_argument('-i', dest='movie',  type=str, help='c_id of customer entity', nargs= 2)
    
    args = parser.parse_args()
    main(args)
    print("Running Time: ", end="")
    print(time.time() - start)
