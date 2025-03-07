import time
import argparse
from helpers.connection import conn
from helpers.utils import print_rows
from helpers.utils import print_rows_to_file
from helpers.utils import is_valid_genre
from helpers.utils import print_command_to_file
from helpers.utils import make_csv
from helpers.utils import is_valid_pro

def display_info(search_type, search_value):
    # TODO
    try:
        cur = conn.cursor()
        
        cur.execute("SET search_path to s_2020030819")

        if search_type == 'all' :
            sql = """
            SELECT 
            p.p_id ,
            p_name,
            major_work,
            STRING_AGG(DISTINCT ocu_name, ', ') AS profession
            FROM participant p
            LEFT OUTER JOIN profession pr on p.p_id = pr.p_id
            LEFT OUTER JOIN occupation o on pr.ocu_id = o.ocu_id
            GROUP BY p.p_id, p_name, major_work
            LIMIT %s

            """
            cur.execute(sql, [search_value])

        elif search_type == 'id' :
            sql = """
            SELECT 
            p.p_id ,
            p_name,
            major_work,
            STRING_AGG(DISTINCT ocu_name, ', ') AS profession
            FROM participant p
            LEFT OUTER JOIN profession pr on p.p_id = pr.p_id
            LEFT OUTER JOIN occupation o on pr.ocu_id = o.ocu_id
            WHERE p.p_id = %s
            GROUP BY p.p_id, p_name, major_work
            """
            cur.execute(sql, [search_value])

        elif search_type == 'name' :
            sql = """
            SELECT 
            p.p_id ,
            p_name,
            major_work,
            STRING_AGG(DISTINCT ocu_name, ', ') AS profession
            FROM participant p
            LEFT OUTER JOIN profession pr on p.p_id = pr.p_id
            LEFT OUTER JOIN occupation o on pr.ocu_id = o.ocu_id
            WHERE p_name ILIKE %s
            GROUP BY p.p_id, p_name, major_work
            """
            cur.execute(sql, [search_value])

        # need to change
        elif search_type == 'profession' :
            sql = """
            SELECT 
            p.p_id ,
            p_name,
            major_work,
            STRING_AGG(DISTINCT ocu_name, ', ') AS profession
            FROM participant p
            LEFT OUTER JOIN profession pr on p.p_id = pr.p_id
            LEFT OUTER JOIN occupation o on pr.ocu_id = o.ocu_id
            WHERE EXISTS (
                SELECT 1
                FROM profession pr2, occupation o2
                WHERE p.p_id = pr2.p_id
                AND pr2.ocu_id = o2.ocu_id
                AND o2.ocu_name ILIKE %s
            )
            GROUP BY p.p_id, p_name, major_work
            """
            cur.execute(sql, [search_value])
        
        
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
    if args.command == "info":
        if args.all:
            display_info('all', args.all)
        elif args.id:
            display_info('id',args.id)
        elif args.name:
            display_info('name', args.name)
        elif args.profession:
            display_info('profession', args.profession)
    
    else :
        print("Error: query command error.")

    pass


if __name__ == "__main__":
    #
    #print_command_to_file()
    #
    start = time.time()
    parser = argparse.ArgumentParser(description = """
    how to use
    1. info [-a(all) / -i(p_id) / -n(p_name) / -pr(profession name)] [value]
    2. ...
    3. ...
    """, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', 
        help='select one of query types [info, ...]')

    #info
    parser_info = subparsers.add_parser('info', help='Display target participant info')
    group_info = parser_info.add_mutually_exclusive_group(required=True)
    # TODO
    group_info.add_argument('-a', dest='all',  type=int, help='c_id of customer entity')
    group_info.add_argument('-i', dest='id',  type=int, help='c_id of customer entity')
    group_info.add_argument('-n', dest='name',  type=str, help='c_id of customer entity')
    group_info.add_argument('-pr', dest='profession',  type=str, help='c_id of customer entity')
    
    args = parser.parse_args()
    main(args)
    print("Running Time: ", end="")
    print(time.time() - start)
