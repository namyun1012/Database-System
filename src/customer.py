import time
import argparse
from helpers.connection import conn
from helpers.utils import print_rows
from helpers.utils import print_rows_to_file
from helpers.utils import is_valid_genre
from helpers.utils import print_command_to_file
from helpers.utils import make_csv

def display_info(search_type, search_value):
    try:
        cur = conn.cursor()
        
        cur.execute("SET search_path to s_2020030819")

        if search_type == 'id' :
            sql = """
            SELECT 
            cu.c_id, 
            cu.c_name, 
            cu.email, 
            cu.gender, 
            cu.phone, 
            STRING_AGG(DISTINCT gr.gr_name, ', ') AS preferred_genres
            FROM customer cu 
            JOIN prefer p ON cu.c_id = p.c_id 
            JOIN genre gr ON p.gr_id = gr.gr_id
            WHERE cu.c_id = %(id)s
            GROUP BY cu.c_id, cu.c_name, cu.email, cu.gender, cu.phone
            ORDER BY cu.c_id ASC;
            """
            cur.execute(sql, {"id": search_value})

        elif search_type == 'name' :
            sql = """
            SELECT
            cu.c_id, 
            cu.c_name, 
            cu.email, 
            cu.gender, 
            cu.phone, 
            STRING_AGG(DISTINCT gr.gr_name, ', ') AS preferred_genres
            FROM customer cu 
            JOIN prefer p ON cu.c_id = p.c_id 
            JOIN genre gr ON p.gr_id = gr.gr_id
            WHERE cu.c_name ILIKE %(name)s
            GROUP BY cu.c_id, cu.c_name, cu.email, cu.gender, cu.phone
            ORDER BY cu.c_id ASC;
            """
            cur.execute(sql, {"name": search_value})

        elif search_type == 'genre' :
            sql = """
            SELECT 
                cu.c_id, 
                cu.c_name, 
                cu.email, 
                cu.gender, 
                cu.phone, 
                STRING_AGG(DISTINCT gr.gr_name, ', ') AS preferred_genres
            FROM customer cu JOIN prefer p ON cu.c_id = p.c_id JOIN genre gr ON p.gr_id = gr.gr_id
            WHERE cu.c_id IN (
                    SELECT cu.c_id
                    FROM customer cu
                    JOIN prefer p ON cu.c_id = p.c_id
                    JOIN genre gr ON p.gr_id = gr.gr_id
                    WHERE gr.gr_name = %(genre)s
                    )
            GROUP BY cu.c_id, cu.c_name, cu.email, cu.gender, cu.phone
            ORDER BY cu.c_id ASC;
            """
            cur.execute(sql, {"genre": search_value})

        elif search_type == 'all' :
            sql = """
            SELECT
            cu.c_id, 
            cu.c_name, 
            cu.email, 
            cu.gender, 
            cu.phone, 
            STRING_AGG(DISTINCT gr.gr_name, ', ') AS preferred_genres
            FROM customer cu 
            JOIN prefer p ON cu.c_id = p.c_id 
            JOIN genre gr ON p.gr_id = gr.gr_id
            GROUP BY cu.c_id, cu.c_name, cu.email, cu.gender, cu.phone
            ORDER BY cu.c_id ASC
            LIMIT %(all)s;
            """
            cur.execute(sql, {"all": search_value})

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

def insert_customer(id, name, email, pwd, gender, phone, genres) :
    # TODO
    try:
        cur = conn.cursor()
        cur.execute("SET search_path to s_2020030819")

        
        if id is None :
            print("c_id is not existed")

        sql = """
            SELECT c_id
            FROM customer
            WHERE c_id = %s
        """
        cur.execute(sql, [id])
        temp_id = cur.fetchone()

        if temp_id is not None:
            display_info('id', id)
            print("Error! this c_id is already existed")
            return 
        
        
    
        cur.execute("""
            INSERT INTO customer VALUES (
                %s, %s, %s, %s, %s, %s
            )            
            """, 
            [id, name, email, pwd, gender, phone])
    
        for genre in genres :     
            sql = """
                SELECT gr_id
                FROM genre
                WHERE gr_name = %s
            """
            cur.execute(sql, [genre])
            
            gr_id =  cur.fetchone()
            
            if gr_id is None:
                continue
        
            sql = """
                INSERT INTO prefer VALUES(
                    %s, %s
                )
            """
            cur.execute(sql, [id, gr_id])
           
        conn.commit()
        display_info('id', id)
    
    except Exception as err:
        print(err)
    
    finally:
        cur.close()
    # end
    pass
    

def update_customer(id, target, value) :
    
    # TODO
    if id is None :
        print("c_id is not existed")
    
    try :
        cur = conn.cursor()
        cur.execute("SET search_path to s_2020030819")

        display_info('id', id)

        if target == 'email':
            sql = """
                UPDATE customer
                SET email = %s
                WHERE c_id = %s
            """
            cur.execute(sql, [value, id])
        # When old password is Right, then change it 
        elif target == 'pwd':
            old_password = value[0]
            new_password = value[1]
            sql = """
                SELECT pwd
                FROM customer
                WHERE c_id = %s
            """
            cur.execute(sql, [id])
            temp = cur.fetchone()
            if temp is not None:
                temp = temp[0]
            else:
                print("password is not existed")
                return
            
            if temp != old_password :
                print("old password is wrong!")
                return
            
            sql = """
                UPDATE customer
                SET pwd = %s
                WHERE c_id = %s
                AND pwd = %s
            """
            cur.execute(sql, [new_password, id, old_password])   
    
        elif target == 'phone':
            sql = """
                UPDATE customer
                SET phone = %s
                WHERE c_id = %s
            """
            cur.execute(sql, [value, id])   

        # genre 3개는 다 바꿈 삭제하고 새로 넣는 것이 빠를 듯
        elif target == 'genre':
            sql = """
                DELETE FROM prefer
                WHERE c_id = %s
            """
            cur.execute(sql, [id])
            
            for val in value :
                sql = """
                    SELECT gr_id
                    FROM genre
                    WHERE gr_name = %s
                 """
                cur.execute(sql, [val])
            
                gr_id =  cur.fetchone()
                #print(gr_id)
            
                if gr_id is None:
                    continue
        
                sql = """
                    INSERT INTO prefer VALUES(
                    %s, %s
                    )
                """
                cur.execute(sql, [id, gr_id])
            
        conn.commit()
        display_info('id', id)

    except Exception as err:
        print(err)
    
    finally:
        cur.close()
    # end
    pass
    
    
def delete_customer(id) :
    # TODO
    if id is None :
        print("c_id is not existed")
        return 
    
    try :
        cur = conn.cursor()
        
        cur.execute("SET search_path to s_2020030819")
        display_info('id', id)

        # 다른 것들도 삭제 필요한가?
        
        sql = """
            DELETE FROM Prefer
            WHERE c_id = %s
    
        """
        cur.execute(sql, [id])
        
        sql = """
            DELETE FROM Customer
            WHERE c_id = %s
    
        """
        cur.execute(sql, [id])
        conn.commit()
        # display_info('id', id)
        
    except Exception as err:
        print(err)
    
    finally:
        cur.close()
    # end
    pass
    
    
def main(args):
    if args.command == "info":
        if args.id:
            display_info('id',args.id)
        elif args.name:
            display_info('name', args.name)
        elif args.genre:
            if not is_valid_genre(args.genre):
                print(f"Error: '{args.genre}' is not a valid genre.")
            else:
                display_info('genre', args.genre)
        elif args.all:
            display_info('all', args.all)

    # What to do?
    elif args.command == "insert":
        if not is_valid_genre(args.genres[0]):
            print(f"Error: '{args.genres[0]}' is not a valid genre.")
        elif not is_valid_genre(args.genres[1]):
            print(f"Error: '{args.genres[1]}' is not a valid genre.")
        elif not is_valid_genre(args.genres[2]):
            print(f"Error: '{args.genres[2]}' is not a valid genre.")
        else:
            insert_customer(args.id, args.name, args.email, args.pwd, args.gender, args.phone, args.genres)

    elif args.command == "update":
        # TODO
        if args.email :
            update_customer(args.id, 'email', args.email)
        
        elif args.pwd:
            update_customer(args.id, 'pwd', args.pwd)
            
        elif args.phone:
            update_customer(args.id, 'phone', args.phone)
        
        elif args.genres:
            if not is_valid_genre(args.genres[0]):
                print(f"Error: '{args.genres[0]}' is not a valid genre.")
            elif not is_valid_genre(args.genres[1]):
                print(f"Error: '{args.genres[1]}' is not a valid genre.")
            elif not is_valid_genre(args.genres[2]):
                print(f"Error: '{args.genres[2]}' is not a valid genre.")
            else:
                update_customer(args.id, 'genre', args.genres)        
        
        else :
            print("command error")
        
    elif args.command == "delete":
        # TODO
        if args.id :
            delete_customer(args.id)
        
    else :
        print("Error: query command error.")


if __name__ == "__main__":
    #
    #print_command_to_file()
    #
    start = time.time()
    
    parser = argparse.ArgumentParser(description = """
    how to use
    1. info [-i(c_id) / -n(c_name) / -g(genre) / -a (all)] [value]
    2. insert c_id, c_name, email, pwd, gender, phone -g (genre1, genre2, genre3)
    3. update -i [c_id] [-m(e-mail) / -p(password) / -ph(phone)] [new_value]
    4. delete -i [c_id]
    """, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', 
        help='select one of query types [info, insert, update, delete]')

    #[1-1]info
    parser_info = subparsers.add_parser('info', help='Display target customers info')
    group_info = parser_info.add_mutually_exclusive_group(required=True)
    group_info.add_argument('-i', dest='id', type=int, help='c_id of customer entity')
    group_info.add_argument('-n', dest='name', type=str, help='c_name of customer entity')
    group_info.add_argument('-g', dest='genre', type=str, help='genre which customer prefer')
    group_info.add_argument('-a', dest='all', type=str, help='display rows with top [value]')

    #[1-2]insert
    parser_insert = subparsers.add_parser('insert', help='Insert new customer data')
    # TODO
    # id, name, email, pwd, gender, phone, genres
    parser_insert.add_argument("id", type=int, help="Customer values to insert")
    parser_insert.add_argument("name", type=str, help="Customer values to insert")
    parser_insert.add_argument("email", type=str, help="Customer values to insert")
    parser_insert.add_argument("pwd", type=str, help="Customer values to insert")
    parser_insert.add_argument("gender", type=str, help="Customer values to insert")
    parser_insert.add_argument("phone", type=str, help="Customer values to insert")
    group_insert = parser_insert.add_mutually_exclusive_group(required=True)
    group_insert.add_argument('-g', dest='genres', nargs='+', type=str, help='genre which customer prefer')
    
    
    #[1-3]update
    parser_update = subparsers.add_parser('update', help='Update one of customer data')
    # TODO
    parser_update.add_argument('-i', dest='id', type=int, help="Customer values to insert")
    group_update = parser_update.add_mutually_exclusive_group(required=True)
    group_update.add_argument('-m', dest='email', type=str, help = 'email')
    group_update.add_argument('-p', dest='pwd', type=str, help = 'pwd', nargs=2)
    group_update.add_argument('-ph', dest='phone', type=str, help = 'phone')
    group_update.add_argument('-gs', dest='genres', nargs='+', type=str, help = 'genres')
    
    
    
    #[1-4]delete
    parser_delete = subparsers.add_parser('delete', help='Delete customer data with associated data')
    # TODO
    group_delete = parser_delete.add_mutually_exclusive_group(required=True)
    group_delete.add_argument('-i', dest='id', type=int, help='c_id of customer entity')
    
    args = parser.parse_args()
    main(args)
    print("Running Time: ", end="")
    print(time.time() - start)
