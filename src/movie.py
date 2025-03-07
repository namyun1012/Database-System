import time
import argparse
from helpers.connection import conn
from helpers.utils import print_rows
from helpers.utils import print_rows_to_file
from helpers.utils import is_valid_genre
from helpers.utils import print_command_to_file
from helpers.utils import make_csv

def display_info(search_type, search_value):
    # TODO
    try:
        cur = conn.cursor()
        
        cur.execute("SET search_path to s_2020030819")

        if search_type == 'asc' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id


            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            LIMIT %s
            """
            cur.execute(sql, [search_value])

        elif search_type == 'id' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE m.m_id = %s

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])

        elif search_type == 'name' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE m_name ILIKE %s

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])
        
        # need to change
        elif search_type == 'genre' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE EXISTS (
                SELECT 1
                FROM classify cl2, genre gr2
                WHERE m.m_id = cl2.m_id
                AND cl2.gr_id = gr2.gr_id
                AND gr2.gr_name ILIKE %s
            )

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])
        
        elif search_type == 'type' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE m_type ILIKE %s

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])
        
        elif search_type == 'start_year' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE start_year >= to_date(%s::TEXT || '-01-01', 'YYYY-MM-DD')

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])
        
        elif search_type == 'end_year' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE end_year >= to_date(%s::TEXT || '-01-01', 'YYYY-MM-DD')

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])
        
        elif search_type == 'is_adult' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE is_adult = %s

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
            """
            cur.execute(sql, [search_value])
        
        elif search_type == 'rating' :
            sql = """
            SELECT  m.m_id, 
		    m.m_name, 
		    m.m_type, 
		    start_year, 
		    end_year, 
		    is_adult, 
		    runtimes,
		    m_rating as imdb_rating, 
		    (
		    	COALESCE((m_rating * votes + comment_sum) / (votes + comment_count), (m_rating * votes) / votes) 
		    ) AS final_rating,
		    STRING_AGG(DISTINCT gr.gr_name, ', ') AS genres

            FROM movie m
            LEFT OUTER JOIN
            (
	            SELECT m_id, count(*) AS comment_count , SUM(rating) AS comment_sum
	            FROM comment_to
	            GROUP BY m_id
            ) user_temp ON m.m_id = user_temp.m_id

            LEFT OUTER JOIN classify cl ON m.m_id = cl.m_id
            LEFT OUTER JOIN genre gr ON cl.gr_id = gr.gr_id
            WHERE m_rating >= %s

            GROUP BY m.m_id, m_name, m_type, start_year, end_year, is_adult, runtimes, m_rating, final_rating
            ORDER BY m.m_id ASC
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
    # TODO
    if args.command == "info":
        if args.asc:
            display_info('asc', args.asc)
        elif args.id:
            display_info('id', args.id)
        elif args.name:
            display_info('name', args.name)
        
        elif args.genre:
            display_info('genre', args.genre)
        
        elif args.type:
            display_info('type', args.type)
            
        elif args.start_year:
            display_info('start_year', args.start_year)   
        
        elif args.end_year:
            display_info('end_year', args.end_year)   
        
        elif args.is_adult:
            display_info('is_adult', args.is_adult)   
        
        elif args.rating:
            display_info('rating', args.rating)
        
    else :
        print("Error: query command error.")


if __name__ == "__main__":
    #
    #print_command_to_file()
    #
    start = time.time()
    parser = argparse.ArgumentParser(description = """
    how to use
    1-1. info [-a(all) / -i(m_id) / -n(m_name) / -g(genre)] [value]
    1-2. info [-sy(start_year) / -ey(end_year) / -ad(is_adult) / -r(rating)] [value]
    2. ...
    3. ...
    """, formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command', 
        help='select one of query types [info, ...]')

    #info
    parser_info = subparsers.add_parser('info', help='Display target movie info')
    group_info = parser_info.add_mutually_exclusive_group(required=True)
    # TODO
    group_info.add_argument('-a', dest='asc',  type=int, help='c_id of customer entity')
    group_info.add_argument('-i', dest='id', type=int, help='c_id of customer entity')
    group_info.add_argument('-n', dest='name', type=str, help='c_id of customer entity')
    group_info.add_argument('-g', dest='genre', type=str, help='c_id of customer entity')
    group_info.add_argument('-t', dest='type', type=str, help='c_id of customer entity')
    group_info.add_argument('-sy', dest='start_year', type=str, help='c_id of customer entity')
    group_info.add_argument('-ey', dest='end_year', type=str, help='c_id of customer entity')
    group_info.add_argument('-ad', dest='is_adult', type=str, help='c_id of customer entity')
    group_info.add_argument('-r', dest='rating', type=float, help='c_id of customer entity')
    
    
    args = parser.parse_args()
    main(args)
    print("Running Time: ", end="")
    print(time.time() - start)
