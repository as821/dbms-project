#                                       #
#   COSC 280 Project 2      main.py     #
#                                       #



# import statements
from definitions import *
from backend import access, create_index, delete_index, create_table, selection, projection, join, task_manager
from parser import parser_main
from optimizer import optimizer



#           #
#   TODO    #
#           #

#   (optimizer.py)  add primary key check and perform any simple selections (see except ValueError part of where_access)

#   (optimizer.py)  test optimizer
#   (parser.py)     test DDL, DML parsing (insert, update, delete, create/drop table/index)

#   (backend.py)    implement DML mid level functions, set operations, and aggregate operators, and DDL drop table
#   (backend.py)    handle foreign key dependencies in mid/low level functions
#   (main.py)       write input loop
#   (main.py)       output resulting relation
#   (main.py)       handle DDL/DML objects
#   (optimizer.py)  add support for aggregate operators
#   (optimizer.py)  support SELECT * (handled in projections section)
#   (optimizer.py)  handle conjunctive/disjunctive selections (only need set union/intersection functions)

#   GROUPBY/HAVING





def main():

    # TODO testing
    table1 = create_table("test1_rel", [("name", "string"), ("age", "int"), ("year", "int")])
    table2 = create_table("test2_rel", [("name", "string", ("income", "int"), ("home_town", "string"))])



    # TODO infinite loop for input here


    # take a string as input (contains entire query)
    inp_line = "SELECT name FROM test1_rel as a WHERE (a.age <= 50) and (a.name = \"Andrew\")"  # and (a.name = \"Andrew\")"



    # call parser on line of input
    parsed_obj = parser_main(inp_line)




    # # TODO testing section
    task_manager(parsed_obj)        # just writes a few tuples to each of the tables to make query testing better







    # TODO END testing section




    if type(parsed_obj) is Query:
        # call optimizer on query output from the parser
        resulting_relation = optimizer(parsed_obj)

        # output resulting relation
        print(resulting_relation)





    # call appropriate DDL/DML command
    elif type(parsed_obj) is DML:
        pass
    else:
        pass
# END main







# call main function
main()