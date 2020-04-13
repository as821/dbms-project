#                                       #
#   COSC 280 Project 2      main.py     #
#                                       #



# import statements
from definitions import *
from backend import access, create_index, delete_index, create_table, drop_table, selection, projection, join, task_manager
from parser import parser_main
from optimizer import optimizer



#           #
#   TODO    #
#           #
#   (backend.py)            implement DML mid level functions
#   (backend.py/main.py)    implement relational integrity (pick a default policy CASCADE/NULL).  Update referenced table in create table (main) and deleting from referenced table (remove and drop table)
#   (main.py)               write input loop
#   (main.py)               handle DML objects


#   *** Features to add ***
#   GROUPBY/HAVING
#   NOW (time logging function)
#   EXPLAIN (outputs information about optimizer decisions)
#   Support multiple foreign keys for each table





def main():

    # TODO testing
    table1 = create_table("test1_rel", [("name", "string"), ("age", "int"), ("year", "int")])
    table2 = create_table("test2_rel", [("name", "string"), ("income", "int"), ("home_town", "string")])


    # TODO infinite loop for input here


    # take a string as input (contains entire query)
    inp_line = "select sum(age) from test1_rel"

    #       *** Sample inputs ***
    #   "delete from test1_rel where test1_rel.name = \"Andrew\""
    #   "update test1_rel set year = 0 where test1_rel.name = \"Andrew\""
    #   "create table test3_rel (name string, school_year string, hourly_salary int, primary key(name), foreign key (school_year) references test1_rel(year))"
    #   "SELECT name FROM test1_rel as a WHERE (((a.age <= 50) and (a.name = \"Andrew\")) or (a.year < 2000)) or ((a.year > 1400) or (a.name = \"Bob\"))"





    # TODO testing section
    task_manager(None)  # just writes a few tuples to each of the tables to make query testing better
    create_index(TABLES["test1_rel"], "ind1", "name")
    # TODO END testing section




    # call parser on line of input
    parsed_obj = parser_main(inp_line)





    if type(parsed_obj) is Query:




        # call optimizer on query output from the parser
        resulting_relation = optimizer(parsed_obj)

        # output resulting relation
        print(resulting_relation)



    # call appropriate DDL/DML command
    elif type(parsed_obj) is DML:
        pass
    else:   # DDL
        if parsed_obj.create:
            if parsed_obj.table:
                create_table(parsed_obj.table_name, parsed_obj.attr)
                TABLES[parsed_obj.table_name].primary_key = parsed_obj.primary_key
                TABLES[parsed_obj.table_name].foreign_key = parsed_obj.foreign_key      # TODO should be doing more set up here (modifying table referenced from to show that it is referenced by this new table --> for relational integrity)
            else:
                create_index(TABLES[parsed_obj.table_name], parsed_obj.index_name, parsed_obj.attr[0])
        else:   # drop
            if parsed_obj.table:
                drop_table(TABLES[parsed_obj.table_name])
            else:
                delete_index(TABLES[INDEX[parsed_obj.index_name]], parsed_obj.index_name)
# END main







# call main function
main()
