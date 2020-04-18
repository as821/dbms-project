#                                       #
#   COSC 280 Project 2      main.py     #
#                                       #


# import statements
from definitions import *
from backend import access, create_index, delete_index, create_table, drop_table, selection, projection, join, dml_delete, dml_update, dml_insert, initializer
from parser import parser_main
from optimizer import optimizer


#       *** Sample inputs ***
#   "select i-i-hundred.* from i-i-hundred  as i natural join i-1-hundred  as one natural join i-i-thousand where (i-i-thousand.id < 30) and (i.id < 5)"
#   "insert into test1_rel (name, age, year) values (\"joe\", 31, 2200)"
#   "delete from test1_rel where test1_rel.name = \"Andrew\""
#   "update test1_rel set year = 0 where test1_rel.name = \"Andrew\""
#   "create table test3_rel (name string, school_year string, hourly_salary int, primary key(name), foreign key (name) references test1_rel(name))"
#   "SELECT name FROM test1_rel as a WHERE (((a.age <= 50) and (a.name = \"Andrew\")) or (a.year < 2000)) or ((a.year > 1400) or (a.name = \"Bob\"))"



# main function
def main():
    # initialize database with the provided tables
    optimize = False
    init_start = time.time()
    initializer()
    init_end = time.time()
    print("initialization time: ", init_end-init_start)

    # start infinite input loop
    keep_looping = True
    while keep_looping:
        try:
            # take line of input
            inp_line = input("\n>>>\t")

            # check for loop termination and optimization settings
            if inp_line == "exit":
                keep_looping = False
            elif inp_line == "optimize_on":
                optimize = True     # turn off pretty output
            elif inp_line == "optimize_off":
                optimize = False    # turn on pretty output
            else:
                # call parser on line of input and start query timer
                start = time.time()
                parsed_obj = parser_main(inp_line)

                # determine command type and call appropriate functions
                if type(parsed_obj) is Query:
                    # call optimizer on query output from the parser
                    attr_list, resulting_relation, explain_string = optimizer(parsed_obj)

                    # output resulting relation
                    if optimize:
                        print(resulting_relation)
                    else:
                        print(t.tabulate(resulting_relation, headers=attr_list, tablefmt="pretty"))

                    print("\nExplain:", explain_string, "\n")


                elif type(parsed_obj) is DML:
                    # call appropriate DML function
                    if parsed_obj.insert:
                        dml_insert(parsed_obj)
                    elif parsed_obj.delete:
                        dml_delete(parsed_obj)
                    else:   # update
                        dml_update(parsed_obj)
                else:
                    # call appropriate DDL function
                    if parsed_obj.create:
                        if parsed_obj.table:
                            # create table
                            create_table(parsed_obj.table_name, parsed_obj.attr)

                            # additional adjustments to the table to avoid the need to pass in the entire object.  Set up relational integrity and NOW functionality
                            TABLES[parsed_obj.table_name].primary_key = parsed_obj.primary_key
                            TABLES[parsed_obj.table_name].foreign_key = parsed_obj.foreign_key

                            # if now specified, append to relation schema
                            if parsed_obj.now:
                                TABLES[parsed_obj.table_name].now = True
                                a = Attribute()
                                a.name = "now"
                                a.type = "str"
                                TABLES[parsed_obj.table_name].attributes.append(a)
                                TABLES[parsed_obj.table_name].attribute_names.add("now")
                                TABLES[parsed_obj.table_name].num_attributes += 1
                                TABLES[parsed_obj.table_name].storage.attr_loc["now"] = TABLES[parsed_obj.table_name].num_attributes - 1


                            # if foreign key is being set, need to update table being referenced as well
                            if len(parsed_obj.foreign_key) > 0:
                                foreign_table = parsed_obj.foreign_key[1]
                                foreign_attr = parsed_obj.foreign_key[2]
                                child_attr = parsed_obj.foreign_key[0]
                                child_table = parsed_obj.table_name
                                TABLES[foreign_table].child_tables.append((foreign_attr, child_table, child_attr))
                        else:
                            # create index on specified table
                            create_index(TABLES[parsed_obj.table_name], parsed_obj.index_name, parsed_obj.attr[0])
                    else:
                        if parsed_obj.table:
                            # drop table
                            drop_table(TABLES[parsed_obj.table_name])
                        else:
                            # drop index on specified table
                            delete_index(TABLES[INDEX[parsed_obj.index_name]], parsed_obj.index_name)

                # end query timer
                end = time.time()
                print("Query durations (sec): ", end-start)
        # allow recovery from errors
        except ValueError:
            pass
# END main



# call main function
main()
