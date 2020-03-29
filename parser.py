#                                           #
#   COSC 280 DBMS Project --> Parser code   #
#                                           #




# import statements
import re




# constants






# Query class
class Query:
    def __init__(self):
        ### object attributes ###
        self.select_attr = []       # give as tuple of (table name, attr name) --> only one table in from then table name can be ""
        self.from_tables = {}       # use alias/table name as key --> maps to table name (simplifies parsing)
        self.where = []             # TODO just a placeholder.  This will have to be some tree structure of comparisons


        ### add new flags/variables as needed here.  These flags will be interpreted by the backend ###
        # handle compound (? unsure proper term) queries here
        union = False
        intersect = False
        difference = False
        left_query = object()   # Query reference
        right_query = object()  # Query reference

        # aggregate operators.  For each supported operator maintain a list of indices (of select attr list) to apply that operator to
        max = []
        min = []
        sum = []
# END Query class



# Comparison class
class Comparison:
    def __init__(self):
        # operands  (breaking comparisons into trees of comparisons allows for more complex+nested comparisons)
        self.right_operand = object()   # using object base class should allow for placing either another Comparison() here or an integer or a string
        self.left_operand = object()
        #   self.eval = False   # result of comparison.  May need this, will decide when writing backend for interpreting it

        # operations ...  (can include IN/BETWEEN later?)
        self._and = False
        self._or  = False
        self.equal = False
        self.not_equal = False
        self.greater = False
        self.less = False
        self.great_equal = False
        self.less_equal = False

        self.assignment = False     # only to be used in support of UPDATE DML command
# END Comparison class





# DML class
class DML:
    def __init__(self):
        self.insert = False
        self.delete = False
        self.update = False
        self.table_name = ""

        # insert
        self.values = []


        # update
        self.set = []   # empty list of Compaison objects.  Set assignment = True

        # update and delete
        self.where = []     # empty list of Comparison objects
# END DML class





# DDL class
class DDL:
    def __init__(self):
        self.table = False      # set to false to signify INDEX  (initialized to a placeholder value)
        self.create = False     # set to false to signify DROP

        self.table_name = ""
        self.index_name = ""
        self.attr = []      # list of tuples of (attribute name, data type)




#           #
#   TODO    #
#           #
#   make contents of "if query" into a recursive function that accepts a query object and a line of input (allow for compound queries)
#   make WHERE clause parsing into own function.  Can be used for queries and for DML (UPDATE + DELETE parsing)
#   handle joins (see if statement w/ pass)
#   decide how to store tables/columns and add valiudation checks (see "TODO validation")
#   add errors to aggregate operators (if an unknown if found, produce error)
#   add support for errors (see TODOs)
#   parse_where needs to support attributes without table specified.  Distinguish from attrs and scalar values


#   determine how to return Query tree (or just have each type return individually??)
#   support tables with same names and different column sets or just tables with different names
#   support NULLs? --> have to add another value to the tuples in DDL.attr (boolean for NULL/NOT NULL)
#   add support for GROUPBY/HAVING clauses in "query" section
#   include IN/BETWEEN too?





def parser_main():  # parameter
    # s: list of tables/columns, string of input
    # take a string as input (contains entire query)
    inp_line = "SELECT * FROM table_name WHERE emp# = 10"
    inp_line = inp_line.lower()


    # classify as DDL or DML --> search for SELECT (a query), INSERT/UPDATE/DELETE (DML), CREATE/DROP (DDL)
    query = False
    dml = False
    ddl = False
    if "select" in inp_line:
        query = True
    elif "insert" in inp_line or "update" in inp_line or "delete" in inp_line:
        dml = True
    elif "create" in inp_line or "drop" in inp_line:
        ddl = True



    # perform query-specific operations
    if query:
        # produce Query object
        this_query = Query()


        #                            #
        #   TODO make recursive here #
        #                            #


        # tokenize input on UNION, INTERSECT, DIFFERENCE (break into different query objects and parse them)
        union = False
        intersect = False
        difference = False


        # calls parser recursively, so only need to handle the "one" or "none" cases
        if "union" in inp_line:
            this_query.union = True
            this_query.left_query = Query()
            this_query.right_query = Query()
            list = re.split("union", inp_line)
            # TODO call query parser on both left and right queries

        elif "intersect" in inp_line:
            this_query.intersect = True
            this_query.left_query = Query()
            this_query.right_query = Query()
            list = re.split("intersect", inp_line)

            # TODO call query parser on both left and right queries

        elif "difference" in inp_line:
            this_query.difference = True
            this_query.left_query = Query()
            this_query.right_query = Query()
            list = re.split("difference", inp_line)

            # TODO call query parser on both left and right queries
        else:
            # break query into SELECT, FROM, and WHERE clauses (can add GROUPBY/HAVING here too) --> parse FROM first to define all aliases
            select_start = inp_line.find("select") + len("select")
            select_end = inp_line.find("from")
            from_end = inp_line.find("where")
            select_clause = inp_line[select_start : select_end]
            from_clause = inp_line[(select_end + len("from")) : from_end]
            where_clause = inp_line[(from_end + len("where")):]


            ### parse FROM ###
            # tokenize on JOIN (classify as NATURAL JOIN, LEFT OUTER JOIN, RIGHT OUTER JOIN, or INNER JOIN) --> validate contents of ON clause for join too
            if "join" in from_clause:
                # TODO determine which join.  Check table name/column combination for ON part of join command
                pass


            # tokenize on ',' to separate other table names from contents of JOINs (and from each other)
            from_list = from_clause.split(',')
            for name in from_list:
                # check each table name for "as" --> recognize aliases and add to from_tables
                alias_list = re.split(" as ", inp_line)
                if len(alias_list) > 1:
                    # strip whitespace off alias and name
                    alias_list[0] = alias_list[0].rstrip()
                    alias_list[0] = alias_list[0].lstrip()
                    alias_list[1] = alias_list[1].rstrip()
                    alias_list[1] = alias_list[1].lstrip()

                    # TODO validate here (alias_list[0])

                    # handle alias (put into from_tables)
                    this_query.from_tables[alias_list[1]] = alias_list[0]   # input both alias and name to dictionary (can access that table either way)
                    this_query.from_tables[alias_list[0]] = alias_list[0]
                else:
                    # no alias found, just insert to query from_table
                    # strip whitespace from ends
                    name = name.rstrip()
                    name = name.lstrip()

                    # TODO validate here (name)

                    # add name to from_tables
                    this_query.from_tables[name] = name



            ### parse SELECT ###
            # tokenize contents of SELECT on ',' --> break up attributes
            select_list = select_clause.split(",")

            for attr in range(len(select_list)):
                agg_list = select_list[attr].split("(")    # opening parenthesis is beginning of aggregate operator
                if len(agg_list) > 1:

                    close_paren_list = agg_list[1].split(')')
                    if len(close_paren_list) != 2:
                        # TODO error (no closing parenthesis on operator)
                        pass
                    else:
                        # identify any aggregate operators on select attributes
                        if "min" in select_list[attr]:
                            this_query.min.append(attr)
                        elif "max" in select_list[attr]:
                            this_query.max.appen(attr)
                        elif "sum" in select_list[attr]:
                            this_query.sum.append(attr)

                    attr_name = close_paren_list[0].rstrip().lstrip()   # attr is between ( and ) and drop whitespace
                else:
                    attr_name = attr.lstrip().rstrip()  # no aggregate operator, so just strip whitespace



                # search for table name of attr
                attr_list = attr_name.split(".")    # split to find if alias used
                if len(attr_list) > 1:
                    # table specified
                    attr_tup = (attr_list[0], attr_list[1])
                elif len(this_query.from_tables) > 1:   # no table specified ( >1 table in query )
                        # TODO error (ambiguous which table the attr is from)
                        pass
                else:
                    # only on table in from, dont need to specify table
                    table_key = this_query.from_tables.keys()[0]    # only one table --> possibly 2 entries if alias used
                    attr_tup = (this_query.from_tables[table_key], attr_name)


                # TODO validate that attr_list[1] is in attr_list[0]  (that desired attr is in table)
                this_query.select_attr.append(attr_tup)




            ### parse WHERE ###
            parse_where(this_query, where_clause)

            ### TODO parse additional clauses here (after have basics working) ###




            # return a Query object
        pass
    elif dml:
        # create DML object
        dml_obj = DML()

        # classify what type of DML
        if insert:
            # break into table name (and potentially columns) and list of values  --> use INTO and VALUES as delimiters




            # validate table name (if columns are provided, check them against columns for a table with the
            #   specified name --> can distinguish tables with the same name.  If columns don't match any, output error)





            # tokenize list of values on "," and load into dml_obj.values list




            pass
        elif update:
            # break into table name, set values, and where condition



            # validate table name




            # tokenize set values by "," --> produces a list of Comparison objects (use assignment option)






            # call WHERE parser (get back a tree of Comparison objects




            pass
        else:       # delete
            # break into table name and WHERE condition





            # validate table name





            # call WHERE parser (get back a tree of Comparison objects)





            pass
        # return DML object
    elif ddl:
        # breakdown and classify as CREATE or DROP / TABLE or INDEX'




        # treat each case appropriately
        if create_table:
            # get table name (validate --> no other tables with same name/columns combination
            #   (I think.  Might want to only support tables with different names)





            # get list of attributes (tokenize by ','.  Record as tuples of (name, datatype))



            pass
        elif drop_table:
             # get table name (and validate it)


            pass
        elif create_index:
            # break into index name/table information (using ON clause)



            # get index name (and make validate --> no other indices with the same name)



            # validate table name (and columns listed --> column to make index on).  Only accept a single attribute for index creation



            pass
        else:       # drop index
            # validate index name


            pass
    else:       # invalid input.  Throw error and take next line of input
        pass




    # TODO return query tree here
# END parser_main








# parse_where
def parse_where(this_query, where_clause):
    # TODO  add support for attributes without a table name specified (ex. only one table used in query)
    # TODO  support/remove parenthesis --> (does breaking of operands need to be recursive too?? Maybe base case should
            # be no operators found...






    # parse by AND/OR recursively --> once done with this, should have atomic comparisons to perform (at leaf level of comparison tree)
    this_comparison = Comparison()
    if "and" in where_clause:
        and_list = re.split(" and ", inp_line)
        this_comparison.left_operand = parse_where(this_query, and_list[0])
        this_comparison.right_operand = parse_where(this_query, and_list[1])

    elif "or" in where_clause:
        or_list = re.split(" or ", inp_line)
        this_comparison.left_operand = parse_where(this_query, or_list[0])
        this_comparison.right_operand = parse_where(this_query, or_list[1])

    else:   # base case
        # break into operands
        if "==" in where_clause:
            op = "=="
            this_comparison.equal = True

        elif "!=" in where_clause:
            op = "!="
            this_comparison.not_equal = True

        elif ">=" in where_clause:
            op = ">="
            this_comparison.great_equal = True

        elif "<=" in where_clause:
            op = "<="
            this_comparison.less_equal = True

        elif "<" in where_clause:
            op = "<"
            this_comparison.greater = True

        elif ">" in where_clause:
            op = ">"
            this_comparison.less = True

        else:
            # TODO error --> no recognized operation
            pass

        operand_list = re.split(op, where_clause)   # split clause on whichever operation is found first
        this_comparison.left_operand = operand_list[0].rstrip().lstrip()
        this_comparison.right_operand = operand_list[1].rstrip().lstrip()

        for operand in range(len(operand_list)):
            # once at leaf level of comparisons, tokenize on "." to find table names
            helper = object()
            attr_list = attr_name.split(".")  # split to find if alias used
            if len(attr_list) > 1:  # if a table name is specified
                # TODO validate attr_tup here
                helper = (attr_list[0], attr_list[1])

            else:   # no table name specified
                # TODO support attributes with no table specified (only one table in query)
                helper = operand_list[operand].rstrip().lstrip()

            # complete the Comparison object
            if operand == 0:
                this_comparison.left_operand = helper
            else:
                this_comparison.right_operand = helper

        # TODO need to add support for aggregate operators.  Maybe do type checking.  if str --> attribute. If int --> for comparison ??



    return this_comparison
# END parse_where






parser_main()
