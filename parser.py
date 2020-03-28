#                                           #
#   COSC 280 DBMS Project --> Parser code   #
#                                           #




# import statements




# constants






# Query class
class Query:
    def __init__(self):
        ### object attributes ###
        self.select_attr = []
        self.from_tables = {}    # use alias/table name as key --> maps to table name (simplifies parsing)
        self.where = []         # TODO just a placeholder.  This will have to be some tree structure of comparisons


        ### add new flags/variables as needed here.  These flags will be interpreted by the backend ###
        # handle compound (? unsure proper term) queries here
        union = False
        intersect = False
        difference = False
        left_query = Query()
        right_query = Query()

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

        # operations (use equal and greater/less together to signify >=/<=) ...  (can include IN/BETWEEN later?)
        self._and = False
        self._or  = False
        self.equal = False
        self.not_equal = False
        self.greater = False
        self.less = False

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
#   determine how to return Query tree (or just have each type return individually??)
#   support tables with same names and different column sets or just tables with different names
#   support NULLs? --> have to add another value to the tuples in DDL.attr (boolean for NULL/NOT NULL)
#   add support for GROUPBY/HAVING clauses in "query" section
#   include IN/BETWEEN too?




def parser_main():  # parameters: list of tables/columns, string of input
    # take a string as input (contains entire query)


    # classify as DDL or DML --> search for SELECT (a query), INSERT/UPDATE/DELETE (DML), CREATE/DROP (DDL)
    if query:
        # tokenize input on UNION, INTERSECT, DIFFERENCE (break into different query objects and parse them)
        # TODO may have to make contents of this if into a recursive function to allow for compound queries



        # break query into SELECT, FROM, and WHERE clauses (can add GROUPBY/HAVING here too) --> parse FROM first to define all aliases

        ### parse FROM ###
        # tokenize on JOIN (classify as NATURAL JOIN, LEFT OUTER JOIN, RIGHT OUTER JOIN, or INNER JOIN) --> validate contents of ON clause for join too



        # tokenize on ',' to separate other table names from contents of JOINs (and from each other)



        # check each table name for "as" --> recognize aliases and add to from_tables



        ### parse SELECT ###
        # tokenize contents of SELECT on ',' --> break up attributes



        # tokenize each attribute looking for supported aggregate operators (if invalid operator found, throw exception)



        # tokenize each attribute name by "." --> looking for use of aliases.  Confirm validity of aliases and attribute name in that table





        ### parse WHERE ###
        # parse by AND/OR recursively --> once done with this, should have atomic comparisons to perform (at leaf level of comparison tree)


        # once at leaf level of comparisons, tokenize on "." to find aliases




        # validate all table/attribute combinations






        ### TODO parse additional clauses here (after have basics working) ###


        # return a Query object
        pass
    elif DML:
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
    elif DDL:
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