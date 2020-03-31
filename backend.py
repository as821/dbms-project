#
#   Backend functions   (a collection of functions with varying level of proximity to the actual data structures)
#




#
#   General design thoughts
#

#       for now, use numpy matrices as the underlying data structure
#       use dictionary type for indices --> dictionaries in python are hashmaps

#       store data in a file when not in use (serialize with pickle),  when want to access, load the file,
#               seek to desired location, and load into memory

#       how to create a DB index in python https://stackoverflow.com/questions/5580201/seek-into-a-file-full-of-pickled-objects





#
#   Shared code with parser (once project is pieced together, they will all just be defined in one place
#


# constants/global variables
TABLES = {}     # dictionary of Table objects.      TODO this should be declared in main driver program... here temporarily

# Class declaration/definitions
# Table class   (not specific to parser.  Should be in main driver file)
class Table:
    def __init__(self):
        self.name = ""
        self.num_attributes = 0
        self.num_tuples = 0
        self.attribute_names = set()    # list of strings (for fast look up on table/attr validation)
        self.attributes = []            # list of Attribute objects for this Table
# END Table class




# Attribute class   (not specific to parser.  Should be in main driver file)
class Attribute:
    def __init__(self):
        self.name = ""
        self.table = object()   # reference to the table that this attribute is a part of
        self.type = ""          # string containing the type of this attribute
# END Attribute class




# Query class
class Query:
    def __init__(self):
        ### object attributes ###
        self.select_attr = []       # give as tuple of (table name, attr name) --> only one table in from then table name can be ""
        self.from_tables = {}       # use alias/table name as key --> maps to table name (simplifies parsing)
        self.where = []             # TODO just a placeholder.  This will have to be some tree structure of comparisons
        self.num_tables = 0

        ### add new flags/variables as needed here.  These flags will be interpreted by the backend ###
        # handle compound (? unsure proper term) queries here
        self.union = False
        self.intersect = False
        self.difference = False
        self.left_query = object()   # Query reference
        self.right_query = object()  # Query reference

        # aggregate operators.  For each supported operator maintain a list of indices (of select attr list) to apply that operator to
        self.max = []
        self.min = []
        self.avg = []
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
        self.set = []   # empty list of Comparison objects.  Set assignment = True

        # update and delete
        self.where = object()   # placeholder until filled with a Comparison object by parser
# END DML class





# DDL class
class DDL:
    def __init__(self):
        self.table = False      # set to false to signify INDEX  (initialized to a placeholder value)
        self.create = False     # set to false to signify DROP

        self.table_name = ""
        self.index_name = ""
        self.attr = []      # list of tuples of (attribute name, data type)














#   Additional classes needed for use in backend









#
#   Low level functions
#

# access function
#   (all accesses to DB MUST be through this function.  Should optimize access paths (check for available indices) and
#   return specified result from specified table)










# write function
#   (all write operations to DB through this function.  Need to find location(s) to write to and then make those edits
#   (should be like an update function).  Must update any relevant indices too )







# remove function
#   (for removing a particular attribute/tuple from the DB.  update relevant indices as well)






# create relation function
#   (for CREATE table command.  Produce the data structure for the given relation)







# delete relation function
#   (for DROP table function. Delete underlying data structure)






# create index function
#   (create the underlying data structure for an index)





# delete index function
#   (delete an existing index)







# update index (called when adding a tuple to a relation with an existing index)












#
#   Mid level functions
#
# select function





# projection function






# join function (pass a parameter to signify which join to do)  --> need to consider which kind of join in best.  This probably should be handled by the optimizer







# aggregate functions (min, max, sum)







# set operations (given results from 2 selects, find difference, intersect, or union --> individual function for each)







# DML insert operation






# DML update operation




# DML delete operation









#
#   High level function
#
# task manager function.  Takes output from optimizer and calls necessary backend functions to execute the query











