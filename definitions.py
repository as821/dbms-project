#                                               #
#   COSC 280 Project 2      definitions.py      #
#                                               #

# import statements
import os, struct, pickle, itertools, re, time
from prettytable import PrettyTable
import tabulate as t



# constants/global variables
TABLES = {}     # dictionary of Table objects.      TODO this should be declared in main driver program... here temporarily
INDEX = {}      # map index name to table name.  Index stored in that table
STORAGE_DIR = "/Users/andrewstange/Desktop/Spring_2020/COSC280/dbms_proj/backend/storage/"
JOIN_MULT = 1.5   # value multiplied by number of tuples in a table to determine if join should be nested loop or sort/merge


# Class declaration/definitions
# Table class   (not specific to parser.  Should be in main driver file)
class Table:
    def __init__(self):
        self.name = ""
        self.num_attributes = 0
        self.num_tuples = 0
        self.attribute_names = set()    # list of strings (for fast look up on table/attr validation)
        self.attributes = []            # list of Attribute objects for this Table
        self.storage = object()         # object() is a placeholder for Storage obj
        self.primary_key = ""
        self.foreign_key = tuple()      # format: (local_attr, parent table, parent attribute)
        self.child_tables = []          # list of tuples: (local_attribute, child table, child attribute)
        self.now = False
# END Table class




# Attribute class   (not specific to parser.  Should be in main driver file)
class Attribute:
    def __init__(self):
        self.name = ""
        self.type = ""          # string containing the type of this attribute
# END Attribute class




# Query class
class Query:
    def __init__(self):
        ### object attributes ###
        self.select_attr = []       # give as tuple of (table name, attr name) --> only one table in from then table name can be ""
        self.from_tables = {}       # use alias/table name as key --> maps to table name (simplifies parsing)
        self.where = []             # TODO backend says [], parser says object()
        self.alias = set()
        self.num_tables = 0

        ### add new flags/variables as needed here.  These flags will be interpreted by the backend ###
        # handle compound (? unsure proper term) queries here
        self.union = False
        self.intersect = False
        self.difference = False
        self.now = False
        self.left_query = object()   # Query reference
        self.right_query = object()  # Query reference

        # aggregate operators.  For each supported operator maintain a list of indices (of select attr list) to apply that operator to
        self.max = []
        self.min = []
        self.avg = []
        self.sum = []
        self.count = []

        # joins
        self.joins = []
# END Query class




# Comparison class
class Comparison:
    def __init__(self):
        # operands  (breaking comparisons into trees of comparisons allows for more complex+nested comparisons)
        self.right_operand = object()   # using object base class should allow for placing either another Comparison() here or an integer or a string
        self.left_operand = object()
        self.leaf = False

        # operations ...  (can include IN/BETWEEN later?)
        self.and_ = False
        self.or_  = False
        self.equal = False
        self.not_equal = False
        self.greater = False
        self.less = False
        self.great_equal = False
        self.less_equal = False
        self.assignment = False     # only to be used in support of UPDATE DML command

    def copy(self, comp):
        # operands  (breaking comparisons into trees of comparisons allows for more complex+nested comparisons)
        self.right_operand = comp.right_operand
        self.left_operand = comp.left_operand
        self.leaf = comp.leaf

        # operations ...  (can include IN/BETWEEN later?)
        self.and_ = comp.and_
        self.or_  = comp.or_
        self.equal = comp.equal
        self.not_equal = comp.not_equal
        self.greater = comp.greater
        self.less = comp.less
        self.great_equal = comp.great_equal
        self.less_equal = comp.less_equal
        self.assignment = comp.assignment
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
        self.now = False        # whether to record time for each insert to the table

        self.table_name = ""
        self.index_name = ""
        self.attr = []      # list of tuples of (attribute name, data type)
        self.primary_key = ""
        self.foreign_key = tuple()






#   Additional classes needed for use in backend
# Storage class
class Storage:
    def __init__(self):
        self.filename = ""
        self.num_tuples = 0
        self.index = {}     # index is a hashmap of key --> location in file.  Can be a list of locations in the file if index not on a key
        self.index_attr = ""    # store name of attribute that the index is on
        self.index_name = ""
        self.attr_loc = {}  # should map attribute name to index of attribute in a given tuple





# standardized error function  --> should be in main driver program ... here temporarily
def error(st=" parsing error. Invalid syntax or input."):
    print("\nERROR: ", st)
    raise ValueError
# END error
