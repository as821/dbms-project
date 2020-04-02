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
#       size of a pickled object https://stackoverflow.com/questions/25653440/determine-size-of-pickled-datetime
#       python file I/O https://www.programiz.com/python-programming/file-operation




#
#   Shared code with parser (once project is pieced together, they will all just be defined in one place
#






# import statements
import os, struct
import pickle







# constants/global variables
TABLES = {}     # dictionary of Table objects.      TODO this should be declared in main driver program... here temporarily
STORAGE_DIR = "/Users/andrewstange/Desktop/Spring_2020/COSC280/dbms_proj/backend/storage/"

# Class declaration/definitions
# Table class   (not specific to parser.  Should be in main driver file)
class Table:
    def __init__(self):
        self.name = ""
        self.num_attributes = 0
        self.attribute_names = set()    # list of strings (for fast look up on table/attr validation)
        self.attributes = []            # list of Attribute objects for this Table
        self.storage = object()         # object() is a placeholder for Storage obj
# END Table class




# Attribute class   (not specific to parser.  Should be in main driver file)
class Attribute:
    def __init__(self):
        self.name = ""
                                # TODO don't think this table attribute is needed
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
# Storage class
class Storage:
    def __init__(self):
        self.filename = ""
        self.num_tuples = 0
        self.index = {}     # index is a hashmap of key --> location in file
        self.attr_loc = {}  # should map attribute name to index of attribute in a given tuple




#           #
#   TODO    #
#           #

#   determine how access paths will work (on index, on primary key --> linear search, or on simple linear search for a match if no primary key specified)
#   make a second low-level access function to be called when an indexed attribute (ex. primary key) is being searched on

#   Determine what DS to use for general storage
#   determine how remove function should work
#   Complete low level functions
#   Determine how indexes will work and implement

#   low level update function is needed too
#   would an attribute-specific storage be more effective than a tuple-specific storage considering the operations we perform --> tuple-specific is good for doing selections first while attribute-specific storage is good for doing projections first








#
#   Low level functions
#

# access function
#   (all general accesses to DB should be through this function)
def access(table_obj):
    # This function should return entire relation to the caller.  Caller can handle application of any selections/projections.
    # This should be a general purpose interface to the underlying data structure
    
    
    # check existence of Storage object
    if type(table_obj.storage) is object:
        # TODO error    Storage object has not been created, so we cannot access it
        print("ERROR")
        pass

    # get index_list from file
    file = open(table_obj.storage.filename, "rb")
    list_location = struct.unpack("L", file.read(8))[0]    # python long is 4 bytes.  Read first 4 bytes of file and unpack them
    file.seek(list_location, 0)   # seek list location
    index_list = pickle.load(file)


    # load relation from memory
    relation = []
    for ind in index_list:
        file.seek(ind, 0)   # seek to specified index, with respect to the head of the file
        relation.append(pickle.load(file))


    # close file
    file.close()
    
    # return the entire relation
    return relation
# END access










# write function
#   (all write operations to DB through this function.  Need to find location(s) to write to and then make those edits
#   (should be like an update function).  Must update any relevant indices too )
def write(table_obj, obj_to_insert, overwrite=False):    # TODO determine what DS to use (list of lists?? for a relation)
    # if overwrite == True, clear entire relation and populate it with the contents of obj_to_insert
    # obj_to_insert should be a list of lists (a list of instances to insert into the table) --> even if just a list around a single list

    # TODO check if index exists.  if so, have to determine where to insert the tuple, insert it there, and insert new tuple into index


    # check existence of Storage object
    if type(table_obj.storage) is object:
        # TODO error    Storage object has not been created, so we cannot access it
        print("ERROR")
        pass

    # get index_list from file
    file = open(table_obj.storage.filename, "rb")
    list_location = struct.unpack("L", file.read(8))[0]  # Read first bytes of file and unpack them
    file.seek(list_location, 0)  # seek list location
    index_list = pickle.load(file)

    # insert tuple at back of file (same address tuple_index used to be at)
    file.seek(list_location, 0)  # seek list location
    for obj in obj_to_insert:
        if type(obj) is not list:           # error checking
            # TODO error    (developer error)
            print("ERROR: dev error in backend.write")

        index_list.append(file.tell())      # record tuple location in file
        pickle.dump(obj, file)              # append tuple to the file
    list_loc = file.tell()
    pickle.dump(index_list, file)
    file.seek(0, 0)     # seek head of file to write new location of list of tuple locations in file
    file.write(struct.pack("L", list_loc))

    # close file
    file.close()
# END write







# update function
#   need this in addition to the write function since write either inserts to back or
#       overwites existing data
#   pass in list of lists (list of instances) that contain the updates
#   pass in a list of the same size that contains a list of indices to update
def update(table_obj, update_list, index_list):
    pass
    
# END update









# remove function
#   (for removing a particular attribute/tuple from the DB.  update relevant indices as well)
def remove(table_obj, key_to_remove):
    # TODO decide how key_to_remove should work --> list of tuples to remove? list of keys or tuple indices to remove?
    
    # check Storage obj exists
    if type(table_obj.storage) is object:
        # TODO error
        pass
    
    # if list of indices, remove item at that index
    relation = []
    if type(key_to_remove[0]) is not int:  # keys are in index form (like index number in the file/list --> tuple #)
        # sort from largest to smallest (delete from back first --> less copying of the list)
        keys_to_remove.sort(reverse=True)

        # access relation
        relation = access(table_obj)

        # delete specified tuples
        for i in key_to_remove:
            relation.pop(i)     # trust in calling function that index is valid

    # if list of lists (match them on primary key (if exists) and remove.  if no primary key, then match entire lists to lists in file to remove proper one)
    elif type(key_to_remove[0]) is list:
        # access relation
        relation = access(table_obj)

        # loop through and match and delete specified tuples
        relation_remove_list = []
        for tup in range(len(relation)):
            k_remove = val
            for k in range(len(key_to_remove)):
                if relation[tup] == key_to_remove[k]:       # multiple linear search for a match
                    # match found, mark both to be removed
                    k_remove = val
                    relation_remove_list.append(rel)
                    break                                   # outer loop tuple was found, move onto the next one

            # remove tuple that is found from key_to_remove list
            key_to_remove.pop(k_remove)

        # remove from relation
        count = 0
        relation_remove_list.sort()     # perform lower index removals first to avoid issues with negative indexing
        for r in relation_remove_list:
            relation.pop(r-count)
            count += 1      # accounts for offset from deleting tuples at a lower index than this one

        # check that key_to_remove is empty (error if not)
        if not key_to_remove:   # empty relations evaluate to boolean false
            # TODO error    dev error.  Tried to delete a tuple that does not exist
            pass

    else:
        # TODO error    dev mistake. Data type besides list of ints or list of lists are not allows
        pass



    # write remaining relation back to file
    file = open(table_obj.storage.filename, "rb")
    file.seek(8, 0)         # leave space for the index_list header
    file.truncate()         # delete rest of file contents (overwrite it)
    index_list = []

    # insert tuple at back of file (same address tuple_index used to be at)
    for obj in relation:
        if type(obj) is not list:  # error checking
            # TODO error    (developer error)
            print("ERROR: dev error in backend.remove write back")

        index_list.append(file.tell())  # record tuple location in file
        pickle.dump(obj, file)  # append tuple to the file
    list_loc = file.tell()
    pickle.dump(index_list, file)
    file.seek(0, 0)  # seek head of file to write new location of list of tuple locations in file
    file.write(struct.pack("L", list_loc))
    file.close()
# END remove






# create relation function
#   (for CREATE table command.  Produce the data structure for the given relation)
def create_relation(table_obj, storage_dir):
    table_obj.storage = Storage()
    table_obj.storage.filename = storage_dir + table_obj.name
    
    file = open(table_obj.storage.filename, "wb+")  # open binary file for r/w
    file.write(struct.pack('L', 0))     # leave space at head of file for location of index table
    tuple_index = []                    # no tuples to insert into file yet, so empty
    index_location = file.tell()
    pickle.dump(tuple_index, file)      # write empty list to file
    file.seek(0, 0)                     # seek to beginning of the file
    file.write(struct.pack('L', index_location))    # write location of index table
    file.close()
# END create relation





# delete relation function
#   (for DROP table function. This function only deletes the underlying data structure)
def delete_relation(table_obj):
    # check if Storage object exists
    if type(table_obj.storage) is object:
        # TODO error
        pass
    
    # delete file
    os.remove(table_obj.storage.filename)

    # remove references to Storage obj from table_obj
    del table_obj.storage
    table_obj.storage = object()    # placeholder
# END delete_relation






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
def task_manager():
    table = Table()
    table.name = "test_rel"
    create_relation(table, STORAGE_DIR)
    access(table)


# END task_manager










# call highest level function
task_manager()








