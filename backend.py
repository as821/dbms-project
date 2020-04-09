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
INDEX = {}      # map index name to table name.  Index stored in that table
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
        self.type = ""          # string containing the type of this attribute
# END Attribute class




# Query class
class Query:
    def __init__(self):
        ### object attributes ###
        self.select_attr = []       # give as tuple of (table name, attr name) --> only one table in from then table name can be ""
        self.from_tables = {}       # use alias/table name as key --> maps to table name (simplifies parsing)
        self.where = []
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
        self.index = {}     # index is a hashmap of key --> location in file.  Can be a list of locations in the file if index not on a key
        self.index_attr = ""    # store name of attribute that the index is on
        self.index_name = ""
        self.attr_loc = {}  # should map attribute name to index of attribute in a given tuple




#           #
#   TODO    #
#           #

# implement mid level functions
# test mid level functions (select, projection, join)
# handle foreign key dependencies in remove methods (see if removals impact other tables)









# standardized error function  --> should be in main driver program ... here temporarily
def error(st=" parsing error. Invalid syntax or input."):
    print("\nERROR: ", st)
    raise ValueError
# END error







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
        error("storage object not created.")

    # get index_list from file
    file = open(table_obj.storage.filename, "rb")
    list_location = struct.unpack("L", file.read(8))[0]    # Read first bytes of file and unpack them
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
#   !!! TODO CALLING FUNCTION MUST MAKE SURE ATTRIBUTES ARE IN THE CORRECT ORDER !!!
def write(table_obj, obj_to_insert):    # TODO determine what DS to use (list of lists?? for a relation)
    # if overwrite == True, clear entire relation and populate it with the contents of obj_to_insert
    # obj_to_insert should be a list of lists (a list of instances to insert into the table) --> even if just a list around a single list

    # check existence of Storage object
    if type(table_obj.storage) is object:
        error("storage object not created.")

    # get index_list from file
    file = open(table_obj.storage.filename, "rb+")
    list_location = struct.unpack("L", file.read(8))[0]  # Read first bytes of file and unpack them
    file.seek(list_location, 0)  # seek list location
    index_list = pickle.load(file)

    # insert tuple at back of file (same address tuple_index used to be at)
    file.seek(list_location, 0)  # seek list location
    for obj in obj_to_insert:
        if type(obj) is not list:           # error checking
            error("dev error in backend.write")
        table_obj.storage.num_tuples += 1
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
    # check Storage object exists
    if type(table_obj.storage) is object:
        error("storage object not created.")

    if len(update_list) != len(index_list):
        error("input lists to update function must be same length.")

    # open file, get index list
    file = open(table_obj.storage.filename, "rb+")
    list_location = struct.unpack("L", file.read(8))[0]  # Read first bytes of file and unpack them
    file.seek(list_location, 0)  # seek list location
    file_index_list = pickle.load(file)

    # load relation from memory
    relation = []
    for ind in file_index_list:
        file.seek(ind, 0)   # seek to specified index, with respect to the head of the file
        relation.append(pickle.load(file))

    # replace specified tuples with the corresponding one in update_list
    count = 0
    for up in index_list:
        relation[up] = update_list[count]
        count += 1

    # clear current file contents (after space left for file_index_list file location)
    file.seek(8, 0)
    file.truncate()

    # write updated relation to file
    file_index_list = []
    for r in relation:
        file_index_list.append(file.tell())      # record tuple location in file
        pickle.dump(r, file)              # append tuple to the file

    # record new location of index list
    list_loc = file.tell()
    pickle.dump(file_index_list, file)
    file.seek(0, 0)  # seek head of file to write new location of list of tuple locations in file
    file.write(struct.pack("L", list_loc))

    # close file
    file.close()
# END update




# remove function
def remove(table_obj, key_to_remove):
    # list of ints or a list of lists
    
    # check Storage obj exists
    if type(table_obj.storage) is object:
        error("storage object not created.")
    
    # if list of indices, remove item at that index
    relation = []
    if type(key_to_remove[0]) is int:  # keys are in index form (like index number in the file/list --> tuple #)
        # sort from largest to smallest (delete from back first --> less copying of the list)
        key_to_remove.sort(reverse=True)

        # access relation
        relation = access(table_obj)

        # delete specified tuples
        for i in key_to_remove:
            relation.pop(i)     # trust in calling function that index is valid
            table_obj.storage.num_tuples -= 1

    # if list of lists (match them on primary key (if exists) and remove.  if no primary key, then match entire lists to lists in file to remove proper one)
    elif type(key_to_remove[0]) is list:
        # access relation
        relation = access(table_obj)

        # loop through and match and delete specified tuples
        relation_remove_list = []
        for tup in range(len(relation)):
            k_remove = -1
            for k in range(len(key_to_remove)):
                if relation[tup] == key_to_remove[k]:       # multiple linear search for a match
                    # match found, mark both to be removed
                    k_remove = k
                    relation_remove_list.append(tup)
                    table_obj.storage.num_tuples -= 1
                    break                                   # outer loop tuple was found, move onto the next one

            # remove tuple that is found from key_to_remove list
            if k_remove != -1:
                key_to_remove.pop(k_remove)

        # remove from relation
        count = 0
        relation_remove_list.sort()     # perform lower index removals first to avoid issues with negative indexing
        for r in relation_remove_list:
            relation.pop(r-count)
            count += 1      # accounts for offset from deleting tuples at a lower index than this one

        # check that key_to_remove is empty (error if not)
        if len(key_to_remove):   # empty relations evaluate to boolean false
            error("trying to remove a tuple that does not exist.")

    else:
        error("input must be an integer index in the relation or a list of lists")



    # write remaining relation back to file
    file = open(table_obj.storage.filename, "rb+")
    file.seek(8, 0)         # leave space for the index_list header
    file.truncate()         # delete rest of file contents (overwrite it)
    index_list = []

    # insert tuple at back of file (same address tuple_index used to be at)
    for obj in relation:
        if type(obj) is not list:  # error checking
            error("backend.remove write back")

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
#   !!! TODO IN CREATE TABLE COMMAND MUST POPULATE ATTRIBUTE INFORMATION FOR OTHER FUNCTIONS TO WORK !!!
def create_relation_storage(table_obj, storage_dir):
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
def delete_relation_storage(table_obj):
    # check if Storage object exists
    if type(table_obj.storage) is object:
        error("storage object not created.")
    
    # delete file
    os.remove(table_obj.storage.filename)

    # remove references to Storage obj from table_obj
    del table_obj.storage
    table_obj.storage = object()    # placeholder
# END delete_relation






# create index function (create the underlying data structure for an index) --> only call on tables with a Storage obj defined
def create_index(table_obj, index_name, attr):  # index_name is user specified index name.  Attr is the attribute the index is on
    if type(table_obj.storage) is object:
        error("storage object not created.")

    # open file
    file = open(table_obj.storage.filename, "rb+")
    list_location = struct.unpack("L", file.read(8))[0]
    file.seek(list_location, 0)  # seek list location
    index_list = pickle.load(file)

    # load relation from memory
    relation = []
    for ind in index_list:
        file.seek(ind, 0)  # seek to specified index, with respect to the head of the file
        relation.append(pickle.load(file))

    # done with file I/O, close file
    file.close()

    # determine which index in the tuples is attr
    if attr not in table_obj.storage.attr_loc:
        error("tried to create an index on nonexistent attribute.")

    attr_location = table_obj.storage.attr_loc[attr]


    # record file location for the tuple in index dictionary, using attr value as key (not making changes to tuples --> no need to write back)
    for r in range(len(relation)):
        # if this attribute value has not been found yet
        if relation[r][attr_location] not in table_obj.storage.index:
            table_obj.storage.index[relation[r][attr_location]] = []    # allows indexing on non-keys if necessary

        # add file location to the index
        table_obj.storage.index[relation[r][attr_location]].append(index_list[r])


    # add this index to INDEX
    INDEX[index_name] = table_obj.name
    table_obj.storage.index_name = index_name
    table_obj.storage.index_attr = attr
# END create_index






# delete index function (delete an existing index)
def delete_index(table_obj, index_name):
    if index_name not in INDEX:
        error("trying to delete a non-existent index.")

    # remove key from INDEX
    INDEX.pop(index_name)

    # delete contents of index data structure
    table_obj.storage.index = {}
    table_obj.storage.index_attr = ""
    table_obj.storage.index_name = ""
# END delete_index







# access index
def access_index(table_obj, index_val, index_name):
    # check storage object exists
    if type(table_obj.storage) is object:
        error("storage object not created.")

    # check index validity
    if index_name not in INDEX:
        error("trying to access a nonexistent index.")
    elif INDEX[index_name] != table_obj.name:
        error("index specified does not match table.")
    elif index_val not in table_obj.storage.index:
        error("specified index key does not exist in index.")

    # open file
    file = open(table_obj.storage.filename, "rb")

    # load specific tuples
    tup = []
    for loc in table_obj.storage.index[index_val]:
        file.seek(loc, 0)  # seek to specified index, with respect to the head of the file
        tup.append(pickle.load(file))

    # close file
    file.close()

    # return
    return tup
# END access_index









# write index       obj_to_insert must be a list of lists, even if only inserting one tuple
def write_index(table_obj, obj_to_insert):
    # check existence of Storage object
    if type(table_obj.storage) is object:
        error("storage object not created.")

    # check index validity
    if table_obj.storage.index_attr == "":
        error("cannot write to a nonexistent index.")

    # get the location in the tuple that the index key is stored at
    index_key = table_obj.storage.attr_loc[table_obj.storage.index_attr]

    # get index_list from file
    file = open(table_obj.storage.filename, "rb+")
    list_location = struct.unpack("L", file.read(8))[0]  # Read first bytes of file and unpack them
    file.seek(list_location, 0)  # seek list location
    index_list = pickle.load(file)

    # insert tuple at back of file (same address tuple_index used to be at)
    file.seek(list_location, 0)  # seek list location
    for obj in obj_to_insert:
        # error checking in function input
        if type(obj) is not list:
            error("backend.write invalid input format.")

        # add tuple to file
        loc = file.tell()
        index_list.append(loc)  # record tuple location in file
        pickle.dump(obj, file)  # append tuple to the file
        table_obj.storage.num_tuples += 1

        # record tuple location in index
        if obj[index_key] not in table_obj.storage.index:
            table_obj.storage.index[obj[index_key]] = []    # if attr value not previously in index, make an empty list
        table_obj.storage.index[obj[index_key]].append(loc) # store location in the file

    # write index_list and its location back to the file
    list_loc = file.tell()
    pickle.dump(index_list, file)
    file.seek(0, 0)  # seek head of file to write new location of list of tuple locations in file
    file.write(struct.pack("L", list_loc))

    # close file
    file.close()
# END write_index









# remove_index  (remove function for tables with indices) --> does not use index to access table, only updates it on deletion
def remove_index(table_obj, key_to_remove):
    # check Storage obj exists
    if type(table_obj.storage) is object:
        error("storage object not created.")

    # check index validity
    if table_obj.storage.index_attr == "":
        error("called remove_index on a nonexistent index.")

    # clear the current index, all going to be overwritten in writing back to the file
    table_obj.storage.index = {}

    # if list of indices, remove item at that index
    relation = []
    if type(key_to_remove[0]) is int:  # keys are in index form (like index number in the file/list --> tuple #)
        # sort from largest to smallest (delete from back first --> less copying of the list)
        key_to_remove.sort(reverse=True)

        # access relation
        relation = access(table_obj)

        # delete specified tuples
        for i in key_to_remove:
            table_obj.storage.num_tuples -= 1
            relation.pop(i)  # trust in calling function that index is valid

    # if list of lists (match them on primary key (if exists) and remove.  if no primary key, then match entire lists to lists in file to remove proper one)
    elif type(key_to_remove[0]) is list:
        # access relation
        relation = access(table_obj)

        # loop through and match and delete specified tuples
        relation_remove_list = []
        for tup in range(len(relation)):
            k_remove = -1
            for k in range(len(key_to_remove)):
                if relation[tup] == key_to_remove[k]:  # multiple linear search for a match
                    # match found, mark both to be removed
                    k_remove = k
                    relation_remove_list.append(tup)
                    table_obj.storage.num_tuples -= 1
                    break  # outer loop tuple was found, move onto the next one

            # remove tuple that is found from key_to_remove list
            if k_remove != -1:
                key_to_remove.pop(k_remove)

        # remove from relation
        count = 0
        relation_remove_list.sort()  # perform lower index removals first to avoid issues with negative indexing
        for r in relation_remove_list:
            relation.pop(r - count)
            count += 1  # accounts for offset from deleting tuples at a lower index than this one

        # check that key_to_remove is empty (error if not)
        if len(key_to_remove) != 0:  # empty relations evaluate to boolean false
            error("tried to delete a tuple that does not exist.")

    else:
        error("invalid input not an integer or a list of lists.")

    # write remaining relation back to file
    file = open(table_obj.storage.filename, "rb+")
    file.seek(8, 0)  # leave space for the index_list header
    file.truncate()  # delete rest of file contents (overwrite it)
    index_list = []


    # determine location of index_attr in tuples of the relation
    index_loc = table_obj.storage.attr_loc[table_obj.storage.index_attr]

    # insert tuple at back of file (same address tuple_index used to be at)
    for obj in relation:
        if type(obj) is not list:  # error checking
            error("backend.remove write back invalid input format (not a list)")

        # write tuple to file
        loc = file.tell()
        index_list.append(loc)  # record tuple location in file
        pickle.dump(obj, file)  # append tuple to the file

        # check if attribute value already in the index, if not --> make an empty list
        if obj[index_loc] not in table_obj.storage.index:
            table_obj.storage.index[obj[index_loc]] = []

        # add tuple location to the index
        table_obj.storage.index[obj[index_loc]].append(loc)
    list_loc = file.tell()
    pickle.dump(index_list, file)
    file.seek(0, 0)  # seek head of file to write new location of list of tuple locations in file
    file.write(struct.pack("L", list_loc))
    file.close()
# END remove_index














# update function
#   need this in addition to the write function since write either inserts to back or
#       overwites existing data
#   pass in list of lists (list of instances) that contain the updates
#   pass in a list of the same size that contains a list of indices to update
def update_index(table_obj, update_list, index_list):
    # cheap way to update with an index --> update as if no index
    update(table_obj, update_list, index_list)

    # then update index after all updates (create_index does not check for any existing indices)
    create_index(table_obj, table_obj.storage.index_name, table_obj.storage.index_attr)
# END update_index

























#
#   Mid level functions
#


# create table      attr is an ordered list tuples containing attributes
def create_table(table_name, attr):
    # input validation
    if table_name in TABLES:
        error("relations cannot share names.")

    if type(attr) is not list:
        error("attr must be a list.")

    # create Table object and populate it
    table = Table()
    table.name = table_name
    table.num_attributes = len(attr)

    table.attribute_names.clear()
    table.attributes.clear()

    for at in attr:
        table.attribute_names.add(at[0])
        attr_obj = Attribute()
        attr_obj.name = at[0]       # index 0 of the tuple is attribute name
        attr_obj.type = at[1]       # index 1 of the tuple is attribute type (a string)
        table.attributes.append(attr_obj)


    # create storage structure for this relation
    create_relation_storage(table, STORAGE_DIR)

    # populate dictionary of attribute locations
    for i in range(len(attr)):
        table.storage.attr_loc[attr[i][0]] = i  # map attribute name to its location in the stored tuple

    # add this table to the global dictionary of tables
    TABLES[table_name] = table

    # return the initialized Table object
    return table
# END create_table








# select function
# relations are a list of lists.  Condition is a comparison object (should be atomic not contain other Comparison objects)
# attr_index specifies the index of the attribute to be compared in the relation (which column number) --> avoid needing table knowledge here
# if only trying to perform a one-table selection, only populate relation1, condition, and attr_index1. Leave rest null
def selection(relation1, relation2, condition, attr_index1, attr_index2):
    return_relation = []

    # determine value to compare against
    if type(condition.left_operand) == tuple:
        if type(condition.right_operand) == tuple:
            # TODO join.  Comparison between table attributes.  Best performed with a join (theta join or natural join)
            # perform a join to perform this as an equi join
            if condition.equal:
                return_relation = join(relation1, relation2, attr_index1, attr_index2, "equi")
            else:
                error(" only equality comparison permitted for conditions comparing attribute values from 2 tables.")
            return return_relation
        else:
            value = condition.right_operand
    else:
        value = condition.left_operand


    # perform a standard selection (one table)
    # loop through relation and perform operation
    for instance in range(len(relation1)):
        if condition.equal:
            if relation1[instance][attr_index] == value:
                return_relation.append(relation1[instance])
        elif condition.not_equal:
            if relation1[instance][attr_index] != value:
                return_relation.append(relation1[instance])
        elif condition.greater:
            if relation1[instance][attr_index] > value:
                return_relation.append(relation1[instance])
        elif condition.less:
            if relation1[instance][attr_index] < value:
                return_relation.append(relation1[instance])
        elif condition.great_equal:
            if relation1[instance][attr_index] >= value:
                return_relation.append(relation1[instance])
        elif condition.less_equal:
            if relation1[instance][attr_index] <= value:
                return_relation.append(relation1[instance])
        else:
            error(" no operation specified in condition.")
    return return_relation
# END select



# projection function
# relation is list of lists.  Indexes are the indices of the attributes to include in the returned relation
def projection(relation, indexes):
    if type(indexes) is not list:
        error(" indexes parameter to projection function must be a list of integers.")
    return_relation = list()
    for instance in relation:
        tup = list()
        for i in indexes:
            tup.append(instance[i])
        return_relation.append(tup)


    # TODO remove duplicates
    return return_relation
# END projection




# join function (pass a parameter to signify which join to do)  --> need to consider which kind of join in best.  This probably should be handled by the optimizer
# relation1 and relation2 are list of lists
# attr1 and attr2 are attribute indexes (specified by ON clause)
# typ refers to the type of join --> (equi, outer, left, right)  outer refers to full outer join.  Left and right are outer joins
def join(relation1, relation2, attr1, attr2, typ, nested=True, natural_list = None):

    #
    #   TODO add option for other type of join (currently always doing nested loop.  Want to support sort/merge too)
    #


    return_relation = []
    if typ == "equi":       # equi joins
        if nested:
            for r1 in relation1:
                for r2 in relation2:
                    if r1[attr1] == r2[attr2]:
                        # add all attributes from r2 to r1, except the shared attribute (at index attr2 in r2)
                        helper = r1.append(r2[r] for r in range(len(r2)) if r != attr2)
                        return_relation.append(helper)
        else:
            # sort left relation on join attribute
            relation1 = sorted(relation1, key=lambda x: x[attr1])

            # sort right relation
            relation2 = sorted(relation2, key=lambda x: x[attr2])

            # scan both tables for matches.  If match found, then add to return relation
            inner_index = 0       # let relation2 be the inner relation and relation1 be the outer relation
            outer_index = 0
            while inner_counter < len(relation2) and outer_counter < len(relation1):    # while still samples in both relations
                key = min(relation2[inner_index][attr2], relation1[outer_index][attr1])
                inner_group = []
                while inner_index < len(relation2) and key == relation2[inner_index][attr2]:    # collects all samples with the given value
                    inner_group.append(relation2[inner_index])
                    inner_index += 1
                outer_group = []
                while outer_index < len(relation1) and key == relation1[outer_index][attr1]:    # collects all samples with the given value
                    outer_group.append(relation1[outer_index])
                    outer_index += 1

                # due to key == statements, contents of i and o match.  Handle left/right joins by adding Nones to the samples of the specific tables (even if no matching in the other table, still include the tuple from the given relation)
                # Here you can handle left or right join by replacing an empty group with a group of one empty row (None,)*len(row)
                for i in inner_group:
                    for o in outer_group:
                        return_relation.append(i+o)     # TODO need to add code here to fix the merging of these attributes (so is in conjunction with attr dictionaries)





    elif typ == "natural":
        if natural_list is not list:
            error("invalid natural_list in natural join")
        if natural_list[0] is not tuple:
            error("invalid natural_list contents in natural join")

        if nested:
            for r1 in relation1:
                for r2 in realtion2:
                    # for each pair of samples, look to see if they match on the common attributes passed to the function
                    match = True
                    for attr in natural_list:
                        if r1[attr[0]] != r2[attr[1]]:      # check if values at the common attribute indices are match
                            match = False
                            break
                    if match:
                        common_list = [i[1] for i in natural_list]      # take the common attribute indices for the second relation and put into list form
                        helper = r1.append(r2[r] for r in range(len(r2)) if r not in common_list)
                        return_relation.append(helper)
        else:
            pass        # TODO ?? with multiple attributes will be hard.  Maybe just reroute those with one attribute in common to a sort/merge equijoin and only handle multi-attribute natural joins with nested loop

    elif typ == "outer":    # full outer join
        if nested:
            for r1 in relation1:
                for r2 in relation2:
                    if r1[attr1] == r2[attr2]:
                        # add all attributes from r2 to r1, except the shared attribute (at index attr2 in r2)
                        helper = r1.append(r2[r] for r in range(len(r2)) if r != attr2)
                        return_relation.append(helper)
                    else:
                        # r1 --> pad back with nulls for the number of attributes in r2-1 (account for shared attribute)
                        left_helper = r1
                        for i in range(len(r2)-1):  # -1 to account for shared attribute
                            left_helper.append(None)

                        # r2 --> pad front with nulls for the number of attributes in r1 (place r2[attr2] value in attr1 index)
                        right_helper = []
                        for i in range(len(r1)):
                            if i == attr1:
                                right_helper.append(r2[attr2])
                            else:
                                right_helper.append(None)
                        for i in range(len(r2)):
                            if i != attr2:  # add contents of r2 (excluding r2[attr2] it was previously added
                                right_helper.append(r2[i])

                        # add both r1 and r2 to return relation
                        return_relation.append(left_helper)
                        return_relation.append(right_helper)
        else:
            pass
    elif typ == "left":     # left outer join
        if nested:
            for r1 in relation1:
                for r2 in relation2:
                    if r1[attr1] == r2[attr2]:
                        # add all attributes from r2 to r1, except the shared attribute (at index attr2 in r2)
                        helper = r1.append(r2[r] for r in range(len(r2)) if r != attr2)
                        return_relation.append(helper)
                    else:
                        # r1 --> pad back with nulls for the number of attributes in r2-1 (account for shared attribute)
                        left_helper = r1
                        for i in range(len(r2)-1):  # -1 to account for shared attribute
                            left_helper.append(None)

                        # add r1 (from left relation) to return relation
                        return_relation.append(left_helper)
        else:
            pass
    elif typ == "right":    # right outer join
        if nested:
            for r1 in relation1:
                for r2 in relation2:
                    if r1[attr1] == r2[attr2]:
                        # add all attributes from r2 to r1, except the shared attribute (at index attr2 in r2)
                        helper = r1.append(r2[r] for r in range(len(r2)) if r != attr2)
                        return_relation.append(helper)
                    else:
                        # r2 --> pad front with nulls for the number of attributes in r1 (place r2[attr2] value in attr1 index)
                        right_helper = []
                        for i in range(len(r1)):
                            if i == attr1:
                                right_helper.append(r2[attr2])
                            else:
                                right_helper.append(None)
                        for i in range(len(r2)):
                            if i != attr2:  # add contents of r2 (excluding r2[attr2] it was previously added
                                right_helper.append(r2[i])

                        # add both r2 (right relation) to return relation
                        return_relation.append(right_helper)
        else:
            pass
    else:
        error(" invalid join type specified")

    return list(set(return_relation))   # remove duplicates
# END join







# aggregate functions (min, max, sum).  Given a relation (list of lists) and an attribute index.
#       Ex. min --> return relation of all tuples where the value at the given index == the min value in that attribute







# set operations (given 2 relations, find difference, intersect, or union --> individual function for each)







# DML insert operation






# DML update operation




# DML delete operation









#
#   High level function
#
# task manager function.  Takes output from optimizer and calls necessary backend functions to execute the query
def task_manager():
    # table testing
    table = create_table("test_rel", [("name", "string"), ("age", "int"), ("year", "int")])
    access(table)

    inp = [["Andrew", 21, 1999], ["Bob", 83, 1800]]
    write(table, inp)

    print(access(table))

    update(table, [["Joe", 56, 1955]], [1])
    print(access(table))






    # index testing
    create_index(table, "test_ind", "name")
    print(access_index(table, "Andrew", "test_ind"))
    write_index(table, [["Fred", 43, 1200]])
    print(access(table))
    remove_index(table, [['Joe', 56, 1955]])
    print(access(table))
    update_index(table, [["Rob", 100, 9]], [1])
    print(access(table))
    print(access_index(table, "Rob", "test_ind"))









# END task_manager










# call highest level function
task_manager()








