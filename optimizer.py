import parser


# optimizer.  Handles execution of atomic queries (NOT higher level queries with UNION/DIFFERENCE/INTERSECTION)

# to access the result of a join (stored in from_tables --> JOIN_leftTableName_rightTableName)  (user-inputted table names can never be capitalized)
# when accessing attribute from a table that was joined, redirect to the join.  from_tables maps original relation name to JOIN... name (these pre-join tables
#       are the only non-alias strings left in from_tables)





#
#   TODO
#
#   TODO based on relative sizes of relations, determine what type of join (sort vs. linear) --> need to update the join function (backend.py) to reflect these options
#   TODO pick outer loop for joins too (should be the smaller relation)
#   TODO determine what joins to do first in the case of a multiple joins
#   TODO add support for aggregate operators

#   TODO be careful not to remove an attribute needed for a join in an early projection (!!! this is a current bug)




def optimizer(this_query):
    # check that queries are atomic
    if this_query.union or this_query.intersect or this_query.difference:
        left = optimizer(this_query.left_query)
        right = optimizer(this_query.right_query)

        result = []
        if this_query.union:
            # TODO call union function from backend.py and return result
            pass
        elif this_query.intersect:
            # TODO call intersect function from backend.py and return result
            pass
        else:   # this_query.difference
            # TODO call difference function from backend.py and return result
            pass

        return result
    # END non-atomic query handling






    # simple query optimizer    (perform joins sequentially)






    # load all relations needed for this query
    # look through selects for any indexed attributes (only have to retrieve samples with that given value
    obtained = []
    pop_list = []
    for c in range(len(this_query.where)):
        cond = this_query.where[c]
        if cond.equal:
            left = type(cond.left_operand) is tuple
            right = type(cond.right_operand) is tuple

            # if one of the operands is a tuple and the other is not
            if left and not right:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.left_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.left_operand[1]:
                    try:
                        this_query.from_tables[cond.left_operand[0]] = (access_index(TABLES[table_name], cond.right_operand,
                                                                                     TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc)
                        obtained.append(cond.left_operand[0])
                        pop_list.append(c)
                    except ValueError:      # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.left_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc)
                        obtained.append(cond.left_operand[0])
            elif right and not left:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.right_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.right_operand[1]:
                    try:
                        this_query.from_tables[cond.right_operand[0]] = (access_index(TABLES[table_name], cond.left_operand,
                                                                                      TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc)
                        obtained.append(cond.right_operand[0])
                        pop_list.append(c)
                    except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.right_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc)
                        obtained.append(cond.right_operand[0])

    # remove those where clause selections that have already been performed using an index access path
    count = 0
    for c in pop_list:  # c will be sorted from the loop above this one
        this_query.where.pop(c-count)   # need to subtract count to make up for indices removed in front of it
        count += 1


    # access remaining tables and load into memory
    normal_access = list(set(this_query.from_tables) - set(obtained))   # set subtraction to remove those that have already been loaded
    for t in normal_access:
        if t not in this_query.alias:   # if this entry is not an alias...
            this_query.from_tables[t] = (access(TABLES[t]), TABLES[t].storage.attr_loc)     # store relation and dictionary of attribute locations


    # perform more restrictive selections first --> place any selections that involve a primary key to execute first
    move_list = []
    for c in range(len(this_query.where)):
        if this_query.where[c].left_operand[1] == this_query.from_tables[this_query.where[c].left_operand[0]].primary_key:
            move_list.append(c)
        elif this_query.where[c].right_operand[1] == this_query.from_tables[this_query.where[c].right_operand[0]].primary_key:
            move_list.append(c)

    # shuffle selections to make optimal ordering
    for m in move_list:
        this_query.where.insert(0, this_query.where.pop(m))

    # perform selections
    for cond in this_query.where:
        # determine where to store result of selection
        if type(cond.left_operand) is tuple:
            if type(cond.right_operand) is tuple:   # essentially an equi-join
                # left side
                left_table_name = cond.left_operand[0]
                left_attr = cond.left_operand[1]

                if left_table_name in this_query.alias:     # aliases should still map to strings
                    left_table_name = this_query.from_tables[left_table_name]  # get non-alias name in order to get access to stored relation
                if type(this_query.from_tables[left_table_name]) is str:
                    left_table_name = this_query.from_tables[left_table_name]   # left table was part of a join. Use result of a join


                # determine index of desired attribute
                left_attr_index = this_query.from_tables[left_table_name][1][left_attr]

                # right side
                right_table_name = cond.right_operand[0]
                right_attr = cond.right_operand[1]

                if right_table_name in this_query.alias:
                    right_table_name = this_query.from_tables[right_table_name]  # get non-alias name in order to get access to stored relation
                if type(this_query.from_tables[right_table_name]) is str:
                    right_table_name = this_query.from_tables[right_table_name]   # left table was part of a join. Use result of a join

                # determine index of desired attribute
                right_attr_index = this_query.from_tables[right_table_name][1][right_attr]

                # call selection function (storing equijoin result here)  after this point any references to tables that compose the join should direct to the corresponding attribute in this table
                join_name = "JOIN_" + left_table_name + "_" + right_table_name



                # this will always be an equijoin, so just update the attribute dictionary here (append attributes of right table to those of the left, removing the right index attribute (shared with the left))
                # updating the dictionary of indices for the result of this join
                join_attr_dict = this_query.from_tables[left_table_name][1]
                attr_offset = TABLES[left_table_name].num_attributes + 1    # + 1 since indexing starts at 0
                for k in this_query.from_tables[right_table_name][1]:
                    attr_ind = this_query.from_tables[right_table_name][1][k] + attr_offset
                    if this_query.from_tables[right_table_name][1][k] > right_attr_index:   # account for the removal of this duplicate attribute
                        attr_ind -= 1
                    join_attr_dict[k] = attr_ind

                # make joined tabled reference the joined result.  Store joined relation and attribute dictionary
                this_query.from_tables[left_table_name] = join_name      # now when these are referenced, redirect to join result (this redirects are the only non-alias strings left in from_tables)
                this_query.from_tables[right_table_name] = join_name
                joined_relation = selection(this_query.from_tables[left_table_name][0], this_query.from_tables[right_table_name][0], cond, left_attr_index, right_attr_index)
                this_query.from_tables[join_name] = (joined_relation, join_attr_dict)

            else:
                # determine table
                table = cond.left_operand[0]
                attr = cond.left_operand[1]

                if table in this_query.alias:
                    table = this_query.from_tables[table]   # get non-alias name in order to get access to stored relation

                # determine index of desired attribute
                attr_index = TABLES[table].storage.attr_loc[attr]

                # call selection function
                dic = this_query.from_tables[table][1]
                this_query.from_tables[table] = (selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic)
        elif type(cond.right_operand) is tuple:
            # determine table
            table = cond.right_operand[0]
            attr = cond.right_operand[1]

            if table in this_query.alias:
                table = this_query.from_tables[table]  # get non-alias name in order to get access to stored relation

            # determine index of desired attribute
            attr_index = TABLES[table].storage.attr_loc[attr]

            # call selection function
            dic = this_query.from_tables[table][1]
            this_query.from_tables[table] = (selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic)
        else:
            error(" no valid comparison does not use at least one table attribute.")






    # perform projections   (be careful to check from_table names to see if they redirect to a JOIN result)
    projection_tables = {}      # map tables to column indices to save.  Dictionary of lists
    for proj in this_query.select_attr:         # aggregate projections to avoid issues
        # determine table name
        if this_query.num_tables == 1:
            table = this_query.from_tables.keys[0]
        else:
            table = proj[0]     # stored table name

            # check if table has been included in a join
            if type(this_query.from_tables[table]) is str:  # if a string is stored and not a list of lists, then table was used in a join previously
                table = this_query.from_tables[table]


        # determine attribute index in table
        attr_index = TABLES[table].storage.attr_loc[proj[1]]


        # store attribute in table-specific lists of list of projections and dictionary of attribute indices
        if table not in projection_tables:
            projection_tables[table] = [list(), dict()]
        projection_tables[table][0].append(attr_index)
        projection_tables[table][1][attr_index] = proj[1]   # reverse mapping.  Index --> attribute name



    # perform projections
    for table in projection_tables:
        indices = list(sorted(projection_tables[table][0]))
        # determine new dict for this relation, resulting from the projection
        project_attr_dict = {}
        counter = 0     # this works because indices are sorted and will be projected in this order too
        for i in indices:
            project_attr_dict[projection_tables[table][1][i]] = counter     # map attr name --> new index in relation
            counter += 1

        # pass in relation and list of indices to return
        this_query.from_tables[table] = (projection(this_query.from_tables[table][0], indices), projection_attr_dict)



    # perform joins
    for join in this_query.joins:
        # left side
        left_table = join[1]
        left_attr = join(3)

        if left_table in this_query.alias:
            left_table = this_query.from_tables[left_table]  # get non-alias name in order to get access to stored relation
        if type(this_query.from_tables[left_table]) is str:
            left_table = this_query.from_tables[left_table]  # left table was part of a join. Use result of a join

        # determine index of desired attribute
        left_attr_index = this_query.from_tables[left_table][1][left_attr]

        # right side
        right_table = join(2)
        right_attr = join(4)

        if right_table in this_query.alias:
            right_table = this_query.from_tables[right_table]  # get non-alias name in order to get access to stored relation
        if type(this_query.from_tables[right_table]) is str:
            right_table = this_query.from_tables[right_table]  # left table was part of a join. Use result of a join

        # determine index of desired attribute
        right_attr_index = this_query.from_tables[right_table][1][right_attr]

        # determine attribute index dictionary for the result of the join (currently if/else all contain same code.  May change if decide to handle outer joins differently)
        ty = join[0]
        natural_list = []
        join_attr_dict = {}

        if ty == "equi":
            # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
            join_attr_dict = this_query.from_tables[left_table][1]
            attr_offset = TABLES[left_table].num_attributes + 1  # + 1 since indexing starts at 0
            for k in this_query.from_tables[right_table][1]:
                attr_ind = this_query.from_tables[right_table][1][k] + attr_offset
                if this_query.from_tables[right_table][1][k] > right_attr_index:  # account for the removal of this duplicate attribute
                    attr_ind -= 1
                join_attr_dict[k] = attr_ind
        elif ty == "natural":
            # determine attributes shared between tables to be natural joined
            common_list = []
            for a1 in TABLES[left_table].attribute_names:
                for a2 in TABLES[right_table].attribute_names:
                    if a1 == a2:    # common attribute found
                        helper = (this_query.from_tables[left_table][a1], this_query.from_tables[right_table][a2])
                        common_list.append(helper[1])   # record common attribute indices in the right table
                        natural_list.append(helper)

            # concatenate right attributes to left ones.  Drop common attributes from the right relation, as done by join function
            join_attr_dict = this_query.from_tables[left_table][1]
            attr_offset = TABLES[left_table].num_attributes + 1     # +1 since indexing starts at 0
            counter = 1     # indexing for right table attributes start at 0.  Must +1 to the counter to avoid giving last attribute of left table and first of the right the same index
            for k in this_query.from_tables[right_table][1]:
                if this_query.from_tables[right_table][1][k] not in common_list:
                    attr_ind = counter + attr_offset
                    join_attr_dict[k] = attr_ind        # map string name to index location

        elif ty == "left":
            # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
            join_attr_dict = this_query.from_tables[left_table][1]
            attr_offset = TABLES[left_table].num_attributes + 1  # + 1 since indexing starts at 0
            for k in this_query.from_tables[right_table][1]:
                attr_ind = this_query.from_tables[right_table][1][k] + attr_offset
                if this_query.from_tables[right_table][1][k] > right_attr_index:  # account for the removal of this duplicate attribute
                    attr_ind -= 1
                join_attr_dict[k] = attr_ind
        elif ty == "right":
            # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
            join_attr_dict = this_query.from_tables[left_table][1]
            attr_offset = TABLES[left_table].num_attributes + 1  # + 1 since indexing starts at 0
            for k in this_query.from_tables[right_table][1]:
                attr_ind = this_query.from_tables[right_table][1][k] + attr_offset
                if this_query.from_tables[right_table][1][
                    k] > right_attr_index:  # account for the removal of this duplicate attribute
                    attr_ind -= 1
                join_attr_dict[k] = attr_ind
        elif ty == "full":
            ty = "outer"    # make compatible with join function
            # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
            join_attr_dict = this_query.from_tables[left_table][1]
            attr_offset = TABLES[left_table].num_attributes + 1  # + 1 since indexing starts at 0
            for k in this_query.from_tables[right_table][1]:
                attr_ind = this_query.from_tables[right_table][1][k] + attr_offset
                if this_query.from_tables[right_table][1][
                    k] > right_attr_index:  # account for the removal of this duplicate attribute
                    attr_ind -= 1
                join_attr_dict[k] = attr_ind
        else:
            error(" parsing error. Unrecognized join type in query optimizer.")

        # join and store
        join_name = "JOIN_" + left_table + "_" + right_table
        this_query.from_tables[left_table] = join_name  # now when these are referenced, redirect to join result (this redirects are the only non-alias strings left in from_tables)
        this_query.from_tables[right_table] = join_name


        joined_relation = join(this_query.from_tables[left_table][0], this_query.from_tables[right_table][0], left_attr_index, right_attr_index, ty)
        this_query.from_tables[join_name] = (joined_relation, join_attr_dict)


    #
    #   TODO apply aggregate operators
    #










    # return resulting relation
    return relation
# END optimizer
