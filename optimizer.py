import parser


# optimizer.  Handles execution of atomic queries (NOT higher level queries with UNION/DIFFERENCE/INTERSECTION)

# to access the result of a join (stored in from_tables --> JOIN_leftTableName_rightTableName)  (user-inputted table names can never be capitalized)
# when accessing attribute from a table that was joined, redirect to the join.  from_tables maps original relation name to JOIN... name (these pre-join tables
#       are the only non-alias strings left in from_tables)

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






    # simple query optimizer    (no access path checks, perform selections sequentially, then perform projections where appropriate,
    #                           then joins sequentially)





    # load all relations needed for this query
    for t in this_query.from_tables:
        if t not in this_query.alias:   # if this entry is not an alias...
            this_query.from_tables[t] = access(TABLES[t])





    # perform selections
    for cond in this_query.where:
        # determine where to store result of selection
        if type(cond.left_operand) is tuple:
            if type(cond.right_operand) is tuple:   # essentially an equi-join
                # left side
                left_table = cond.left_operand[0]
                left_attr = cond.left_operand[1]

                if left_table in this_query.alias:
                    left_table = this_query.from_tables[left_table]  # get non-alias name in order to get access to stored relation

                # determine index of desired attribute
                left_attr_index = TABLES[left_table].storage.attr_loc[left_attr]

                # right side
                right_table = cond.right_operand[0]
                right_attr = cond.right_operand[1]

                if right_table in this_query.alias:
                    right_table = this_query.from_tables[right_table]  # get non-alias name in order to get access to stored relation

                # determine index of desired attribute
                right_attr_index = TABLES[right_table].storage.attr_loc[right_attr]

                # call selection function (storing equijoin result here)  after this point any references to tables that compose the join should direct to the corresponding attribute in this table
                join_name = "JOIN_" + left_table + "_" + right_table
                this_query.from_tables[join_name] = selection(this_query.from_tables[left_table], this_query.from_tables[right_table], cond, left_attr_index, right_attr_index)
                this_query.from_tables[left_table] = join_name      # now when these are referenced, redirect to join result (this redirects are the only non-alias strings left in from_tables)
                this_query.from_tables[right_table] = join_name
            else:
                # determine table
                table = cond.left_operand[0]
                attr = cond.left_operand[1]

                if table in this_query.alias:
                    table = this_query.from_tables[table]   # get non-alias name in order to get access to stored relation

                # determine index of desired attribute
                attr_index = TABLES[table].storage.attr_loc[attr]

                # call selection function
                this_query.from_tables[table] = selection(this_query.from_tables[table], None, cond, attr_index, None)
        elif type(cond.right_operand) is tuple:
            # determine table
            table = cond.right_operand[0]
            attr = cond.right_operand[1]

            if table in this_query.alias:
                table = this_query.from_tables[table]  # get non-alias name in order to get access to stored relation

            # determine index of desired attribute
            attr_index = TABLES[table].storage.attr_loc[attr]

            # call selection function
            this_query.from_tables[table] = selection(this_query.from_tables[table], None, cond, attr_index, None)
        else:
            error(" no valid comparison does not use at least one table attribute.")





    #
    #   TODO pick up working here
    #



    # perform projections   (be careful to check from_table names to see if they redirect to a JOIN result)


















    # determine join order (multiple joins at once, then join smallest tables first)




    # TODO based on relative sizes of relations, determine what type of join (sort vs. linear) --> need to update the join function (backend.py) to reflect these options
    # TODO check access paths
    # TODO perform selections before projections (might remove an attribute needed for a selection during a projection)
    # TODO perform most restrictive selections first

    # return resulting relation
    return relation
# END optimizer