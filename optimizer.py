

# optimizer.  Handles execution of atomic queries (NOT higher level queries with UNION/DIFFERENCE/INTERSECTION)

# to access the result of a join (stored in from_tables --> JOIN_leftTableName_rightTableName)  (user-inputted table names can never be capitalized)
# when accessing attribute from a table that was joined, redirect to the join.  from_tables maps original relation name to JOIN... name (these pre-join tables
#       are the only non-alias strings left in from_tables)



from definitions import *
from backend import access, selection, projection, join, union, difference, intersection, max_agg, min_agg, avg_agg, sum_agg, count_agg, access_index




def optimizer(this_query):
    # check that queries are atomic
    if this_query.union or this_query.intersect or this_query.difference:
        left_list, left = optimizer(this_query.left_query)
        right_list, right = optimizer(this_query.right_query)

        # perform proper
        result = []
        if this_query.union:
            result = union(left, right)
        elif this_query.intersect:
            result = intersection(left, right)
        else:   # this_query.difference
            result = difference(left, right)


        # determine naming
        max_len = max(len(left_list), len(right_list))
        out_list = []
        for i in range(max_len):
            # determine if attributes overlap
            l = False
            r = False
            if i < len(left_list):
                l = True
            if i < len(right_list):
                r = True

            # logic for merging lists of attributes
            if l and r:
                if left_list[i] == right_list[i]:
                    name = left_list[i]
                else:
                    name = left_list[i] + "/" + right_list[i]
            elif l:
                name = left_list[i]
            elif r:
                name = right_list[i]
            out_list.append(name)

        return out_list, result
    # END non-atomic query handling


    # load all relations needed for this query
    # look through selects for any indexed attributes (only have to retrieve samples with that given value
    # perform early selections (only those that can be accessed with an index)
    if not where_access(this_query.where, this_query):
        this_query.where = None  # tree of comparisons is not longer needed

    # access remaining tables
    for key in this_query.from_tables:
        if key not in this_query.alias:
            if type(this_query.from_tables[key]) == str:
                if this_query.from_tables[key] == key:  # only complete table names are self-referential
                    this_query.from_tables[key] = (access(TABLES[key]), TABLES[key].storage.attr_loc.copy())   # load in relation here


    # perform remaining selections
    if this_query.where is not None:    # if conditions left to evaluate
        where_evaluate(this_query, this_query.where, ret=False)


    # determine tables involved in a join (do not perform projections for that table early)
    outer_tables = []
    for j in this_query.joins:
        outer_tables.append(j[1])
        outer_tables.append(j[2])
    outer_tables = list(set(outer_tables))      # remove duplicates


    # perform projections   (be careful to check from_table names to see if they redirect to a JOIN result)
    projection_tables = {}      # map tables to column indices to save.  Dictionary of lists
    star_tables = []
    for proj in this_query.select_attr:         # aggregate projections to avoid issues
        # determine table name
        if this_query.num_tables == 1:
            table_keys = [*this_query.from_tables]
            table = table_keys[0]
            if table in this_query.alias:
                table = this_query.from_tables[table]

        else:
            table = proj[0]     # stored table name

            # check if table has been included in a join
            if type(this_query.from_tables[table]) is str:  # if a string is stored and not a list of lists, then table was used in a join previously
                table = this_query.from_tables[table]

        # perform * later in optimizer (make compatible with joins)
        if proj[1] == '*':
            star_tables.append(proj[0])
            continue

        # determine attribute index in table
        attr_index = this_query.from_tables[table][1][proj[1]]

        # store attribute in table-specific lists of list of projections and dictionary of attribute indices
        if table not in projection_tables:
            projection_tables[table] = [list(), dict()]
        projection_tables[table][0].append(attr_index)
        projection_tables[table][1][attr_index] = proj[1]   # reverse mapping.  Index --> attribute name


    # find ON clause attributes that would be removed by projections and include them (mark for later removal)
    attr_to_remove = []
    for join_instance in this_query.joins:
        if join_instance[0] != "natural" and join_instance[0] != "equi":  # only look at those joins with an ON clause
            left_table = join_instance[1]
            left_attr = join_instance[3]
            right_table = join_instance[2]
            right_attr = join_instance[4]

            if left_table in projection_tables:
                left_attr_ind = this_query.from_tables[left_table][1][left_attr]
                if left_attr_ind not in projection_tables[left_table][1]:
                    projection_tables[left_table][0].append(left_attr_ind)
                    projection_tables[left_table][1][left_attr_ind] = left_attr
                    attr_to_remove.append((left_table, left_attr))

            if right_table in projection_tables:
                right_attr_ind = this_query.from_tables[right_table][1][right_attr]
                if right_attr_ind not in projection_tables[right_table][1]:
                    projection_tables[right_table][0].append(right_attr_ind)
                    projection_tables[right_table][1][right_attr_ind] = right_attr
                    attr_to_remove.append((right_table, right_attr))

    # perform projections
    # only perform projections early if there is not an outer join included
    for table in projection_tables:
        if table not in outer_tables:       # performing an early projection for an outer join is useless.  Will have to perform join again later
            indices = list(sorted(projection_tables[table][0]))
            # determine new dict for this relation, resulting from the projection
            project_attr_dict = {}
            counter = 0     # this works because indices are sorted and will be projected in this order too
            for i in indices:
                project_attr_dict[projection_tables[table][1][i]] = counter     # map attr name --> new index in relation
                counter += 1

            # pass in relation and list of indices to return
            this_query.from_tables[table] = (projection(this_query.from_tables[table][0], indices), project_attr_dict)






    # order joins based on summed number of tuples
    if len(this_query.joins) > 0:
        this_query.joins.sort(key= lambda x: (len(this_query.from_tables[x[1]][0]) + len(this_query.from_tables[x[2]][0])))     # perform smallest join first

        # perform joins
        for join_tup in this_query.joins:
            # left side
            left_table = join_tup[1]
            left_attr = join_tup[3]
            if left_table in this_query.alias:
                left_table = this_query.from_tables[left_table]     # get non-alias name
            if type(this_query.from_tables[left_table]) is str:     # check if left table was part of a join without loading relation
                left_table = this_query.from_tables[left_table]
            if left_attr is not None and len(left_attr) > 0:
                left_attr_index = this_query.from_tables[left_table][1][left_attr]      # determine index of desired attribute
            else:
                left_attr_index = None

            # right side
            right_table = join_tup[2]
            right_attr = join_tup[4]
            if right_table in this_query.alias:
                right_table = this_query.from_tables[right_table]   # get non-alias name
            if type(this_query.from_tables[right_table]) is str:    # check if right table was part of a join without loading relation
                right_table = this_query.from_tables[right_table]
            if right_attr is not None and len(right_attr) > 0:
                right_attr_index = this_query.from_tables[right_table][1][right_attr]   # determine index of desired attribute
            else:
                right_attr_index = None

            # determine join type based on the relative sizes of the relations to join
            ty = join_tup[0]        # get type of join.  This may need to be altered due to outer/inner table swaps
            left_size = len(this_query.from_tables[left_table][0])
            right_size = len(this_query.from_tables[right_table][0])
            nested = False      # default to sort/merge
            if left_size >= JOIN_MULT*right_size or right_size >= JOIN_MULT*left_size:
                nested = True   # if relation sizes are significantly different, use nested loop join

                # determine outer table (the left table) --> switch names, attribute indices, and type of join (left <--> right)
                if right_size < left_size:      # if smaller table is not the outer table, then flip relation positions
                    temp_relation = left_table
                    left_table = right_table
                    right_table = temp_relation

                    temp_attr = left_attr_index
                    left_attr_index = right_attr_index
                    right_attr_index = temp_attr

                    # left/right outer joins are not commutative, so need to flip to keep query the same
                    if ty == "left":
                        ty = "right"
                    elif ty == "right":
                        ty = "left"

            # determine attribute index dictionary for the result of the join (currently if/else all contain same code.  May change if decide to handle outer joins differently)
            natural_list = []
            join_attr_dict = {}
            if ty == "equi":
                # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
                join_attr_dict = this_query.from_tables[left_table][1]
                attr_offset = len(join_attr_dict)  # + 1 since indexing starts at 0
                for k in this_query.from_tables[right_table][1]:
                    attr_ind = this_query.from_tables[right_table][1][k] + attr_offset
                    if this_query.from_tables[right_table][1][k] > right_attr_index:  # account for the removal of this duplicate attribute
                        attr_ind -= 1
                    join_attr_dict[k] = attr_ind
            elif ty == "natural":
                # determine attributes shared between tables to be natural joined
                common_list = []
                for a1 in list(this_query.from_tables[left_table][1].keys()):
                    for a2 in list(this_query.from_tables[right_table][1].keys()):
                        if a1 == a2:    # common attribute found
                            helper = (this_query.from_tables[left_table][1][a1], this_query.from_tables[right_table][1][a2])
                            common_list.append(helper[1])   # record common attribute indices in the right table
                            natural_list.append(helper)

                # concatenate right attributes to left ones.  Drop common attributes from the right relation, as done by join function
                join_attr_dict = this_query.from_tables[left_table][1]
                attr_offset = len(join_attr_dict)     # dont need +1 since indexing starts at 0
                counter = 0     # indexing for right table attributes start at 0.  Must +1 to the counter to avoid giving last attribute of left table and first of the right the same index
                for k in this_query.from_tables[right_table][1]:
                    if this_query.from_tables[right_table][1][k] not in common_list:
                        attr_ind = counter + attr_offset
                        join_attr_dict[k] = attr_ind        # map string name to index location
                        counter += 1

            elif ty == "left" or ty == "right" or ty == "full":
                # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
                join_attr_dict = this_query.from_tables[left_table][1].copy()
                attr_offset = len(join_attr_dict)  # doesnt need a +1 since indexing starts at 0
                for k in this_query.from_tables[right_table][1]:
                    raw_ind = this_query.from_tables[right_table][1][k]
                    attr_ind = raw_ind + attr_offset
                    if raw_ind == right_attr_index:
                        join_attr_dict[k] = left_attr_index
                        continue
                    elif raw_ind > right_attr_index:  # account for the removal of this duplicate attribute
                        attr_ind -= 1
                    join_attr_dict[k] = attr_ind
            else:
                error(" parsing error. Unrecognized join type in query optimizer.")

            if ty == "full":
                ty = "outer"    # make compatible with join function


            # join and store
            if ty == "natural" and len(natural_list) < 0:
                joined_relation = []    # no common attributes, so return an empty relation
            else:
                joined_relation = join(this_query.from_tables[left_table][0], this_query.from_tables[right_table][0], left_attr_index, right_attr_index, ty, nested, natural_list)
            join_name = "JOIN_" + left_table + "_" + right_table
            this_query.from_tables[left_table] = join_name  # now when these are referenced, redirect to join result (this redirects are the only non-alias strings left in from_tables)
            this_query.from_tables[right_table] = join_name


            this_query.from_tables[join_name] = (joined_relation, join_attr_dict)


        # remove those ON clause attributes that were maintained to allow joins to work (another round of projections)
        remove_table = {}
        for at in attr_to_remove:         # aggregate projections to avoid issues
            # check table name for aliasing or reference to a join result
            table = at[0]
            attribute = at[1]
            if table in this_query.alias:
                table = this_query.from_tables[table]
            if this_query.from_tables[table] is str:
                table = this_query.from_tables[table]   # table was used in a join.  Use the result from this join


            # store attributes to keep in table-specific lists of list of projections and dictionary of attribute indices
            if table not in remove_table:
                remove_table[table] = [list(), dict()]

                # determine those attributes to keep in the relation and populate an updated list of indices to keep and dictionary of attribute locations
                for a in this_query.from_tables[table][1]:
                    if a != attribute:
                        attr_index = this_query.from_tables[table][1][a]
                        remove_table[table][0].append(attr_index)
                        remove_table[table][1][attr_index] = a          # reverse mapping.  Index --> attribute name

            else:   # if the table entry has already been initialized in remove_table, just remove the attribute that we want to drop
                remove_table[table][0].pop(remove_table[table][0].index(attribute))
                remove_table[table][1].pop(attribute)

        # apply projections to table
        for table in remove_table:
            indices = list(sorted(remove_table[table][0]))
            # determine new dict for this relation, resulting from the projection
            new_attr_dict = {}
            counter = 0     # this works because indices are sorted and will be projected in this order too
            for i in indices:
                new_attr_dict[remove_table[table][1][i]] = counter     # map attr name --> new index in relation
                counter += 1

            # pass in relation and list of indices to return
            this_query.from_tables[table] = (projection(this_query.from_tables[table][0], indices), new_attr_dict)


    # perform any projections that were delayed due to involvement of that table in an outer join
    for table in projection_tables:
        if table in outer_tables:  # performing an early projection for an outer join is useless.  Will have to perform join again later
            projection_tables[table][1] = {}        # clear dictionary.  Mappings are no longer valid after the outer join
            # since table was involved in a join, need to reference the result of that join
            joined_table = table
            if type(this_query.from_tables[table]) is str:
                joined_table = this_query.from_tables[table]

            # cannot trust pre-join indices.  Must recalculate indices for those attributes to keep
            indices = []
            for a in this_query.select_attr:
                if a[0] == table:
                    ind = this_query.from_tables[joined_table][1][a[1]]
                    indices.append(ind)     # append the new attribute index to indices list
                    projection_tables[table][1][ind] = a[1]

            # make indices a sorted list
            indices.sort()


            # determine new dict for this relation, resulting from the projection
            project_attr_dict = {}
            counter = 0  # this works because indices are sorted and will be projected in this order too
            for i in indices:
                project_attr_dict[projection_tables[table][1][i]] = counter  # map attr name --> new index in relation
                counter += 1

            # pass in relation and list of indices to return
            this_query.from_tables[joined_table] = (projection(this_query.from_tables[joined_table][0], indices), project_attr_dict)





    # determine if only selections are those using aggregate operators
    all_agg = True
    agg_count = 0
    for s in this_query.select_attr:
        t = s[:2]
        if t not in this_query.min and t not in this_query.max and t not in this_query.sum and t not in this_query.avg and t not in this_query.count:
            all_agg = False
        else:
            agg_count += 1

    # if yes, only output one tuple
    if agg_count > 0:
        if all_agg:
            output_list = []
            for s in this_query.select_attr:
                # get proper table name
                table_name = s[0]
                if table_name in this_query.alias:
                    table_name = this_query.from_tables[table_name]
                if type(this_query.from_tables[table_name]) is str:
                    table_name = this_query.from_tables[table_name]

                # calculate aggregate value
                val = 0
                if s[2] == "max":
                    val = max_agg(this_query.from_tables[table_name][0],  this_query.from_tables[table_name][1][s[1]])
                elif s[2] == "min":
                    val = min_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif s[2] == "sum":
                    val = sum_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif s[2] == "avg":
                    val = avg_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif s[2] == "count":
                    val = count_agg(this_query.from_tables[table_name][0])

                # add to output list
                output_list.append(val)

            # output
            return output_list

        # if no, need to alter value to the min/max/avg/sum/count value for that attribute for every tuple in the relation
        else:
            for s in this_query.select_attr:
                # determine if this select attribute has an aggregate operator
                try:
                    agg_type = s[2]
                except IndexError:
                    continue

                # get proper table name
                table_name = s[0]
                if table_name in this_query.alias:
                    table_name = this_query.from_tables[table_name]
                if type(this_query.from_tables[table_name]) is str:
                    table_name = this_query.from_tables[table_name]

                # calculate aggregate value
                val = -1
                if agg_type == "max":
                    val = max_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif agg_type == "min":
                    val = min_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif agg_type == "sum":
                    val = sum_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif agg_type == "avg":
                    val = avg_agg(this_query.from_tables[table_name][0], this_query.from_tables[table_name][1][s[1]])
                elif agg_type == "count":
                    val = count_agg(this_query.from_tables[table_name][0])

                # apply this value to each tuple in the specified relation
                for r in range(len(this_query.from_tables[s[0]])):
                    tup = this_query.from_tables[s[0]][r]
                    tup[s[1]] = val
                    this_query.from_tables[s[0]][r] = tup


    # update star_tables list to accommodate joins/aliasing
    for st in range(len(star_tables)):
        s = star_tables[st]
        if s in this_query.alias:
            s = this_query.from_tables[s]
        while type(this_query.from_tables[s]) == str:
            s = this_query.from_tables[s]
        star_tables[st] = s

    # return resulting relation
    relation = []

    # for each table listed in the select clause, add to relation to be printed (all already have projections applied)
    tables_to_include = [proj[0] for proj in this_query.select_attr]
    for tab in range(len(tables_to_include)):
        table = tables_to_include[tab]
        if table in this_query.alias:
            table = tables_to_include[tab] = this_query.from_tables[table]
        while type(this_query.from_tables[table]) == str:           # get to the table that does not contain a naming link to another relation
            table = tables_to_include[tab] = this_query.from_tables[table]
    tables_to_output = list(set(tables_to_include))

    # loop through tables to output.  Get contents of relations and attribute names (in order0
    attr_out_list = []
    for ta in range(len(tables_to_output)):
        local_attr_list = [*this_query.from_tables[tables_to_output[ta]][1]]
        attr_out_list.extend(local_attr_list)



        table = this_query.from_tables[tables_to_output[ta]][0]
        for t in range(len(table)):
            if ta == 0:     # allows for direct indexing of the relation list below (and allows for all iterations of outer for loop to be the same)
                relation.append([])
            tup = table[t]          # get the information from the corresponding tuple of "ta" relation
            relation[t].extend(tup) # append information to that information already in the relation to be outputted


    return attr_out_list, relation
# END optimizer














# where_evaluate        as of now --> only applies results to tables when ret = False (should only occur for top-level function call)
def where_evaluate(this_query, cond, ret=False):
    if type(cond.left_operand) is tuple:
        if type(cond.right_operand) is tuple:  # essentially an equi-join
            # left side
            left_table_name = cond.left_operand[0]
            left_attr = cond.left_operand[1]

            if left_table_name in this_query.alias:  # aliases should still map to strings
                left_table_name = this_query.from_tables[left_table_name]  # get non-alias name in order to get access to stored relation
            if type(this_query.from_tables[left_table_name]) is str:
                left_table_name = this_query.from_tables[left_table_name]  # left table was part of a join. Use result of a join
            left_attr_index = this_query.from_tables[left_table_name][1][left_attr]     # determine index of desired attribute


            # right side
            right_table_name = cond.right_operand[0]
            right_attr = cond.right_operand[1]

            if right_table_name in this_query.alias:
                right_table_name = this_query.from_tables[right_table_name]  # get non-alias name in order to get access to stored relation
            if type(this_query.from_tables[right_table_name]) is str:
                right_table_name = this_query.from_tables[right_table_name]  # left table was part of a join. Use result of a join
            right_attr_index = this_query.from_tables[right_table_name][1][right_attr]  # determine index of desired attribute


            # call selection function (storing equijoin result here)  after this point any references to tables that compose the join should direct to the corresponding attribute in this table
            join_name = "JOIN_" + left_table_name + "_" + right_table_name

            # this will always be an equijoin, so just update the attribute dictionary here (append attributes of right table to those of the left, removing the right index attribute (shared with the left))
            # updating the dictionary of indices for the result of this join
            join_attr_dict = this_query.from_tables[left_table_name][1]
            attr_offset = len(this_query.from_tables[left_table_name][1]) + 1  # + 1 since indexing starts at 0
            for k in this_query.from_tables[right_table_name][1]:
                attr_ind = this_query.from_tables[right_table_name][1][k] + attr_offset
                if this_query.from_tables[right_table_name][1][k] > right_attr_index:  # account for the removal of this duplicate attribute
                    attr_ind -= 1
                join_attr_dict[k] = attr_ind

            joined_relation = selection(this_query.from_tables[left_table_name][0], this_query.from_tables[right_table_name][0],
                                        cond, left_attr_index, right_attr_index)

            # make joined tabled reference the joined result.  Store joined relation and attribute dictionary
            this_query.from_tables[left_table_name] = join_name  # now when these are referenced, redirect to join result (this redirects are the only non-alias strings left in from_tables)
            this_query.from_tables[right_table_name] = join_name
            this_query.from_tables[join_name] = (joined_relation, join_attr_dict)

            if ret:
                return joined_relation, join_attr_dict, [left_table_name, right_table_name]

        else:  # type(cond.left_operand) is tuple
            # determine table
            table = cond.left_operand[0]
            attr = cond.left_operand[1]

            if table in this_query.alias:
                table = this_query.from_tables[table]  # get non-alias name in order to get access to stored relation
            if type(this_query.from_tables[table]) is str:
                table = this_query.from_tables[table]

            # determine index of desired attribute
            attr_index = this_query.from_tables[table][1][attr]

            # call selection function
            dic = this_query.from_tables[table][1]

            if ret:
                return selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic, [table]
            else:
                this_query.from_tables[table] = (selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic)

    elif type(cond.right_operand) is tuple:
        # determine table
        table = cond.right_operand[0]
        attr = cond.right_operand[1]

        if table in this_query.alias:
            table = this_query.from_tables[table]  # get non-alias name in order to get access to stored relation

        # determine index of desired attribute
        attr_index = this_query.from_tables[table][1][attr]

        # call selection function
        dic = this_query.from_tables[table][1]
        if ret:
            return selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic, [table]
        else:
            this_query.from_tables[table] = (selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic)
    elif cond.and_ or cond.or_:  # compound query
        result_relation = []
        attr_dict = {}
        if cond.and_:
            left, left_dict, affected_left = where_evaluate(this_query, cond.left_operand, ret=True)
            right, right_dict, affected_right = where_evaluate(this_query, cond.right_operand, ret=True)
            if left_dict == right_dict:
                result_relation = intersection(left, right)
                attr_dict = left_dict
            else:
                # occurs when operands do not both operate on the same table.  As a result, store those that meet the respective conditions (may not be the right answer for more complex conditions)
                if ret:     # ret means we are at an internal node of the tree of conditions, if so this solution may produce incorrect solutions (ex. if level above this is an or)
                    error("dictionaries do not match.  Cannot perform intersection operation.")
                else:
                    # apply to left
                    for tab in affected_left:
                        this_query.from_tables[tab] = (left, left_dict)

                    # apply to right
                    for tab in affected_right:
                        this_query.from_tables[tab] = (right, right_dict)

                    return      # allowing function to terminate normally will overwrite the changes made here

        else:   # cond._or
            left, left_dict, affected_left = where_evaluate(this_query, cond.left_operand, ret=True)
            right, right_dict, affected_right = where_evaluate(this_query, cond.right_operand, ret=True)
            if left_dict == right_dict:
                result_relation = union(left, right)
                attr_dict = left_dict
            else:
                # occurs when operands do not both operate on the same table.  As a result, store those that meet the respective conditions (may not be the right answer for more complex conditions)
                if ret:  # ret means we are at an internal node of the tree of conditions, if so this solution may produce incorrect solutions (ex. if level above this is an or)
                    error("dictionaries do not match.  Cannot perform union operation.")
                else:
                    # apply to left
                    for tab in affected_left:
                        this_query.from_tables[tab] = (left, left_dict)

                        # apply to right
                    for tab in affected_right:
                        this_query.from_tables[tab] = (right, right_dict)

                    return      # allowing function to terminate normally will overwrite the changes made here


        # apply results to affected tables
        total_affected = list(set(affected_left + affected_right))   # combine lists and remove duplicates
        if ret:
            return result_relation, left_dict, total_affected
        else:
            for table in total_affected:
                if this_query.from_tables[table] is str:  # essentially, if table was used in a join.  No aliases should make it to here
                    table = this_query.from_tables[table]
                this_query.from_tables[table] = (result_relation, attr_dict)
    else:
        error(" no valid comparison does not use at least one table attribute.")
# END where_evaluate








# find where clause in an and.  Access early if possible and alter select tree to reflect this
def where_access(this_comparison, this_query):
    if this_comparison is None:
        return False

    if this_comparison.and_:
        left_prune = False
        right_prune = False

        # handle left subtree
        if this_comparison.left_operand.leaf and this_comparison.left_operand.equal:
            remove_and = False
            keep_left = True
            keep_right = True
            cond = this_comparison.left_operand
            left = type(cond.left_operand) is tuple
            right = type(cond.right_operand) is tuple

            # if one of the operands is a tuple and the other is not
            if left and not right:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.left_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.left_operand[1]:
                    try:
                        this_query.from_tables[cond.left_operand[0]] = (access_index(TABLES[table_name], cond.right_operand, TABLES[table_name].storage.index_name),
                                                                        TABLES[table_name].storage.attr_loc)
                        remove_and = True
                        keep_left = False
                    except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.left_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc)
            elif right and not left:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.right_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.right_operand[1]:
                    try:
                        this_query.from_tables[cond.right_operand[0]] = (access_index(TABLES[table_name], cond.left_operand,
                                     TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc)
                        remove_and = True
                        keep_right = False
                    except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.right_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc)

            # determine how to prune this branch
            if remove_and:
                if not keep_right:
                    if not keep_left:
                        return False    # tell parent to remove this branch entirely
                    else:
                        # copy contents of left_operand into this_comparison
                        this_comparison.copy(this_comparison.left_operand)
                elif not keep_left:
                    # copy contents of right_operand into this_comparison
                    this_comparison.copy(this_comparison.right_operand)
        else:
            if not where_access(this_comparison.left_operand, this_query):
                left_prune = True


        # handle right subtree
        if this_comparison.right_operand.leaf and this_comparison.right_operand.equal:
            remove_and = False
            keep_left = True
            keep_right = True
            cond = this_comparison.right_operand
            left = type(cond.left_operand) is tuple
            right = type(cond.right_operand) is tuple

            # if one of the operands is a tuple and the other is not
            if left and not right:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.left_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.left_operand[1]:
                    try:
                        this_query.from_tables[cond.left_operand[0]] = (access_index(TABLES[table_name], cond.right_operand, TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc.copy())
                        remove_and = True
                        keep_left = False
                    except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.left_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc.copy())
            elif right and not left:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.right_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.right_operand[1]:
                    try:
                        this_query.from_tables[cond.right_operand[0]] = (access_index(TABLES[table_name], cond.left_operand, TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc.copy())
                        remove_and = True
                        keep_right = False
                    except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.right_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc.copy())

            # determine how to prune this branch
            if remove_and:
                if not keep_right:
                    if not keep_left:
                        return False  # tell parent to remove this branch entirely
                    else:
                        # copy contents of left_operand into this_comparison
                        this_comparison.copy(this_comparison.left_operand)
                elif not keep_left:
                    # copy contents of right_operand into this_comparison
                    this_comparison.copy(this_comparison.right_operand)
        else:
            if not where_access(this_comparison.right_operand, this_query):
                right_prune = True


        # handle parent-level pruning
        if right_prune:
            if left_prune:
                return False    # both left and right branches should be pruned. Tell parent that this entire branch is no longer needed
            else:
                # right branch no longer needed. Substitute in the left branch
                this_comparison.copy(this_comparison.left_operand)
                return True
        elif left_prune:
            # left branch no longer needed. Substitute in the right branch
            this_comparison.copy(this_comparison.right_operand)
            return True
        else:
            return True     # no parent-level pruning needed

    elif this_comparison.equal and this_comparison.leaf:    # this only happens if is initially only one condition exists in the where clause
        remove_and = False
        keep_left = True
        keep_right = True
        cond = this_comparison.right_operand
        left = type(cond.left_operand) is tuple
        right = type(cond.right_operand) is tuple

        # if one of the operands is a tuple and the other is not
        if left and not right:
            # check for an index on this attribute
            table_name = this_query.from_tables[cond.left_operand[0]]
            if TABLES[table_name].storage.index_attr == cond.left_operand[1]:
                try:
                    this_query.from_tables[cond.left_operand[0]] = (access_index(TABLES[table_name], cond.right_operand, TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc.copy())
                    remove_and = True
                    keep_left = False
                except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                    this_query.from_tables[cond.left_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc.copy())
        elif right and not left:
            # check for an index on this attribute
            table_name = this_query.from_tables[cond.right_operand[0]]
            if TABLES[table_name].storage.index_attr == cond.right_operand[1]:
                try:
                    this_query.from_tables[cond.right_operand[0]] = (access_index(TABLES[table_name], cond.left_operand, TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc.copy())
                    remove_and = True
                    keep_right = False
                except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                    this_query.from_tables[cond.right_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc.copy())

        # determine how to prune this branch
        if remove_and:
            if not keep_right:
                if not keep_left:
                    return False  # tell parent to remove this branch entirely
                else:
                    # copy contents of left_operand into this_comparison
                    this_comparison.copy(this_comparison.left_operand)
            elif not keep_left:
                # copy contents of right_operand into this_comparison
                this_comparison.copy(this_comparison.right_op)
        return True     # this subtree should be maintained for later evaluation
    else:
        return True     # who knows whats in this condition, leave for later evaluation
# END where_list
