#                                               #
#   COSC 280 Project 2      optimizer.py        #
#                                               #


# import statements
from definitions import *
from backend import access, selection, projection, join, union, difference, intersection, max_agg, min_agg, avg_agg, sum_agg, count_agg, access_index



# optimizer function
def optimizer(this_query):
    explain_string = ""

    #                               #
    #   Handle non-atomic queries   #
    #                               #
    if this_query.union or this_query.intersect or this_query.difference:
        # perform both subqueries
        left_list, left, left_ex = optimizer(this_query.left_query)
        right_list, right, right_ex = optimizer(this_query.right_query)
        explain_string += left_ex
        explain_string += right_ex

        # perform specified set operation
        result = []
        if this_query.union:
            result = union(left, right)
        elif this_query.intersect:
            result = intersection(left, right)
        else:   # this_query.difference
            result = difference(left, right)

        # determine naming of output attributes
        max_len = max(len(left_list), len(right_list))
        out_list = []

        # merge attribute names
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

        return out_list, result, explain_string
    # END non-atomic query handling



    #                                           #
    #   Load relations and perform selections   #
    #                                           #
    # perform early selections (only those that can be accessed with an index)
    if not where_access(this_query.where, this_query, explain_string):
        # tree of comparisons is not longer needed, all comparisons have been performed
        this_query.where = None

    # access remaining tables (those that could not be accessed with an index)
    for key in this_query.from_tables:
        if key not in this_query.alias:
            if type(this_query.from_tables[key]) == str:
                # only load self-referential table names
                if this_query.from_tables[key] == key:
                    this_query.from_tables[key] = (access(TABLES[key]), TABLES[key].storage.attr_loc.copy())

    # perform remaining selections
    if this_query.where is not None:    # if conditions left to evaluate
        where_evaluate(this_query, this_query.where, ret=False)
        explain_string += "\nAll selections performed"



    #                                                               #
    #   Perform early projections (those that won't impact a join)  #
    #                                                               #
    # determine tables involved in a join (do not perform projections for those tables early)
    outer_tables = []
    for j in this_query.joins:
        outer_tables.append(j[1])
        outer_tables.append(j[2])
    outer_tables = list(set(outer_tables))      # remove duplicates

    # perform projections (gather all projections for each table and then perform all projections at once)
    projection_tables = {}
    for proj in this_query.select_attr:
        # determine table name of attribute in select clause
        if this_query.num_tables == 1:
            table_keys = [*this_query.from_tables]
            table = table_keys[0]
            if table in this_query.alias:
                table = this_query.from_tables[table]

        else:
            table = proj[0]

            # check if table has been included in a join
            if type(this_query.from_tables[table]) is str:
                table = this_query.from_tables[table]

        # if *, dont perform projections
        if proj[1] == '*':
            continue

        # determine attribute index in table
        attr_index = this_query.from_tables[table][1][proj[1]]

        # store attribute in table-specific lists of tuples storing a list of projections and a dictionary of attribute indices
        if table not in projection_tables:
            projection_tables[table] = [list(), dict()]
        projection_tables[table][0].append(attr_index)
        projection_tables[table][1][attr_index] = proj[1]   # reverse mapping.  Index --> attribute name

    # find ON clause attributes that would be removed by projections and do not perform them (mark for later)
    attr_to_remove = []
    for join_instance in this_query.joins:
        if join_instance[0] != "natural" and join_instance[0] != "equi":  # only look at those joins with an ON clause
            # load join information
            left_table = join_instance[1]
            left_attr = join_instance[3]
            right_table = join_instance[2]
            right_attr = join_instance[4]

            # check for projections affecting the left table
            if left_table in projection_tables:
                left_attr_ind = this_query.from_tables[left_table][1][left_attr]
                if left_attr_ind not in projection_tables[left_table][1]:
                    projection_tables[left_table][0].append(left_attr_ind)
                    projection_tables[left_table][1][left_attr_ind] = left_attr
                    attr_to_remove.append((left_table, left_attr))

            # check for projections affecting the right table
            if right_table in projection_tables:
                right_attr_ind = this_query.from_tables[right_table][1][right_attr]
                if right_attr_ind not in projection_tables[right_table][1]:
                    projection_tables[right_table][0].append(right_attr_ind)
                    projection_tables[right_table][1][right_attr_ind] = right_attr
                    attr_to_remove.append((right_table, right_attr))

    # perform projections
    for table in projection_tables:
        if table not in outer_tables:   # if table is not involved in a join
            # projection set up
            projection_tables[table][0] = list(set(projection_tables[table][0]))
            indices = list(sorted(projection_tables[table][0]))

            # determine new dict for this relation, resulting from the projection
            project_attr_dict = {}
            counter = 0     # indices are sorted and will be projected in this sorted order
            for i in indices:
                project_attr_dict[projection_tables[table][1][i]] = counter     # map attr name --> new index in post-projection relation
                counter += 1

            # pass in relation and list of indices to return
            this_query.from_tables[table] = (projection(this_query.from_tables[table][0], indices), project_attr_dict)
            l = [*projection_tables[table][1].values()]
            li = [l[element] + ", " for element in range(len(l)) if element < (len(l)-1)]
            li.append(l[-1])
            explain_string = explain_string + "\nProjection: (" + "".join(li) + ") for table " + table + " performed before joins."



    #                   #
    #   Perform joins   #
    #                   #
    if len(this_query.joins) > 0:
        # order joins based on summed number of tuples.  Perform smallest first
        this_query.joins.sort(key= lambda x: (len(this_query.from_tables[x[1]][0]) + len(this_query.from_tables[x[2]][0])))
        explain_string = explain_string + "\nJoin order: " + "".join(["\tPair: " + str(x[1]) + ' '+ str(x[2]) for x in this_query.joins])

        # perform joins
        for join_tup in this_query.joins:
            # left table set up.  Resolve aliases
            left_table = join_tup[1]
            left_attr = join_tup[3]
            if left_table in this_query.alias:
                left_table = this_query.from_tables[left_table]
            if type(this_query.from_tables[left_table]) is str:
                left_table = this_query.from_tables[left_table]
            if left_attr is not None and len(left_attr) > 0:    # determine index of desired attribute
                left_attr_index = this_query.from_tables[left_table][1][left_attr]
            else:
                left_attr_index = None

            # right table set up.  Resolve aliases
            right_table = join_tup[2]
            right_attr = join_tup[4]
            if right_table in this_query.alias:
                right_table = this_query.from_tables[right_table]
            if type(this_query.from_tables[right_table]) is str:
                right_table = this_query.from_tables[right_table]
            if right_attr is not None and len(right_attr) > 0:  # determine index of desired attribute
                right_attr_index = this_query.from_tables[right_table][1][right_attr]
            else:
                right_attr_index = None

            # determine join type based on the relative sizes of the relations to join
            ty = join_tup[0]
            left_size = len(this_query.from_tables[left_table][0])
            right_size = len(this_query.from_tables[right_table][0])
            nested = False      # default to sort/merge join
            if left_size >= JOIN_MULT*right_size or right_size >= JOIN_MULT*left_size:      # if joins differ significantly in size, use nested join
                explain_string = explain_string + "\nJoin " + join_tup[1] + " " + join_tup[2] + ": nested join"
                nested = True

                # determine outer table --> if needed, switch names, attribute indices, and type of join (left <--> right)
                if right_size < left_size:      # if smaller table is not the outer table, then flip relation positions
                    explain_string += ". Table order flipped"
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
            else:
                explain_string = explain_string + "\nJoin " + join_tup[1] + " " + join_tup[2] + ": sort/merge join"

            # determine attribute index dictionary resulting from this join
            natural_list = []
            join_attr_dict = {}
            match_flag = True
            if ty == "equi":
                # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
                join_attr_dict = this_query.from_tables[left_table][1]
                attr_offset = len(join_attr_dict)
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
                attr_offset = len(join_attr_dict)
                counter = 0
                for k in this_query.from_tables[right_table][1]:
                    if this_query.from_tables[right_table][1][k] not in common_list:
                        attr_ind = counter + attr_offset
                        join_attr_dict[k] = attr_ind        # map string name to index location
                        counter += 1

            elif ty == "left" or ty == "right" or ty == "full":
                # need to determine if the attributes being joined on are the same (same name)
                if left_attr == right_attr:
                    # concatenate right attributes to left ones.  Drop shared attribute from right relation, as is done by join function
                    join_attr_dict = this_query.from_tables[left_table][1].copy()
                    attr_offset = len(join_attr_dict)
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
                    match_flag = False      # if on clause operands do not share a name, cannot remove common attribute from the table
                    join_attr_dict = this_query.from_tables[left_table][1].copy()

                    # add a table prefix to any common attributes between tables (disambiguate between table/attributes)
                    attr_list = [*join_attr_dict]
                    for i in attr_list:
                        if i in this_query.from_tables[right_table][1]:
                            ind = join_attr_dict[i]
                            join_attr_dict.pop(i)
                            join_attr_dict[left_table + "." + i] = ind

                            # update select clause to reflect this change in attribute name
                            for at in range(len(this_query.select_attr)):
                                a = this_query.select_attr[at]
                                if a[1] == i and a[0] == left_table:
                                    if len(a) == 3:
                                        a = (a[0], a[0] + "." + a[1], a[2])
                                    else:
                                        a = (a[0], a[0] + "." + a[1])
                                    this_query.select_attr[at] = a

                    attr_offset = len(join_attr_dict)
                    for k in this_query.from_tables[right_table][1]:
                        raw_ind = this_query.from_tables[right_table][1][k]
                        attr_ind = raw_ind + attr_offset
                        if k in this_query.from_tables[left_table][1]:
                            # update select clause to reflect this change in name
                            for at in range(len(this_query.select_attr)):
                                a = this_query.select_attr[at]
                                if a[1] == k and a[0] == right_table:
                                    if len(a) == 3:
                                        a = (a[0], a[0] + "." + a[1], a[2])
                                    else:
                                        a = (a[0], a[0] + "." + a[1])
                                    this_query.select_attr[at] = a

                            k = right_table + "." + k
                        join_attr_dict[k] = attr_ind    # keep this line down here so naming changes will be applied

                # make compatible with join function (parser/backend naming conventions differ)
                if ty == "full":
                    ty = "outer"
            else:
                error(" parsing error. Unrecognized join type in query optimizer.")

            # perform join and store result
            if ty == "natural" and len(natural_list) < 0:
                joined_relation = []    # no common attributes, so return an empty relation
            else:
                joined_relation = join(this_query.from_tables[left_table][0], this_query.from_tables[right_table][0], left_attr_index, right_attr_index, ty, nested, natural_list, match_flag)

            # direct names of tables used in join to the join result
            join_name = "JOIN_" + left_table + "_" + right_table
            this_query.from_tables[left_table] = join_name  # now when these are referenced, redirect to join result (this redirects are the only non-alias strings left in from_tables)
            this_query.from_tables[right_table] = join_name
            this_query.from_tables[join_name] = (joined_relation, join_attr_dict)

    #                                                                   #
    #   Finish projections that were delayed due to join involvement    #
    #                                                                   #
    # perform any projections that were delayed due to involvement of that table in an outer join
    joined_table_attrs = {}
    for table in projection_tables:
        if table in outer_tables:
            # resolve name aliasing
            joined_table = table
            while type(this_query.from_tables[joined_table]) is str:
                joined_table = this_query.from_tables[joined_table]

            if joined_table not in joined_table_attrs:
                joined_table_attrs[joined_table] = ([], {})     # list to store all indices to keep, dictionary to map indices to names

            # determine indices of attributes to remove in joined relation
            for a in this_query.select_attr:
                if a[0] == table:
                    ind = this_query.from_tables[joined_table][1][a[1]]
                    joined_table_attrs[joined_table][0].append(ind)
                    joined_table_attrs[joined_table][1][ind] = a[1]


    # perform projections. Update attribute dictionaries appropriately
    for j in [*joined_table_attrs]:     # loop through the keys of joined_table_attrs
        joined_table_attrs[j] = (list(set(joined_table_attrs[j][0])), joined_table_attrs[j][1])

        # determine new dict for this relation, resulting from the projection
        project_attr_dict = {}
        counter = 0
        for i in joined_table_attrs[j][0]:
            project_attr_dict[joined_table_attrs[j][1][i]] = counter  # map attr name --> new index in relation
            counter += 1

        # pass in relation and list of indices to return
        this_query.from_tables[j] = (projection(this_query.from_tables[j][0], joined_table_attrs[j][0]), project_attr_dict)
        l = [*project_attr_dict]
        li = [l[element] + ", " for element in range(len(l)) if element < (len(l) - 1)]
        li.append(l[-1])
        explain_string = explain_string + "\nProjections delayed because table involved in a join: (" + "".join(li) + ") for " + j + " performed."


    #                                                       #
    #   Apply aggregate operators found in SELECT clause    #
    #                                                       #
    # determine how aggregate operators are used in SELECT clause
    all_agg = True
    agg_count = 0
    for s in this_query.select_attr:
        if len(s) > 2:
            agg_count += 1
        else:
            all_agg = False

    if agg_count > 0:
        if all_agg:
            # if all attributes in SELECT clause use aggregate operators, only output a single tuple
            attr_list = []
            output_list = []
            for s in this_query.select_attr:
                # resolve naming aliases
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
                attr_list.append(s[2] + "(" + s[1] + ")")
                output_list.append(val)

            # if "now" function was specified, include it now
            if this_query.now:
                t = time.localtime()
                current_time = time.strftime("%H:%M:%S", t)
                attr_list.append("NOW")
                output_list.append(current_time)

            # output result
            return attr_list, [output_list], explain_string


        else:
            # if only some attributes have aggregate operators, apply result to all tuples
            for se in range(len(this_query.select_attr)):
                s = this_query.select_attr[se]
                if len(s) <= 2:     # if attribute does not use an agg. operator, continue
                    continue

                # resolve name aliasing
                table_name = s[0]
                if table_name in this_query.alias:
                    table_name = this_query.from_tables[table_name]
                if type(this_query.from_tables[table_name]) is str:
                    table_name = this_query.from_tables[table_name]

                # calculate aggregate value
                agg_type = s[2]
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

                # store value for later application to the table
                this_query.select_attr[se] = (s[0], s[1], s[2], val)


            # remove any raw attributes that are no longer needed
            remove_list = set()
            for s in this_query.select_attr:
                # determine if attribute uses an aggregate operator
                if len(s) <= 2:
                    continue

                # check if raw attribute is no longer needed, overwrite with aggregate value.  Else, append new agg. value to all tuples
                keep_raw = False
                for r in this_query.select_attr:        # looking for matches (need to keep raw if a match exists)
                    if s != r and r not in remove_list:
                        if len(r) == 2:      # only consider at attribute without agg. operators
                            if r == (s[0], s[1]):
                                keep_raw = True
                                break
                        elif r[:2] == (s[0], s[1]) and r[:2] in remove_list:
                            # raw attribute shared by these agg. operations was overwritten by the first one, so append the second to all tuples
                            keep_raw = True
                            break

                # resolve name aliasing
                table = s[0]
                while type(this_query.from_tables[table]) is str:
                    table = this_query.from_tables[table]

                # apply agg. operators appropriately
                if keep_raw:
                    # append agg. value to each tuple in the relation
                    for r in range(len(this_query.from_tables[table][0])):
                        tup = this_query.from_tables[table][0][r]
                        tup.append(s[3])
                        this_query.from_tables[table][0][r] = tup

                    # update the dictionary to reflect this change (take max index and add 1)
                    this_query.from_tables[table][1][s[2] + "(" + s[1] + ")"] = max(list(this_query.from_tables[table][1].values())) + 1

                else:
                    # overwrite raw attribute
                    attr = this_query.from_tables[table][1][s[1]]
                    for r in range(len(this_query.from_tables[table][0])):
                        tup = this_query.from_tables[table][0][r]
                        tup[attr] = s[3]
                        this_query.from_tables[table][0][r] = tup

                    # update dictionary (remove the raw value and replace with the aggregate one)
                    this_query.from_tables[table][1].pop(s[1])
                    this_query.from_tables[table][1][s[2] + "(" + s[1] + ")"] = attr

                    # add raw to remove_list (avoid trying to remove it twice.  Use s[0] because table may be a join name)
                    remove_list.add((s[0], s[1]))


    #                               #
    #   Create relation to output   #
    #                               #
    # for each table listed in the select clause, add to output relation (all have necessary projections applied)
    relation = []
    tables_to_include = [proj[0] for proj in this_query.select_attr]
    for tab in range(len(tables_to_include)):
        # resolve name aliasing
        table = tables_to_include[tab]
        if table in this_query.alias:
            table = tables_to_include[tab] = this_query.from_tables[table]
        while type(this_query.from_tables[table]) == str:
            table = tables_to_include[tab] = this_query.from_tables[table]
    tables_to_output = list(set(tables_to_include))     # remove duplicates

    # loop through tables to output.  Get contents of relations and attribute names
    attr_out_list = []
    for ta in range(len(tables_to_output)):
        local_attr_list = sorted(this_query.from_tables[tables_to_output[ta]][1], key=this_query.from_tables[tables_to_output[ta]][1].get)
        attr_out_list.extend(local_attr_list)
        table = this_query.from_tables[tables_to_output[ta]][0]
        for t in range(len(table)):
            if ta == 0:
                relation.append([])
            tup = table[t]          # get information from corresponding tuple of "ta" relation
            relation[t].extend(tup) # append information to tuple already in output relation

    # if now function was specified in query, append it to output relation tuples
    if this_query.now:
        t = time.localtime()
        current_time = time.strftime("%H:%M:%S", t)
        attr_out_list.append("NOW")
        for r in range(len(relation)):
            relation[r].append(current_time)

    return attr_out_list, relation, explain_string
# END optimizer












# where_evaluate
def where_evaluate(this_query, cond, ret=False):
    # results are applied to this_query when ret=False.  Recursive calls set ret=True to avoid cross-contamination of recursive calls
    # classify condition
    if type(cond.left_operand) is tuple:
        if type(cond.right_operand) is tuple:  # comparing 2 attributes --> an equi-join
            # left operand set up
            left_table_name = cond.left_operand[0]
            left_attr = cond.left_operand[1]
            if left_table_name in this_query.alias:  # resolve name aliasing
                left_table_name = this_query.from_tables[left_table_name]
            if type(this_query.from_tables[left_table_name]) is str:
                left_table_name = this_query.from_tables[left_table_name]
            left_attr_index = this_query.from_tables[left_table_name][1][left_attr]     # get index of joining attribute

            # right operand set up
            right_table_name = cond.right_operand[0]
            right_attr = cond.right_operand[1]
            if right_table_name in this_query.alias:
                right_table_name = this_query.from_tables[right_table_name]
            if type(this_query.from_tables[right_table_name]) is str:
                right_table_name = this_query.from_tables[right_table_name]
            right_attr_index = this_query.from_tables[right_table_name][1][right_attr]  # get index of joining attribute

            # updating the dictionary of indices for the result of this join
            join_attr_dict = this_query.from_tables[left_table_name][1].copy()
            attr_offset = len(this_query.from_tables[left_table_name][1])
            for k in this_query.from_tables[right_table_name][1]:
                if k not in join_attr_dict:
                    join_attr_dict[k] = attr_offset
                    attr_offset += 1

            # selection function calls join with an equi-join specified
            joined_relation = selection(this_query.from_tables[left_table_name][0], this_query.from_tables[right_table_name][0], cond, left_attr_index, right_attr_index)

            # make tables used in join reference its result
            join_name = "JOIN_" + left_table_name + "_" + right_table_name
            this_query.from_tables[left_table_name] = join_name
            this_query.from_tables[right_table_name] = join_name
            this_query.from_tables[join_name] = (joined_relation, join_attr_dict)

            if ret:
                return joined_relation, join_attr_dict, [left_table_name, right_table_name]

        else:
            # only left operand is a table/attribute pair
            table = cond.left_operand[0]
            attr = cond.left_operand[1]

            # resolve name aliasing
            if table in this_query.alias:
                table = this_query.from_tables[table]
            if type(this_query.from_tables[table]) is str:
                table = this_query.from_tables[table]

            # determine index of desired attribute, get attribute dictionary, call selection function
            attr_index = this_query.from_tables[table][1][attr]
            dic = this_query.from_tables[table][1]
            if ret:
                return selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic, [table]
            else:
                this_query.from_tables[table] = (selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic)

    elif type(cond.right_operand) is tuple:
        # only right operand is a table/attribute pair
        table = cond.right_operand[0]
        attr = cond.right_operand[1]

        # resolve name aliasing
        if table in this_query.alias:
            table = this_query.from_tables[table]  # get non-alias name in order to get access to stored relation
        if type(this_query.from_tables[table]) is str:
            table = this_query.from_tables[table]

        # determine index of desired attribute
        attr_index = this_query.from_tables[table][1][attr]
        dic = this_query.from_tables[table][1]
        if ret:
            return selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic, [table]
        else:
            this_query.from_tables[table] = (selection(this_query.from_tables[table][0], None, cond, attr_index, None), dic)

    elif cond.and_ or cond.or_:
        # compound query. Break into operands, evaluate, and combine appropriately
        result_relation = []
        attr_dict = {}
        if cond.and_:
            left, left_dict, affected_left = where_evaluate(this_query, cond.left_operand, ret=True)
            right, right_dict, affected_right = where_evaluate(this_query, cond.right_operand, ret=True)
            if left_dict == right_dict:
                result_relation = intersection(left, right)
                attr_dict = left_dict
            else:
                # operands do not both operate on the same table.  Store those that meet the respective conditions
                if ret:     # ret means at an internal node in tree of conditions.  This solution may produce incorrect solutions
                    error("dictionaries do not match.  Cannot perform AND operation in WHERE clause.")
                else:
                    # apply to left
                    for tab in affected_left:
                        this_query.from_tables[tab] = (left, left_dict)

                    # apply to right
                    for tab in affected_right:
                        this_query.from_tables[tab] = (right, right_dict)
                    return [], {}, []      # allowing function to terminate normally will overwrite the changes made here

        else:   # cond._or
            left, left_dict, affected_left = where_evaluate(this_query, cond.left_operand, ret=True)
            right, right_dict, affected_right = where_evaluate(this_query, cond.right_operand, ret=True)
            if left_dict == right_dict:
                result_relation = union(left, right)
                attr_dict = left_dict
            else:
                # operands do not both operate on the same table.  Store those that meet the respective conditions
                if ret:  # ret means at an internal node in tree of conditions.  This solution may produce incorrect solutions
                    error("dictionaries do not match.  Cannot perform union operation.")
                else:
                    # apply to left
                    for tab in affected_left:
                        this_query.from_tables[tab] = (left, left_dict)

                    # apply to right
                    for tab in affected_right:
                        this_query.from_tables[tab] = (right, right_dict)

                    return [], {}, []     # allowing function to terminate normally will overwrite the changes made here


        # apply results to affected tables.  Combine lists and remove duplicates
        total_affected = list(set(affected_left + affected_right))
        if ret:
            return result_relation, left_dict, total_affected
        else:
            for table in total_affected:
                # resolve name aliasing
                if this_query.from_tables[table] is str:
                    table = this_query.from_tables[table]
                this_query.from_tables[table] = (result_relation, attr_dict)
    else:
        error(" no valid comparison does not use at least one table attribute.")
# END where_evaluate










# where_access      determine selections that can be performed using an index
def where_access(this_comparison, this_query, explain_string):
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

            # if one of the operands is a table/attribute pair and the other is not
            if left and not right:
                # check for an index on this attribute
                table_name = this_query.from_tables[cond.left_operand[0]]
                if TABLES[table_name].storage.index_attr == cond.left_operand[1]:
                    try:
                        this_query.from_tables[cond.left_operand[0]] = (access_index(TABLES[table_name], cond.right_operand, TABLES[table_name].storage.index_name),
                                                                        TABLES[table_name].storage.attr_loc)
                        explain_string += "\n" + table_name + " loaded using an index."
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
                        explain_string += "\n" + table_name + " loaded using an index."
                        remove_and = True
                        keep_right = False
                    except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                        this_query.from_tables[cond.right_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc)

            # determine how to prune this branch
            if remove_and:
                if not keep_right:
                    if not keep_left:
                        return False    # tell parent to remove this branch entirely, both selections involved have already been performed
                    else:
                        # copy contents of left_operand into this_comparison
                        this_comparison.copy(this_comparison.left_operand)
                elif not keep_left:
                    # copy contents of right_operand into this_comparison
                    this_comparison.copy(this_comparison.right_operand)
        else:
            if not where_access(this_comparison.left_operand, this_query, explain_string):
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
                        explain_string += "\n" + table_name + " loaded using an index."
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
                        explain_string += "\n" + table_name + " loaded using an index."
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
            if not where_access(this_comparison.right_operand, this_query, explain_string):
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
        evaluated = False
        left = type(this_comparison.left_operand) is tuple
        right = type(this_comparison.right_operand) is tuple

        # if one of the operands is a table/attribute pair and the other is not
        if left and not right:
            # check for an index on this attribute
            table_name = this_query.from_tables[this_comparison.left_operand[0]]
            if TABLES[table_name].storage.index_attr == this_comparison.left_operand[1]:
                try:
                    this_query.from_tables[this_comparison.left_operand[0]] = (access_index(TABLES[table_name], this_comparison.right_operand, TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc.copy())
                    evaluated = True
                except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                    this_query.from_tables[this_comparison.left_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc.copy())
        elif right and not left:
            # check for an index on this attribute
            table_name = this_query.from_tables[this_comparison.right_operand[0]]
            if TABLES[table_name].storage.index_attr == this_comparison.right_operand[1]:
                try:
                    this_query.from_tables[this_comparison.right_operand[0]] = (access_index(TABLES[table_name], this_comparison.left_operand, TABLES[table_name].storage.index_name), TABLES[table_name].storage.attr_loc.copy())
                    evaluated = True
                except ValueError:  # if index access unsuccessful (invalid key?), then resort to normal access path
                    this_query.from_tables[this_comparison.right_operand[0]] = (access(TABLES[table_name]), TABLES[table_name].storage.attr_loc.copy())
        # determine how to prune this branch
        if evaluated:       # if condition has been evaluated, remove it
            return False
        else:
            return True

    else:
        return True     # who knows whats in this condition, leave in tree for later evaluation
# END where_list
