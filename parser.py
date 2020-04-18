#                                           #
#   COSC 280 DBMS Project       parser.py   #
#                                           #



from definitions import *



# parser_main
def parser_main(inp_line):
    inp_line = inp_line.lower()

    # classify as DDL or DML --> search for SELECT (a query), INSERT/UPDATE/DELETE (DML), CREATE/DROP (DDL)
    query = False
    dml = False
    ddl = False
    if "select " in inp_line:
        query = True
    elif "insert " in inp_line or "update " in inp_line or "delete " in inp_line:
        dml = True
    elif "create " in inp_line or "drop " in inp_line:
        ddl = True


    if query:
        # parse query and return result
        return parse_query(Query(), inp_line)

    elif dml:
        # create DML object
        dml_obj = DML()

        # classify what type of DML
        if "insert " in inp_line:
            dml_obj.insert = True
            # break into table name (and potentially columns) and list of values  --> use INTO and VALUES as delimiters
            l = re.split(" values ", inp_line)
            if len(l) != 2:
                error(" invalid insert syntax.")
            li = re.split(" into ", l[0])
            if len(li) != 2:
                error(" invalid insert syntax")
            table_name_attr = li[1].split("(")
            value_list = l[1].split("(")

            # validate table (and check all columns too)
            if len(table_name_attr) != 2:
                error(" invalid insert syntax")
            else:
                table_name = table_name_attr[0].rstrip().lstrip()
                dml_obj.table_name = table_name
                table_columns = table_name_attr[1].replace(")", "").split(",")

                for i in range(len(table_columns)):     # clean whitespace and closing )
                    table_columns[i] = table_columns[i].lstrip().rstrip()

                if table_name in TABLES:        # check that all attributes have been specified in the insert command
                    column_set = set(table_columns)

                    # if difference is non-empty, sets are not identical
                    if len(TABLES[table_name].attribute_names.difference(column_set)) != 0 or len(column_set.difference(TABLES[table_name].attribute_names)) != 0:
                        error(" table names match, but attribute sets do not.")
                else:
                    error(" invalid table name in insert.")

            # syntax checking/casting for specified values
            if len(value_list) != 2:
                error(" invalid syntax.  Missing (")
            else:
                val_list = value_list[1].split(")")
                if len(val_list) != 2:
                    error(" invalid syntax.  Missing )")
                else:
                    temp = val_list[0].split(",")
                    for i in temp:
                        helper = i.rstrip().lstrip()
                        if helper.replace(".", "").isdigit():
                            if len(helper.split(".")) != 1:
                                dml_obj.values.append(float(helper))
                            else:
                                dml_obj.values.append(int(helper))
                        else:
                            dml_obj.values.append(helper.replace("\"", ""))  # drop whitespace and insert into list

        elif "update " in inp_line:
            dml_obj.update = True

            # break into table name, set values, and where condition
            l = re.split(" set ", inp_line)
            if len(l) != 2:
                error(" syntax error update.")

            # determine table name
            up = re.split("update ", l[0])
            if len(up) != 2:
                error(" missing table name in update command.")
            table_name = up[1].lstrip().rstrip()
            dml_obj.table_name = table_name

            # validate table name
            if table_name not in TABLES:
                error(" invalid table name used in update.")

            # determine set values and parse where condition if it exists
            se = re.split(" where ", l[1])
            if len(se) == 2:    # where clause found
                set_values = se[0].split(",")
                dml_obj.where = parse_where_dml(dml_obj, se[1])
            elif len(se) == 1:  # no where clause included
                set_values = se[0].split(",")
            else:
                error(" invalid update syntax")

            # produces a list of Comparison objects (use assignment option) to represent set statements
            for s in set_values:
                this_comp = Comparison()
                this_comp.assignment = True
                operands = s.split("=")
                if len(operands) != 2:
                    error(" invalid number of operands in set clause of update.")

                # determine data types of operands and clear of whitespace
                left = operands[0].lstrip().rstrip()
                if left.replace(".", "").isdigit():
                    error(" attribute being assigned must be on the left side")
                else:
                    this_comp.left_operand = left
                    if left not in TABLES[table_name].attribute_names:
                        error(" cannot update a non-existent attribute.")
                right = operands[1].lstrip().rstrip()
                if right.replace(".", "").isdigit():
                    if len(right.split(".")) == 2:
                        this_comp.right_operand = float(right)
                    else:
                        this_comp.right_operand = int(right)
                else:
                    this_comp.right_operand = right

                # add this comparison object to dml_obj
                dml_obj.set.append(this_comp)
        else:       # delete
            dml_obj.delete = True

            # break into table name and WHERE condition (and parse where condition if it exists
            del_list = re.split(" where ", inp_line)
            if len(del_list) == 2:  # where clause found
                dml_obj.where = parse_where_dml(dml_obj, del_list[1])
            elif len(del_list) != 1:
                error(" too many WHERE clauses in delete")
            # else --> no where clause found.  Can parse table name the same whether or not where clause was found

            # get table name
            t_name = re.split("elete from", del_list[0])    # remove d from delete so can tell if produces 1 or two items
            if len(t_name) != 2:
                error(" delete command syntax error.")

            # validate table name
            table_name = t_name[1].lstrip().rstrip()
            if table_name in TABLES:
                dml_obj.table_name = table_name
            else:
                error(" invalid table name in delete")

        # return DML object
        return dml_obj
    elif ddl:
        # breakdown and classify as CREATE or DROP / TABLE or INDEX'
        ddl_obj = DDL()

        # treat each case appropriately
        if "create " in inp_line and " table " in inp_line:
            ddl_obj.create = True
            ddl_obj.table = True

            # break up input
            create_list = inp_line.split("(", maxsplit=1)
            if len(create_list) != 2:
                error(" invalid create table syntax.")

            # determine table name and verify no table exist with the same name
            t_name = re.split("create table ", create_list[0])
            if len(t_name) != 2:
                error(" invalid create table syntax.  No table name supplied.")
            table_name = t_name[1].lstrip().rstrip()
            if table_name in TABLES:
                error(" table name already in use.")
            ddl_obj.table_name = table_name

            # get list of attributes (tokenize by ','.  Record as tuples of (name, datatype))
            attr_list = create_list[1].split(",")
            attr_list[-1] = attr_list[-1][:len(attr_list[-1]) - len(")")]       # strip only the final ")" from input
            for attr in attr_list:  # for each attribute in list, split into name and data type
                if "primary key" in attr:
                    # take the value between the parenthesis and save as primary key
                    key_attr = attr.split("(")[1].split(")")[0].lstrip().rstrip()

                    # determine if primary key attribute exists
                    exists = False
                    for a in ddl_obj.attr:
                        if a[0] == key_attr:
                            ddl_obj.primary_key = key_attr
                            exists = True
                            break
                    if not exists:
                        error(" primary key does not match any specified attributes.")
                elif "foreign key" in attr:
                    # get key attribute
                    spl = attr.split(")", 1)    # only split on first parenthesis
                    key_attr = spl[0].split("(")[1]

                    # get table/attribute reference
                    table_attr_pair = re.split(" references", spl[1])[1]
                    h = table_attr_pair.split("(")
                    for_table = h[0].lstrip().rstrip()
                    for_attr = h[1].split(")")[0].lstrip().rstrip()

                    # validate table/attr reference
                    if for_table not in TABLES:
                        error(" cannot have foreign key reference a nonexistent table.")
                    if for_attr != TABLES[for_table].primary_key:
                        error(" cannot have foreign key reference a non-primary key attribute (existing table).")

                    # validate key attribute
                    found = False
                    for a in ddl_obj.attr:
                        if a[0] == key_attr:
                            found = True
                            break
                    if not found:
                        error(" foreign key attribute does not exist in this relation.")

                    # save result
                    ddl_obj.foreign_key = (key_attr, for_table, for_attr)
                elif attr.lstrip().rstrip() == "now":
                    ddl_obj.now = True
                else:
                    l = attr.split()
                    if len(l) != 2:
                        error(" missing data type")
                    ddl_obj.attr.append((l[0].lstrip().rstrip(), l[1].lstrip().rstrip()))   # append tuple (attr, dtype)
        elif "drop " in inp_line and " table " in inp_line:
            ddl_obj.create = False
            ddl_obj.table = True

            # get table name (and validate it)
            l = re.split(" table ", inp_line)
            table_name = l[1].lstrip().rstrip()

            # validate table name
            if table_name not in TABLES:
                error(" invalid table name.  Cannot delete table that does not exist")
            else:
                ddl_obj.table_name = table_name
        elif "create " in inp_line and " index " in inp_line:
            ddl_obj.create = True
            ddl_obj.table = False
            # break into index name/table information (using ON clause)
            c_ind_list = re.split(" on ", inp_line)
            if len(c_ind_list) != 2:
                error(" invalid create index syntax.")

            # get index name
            ind = re.split(" index ", c_ind_list[0])
            ind_name = ind[1].rstrip().lstrip()

            # validate index name (make sure does not exist)
            if ind_name in INDEX:
                error(" cannot create 2 indexes with the same name.")
            ddl_obj.index_name = ind_name

            # get table/attr name
            t_name = c_ind_list[1].split("(")
            if len(t_name) != 2:
                error(" invalid syntax.  Must specify what attribute to create index on.")
            table_name = t_name[0].rstrip().lstrip()    # clean whitespace
            attr = t_name[1].split(')')                 # drop closing parenthesis from attribute name
            attr_name = attr[0].rstrip().lstrip()       # clean whitespace

            # validate table name
            if table_name not in TABLES:
                error(" cannot create an index on a nonexistent table.")
            elif attr_name not in TABLES[table_name].attribute_names:
                error(" cannot create an index on an attribute that does not exist in the given table.")

            # else, set valid table and attribute names
            ddl_obj.table_name = table_name
            ddl_obj.attr.append(attr_name)
        else:       # drop index
            # validate index name
            ddl_obj.create = False
            ddl_obj.table = False

            # get table name (and validate it)
            l = re.split(" index ", inp_line)
            ind_name = l[1].lstrip().rstrip()

            # validate index name
            if ind_name not in INDEX:
                error(" cannot drop a non-existent index.")
            ddl_obj.index_name = ind_name
        return ddl_obj
    else:       # invalid input.  Throw error and take next line of input
        error(" unrecognized input type (not DML or DDL)")
# END parser_main








# parse_query
def parse_query(this_query, inp_line):
    # calls parser recursively, so only need to handle the "one" or "none" cases
    if "union" in inp_line:
        this_query.union = True
        l = re.split("union", inp_line)

        # call query parser on both left and right queries
        this_query.left_query = parse_query(Query(), l[0])
        this_query.right_query = parse_query(Query(), l[1])
    elif "intersect" in inp_line:
        this_query.intersect = True
        l = re.split("intersect", inp_line)

        # call query parser on both left and right queries
        this_query.left_query = parse_query(Query(), l[0])
        this_query.right_query = parse_query(Query(), l[1])
    elif "difference" in inp_line:
        this_query.difference = True
        l = re.split("difference", inp_line)

        # call query parser on both left and right queries
        this_query.left_query = parse_query(Query(), l[0])
        this_query.right_query = parse_query(Query(), l[1])
    else:
        # break query into SELECT, FROM, and WHERE clauses --> parse FROM first to define all aliases
        select_start = inp_line.find("select") + len("select")
        select_end = inp_line.find("from")
        from_end = inp_line.find("where")
        if from_end == -1:
            from_end = len(inp_line) + 1
        select_clause = inp_line[select_start: select_end]
        from_clause = inp_line[(select_end + len("from")): from_end]
        where_clause = inp_line[(from_end + len("where")):]

        ### parse FROM ###
        # tokenize on ',' to separate table names
        from_list = from_clause.split(',')
        for name in from_list:
            # tokenize on JOIN (classify as NATURAL JOIN, LEFT OUTER JOIN, RIGHT OUTER JOIN, or INNER JOIN) --> validate contents of ON clause for join too
            if " join " in name:
                # for each pair of partitions (table_info JOIN table_info --> (table_info1, table_info2))
                # split on "join" --> more than 2 partitions? (means >1 join)
                joins = re.split(" join ", name)
                this_query.num_tables += len(joins)
                if len(joins) <= 2:
                    # determine join type
                    join_type = ""
                    left_side = ""
                    left_on = ()
                    right_on = ()
                    on_clause = False
                    if " natural" in joins[0]:
                        join_type = "natural"
                        left_on = None
                        right_on = None
                        left_side = re.split(" natural", joins[0])[0]
                    elif " outer" in joins[0]:
                        if " left " in joins[0]:
                            join_type = "left"
                            left_side = re.split(" left outer", joins[0])[0]
                        elif " right " in joins[0]:
                            join_type = "right"
                            left_side = re.split(" right outer", joins[0])[0]
                        else:
                            join_type = "full"
                            left_side = re.split(" full outer", joins[0])[0]
                        on_clause = True
                    else:
                        error(" unrecognized join type.  Must be NATURAL, LEFT/RIGHT/FULL OUTER.")

                    # split off ON clause (if exists)
                    right_side = joins[1]
                    if on_clause:
                        # break clause up into operands --> validate at end of iteration to allow for the use of aliases
                        clause = re.split(" on ", joins[1])
                        if len(clause) != 2:
                            error("missing ON clause.")
                        right_side = clause[0].lstrip().rstrip()
                        o = clause[1].split(" = ")

                        # parse left side of ON clause
                        left_o = o[0].split(".")
                        if len(left_o) > 1:     # table name specified
                            left_on = (left_o[0].lstrip().rstrip(), left_o[1].lstrip().rstrip())
                        else:
                            left_on = ("", left_o[0])

                        # parse right side of ON clause
                        right_o = o[1].split(".")
                        if len(right_o) > 1:    # table name specified
                            right_on = (right_o[0].lstrip().rstrip(), right_o[1].lstrip().rstrip())
                        else:
                            right_on = ("", right_o[0])

                    # left table aliasing
                    left_name = ""
                    alias_list = re.split(" as ", left_side)
                    if len(alias_list) > 1:
                        # strip whitespace off alias and name
                        alias_list[0] = alias_list[0].rstrip().lstrip()
                        alias_list[1] = alias_list[1].rstrip().lstrip()
                        if alias_list[0] in TABLES:     # validate table existence (alias_list[0])
                            # table exists.  put both alias and table name into from_tables (put into from_tables)
                            this_query.from_tables[alias_list[1]] = alias_list[0]
                            this_query.from_tables[alias_list[0]] = alias_list[0]
                            this_query.alias.add(alias_list[1])
                            left_name = alias_list[0]
                        else:
                            error("nonexistent table used in FROM clause")
                    else:
                        # no alias found, just insert to query from_table
                        # strip whitespace from ends
                        left_side = left_side.rstrip().lstrip()
                        if left_side in TABLES:     # validate table name
                            this_query.from_tables[left_side] = left_side
                            left_name = left_side
                        else:
                            error("nonexistent table used in FROM clause")

                    # right table aliasing
                    alias_list = []     # empty list, just to be safe
                    right_name = ""
                    alias_list = re.split(" as ", right_side)
                    if len(alias_list) > 1:
                        # strip whitespace off alias and name
                        alias_list[0] = alias_list[0].rstrip().lstrip()
                        alias_list[1] = alias_list[1].rstrip().lstrip()
                        if alias_list[0] in TABLES:     # validate table existence (alias_list[0])
                            # table exists.  put both alias and table name into from_tables (put into from_tables)
                            this_query.from_tables[alias_list[1]] = alias_list[0]
                            this_query.from_tables[alias_list[0]] = alias_list[0]
                            this_query.alias.add(alias_list[1])
                            right_name = alias_list[0]
                        else:
                            error("nonexistent table used in FROM clause")
                    else:
                        # no alias found, just insert to query from_table
                        right_side = right_side.rstrip().lstrip()
                        if right_side in TABLES:        # validate table name
                            this_query.from_tables[right_side] = right_side
                            right_name = right_side
                        else:
                            error("nonexistent table used in FROM clause")

                    # validate ON clause contents, if they exist
                    if on_clause:
                        if left_on[0] == "":        # validate left table
                            if this_query.num_tables > 1:  # no table specified ( >1 table in query )
                                error(" ambiguous attribute name.  When >1 table used in query, need to specify table.")
                        else:
                            if left_on[0] in this_query.alias:
                                left_on = (this_query.from_tables[left_on[0]], left_on[1])
                            if left_on[0] in TABLES:
                                if left_on[1] not in TABLES[left_on[0]].attribute_names:
                                    error(" nonexistent attribute name in join ON clause.  Valid table name.")
                            else:
                                error(" invalid table name in join ON clause.")

                        if right_on[0] == "":       # validate right table
                            if this_query.num_tables > 1:  # no table specified ( >1 table in query )
                                error(" ambiguous attribute name.  When >1 table used in query, need to specify table.")
                        else:
                            if right_on[0] in this_query.alias:
                                right_on = (this_query.from_tables[right_on[0]], right_on[1])
                            if right_on[0] in TABLES:
                                if right_on[1] not in TABLES[right_on[0]].attribute_names:
                                    error(" nonexistent attribute name in join ON clause.  Valid table name.")
                            else:
                                error(" invalid table name in join ON clause.")

                    # add join to this_query
                    if right_on is None or left_on is None:
                        this_query.joins.append((join_type, left_name, right_name, None, None))
                    else:
                        this_query.joins.append((join_type, left_name, right_name, left_on[1], right_on[1]))
                else:
                    # know there are multiple joins --> know what type of joins to perform.  Split on join so specifier is in the preceding partition
                    join_types = []
                    for i in range(len(joins)):
                        if i > 0:
                            t = ""
                            if " left outer" in joins[i-1]:
                                t = "left"
                                joins[i-1] = re.split(" left outer", joins[i-1])[0]     # strip off join specifier
                            elif " right outer" in joins[i-1]:
                                t = "right"
                                joins[i-1] = re.split(" right outer", joins[i-1])[0]    # strip off join specifier
                            elif " natural" in joins[i-1]:
                                t = "natural"
                                joins[i-1] = re.split(" natural", joins[i-1])[0]        # strip off join specifier
                            elif " full outer" in joins[i-1]:
                                t = "full"
                                joins[i-1] = re.split(" full outer", joins[i-1])[0]     # strip off join specifier
                            else:
                                error(" unknown type of join in multiple join parsing.")
                            join_types.append(t)

                    # split into multiple joins --> create a sequential list of joins that will be parsed after first one handled
                    for i in range(len(joins)):
                        if i < 2:
                            if i == 0:
                                # split off ON clause (if exists)
                                left_on = ()
                                right_on = ()
                                on_clause = True
                                join_type = join_types[0]
                                if join_type == "natural":
                                    on_clause = False
                                    left_on = None
                                    right_on = None
                                left_side = joins[0]
                                right_side = joins[1]
                                if on_clause:
                                    # break clause up into operands --> validate at end of iteration to allow for the use of aliases
                                    clause = re.split(" on ", right_side)
                                    right_side = clause[0].lstrip().rstrip()
                                    o = clause[1].split(" = ")

                                    # parse left side of ON clause
                                    left_o = o[0].split(".")
                                    if len(left_o) > 1:     # table specified
                                        left_on = (left_o[0].lstrip().rstrip(), left_o[1].lstrip().rstrip())
                                    else:
                                        left_on = ("", left_o[0])

                                    # parse right side of ON clause
                                    right_o = o[1].split(".")
                                    if len(right_o) > 1:    # table specified
                                        right_on = (right_o[0].lstrip().rstrip(), right_o[1].lstrip().rstrip())
                                    else:
                                        right_on = ("", right_o[0])

                                # left table aliasing
                                left_name = ""
                                alias_list = re.split(" as ", left_side)
                                if len(alias_list) > 1:
                                    alias_list[0] = alias_list[0].rstrip().lstrip()
                                    alias_list[1] = alias_list[1].rstrip().lstrip()

                                    # validate table existence (alias_list[0])
                                    if alias_list[0] in TABLES:
                                        this_query.from_tables[alias_list[1]] = alias_list[0]
                                        this_query.from_tables[alias_list[0]] = alias_list[0]
                                        this_query.alias.add(alias_list[1])
                                        left_name = alias_list[0]
                                    else:
                                        error("nonexistent table used in FROM clause")

                                else:
                                    # no alias found
                                    left_side = left_side.rstrip().lstrip()
                                    if left_side in TABLES:
                                        this_query.from_tables[left_side] = left_side
                                        left_name = left_side
                                    else:
                                        error("nonexistent table used in FROM clause")

                                # right table aliasing
                                alias_list = []  # empty list, just to be safe
                                right_name = ""
                                alias_list = re.split(" as ", right_side)
                                if len(alias_list) > 1:
                                    alias_list[0] = alias_list[0].rstrip().lstrip()
                                    alias_list[1] = alias_list[1].rstrip().lstrip()

                                    # validate table existence (alias_list[0])
                                    if alias_list[0] in TABLES:
                                        this_query.from_tables[alias_list[1]] = alias_list[0]
                                        this_query.from_tables[alias_list[0]] = alias_list[0]
                                        this_query.alias.add(alias_list[1])
                                        right_name = alias_list[0]
                                    else:
                                        error("nonexistent table used in FROM clause")
                                else:
                                    # no alias found
                                    right_side = right_side.rstrip().lstrip()
                                    if right_side in TABLES:
                                        this_query.from_tables[right_side] = right_side
                                        right_name = right_side
                                    else:
                                        error("nonexistent table used in FROM clause")

                                # validate ON clause contents
                                if on_clause:
                                    # validate left operand
                                    if left_on[0] == "":
                                        if this_query.num_tables > 1:  # no table specified ( >1 table in query )
                                            error("ambiguous attribute name.  When >1 table used in query, need to specify table.")
                                    else:
                                        if left_on[0] in TABLES:
                                            if left_on[1] not in TABLES[left_on[0]].attribute_names:
                                                error("nonexistent attribute name in join ON clause.  Valid table name.")
                                        else:
                                            error("invalid table name in join ON clause.")

                                    # validate right operand
                                    if right_on[0] == "":
                                        if this_query.num_tables > 1:  # no table specified ( >1 table in query )
                                            error("ambiguous attribute name.  When >1 table used in query, need to specify table.")
                                    else:
                                        if right_on[0] in TABLES:
                                            if right_on[1] not in TABLES[right_on[0]].attribute_names:
                                                error("nonexistent attribute name in join ON clause.  Valid table name.")
                                        else:
                                            error("invalid table name in join ON clause.")

                                # add join to this_query
                                if left_on is None or right_on is None:
                                    this_query.joins.append((join_type, left_name, right_name, None, None))
                                else:
                                    this_query.joins.append((join_type, left_name, right_name, left_on[1], right_on[1]))
                            # else:  pass    # --> do nothing for the middle partition (i == 1)
                        else:
                            # all remaining joins are appended to the first one.  Only have to examine right side since left has been parsed already
                            # parse right side and take right table name or preceeding join as the left table for this join ("links" the joins together)
                            on_clause = True
                            join_type = join_types[i-1]
                            if join_type == "natural":
                                on_clause = False
                            right_side = joins[i]

                            # split off ON clause
                            left_on = ()
                            right_on = ()
                            if on_clause:
                                # break clause up into operands --> validate at end of iteration to allow for the use of aliases
                                clause = re.split(" on ", right_side)
                                right_side = clause[0].lstrip().rstrip()
                                o = clause[1].split(" = ")

                                # parse left side of ON clause
                                left_o = o[0].split(".")
                                if len(left_o) > 1:     # table specified
                                    left_on = (left_o[0].lstrip().rstrip(), left_o[1].lstrip().rstrip())
                                else:
                                    left_on = ("", left_o[0])

                                # parse right side of ON clause
                                right_o = o[1].split(".")
                                if len(right_o) > 1:    # table specified
                                    right_on = (right_o[0].lstrip().rstrip(), right_o[1].lstrip().rstrip())
                                else:
                                    right_on = ("", right_o[0])

                            # right alias checking
                            alias_list = []
                            right_name = ""
                            alias_list = re.split(" as ", right_side)
                            if len(alias_list) > 1:
                                alias_list[0] = alias_list[0].rstrip().lstrip()
                                alias_list[1] = alias_list[1].rstrip().lstrip()

                                # validate table existence (alias_list[0])
                                if alias_list[0] in TABLES:
                                    this_query.from_tables[alias_list[1]] = alias_list[0]
                                    this_query.from_tables[alias_list[0]] = alias_list[0]
                                    this_query.alias.add(alias_list[1])
                                    right_name = alias_list[0]
                                else:
                                    error("nonexistent table used in FROM clause")
                            else:
                                # no alias found
                                right_side = right_side.rstrip().lstrip()
                                if right_side in TABLES:    # validate table name
                                    this_query.from_tables[right_side] = right_side
                                    right_name = right_side
                                else:
                                    error("nonexistent table used in FROM clause")

                            # validate ON clause contents
                            if on_clause:
                                # validate left operand
                                if left_on[0] == "":
                                    if this_query.num_tables > 1:  # no table specified ( >1 table in query )
                                        error(" ambiguous attribute name.  When >1 table used in query, need to specify table.")
                                else:
                                    left_on = (this_query.from_tables[left_on[0]], left_on[1])
                                    if left_on[0] in TABLES:
                                        if left_on[1] not in TABLES[left_on[0]].attribute_names:
                                            error("nonexistent attribute name in join ON clause.  Valid table name.")
                                    else:
                                        error("invalid table name in join ON clause.")

                                # validate right operand
                                if right_on[0] == "":
                                    if this_query.num_tables > 1:  # no table specified ( >1 table in query )
                                        error("ambiguous attribute name.  When >1 table used in query, need to specify table.")
                                else:
                                    right_on = (this_query.from_tables[right_on[0]], right_on[1])
                                    if right_on[0] in TABLES:
                                        if right_on[1] not in TABLES[right_on[0]].attribute_names:
                                            error("nonexistent attribute name in join ON clause.  Valid table name.")
                                    else:
                                        error("invalid table name in join ON clause.")

                                # on clause operand that does not match the right table name should contain the name of the left table of the join
                                if left_on[0] == right_name:
                                    left_name = right_on[0]
                                elif right_on == right_name:
                                    left_name = left_on[0]
                                else:
                                    error(" on clause does not reference the right table of the join.")
                            else:
                                left_name = this_query.joins[-1][2]     # take the right table name from the preceding join and use that as the left name for this join

                            # add join to this_query
                            this_query.joins.append((join_type, left_name, right_name, left_on, right_on))

            else:       # no joins in this section of FROM clause
                # check each table name for "as" --> recognize aliases and add to from_tables
                this_query.num_tables += 1
                alias_list = re.split(" as ", name)
                if len(alias_list) > 1:
                    alias_list[0] = alias_list[0].rstrip().lstrip()
                    alias_list[1] = alias_list[1].rstrip().lstrip()

                    # validate table existence (alias_list[0])
                    if alias_list[0] in TABLES:
                        this_query.from_tables[alias_list[1]] = alias_list[0]
                        this_query.from_tables[alias_list[0]] = alias_list[0]
                        this_query.alias.add(alias_list[1])
                    else:
                        error("nonexistent table used in FROM clause")
                else:
                    # no alias found
                    name = name.rstrip().lstrip()
                    if name in TABLES:
                        this_query.from_tables[name] = name
                    else:
                        error("nonexistent table used in FROM clause")

        ### parse SELECT ###
        # tokenize contents of SELECT on ',' --> break up attributes
        select_list = select_clause.split(",")
        for attr in range(len(select_list)):
            agg_list = select_list[attr].split("(")  # opening parenthesis is beginning of aggregate operator
            if len(agg_list) > 1:
                # second condition to ensure that this is a call to NOW() and not a now attribute name
                if agg_list[0].rstrip().lstrip() == "now" and ")" in agg_list[1]:
                    this_query.now = True
                    continue

                # parse on parenthesis to get attribute name
                close_paren_list = agg_list[1].split(')')
                attr_name = close_paren_list[0].rstrip().lstrip()  # attr is between ( and ) and drop whitespace
                attr_list = attr_name.split(".")  # split to find if alias used
                if len(attr_list) > 1:
                    attr_tup = (this_query.from_tables[attr_list[0]], attr_list[1])
                elif this_query.num_tables > 1:  # no table specified ( >1 table in query )
                    error(" ambiguous attribute name.  When >1 table used in query, need to specify table.")
                else:
                    table_key = list(this_query.from_tables.keys())[0]  # only one table --> possibly 2 entries if alias used (either raw name or alias is fine)
                    attr_tup = (this_query.from_tables[table_key], attr_name)

                # validate that desired attr is in table
                if attr_tup[0] in TABLES:
                    if attr_tup[1] not in TABLES[attr_tup[0]].attribute_names:
                        error(" valid table name.  Invalid attribute in SELECT clause")
                else:
                    error(" invalid table used in SELECT clause")

                if len(close_paren_list) != 2:
                    error(" invalid aggregate operator syntax")
                else:
                    # identify any aggregate operators on select attributes
                    add_on = ""
                    if " min(" in select_list[attr]:
                        this_query.min.append(attr_tup)
                        add_on = "min"
                    elif " max(" in select_list[attr]:
                        this_query.max.append(attr_tup)
                        add_on = "max"
                    elif " avg(" in select_list[attr]:
                        this_query.avg.append(attr_tup)
                        add_on = "avg"
                    elif " count(" in select_list[attr]:
                        this_query.count.append(attr_tup)
                        add_on = "count"
                    elif " sum(" in select_list[attr]:
                        this_query.sum.append(attr_tup)
                        add_on = "sum"
                    else:
                        error(" invalid aggregate operator included in SELECT.")
                    attr_tup = (attr_tup[0], attr_tup[1], add_on)
                this_query.select_attr.append(attr_tup)
            else:
                attr_name = select_list[attr].lstrip().rstrip()  # no aggregate operator
                attr_list = attr_name.split(".")  # split to find if alias used
                if len(attr_list) > 1:
                    attr_tup = (this_query.from_tables[attr_list[0]], attr_list[1])
                elif this_query.num_tables > 1:  # no table specified ( >1 table in query )
                    error(" ambiguous attribute name.  When >1 table used in query, need to specify table. (if using '*', need to specify table)")
                else:
                    # only one table in from, dont need to specify table
                    table_key = list(this_query.from_tables.keys())[0]
                    attr_tup = (this_query.from_tables[table_key], attr_name)

                # validate that desired attr is in table
                if attr_tup[0] in TABLES:
                    if attr_tup[1] in TABLES[attr_tup[0]].attribute_names:
                        this_query.select_attr.append(attr_tup)
                    elif attr_tup[1] == "*":
                        # * specified.  Add all attributes from the specified table (handled by optimizer)
                        this_query.select_attr.append((attr_tup[0], '*'))
                    else:
                        error(" valid table name.  Invalid attribute in SELECT clause")
                else:
                    error(" invalid table used in SELECT clause")
            this_query.select_attr = list(set(this_query.select_attr))      # remove duplicate tuples

        ### parse WHERE ###
        if where_clause != "":
            this_query.where = parse_where(this_query, where_clause)
        else:
            this_query.where = None

    # return a Query object
    return this_query
# END parse_query








# parse_where
def parse_where(this_query, where_clause):
    # parse by AND/OR recursively --> once done with this, should have atomic comparisons to perform (at leaf level of comparison tree)
    this_comparison = Comparison()
    if " and " in where_clause or " or " in where_clause:
        close_list = where_clause.split(")")
        close_list = [c for c in close_list if c != ""]
        remove_list = []
        counter = 0
        for c in range(len(close_list)):
            num_open = close_list[c].count("(")
            if num_open > 1:
                counter += 1
                num_open -= 1
                for i in range(num_open):
                    remove_list.append(c + i + 1)
        if counter > 0:
            count = 0
            for r in remove_list:
                close_list.pop(r - count)
                count += 1

            # fix regular expression to avoid errors
            helper_list = close_list[1].split("(")
            if len(helper_list) > 1:
                for i in range(1, len(helper_list)):
                    if i != 0:
                        helper_list[i] = "\(" + helper_list[i]
                fixed_string = ''
                for i in helper_list:
                    fixed_string += i
                close_list[1] = fixed_string
            useful_list = re.split(close_list[1], where_clause)
            useful_list[1] = close_list[1] + useful_list[1]
            useful_list[1] = useful_list[1].replace("\\", "")
            open_list = useful_list[1].split("(", maxsplit=1)
            useful_list[1] = "(" + open_list[1]
            if " or " in open_list[0]:
                # clean outer parenthesis
                useful_list[0] = useful_list[0].split("(", maxsplit=1)[1]
                useful_list[1] = useful_list[1].split("(", maxsplit=1)[1]
                zero_ind = useful_list[0].rfind(")")
                useful_list[0] = useful_list[0][:zero_ind]
                one_ind = useful_list[1].rfind(")")
                useful_list[1] = useful_list[1][:one_ind]

                # recursive calls
                this_comparison.left_operand = parse_where(this_query, useful_list[0])
                this_comparison.right_operand = parse_where(this_query, useful_list[1])
                this_comparison.or_ = True
            elif " and " in open_list[0]:
                # clean outer parenthesis
                useful_list[0] = useful_list[0].split("(", maxsplit=1)[1]
                useful_list[1] = useful_list[1].split("(", maxsplit=1)[1]
                zero_ind = useful_list[0].rfind(")")
                useful_list[0] = useful_list[0][:zero_ind]
                one_ind = useful_list[1].rfind(")")
                useful_list[1] = useful_list[1][:one_ind]

                # recursive calls
                this_comparison.left_operand = parse_where(this_query, useful_list[0])
                this_comparison.right_operand = parse_where(this_query, useful_list[1])
                this_comparison.and_ = True
            else:
                error("invalid syntax in where clause")
        else:
            if " and " in where_clause:
                and_list = re.split(" and ", where_clause, maxsplit=1)
                this_comparison.left_operand = parse_where(this_query, and_list[0])
                this_comparison.right_operand = parse_where(this_query, and_list[1])
                this_comparison.and_ = True
            elif " or " in where_clause:
                or_list = re.split(" or ", where_clause, maxsplit=1)
                this_comparison.left_operand = parse_where(this_query, or_list[0])
                this_comparison.right_operand = parse_where(this_query, or_list[1])
                this_comparison.or_ = True
    else:   # base case
        # break into operands
        this_comparison.leaf = True
        if " =" in where_clause:
            op = "="
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
            this_comparison.less = True
        elif ">" in where_clause:
            op = ">"
            this_comparison.greater = True
        else:
            error(" no recognized operation used in the WHERE clause")

        operand_list = re.split(op, where_clause)   # split clause on whichever operation is found first
        for o in range(len(operand_list)):
            operand_list[o] = operand_list[o].rstrip().lstrip().strip('(').strip(')')
        for operand in range(len(operand_list)):
            # once at leaf level of comparisons, tokenize on "." to find table names (careful not to misclassify floats)
            if operand_list[operand].replace(".", "").isdigit():
                if '.' in operand_list[operand]:
                    helper = float(operand_list[operand])
                else:
                    helper = int(operand_list[operand])
            else:
                attr_list = operand_list[operand].split(".")
                if len(attr_list) > 1:      # validate attr_list
                    attr_list[0] = this_query.from_tables[attr_list[0]]
                    if attr_list[0] in TABLES:
                        if attr_list[1] in TABLES[attr_list[0]].attribute_names:
                            helper = (attr_list[0].rstrip().lstrip(), attr_list[1].rstrip().lstrip())
                        else:
                            error(" valid table name, invalid attribute in WHERE clause")
                    else:
                        error(" invalid table name in WHERE clause")
                else:
                    quote_list = operand_list[operand].split("\"")
                    if len(quote_list) > 1:
                        if len(quote_list) == 3:
                            helper = quote_list[1].lstrip().rstrip()
                        else:
                            error(" syntax error with \"...\" in WHERE clause.")
                    elif this_query.num_tables ==  1:  # no table specified ( >1 table in query )
                        table_key = list(this_query.from_tables.keys())[0]  # only one table --> possibly 2 entries if alias used (either raw name or alias is fine)
                        if operand_list[operand] in TABLES[this_query.from_tables[table_key]].attribute_names:
                             helper = (this_query.from_tables[table_key], operand_list[operand])
                        else:
                            error(" valid table name, invalid attribute in WHERE clause")
                    else:
                        error(" ambiguous which table attribute is from in the WHERE clause. >1 table in querey, must specify where the attribute comes from")

            # complete the Comparison object
            if operand == 0:
                this_comparison.left_operand = helper
            else:
                this_comparison.right_operand = helper
    return this_comparison
# END parse_where













# parse_where
def parse_where_dml(this_query, where_clause):
    # parse by AND/OR recursively --> once done with this, should have atomic comparisons to perform (at leaf level of comparison tree)
    this_comparison = Comparison()
    if " and " in where_clause or " or " in where_clause:
        close_list = where_clause.split(")")
        close_list = [c for c in close_list if c != ""]
        remove_list = []
        counter = 0
        for c in range(len(close_list)):
            num_open = close_list[c].count("(")
            if num_open > 1:
                counter += 1
                num_open -= 1
                for i in range(num_open):
                    remove_list.append(c + i + 1)
        if counter > 0:
            count = 0
            for r in remove_list:
                close_list.pop(r - count)
                count += 1

            # fix regular expression to avoid errors
            helper_list = close_list[1].split("(")
            if len(helper_list) > 1:
                for i in range(1, len(helper_list)):
                    if i != 0:
                        helper_list[i] = "\(" + helper_list[i]
                fixed_string = ''
                for i in helper_list:
                    fixed_string += i
                close_list[1] = fixed_string

            # have narrowed close_list down to only top level operators.  take one at a time (lower levels of recursion will handle the rest)
            useful_list = re.split(close_list[1], where_clause)
            useful_list[1] = close_list[1] + useful_list[1]
            useful_list[1] = useful_list[1].replace("\\", "")

            # determine operation to perform
            open_list = useful_list[1].split("(", maxsplit=1)
            useful_list[1] = "(" + open_list[1]
            if " or " in open_list[0]:
                # clean outer parenthesis
                useful_list[0] = useful_list[0].split("(", maxsplit=1)[1]
                useful_list[1] = useful_list[1].split("(", maxsplit=1)[1]
                zero_ind = useful_list[0].rfind(")")
                useful_list[0] = useful_list[0][:zero_ind]
                one_ind = useful_list[1].rfind(")")
                useful_list[1] = useful_list[1][:one_ind]

                # recursive calls
                this_comparison.left_operand = parse_where(this_query, useful_list[0])
                this_comparison.right_operand = parse_where(this_query, useful_list[1])
                this_comparison.or_ = True
            elif " and " in open_list[0]:
                # clean outer parenthesis
                useful_list[0] = useful_list[0].split("(", maxsplit=1)[1]
                useful_list[1] = useful_list[1].split("(", maxsplit=1)[1]
                zero_ind = useful_list[0].rfind(")")
                useful_list[0] = useful_list[0][:zero_ind]
                one_ind = useful_list[1].rfind(")")
                useful_list[1] = useful_list[1][:one_ind]

                # recursive calls
                this_comparison.left_operand = parse_where(this_query, useful_list[0])
                this_comparison.right_operand = parse_where(this_query, useful_list[1])
                this_comparison.and_ = True
            else:
                error("invalid syntax in where clause")
        else:
            if " and " in where_clause:
                and_list = re.split(" and ", where_clause, maxsplit=1)
                this_comparison.left_operand = parse_where(this_query, and_list[0])
                this_comparison.right_operand = parse_where(this_query, and_list[1])
                this_comparison.and_ = True

            elif " or " in where_clause:
                or_list = re.split(" or ", where_clause, maxsplit=1)
                this_comparison.left_operand = parse_where(this_query, or_list[0])
                this_comparison.right_operand = parse_where(this_query, or_list[1])
                this_comparison.or_ = True
    else:   # base case
        # break into operands
        this_comparison.leaf = True
        if " =" in where_clause:
            op = "="
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
            this_comparison.less = True
        elif ">" in where_clause:
            op = ">"
            this_comparison.greater = True
        else:
            error(" no recognized operation used in the WHERE clause")

        operand_list = re.split(op, where_clause)   # split clause on whichever operation is found first
        for o in range(len(operand_list)):      # clean operands before processing
            operand_list[o] = operand_list[o].rstrip().lstrip().strip('(').strip(')')
        for operand in range(len(operand_list)):
            # once at leaf level of comparisons, tokenize on "." to find table names (careful not to misclassify floats)
            if operand_list[operand].replace(".", "").isdigit():
                if '.' in operand_list[operand]:
                    helper = float(operand_list[operand])
                else:
                    helper = int(operand_list[operand])
            else:   # operand is a name of something (not a number)
                attr_list = operand_list[operand].split(".")
                if operand_list[operand].replace(".", "").lstrip().rstrip().isdigit():
                    if len(attr_list) > 1:
                        helper = float(operand_list[operand])
                    else:
                        helper = int(operand_list[operand])
                elif len(attr_list) > 1:
                    # validate attr_list here
                    if attr_list[0] in TABLES:
                        if attr_list[1] in TABLES[attr_list[0]].attribute_names:
                            helper = (attr_list[0].rstrip().lstrip(), attr_list[1].rstrip().lstrip())
                        else:
                            error(" valid table name, invalid attribute in WHERE clause")
                    else:
                        error(" invalid table name in WHERE clause")
                else:
                    quote_list = operand_list[operand].split("\"")
                    if len(quote_list) > 1:
                        if len(quote_list) == 3:
                            helper = quote_list[1].lstrip().rstrip()
                        else:
                            error(" syntax error with \"...\" in WHERE clause.")
                    else:
                        error(" unrecognized input in WHERE clause. (did you specify tables for all attributes used in WHERE clause?)")

            # complete the Comparison object
            if operand == 0:
                this_comparison.left_operand = helper
            else:
                this_comparison.right_operand = helper
    return this_comparison
# END parse_where_dml
