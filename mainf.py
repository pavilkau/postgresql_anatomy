CREATE or replace FUNCTION mainfunc(
    table_name text,
    sa_name text,
    qi_columns text[],
    l_level integer,
    schema text default 'public',
    create_qi_table boolean default true,
    create_sa_table boolean default true,
    qi_table_name text default 'qi_table',
    sa_table_name text default 'sa_table'
)
RETURNS void AS $$
# Usage:
# select mainfunc('main_table', 'disease', '{"*"}', 2);
# select mainfunc('bank_churners', 'income_category', '{"*"}', 5);

plpy.info('\n\n\n\n')


# *****************************************************************************************
# L parameter eligibility check
# *****************************************************************************************

sa_distribution = GD['fetch_sa_distribution'](table_name, sa_name)
max_l = len(sa_distribution)
if l_level > max_l:
    error_message = "Can't anatomize with this l_level. Max possible l_level = {}".format(max_l)
    plpy.error(error_message)


# *****************************************************************************************
# Database preparation (fetch column types, create qi & sa tables)
# *****************************************************************************************

qi_column_metadata, sa_metadata = GD['fetch_column_metadata'](schema, table_name, sa_name, qi_columns)
qi_column_names = qi_column_metadata.keys()

if create_qi_table == True:
    GD['create_qi_table'](qi_table_name, qi_column_names, qi_column_metadata)

if create_sa_table == True:
    GD['create_sa_table'](sa_table_name, sa_name, sa_metadata)


# *****************************************************************************************
# Data supression. If distribution eligibility requirement is not met - suppress overpopulated SA attributes
# *****************************************************************************************

dataset_size = GD['fetch_table_size'](table_name)
max_l_without_loss = dataset_size // max(sa_distribution.values())


if max_l_without_loss < l_level:
    temp_table_name = "temp_{}".format(table_name)
    plpy.execute("create table {} as (select * from {})".format(temp_table_name, table_name))

    _, sa_delta_hash = GD['calculate_data_loss'](l_level, dataset_size, sa_distribution)
    GD['suppress_dataset'](temp_table_name, sa_delta_hash, sa_name)

    data = plpy.execute("select * from {}".format(temp_table_name))
    plpy.execute("drop table {}".format(temp_table_name))
else:
    data = plpy.execute("select * from {}".format(table_name))


# *****************************************************************************************
# Anatomizaton algorithm
# *****************************************************************************************

# Hash the tuples in T (rows) by their As (sensitive attr) values (each bucket per As value)
# data = plpy.execute("select * from {}".format(table_name))
buckets = GD["hash_tuples_into_buckets"](data, sa_name)

# Create QI groups
QIgroups, buckets = GD["create_qi_groups"](buckets, l_level)

# Assign residue tuples to QIgroups
QIgroups = GD["assign_residue_tuples"](QIgroups, buckets, sa_name)

# Split QIgroups into qi attributes list and sa list
list_of_qi_attributes, list_of_sa = GD['anatomize'](QIgroups, qi_column_names, sa_name)


# *****************************************************************************************
# Data insertion part
# *****************************************************************************************

sql_qi_insertion_template = GD["qi_insertion_template"](qi_table_name, qi_column_metadata, qi_column_names)
sql_sa_insertion_template = GD["sa_insertion_template"](sa_table_name, sa_metadata, sa_name)

for row in list_of_qi_attributes:
        sql_qi_insertion_template.execute(row)

for row in list_of_sa:
        sql_sa_insertion_template.execute(row)

$$
LANGUAGE plpython3u;
