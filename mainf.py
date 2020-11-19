CREATE or replace FUNCTION mainfunc(
    table_name text,
    sa_name text,
    qi_columns text[],
    schema text default 'public',
    create_qi_table boolean default true,
    create_sa_table boolean default true,
    qi_table_name text default 'qi_table',
    sa_table_name text default 'sa_table'
)
RETURNS void AS $$
from collections import OrderedDict
import random

plpy.info('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')

L = 2
BUCKET_VALUE_IDX = 1
SA_NAME = sa_name

fetch_column_metadata_string = 'select column_name, column_default, is_nullable, data_type '\
                    'from information_schema.columns '\
                    'where table_schema = $1 '\
                    'and table_name = $2'


fetch_columns_metadata = plpy.prepare(fetch_column_metadata_string, ['text', 'text'])
column_metadata = fetch_columns_metadata.execute([schema, table_name])


qi_column_metadata_dict = {}

for row in column_metadata:
    column_name = row['column_name']

    if column_name == 'id':
        continue
    if column_name == SA_NAME:
        sa_metadata = row
    else:
        qi_column_metadata_dict[column_name] = row


# If qi columns specified, remove them from metadata dict
if qi_columns != ['*']:
    for column_name in qi_column_metadata_dict.keys():
        if not column_name in qi_columns:
            qi_column_metadata_dict.pop(column_name)


qi_column_names = qi_column_metadata_dict.keys()


if create_qi_table == True:
    plpy.execute("drop table if exists {}".format(qi_table_name))

    table_creation_string = "create table {}(id serial unique, ".format(qi_table_name)
    table_creation_string+= 'group_id integer not null, '

    # plpy.info(qi_column_metadata_dict.items())
    for column_name in qi_column_names:
        metadata = qi_column_metadata_dict[column_name]
        table_creation_string+= column_name + ' ' + metadata['data_type']

        if metadata['column_default'] is not None:
            table_creation_string+= ' default ' + metadata['column_default']
        if metadata['is_nullable'] == 'NO':
            table_creation_string+= ' ' + 'not null'
        if column_name == qi_column_names[-1]:
            table_creation_string+= ')'
        else:
            table_creation_string+= ', '

    plpy.execute(table_creation_string)


if create_sa_table == True:
    plpy.execute("drop table if exists {}".format(sa_table_name))

    table_creation_string = "create table {}(id serial unique, ".format(sa_table_name)
    table_creation_string+= 'group_id integer not null, '
    table_creation_string+= SA_NAME + ' ' + sa_metadata['data_type'] + ' ' + 'not null, '
    table_creation_string+= 'count integer default 1)'

    plpy.execute(table_creation_string)

rows = plpy.execute('select * from main_table')


# 2. hash the tuples in T (rows) by their As (sensitive attr) values (each bucket per As value)
buckets = {}

for row in rows:
    sensitive_attr = row[sa_name]

    if sensitive_attr in buckets:
        buckets[sensitive_attr].append(row)
    else:
        buckets[sensitive_attr] = [row]


# Sort buckets (largest -> lowest) for step 5: S = the set of l largest buckets
buckets = OrderedDict(
    sorted(buckets.items(), key=lambda bucket: len(bucket[BUCKET_VALUE_IDX]), reverse=True)
    )

# 3-8 are the group-creation step
# init vars for step 3
group_count = 0
QIgroups = {}

# 3. while there are at least l non-empty hash buckets
while len(buckets) >= L:

    #4. gcnt = gcnt + 1; QIgcnt = ∅
    group_count +=1
    QIgroups[group_count] = []

    #5. S = the set of l largest buckets; 6. for each bucket in S
    for i, key in enumerate(buckets, start=1):
        if i > L:
            break

        # 7. remove an arbitrary tuple t from the bucket; 8. QIgcnt = QIgcnt ∪ {t}
        QIgroups[group_count].append(buckets[key][0])
        del buckets[key][0]

        if len(buckets[key]) < 1:
            buckets.pop(key)

    buckets = OrderedDict(
        sorted(buckets.items(), key=lambda bucket: len(bucket[BUCKET_VALUE_IDX]), reverse=True)
    )

# 9-12 are the residue-assignment step
# 9. for each non-empty bucket
while len(buckets) > 0:
    for key, residue_tuples in buckets.items():

        # this bucket has only one tuple; see Property 1
        if len(residue_tuples) > 1:
            plpy.warning('More than one tuple left')

        # 10. t (residue_tuple) = the only residue tuple of the bucket
        residue_tuple = residue_tuples[0]
        residue_tuple_sa = residue_tuple[SA_NAME]
        # plpy.info(residue_tuple_sa)

        # 11. S` (possible_groups_for_residue) = the set of QI-groups that do not contain the As value t[d + 1]
        possible_groups_for_residue = []
        for group_number, tuples in QIgroups.items():

            sa_present_in_group = False
            for tuple in tuples:
                if tuple[SA_NAME] == residue_tuple_sa:
                    sa_present_in_group = True

            if sa_present_in_group == False:
                possible_groups_for_residue.append(group_number)

        # S` has at least one QI-group; see Property 2
        if len(possible_groups_for_residue) < 1:
            plpy.warning('Error: no possible QI groups')

        # plpy.info(possible_groups_for_residue)

        # 12. assign t to a random QI-group in S
        chosen_group_number = random.choice(possible_groups_for_residue)
        QIgroups[chosen_group_number].append(residue_tuple)
        buckets.pop(key)

list_of_qi_attributes = []
list_of_sa = []
for group_number, tuples in QIgroups.items():
    current_group_sa_list = []
    sa_counts = {}

    for tuple in tuples:
        qi_attributes = [group_number]

        for column_name in qi_column_names:
            qi_attributes.append(tuple[column_name])
        list_of_qi_attributes.append(qi_attributes)

        current_group_sa_list.append(tuple[sa_name])

    for sa in current_group_sa_list:
        if sa in sa_counts:
            sa_counts[sa] += 1
        else:
            sa_counts[sa] = 1

    sa_keys = sa_counts.keys()
    random.shuffle(sa_keys)

    for sa in sa_keys: # {alsheimer, 1}
        list_of_sa.append([group_number, sa, sa_counts[sa]])


sql_insert_qi_attrs_string_base = 'insert into ' + qi_table_name + '(group_id, '
param_references = ['$1']

for column_counter, column_name in enumerate(qi_column_names, start = 2):
    if column_name == qi_column_names[-1]:
        sql_insert_qi_attrs_string_base += column_name + ')'
    else:
        sql_insert_qi_attrs_string_base += column_name + ', '

    param_references.append("${}".format(column_counter))


param_references = '(' + ', '.join(param_references) + ')'
sql_insert_qi_attris_string = sql_insert_qi_attrs_string_base + ' values ' + param_references
qi_insertion_template = plpy.prepare(sql_insert_qi_attris_string, ["int", "int", "text", "text"])

sql_insert_sa_string_base = 'insert into ' + sa_table_name + '(group_id, ' + sa_name + ', count)'
sql_insert_sa_string = sql_insert_sa_string_base + ' values ($1, $2, $3)'
sa_insertion_template = plpy.prepare(sql_insert_sa_string, ["int", "text", "int"])

for x in list_of_qi_attributes:
    qi_insertion_template.execute(x)

for x in list_of_sa:
    sa_insertion_template.execute(x)

$$
LANGUAGE plpythonu;

select mainfunc('main_table', 'disease', '{"*"}');


