create or replace function _meta()
returns void as $$
from collections import OrderedDict
import random

BUCKET_VALUE_IDX = 1

def fetch_column_metadata(schema, table_name, sa_name, specified_qi_columns):
    plpy.info('fetching metadata')

    fetch_column_metadata_string = 'select column_name, column_default, is_nullable, data_type '\
                    'from information_schema.columns '\
                    'where table_schema = $1 '\
                    'and table_name = $2'

    fetch_columns_metadata_plan = plpy.prepare(fetch_column_metadata_string, ['text', 'text'])
    column_metadata = fetch_columns_metadata_plan.execute([schema, table_name])

    qi_column_metadata = {}

    for row in column_metadata:
        column_name = row['column_name']

        if column_name == 'id':
            continue
        if column_name == sa_name:
            sa_metadata = row
        else:
            qi_column_metadata[column_name] = row


    # If qi columns specified, remove them from metadata dict
    if specified_qi_columns != ['*']:
        for column_name in qi_column_metadata.keys():
            if not column_name in specified_qi_columns:
                qi_column_metadata.pop(column_name)

    return qi_column_metadata, sa_metadata

GD["fetch_column_metadata"] = fetch_column_metadata


def create_qi_table(table_name, column_names, column_metadata):
    plpy.info('creating qi table')

    plpy.execute("drop table if exists {}".format(table_name))

    table_creation_string = "create table {}(id serial unique, ".format(table_name)
    table_creation_string+= 'group_id integer not null, '

    # plpy.info(qi_column_metadata.items())
    for column_name in column_names:
        metadata = column_metadata[column_name]
        table_creation_string+= column_name + ' ' + metadata['data_type']

        if metadata['column_default'] is not None:
            table_creation_string+= ' default ' + metadata['column_default']
        if metadata['is_nullable'] == 'NO':
            table_creation_string+= ' ' + 'not null'
        if column_name == column_names[-1]:
            table_creation_string+= ')'
        else:
            table_creation_string+= ', '

    plpy.execute(table_creation_string)

GD["create_qi_table"] = create_qi_table


def create_sa_table(table_name, column_name, column_metadata):
    plpy.info('creating sa table')

    plpy.execute("drop table if exists {}".format(table_name))

    table_creation_string = "create table {}(id serial unique, ".format(table_name)
    table_creation_string+= 'group_id integer not null, '
    table_creation_string+= column_name + ' ' + column_metadata['data_type'] + ' ' + 'not null, '
    table_creation_string+= 'count integer default 1)'

    plpy.execute(table_creation_string)

GD["create_sa_table"] = create_sa_table


def hash_tuples_into_buckets(data, sa_name):
    # 2. hash the tuples in T (rows) by their As (sensitive attr) values (each bucket per As value)
    plpy.info('hashing_tuples')

    buckets = {}

    for row in data:
        sensitive_attr = row[sa_name]

        if sensitive_attr in buckets:
            buckets[sensitive_attr].append(row)
        else:
            buckets[sensitive_attr] = [row]

    return buckets

GD["hash_tuples_into_buckets"] = hash_tuples_into_buckets


def create_qi_groups(buckets, l_level):
    plpy.info('creating QI groups')

    # Sort buckets (largest -> lowest) for step 5: S = the set of l largest buckets
    buckets = OrderedDict(
        sorted(buckets.items(), key=lambda bucket: len(bucket[BUCKET_VALUE_IDX]), reverse=True)
        )

    # 3-8 are the group-creation step
    # init vars for step 3
    group_count = 0
    QIgroups = {}

    # 3. while there are at least l non-empty hash buckets
    while len(buckets) >= l_level:

        #4. gcnt = gcnt + 1; QIgcnt = ∅
        group_count +=1
        QIgroups[group_count] = []

        #5. S = the set of l largest buckets; 6. for each bucket in S
        # Instead of assigning l largest buckets to variable S
        # buckets dictionary is iterated l times
        for i, key in enumerate(buckets, start=1):
            if i > l_level:
                break

            # 7. remove an arbitrary tuple t from the bucket; 8. QIgcnt = QIgcnt ∪ {t}
            random_tuple_idx = random.randrange(len(buckets[key]))
            QIgroups[group_count].append(buckets[key][random_tuple_idx])
            del buckets[key][random_tuple_idx]

            if len(buckets[key]) < 1:
                buckets.pop(key)

        buckets = OrderedDict(
            sorted(buckets.items(), key=lambda bucket: len(bucket[BUCKET_VALUE_IDX]), reverse=True)
        )

    return QIgroups, buckets

GD["create_qi_groups"] = create_qi_groups


def assign_residue_tuples(QIgroups, buckets, sa_name):
    plpy.info('procesing residue assign')

    while len(buckets) > 0:
        for key, residue_tuples in buckets.items():

            # this bucket has only one tuple; see Property 1
            if len(residue_tuples) > 1:
                plpy.warning('More than one tuple left')

            # 10. t (residue_tuple) = the only residue tuple of the bucket
            residue_tuple = residue_tuples[0]
            residue_tuple_sa = residue_tuple[sa_name]
            # plpy.info(residue_tuple_sa)

            # 11. S` (possible_groups_for_residue) = the set of QI-groups that do not contain the As value t[d + 1]
            possible_groups_for_residue = []
            for group_number, tuples in QIgroups.items():

                sa_present_in_group = False
                for tuple in tuples:
                    if tuple[sa_name] == residue_tuple_sa:
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

    return QIgroups

GD["assign_residue_tuples"] = assign_residue_tuples


def anatomize(QIgroups, qi_column_names, sa_name):
    plpy.info('splitting qigroups into qi and sa lists')

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

        for sa in sa_keys:
            list_of_sa.append([group_number, sa, sa_counts[sa]])

    return list_of_qi_attributes, list_of_sa

GD["anatomize"] = anatomize


def qi_insertion_template(table_name, column_metadata, column_names):
    plpy.info('generating qi insertion template')

    # init data types list with integer for group_id
    qi_data_types_list = ['integer']
    sql_insert_string_base = 'insert into ' + table_name + '(group_id, '
    param_references = ['$1']

    for column_counter, column_name in enumerate(column_names, start = 2):
        qi_data_types_list.append(column_metadata[column_name]['data_type'])

        if column_name == column_names[-1]:
            sql_insert_string_base += column_name + ')'
        else:
            sql_insert_string_base += column_name + ', '

        param_references.append("${}".format(column_counter))

    param_references = '(' + ', '.join(param_references) + ')'
    sql_insert_string = sql_insert_string_base + ' values ' + param_references
    sql_insertion_template = plpy.prepare(sql_insert_string, qi_data_types_list)

    return sql_insertion_template

GD["qi_insertion_template"] = qi_insertion_template


def sa_insertion_template(table_name, sa_column_metadata, sa_name):
    plpy.info('generating sa insertion template')

    sql_insert_string_base = 'insert into ' + table_name + '(group_id, ' + sa_name + ', count)'
    sql_insert_string = sql_insert_string_base + ' values ($1, $2, $3)'
    sql_insertion_template = plpy.prepare(sql_insert_string, ["integer", sa_column_metadata['data_type'], "integer"])

    return sql_insertion_template

GD["sa_insertion_template"] = sa_insertion_template

$$
language 'plpythonu';
