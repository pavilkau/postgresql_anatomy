-- *****************************************************************************************
-- Initialize an example table
-- *****************************************************************************************

create or replace function init_example_table()
returns void as $$
begin
    drop table if exists example_table;

    create table example_table(
        id SERIAL UNIQUE,
        age integer,
        fname varchar not null,
        postal_code varchar,
        disease varchar(19)
    );

    insert into example_table(age, fname, postal_code, disease) values
    (23, 'John', '12345', 'Cancer');

    insert into example_table(age, fname, postal_code, disease) values
    (65, 'James', '54674', 'Arthritis');

    insert into example_table(age, fname, postal_code, disease) values
    (32, 'Rita', '34532', 'Asthma');

    insert into example_table(age, fname, postal_code, disease) values
    (31, 'Ruth', '45765', 'Asthma');

    insert into example_table(age, fname, postal_code, disease) values
    (22, 'Adam', '34521', 'Influenza');

    insert into example_table(age, fname, postal_code, disease) values
    (11, 'Jeff', '98799', 'Influenza');

    insert into example_table(age, fname, postal_code, disease) values
    (78, 'Paul', '34523', 'Arthritis');

    insert into example_table(age, fname, postal_code, disease) values
    (77, 'Lin', '12323', 'Arthritis');

    insert into example_table(age, fname, postal_code, disease) values
    (65, 'Alice', '45673', 'ALS');

    insert into example_table(age, fname, postal_code, disease) values
    (23, 'James', '12376', 'ALS');

    insert into example_table(age, fname, postal_code, disease) values
    (32, 'Jimmy', '54732', 'ALS');

    insert into example_table(age, fname, postal_code, disease) values
    (46, 'David', '34521', 'Arthritis');

    insert into example_table(age, fname, postal_code, disease) values
    (44, 'Oscar', '68753', 'Arthritis');

    insert into example_table(age, fname, postal_code, disease) values
    (31, 'Martha', '45313', 'Cancer');

    insert into example_table(age, fname, postal_code, disease) values
    (13, 'Jimmy', '34512', 'ALS');
end;
$$
language plpgsql;

select init_example_table();



-- *****************************************************************************************
-- Initialize the similarity table
-- *****************************************************************************************

create or replace function init_similarity_table()
returns void as $$
begin
    drop table if exists similarity_table;

    create table similarity_table(
        id SERIAL UNIQUE,
        age integer,
        fname varchar not null,
        postal_code varchar,
        disease varchar(19)
    );

    insert into similarity_table(age, fname, postal_code, disease) values
    (23, 'John', '12345', 'Cancer');

    insert into similarity_table(age, fname, postal_code, disease) values
    (65, 'James', '54674', 'Leukemia');

    insert into similarity_table(age, fname, postal_code, disease) values
    (32, 'Rita', '34532', 'Leukemia');

    insert into similarity_table(age, fname, postal_code, disease) values
    (31, 'Ruth', '45765', 'Gastric_Cancer');

    insert into similarity_table(age, fname, postal_code, disease) values
    (22, 'Adam', '34521', 'Influenza');

    insert into similarity_table(age, fname, postal_code, disease) values
    (11, 'Jeff', '98799', 'Hypertension');

    insert into similarity_table(age, fname, postal_code, disease) values
    (78, 'Paul', '34523', 'Arthritis');

    insert into similarity_table(age, fname, postal_code, disease) values
    (77, 'Lin', '12323', 'Hypertension');

    insert into similarity_table(age, fname, postal_code, disease) values
    (65, 'Alice', '45673', 'Hypertension');

    insert into similarity_table(age, fname, postal_code, disease) values
    (23, 'James', '12376', 'ALS');

    insert into similarity_table(age, fname, postal_code, disease) values
    (32, 'Oak', '54732', 'Hypertension');

    insert into similarity_table(age, fname, postal_code, disease) values
    (46, 'Darrel', '34521', 'Leukemia');

    insert into similarity_table(age, fname, postal_code, disease) values
    (55, 'Veronica', '68753', 'Arthritis');

    insert into similarity_table(age, fname, postal_code, disease) values
    (23, 'Luke', '45313', 'Cancer');

    insert into similarity_table(age, fname, postal_code, disease) values
    (54, 'Dinesh', '34512', 'Cancer');

    insert into similarity_table(age, fname, postal_code, disease) values
    (34, 'Wolfgang', '45313', 'Leukemia');

    insert into similarity_table(age, fname, postal_code, disease) values
    (52, 'Amanda', '34512', 'ALS');

    insert into similarity_table(age, fname, postal_code, disease) values
    (76, 'Lara', '45313', 'Hypertension');

    insert into similarity_table(age, fname, postal_code, disease) values
    (44, 'Greta', '34512', 'Leukemia');

    insert into similarity_table(age, fname, postal_code, disease) values
    (13, 'Amanda', '34512', 'Arthritis');
end;
$$
language plpgsql;


select init_similarity_table();



-- *****************************************************************************************
-- Initialize the bank_churners table from csv
-- *****************************************************************************************

create or replace function init_csv_dataset(path_to_file text)
returns void as $$
begin
    drop table if exists bank_churners;

    create table bank_churners(
        id serial,
        age integer,
        gender text,
        dependent_count integer,
        education text,
        marital_status text,
        income_category text
    );

    execute 'copy bank_churners(age, gender, dependent_count, education, marital_status, income_category) from '||quote_literal(path_to_file)||' delimiter '||quote_literal(',')||' csv header';
end;
$$
language plpgsql;




-- *****************************************************************************************
-- Helper functions
-- *****************************************************************************************

create or replace function helper_functions()
returns void as $$
from collections import OrderedDict
import random

BUCKET_VALUE_IDX = 1

def fetch_table_size(table_name):
    fetch_table_size_string = 'select count(*) from {}'.format(table_name)
    return plpy.execute(fetch_table_size_string)[0]['count']

GD['fetch_table_size'] = fetch_table_size



def fetch_sa_distribution(table_name, sa_column_name):
    fetch_sa_distribution_string = 'select distinct({column_name}), '\
                        'count({column_name}) from {table_name} '\
                        'group by {column_name}'.format(column_name=sa_column_name, table_name=table_name)

    sa_distribution_object = plpy.execute(fetch_sa_distribution_string)

    sa_distribution = {}
    for row in sa_distribution_object:
        if row[sa_column_name][:1] == '_':
            if row[sa_column_name][:5] in sa_distribution:
                sa_distribution[row[sa_column_name][:5]] += row['count']
            else:
                sa_distribution[row[sa_column_name][:5]] = row['count']
        else:
            sa_distribution[row[sa_column_name]] = row['count']

    return sa_distribution

GD['fetch_sa_distribution'] = fetch_sa_distribution



def calculate_data_loss(l_level, dataset_size, sa_distribution):
    data_loss_sum = 0
    sa_delta_totals = {}

    while True:
        max_possible_sa = dataset_size // l_level
        overpopulated_sa = { k: v for (k, v) in sa_distribution.items() if v > max_possible_sa }

        if len(overpopulated_sa) == 0:
            break

        sa_overpopulation_delta = { k: v - max_possible_sa for (k, v) in overpopulated_sa.items() }

        for k, v in sa_overpopulation_delta.copy().items():
            if k in sa_delta_totals:
                sa_delta_totals[k] += v
            else:
                sa_delta_totals[k] = v

        for k, v in sa_distribution.copy().items():
            if k in overpopulated_sa:
                sa_distribution[k] = v - sa_overpopulation_delta[k]
            else:
                sa_distribution[k] = v

        data_loss = sum(sa_overpopulation_delta.values())
        data_loss_sum += data_loss
        dataset_size -= data_loss

    return data_loss_sum, sa_delta_totals

GD["calculate_data_loss"] = calculate_data_loss



def suppress_dataset(table_name, sa_delta_hash, sa_column_name):
    delete_records_string_base = "delete from {} where id in".format(table_name)
    delete_records_string_base += " (select id from {} where {} ".format(table_name, sa_column_name)

    for sa_name, rows_to_suppress in sa_delta_hash.items():

        if sa_name[:1] == '_':
            delete_records_string = delete_records_string_base + "like'{}%'  limit {})".format(sa_name, rows_to_suppress)
        else:
            delete_records_string = delete_records_string_base + "='{}'  limit {})".format(sa_name, rows_to_suppress)
        plpy.execute(delete_records_string)


GD["suppress_dataset"] = suppress_dataset



def fetch_column_metadata(schema, table_name, sa_name, specified_qi_columns, add_reference):
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
            if add_reference == True:
                row['column_default'] = None
                qi_column_metadata['reference_no'] = row

            continue
        if column_name == sa_name:
            sa_metadata = row
        else:
            qi_column_metadata[column_name] = row


    # If qi columns specified, keep only the specified ones
    if specified_qi_columns != ['*']:
        for column_name in list(qi_column_metadata.keys()):
            if not column_name in specified_qi_columns:
                if add_reference == True:
                    pass
                else:
                    qi_column_metadata.pop(column_name)

    return qi_column_metadata, sa_metadata

GD["fetch_column_metadata"] = fetch_column_metadata



def create_qi_table(table_name, column_names, column_metadata):
    plpy.info('creating qi table')

    plpy.execute("drop table if exists {}".format(table_name))

    table_creation_string = "create table {}(id serial unique, ".format(table_name)
    table_creation_string+= 'group_id integer not null, '

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
    table_creation_string+= column_name + ' ' + column_metadata['data_type'] + ' ' + 'not null)'

    plpy.execute(table_creation_string)

GD["create_sa_table"] = create_sa_table



def hash_tuples_into_buckets(data, sa_name):
    # 2. hash the tuples in T (rows) by their As (sensitive attr) values (each bucket per As value)
    plpy.info('hashing_tuples')

    # buckets is a dictionary with SA values as keys
    buckets = {}

    for row in data:
        sensitive_attr = row[sa_name]

        if sensitive_attr[:1] == '_':
            sensitive_attr = sensitive_attr[:5]

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
        for i, key in enumerate(list(buckets), start=1):
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
        for key, residue_tuples in buckets.copy().items():

            # this bucket has only one tuple; see Property 1
            if len(residue_tuples) > 1:
                plpy.warning('More than one tuple left')

            # 10. t (residue_tuple) = the only residue tuple of the bucket
            residue_tuple = residue_tuples[0]
            residue_tuple_sa = residue_tuple[sa_name]

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

            # 12. assign t to a random QI-group in S
            chosen_group_number = random.choice(possible_groups_for_residue)
            QIgroups[chosen_group_number].append(residue_tuple)
            buckets.pop(key)

    return QIgroups

GD["assign_residue_tuples"] = assign_residue_tuples



def split(QIgroups, qi_column_names, sa_name):
    plpy.info('splitting qigroups into qi and sa lists')
    
    list_of_qi_attributes = []
    list_of_sa = []
   
    for group_number, tuples in QIgroups.items():
        current_group_sa_list = []
        sa_counts = {}
        
        for tuple in tuples:
           
            qi_attributes = [group_number]
            
            for column_name in qi_column_names:
                
                if column_name == 'reference_no':
                    qi_attributes.append(tuple['id'])
                else:
                    qi_attributes.append(tuple[column_name])
            list_of_qi_attributes.append(qi_attributes)

            current_group_sa_list.append(tuple[sa_name])

        for sa in current_group_sa_list:
            if sa in sa_counts:
                sa_counts[sa] += 1
            else:
                sa_counts[sa] = 1

        sa_keys = list(sa_counts.keys())
        random.shuffle(sa_keys)

        for sa in sa_keys:
            list_of_sa.append([group_number, sa])

    return list_of_qi_attributes, list_of_sa

GD["split"] = split



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

    sql_insert_string_base = 'insert into ' + table_name + '(group_id, ' + sa_name + ')'
    sql_insert_string = sql_insert_string_base + ' values ($1, $2)'
    sql_insertion_template = plpy.prepare(sql_insert_string, ["integer", sa_column_metadata['data_type']])

    return sql_insertion_template

GD["sa_insertion_template"] = sa_insertion_template

$$
language 'plpython3u';

select helper_functions();



-- *****************************************************************************************
-- Analyze dataset
-- *****************************************************************************************

CREATE or replace FUNCTION analyze_dataset(
    table_name text,
    sa_column_name text
)
RETURNS void AS $$
plpy.info('\n\n\n')
plpy.execute('select helper_functions()')

# Fetch dataset size
dataset_size = GD['fetch_table_size'](table_name)
plpy.info("Dataset size: {}".format(dataset_size))

# Fetch the distribution of sa attributes (distinct sa + count)
sa_distribution = GD['fetch_sa_distribution'](table_name, sa_column_name)

for x in sa_distribution:
    plpy.info(x, sa_distribution[x])

plpy.info('\n')
# Print eligible l options
max_l_without_loss = dataset_size // max(sa_distribution.values())
plpy.info("Max L without data loss = {}".format(max_l_without_loss))

for l in range(1, max_l_without_loss + 1):
    number_of_groups = dataset_size // l
    plpy.info("L = {}".format(l) + " -> No. of groups = {}".format(number_of_groups))


# Calculate data loss for non eligible l options
number_of_distinct_sa = len(sa_distribution)

for l in range(max_l_without_loss + 1, number_of_distinct_sa + 1):
    data_loss, _ = GD['calculate_data_loss'](l, dataset_size, sa_distribution.copy())

    updated_dataset_size = dataset_size - data_loss
    number_of_groups = updated_dataset_size // l

    plpy.info("L = {}".format(l) +
        " -> No. of suppressed records = {}".format(data_loss) +
        "; No. of groups = {}".format(number_of_groups))
    

$$
LANGUAGE plpython3u;



-- *****************************************************************************************
-- Apply / remove SA grouping tag
-- *****************************************************************************************


CREATE or replace FUNCTION apply_sa_tag(
    table_name text,
    sa_column_name text,
    tag text, 
    sa_values text[]
)
RETURNS void AS $$


query_string_base = "update {} set {} = regexp_replace({}, ".format(table_name, sa_column_name, sa_column_name)

for val in sa_values:
    find_value_regexp = "^{}".format(val)
    adjusted_value = "_{}_".format(tag) + val
    
    replace_string = query_string_base + "'{}', '{}'".format(find_value_regexp, adjusted_value) + ')'

    plpy.execute(replace_string)

$$
LANGUAGE plpython3u;




CREATE or replace FUNCTION remove_sa_tags(
    table_name text,
    sa_column_name text,
    tags text[]
)
RETURNS void AS $$

query_string_base = "update {} set {} = regexp_replace({}, ".format(table_name, sa_column_name, sa_column_name)

for tag in tags:
    regexp= '^_{}_'.format(tag)
    replace_string = query_string_base + "'{}', '')".format(regexp)
    where_clause = " where {} like '_{}_%'".format(sa_column_name, tag)
    query_string= replace_string + where_clause
    plpy.execute(query_string)
$$
LANGUAGE plpython3u;




-- *****************************************************************************************
-- Anatomy
-- *****************************************************************************************

CREATE or replace FUNCTION anatomy(
    table_name text,
    sa_name text,
    qi_columns text[],
    l_level integer,
    add_reference boolean default true,
    schema text default 'public',
    create_qi_table boolean default true,
    create_sa_table boolean default true,
    qi_table_name text default 'qi_table',
    sa_table_name text default 'sa_table'
)
RETURNS void AS $$

plpy.info('\n\n\n\n')

plpy.execute('select helper_functions()')
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

qi_column_metadata, sa_metadata = GD['fetch_column_metadata'](schema, table_name, sa_name, qi_columns, add_reference)
qi_column_names = list(qi_column_metadata.keys())

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
list_of_qi_attributes, list_of_sa = GD['split'](QIgroups, qi_column_names, sa_name)


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



-- *****************************************************************************************
-- Trigger to delete equivalence class
-- *****************************************************************************************

CREATE or replace FUNCTION delete_eq_class() 
  RETURNS trigger AS
$$
declare
begin
    execute 'delete from '||TG_TABLE_SCHEMA||'.'||TG_TABLE_NAME||' where group_id ='||old.group_id;
    execute 'delete from '||TG_TABLE_SCHEMA||'.'||TG_ARGV[0]||' where group_id ='||old.group_id;
    return old;
END;
$$
language plpgsql;


DROP TRIGGER IF EXISTS del_eq_class on "qi_table";

create or replace function set_del_eq_class_trigger(qi_table_name text default 'qi_table', sa_table_name text default 'sa_table', schema_name text default 'public')
returns void as $$
begin
    execute 'create trigger del_eq_class after delete on '||schema_name||'.'||qi_table_name||' for each row execute procedure delete_eq_class('||sa_table_name||')';
end;
$$
language plpgsql;


-- select set_del_eq_class_trigger();
