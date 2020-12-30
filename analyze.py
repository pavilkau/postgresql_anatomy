CREATE or replace FUNCTION analyze_dataset_for_l(
    table_name text,
    sa_column_name text
)
RETURNS void AS $$
plpy.info('\n\n\n')
plpy.execute('select _meta()')

# Fetch dataset size
dataset_size = GD['fetch_table_size'](table_name)
plpy.info("Dataset size: {}".format(dataset_size))

# Fetch the distribution of sa attributes (distinct sa + count)
sa_distribution = GD['fetch_sa_distribution'](table_name, sa_column_name)

# Print eligible l options
max_l_without_loss = dataset_size // max(sa_distribution.values())
plpy.info("Max L without data loss = {}".format(max_l_without_loss))

for l in range(1, max_l_without_loss + 1):
    number_of_groups = dataset_size // l
    plpy.info("L = {}".format(l) + " -> No. of groups = {}".format(number_of_groups))


# Calculate data loss for non eligible l options
number_of_distinct_sa = len(sa_distribution)

for l in range(max_l_without_loss + 1, number_of_distinct_sa + 1):
    data_loss, _ = GD['calculate_data_loss'](l, dataset_size, sa_distribution)

    updated_dataset_size = dataset_size - data_loss
    number_of_groups = updated_dataset_size // l
    
    plpy.info("L = {}".format(l) +
        " -> No. of supressed records = {}".format(data_loss) +
        "; No. of groups = {}".format(number_of_groups))

$$
LANGUAGE plpythonu;


select analyze_dataset_for_l('bank_churners', 'income_category');

