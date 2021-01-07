
CREATE or replace FUNCTION calculate_age_change()
RETURNS void AS $$
options = ['0.01', '0.02', '0.03', '0.04', '0.05', '0.06', '0.07', '0.08', '0.09', '0.1', '0.12', '0.14', '0.16', '0.18', '0.2', '0.3', '0.5']

results = {}
for option in options:
    ages = {}
    adjusted_ages = {}
    rez = []

    plpy.execute('create table temp_qi as (select * from qi_table)')

    original_values = plpy.execute('select id, age from qi_table')
    for x in original_values:
        ages[x['id']] = x['age']

    plpy.execute("select anon.add_noise_on_numeric_column('temp_qi', 'age', {})".format(option))

    adjusted_values = plpy.execute('select id, age from temp_qi')
    for x in adjusted_values:
        adjusted_ages[x['id']] = x['age']

    max_id = max(ages.keys())

    for id in range(1, max_id+1):
        delta = abs(adjusted_ages[id] - ages[id])
        rez.append(delta)

    results[option] = sum(rez) / len(rez)

    plpy.execute('drop table temp_qi')

for x in results:
    plpy.info("{0}, {1:.2f}".format(x, results[x]))