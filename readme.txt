Instructions for Ubuntu/Debian. (I couldn't get to install plpython3 postgresql extension on Arch Linux or MacOS)

Dependencies:
postgresql12
plpython3  

sudo apt -y install postgresql-12 postgresql-client-12
sudo apt-get install postgresql-plpython3-12

Setup from scratch:
    From any dir:
        sudo -u postgres -i
        psql 
        create role username with login superuser
        create database db_name with owner=username
        exit

    From ba/src
        sudo make install
        psql db_name
        create extension plpython3u;
        create extension gdpr_anatomizer;

Example table and Similarity table should be initialized by default.
To upload the dataset from csv, use function select init_csv_dataset('/home/user/anatomy_gdpr_ba/output.csv'); (use single quotes!)

To use different columns from BankChurners.csv, adjust parse_dataset.py, run it with python3, and upload new output.csv with init_csv_dataset()



Functions:

Data Analyzer:
select analyze_dataset(table_name, column_name);
    e.g.: select analyze_dataset('similarity_table', 'disease');
    e.g.: select analyze_dataset('bank_churners', 'education');

Anatomy:
    select anatomy(table_name, sa_name, qi_columns[], l_level, add_reference*, schema*, create_qi_table*, create_sa_table*, qi_table_name*, sa_table_name*)

        * denotes optional params. that can be added as keyword args, eg "select anatomy('similarity_table', 'disease', '{"*"}', 3, schema:='private');

        e.g. select anatomy('similarity_table', 'disease', '{"*"}', 3);
        e.g. select anatomy('bank_churners', 'income_category', '{"age", "gender"}', 3);
        e.g. select anatomy('bank_churners', 'education', '{"*"}', 3);

Set deletion trigger:
set_del_eq_class_trigger(qi_table_name*, sa_table_name*, schema_name*)

    *all attributes are optional. be default 'qi_table' and 'sa_table' are used
    e.g. select set_del_eq_class_trigger();


Apply/remove SA tags:
select apply_sa_tag(table_name, column_name, tag_value, sa_values[])
    e.g. select apply_sa_tag('similarity_table', 'disease', 'tag', '{"Arthritis", "ALS"}');
    e.g. select apply_sa_tag('bank_churners', 'education', 'tag', '{"Uneducated", "College"}');

select remove_sa_tags(table_name, column_name, tags[])
    e.g. select remove_sa_tags('sa_table', 'education', '{"tag"}');

