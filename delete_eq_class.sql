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


select set_del_eq_class_trigger()