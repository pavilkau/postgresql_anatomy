drop table if exists main_table;

create table main_table(
    id SERIAL UNIQUE,
    age integer,
    fname varchar not null,
    postal_code varchar,
    disease varchar(19)
);

insert into main_table(age, fname, postal_code, disease) values
(23, 'John', '12345', 'Cancer');

insert into main_table(age, fname, postal_code, disease) values
(65, 'James', '54674', 'Arthritis');

insert into main_table(age, fname, postal_code, disease) values
(32, 'Rita', '34532', 'Asthma');

insert into main_table(age, fname, postal_code, disease) values
(31, 'Ruth', '45765', 'Asthma');

insert into main_table(age, fname, postal_code, disease) values
(22, 'Adam', '34521', 'Influenza');

insert into main_table(age, fname, postal_code, disease) values
(11, 'Jeff', '98799', 'Influenza');

insert into main_table(age, fname, postal_code, disease) values
(78, 'Paul', '34523', 'Arthritis');

insert into main_table(age, fname, postal_code, disease) values
(77, 'Lin', '12323', 'Arthritis');

insert into main_table(age, fname, postal_code, disease) values
(65, 'Alice', '45673', 'ALS');

insert into main_table(age, fname, postal_code, disease) values
(23, 'James', '12376', 'ALS');

insert into main_table(age, fname, postal_code, disease) values
(32, 'Jimmy', '54732', 'ALS');

insert into main_table(age, fname, postal_code, disease) values
(46, 'David', '34521', 'Arthritis');

insert into main_table(age, fname, postal_code, disease) values
(44, 'Oscar', '68753', 'Arthritis');

insert into main_table(age, fname, postal_code, disease) values
(31, 'Martha', '45313', 'Cancer');

insert into main_table(age, fname, postal_code, disease) values
(13, 'Jimmy', '34512', 'ALS');


-- drop table if exists qi_table;
-- create table qi_table(
--     id SERIAL UNIQUE,
--     age integer,
--     fname varchar,
--     postal_code varchar,
--     group_id integer
-- );

-- drop table if exists sa_table;
-- create table sa_table(
--     id SERIAL UNIQUE,
--     disease varchar,
--     count integer,
--     group_id integer
-- );

-- SELECT 'DROP FUNCTION ' || oid::regprocedure
-- FROM   pg_proc
-- WHERE  proname = 'mainfunc'
-- AND    pg_function_is_visible(oid);
