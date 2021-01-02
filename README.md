# gdpr_anatomzer postgresl extension

## Dependencies

plpython:
sudo apt-get install postgresql-plpython3-12
in psql: create extension plpython3u;

dataset parser:
pip3 needed: sudo apt install python3-pip
pip3 install pandas

postgresql_anonymizer:
sudo apt-get install build-essential
sudo apt install pgxnclient postgresql-server-dev-12
sudo pgxn install postgresql_anonymizer

## Setup:

from scratch:
sudo -u postgres -i
psql
create role username with login superuser
create database gdpr_ba with owner=username
exit


sudo make install
psql gdpr_ba
create extension gdpr_anatomizer;

initialize example table: init_example_table();
initialize bank churners dataset: select init_csv_dataset('/home/chridcrow/anatomy_gdpr_ba/output.csv');


## Example: anatomy with 3 - diversity

### Initial table
![full](https://i.postimg.cc/qBbdZYXQ/Screenshot-2020-11-19-at-15-33-34.png)

### QI table
![qi](https://i.postimg.cc/VNBx9Hkq/Screenshot-2020-11-19-at-15-34-55.png)

### SA table
![sa](https://i.postimg.cc/Lspd1M24/Screenshot-2020-11-19-at-15-35-10.png)
