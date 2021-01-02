EXTENSION = gdpr_anatomizer 
DATA = gdpr_anatomizer--0.0.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
