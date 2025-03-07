# gcp-sf-utils
A library of utilities to use snowflake from gcp notebooks


This library adheres closely to the interfaces provided by the previous GCP utils library with the following important caveats:
1. An OOP oriented design is provided for quick switching in emergency cases, but this object and its associated singleton constructor should only be used for "quick fixes" and not for newer code
2. The newer approach is more funcitonal than OOP
3. The functions retain, where possible the same name and argument orders of the methods in the previous OOP version
4. The new functional approach requires the connection object to be passed in each time

## Installation Instructions

```bash
%pip install --upgrade git+https://<github_username>:<github_access_token>@github.com/guardian-group-ti/gcp-sf-utils.git
```

## Usage examples

```python
from gcp_secret_accessor import secret_loader # imports separate secret loader 
from gcp_sf_utils import sf_utils # imports this library

# access secret for SF access
db_secret = secret_loader.SecretLoader(PROJECT_ID).load_secret(secret_id, version_id)

database = # fill in database to be used here
schema = # schema to be used here, no need to double quote schema starting with numbers

df = # some dataframe to write
table_name = # name of the table to write df to 
query = # some query to execute

table_to_inspect = # table to get information of columns and types

# constructs connector context with an associated database and schema
with sf_utils.build_snowflake_connector(db_secret, database, schema) as ctx:
  sf_utils.execute_query(query, ctx) # executes a query
  df_new_1 = sf_utils.retrieve_query_res(query, ctx) # executes a query and retrieves the result as a dataframe
  info_on_table_to_inspect = sf_utils.get_column_names_and_types(table_to_inspect, ctx) # gets table information

  # inserts the dataframe as a table
  # NB
  # create_table=True means that if the table does not exist, the table will be created
  # append_table=True means that the records are appended to the table. If this is set to false, the table's contents are overwritten
  sf_utils.insert_dataframe(table_name , df, ctx, create_table=True, append_table=True, verbose=True) 

```
