import re
import pandas as pd

# Define the Snowflake DDL statement
ddl_statement = '''
CREATE TABLE my_schema.my_table (
    col1 INTEGER DEFAULT 0 NOT NULL,
    col2 VARCHAR(100) NOT NULL,
    col3 BOOLEAN DEFAULT FALSE,
    col4 TIMESTAMP_LTZ NOT NULL,
    col5 FLOAT DEFAULT 3.14,
    col6 VARIANT,
    col7 ARRAY,
    col8 OBJECT,
    CONSTRAINT check_col3 CHECK (col3 IN (TRUE, FALSE)),
    CONSTRAINT pk PRIMARY KEY (col1, col4)
) CLUSTER BY (col4);
'''

# Extract the table name and column definitions
table_name_pattern = r'CREATE TABLE (\S+) \('
column_pattern = r'(\S+)\s+(\S+)(?:\s+DEFAULT\s+(.*?))?(?:\s+(NOT NULL))?(?:\s+CHECK\s+\((.*)\))?(?:\s+(VARIANT|ARRAY|OBJECT))?(?:\s+(.*?))?,?$'
constraint_pattern = r'CONSTRAINT (\S+)\s+(PRIMARY KEY|UNIQUE|CHECK)\s+\((.*)\)'
table_name_match = re.search(table_name_pattern, ddl_statement)
column_matches = re.findall(column_pattern, ddl_statement)
constraint_matches = re.findall(constraint_pattern, ddl_statement)

# Build the dataframe
column_names = ['table_catalog', 'table_schema', 'table_name', 'column_name', 'data_type', 'default', 'not_null', 'check_constraint', 'clustering_key', 'comment']
rows = []
for match in column_matches:
    column_info = list(table_name_match.groups()) + list(match)
    column_info[4] = column_info[4].upper()  # convert Snowflake data type to uppercase
    column_info.append(None)  # initialize comment
    rows.append(column_info)
for match in constraint_matches:
    constraint_info = list(table_name_match.groups()) + list(match)
    constraint_info.append(None)  # initialize data type, default, not_null, check_constraint, and clustering_key
    constraint_info.append(None)
    rows.append(constraint_info)
df = pd.DataFrame(rows, columns=column_names)

# Fill in comments for columns and constraints
column_comment_pattern = r'COMMENT ON COLUMN \S+\.\S+\.(\S+) IS \'(.*)\';'
constraint_comment_pattern = r'COMMENT ON (PRIMARY KEY|UNIQUE|CHECK) \S+\.(\S+) IS \'(.*)\';'
column_comments = re.findall(column_comment_pattern, ddl_statement)
constraint_comments = re.findall(constraint_comment_pattern, ddl_statement)
for match in column_comments:
    df.loc[(df['table_name'] == table_name_match.group(1)) & (df['column_name'] == match[0]), 'comment'] = match[1]
for match in constraint_comments:
    df.loc[(df['table_name'] == table_name_match.group(1)) & (df['column_name'].isna()) & (df['check_constraint'] == match[1]), 'comment'] = match[2]

# Print the resulting dataframe
print(df)
