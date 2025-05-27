#! /usr/bin/env python
'''
This application will propagate schema changes to given code/fixture files

Usage:
    propagate.py (-h | --help)
    propagate.py column_spec --diff=<schema_diff_file> <col_file_spec>
    propagate.py column_tran --diff=<schema_diff_file> <col_trans_file>
    propagate.py fixture [--seed=<SEED>] --diff=<schema_diff_file> <ci_fixture_file>
    propagate.py sql_template --diff=<schema_diff_file> <sql_file>
    propagate.py sql_ddl --diff=<schema_diff_file> [--catalog=<catalog>] <table_spec>

Options:
    -h, --help                  Print this screen and exit.
    --diff=<schema_diff_file>   Specify the schema diff file contains new schema fields only.
    --catalog=<catalog>         Specify the catalog name of prod tables.
                                [default: hive_prod]
    --seed=<SEED>               Specify random seed number for fill fixture data, seed less or equal 0
                                won't be applied, thus use the default python implementation.
                                [default: 0]
    <col_file_spec>             Specify the column file path and column name suffix.
    <col_trans_file>            Specify the column transform file.
    <ci_fixture_file>           Specify the fixture data file path.
    <sql_file>                  Specify the SQL template file path.
    <table_spec>                Specify the iceberg table and column suffix.
'''

from docopt import docopt
import sys
import logging
import random

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
import json

_LOGGER     = logging.getLogger('propagate.py')


# util functions
def pick_random_sample_data(conf):
    return random.choice(conf["sample_data"])

def add_coba_obj_value(data, keys, value):
    assert len(keys) > 0, "there should at least 1 key"
    if len(keys) == 1:
        currentKey = keys[0]
        if currentKey.endswith("[]"):
            currentKey = keys[0][:-2]
        # won't add new value if current key exists
        if currentKey not in data:
            data[currentKey] = value
    else:
        currentKey = keys[0]
        if currentKey.endswith("[]"):
            currentKey = currentKey[:-2]
            assert not currentKey.endswith("[]"), "array on array not supported"
            if currentKey not in data:
                data[currentKey] = [{}]
            for d in data[currentKey]:
                add_coba_obj_value(d, keys[1:], value)
        else:
            if currentKey not in data:
                data[currentKey] = {}
            add_coba_obj_value(data[currentKey], keys[1:], value)


def list_leaf(schema, keys=[]):
    if isinstance(schema['type'], dict):
        if 'elementType' in schema['type']:
            newKeys = keys.copy()
            newKeys.append(f"{schema['name']}[]")
            if isinstance(schema['type']['elementType'], dict):
                assert schema['type']['elementType']['type'] == 'struct', 'array in array type is not supported yet!'
                results = []
                for elem in schema['type']['elementType']['fields']:
                    results.extend(list_leaf(elem, newKeys))
                return results
            else:
                _LOGGER.debug(f"Add leaf {keys}, elementType: {schema['type']['elementType']}")
                return [(newKeys, schema)]
        elif 'type' in schema['type'] and 'struct' == schema['type']['type']:
            newKeys = keys.copy()
            newKeys.append(f"{schema['name']}")
            results = []
            for elem in schema['type']['fields']:
                results.extend(list_leaf(elem, newKeys))
            return results
        else:
            raise Exception(f"Unsupported schema element type: {schema['type']}!")
    elif isinstance(schema['type'], str):
        if 'struct' == schema['type']:
            results = []
            for elem in schema['fields']:
                results.extend(list_leaf(elem, keys))
            return results
        else:
            newKeys = keys.copy()
            newKeys.append(f"{schema['name']}")
            _LOGGER.info(f"Add leaf {newKeys}, elementType: {schema['type']}")
            return [(newKeys, schema)]
    else:
        raise Exception(f"Unsupported schema type: {schema['type']}")


def modify_data(datastr, schema_diff):
    data = json.loads(datastr)
    result = list_leaf(schema_diff)
    _LOGGER.info(f"There are {len(result)} fields to add!")
    for keys, conf in result:
        value = pick_random_sample_data(conf)
        add_coba_obj_value(data, keys, value)
    return json.dumps(data, sort_keys=True, indent=None)

def modify_derived_data(line, schema_diff, col_suffix):
    data = json.loads(line)
    result = list_leaf(schema_diff)
    _LOGGER.info(f"modify_derived_data: there are {len(result)} fields to add!")
    for _, conf in result:
        value = pick_random_sample_data(conf)
        keys = [f'{conf["target_name"]}{col_suffix}']
        add_coba_obj_value(data, keys, value)
    return json.dumps(data, indent=None)


# # functions for change sql files

def extract_columns_from_sqlddl(ddl):
    """
    Extract column definition from SQL DDL.
    parameters:
        ddl: SQL DDL in string type
    return:
        A turple of 3 values, explained in order with names in the function.
        prefix:         A string, which contains DDL part before column definition.
                        Include the '(' which marks start of column definition.
        columns:        An array of strings, contains column definitions (in string) one
                        by one, keep the order of the input ddl.
        suffix:         A string, contains the DDL string after column definition.
                        Include the leading ')' which marks end of column definition.
    example:
    >>> sql_str = '''
    ... CREATE TABLE IF NOT EXISTS ?table? (
    ...     event_id string,
    ...     imp_id string,
    ...     winning_bid_adomain array<string>,
    ...     bid_price decimal(14, 5),
    ...     txn_time timestamp,
    ...     ingest_time timestamp,
    ...     ingest_time_at_s3 timestamp
    ... )
    ... USING iceberg
    ... PARTITIONED BY (hours(txn_time), bucket(?bucket_num?, event_id))
    ... LOCATION "?location?"
    ... '''
    >>> prefix, columns, suffix = extract_columns_from_sqlddl(sql_str)
    >>> print(prefix)
    <BLANKLINE>
    CREATE TABLE IF NOT EXISTS ?table? (
    >>> print(suffix)
    )
    USING iceberg
    PARTITIONED BY (hours(txn_time), bucket(?bucket_num?, event_id))
    LOCATION "?location?"
    <BLANKLINE>
    >>> print(columns[2])
    winning_bid_adomain array<string>
    >>> print(columns[3])
    bid_price decimal(14, 5)
    """
    # find the place of the first '('
    open_idx = ddl.find('(')
    if open_idx == -1:
        raise ValueError("No opening parenthesis found in DDL")

    # find ')' to track ',' level
    depth = 0
    close_idx = -1
    for i, ch in enumerate(ddl[open_idx:], start=open_idx):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                close_idx = i
                break
    if close_idx == -1:
        raise ValueError("No matching closing parenthesis found in DDL")

    # split to prefix, columns_str, suffix
    prefix = ddl[:open_idx + 1]
    columns_str = ddl[open_idx + 1:close_idx]
    suffix = ddl[close_idx:]

    # extract columns by the top ','
    columns = []
    buffer = []
    depth = 0
    for ch in columns_str:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        # depth == 0 to ensure not split in the middle of a column definition for special types
        # e.g. "bid_price decimal(14, 5)"
        if ch == ',' and depth == 0:
            columns.append(''.join(buffer).strip())
            buffer = []
        else:
            buffer.append(ch)
    # Add the last column
    last_col = ''.join(buffer).strip()
    if last_col:
        columns.append(last_col)

    return prefix, columns, suffix

def is_schema_simple_array(conf):
    return "elementType" in conf and isinstance(conf["elementType"], str) and conf["elementType"] != "struct"

def get_iceberg_pod_type(type_str):
    """
    convert spark dataframe primitive type (represented by json string) to iceberg table type
    """
    if "boolean" == type_str:
        return "boolean"
    elif "byte" == type_str:
        return "int"
    elif "short" == type_str:
        return "int"
    elif "integer" == type_str:
        return "int"
    elif "long" == type_str:
        return "long"
    elif "float" == type_str:
        return "float"
    elif "double" == type_str:
        return "double"
    elif "string" == type_str:
        return "string"
    elif "binary" == type_str:
        return "binary"
    elif "date" == type_str:
        return "date"
    elif "timestamp" == type_str:
        return "timestamp"
    else:
        raise ValueError(f"unspported spark type string {type_str}!")


def get_sql_type(conf):
    if isinstance(conf["type"], dict):
        assert is_schema_simple_array(conf["type"]), f"Only allowed complex type is array of POD type, but got {conf}"
        return f'array<{get_iceberg_pod_type(conf["type"]["elementType"])}>'
    return get_iceberg_pod_type(conf["type"])


# wrapper functions
def modify_fixture(fixture_spec, schema_diff_file, seed):
    # we only support new line delimited json data as ci fixture
    if seed > 0:
        random.seed(seed)

    with open(schema_diff_file) as sf:
        schema_diff = yaml.safe_load(sf)

    specs = fixture_spec.split(":")
    fixture_file = specs[0]
    is_derived = (specs[1] == "derived")
    col_suffix = specs[2]
    lines = []
    with open(fixture_file) as f:
        for l in f:
            if not is_derived:
                lines.append(f"{modify_data(l, schema_diff)}\n")
            else:
                lines.append(f"{modify_derived_data(l, schema_diff, col_suffix)}\n")

    _LOGGER.info(f"Totally {len(lines)} fixture data are modified!")
    with open(fixture_file, 'w') as f:
        f.writelines(lines)

def show_sql_ddl(table_spec, schema_diff_file, catalog):
    _LOGGER.info(f"Print SQL DDL, table_spec: {table_spec}, schema diff: {schema_diff_file}, catalog: {catalog}")
    with open(schema_diff_file) as f:
        schema_diff = yaml.safe_load(f)

    tbl_specs = table_spec.split(":")
    table, col_suffix = tbl_specs[0], tbl_specs[1]
    table_name = f"{catalog}.{table}"
    leaves = list_leaf(schema_diff)
    for key, conf in leaves:
        col_name = f'{conf["target_name"]}{col_suffix}'
        type_str = get_sql_type(conf).upper()
        print(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {type_str};")

def modify_sql_file(sql_spec, schema_diff_file):
    _LOGGER.info(f"modify {sql_spec} with diff {schema_diff_file}")
    with open(schema_diff_file) as f:
        schema_diff = yaml.safe_load(f)

    sql_specs = sql_spec.split(":")
    sql_file, col_suffix = sql_specs[0], sql_specs[1]
    with open(sql_file) as f:
        sql_str = f.read()
    prefix, columns, suffix = extract_columns_from_sqlddl(sql_str)
    old_col_def = [s.split(maxsplit=1) for s in columns]
    leaves = list_leaf(schema_diff)

    new_col_def = []
    for key, conf in leaves:
        name = conf["target_name"]
        col_name = f"{name}{col_suffix}"
        new_col_def.append([col_name, get_sql_type(conf)])

    txn_idxs = [i for i, col in enumerate(old_col_def) if col[0] == "txn_time"]
    if len(txn_idxs) > 0:
        idx = txn_idxs[0]
        col_def = old_col_def[:idx] + new_col_def + old_col_def[idx:]
    else:
        col_def = old_col_def + new_col_def

    new_columns = [" ".join(d) for d in col_def]
    with open(sql_file, "w") as f:
        f.write(prefix)
        f.write("\n    ")
        f.write(",\n    ".join(new_columns))
        f.write("\n")
        f.write(suffix)

def add_column_from_diff_file(column_file_spec, schema_diff_file):
    with open(schema_diff_file) as f:
        schema_diff = yaml.safe_load(f)

    with open(column_file_spec) as f:
        columns = json.load(f)

    leaves = list_leaf(schema_diff)
    new_columns = [conf["target_name"] for key, conf in leaves]
    # Insert new columns before "txn_time" if it exists
    if "txn_time" in columns:
        idx = columns.index("txn_time")
        columns = columns[:idx] + new_columns + columns[idx:]
    else:
        columns.extend(new_columns)

    with open(column_file_spec, 'w') as f:
        json.dump(columns, f, indent=2)
        f.write("\n")

def add_column_mapping_from_diff_file(column_trans_file, schema_diff_file):
    with open(schema_diff_file) as f:
        schema_diff = yaml.safe_load(f)

    with open(column_trans_file) as f:
        column_mapping = json.load(f)

    leaves = list_leaf(schema_diff)
    for key, conf in leaves:
        col_left_exp = conf['source_exp']
        target_col_name = conf['target_name']
        column_mapping[col_left_exp] = target_col_name

    with open(column_trans_file, 'w') as f:
        json.dump(column_mapping, f, indent=2)
        f.write("\n")


def main(args):
    logging.basicConfig(
        format='%(asctime)s %(name)s[%(levelname)s]: %(message)s',
        level=logging.INFO
    )
    if args["fixture"]:
        modify_fixture(args["<ci_fixture_file>"], args["--diff"], int(args["--seed"]))
        return 0
    if args["sql_template"]:
        modify_sql_file(args["<sql_file>"], args["--diff"])
        return 0
    if args["sql_ddl"]:
        show_sql_ddl(args["<table_spec>"], args["--diff"], args["--catalog"])
        return 0
    if args["column_spec"]:
        add_column_from_diff_file(args["<col_file_spec>"], args["--diff"])
        return 0
    if args["column_tran"]:
        add_column_mapping_from_diff_file(args["<col_trans_file>"], args["--diff"])
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main(docopt(__doc__)))
