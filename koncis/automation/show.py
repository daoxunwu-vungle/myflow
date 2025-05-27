#! /usr/bin/env python
'''
This application will try to print filepath/info for given components based on schema file.

Usage:
    show.py (-h | --help)
    show.py column_files [--schema-config=<config_file>] <schema_file>
    show.py column_trans [--schema-config=<config_file>] <schema_file>
    show.py fixtures [--schema-config=<config_file>] <schema_file>
    show.py sql_templates [--schema-config=<config_file>] <schema_file>
    show.py litepipe [--schema-config=<config_file>] <schema_file>
    show.py iceberg_tables [--schema-config=<config_file>] <schema_file>

Options:
    -h, --help                          Print this screen and exit.
    <schema_file>                       The schema file path whose content has changed.
    --schema-config=<config_file>       Specify the schema config file path
                                        [default: automation/schema_config.yaml]
'''

from docopt import docopt
import sys
import logging

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

_LOGGER     = logging.getLogger('show.py')

# util functions
def get_schema_config(configs, schema_file_path):
    """
    Get schema config for the specific schema, which is identified by the
    schema_file_path
    parameters:
        configs:                Dict which was load from schemas config file, the file
                                is in YAML format.
        schema_file_path:       file path of the schema file, which is used to identify
                                a specific schema.
    return:
        Python dict of the config of the specific schema.
    """
    for conf in configs['schemas']:
        _LOGGER.debug(f'Checking schema {conf["name"]} in file {conf["file"]}')
        if conf['file'] == schema_file_path:
            _LOGGER.debug(f'schema {conf["name"]} matched {schema_file_path}, return it!')
            return conf
    return None

def parse_schema_fixtures(config):
    fixtures = [f"{path}::" for path in config["ci_fixture"]]
    fixtures_derived = [f"{conf['file']}:derived:{safe_suffix(conf, 'col_suffix')}" for conf in config["ci_fixture_derived"]]
    fixtures.extend(fixtures_derived)
    return fixtures

def safe_suffix(data, suffix_key):
    suffix = data.get(suffix_key, "")
    if suffix is None:
        return ""
    return suffix


def parse_schema_sql_templates(config):
    template_list = config["iceberg_table_schemas"]
    return [f'{d["file"]}:{safe_suffix(d, "col_suffix")}' for d in template_list]

def parse_schema_column_files(config):
    return config["col_files"]

def parse_schema_column_mapping_files(config):
    return config["col_mappings"]

def parse_schema_related_litepipes(config):
    return config["litepipes"]

def parse_iceberg_tables_spec(config):
    tables_list = config["iceberg_table_schemas"]
    results = []
    for conf in tables_list:
        _LOGGER.info(f"Process {conf['file']}")
        tables = conf.get("prod_tables", [])
        if tables:
            col_suffix = safe_suffix(conf, "col_suffix")
            results.extend([f'{table}:{col_suffix}' for table in tables])
    return results

# wrapper functions
def print_list(str_list):
    for s in str_list:
        _LOGGER.debug(f"print_list output: '{s}'")
        print(s)

def load_schema_config(schema_config_path):
    with open(schema_config_path) as f:
        return yaml.safe_load(f)


def show_fixtures(schema_path, schema_config_path):
    schema_config = load_schema_config(schema_config_path)
    config = get_schema_config(schema_config, schema_path)
    print_list(parse_schema_fixtures(config))

def show_sql_templates(schema_path, schema_config_path):
    schema_config = load_schema_config(schema_config_path)
    config = get_schema_config(schema_config, schema_path)
    print_list(parse_schema_sql_templates(config))

def show_column_files(schema_path, schema_config_path):
    schema_config = load_schema_config(schema_config_path)
    config = get_schema_config(schema_config, schema_path)
    print_list(parse_schema_column_files(config))

def show_column_mapping_files(schema_path, schema_config_path):
    schema_config = load_schema_config(schema_config_path)
    config = get_schema_config(schema_config, schema_path)
    print_list(parse_schema_column_mapping_files(config))

def show_litepipe_names(schema_path, schema_config_path):
    schema_config = load_schema_config(schema_config_path)
    config = get_schema_config(schema_config, schema_path)
    print_list(parse_schema_related_litepipes(config))

def show_iceberg_table_specs(schema_path, schema_config_path):
    schema_config = load_schema_config(schema_config_path)
    config = get_schema_config(schema_config, schema_path)
    print_list(parse_iceberg_tables_spec(config))


def main(args):
    logging.basicConfig(
        format='%(asctime)s %(name)s[%(levelname)s]: %(message)s',
        level=logging.INFO
    )
    if args["fixtures"]:
        show_fixtures(args["<schema_file>"], args["--schema-config"])
        return 0
    if args["sql_templates"]:
        show_sql_templates(args["<schema_file>"], args["--schema-config"])
        return 0
    if args["column_files"]:
        show_column_files(args["<schema_file>"], args["--schema-config"])
        return 0
    if args["column_trans"]:
        show_column_mapping_files(args["<schema_file>"], args["--schema-config"])
        return 0
    if args["litepipe"]:
        show_litepipe_names(args["<schema_file>"], args["--schema-config"])
        return 0
    if args["iceberg_tables"]:
        show_iceberg_table_specs(args["<schema_file>"], args["--schema-config"])
        return 0

if __name__ == "__main__":
    sys.exit(main(docopt(__doc__)))
