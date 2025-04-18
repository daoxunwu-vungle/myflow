#! /usr/bin/env python
'''
This application will extract schema changes from 2 version of schema file

Usage:
    schema_extract.py (-h | --help)
    schema_extract.py <new_version_file> <old_version_file> <output_file>

Options:
    -h, --help          Print this screen and exit.
    <new_version_file>  New schema version file, contains new added fields.
    <old_version_file>  Base schema file.
    <output_file>       The file path which accept schema diff info.
'''
from docopt import docopt
import sys
import logging

import yaml
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

_LOGGER     = logging.getLogger('schema_extract.py')

def is_empty_schema(scm):
    if scm == None:
        return True

    if scm['type'] == 'struct':
        return len(scm['fields']) == 0

def find_name(fields, name):
    for i,v in enumerate(fields):
        if v['name'] == name:
            _LOGGER.debug(f"Find {name} in index {i}")
            return i
    _LOGGER.debug(f"Didn't find {name}, return -1" )
    return -1

def schema_subtract(new_scm, old_scm):
    if isinstance(new_scm, str):
        assert new_scm == old_scm, f"string type must be matched"
        _LOGGER.debug(f"schema_subtract: applied subtract on matched type {new_scm}")
        return None
    if isinstance(new_scm['type'], dict):
        # it should be array
        if 'elementType' in new_scm['type']:
            assert isinstance(old_scm['type'], dict), f"schema_subtract: the old schema should be array type"
            assert 'elementType' in old_scm['type'], f'schema_subtract: old schema must have elementType in array type'
            res = schema_subtract(new_scm['type']['elementType'], old_scm['type']['elementType'])
            _LOGGER.debug(f"schema_subtract: applied on array type with name: {new_scm['name']}")
            if res == None:
                return None
            else:
                return new_scm
        elif 'type' in new_scm['type']:
            assert isinstance(old_scm['type'], dict), f"schema_subtract: the old schema should be struct type"
            assert 'type' in old_scm['type'], 'schema_subtract: the old schema must have type in struct type'
            res = schema_subtract_struct(new_scm['type'], old_scm['type'])
            if res == None:
                return None
            else:
                return new_scm
    elif new_scm['type'] == 'struct':
        return schema_subtract_struct(new_scm, old_scm)
    else:
        # for POD types
        _LOGGER.debug("this should be touched")
        assert new_scm['type'] == old_scm['type'], f"schema_subtract: pod types must be matched, but get {new_scm['type']} VS {old_scm['type']}"
        return None

def schema_subtract_struct(new_scm, old_scm):
    assert old_scm['type'] == 'struct', f"schema_subtract: old schema must be struct type, but it is {old_scm['type']}"
    for i,v in enumerate(new_scm['fields']):
        name = new_scm['fields'][i]['name']
        _LOGGER.debug(f"Try check for {name}")
        j = find_name(old_scm['fields'], name)
        if j < 0:
            # didn't find the name
            continue
        res = schema_subtract(new_scm['fields'][i], old_scm['fields'][j])
        new_scm['fields'][i] = res
    _LOGGER.debug(f"fields: {new_scm['fields']}")
    new_scm['fields'] = [i for i in new_scm['fields'] if i]
    _LOGGER.debug(f"schema_subtract: applied on struct type with name: {new_scm.get('name', 'top')}")
    if len(new_scm['fields']) == 0:
        return None
    else:
        return new_scm


def main(args):
    _LOGGER.info(f'Input new schema file {args["<new_version_file>"]}')
    _LOGGER.info(f'Input base schema file {args["<old_version_file>"]}')
    with open(args["<new_version_file>"]) as new_file,\
         open(args["<old_version_file>"]) as old_file:

         new_config = yaml.load(new_file, Loader=Loader)
         old_config = yaml.load(old_file, Loader=Loader)

         remains = schema_subtract(new_config, old_config)
         _LOGGER.debug(f"The subtract results is {remains}")
         if not is_empty_schema(remains):
             with open(args["<output_file>"], "w") as of:
                 _LOGGER.info(f'Saving added schemas to file {args["<output_file>"]}')
                 yaml.dump(remains, of, sort_keys=False)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s %(name)s[%(levelname)s]: %(message)s',
        level=logging.INFO
    )
    sys.exit(main(docopt(__doc__)))
