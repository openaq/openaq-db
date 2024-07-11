
import logging
import pytest
from os import environ


logger = logging.getLogger(__name__)

from client import DB


tables = {
    "users": 2700,
    "lists": 406,
    "users_lists": 0,
    "user_keys": 2540,
    "countries": 258,
    "sensor_nodes": 73000,
    "sensors": 515000,
    "providers": 216,
    "providers_licenses": 44,
    }

@pytest.mark.parametrize("table", tables.keys())
def test_tables_populated(table):
    db = DB()
    n = tables[table]
    res = db.query(f'SELECT COUNT(1) as n FROM {table}')
    assert res[0]['n'] >= n


nodes = {
    "2178": "US",
    }


@pytest.mark.parametrize("node", nodes.keys())
def test_node_countries(node):
    db = DB()
    iso = nodes[node]
    sql = "SELECT iso FROM sensor_nodes n JOIN countries c ON (c.countries_id = n.countries_id) WHERE n.sensor_nodes_id = :node;"
    res = db.query(sql, dict(node = int(node)))
    assert res[0]['iso'] == iso
