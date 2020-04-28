import pytest
from database import load_configure, create_conn_str, create_engine, create_tables


def test_readable_load_configure():
    conf = load_configure()
    assert conf != {}
    assert conf['db'] is not None
    assert conf['db']['driver'] == 'psycopg2'


def test_connectable_database():
    conf = load_configure()
    conn_str = create_conn_str(conf)
    try:
        engine = create_engine(conn_str)
        result = create_tables(engine)
        assert result == True
    except Exception:
        pytest.fail("exception raised while executing create_engine() method.")

    

