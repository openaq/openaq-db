from migra import Migration
from settings import settings
from pprint import pprint

from sqlbag import S

from_url = settings.REMOTE_DATABASE_URL
target_url = settings.DATABASE_WRITE_URL

with S(from_url) as ac0, S(target_url) as ac1:
    m = Migration(
        ac0,
        ac1,
        schema='public',
    )
    # turn safety off to allow drops
    m.set_safety(False)
    #m.add_all_changes(privileges=True)
    #m.add(m.changes.selectables())
    m.add(m.changes.tables_only_selectables())
    sql = m.sql.encode('utf8')
    print(len(m.statements))
    for statement in m.statements:
        pprint(statement)
        #print(str(sql))
