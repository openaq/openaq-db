from migra import Migration
from settings import settings
from pprint import pprint
from sqlbag import S
from datetime import date

from_url = settings.REMOTE_DATABASE_URL
target_url = settings.DATABASE_WRITE_URL

today = date.today().strftime('%Y%m%d')

with S(from_url) as ac0, S(target_url) as ac1:
    m = Migration(
        ac0,
        ac1,
        schema='public',
    )
    # turn safety off to allow drops
    m.set_safety(False)

    m.add_all_changes(privileges=False)

    sql = m.sql.encode('utf8')
    with open(f'migra/patches/patch_{today}_chk.sql', 'w') as f:
        for statement in m.statements:
            f.write(f"----------------\n{statement}\n")
