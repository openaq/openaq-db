#from migra import Migration
from results import db
from settings import settings
from pprint import pprint
from datetime import date

production_url = settings.REMOTE_DATABASE_URL
target_url = settings.DATABASE_WRITE_URL

today = date.today().strftime('%Y%m%d')
a = db(production_url)
b = db(target_url)

public = b.schemadiff_as_statements(a, schema='public')
logs = b.schemadiff_as_statements(a, schema='logs')
deployments = b.schemadiff_as_statements(a, schema='deployments')

with open(f"diff_{today}.sql", "w") as f:
    for statement in logs:
        f.write(f"----------------\n{statement}\n")
    for statement in public:
        f.write(f"----------------\n{statement}\n")
    for statement in deployments:
        f.write(f"----------------\n{statement}\n")
