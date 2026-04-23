#from migra import Migration
from results import db
from settings import settings
from pprint import pprint
from datetime import date

production_url = settings.REMOTE_DATABASE_URL
target_url = settings.DATABASE_WRITE_URL

today = date.today().strftime('%Y%m%d')
b = db(production_url)
a = db(target_url)

schemas = [
    'public',
    'logs',
    'fetcher'
    ]


## get diff from db
diff = {}
for schema in schemas:
    diff[schema] = b.schemadiff_as_statements(a, schema=schema)

## write out the statements
with open(f"diff_{today}.sql", "w") as f:
    for (schema, statements) in diff.items():
        print(f"printing {schema} - {len(statements)} statements")
        f.write(f"----------------\n{schema}\n")
        for statement in statements:
            f.write(f"----------------\n{statement}\n")
