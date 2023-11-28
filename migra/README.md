# Schema updates

There are two ways to compare one database to another:

* Command line tool that produces one migration file which you can review
* The python script which allows you to pick and choose what to compare

## Installing
Both methods require that you install the requirements, ideally in a virtual environment.
```python
python3 -m venv env
source env/bin/activate
python3 -m pip install -r requirements.txt
```
At this point you should have both the migra executable and the packages installed.

## Command line executable
The easiest way to get a schema diff at this point is to run the following
```sh
migra postgresql://from_url \
      postgresql://target_url \
      --unsafe \
      --schema=public > migrate.sql
```
Where `--unsafe` may or may not be required based on whether the method will produce any drop statements.

## Python script
There is also a python script provided which will let you create a migration object and then let you play around with the lower level functions of migra. This method is more tedious to use but is more useful if you are just looking for specific differences.

Here is the `.env` file used for the following example. In it I am comparing a remote database (e.g. staging) to the one that I just built in docker, which represents the target schema.
```sh
# .env.docker
ENV=docker
DATABASE_WRITE_USER=postgres
DATABASE_WRITE_PASSWORD=postgres
DATABASE_HOST=172.17.0.2
DATABASE_PORT=5432
DATABASE_DB=postgres
# update url as needed
REMOTE_DATABASE_URL=postgresql://user:pwd@host:port/openaq
```

```sh
ENV=docker python3 compare.py
```

## Notes
* The migra tool identifies some queries as different even though they are not not structurally different, not sure why.
