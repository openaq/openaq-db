import psycopg
import time
import logging

from settings import settings
from psycopg.rows import dict_row
from psycopg.types.json import Json

logger = logging.getLogger(__name__)

class DB:
    def __init__(self, keep_open: bool = False, rollback: bool = False):
        self.conn = None
        self.keep_open = keep_open
        self.rollback = rollback

    def get_connection(self):
        cstring = settings.DATABASE_READ_URL
        conn = psycopg.connect(cstring, row_factory=dict_row)
        return conn

    def query(
        self,
        query: str,
        params: dict = {},
        keep_open: bool = False,
        rollback: bool = False,
    ):
        start = time.time()
        data = []
        fields = {}
        n = None
        for key in params.keys():
            query = query.replace(f":{key}", f"%({key})s")
            if isinstance(params[key], dict):
                params[key] = Json(params[key])

        if self.conn is None or self.conn.closed:
            self.conn = self.get_connection()

        cur = self.conn.cursor()

        try:
            logger.debug(f"query:\n{query}\nparameters: {params}")
            cur.execute(query, params)
            n = cur.rowcount
            logger.info("query executed: seconds: %0.4f, results: %s", time.time() - start, n)

            data = cur.fetchall()
            fields = [desc[0] for desc in cur.description]

            dur = time.time() - start
            logger.info("query fetched: seconds: %0.4f", dur)
            if n > 0:
                if isinstance(data, list):
                    logger.debug(f'First row: {data[0]}')
                else:
                    logger.debug(f'Value: {data}')

        except Exception as e:
            self.conn.rollback()
            self.conn.close()
            logger.warning(f"Query error: {e}")
            raise ValueError(f"{e}") from None

        if not (keep_open or self.keep_open):
            if rollback or self.rollback:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

        return data
