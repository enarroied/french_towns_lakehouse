# utils/db.py
import duckdb


class DuckDBConnection:
    _instance = None

    @classmethod
    def get(cls):
        if cls._instance is None:
            cls._instance = duckdb.connect()
            cls._instance.execute("INSTALL spatial; LOAD spatial;")
        return cls._instance

    @classmethod
    def close(cls):
        if cls._instance:
            cls._instance.close()
            cls._instance = None
