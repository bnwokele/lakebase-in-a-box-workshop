import os
import logging
import psycopg
from psycopg_pool import ConnectionPool
from server.config import get_workspace_client

logger = logging.getLogger(__name__)

LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "ecommerce")

w = get_workspace_client()


class OAuthConnection(psycopg.Connection):
    @classmethod
    def connect(cls, conninfo="", **kwargs):
        endpoint_name = os.environ["ENDPOINT_NAME"]
        logger.info(f"Generating DB credential for endpoint: {endpoint_name}")
        try:
            credential = w.postgres.generate_database_credential(endpoint=endpoint_name)
            logger.info(f"Credential generated, expires: {credential.expire_time}")
            kwargs["password"] = credential.token
        except Exception as e:
            logger.error(f"Failed to generate DB credential: {e}")
            raise
        try:
            conn = super().connect(conninfo, **kwargs)
            logger.info("Database connection established")
            return conn
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise


username = os.environ.get("PGUSER", "")
host = os.environ.get("PGHOST", "")
port = os.environ.get("PGPORT", "5432")
database = os.environ.get("PGDATABASE", "databricks_postgres")
sslmode = os.environ.get("PGSSLMODE", "require")

pool = ConnectionPool(
    conninfo=f"dbname={database} user={username} host={host} port={port} sslmode={sslmode} connect_timeout=15",
    connection_class=OAuthConnection,
    min_size=1,
    max_size=10,
    max_lifetime=2700,
    open=False,
)


def get_branch_connection(branch_id: str) -> psycopg.Connection:
    """Get a direct connection to a specific branch endpoint."""
    branch_full = f"projects/{LAKEBASE_PROJECT}/branches/{branch_id}"
    endpoints = list(w.postgres.list_endpoints(parent=branch_full))
    if not endpoints:
        raise Exception(f"No endpoint found for branch '{branch_id}'")
    branch_host = endpoints[0].status.hosts.host
    endpoint_name = endpoints[0].name
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    return psycopg.connect(
        dbname=database,
        user=username,
        password=cred.token,
        host=branch_host,
        port=int(port),
        sslmode=sslmode,
        autocommit=True,
        connect_timeout=15,
    )
