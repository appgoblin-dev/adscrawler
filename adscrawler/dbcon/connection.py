import asyncio
import socket
import threading
from contextlib import contextmanager
from socket import gethostbyname

import asyncssh
import sqlalchemy
from sqlalchemy.engine import Engine

from adscrawler.config import CONFIG, SSH_KNOWN_HOSTS, get_logger

logger = get_logger(__name__)


class PostgresCon:
    """Class for managing the connection to PostgreSQL with extended database operations."""

    def __init__(self, db_name: str, db_ip: str, db_port: str) -> None:
        """
        Initialize the PostgreSQL connection.
        Args:
            db_name (str): Name of the database.
            db_ip (str): IP address of the database server.
            db_port (str): Port number of the database server.
        """
        self.db_name = db_name
        self.db_ip = db_ip
        self.db_port = db_port
        self.engine: Engine

        try:
            self.db_pass = CONFIG[self.db_name]["db_password"]
            self.db_user = CONFIG[self.db_name]["db_user"]
        except KeyError as error:
            logger.error(f"Loading db_auth for {self.db_name}, error: {error}")
            raise

    def set_engine(self) -> None:
        """Set up the SQLAlchemy engine."""
        try:
            db_userpass = (
                f"{self.db_user}:{self.db_pass}" if self.db_pass else self.db_user
            )
            db_login = f"postgresql+psycopg://{db_userpass}"
            db_uri = f"{db_login}@{self.db_ip}:{self.db_port}/{self.db_name}"
            logger.info(f"Adscrawler connecting to PostgreSQL {self.db_name}")
            self.engine = sqlalchemy.create_engine(
                db_uri,
                connect_args={"connect_timeout": 10, "application_name": "adscrawler"},
            )
        except Exception as error:
            logger.error(
                f"Failed to connect {self.db_name} @ {self.db_ip}, error: {error}",
            )
            raise

    @contextmanager
    def get_cursor(self) -> None:
        """Context manager for database connection and cursor."""
        conn = self.engine.raw_connection()
        try:
            cursor = conn.cursor()
            yield cursor
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database operation error: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()


def manage_tunnel_thread(
    host: str,
    os_user: str,
    ssh_port: int,
    remote_port: int,
    ssh_pkey: str,
    ssh_pkey_password: str,
) -> int:
    result = {}

    def runner():
        async def main():
            async with asyncssh.connect(
                host,
                username=os_user,
                port=ssh_port,
                known_hosts=SSH_KNOWN_HOSTS.as_posix(),
                client_keys=ssh_pkey,
                passphrase=ssh_pkey_password,
                family=socket.AF_INET,
            ) as conn:
                listener = await conn.forward_local_port(
                    listen_host="localhost",
                    listen_port=0,
                    dest_host="127.0.0.1",
                    dest_port=remote_port,
                )
                result["port"] = listener.get_port()
                await listener.wait_closed()

        asyncio.run(main())

    t = threading.Thread(target=runner, daemon=True)
    t.start()
    while "port" not in result:
        pass  # wait for listener to be ready
    return result["port"]


def start_ssh_tunnel(server_name: str) -> int:
    ssh_port = CONFIG[server_name].get("ssh_port", 22)
    remote_port = CONFIG[server_name].get("remote_port", 5432)
    host = CONFIG[server_name]["host"]
    # host = get_host_ip(CONFIG[server_name]["host"])
    os_user = CONFIG[server_name]["os_user"]
    ssh_pkey = CONFIG[server_name].get("ssh_pkey")
    ssh_pkey_password = CONFIG[server_name].get("ssh_pkey_password")
    ssh_local_port = manage_tunnel_thread(
        host, os_user, ssh_port, remote_port, ssh_pkey, ssh_pkey_password
    )
    return ssh_local_port


def get_db_connection(
    use_ssh_tunnel: bool = False, server_name: str = "madrone"
) -> PostgresCon:
    """
    Get a database connection, optionally using an SSH tunnel.

    Args:
        use_ssh_tunnel (bool): Whether to use an SSH tunnel for the connection.

    Returns:
        PostgresCon: A PostgreSQL connection object.
    """
    host = CONFIG[server_name]["host"]

    if use_ssh_tunnel:
        ssh_local_port = start_ssh_tunnel(server_name)
        host = "127.0.0.1"
        db_port = str(ssh_local_port)
    else:
        db_port = "5432"
    conn = PostgresCon(server_name, host, db_port)
    conn.set_engine()
    return conn


def get_host_ip(hostname: str) -> str:
    """Convert hostname to IPv4 address if needed."""
    # Check if hostname is already an IPv4 address
    if all(part.isdigit() and 0 <= int(part) <= 255 for part in hostname.split(".")):  # noqa: PLR2004
        return hostname
    ip_address = gethostbyname(hostname)
    logger.info(f"Resolved {hostname} to {ip_address}")
    return ip_address
