from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Sequence

import logging
import os

import pandas.io.json
import pyarrow
import requests
from harlequin import HarlequinAdapter, HarlequinCursor, HarlequinConnection
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError, HarlequinError
from harlequin.options import HarlequinAdapterOption
from textual_fastdatatable.backend import AutoBackendType
from wherobots.db import Connection, Cursor, connect, connect_direct, Runtime, Region
from wherobots.db.constants import DEFAULT_ENDPOINT
from wherobots.db.errors import DatabaseError

from .cli_options import WHEROBOTS_ADAPTER_OPTIONS

# Setup logging if requested
_log_file = os.getenv("WHEROBOTS_HARLEQUIN_ADAPTER_LOG")
_log_debug = os.getenv("WHEROBOTS_HARLEQUIN_ADAPTER_DEBUG")
if _log_file:
    logging.basicConfig(
        filename=_log_file,
        level=logging.DEBUG if _log_debug == "1" else logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)25s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class HarlequinWherobotsCursor(HarlequinCursor):
    def __init__(self, cursor: Cursor) -> None:
        self.cursor = cursor
        self.results = None
        self.schema = None

    def columns(self) -> list[tuple[str, str]]:
        if self.schema is None:
            return []

        fields = self.schema["fields"]
        primary_key = self.schema["primaryKey"]
        return [
            (field["name"], field["type"])
            for field in fields
            if field["name"] not in primary_key
        ]

    def set_limit(self, limit: int) -> HarlequinCursor:
        return self

    def fetchall(self) -> AutoBackendType | None:
        if self.results is None:
            try:
                self.results = self.cursor.fetchall()
                self.schema = pandas.io.json.build_table_schema(self.results)
                self.cursor.close()
                self.cursor = None
            except DatabaseError as e:
                raise HarlequinQueryError(f"Query error: {e}") from e

        return pyarrow.Table.from_pandas(self.results)

    def close(self) -> None:
        if self.cursor is not None:
            self.cursor.close()


class HarlequinWherobotsConnection(HarlequinConnection):
    def __init__(
        self,
        host: str,
        token: str | None = None,
        api_key: str | None = None,
        runtime: str | None = None,
        region: str | None = None,
        ws_url: str | None = None,
        init_message: str = "",
    ) -> None:
        self.init_message = init_message
        self.conn = None
        self.cursors = set()

        self.host = host
        self.token = token
        self.api_key = api_key
        self.runtime = Runtime[runtime] if runtime else None
        self.region = Region[region] if region else None
        self.ws_url = ws_url

        self.headers: dict[str, str] = {}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"
        elif api_key:
            self.headers["X-API-Key"] = api_key

        if self.ws_url:
            self.conn: Connection = connect_direct(
                uri=self.ws_url,
                headers=self.headers,
            )
        else:
            self.conn: Connection = connect(
                host=self.host,
                token=self.token,
                api_key=self.api_key,
                runtime=self.runtime,
                region=self.region,
            )

    def execute(self, query: str) -> HarlequinCursor | None:
        cursor: Cursor = self.conn.cursor()
        cursor.execute(query)
        hc = HarlequinWherobotsCursor(cursor)
        self.cursors.add(hc)
        return hc

    def cancel(self) -> None:
        for cursor in self.cursors:
            cursor.close()

    def get_catalog(self) -> Catalog:
        try:
            response = requests.get(
                f"https://{self.host}/catalog/hierarchy",
                headers=self.headers
            )
            response.raise_for_status()
        except Exception as e:
            raise HarlequinError("Error reading catalog information from Wherobots") from e

        executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="wherobots-catalog-fetcher")
        try:
            items = self.__build_catalog(response.json(), executor)
            return Catalog(items)
        except Exception as e:
            raise HarlequinError("Invalid catalog data!") from e
        finally:
            executor.shutdown(wait=True)

    def __build_catalog(self, response, executor: ThreadPoolExecutor):
        items: list[CatalogItem] = []
        tasks: list[Future] = []
        for catalog in response["catalogs"]:
            dbs: list[CatalogItem] = []
            for db in catalog["databases"]:
                tables: list[CatalogItem] = []
                for table in db["tables"]:
                    children = []
                    tables.append(
                        CatalogItem(
                            qualified_identifier=f"{catalog['name']}.{db['name']}.{table['name']}",
                            query_name=f"{catalog['name']}.{db['name']}.{table['name']}",
                            label=table["name"],
                            type_label="table",
                            children=children,
                        )
                    )
                    task: Future = executor.submit(
                        self.__get_table_schema,
                        catalog["extId"],
                        catalog["name"],
                        db["name"],
                        table["name"],
                        children,
                    )
                    tasks.append(task)

                dbs.append(
                    CatalogItem(
                        qualified_identifier=f"{catalog['name']}.{db['name']}",
                        query_name=f"{catalog['name']}.{db['name']}",
                        label=db["name"],
                        type_label="db",
                        children=tables,
                    )
                )
            items.append(
                CatalogItem(
                    qualified_identifier=catalog["name"],
                    query_name=catalog["name"],
                    label=catalog["name"],
                    type_label="catalog",
                    children=dbs,
                )
            )

        wait(tasks)
        return items

    def __get_table_schema(self, catalog_id, catalog, db, table, into):
        """Get the schema for a table and add it to the table's CatalogItem."""
        logging.debug("Getting schema for %s.%s.%s ...", catalog, db, table)
        response = requests.get(
            f"https://{self.host}/catalog/{catalog_id}/databases/{db}/tables/{table}",
            headers=self.headers,
        )
        if response.status_code != 200:
            # Ignore errors getting table schemas.
            return

        into += [
            CatalogItem(
                qualified_identifier=f"{catalog}.{db}.{table}.{field['name']}",
                query_name=f"{field['name']}",
                label=field["name"],
                type_label=field["type"],
            )
            for field in response.json()["schema"]
        ]

    def close(self):
        if self.conn:
            logging.info("Closing connection to Wherobots ...")
            self.conn.close()


class HarlequinWherobotsAdapter(HarlequinAdapter):
    """Harlequin adapter for Wherobots DB, using the Wherobots Spatial SQL API.

    `conn_str` is expected to contain a single string, the base domain of the target Wherobots stack.
    This is typically "cloud.wherobots.com".

    One of `token` or `api_key` must be provided.
    If a known SQL session already exists, `ws_url` can be provided to establish a direct connection to it.
    """

    ADAPTER_OPTIONS: list[HarlequinAdapterOption] | None = WHEROBOTS_ADAPTER_OPTIONS
    IMPLEMENTS_CANCEL = True

    def __init__(
        self,
        conn_str: Sequence[str],
        token: str | None = None,
        api_key: str | None = None,
        runtime: str | None = None,
        region: str | None = None,
        ws_url: str | None = None,
    ) -> None:
        self.conn_str = conn_str
        self.token = token
        self.api_key = api_key
        self.runtime = runtime
        self.region = region
        self.ws_url = ws_url

    def connect(self) -> HarlequinConnection:
        """Establish a connection to the Wherobots.

        If no connection string is provided, DEFAULT_ENDPOINT is used.
        """
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide more than one connection string for the Wherobots adapter."
            )

        # If no connection string is provided, use the driver's default endpoint
        host = f"api.{self.conn_str[0]}" if self.conn_str else DEFAULT_ENDPOINT

        try:
            return HarlequinWherobotsConnection(
                host=host,
                token=self.token,
                api_key=self.api_key,
                runtime=self.runtime,
                region=self.region,
                ws_url=self.ws_url,
            )
        except Exception as e:
            logging.exception(e)
            raise HarlequinConnectionError(f"Failed to connect to Wherobots: {e}") from e
