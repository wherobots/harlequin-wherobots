from typing import Sequence

import pyarrow
import requests
from harlequin import HarlequinAdapter, HarlequinCursor, HarlequinConnection
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError, HarlequinQueryError
from harlequin.options import HarlequinAdapterOption
from textual_fastdatatable.backend import AutoBackendType
from wherobots.db import Connection, Cursor, connect, connect_direct
from wherobots.db.errors import DatabaseError, InterfaceError

from .options import WHEROBOTS_ADAPTER_OPTIONS

DEFAULT_ENDPOINT = "cloud.wherobots.com"


class HarlequinWherobotsCursor(HarlequinCursor):
    def __init__(self, cursor: Cursor) -> None:
        self.cursor = cursor
        try:
            self.results = cursor.fetchall()
        except DatabaseError as e:
            raise HarlequinQueryError("Query error") from e

    def columns(self) -> list[tuple[str, str]]:
        return [(str(col), "") for col in self.results.columns]

    def set_limit(self, limit: int) -> "HarlequinCursor":
        pass

    def fetchall(self) -> AutoBackendType | None:
        return pyarrow.Table.from_pandas(self.results)


class HarlequinWherobotsConnection(HarlequinConnection):
    def __init__(
        self,
        host: str,
        token: str | None = None,
        ws_url: str | None = None,
        api_key: str | None = None,
        init_message: str = "",
    ) -> None:
        self.init_message: str = init_message
        self.host: str = host
        self.ws_url: str | None = ws_url
        self.token: str | None = token
        self.api_key: str | None = api_key

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
                host=f"api.{self.host}",
                token=self.token,
                api_key=self.api_key,
            )

    def execute(self, query: str) -> HarlequinCursor | None:
        cursor: Cursor = self.conn.cursor()
        cursor.execute(query)
        return HarlequinWherobotsCursor(cursor)

    def get_catalog(self) -> Catalog:
        response = requests.get(
            f"https://catalog.{self.host}/catalog/hierarchy", headers=self.headers
        )
        response.raise_for_status()

        items: list[CatalogItem] = []
        response_json = response.json()
        for catalog in response_json["catalogs"]:
            dbs: list[CatalogItem] = []
            for db in catalog["databases"]:
                tables: list[CatalogItem] = []
                for table in db["tables"]:
                    tables.append(
                        CatalogItem(
                            qualified_identifier=f"{catalog['name']}.{db['name']}.{table['name']}",
                            query_name=f"{catalog['name']}.{db['name']}.{table['name']}",
                            label=table["name"],
                            type_label="table",
                        )
                    )
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
        return Catalog(items)


class HarlequinWherobotsAdapter(HarlequinAdapter):
    """Harlequin adapter for Wherobots DB, using the Wherobots Spatial SQL API.

    `conn_str` is expected to contain a single string, the base domain of the target Wherobots stack.
    This is typically "cloud.wherobots.com".

    One of `token` or `api_key` must be provided.
    If a known SQL session already exists, `ws_url` can be provided to establish a direct connection to it.
    """

    ADAPTER_OPTIONS: list[HarlequinAdapterOption] | None = WHEROBOTS_ADAPTER_OPTIONS

    def __init__(
        self,
        conn_str: Sequence[str],
        token: str | None = None,
        api_key: str | None = None,
        ws_url: str | None = None,
    ) -> None:
        self.conn_str = conn_str
        self.token = token
        self.api_key = api_key
        self.ws_url = ws_url

    def connect(self) -> HarlequinConnection:
        """Establish a connection to the Wherobots.

        If no connection string is provided, DEFAULT_ENDPOINT is used.
        """
        host = DEFAULT_ENDPOINT
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide more than one connection string for the Wherobots adapter."
            )
        if self.conn_str:
            host = self.conn_str[0]
        try:
            return HarlequinWherobotsConnection(
                host=host,
                ws_url=self.ws_url,
                token=self.token,
                api_key=self.api_key,
            )
        except Exception as e:
            raise HarlequinConnectionError("Failed to connect to Wherobots") from e
