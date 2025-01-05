# -*- coding: utf-8 -*-

from collections.abc import Awaitable
from contextlib import AbstractAsyncContextManager

from .utils import delegate_to_executor, proxy_property_directly, proxy_method_directly


class AsyncBase:
    def __init__(self, connection):
        self._connection = connection

    def __repr__(self):
        return super().__repr__() + " wrapping " + repr(self._connection)


class AiofilesContextManager(Awaitable, AbstractAsyncContextManager):
    """An adjusted async context manager for aiofiles."""

    __slots__ = ("_coro", "_obj")

    def __init__(self, coro):
        self._coro = coro
        self._obj = None

    def __await__(self):
        if self._obj is None:
            self._obj = yield from self._coro.__await__()
        return self._obj

    async def __aenter__(self):
        return await self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await get_running_loop().run_in_executor(
            None, self._obj._file.__exit__, exc_type, exc_val, exc_tb
        )
        self._obj = None


@delegate_to_executor(
    "append",
    "arrow",
    "begin",
    "checkpoint",
    "close",
    "commit",
    "create_function",
    "df",
    "execute",
    "executemany",
    "extract_statements",
    "fetchall",
    "fetchdf",
    "fetchmany",
    "fetchnumpy",
    "fetchone",
    "fetch_arrow_table",
    "fetch_df",
    "fetch_df_chunk",
    "fetch_record_batch",
    "from_arrow",
    "from_csv_auto",
    "from_df",
    "from_parquet",
    "from_query",
    "from_substrait",
    "from_substrait_json",
    "get_substrait",
    "get_substrait_json",
    "get_table_names",
    "install_extension",
    "interrupt",
    "list_filesystems",
    "list_type",
    "load_extension",
    "pl",
    "query",
    "read_csv",
    "read_json",
    "read_parquet",
    "register",
    "register_filesystem",
    "remove_function",
    "rollback",
    "sql",
    "table",
    "table_function",
    "tf",
    "torch",
    "unregister",
    "unregister_filesystem",
    "values",
    "view",
)
@proxy_method_directly(
    "array_type",
    "decimal_type",
    "dtype",
    "enum_type",
    "filesystem_is_registered",
    "map_type",
    "row_type",
    "sqltype",
    "string_type",
    "struct_type",
    "type",
    "union_type",
)
@proxy_property_directly("description", "rowcount")
class DuckDBConnectionWrapper(AsyncBase):
    """The asyncio executor version of duckdb."""
