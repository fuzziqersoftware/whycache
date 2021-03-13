import aiomysql
import collections
import os
import re

from typing import Optional, Tuple, List, Dict, Any
from WhycacheInstance import WhycacheInstance


# This file serves as a template for integrating whycache into a MySQL-based
# application architecture.
#
# To use this class, you must first create a MySQL table with this schema (you
# can use a different table name if you pass it to the class constructor):
# CREATE TABLE `whycache` (
#   id BIGINT NOT NULL AUTO_INCREMENT,  -- update id
#   single_key VARBINARY(1024),  -- NULL if the update contains multiple keys
#   contents MEDIUMBLOB NOT NULL,  -- serialized update contents
#   PRIMARY KEY (id),
#   UNIQUE KEY (single_key)
# ) ENGINE=InnoDB AUTO_INCREMENT=1;
# You may want to use a different BLOB type for the contents field, depending on
# max_update_size (if you override it from the base class). The abstraction will
# never attempt to write more than max_update_size bytes to this field.


class MySQLWhycacheInstance(WhycacheInstance):
  MySQLResult = collections.namedtuple(
      'MySQLResult',
      ['rows', 'lastrowid', 'rowcount'])

  def __init__(self,
    instance_name: str,
    table_name: str,
    mysql_host: str,
    mysql_port: int,
    mysql_user: str,
    mysql_password: str,
    mysql_database: str,
  ):
    super().__init__(instance_name)
    self.table_name = table_name
    self.mysql_host = mysql_host
    self.mysql_port = mysql_port
    self.mysql_user = mysql_user
    self.mysql_password = mysql_password
    self.mysql_database = mysql_database
    self.pool = None

  async def close(self):
    pool = self.pool
    self.pool = None
    pool.close()
    await pool.wait_closed()

  def __del__(self):
    if self.pool is not None:
      self.pool.close()


  async def _execute_query(self,
    sql: str,
    args: List[Any] = [],
  ) -> List[Dict[str, Any]]:
    """Runs a MySQL query and returns the result set. Opens a connection to the
    server if none exists already."""
    if self.pool is None:
      self.pool = await aiomysql.create_pool(
          host=self.mysql_host,
          port=self.mysql_port,
          user=self.mysql_user,
          password=self.mysql_password,
          db=self.mysql_database,
          autocommit=True)

    async with self.pool.acquire() as conn:
      async with conn.cursor(aiomysql.DictCursor) as cursor:
        await cursor.execute(sql, args)
        rows = await cursor.fetchall()
        return self.MySQLResult(
            rows=rows,
            lastrowid=cursor.lastrowid,
            rowcount=cursor.rowcount)

  async def _select_single(self,
    sql: str,
    args: List[Any] = [],
  ) -> Optional[Dict[str, Any]]:
    """Runs a query that is expected to return not more than one row. Returns
    None if no row is returned, or a dict if one row is returned. Raises if more
    than one row is returned."""
    result = await self._execute_query(sql, args)
    if len(result.rows) == 0:
      return None
    assert len(result.rows) == 1
    return result.rows[0]


  async def get_update_range_contents(self,
    update_id_low: int,
    update_id_high: int,
  ) -> List[Tuple[int, bytes]]:
    result = await self._execute_query(
        f'SELECT id, contents FROM {self.table_name} WHERE id >= %s AND id <= %s',
        [update_id_low, update_id_high])
    return [(row['id'], row['contents']) for row in result.rows]

  async def get_latest_update_id(self) -> int:
    row = await self._select_single(
        f'SELECT MAX(id) AS m FROM {self.table_name}')
    return 0 if ((row is None) or (row['m'] is None)) else row['m']

  async def commit_update(self,
    update_contents: bytes,
    single_key_name: Optional[bytes] = None,
  ) -> int:
    result = await self._execute_query(
        f'REPLACE INTO {self.table_name} (single_key, contents) VALUES (%s, %s)',
        [single_key_name, update_contents])
    return result.lastrowid

  async def overwrite_update(self, update_id: int, update_contents: bytes, single_key_name: Optional[bytes] = None) -> None:
    await self._execute_query(
        f'REPLACE INTO {self.table_name} (id, single_key, contents) VALUES (%s, %s, %s)',
        [update_id, single_key_name, update_contents])

  async def delete_updates_between(self, low_id: int, high_id: int) -> int:
    total_deleted = 0
    batch_deleted = 1
    while batch_deleted != 0:
      cursor = await self._execute_query(
          f'DELETE FROM {self.table_name} WHERE id >= %s AND id <= %s ORDER BY id ASC LIMIT 1000',
          [low_id, high_id])
      batch_deleted = cursor.rowcount
      total_deleted += batch_deleted
    return total_deleted
