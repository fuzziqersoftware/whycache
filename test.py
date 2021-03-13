import os
import sys
import asyncio
import traceback
import shutil

from typing import Callable

from WhycacheInstance import WhycacheInstance
from FileWhycacheInstance import FileWhycacheInstance
from MySQLWhycacheInstance import MySQLWhycacheInstance



all_test_cases = []

def testcase_file(fn):
  assert fn.__name__.startswith('test_')

  async def new_fn():
    os.mkdir('test_data')
    pc = FileWhycacheInstance('test', 'test_data')
    await pc.delete_all_history()
    await pc.update()
    await fn(pc)
    shutil.rmtree('test_data')
  new_fn.__name__ = '@testcase FileWhycacheInstance ' + fn.__name__
  all_test_cases.append(new_fn)

def testcase_mysql(fn):
  assert fn.__name__.startswith('test_')

  async def new_fn():
    pc = MySQLWhycacheInstance('test', 'whycache', '127.0.0.1', 3306, 'root', 'root', 'whycache_test')
    await pc.delete_all_history()
    await pc.update()
    await fn(pc)
  new_fn.__name__ = '@testcase MySQLWhycacheInstance ' + fn.__name__
  all_test_cases.append(new_fn)

  return fn

class AssertRaises:
  def __init__(self, exc_type):
    self.expected_exc_type = exc_type

  def __enter__(self):
    pass

  def __exit__(self, t, v, tb):
    assert t is self.expected_exc_type, f'expected exception of type {self.expected_exc_type} but received exception of type {t}'
    return True



@testcase_file
@testcase_mysql
async def test_get_missing_key(pc: WhycacheInstance):
  with AssertRaises(KeyError):
    pc.get(b'missing_key')

@testcase_file
@testcase_mysql
async def test_get_after_set(pc: WhycacheInstance):
  await pc.set(b'key1', 'value1')
  await pc.set(b'key2', 2)
  await pc.set(b'key3', None)
  await pc.set(b'key4', (1, False, b'true'))
  assert pc.get(b'key1') == 'value1'
  assert pc.get(b'key2') == 2
  assert pc.get(b'key3') is None
  assert pc.get(b'key4') == (1, False, b'true')

@testcase_file
@testcase_mysql
async def test_get_after_delete(pc: WhycacheInstance):
  await pc.set(b'key1', 'value1')
  await pc.set(b'key2', 2)
  assert pc.get(b'key1') == 'value1'
  assert pc.get(b'key2') == 2

  await pc.delete(b'key2')
  assert pc.get(b'key1') == 'value1'
  with AssertRaises(KeyError):
    pc.get(b'key2')

async def test_get_shared_meta(pc: WhycacheInstance, create_instance: Callable[[], WhycacheInstance]):
  await pc.set(b'key1', 'value1')
  await pc.set(b'key2', 2)
  assert pc.get(b'key1') == 'value1'
  assert pc.get(b'key2') == 2

  update_id = pc.get_shared_state_update_id()

  # Another whycache instance using the same shared state should be able to see
  # the values without calling update()
  other_pc = create_instance()
  assert other_pc.get_shared_state_update_id() == update_id
  assert other_pc.get(b'key1') == 'value1'
  assert other_pc.get(b'key2') == 2

  # Changes should be visible immediately since the two instances use the same
  # shared state
  await other_pc.set(b'key1', 'value1111')
  assert pc.get(b'key1') == 'value1111'

@testcase_file
async def test_get_shared_file(pc: WhycacheInstance):
  await test_get_shared_meta(pc, lambda: FileWhycacheInstance(pc.instance_name, pc.data_directory))

@testcase_mysql
async def test_get_shared_mysql(pc: WhycacheInstance):
  await test_get_shared_meta(pc, lambda: MySQLWhycacheInstance(pc.instance_name, pc.table_name, pc.mysql_host, pc.mysql_port, pc.mysql_user, pc.mysql_password, pc.mysql_database))

@testcase_file
@testcase_mysql
async def test_recreate_shared_state(pc: WhycacheInstance):
  assert pc.valid()

  await pc.set(b'key1', 'value1')
  await pc.set(b'key2', 2)
  assert pc.get(b'key1') == 'value1'
  assert pc.get(b'key2') == 2

  assert pc.valid()
  pc.delete_shared_state()
  assert not pc.valid()

  await pc.update()
  assert pc.valid()

  assert pc.get(b'key1') == 'value1'
  assert pc.get(b'key2') == 2

@testcase_file
@testcase_mysql
async def test_iterators(pc: WhycacheInstance):
  await pc.set(b'key1', 1)
  await pc.set(b'key3', 3)
  await pc.set(b'key5', 5)

  assert list(pc.keys()) == [b'key1', b'key3', b'key5']
  it = pc.keys()
  assert next(it) == b'key1'
  await pc.set(b'key2', 2)  # invisible since key3 was already found by iterator
  await pc.set(b'key4', 4)  # visible since iterator hasn't passed it yet
  assert next(it) == b'key3'
  assert next(it) == b'key4'
  assert next(it) == b'key5'
  with AssertRaises(StopIteration):
    next(it)
  await pc.delete_multi([b'key2', b'key4'])

  assert list(pc.values()) == [1, 3, 5]
  it = pc.values()
  assert next(it) == 1
  await pc.set(b'key2', 2)  # invisible since key3 was already found by iterator
  await pc.set(b'key4', 4)  # visible since iterator hasn't passed it yet
  assert next(it) == 3
  assert next(it) == 4
  assert next(it) == 5
  with AssertRaises(StopIteration):
    next(it)
  await pc.delete_multi([b'key2', b'key4'])

  assert list(pc.items()) == [(b'key1', 1), (b'key3', 3), (b'key5', 5)]
  it = pc.items()
  assert next(it) == (b'key1', 1)
  await pc.set(b'key2', 2)  # invisible since key3 was already found by iterator
  await pc.set(b'key4', 4)  # visible since iterator hasn't passed it yet
  assert next(it) == (b'key3', 3)
  assert next(it) == (b'key4', 4)
  assert next(it) == (b'key5', 5)
  with AssertRaises(StopIteration):
    next(it)
  await pc.delete_multi([b'key2', b'key4'])

@testcase_file
@testcase_mysql
async def test_value_too_large(pc: WhycacheInstance):
  with AssertRaises(ValueError):
    data = b'*' * (1024 * 1024)
    await pc.set(b'key1', data)

@testcase_file
@testcase_mysql
async def test_set_multi_large_values(pc: WhycacheInstance):
  data = b'*' * 128 * 1024
  items = {(b'key%d' % x): data for x in range(20)}
  await pc.set_multi(items)
  for x in range(20):
    assert pc.get(b'key%d' % x) == data

@testcase_file
@testcase_mysql
async def test_set_multi_single_excessive_value(pc: WhycacheInstance):
  # set_multi should not write any of the values passed to it if even if only
  # one is oversize
  data = b'*' * 128 * 1024
  items = {(b'key%d' % x): data for x in range(20)}
  items[b'largekey'] = data * 8
  with AssertRaises(ValueError):
    await pc.set_multi(items)
  for x in range(20):
    with AssertRaises(KeyError):
      pc.get(b'key%d' % x)
  with AssertRaises(KeyError):
    pc.get(b'largekey')

@testcase_file
@testcase_mysql
async def test_set_empty_key(pc: WhycacheInstance):
  with AssertRaises(ValueError):
    await pc.set(b'', 4)
  with AssertRaises(ValueError):
    await pc.set_multi({b'': 4, b'key1': 5})



async def main() -> int:
  if os.path.exists('test_data'):
    shutil.rmtree('test_data')
  for fn in all_test_cases:
    print(fn.__name__, '...')
    try:
      await fn()
      print(fn.__name__, 'PASS')
    except Exception:
      print(fn.__name__, 'FAIL')
      raise

if __name__ == '__main__':
  sys.exit(asyncio.get_event_loop().run_until_complete(main()))
