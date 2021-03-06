import collections
import sharedstructures
import pickle
import struct
import os

from abc import ABC, abstractmethod
from typing import Iterable, Iterator, Any, Tuple, Optional, Dict, List, AsyncGenerator, Generator, Callable


class WhycacheInstance(ABC):
  max_update_size = 1024 * 1024  # 1MB
  fast_forward_threshold = 200

  def __init__(self, instance_name: str):
    self.instance_name = instance_name
    self.shared_state = None


  def close_shared_state(self) -> None:
    """Closes the shared state file used by this whycache instance.

    When no process has the shared state file open, it is safe to copy to
    another machine."""
    self.shared_state = None

  def delete_shared_state(self) -> None:
    """Closes and deletes the shared state file used by this whycache instance.

    If other processes still have the file open, they will continue to use it;
    it will not actually be deleted from disk until all processes close it."""
    try:
      os.remove(self.shared_state_filename())
    except FileNotFoundError:
      pass  # We're deleting it; we don't care if it already doesn't exist
    self.close_shared_state()

  def get_shared_state(self) -> sharedstructures.PrefixTree:
    """Returns the shared state object, opening it if necessary."""
    if self.shared_state is None:
      self.shared_state = sharedstructures.PrefixTree(self.shared_state_filename(), 'logarithmic', 0)
    return self.shared_state

  @abstractmethod
  async def get_update_range_contents(self,
    update_id_low: int,
    update_id_high: int,
  ) -> AsyncGenerator[Tuple[int, bytes], None]:
    """Async generator that yields (update_id, update_contents) tuples
    representing the updates in the range between update_id_low and
    update_id_high, inclusive.

    update_content is the bytes object passed to commit_update when
    commit_update returned the corresponding update_id.

    This function must be overridden by a subclass."""
    raise NotImplementedError()

  @abstractmethod
  async def get_latest_update_id(self) -> int:
    """Returns the highest update_id ever returned by commit_update.

    This function must be overridden by a subclass."""
    raise NotImplementedError()

  @abstractmethod
  async def commit_update(self, update_contents: bytes, single_key_name: Optional[bytes] = None) -> int:
    """Writes the given bytes object to the external store, and returns a unique
    number identifying the update. The unique number returned will be greater
    than any other number this function has ever returned for this instance, and
    ideally would be only one greater than the previously-committed update.

    After this call, a call to get_latest_update_id() will return the number
    just returned by this function (until it is called again).

    This function must be overridden by a subclass."""
    raise NotImplementedError()

  @abstractmethod
  async def overwrite_update(self, update_id: int, update_contents: bytes, single_key_name: Optional[bytes] = None) -> None:
    """Writes the given bytes object to the external store, overwriting the
    update with update_id if it exists and creating it if it doesn't exist.

    This function is never to be called with an update_id greater than the value
    returned by get_latest_update_id(), so its return value should not be
    affected by a call to this function.

    This function must be overridden by a subclass."""
    raise NotImplementedError()

  @abstractmethod
  async def delete_updates_between(self,
    oldest_update_id: int,
    newest_update_id: int,
  ) -> int:
    """Deletes all updates between oldest_update_id and newest_update_id
    inclusive, and returns the number of updates deleted. Updates are deleted
    from the external store starting with the oldest.

    Updates are not necessarily deleted atomically. For example, if using MySQL,
    the deletes may be done with multiple queries without using a transaction.

    The number of updates deleted is only used for reporting purposes, so it is
    acceptable for an override function in a subclass to return an incorrect
    value here if the external store does not expose the number of items
    actually deleted by a delete query.

    This function must be overridden by a subclass."""
    raise NotImplementedError()


  def shared_state_filename(self) -> str:
    """Returns the full path to the shared state file."""
    return f'/tmp/whycache-{self.instance_name}.sspt'

  def encode_value(self, value: Any) -> bytes:
    """Encodes a Python object into a bytes object for the external store."""
    return pickle.dumps(value)

  def decode_value(self, value_bytes: bytes) -> Any:
    """Decodes a bytes object from the external store into a Python object."""
    return pickle.loads(value_bytes)


  async def delete_all_history(self):
    """Deletes all keys from the external store, and deletes the local shared
    state file.

    This function deletes all updates from the external store, but does not
    write any new events to the external store. Other instances using different
    shared state files (perhaps on other machines) may not notice that the
    history has been deleted! This method should generally only be used in
    tests."""
    update_id = await self.get_latest_update_id()
    await self.delete_updates_between(0, update_id)
    self.delete_shared_state()


  @staticmethod
  def _decode_update_contents(update_contents: bytes) -> Generator[Tuple[bytes, bytes], None, None]:
    item_count = struct.unpack('>L', update_contents[:4])[0]
    offset = 4
    for x in range(item_count):
      key_len, value_len = struct.unpack('>QQ', update_contents[offset : offset + 16])
      offset += 16
      key = update_contents[offset : offset + key_len]
      offset += key_len
      value_encoded = update_contents[offset : offset + value_len]
      offset += value_len
      yield (key, value_encoded)

  def _apply_update_local(self, update_id: int, update_contents: bytes) -> None:
    """Applies a serialized update to the local shared state.

    This function must never be called out of order. It must be called for every
    update fetched from the external store in sequence, never skipping any."""
    prev_update_id = self.get_shared_state()[b'']
    if update_id <= prev_update_id:
      return False

    for key, value_encoded in self._decode_update_contents(update_contents):
      if value_encoded == b'':
        cas_result = self.get_shared_state().check_and_set(b'', prev_update_id, key)
      else:
        value = self.decode_value(value_encoded)
        cas_result = self.get_shared_state().check_and_set(b'', prev_update_id, key, value)
      if not cas_result:
        return False

    return self.get_shared_state().check_and_set(b'', prev_update_id, b'', update_id)

  class _CommitBatch:
    """Internal class uses by _set_multi_encoded to split a large write into
    smaller batches."""
    header_bytes = 4  # uint32_t count

    def __init__(self):
      self.bytes = 4  # header: uint32_t count
      self.items = []
      self.last_key = None
      self.encoded = None

    def add_part(self, key: bytes, item: bytes) -> None:
      assert self.encoded is None
      self.items.append(item)
      self.bytes += len(item)
      self.last_key = key

    @staticmethod
    def encode_part(key: bytes, value_encoded: bytes) -> bytes:
      return struct.pack('>QQ', len(key), len(value_encoded)) + key + value_encoded

    def encode(self) -> bytes:
      if self.encoded is not None:
        return
      self.encoded = struct.pack('>L', len(self.items)) + b''.join(self.items)
      if len(self.items) != 1:
        self.last_key = None
      self.items = None

  async def _set_multi_encoded(self, items: Iterable[Tuple[bytes, bytes]]) -> Optional[int]:
    """Writes a batch of keys (or deletes) to the external store.

    This function does not update the shared state; the caller is expected to do
    so immediately after."""
    if not items:
      return None

    batches = [self._CommitBatch()]
    for key, value_encoded in items:
      if type(key) is not bytes:
        raise TypeError(f'key must be bytes; instead it is {type(key)}')
      if key == b'':
        raise ValueError('b\'\' is not a valid whycache key')

      part = self._CommitBatch.encode_part(key, value_encoded)

      batch = batches[-1]
      if batch.bytes + len(part) > self.max_update_size:
        batch.encode()
        batch = self._CommitBatch()
        batches.append(batch)

      if self._CommitBatch.header_bytes + len(part) > self.max_update_size:
        raise ValueError(f'part for key {key!r} is too large')

      batch.add_part(key, part)

    batches[-1].encode()

    last_commit_id = None
    for batch in batches:
      last_commit_id = await self.commit_update(batch.encoded, single_key_name=batch.last_key)

    return last_commit_id


  async def update(self, to_update_id: Optional[int] = None) -> int:
    """Reads new updates from the external store and applies them to the shared
    state."""
    if self.get_shared_state().check_missing_and_set(b'', b'', 0):
      local_update_id = 0
    else:
      local_update_id = self.get_shared_state()[b'']
    global_update_id = to_update_id if to_update_id is not None else (await self.get_latest_update_id())
    assert global_update_id >= local_update_id

    if global_update_id == local_update_id:
      return

    async for update_id, update_contents in self.get_update_range_contents(local_update_id + 1, global_update_id):
      self._apply_update_local(update_id, update_contents)

  def valid(self) -> bool:
    """Returns true if the shared state appears to be valid.

    If it is not valid, update() needs to be called."""
    try:
      self.get_shared_state()[b'']
      return True
    except KeyError:
      return False

  def assert_valid(self):
    """Asserts that the shared state is valid."""
    assert self.valid(), 'update() must be called first'

  def get_shared_state_update_id(self) -> int:
    """Returns the latest update id that was applied to the shared state."""
    return self.get_shared_state()[b'']

  def get(self, key: bytes) -> Any:
    """Returns the value for a given key; raises KeyError if it is missing."""
    # Note: we don't call assert_valid() here to maximize performance; it's the
    # caller's responsibility to call update() first
    if type(key) is not bytes:
      raise TypeError(f'key must be bytes; instead it is {type(key)}')
    return self.get_shared_state()[key]

  async def set(self, key: bytes, value: Any) -> int:
    """Updates the value for a given key. Other instances will see this when
    they call update()."""
    ret = await self._set_multi_encoded([(key, self.encode_value(value))])
    self.get_shared_state()[key] = value
    return ret

  async def set_multi(self, items: Dict[bytes, Any]) -> Optional[int]:
    """Updates the values for multiple keys. Other instances will see this when
    they call update().

    This is only atomic for certain types of errors. If one or more keys in the
    items dict is too large, no keys are updated. However, if a write error
    occurs in the external store, some batches may already have been committed,
    so it is up to the application to handle inconsistency if it uses large
    batch writes."""
    ret = await self._set_multi_encoded((key, self.encode_value(value)) for key, value in items.items())
    for key, value in items.items():
      self.get_shared_state()[key] = value
    return ret

  async def delete(self, key: bytes) -> int:
    """Deletes a key. Other instances will see this when they call update()."""
    ret = await self._set_multi_encoded([(key, b'')])
    del self.get_shared_state()[key]
    return ret

  async def delete_multi(self, keys: Iterable[bytes]) -> Optional[int]:
    """Deletes a key. Other instances will see this when they call update().

    Like set_multi, this is only atomic for certain types of errors. Unlike
    set_multi, the written update does not include values, so it's far less
    likely that an operation will be split into multiple batches."""
    ret = await self._set_multi_encoded((key, b'') for key in keys)
    for key in keys:
      del self.get_shared_state()[key]
    return ret

  def keys(self) -> Iterator[bytes]:
    """Returns an iterator that yields all keys in the local shared state."""
    self.assert_valid()
    it = self.get_shared_state().keys()
    assert next(it) == b'', 'shared state does not contain latest update id key'
    return it

  def values(self) -> Iterator[Any]:
    """Returns an iterator that yields all values in the local shared state."""
    self.assert_valid()
    it = self.get_shared_state().values()
    # Unfortunately, we can't assert that this is the update id key like we do
    # in keys() and items(), but PrefixTree always iterated in lexicographic
    # order, assert_valid() ensures that b'' exists, and no codepath in whycache
    # deletes it, so it's very unlikely that this next() call skips a different
    # key.
    next(it)
    return it

  def items(self) -> Iterator[Tuple[bytes, Any]]:
    """Returns an iterator that yields all items in the local shared state."""
    it = self.get_shared_state().items()
    assert next(it)[0] == b'', 'shared state does not contain latest update id key'
    return it


  async def rewrite_history(self,
    limit_update_id: int,
    filter_fn: Optional[Callable[[bytes, Any], bool]] = None,
    verbose=True,
  ) -> None:
    """Rewrites the update history to eliminate obviated updates from the
    external store.

    At a high level, this procedure is:
    1. Read all the updates from the external store, producing a snapshot of the
       shared state as of limit_update_id. At the same time, find the largest
       unused section of id space before limit_update_id.
    2. If filter_fn is given, apply it to the state.
    3. Produce a minimal set updates that generate this shared state.
    4. Write the new updates to the unused space found in step 1.
    5. Delete all updates before limit_update_id, except those that were just
       written in the previous step.

    At any point during this procedure, a process should be able to read the
    state of whycache from the external store up to limit_update_id and get the
    same snapshot as was generates in step 1. Notably, this means that the
    deletions in step 4 must be ordered by id, starting with the oldest first.

    filter_fn allows you to delete keys during the rewrite procedure. If
    filter_fn is given, it is called once for every key in the state, and any
    keys for which it returns false will be deleted from the state. This is a
    way to intentionally cause inconsistency between the shared state on
    machines and the external datastore, so only do this if you understand the
    consequences! It is probably safe to do this if you're sure that the keys
    you're filtering out are completely unused by your application.

    It is not safe for this function to run concurrently on the same whycache
    instance. Make sure at most one instance of this function is running per
    whycache instance at any given time.

    """

    # (Step 1) Build state up to limit_update_id and find unused id space
    largest_unused_space_start = 0
    largest_unused_space_size = 0
    prev_update_id = 0
    state = {}
    async for update_id, update_contents in self.get_update_range_contents(0, limit_update_id):
      for key, value_encoded in self._decode_update_contents(update_contents):
        if value_encoded == b'':
          del state[key]
        else:
          state[key] = value_encoded

      space_before_this_update = update_id - prev_update_id - 1
      if space_before_this_update > largest_unused_space_size:
        largest_unused_space_size = space_before_this_update
        largest_unused_space_start = prev_update_id + 1
      prev_update_id = update_id

    if verbose:
      print(f'[whycache.rewrite_history] read updates through {limit_update_id}')
      print(f'[whycache.rewrite_history] state contains {len(state)} keys')
      print(f'[whycache.rewrite_history] largest unused space is {largest_unused_space_size} ids starting at {largest_unused_space_start}')

    # (Step 2) Apply filter_fn if given
    if filter_fn is not None:
      new_state = {}
      for key, value_encoded in state.items():
        if filter_fn(key, self.decode_value(value_encoded)):
          new_state[key] = value_encoded
      if verbose:
        print(f'[whycache.rewrite_history] after filtering, state contains {len(new_state)} keys ({len(state) - len(new_state)} deleted)')
      state = new_state

    # (Step 2) Produce a minimal set of updates that generate this state
    total_bytes = 0
    batches = [self._CommitBatch()]
    for key, value_encoded in state.items():
      part = self._CommitBatch.encode_part(key, value_encoded)

      batch = batches[-1]
      if batch.bytes + len(part) > self.max_update_size:
        batch.encode()
        total_bytes += len(batch.encoded)
        batch = self._CommitBatch()
        batches.append(batch)
      batch.add_part(key, part)

    batches[-1].encode()
    total_bytes += len(batches[-1].encoded)
    if verbose:
      print(f'[whycache.rewrite_history] state encodes into {len(batches)} updates with a total of {total_bytes} bytes')

    # (Step 3) Write the new updates into the unused space
    if len(batches) > largest_unused_space_size:
      raise RuntimeError(f'not enough unused id space to write new history: {largest_unused_space_size} available, {len(batches)} needed')
    for update_id, batch in enumerate(batches, start=largest_unused_space_start):
      await self.overwrite_update(update_id, batch.encoded)
    if verbose:
      print(f'[whycache.rewrite_history] all updates written')

    # (Step 4) Delete all updates before limit_update_id except the new ones
    updates_deleted_before = await self.delete_updates_between(0, largest_unused_space_start - 1)
    if verbose:
      print(f'[whycache.rewrite_history] {updates_deleted_before} updates deleted before rewritten history')
    updates_deleted_after = await self.delete_updates_between(largest_unused_space_start + len(batches), limit_update_id)
    if verbose:
      print(f'[whycache.rewrite_history] {updates_deleted_after} updates deleted after rewritten history')
