import os
import re

from typing import Optional, List, Tuple, AsyncGenerator
from WhycacheInstance import WhycacheInstance


class FileWhycacheInstance(WhycacheInstance):
  def __init__(self, instance_name: str, data_directory: str):
    super().__init__(instance_name)
    self.data_directory = data_directory


  def shared_state_filename(self) -> str:
    return f'{self.data_directory}/state.sspt'


  def _latest_update_filename(self) -> str:
    return f'{self.data_directory}/latest.repr'

  def _update_filename(self, update_id) -> str:
    return f'{self.data_directory}/{update_id}.bin'

  def _read_latest_update_id(self) -> int:
    try:
      with open(self._latest_update_filename(), 'rt') as f:
        return int(f.read())
    except FileNotFoundError:
      latest_update_id = 0
      for item in os.listdir(self.data_directory):
        match = re.match(r'^([0-9]+)\.whycache\.bin$', item)
        if match is None:
          continue
        update_id = int(match.group(1))
        if update_id > latest_update_id:
          latest_update_id = update_id
      self._write_latest_update_id(latest_update_id)
      return latest_update_id


  def _write_latest_update_id(self, latest_update_id: int) -> None:
    with open(self._latest_update_filename(), 'wt') as f:
      f.write(str(latest_update_id))

  def _read_update(self, update_id: int) -> Optional[bytes]:
    try:
      with open(self._update_filename(update_id), 'rb') as f:
        return f.read()
    except FileNotFoundError:
      return None

  def _write_update(self, update_id: int, contents: bytes) -> None:
    with open(self._update_filename(update_id), 'wb') as f:
      f.write(contents)


  async def get_update_range_contents(self, update_id_low: int, update_id_high: int) -> AsyncGenerator[Tuple[int, bytes], None]:
    for update_id in range(update_id_low, update_id_high + 1):
      contents = self._read_update(update_id)
      if contents is not None:
        yield (update_id, contents)

  async def get_latest_update_id(self) -> int:
    return self._read_latest_update_id()

  async def commit_update(self, update_contents: bytes, single_key_name: Optional[bytes] = None) -> int:
    update_id = self._read_latest_update_id() + 1
    self._write_update(update_id, update_contents)
    self._write_latest_update_id(update_id)
    return update_id

  async def overwrite_update(self, update_id: int, update_contents: bytes, single_key_name: Optional[bytes] = None) -> None:
    self._write_update(update_id, update_contents)

  async def delete_updates_between(self, low_id: int, high_id: int) -> int:
    for item in os.listdir(self.data_directory):
      match = re.match(r'^([0-9]+)\.whycache\.bin$', item)
      if match is None:
        continue
      update_id = int(match.group(1))
      if update_id >= low_id and update_id <= high_id:
        os.remove(self._update_filename(update_id))
