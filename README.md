# whycache

whycache is an in-process distributed caching system designed for very fast reads and slow but guaranteed writes. This implementation is inspired by [Quora's implementation of a caching system by a similar name](https://www.quora.com/q/quoraengineering/Pycache-lightning-fast-in-process-caching).

## Architecture

whycache is a distributed key-value store built on top of [sharedstructures](https://github.com/fuzziqersoftware/sharedstructures). On each machine, the dataset is stored in a memory-mapped file shared between all processes that access the same instance.

Ground-truth data is stored in a mostly-append-only external datastore as a log of changes, which each client machine applies to its view of the dataset. To rein in growth, the history can be rewritten periodically to remove log entries whose effects have been overwritten by later log entries.

## Usage

### Prerequisites

Install [sharedstructures](https://github.com/fuzziqersoftware/sharedstructures), and aiomysql if you plan to use the MySQL datastore.

### Integration

whycache depends on an external datastore, but the abstraction doesn't make assumptions about which datastore(s) you may want to use. To integrate whycache into your existing system, declare a subclass of WhycacheInstance and implement the abstract functions defined in the base class, or adapt one of the example subclasses to your use case.

whycache comes with two example implementations, FileWhycacheInstance and MySQLWhycacheInstance. FileWhycacheInstance simply stores updates in binary files on the local disk, which is primarily used for testing. MySQLWhycacheInstance stores updates in a MySQL table; see the comments in the file for the table schema (you'll have to create it manually).

### Usage

After writing a subclass to integrate with your application, instantiate the subclass somehow (probably globally) and it's ready to use.

Read functions:
* `get(key: bytes)` gets the value for a key, or raises KeyError if it is missing.
* `keys()`, `values()`, and `items()` return iterators that yield all keys, values, or (key, value) pairs in the local shared state. Other processes may update the shared state during iteration, so you may not get a consistent snapshot.

Write functions:
* `async set(key: bytes, value: Any)` sets the value for a key and returns the created update id.
* `async set_multi(items: Dict[bytes, Any])` sets the values for multiple keys and returns the created update id (or the last update id, if multiple were created).
* `async delete(key: bytes)` deletes the value for a key and returns the created update id.
* `async delete_multi(keys: Iterable[bytes])` deletes the values for multiple keys and returns the created update id (or the last update id, if multiple were created).

Shared state functions:
* `async update()` reads new updates from the external store and applies them to the shared state. Generally you should call this regularly in each process - for example, before each incoming web request on a webserver.
* `get_shared_state_update_id()` returns the latest update id that has been applied locally.
* `close_shared_state()` closes the shared state file on disk. It will be automatically reopened if you call most other functions on the whycache instance, so make sure to do your work with it before doing so. (See the next section for when this may be useful.)
* `get_shared_state()` returns the underlying PrefixTree object. It's inadvisable to modify this object in any way, but this is a convenient way to inspect it.
* `shared_state_filename()` returns the full path to the shared state file.

Maintenance functions:
* `async rewrite_history(limit_update_id)` replaces the updates before `limit_update_id` with a minimal set that generate the state as of that update id. This can be used periodically to keep the size of data stored in the external store under control.

### Bootstrapping

When loading a whycache instance for the first time on a new machine, it's often undesirable to recreate the shared state from scratch from the datastore. To bootstrap the shared state on a new machine, create a periodic job (perhaps a daily cron) that loads the whycache instance from the datastore, then call `whycache_instance.close_shared_state()`, then copy the file located by `whycache_instance.shared_state_filename()` to another machine. For wider distribution, it might make sense to upload it to a blob store like Amazon S3, and have new machines download the latest version at boot or deploy time.

## Future work

There's a lot to do here.
* Write the `rewrite_history` function.
* Provide more detail on the bootstrapping section in this README.
* Figure out a way to make rewrite_history work when there are no gaps in the id space.
* Make single-key updates more space-efficient in the external store.
* Write a MySQL+memcache example implementation.
