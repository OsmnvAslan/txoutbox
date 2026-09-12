"""Built-in storage and publisher adapters.

* :mod:`txoutbox.adapters.memory` - in-process, for tests and demos.
* :mod:`txoutbox.adapters.sqlite` - stdlib ``sqlite3``, for small services and examples.
* :mod:`txoutbox.adapters.postgres` - ``asyncpg`` with ``FOR UPDATE SKIP LOCKED``
  (install extra ``postgres``).
"""
