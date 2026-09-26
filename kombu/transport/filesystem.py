"""File-system Transport module for kombu.

Transport using the file-system as the message store. Messages written to the
queue are stored in `data_folder_in` directory and
messages read from the queue are read from `data_folder_out` directory. Both
directories must be created manually. Simple example:

* Producer:

.. code-block:: python

    import kombu

    conn = kombu.Connection(
        'filesystem://', transport_options={
            'data_folder_in': 'data_in', 'data_folder_out': 'data_out'
        }
    )
    conn.connect()

    test_queue = kombu.Queue('test', routing_key='test')

    with conn as conn:
        with conn.default_channel as channel:
            producer = kombu.Producer(channel)
            producer.publish(
                        {'hello': 'world'},
                        retry=True,
                        exchange=test_queue.exchange,
                        routing_key=test_queue.routing_key,
                        declare=[test_queue],
                        serializer='pickle'
            )

* Consumer:

.. code-block:: python

    import kombu

    conn = kombu.Connection(
        'filesystem://', transport_options={
            'data_folder_in': 'data_out', 'data_folder_out': 'data_in'
        }
    )
    conn.connect()

    def callback(body, message):
        print(body, message)
        message.ack()

    test_queue = kombu.Queue('test', routing_key='test')

    with conn as conn:
        with conn.default_channel as channel:
            consumer = kombu.Consumer(
                conn, [test_queue], accept=['pickle']
            )
            consumer.register_callback(callback)
            with consumer:
                conn.drain_events(timeout=1)

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: No
* Supports TTL: No

Connection String
=================
Connection string is in the following format:

.. code-block::

    filesystem://

Transport Options
=================
* ``data_folder_in`` - directory where are messages stored when written
  to queue.
* ``data_folder_out`` - directory from which are messages read when read from
  queue.
* ``store_processed`` - if set to True, acknowledged messages are moved to
  ``processed_folder``. If False, they are deleted on acknowledgement.
* ``processed_folder`` - directory where acknowledged messages are archived.
  Must be on the same filesystem device as ``data_folder_in`` so the move
  stays atomic; the channel refuses to start otherwise.
* ``control_folder`` - directory where are exchange-queue table stored.
* ``lease_ttl`` - seconds a claimed (unacknowledged) message is reserved for
  its consumer before a crash recovery sweep may return it to the queue.
  Defaults to 300.
* ``recovery_interval`` - minimum seconds between two background recovery
  sweeps while polling; one sweep always runs at startup. Defaults to 10.

Disk protocol
=============
Messages are first written and fsynced into a ``.tmp.*`` sibling file inside
the target directory and then atomically renamed to
``<timestamp>_<message-id>.<queue>.msg``, so a producer crash can never
expose a truncated message. Consuming atomically renames the file to a
``.lease.<expiry>.<owner>`` claim; it is deleted or moved to
``processed_folder`` only when acknowledged, and ``basic.reject(requeue=True)``
renames it back under its original name. Startup and periodic sweeps
quarantine stale temp files and reclaim expired leases. Files written by
older releases keep being read.
"""

from __future__ import annotations

import contextlib
import os
import re
import tempfile
import uuid
from collections import namedtuple
from pathlib import Path
from queue import Empty
from time import monotonic, time

from kombu.exceptions import ChannelError
from kombu.transport import virtual
from kombu.utils.encoding import bytes_to_str, str_to_bytes
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

VERSION = (1, 0, 0)
__version__ = '.'.join(map(str, VERSION))

# ----------------------------------------------------------------------------
# On-disk message protocol
# ----------------------------------------------------------------------------
#
# Three file kinds live inside a data folder, all carrying the same payload
# bytes (encoded JSON):
#
#   * Published (ready to consume)::
#
#         <epoch-ms>_<stable-message-id>.<queue>.msg
#
#     The file appears atomically: the payload is first written and fsynced
#     into a sibling temporary file (``.tmp.<random>``) and only then renamed
#     to this name.  Readers therefore never observe a truncated message.
#     The stable id is the message ``delivery_tag`` and is preserved when a
#     message is requeued.  Files written by older releases
#     (``<monotonic-ms>_<uuid4>.<queue>.msg``) match the same pattern and are
#     consumed unchanged.
#
#   * Claimed by a consumer (recoverable lease)::
#
#         <ready-name>.lease.<expires-at-ms>.<owner-token>
#
#     Created by a single atomic rename of the ready file, so competing
#     consumers cannot both claim the same message.  The owner finalises the
#     lease on ack/reject; if its process dies, a startup or periodic sweep
#     renames leases whose deadline has passed back to their ready name.
#
#   * Temporary publication files (``.tmp.*``) and malformed message files
#     are moved, never read as messages, into a ``.quarantine`` sibling
#     directory once they look stale, so a crashed publisher cannot strand a
#     message forever or poison the queue.

#: Matches a published message file; group ``queue`` may itself contain dots,
#: so it is matched greedily and anchored at both ends.
READY_FILE_RE = re.compile(r'\A\d+_[^.]+\.(?P<queue>.+)\.msg\Z')

#: Matches a lease file; ``ready`` is the original ready file name.
LEASE_FILE_RE = re.compile(
    r'\A(?P<ready>.+)\.lease\.(?P<expires>\d+)\.(?P<token>[0-9a-fA-F-]+)\Z'
)

#: Prefix of the sibling file a payload is written into before it is
#: atomically renamed to its published name.
TMP_FILE_PREFIX = '.tmp.'

#: Directory, relative to a data folder, that collects stale temporary
#: files and unreadable message files.
QUARANTINE_DIR = '.quarantine'

#: Default lifetime of a claim, in seconds.  A lease that outlives its owner
#: becomes eligible for recovery once this deadline passes.
DEFAULT_LEASE_TTL = 300.0

#: Minimum interval between two recovery sweeps performed while polling.
DEFAULT_RECOVERY_INTERVAL = 10.0

#: Claim metadata remembered for messages delivered by this channel.
lease_t = namedtuple(
    'lease_t',
    ['queue', 'ready_name', 'ready_path', 'lease_path'],
)

# needs win32all to work on Windows
if os.name == 'nt':

    import pywintypes
    import win32con
    import win32file

    LOCK_EX = win32con.LOCKFILE_EXCLUSIVE_LOCK
    # 0 is the default
    LOCK_SH = 0
    LOCK_NB = win32con.LOCKFILE_FAIL_IMMEDIATELY
    __overlapped = pywintypes.OVERLAPPED()

    def lock(file, flags):
        """Create file lock."""
        hfile = win32file._get_osfhandle(file.fileno())
        win32file.LockFileEx(hfile, flags, 0, 0xffff0000, __overlapped)

    def unlock(file):
        """Remove file lock."""
        hfile = win32file._get_osfhandle(file.fileno())
        win32file.UnlockFileEx(hfile, 0, 0xffff0000, __overlapped)


elif os.name == 'posix':

    import fcntl
    from fcntl import LOCK_EX, LOCK_SH

    def lock(file, flags):
        """Create file lock."""
        fcntl.flock(file.fileno(), flags)

    def unlock(file):
        """Remove file lock."""
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)


else:
    raise RuntimeError(
        'Filesystem plugin only defined for NT and POSIX platforms')


exchange_queue_t = namedtuple("exchange_queue_t",
                              ["routing_key", "pattern", "queue"])

#: Characters accepted in an exchange name.  The name is interpolated into a
#: filename under ``control_folder``, so rather than blocklisting the
#: constructs that can redirect that path, only characters that cannot are
#: allowed.  This rejects path separators, ``..``, Windows drive-relative
#: prefixes (``D:evil``) and UNC prefixes on every platform, rather than only
#: on the one the tests happen to run on.
EXCHANGE_NAME_RE = re.compile(r'\A[A-Za-z0-9._-]+\Z')

#: Names that address a device rather than a file on Windows.  A suffix does
#: not help: ``CON.exchange`` is still the console.  Windows resolves these
#: case-insensitively and on the portion before the first dot, and counts the
#: ISO/IEC 8859-1 superscript digits as digits in ``COM#``/``LPT#``.  Mirrors
#: the list :mod:`pathlib` carries.  ``EXCHANGE_NAME_RE`` happens to reject
#: the non-ASCII and ``$`` spellings before they reach this set, but the set
#: is kept complete on its own so that widening the pattern later cannot
#: quietly let them through.
WIN_RESERVED_NAMES = frozenset(
    ['CON', 'PRN', 'AUX', 'NUL', 'CONIN$', 'CONOUT$']
    + [f'COM{c}' for c in '123456789\xb9\xb2\xb3']
    + [f'LPT{c}' for c in '123456789\xb9\xb2\xb3']
)


class Channel(virtual.Channel):
    """Filesystem Channel."""

    supports_fanout = True

    def __init__(self, connection, **kwargs):
        super().__init__(connection, **kwargs)
        #: delivery_tag -> lease_t, for messages claimed by this channel and
        #: not yet finalised.
        self._leases = {}
        self._next_sweep = 0.0
        # Fail fast on a layout the on-disk protocol cannot support, and
        # recover messages orphaned by previous processes before serving.
        self._validate_storage()
        self._sweep(force=True)

    def _validate_storage(self):
        """Verify the configured folders before any message is exchanged.

        Data folders must already exist (this transport never creates them)
        and, when processed messages are archived, the archive must live on
        the same device as the queue so acknowledgement can stay a single
        atomic rename -- a cross-device move would require a non-atomic copy
        and is rejected here rather than degraded to one.
        """
        for folder in (self.data_folder_in, self.data_folder_out,
                       self.control_folder):
            if not folder.is_dir():
                raise ChannelError(
                    f'Filesystem transport folder does not exist: {folder!r}')

        if self.store_processed:
            self.processed_folder.mkdir(parents=True, exist_ok=True)
            if (os.stat(self.data_folder_in).st_dev !=
                    os.stat(self.processed_folder).st_dev):
                raise ChannelError(
                    'processed_folder {!r} is on a different filesystem '
                    "device than data_folder_in {!r}; acknowledged messages "
                    'could not be moved atomically, so refusing to start '
                    'instead of falling back to a copy.'.format(
                        self.processed_folder, self.data_folder_in))

    @staticmethod
    def _fsync_dir(folder):
        """Best-effort flush of a directory's rename/unlink metadata."""
        flags = os.O_RDONLY
        if hasattr(os, 'O_DIRECTORY'):
            flags |= os.O_DIRECTORY
        try:
            dir_fd = os.open(folder, flags)
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)

    def _quarantine(self, folder, path):
        """Move an unreadable/stale file aside instead of delivering it."""
        quarantine = folder / QUARANTINE_DIR
        try:
            quarantine.mkdir(exist_ok=True)
            target = quarantine / path.name
            if target.exists():
                target = quarantine / f'{path.name}.{uuid.uuid4().hex}'
            os.replace(path, target)
        except OSError:
            # quarantine is best effort; a vanished file was simply handled
            # elsewhere, an unwritable folder gets retried on the next sweep
            pass

    def _sweep(self, force=False):
        """Quarantine stale temp files and recover expired leases.

        Runs once at startup and, rate-limited by ``recovery_interval``,
        while polling for messages.
        """
        now = monotonic()
        if not force and now < self._next_sweep:
            return
        self._next_sweep = now + self.recovery_interval

        for folder in {self.data_folder_in, self.data_folder_out}:
            self._sweep_folder(folder)

    def _sweep_folder(self, folder):
        try:
            entries = os.listdir(folder)
        except OSError:
            return
        stale_before = time() - self.lease_ttl
        for name in entries:
            path = folder / name
            if not path.is_file():
                continue

            if name.startswith(TMP_FILE_PREFIX):
                # A publisher died between creating the temp file and the
                # atomic publish.  Give a live, slow writer a grace period;
                # anything older is isolated rather than left forever.
                try:
                    if path.stat().st_mtime <= stale_before:
                        self._quarantine(folder, path)
                except FileNotFoundError:
                    pass
                continue

            lease_match = LEASE_FILE_RE.match(name)
            if lease_match is None:
                continue
            if int(lease_match.group('expires')) / 1000 > time():
                continue
            # Expired claim: return the file to the queue under its original
            # published name.  The rename is atomic, so a live owner that
            # finalises afterwards finds its lease gone and is fenced.
            ready_name = lease_match.group('ready')
            if READY_FILE_RE.match(ready_name) is None:
                self._quarantine(folder, path)
                continue
            try:
                os.replace(path, folder / ready_name)
                self._fsync_dir(folder)
            except FileNotFoundError:
                pass
            except OSError:
                pass

    def _publish_atomic(self, folder, ready_name, data):
        """Write ``data`` to a sibling temp file, fsync it, publish it."""
        fd, tmp_target = tempfile.mkstemp(
            prefix=TMP_FILE_PREFIX, dir=folder)
        tmp_path = Path(tmp_target)
        try:
            with os.fdopen(fd, 'wb', buffering=0) as tmp_file:
                tmp_file.write(data)
                tmp_file.flush()
                os.fsync(tmp_file.fileno())
            os.replace(tmp_path, folder / ready_name)
            self._fsync_dir(folder)
        except BaseException:
            with contextlib.suppress(OSError):
                tmp_path.unlink()
            raise

    @staticmethod
    def _is_valid_exchange_name(exchange):
        if not isinstance(exchange, str):
            return False
        if exchange == "":
            # the AMQP default exchange.  It yields a plain ".exchange"
            # file inside the control folder and cannot redirect the path,
            # so it keeps working as it always has.
            return True
        if not EXCHANGE_NAME_RE.match(exchange):
            return False
        if not exchange.strip("."):
            # "." and "..", and any all-dot name, name a directory
            return False
        # a suffix does not disarm a Windows device name
        return exchange.partition(".")[0].upper() not in WIN_RESERVED_NAMES

    def _exchange_file(self, exchange):
        if not self._is_valid_exchange_name(exchange):
            raise ChannelError(f"Invalid exchange name: {exchange!r}")
        file = self.control_folder / f"{exchange}.exchange"
        # defence in depth: whatever the platform makes of the name, the
        # result has to be a direct child of the control folder.
        if file.parent != self.control_folder:
            raise ChannelError(f"Invalid exchange name: {exchange!r}")
        return file

    def get_table(self, exchange):
        file = self._exchange_file(exchange)
        try:
            f_obj = file.open("r")
            try:
                lock(f_obj, LOCK_SH)
                exchange_table = loads(bytes_to_str(f_obj.read()))
                return [exchange_queue_t(*q) for q in exchange_table]
            finally:
                unlock(f_obj)
                f_obj.close()
        except FileNotFoundError:
            return []
        except OSError:
            raise ChannelError(f"Cannot open {file}")

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        file = self._exchange_file(exchange)
        self.control_folder.mkdir(exist_ok=True)
        queue_val = exchange_queue_t(routing_key or "", pattern or "",
                                     queue or "")
        try:
            if file.exists():
                f_obj = file.open("rb+", buffering=0)
                lock(f_obj, LOCK_EX)
                exchange_table = loads(bytes_to_str(f_obj.read()))
                queues = [exchange_queue_t(*q) for q in exchange_table]
                if queue_val not in queues:
                    queues.insert(0, queue_val)
                    f_obj.seek(0)
                    f_obj.write(str_to_bytes(dumps(queues)))
            else:
                f_obj = file.open("wb", buffering=0)
                lock(f_obj, LOCK_EX)
                queues = [queue_val]
                f_obj.write(str_to_bytes(dumps(queues)))
        finally:
            unlock(f_obj)
            f_obj.close()

    def _put_fanout(self, exchange, payload, routing_key, **kwargs):
        for q in self.get_table(exchange):
            self._put(q.queue, payload, **kwargs)

    def _put(self, queue, payload, **kwargs):
        """Put `message` onto `queue`.

        The message is written to a temporary sibling, fsynced and only then
        renamed to its published name, so a crash mid-write can never leave a
        truncated ``.msg`` file behind.  The published name embeds the
        message delivery tag, which is stable across requeues.
        """
        message_id = payload['properties']['delivery_tag']
        ready_name = f'{int(time() * 1000)}_{message_id}.{queue}.msg'
        folder = self.data_folder_out

        try:
            self._publish_atomic(folder, ready_name,
                                 str_to_bytes(dumps(payload)))
        except OSError:
            raise ChannelError(
                f'Cannot add file {ready_name!r} to directory {folder!r}')

    def _get(self, queue):
        """Claim next message from `queue`.

        The ready file is atomically renamed to a lease file carrying this
        owner's token and an expiry deadline.  The message is only removed or
        archived once it is acknowledged (see :meth:`basic_ack`); if this
        process dies first, the sweep returns an expired lease to the queue.
        """
        self._sweep()
        folder = self.data_folder_in
        try:
            entries = sorted(os.listdir(folder))
        except FileNotFoundError:
            raise Empty()
        except OSError:
            raise ChannelError(f'Cannot read queue folder {folder!r}')

        for name in entries:
            match = READY_FILE_RE.match(name)
            if match is None or match.group('queue') != queue:
                continue

            ready_path = folder / name
            token = uuid.uuid4().hex
            expires_ms = int((time() + self.lease_ttl) * 1000)
            lease_name = f'{name}.lease.{expires_ms}.{token}'
            lease_path = folder / lease_name

            try:
                # Atomic claim: exactly one competing consumer wins.
                os.replace(ready_path, lease_path)
            except FileNotFoundError:
                # claimed or purged by somebody else
                continue
            except OSError:
                continue

            try:
                payload = loads(bytes_to_str(lease_path.read_bytes()))
                delivery_tag = payload['properties']['delivery_tag']
            except (OSError, ValueError, KeyError):
                # A published file should always be complete JSON; the only
                # way to see garbage is a file written by an older release
                # whose publisher crashed.  Isolate it and keep serving.
                self._quarantine(folder, lease_path)
                continue

            self._leases[delivery_tag] = lease_t(
                queue=queue, ready_name=name,
                ready_path=ready_path, lease_path=lease_path,
            )
            return payload

        raise Empty()

    def _remove_lease(self, lease):
        try:
            lease.lease_path.unlink()
        except FileNotFoundError:
            # The lease expired and was recovered (and possibly
            # redelivered) while processing; this owner no longer has a say.
            pass
        except OSError:
            raise ChannelError(
                f'Cannot finalise file {lease.ready_name!r}')

    def basic_ack(self, delivery_tag, multiple=False):
        lease = self._leases.pop(delivery_tag, None)
        if lease is not None:
            if self.store_processed:
                # Only acknowledged messages ever reach the archive, and the
                # move is an atomic same-device rename (checked at startup).
                try:
                    os.replace(lease.lease_path,
                               self.processed_folder / lease.ready_name)
                except FileNotFoundError:
                    # lease recovered and redelivered while processing
                    pass
                except OSError:
                    raise ChannelError(
                        f'Cannot archive acknowledged file '
                        f'{lease.ready_name!r}')
            else:
                self._remove_lease(lease)
        return super().basic_ack(delivery_tag, multiple)

    def basic_reject(self, delivery_tag, requeue=False):
        if not requeue:
            # A discarded message is not a successful acknowledgement, so it
            # is never archived with the processed messages.
            lease = self._leases.pop(delivery_tag, None)
            if lease is not None:
                self._remove_lease(lease)
        return super().basic_reject(delivery_tag, requeue=requeue)

    def _restore(self, message):
        """Return a claimed message to its queue without losing its identity.

        Renaming the lease back to the original published name keeps the
        stable message id and the message's position relative to older files.
        Used for ``basic.reject(requeue=True)``, ``basic.recover`` and for
        unacked messages at graceful shutdown.
        """
        lease = self._leases.pop(getattr(message, 'delivery_tag', None), None)
        if lease is not None:
            try:
                os.replace(lease.lease_path, lease.ready_path)
                self._fsync_dir(self.data_folder_in)
            except FileNotFoundError:
                # already recovered and redelivered by another sweep
                pass
            except OSError:
                raise ChannelError(
                    f'Cannot requeue file {lease.ready_name!r}')
            return
        super()._restore(message)

    def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
        super()._delete(queue, exchange, routing_key, pattern, *args, **kwargs)

        file = self._exchange_file(exchange)
        queue_val = exchange_queue_t(routing_key or "", pattern or "",
                                     queue or "")
        f_obj = None
        try:
            try:
                f_obj = file.open("rb+", buffering=0)
            except FileNotFoundError:
                # Exchange file was removed concurrently; nothing to update.
                return
            lock(f_obj, LOCK_EX)
            exchange_table = loads(bytes_to_str(f_obj.read()))
            queues = [exchange_queue_t(*q) for q in exchange_table]
            original_len = len(queues)
            try:
                queues.remove(queue_val)
            except ValueError:
                # queue_val was not present; nothing to remove
                pass
            if len(queues) != original_len:
                f_obj.seek(0)
                f_obj.write(str_to_bytes(dumps(queues)))
                f_obj.truncate()
        finally:
            if f_obj is not None:
                unlock(f_obj)
                f_obj.close()

    def _iter_ready_files(self, queue):
        """Yield ``(name, path)`` of published files routed to `queue`.

        Temporary files, leases (unacked claims) and quarantine contents are
        never counted as queue contents.
        """
        try:
            entries = os.listdir(self.data_folder_in)
        except FileNotFoundError:
            return
        for name in entries:
            match = READY_FILE_RE.match(name)
            if match is None or match.group('queue') != queue:
                continue
            path = self.data_folder_in / name
            if path.is_file():
                yield name, path

    def _purge(self, queue):
        """Remove all published messages from `queue`.

        Leased (unacknowledged) messages are left alone: they are owned by a
        consumer and are either finalised or returned by the recovery sweep.
        """
        count = 0
        for _, path in self._iter_ready_files(queue):
            try:
                path.unlink()
                count += 1
            except FileNotFoundError:
                # claimed by another consumer between listing and unlinking
                pass
            except OSError:
                pass

        return count

    def _size(self, queue):
        """Return the number of published messages in `queue`.

        Unacknowledged (leased) messages are no longer in the ready queue and
        are not counted.
        """
        return sum(1 for _ in self._iter_ready_files(queue))

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def data_folder_in(self):
        return Path(self.transport_options.get('data_folder_in', 'data_in'))

    @cached_property
    def data_folder_out(self):
        return Path(self.transport_options.get('data_folder_out', 'data_out'))

    @cached_property
    def store_processed(self):
        return self.transport_options.get('store_processed', False)

    @cached_property
    def processed_folder(self):
        return Path(
            self.transport_options.get('processed_folder', 'processed'))

    @cached_property
    def lease_ttl(self):
        return float(
            self.transport_options.get('lease_ttl', DEFAULT_LEASE_TTL))

    @cached_property
    def recovery_interval(self):
        return float(self.transport_options.get(
            'recovery_interval', DEFAULT_RECOVERY_INTERVAL))

    @property
    def control_folder(self):
        return Path(self.transport_options.get('control_folder', 'control'))


class Transport(virtual.Transport):
    """Filesystem Transport."""

    implements = virtual.Transport.implements.extend(
        asynchronous=False,
        exchange_type=frozenset(['direct', 'topic', 'fanout'])
    )

    Channel = Channel
    # filesystem backend state is global.
    global_state = virtual.BrokerState()
    default_port = 0
    driver_type = 'filesystem'
    driver_name = 'filesystem'

    def __init__(self, client, **kwargs):
        super().__init__(client, **kwargs)
        self.state = self.global_state

    def driver_version(self):
        return 'N/A'
