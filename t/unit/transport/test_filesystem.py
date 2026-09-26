from __future__ import annotations

import contextlib
import os
import shutil
import tempfile
import time
import uuid
from pathlib import PurePosixPath, PureWindowsPath
from queue import Empty
from typing import Generator
from unittest.mock import call, patch

import pytest

import t.skip
from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.exceptions import ChannelError, OperationalError
from kombu.transport import filesystem as filesystem_transport
from kombu.transport.filesystem import (
    QUARANTINE_DIR,
    Channel as FilesystemChannel,
)
from kombu.transport.virtual import Channel
from kombu.utils.encoding import bytes_to_str, str_to_bytes
from kombu.utils.json import dumps, loads


class WithJanitorMixin:
    def _remove_temporary_folders(self):
        for path in (self.data_folder_in, self.data_folder_out, self.control_folder):
            try:
                shutil.rmtree(path)
            except OSError:
                pass


@contextlib.contextmanager
def managed_consumer(channel, queues=None, *args, **kwargs) -> Generator[Consumer]:
    consumer = Consumer(channel, queues, *args, **kwargs)
    try:
        yield consumer
    finally:
        for q in consumer.queues:
            q: Queue
            with contextlib.suppress(Exception):
                q(consumer.channel).delete()


@t.skip.if_win32
class test_FilesystemTransport(WithJanitorMixin):

    def setup_method(self):
        self.channels: set[Channel] = set()
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip('filesystem transport: cannot create tempfiles')
        self.c = Connection(transport='filesystem',
                            transport_options={
                                'data_folder_in': self.data_folder_in,
                                'data_folder_out': self.data_folder_out,
                                'control_folder': self.control_folder,
                            })
        self.channels.add(self.c.default_channel)
        self.p = Connection(transport='filesystem',
                            transport_options={
                                'data_folder_in': self.data_folder_out,
                                'data_folder_out': self.data_folder_in,
                                'control_folder': self.control_folder,
                            })
        self.channels.add(self.p.default_channel)
        self.e_name = 'test_transport_filesystem'
        self.e = Exchange(self.e_name)
        self.q = Queue('test_transport_filesystem',
                       exchange=self.e,
                       routing_key='test_transport_filesystem')
        self.q2 = Queue('test_transport_filesystem2',
                        exchange=self.e,
                        routing_key='test_transport_filesystem2')

    def teardown_method(self):
        # make sure we don't attempt to restore messages at shutdown.
        for channel in self.channels:
            try:
                channel._qos._dirty.clear()
            except AttributeError:
                pass
            try:
                channel._qos._delivered.clear()
            except AttributeError:
                pass

        self._remove_temporary_folders()

    def _add_channel(self, channel) -> Channel:
        self.channels.add(channel)
        return channel

    def _prepare_bind(self, channel: Channel, queue: Queue) -> tuple:
        # create exchange_queue_t tuple (see transport/filesystem.py)
        bind: tuple = channel.typeof(self.e_name).prepare_bind(queue.name, self.e_name, queue.routing_key, None)
        return tuple(map(lambda v: v or '', bind))

    def test_produce_consume_noack(self):
        consumer_channel = self._add_channel(self.c.channel())
        producer = Producer(self._add_channel(self.p.channel()), self.e)
        with managed_consumer(consumer_channel, self.q,
                              no_ack=True) as consumer:
            for i in range(10):
                producer.publish({'foo': i},
                                 routing_key='test_transport_filesystem')

            _received = []

            def callback(message_data, message):
                _received.append(message)

            consumer.register_callback(callback)
            consumer.consume()

            while 1:
                if len(_received) == 10:
                    break
                self.c.drain_events()

            assert len(_received) == 10

    def test_produce_consume(self):
        producer_channel = self._add_channel(self.p.channel())
        consumer_channel = self._add_channel(self.c.channel())
        producer = Producer(producer_channel, self.e)
        with (
            managed_consumer(consumer_channel, self.q) as consumer1,
            managed_consumer(consumer_channel, self.q2) as consumer2,
        ):
            self.q2(consumer_channel).declare()

            for i in range(10):
                producer.publish({'foo': i},
                                 routing_key='test_transport_filesystem')
            for i in range(10):
                producer.publish({'foo': i},
                                 routing_key='test_transport_filesystem2')

            _received1 = []
            _received2 = []

            def callback1(message_data, message):
                _received1.append(message)
                message.ack()

            def callback2(message_data, message):
                _received2.append(message)
                message.ack()

            consumer1.register_callback(callback1)
            consumer2.register_callback(callback2)

            consumer1.consume()
            consumer2.consume()

            while 1:
                if len(_received1) + len(_received2) == 20:
                    break
                self.c.drain_events()

            assert len(_received1) + len(_received2) == 20

            # compression
            producer.publish({'compressed': True},
                             routing_key='test_transport_filesystem',
                             compression='zlib')
            m = self.q(consumer_channel).get()
            assert m.payload == {'compressed': True}

            # queue.delete
            for i in range(10):
                producer.publish({'foo': i},
                                 routing_key='test_transport_filesystem')
            assert self.q(consumer_channel).get()

            # assert q and q2 are in consumer_channel's table
            consumer_channel_table: list[tuple] = consumer_channel.get_table(self.e_name)
            assert len(consumer_channel_table) == 2
            assert self._prepare_bind(consumer_channel, self.q) in consumer_channel_table
            assert self._prepare_bind(consumer_channel, self.q2) in consumer_channel_table

            self.q(consumer_channel).delete()

            # assert only q2 is in consumer_channel's table after .delete()
            consumer_channel_table2: list[tuple] = consumer_channel.get_table(self.e_name)
            assert len(consumer_channel_table2) == 1
            assert self._prepare_bind(consumer_channel, self.q) not in consumer_channel_table2
            assert self._prepare_bind(consumer_channel, self.q2) in consumer_channel_table2

            self.q(consumer_channel).declare()
            assert self.q(consumer_channel).get() is None

            # queue.purge
            for i in range(10):
                producer.publish({'foo': i},
                                 routing_key='test_transport_filesystem2')
            assert self.q2(consumer_channel).get()
            self.q2(consumer_channel).purge()
            assert self.q2(consumer_channel).get() is None

    def test_dotted_queue_name_not_matched_by_suffix(self):
        producer_channel = self._add_channel(self.p.channel())
        consumer_channel = self._add_channel(self.c.channel())
        producer = Producer(producer_channel, self.e)
        # 'b' is a dotted suffix of 'a.b', the two must not share messages
        dotted = Queue('a.b', exchange=self.e, routing_key='a.b')
        suffix = Queue('b', exchange=self.e, routing_key='b')
        with (
            managed_consumer(consumer_channel, dotted),
            managed_consumer(consumer_channel, suffix),
        ):
            dotted(consumer_channel).declare()
            suffix(consumer_channel).declare()

            producer.publish({'foo': 1}, routing_key='a.b')

            assert suffix(consumer_channel).queue_declare(
                passive=True).message_count == 0
            assert suffix(consumer_channel).get() is None

            # purging 'b' must leave the message routed to 'a.b' in place
            suffix(consumer_channel).purge()
            assert dotted(consumer_channel).get()


@t.skip.if_win32
class test_FilesystemFanout(WithJanitorMixin):
    def setup_method(self):
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")

        self.consumer_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_in,
                "data_folder_out": self.data_folder_out,
                "control_folder": self.control_folder,
            },
        )
        self.consume_channel = self.consumer_connection.channel()
        self.produce_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_out,
                "data_folder_out": self.data_folder_in,
                "control_folder": self.control_folder,
            },
        )
        self.producer_channel = self.produce_connection.channel()
        self.exchange = Exchange("filesystem_exchange_fanout", type="fanout")
        self.q1 = Queue("queue1", exchange=self.exchange)
        self.q2 = Queue("queue2", exchange=self.exchange)

    def teardown_method(self):
        # make sure we don't attempt to restore messages at shutdown.
        for channel in [self.producer_channel, self.consumer_connection]:
            try:
                channel._qos._dirty.clear()
            except AttributeError:
                pass
            try:
                channel._qos._delivered.clear()
            except AttributeError:
                pass

        self._remove_temporary_folders()

    def test_produce_consume(self):

        producer = Producer(self.producer_channel, self.exchange)
        consumer1 = Consumer(self.consume_channel, self.q1)
        consumer2 = Consumer(self.consume_channel, self.q2)
        self.q2(self.consume_channel).declare()

        for i in range(10):
            producer.publish({"foo": i})

        _received1 = []
        _received2 = []

        def callback1(message_data, message):
            _received1.append(message)
            message.ack()

        def callback2(message_data, message):
            _received2.append(message)
            message.ack()

        consumer1.register_callback(callback1)
        consumer2.register_callback(callback2)

        consumer1.consume()
        consumer2.consume()

        while 1:
            try:
                self.consume_channel.drain_events()
            except Empty:
                break

        assert len(_received1) + len(_received2) == 20

        # queue.delete
        for i in range(10):
            producer.publish({"foo": i})
        assert self.q1(self.consume_channel).get()
        self.q1(self.consume_channel).delete()
        self.q1(self.consume_channel).declare()
        assert self.q1(self.consume_channel).get() is None

        # queue.purge
        assert self.q2(self.consume_channel).get()
        self.q2(self.consume_channel).purge()
        assert self.q2(self.consume_channel).get() is None


@t.skip.if_win32
class test_FilesystemLock(WithJanitorMixin):
    def setup_method(self):
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")

        self.consumer_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_in,
                "data_folder_out": self.data_folder_out,
                "control_folder": self.control_folder,
            },
        )
        self.consume_channel = self.consumer_connection.channel()
        self.produce_connection = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_out,
                "data_folder_out": self.data_folder_in,
                "control_folder": self.control_folder,
            },
        )
        self.producer_channel = self.produce_connection.channel()
        self.exchange = Exchange("filesystem_exchange_lock", type="fanout")
        self.q = Queue("queue1", exchange=self.exchange)

    def teardown_method(self):
        # make sure we don't attempt to restore messages at shutdown.
        for channel in [self.producer_channel, self.consumer_connection]:
            try:
                channel._qos._dirty.clear()
            except AttributeError:
                pass
            try:
                channel._qos._delivered.clear()
            except AttributeError:
                pass

        self._remove_temporary_folders()

    def test_lock_during_process(self):
        pytest.importorskip('fcntl')
        from fcntl import LOCK_EX, LOCK_SH

        producer = Producer(self.producer_channel, self.exchange)

        with patch("kombu.transport.filesystem.lock") as lock_m, patch(
            "kombu.transport.filesystem.unlock"
        ) as unlock_m:
            Consumer(self.consume_channel, self.q)
            assert unlock_m.call_count == 1
            lock_m.assert_called_once_with(unlock_m.call_args[0][0], LOCK_EX)

        self.q(self.consume_channel).declare()
        with patch("kombu.transport.filesystem.lock") as lock_m, patch(
            "kombu.transport.filesystem.unlock"
        ) as unlock_m:
            producer.publish({"foo": 1})
            # publishing only reads the fanout routing table; the message
            # itself is published with a temp file + atomic rename and needs
            # no advisory lock
            assert unlock_m.call_count == 1
            assert lock_m.call_count == 1
            exchange_file_obj = unlock_m.call_args_list[0][0][0]
            assert lock_m.call_args_list == [call(exchange_file_obj, LOCK_SH)]


@t.skip.if_win32
class test_FilesystemDiskProtocol(WithJanitorMixin):
    """Durable on-disk protocol: atomic publish, leases, recovery."""

    QUEUE = 'diskq'

    def setup_method(self):
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
            self.processed_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip('filesystem transport: cannot create tempfiles')
        self.connections = []
        self.channels = []

    def teardown_method(self):
        # make sure we don't attempt to restore messages at shutdown
        for channel in self.channels:
            try:
                channel._qos._dirty.clear()
                channel._qos._delivered.clear()
            except AttributeError:
                pass
        self._remove_temporary_folders()
        try:
            shutil.rmtree(self.processed_folder)
        except OSError:
            pass

    def _connection(self, in_folder, out_folder, **options):
        opts = {
            'data_folder_in': in_folder,
            'data_folder_out': out_folder,
            'control_folder': self.control_folder,
        }
        opts.update(options)
        conn = Connection(transport='filesystem', transport_options=opts)
        self.connections.append(conn)
        return conn

    def _channel(self, connection):
        channel = connection.channel()
        self.channels.append(channel)
        return channel

    @property
    def consumer_conn(self):
        return self._connection(self.data_folder_in, self.data_folder_out)

    @property
    def producer_conn(self):
        return self._connection(self.data_folder_out, self.data_folder_in)

    def _payload(self, body='hello', tag=None):
        return {
            'body': body,
            'properties': {
                'delivery_tag': tag or str(uuid.uuid4()),
                'delivery_info': {},
            },
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
        }

    def _put_raw(self, payload, queue=QUEUE):
        channel = self._channel(self.producer_conn)
        channel._put(queue, payload)
        return channel

    def _entries(self, folder):
        return [n for n in os.listdir(folder)
                if os.path.isfile(os.path.join(folder, n))]

    def _ready_names(self, folder):
        return [n for n in self._entries(folder) if n.endswith('.msg')]

    def _lease_names(self, folder):
        return [n for n in self._entries(folder) if '.lease.' in n]

    def _expire_lease(self, folder, lease_name):
        """Rewrite a lease name as if its deadline had passed."""
        ready, _, suffix = lease_name.partition('.lease.')
        _, _, token = suffix.rpartition('.')
        expired_ms = int(time.time() * 1000) - 60_000
        expired_name = f'{ready}.lease.{expired_ms}.{token}'
        os.replace(os.path.join(folder, lease_name),
                   os.path.join(folder, expired_name))
        return expired_name

    def test_publish_leaves_one_complete_named_file_no_temp(self):
        tag = str(uuid.uuid4())
        payload = self._payload(tag=tag)
        producer = self._channel(self.producer_conn)
        producer._put(self.QUEUE, payload)

        entries = self._entries(self.data_folder_in)
        assert len(entries) == 1
        name = entries[0]
        assert name == f'{name.split("_")[0]}_{tag}.{self.QUEUE}.msg'
        # the payload on disk is the complete JSON, never a partial write
        with open(os.path.join(self.data_folder_in, name), 'rb') as fh:
            assert loads_bytes(fh.read())['properties']['delivery_tag'] == tag

    def test_stale_temp_file_is_quarantined_and_never_delivered(self):
        stale = os.path.join(self.data_folder_in, '.tmp.crashed-writer')
        with open(stale, 'wb') as fh:
            fh.write(b'{"truncated": ')
        old = time.time() - 1000
        os.utime(stale, (old, old))

        channel = self._channel(self.consumer_conn)
        channel._sweep(force=True)

        assert not os.path.exists(stale)
        quarantined = os.listdir(
            os.path.join(self.data_folder_in, QUARANTINE_DIR))
        assert quarantined == ['.tmp.crashed-writer']
        assert channel._size(self.QUEUE) == 0
        with pytest.raises(Empty):
            channel._get(self.QUEUE)

    def test_recent_temp_file_is_left_for_live_writer(self):
        channel = self._channel(self.consumer_conn)
        fresh = os.path.join(self.data_folder_in, '.tmp.in-flight')
        with open(fresh, 'wb') as fh:
            fh.write(b'x')
        channel._sweep(force=True)
        assert os.path.exists(fresh)

    def test_sweep_is_rate_limited_between_startup_and_polling(self):
        channel = self._channel(self.consumer_conn)
        stale = os.path.join(self.data_folder_in, '.tmp.old')
        with open(stale, 'wb') as fh:
            fh.write(b'x')
        old = time.time() - 1000
        os.utime(stale, (old, old))

        channel._sweep()  # within the recovery interval: must not act
        assert os.path.exists(stale)
        channel._sweep(force=True)
        assert not os.path.exists(stale)

    def test_claim_leaves_lease_and_excludes_it_from_size(self):
        self._put_raw(self._payload())
        first = self._channel(self.consumer_conn)
        competitor = self._channel(self.consumer_conn)

        message = first.basic_get(self.QUEUE)
        assert message is not None
        assert self._ready_names(self.data_folder_in) == []
        assert len(self._lease_names(self.data_folder_in)) == 1
        assert first._size(self.QUEUE) == 0
        # a competing consumer cannot get the same message
        assert competitor.basic_get(self.QUEUE) is None

    def test_ack_deletes_lease(self):
        self._put_raw(self._payload())
        channel = self._channel(self.consumer_conn)
        message = channel.basic_get(self.QUEUE)
        channel.basic_ack(message.delivery_tag)
        assert self._entries(self.data_folder_in) == []

    def test_ack_archives_original_name_and_bytes_in_processed_folder(self):
        tag = str(uuid.uuid4())
        self._put_raw(self._payload(tag=tag))
        original_name = self._ready_names(self.data_folder_in)[0]
        with open(os.path.join(self.data_folder_in, original_name), 'rb') as fh:
            original_bytes = fh.read()

        conn = self._connection(
            self.data_folder_in, self.data_folder_out,
            store_processed=True, processed_folder=self.processed_folder)
        channel = self._channel(conn)
        message = channel.basic_get(self.QUEUE)
        assert os.listdir(self.processed_folder) == []
        channel.basic_ack(message.delivery_tag)

        assert os.listdir(self.processed_folder) == [original_name]
        with open(os.path.join(self.processed_folder, original_name), 'rb') as fh:
            assert fh.read() == original_bytes

    def test_claim_does_not_archive_message_before_ack(self):
        conn = self._connection(
            self.data_folder_in, self.data_folder_out,
            store_processed=True, processed_folder=self.processed_folder)
        self._put_raw(self._payload())
        channel = self._channel(conn)

        channel.basic_get(self.QUEUE)
        assert os.listdir(self.processed_folder) == []
        assert len(self._lease_names(self.data_folder_in)) == 1

    def test_reject_requeue_restores_original_name_and_identity(self):
        tag = str(uuid.uuid4())
        self._put_raw(self._payload(body='rebound', tag=tag))
        original_name = self._ready_names(self.data_folder_in)[0]
        channel = self._channel(self.consumer_conn)

        message = channel.basic_get(self.QUEUE)
        channel.basic_reject(message.delivery_tag, requeue=True)

        assert self._ready_names(self.data_folder_in) == [original_name]
        assert channel._size(self.QUEUE) == 1
        redelivered = channel.basic_get(self.QUEUE)
        assert redelivered.delivery_tag == tag
        assert bytes_to_str(redelivered.body) == 'rebound'
        channel.basic_ack(redelivered.delivery_tag)
        assert self._entries(self.data_folder_in) == []

    def test_reject_without_requeue_discards_lease(self):
        conn = self._connection(
            self.data_folder_in, self.data_folder_out,
            store_processed=True, processed_folder=self.processed_folder)
        self._put_raw(self._payload())
        channel = self._channel(conn)
        message = channel.basic_get(self.QUEUE)

        channel.basic_reject(message.delivery_tag, requeue=False)

        assert self._entries(self.data_folder_in) == []
        # discarded messages are not treated as processed
        assert os.listdir(self.processed_folder) == []

    def test_expired_lease_is_reclaimed_and_stale_owner_is_fenced(self):
        tag = str(uuid.uuid4())
        self._put_raw(self._payload(tag=tag))
        stale_owner = self._channel(self.consumer_conn)
        message = stale_owner.basic_get(self.QUEUE)
        lease_name = self._lease_names(self.data_folder_in)[0]
        # the owning process hangs past the lease deadline
        self._expire_lease(self.data_folder_in, lease_name)

        new_owner = self._channel(self.consumer_conn)  # startup sweep
        redelivered = new_owner.basic_get(self.QUEUE)
        assert redelivered is not None
        assert redelivered.delivery_tag == tag
        new_lease = self._lease_names(self.data_folder_in)[0]

        # the stale owner finalising late must not remove the new lease
        stale_owner.basic_ack(message.delivery_tag)
        assert self._lease_names(self.data_folder_in) == [new_lease]
        new_owner.basic_ack(redelivered.delivery_tag)
        assert self._entries(self.data_folder_in) == []

    def test_orphan_lease_from_dead_process_recovered_at_startup(self):
        tag = str(uuid.uuid4())
        ready_name = f'1700000000000_{tag}.{self.QUEUE}.msg'
        lease_name = f'{ready_name}.lease.1.{uuid.uuid4().hex}'
        with open(os.path.join(self.data_folder_in, lease_name), 'wb') as fh:
            fh.write(str_to_bytes(dumps(self._payload(tag=tag))))

        channel = self._channel(self.consumer_conn)  # sweeps at startup

        assert self._ready_names(self.data_folder_in) == [ready_name]
        message = channel.basic_get(self.QUEUE)
        assert message is not None
        assert message.delivery_tag == tag

    def test_legacy_file_names_are_still_consumed(self):
        tag = str(uuid.uuid4())
        legacy_name = f'123456789_{uuid.uuid4()}.{self.QUEUE}.msg'
        with open(os.path.join(self.data_folder_in, legacy_name), 'wb') as fh:
            fh.write(str_to_bytes(dumps(self._payload(tag=tag))))

        channel = self._channel(self.consumer_conn)
        message = channel.basic_get(self.QUEUE)
        assert message is not None
        assert message.delivery_tag == tag
        channel.basic_ack(message.delivery_tag)
        assert self._entries(self.data_folder_in) == []

    def test_corrupt_message_is_quarantined_and_queue_keeps_serving(self):
        bad_name = f'000000000000_{uuid.uuid4()}.{self.QUEUE}.msg'
        with open(os.path.join(self.data_folder_in, bad_name), 'wb') as fh:
            fh.write(b'{"truncated": ')

        good_tag = str(uuid.uuid4())
        self._put_raw(self._payload(tag=good_tag))

        channel = self._channel(self.consumer_conn)
        message = channel._get(self.QUEUE)
        assert message['properties']['delivery_tag'] == good_tag
        assert self._ready_names(self.data_folder_in) == []
        assert len(self._lease_names(self.data_folder_in)) == 1
        quarantine = os.path.join(self.data_folder_in, QUARANTINE_DIR)
        assert any(name.startswith(bad_name)
                   for name in os.listdir(quarantine))

    def test_purge_and_size_only_cover_published_messages(self):
        self._put_raw(self._payload())
        self._put_raw(self._payload())
        channel = self._channel(self.consumer_conn)
        channel.basic_get(self.QUEUE)  # one message leased, one still ready

        assert channel._size(self.QUEUE) == 1
        assert channel._purge(self.QUEUE) == 1
        assert channel._size(self.QUEUE) == 0
        assert len(self._lease_names(self.data_folder_in)) == 1

    def test_unacked_message_restored_to_queue_on_clean_shutdown(self):
        tag = str(uuid.uuid4())
        self._put_raw(self._payload(tag=tag))
        conn = self.consumer_conn
        channel = self._channel(conn)
        assert channel.basic_get(self.QUEUE) is not None
        assert self._ready_names(self.data_folder_in) == []

        channel.close()

        assert len(self._ready_names(self.data_folder_in)) == 1
        other = self._channel(self.consumer_conn)
        redelivered = other.basic_get(self.QUEUE)
        assert redelivered.delivery_tag == tag

    def test_cross_device_processed_folder_rejected_upfront(self):
        real_stat = os.stat
        processed = os.path.abspath(self.processed_folder)

        def fake_stat(path, *args, **kwargs):
            result = real_stat(path, *args, **kwargs)
            if os.path.abspath(str(path)) == processed:
                # a genuine stat_result that only differs by device id, so
                # is_dir()/mkdir() keep behaving normally
                values = list(result)
                values[2] = values[2] + 1
                return os.stat_result(values)
            return result

        with patch.object(
                filesystem_transport.os, 'stat', side_effect=fake_stat):
            with pytest.raises(
                    (ChannelError, OperationalError),
                    match='different filesystem'):
                conn = self._connection(
                    self.data_folder_in, self.data_folder_out,
                    store_processed=True,
                    processed_folder=self.processed_folder,
                    max_retries=0, interval_start=0,
                    interval_step=0, interval_max=0)
                conn.default_channel

    def test_missing_data_folder_rejected_at_startup(self):
        missing = tempfile.mkdtemp()
        shutil.rmtree(missing)
        with pytest.raises((ChannelError, OperationalError),
                           match='does not exist'):
            conn = self._connection(
                missing, self.data_folder_out,
                max_retries=0, interval_start=0,
                interval_step=0, interval_max=0)
            conn.default_channel


def loads_bytes(raw):
    return loads(bytes_to_str(raw))


@t.skip.if_win32
class test_FilesystemExchangeNameSanitization(WithJanitorMixin):
    # An exchange name is used to build a filename under control_folder; a
    # name containing path separators or ".." must not escape that folder.

    def setup_method(self):
        try:
            self.data_folder_in = tempfile.mkdtemp()
            self.data_folder_out = tempfile.mkdtemp()
            self.control_folder = tempfile.mkdtemp()
        except Exception:
            pytest.skip("filesystem transport: cannot create tempfiles")
        self.conn = Connection(
            transport="filesystem",
            transport_options={
                "data_folder_in": self.data_folder_in,
                "data_folder_out": self.data_folder_out,
                "control_folder": self.control_folder,
            },
        )
        self.channel = self.conn.default_channel

    def teardown_method(self):
        self._remove_temporary_folders()

    def test_legitimate_dotted_exchange_name_allowed(self):
        # ordinary exchange names contain dots but no separators
        self.channel._queue_bind("reply.celery.pidbox", "rk", "", "q")
        assert ("rk", "", "q") in self.channel.get_table("reply.celery.pidbox")

    @pytest.mark.parametrize("exchange", [
        "../../tmp/evil", "../escape", "/etc/passwd", "sub/child",
        ".", "..", "./.", "./reply.celery.pidbox",
        # Windows-shaped payloads.  The guard is not OS-conditional, so
        # these are rejected here too rather than only on Windows.
        "D:evil", "C:evil", "sub\\child", "..\\..\\evil", "CON", "NUL",
    ])
    def test_invalid_exchange_name_rejected_on_bind(self, exchange):
        with pytest.raises(ChannelError):
            self.channel._queue_bind(exchange, "rk", "", "q")

    @pytest.mark.parametrize("exchange", [
        "../../etc/passwd", "/etc/passwd", "./reply.celery.pidbox",
        "D:evil", "C:evil", "CON",
    ])
    def test_invalid_exchange_name_rejected_on_get_table(self, exchange):
        with pytest.raises(ChannelError):
            self.channel.get_table(exchange)

    def test_no_file_created_outside_control_folder(self):
        target = os.path.join(
            os.path.dirname(self.control_folder), "escaped.exchange")
        with pytest.raises(ChannelError):
            self.channel._queue_bind("../escaped", "rk", "", "q")
        assert not os.path.exists(target)

    def test_relative_name_cannot_alias_another_exchange(self):
        # a legit exchange and a "./"-prefixed variant must not normalise to
        # the same file (which would let one poison the other's table)
        self.channel._queue_bind("reply.celery.pidbox", "rk", "", "q")
        with pytest.raises(ChannelError):
            self.channel._queue_bind("./reply.celery.pidbox", "evil", "", "q")
        assert self.channel.get_table("reply.celery.pidbox") == [("rk", "", "q")]


class test_exchange_file_name_guard:
    """Name validation alone, with no filesystem and no locking.

    Deliberately *not* skipped on win32, and parametrized over both path
    flavours, so the Windows behaviour of the guard is executed by the
    suite on a POSIX runner instead of being reasoned about.
    """

    def _exchange_file(self, exchange, folder):
        class _Channel(FilesystemChannel):
            # the real guard, with control_folder pinned to the flavour
            # under test and no connection or filesystem behind it
            def __init__(self):
                pass

            @property
            def control_folder(self):
                return folder

        return _Channel()._exchange_file(exchange)

    @pytest.mark.parametrize("folder", [
        PureWindowsPath(r"C:\app\control"), PurePosixPath("/app/control"),
    ], ids=["windows", "posix"])
    @pytest.mark.parametrize("exchange", [
        # traversal and separators
        "..", ".", "...", "./x", "../escape", "a/../b",
        "/etc/passwd", "sub/child", "sub\\child", "..\\..\\evil",
        # Windows drive-relative names: neither contains a separator, and
        # pathlib drops the control folder for the non-anchor drive while
        # the anchor drive silently aliases another exchange.
        "D:evil", "C:evil", "d:evil",
        # UNC prefix
        "\\\\server\\share\\x",
        # device names, which a suffix does not disarm
        "CON", "NUL", "COM1", "LPT1", "con.foo",
        # characters with no business in a filename
        "a b", "a\x00b", "~/x",
        # not a name at all
        None, 42,
    ])
    def test_rejected(self, exchange, folder):
        with pytest.raises(ChannelError):
            self._exchange_file(exchange, folder)

    @pytest.mark.parametrize("folder", [
        PureWindowsPath(r"C:\app\control"), PurePosixPath("/app/control"),
    ], ids=["windows", "posix"])
    def test_default_exchange_name_still_allowed(self, folder):
        # "" is the AMQP default exchange.  virtual.Channel.queue_bind()
        # rewrites it to "amq.direct", but get_table() and
        # exchange_delete() pass it through, so rejecting it here would
        # break the default exchange rather than close a hole: it yields a
        # plain ".exchange" file inside the control folder.
        file = self._exchange_file("", folder)
        assert file.name == ".exchange"
        assert file.parent == folder

    @pytest.mark.parametrize("folder", [
        PureWindowsPath(r"C:\app\control"), PurePosixPath("/app/control"),
    ], ids=["windows", "posix"])
    @pytest.mark.parametrize("exchange", [
        "good.name", "reply.celery.pidbox", "celeryev", "tasks",
        "my-exchange", "my_exchange", "X2", "a.b.c.d",
    ])
    def test_accepted(self, exchange, folder):
        file = self._exchange_file(exchange, folder)
        assert file.name == f"{exchange}.exchange"
        assert file.parent == folder

    def test_drive_relative_name_cannot_alias_another_exchange(self):
        # "C:evil" would resolve to the same file as "evil" on Windows
        folder = PureWindowsPath(r"C:\app\control")
        assert folder / "C:evil.exchange" == folder / "evil.exchange"
        with pytest.raises(ChannelError):
            self._exchange_file("C:evil", folder)

    def test_drive_relative_name_escapes_when_drive_differs(self):
        # the same name on a control folder hosted elsewhere leaves it
        folder = PureWindowsPath(r"C:\app\control")
        assert (folder / "D:evil.exchange").parent != folder
        with pytest.raises(ChannelError):
            self._exchange_file("D:evil", folder)

    # every name Microsoft documents as reserved, including the superscript
    # COM#/LPT# spellings and the two console devices, in the casings and
    # suffixed forms Windows still resolves to the device
    @pytest.mark.parametrize("name", (
        ["CON", "PRN", "AUX", "NUL", "CONIN$", "CONOUT$"]
        + [f"COM{c}" for c in "123456789\xb9\xb2\xb3"]
        + [f"LPT{c}" for c in "123456789\xb9\xb2\xb3"]
    ))
    @pytest.mark.parametrize("shape", ["{}", "{}.exchange", "{}.", "{}.txt"])
    def test_windows_device_names_rejected(self, name, shape):
        folder = PureWindowsPath(r"C:\app\control")
        for spelling in (name, name.lower(), name.capitalize()):
            with pytest.raises(ChannelError):
                self._exchange_file(shape.format(spelling), folder)
