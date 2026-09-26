from __future__ import annotations

import multiprocessing
import socket
import warnings
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from unittest.mock import Mock, patch

import pytest

from kombu import Connection, pidbox
from kombu.exceptions import ContentDisallowed, InconsistencyError
from kombu.utils.uuid import uuid


def is_cast(message):
    return message['method']


def is_call(message):
    return message['method'] and message['reply_to']


class test_Mailbox:

    class Mailbox(pidbox.Mailbox):

        def _collect(self, *args, **kwargs):
            return 'COLLECTED'

    def setup_method(self):
        self.mailbox = self.Mailbox('test_pidbox')
        self.connection = Connection(transport='memory')
        self.state = {'var': 1}
        self.handlers = {'mymethod': self._handler}
        self.bound = self.mailbox(self.connection)
        self.default_chan = self.connection.channel()
        self.node = self.bound.Node(
            'test_pidbox',
            state=self.state, handlers=self.handlers,
            channel=self.default_chan,
        )

    def _handler(self, state):
        return self.stats['var']

    def test_broadcast_matcher_pattern_string_type(self):
        mailbox = pidbox.Mailbox("test_matcher_str")(self.connection)
        with pytest.raises(ValueError):
            mailbox._broadcast("ping", pattern=1, matcher=2)

    def test_publish_reply_ignores_InconsistencyError(self):
        mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
        with patch('kombu.pidbox.Producer') as Producer:
            producer = Producer.return_value = Mock(name='producer')
            producer.publish.side_effect = InconsistencyError()
            mailbox._publish_reply(
                {'foo': 'bar'}, mailbox.reply_exchange, mailbox.oid, 'foo',
            )
            producer.publish.assert_called()

    def test_reply__collect(self):
        mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
        exchange = mailbox.reply_exchange.name
        channel = self.connection.channel()

        ticket = uuid()
        mailbox.get_ticket_reply_queue(ticket)(channel).declare()
        mailbox._publish_reply({'foo': 'bar'}, exchange,
                               mailbox.ticket_routing_key(ticket), ticket)
        _callback_called = [False]

        def callback(body):
            _callback_called[0] = True

        reply = mailbox._collect(ticket, limit=1,
                                 callback=callback, channel=channel)
        assert reply == [{'foo': 'bar'}]
        assert _callback_called[0]

        ticket = uuid()
        mailbox.get_ticket_reply_queue(ticket)(channel).declare()
        mailbox._publish_reply({'biz': 'boz'}, exchange,
                               mailbox.ticket_routing_key(ticket), ticket)
        reply = mailbox._collect(ticket, limit=1, channel=channel)
        assert reply == [{'biz': 'boz'}]

        ticket = 'doom'
        mailbox.get_ticket_reply_queue(ticket)(channel).declare()
        mailbox._publish_reply(
            {'foo': 'BAM'}, exchange,
            mailbox.ticket_routing_key(ticket), ticket,
            serializer='pickle',
        )
        with pytest.raises(ContentDisallowed):
            reply = mailbox._collect(ticket, limit=1, channel=channel)
        mailbox._publish_reply(
            {'foo': 'BAMBAM'}, exchange,
            mailbox.ticket_routing_key(ticket), ticket,
            serializer='pickle',
        )
        reply = mailbox._collect(ticket, limit=1, channel=channel,
                                 accept=['pickle'])
        assert reply[0]['foo'] == 'BAMBAM'

        de = mailbox.connection.drain_events = Mock()
        de.side_effect = socket.timeout
        mailbox._collect(uuid(), limit=1, channel=channel)

    def test_reply__collect_completes_before_timeout(self):
        mailbox = pidbox.Mailbox('test_collect_complete')(self.connection)
        exchange = mailbox.reply_exchange.name
        channel = self.connection.channel()

        ticket = uuid()
        mailbox.get_ticket_reply_queue(ticket)(channel).declare()
        mailbox._publish_reply({'foo': 'bar'}, exchange,
                               mailbox.ticket_routing_key(ticket), ticket)

        collected = mailbox._collect(ticket, limit=1, timeout=10,
                                     channel=channel)
        assert collected == [{'foo': 'bar'}]

    def test_late_reply_to_old_ticket_not_stolen_by_next_ticket(self):
        mailbox = pidbox.Mailbox('test_ticket_isolation')(self.connection)
        exchange = mailbox.reply_exchange.name
        channel = self.connection.channel()

        ticket_a = uuid()
        mailbox.get_ticket_reply_queue(ticket_a)(channel).declare()
        # Command A has timed out; its reply shows up only afterwards.
        mailbox._publish_reply({'a': 1}, exchange,
                               mailbox.ticket_routing_key(ticket_a), ticket_a)

        ticket_b = uuid()
        mailbox.get_ticket_reply_queue(ticket_b)(channel).declare()
        with patch.object(mailbox.connection, 'drain_events',
                          side_effect=socket.timeout):
            b_replies = mailbox._collect(ticket_b, limit=1, channel=channel)
        # The next command must not swallow A's late reply.
        assert b_replies == []

        # A's reply is still waiting on A's own ticket queue.
        a_replies = mailbox._collect(ticket_a, limit=1, channel=channel)
        assert a_replies == [{'a': 1}]

    def test_collect_callback_exception_does_not_swallow_replies(self):
        mailbox = pidbox.Mailbox('test_callback_isolated')(self.connection)
        exchange = mailbox.reply_exchange.name
        channel = self.connection.channel()

        seen = []

        def callback(body):
            seen.append(body)
            if body == {'i': 1}:
                raise KeyError('boom')

        ticket = uuid()
        mailbox.get_ticket_reply_queue(ticket)(channel).declare()
        for i in range(2):
            mailbox._publish_reply(
                {'i': i + 1}, exchange,
                mailbox.ticket_routing_key(ticket), ticket,
            )

        replies = mailbox._collect(ticket, limit=2, channel=channel,
                                   callback=callback)
        assert replies == [{'i': 1}, {'i': 2}]
        assert seen == [{'i': 1}, {'i': 2}]

    def test_connection_loss_ends_collect_and_tracks_pending_cleanup(self):
        mailbox = pidbox.Mailbox('test_connection_loss')(self.connection)
        channel = self.connection.channel()
        conn_error = self.connection.connection_errors[0]
        ticket = uuid()

        with patch.object(channel, 'after_reply_message_received',
                          side_effect=conn_error('closed')):
            with patch.object(mailbox.connection, 'drain_events',
                              side_effect=conn_error('closed')):
                replies = mailbox._collect(ticket, limit=2, channel=channel)

        assert replies == []
        assert ticket in [
            name.split('.')[1] for name in mailbox._pending_cleanup
        ]

        # Same connection recovered: deletion succeeds and the pending
        # entry is discarded.
        with patch.object(channel, 'queue_delete'):
            mailbox._retry_pending_cleanup(channel)
        assert not mailbox._pending_cleanup

    def test_pending_cleanup_tolerates_already_gone_queue(self):
        mailbox = pidbox.Mailbox('test_cleanup_404')(self.connection)
        channel = self.connection.channel()

        class NotFound(self.connection.channel_errors[0]):
            code = 404

        mailbox._pending_cleanup.add('ghost.reply.queue')
        with patch.object(channel, 'queue_delete',
                          side_effect=NotFound()):
            mailbox._retry_pending_cleanup(channel)
        assert not mailbox._pending_cleanup

    def test_broadcast_publish_failure_cleans_ticket_up(self):
        mailbox = pidbox.Mailbox('test_publish_failure')(self.connection)
        channel = self.connection.channel()
        with patch.object(mailbox, '_publish',
                          side_effect=KeyError('publish failed')):
            with patch.object(channel, 'after_reply_message_received') as hook:
                with pytest.raises(KeyError):
                    mailbox._broadcast('mymethod', reply=True,
                                       channel=channel)
                hook.assert_called_once()
        assert not mailbox._pending_cleanup
        assert not mailbox._tickets

    def test_reply__collect_uses_default_channel(self):
        class ConsumerCalled(Exception):
            pass

        def fake_Consumer(channel, *args, **kwargs):
            raise ConsumerCalled(channel)

        ticket = uuid()
        with patch('kombu.pidbox.Consumer') as Consumer:
            mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
            assert mailbox.connection.default_channel is not None
            Consumer.side_effect = fake_Consumer
            try:
                mailbox._collect(ticket, limit=1)
            except ConsumerCalled as c:
                assert c.args[0] is not None
            except Exception:
                raise
            else:
                assert False, "Consumer not called"

    def test__publish_uses_default_channel(self):
        class QueueCalled(Exception):
            pass

        def queue__call__side(channel, *args, **kwargs):
            raise QueueCalled(channel)

        ticket = uuid()
        with patch.object(pidbox.Queue, '__call__') as queue__call__:
            mailbox = pidbox.Mailbox('test_reply__collect')(self.connection)
            queue__call__.side_effect = queue__call__side
            try:
                mailbox._publish(ticket, {}, reply_ticket=ticket)
            except QueueCalled as c:
                assert c.args[0] is not None
            except Exception:
                raise
            else:
                assert False, "Queue not called"

    def test_constructor(self):
        assert self.mailbox.connection is None
        assert self.mailbox.exchange.name
        assert self.mailbox.reply_exchange.name

    def test_bound(self):
        bound = self.mailbox(self.connection)
        assert bound.connection is self.connection

    def test_Node(self):
        assert self.node.hostname
        assert self.node.state
        assert self.node.mailbox is self.bound
        assert self.handlers

        # No initial handlers
        node2 = self.bound.Node('test_pidbox2', state=self.state)
        assert node2.handlers == {}

    def test_Node_consumer(self):
        consumer1 = self.node.Consumer()
        assert consumer1.channel is self.default_chan
        assert consumer1.no_ack

        chan2 = self.connection.channel()
        consumer2 = self.node.Consumer(channel=chan2, no_ack=False)
        assert consumer2.channel is chan2
        assert not consumer2.no_ack

    def test_Node_consumer_resource_locked_raises_inconsistency(self):
        base_channel_error = self.connection.channel_errors[0]

        class ResourceLockedChannelError(base_channel_error):
            code = 405

            def __str__(self):
                return (
                    "RESOURCE_LOCKED - cannot obtain exclusive access to "
                    "locked queue 'pidbox': already using this queue"
                )

        with patch('kombu.pidbox.Consumer') as MockConsumer:
            MockConsumer.side_effect = ResourceLockedChannelError()
            with pytest.raises(InconsistencyError, match='already using'):
                self.node.Consumer()

    def test_Node_consumer_other_channel_error_propagates(self):
        base_channel_error = self.connection.channel_errors[0]

        class AccessRefusedError(base_channel_error):
            code = 403

        with patch('kombu.pidbox.Consumer') as MockConsumer:
            MockConsumer.side_effect = AccessRefusedError()
            with pytest.raises(AccessRefusedError):
                self.node.Consumer()

    def test_Node_consumer_multiple_listeners(self):
        warnings.resetwarnings()
        consumer = self.node.Consumer()
        q = consumer.queues[0]
        with warnings.catch_warnings(record=True) as log:
            q.on_declared('foo', 1, 1)
            assert log
            assert 'already using this' in log[0].message.args[0]

        with warnings.catch_warnings(record=True) as log:
            q.on_declared('foo', 1, 0)
            assert not log

    def test_handler(self):
        node = self.bound.Node('test_handler', state=self.state)

        @node.handler
        def my_handler_name(state):
            return 42

        assert 'my_handler_name' in node.handlers

    def test_dispatch(self):
        node = self.bound.Node('test_dispatch', state=self.state)

        @node.handler
        def my_handler_name(state, x=None, y=None):
            return x + y

        assert node.dispatch('my_handler_name',
                             arguments={'x': 10, 'y': 10}) == 20

    def test_dispatch_raising_SystemExit(self):
        node = self.bound.Node('test_dispatch_raising_SystemExit',
                               state=self.state)

        @node.handler
        def my_handler_name(state):
            raise SystemExit

        with pytest.raises(SystemExit):
            node.dispatch('my_handler_name')

    def test_dispatch_raising(self):
        node = self.bound.Node('test_dispatch_raising', state=self.state)

        @node.handler
        def my_handler_name(state):
            raise KeyError('foo')

        res = node.dispatch('my_handler_name')
        assert 'error' in res
        assert 'KeyError' in res['error']

    def test_dispatch_replies(self):
        _replied = [False]

        def reply(data, **options):
            _replied[0] = True

        node = self.bound.Node('test_dispatch', state=self.state)
        node.reply = reply

        @node.handler
        def my_handler_name(state, x=None, y=None):
            return x + y

        node.dispatch('my_handler_name',
                      arguments={'x': 10, 'y': 10},
                      reply_to={'exchange': 'foo', 'routing_key': 'bar'})
        assert _replied[0]

    def test_reply(self):
        _replied = [(None, None, None)]

        def publish_reply(data, exchange, routing_key, ticket, **kwargs):
            _replied[0] = (data, exchange, routing_key, ticket)

        mailbox = self.mailbox(self.connection)
        mailbox._publish_reply = publish_reply
        node = mailbox.Node('test_reply')

        @node.handler
        def my_handler_name(state):
            return 42

        node.dispatch('my_handler_name',
                      reply_to={'exchange': 'exchange',
                                'routing_key': 'rkey'},
                      ticket='TICKET')
        data, exchange, routing_key, ticket = _replied[0]
        assert data == {'test_reply': 42}
        assert exchange == 'exchange'
        assert routing_key == 'rkey'
        assert ticket == 'TICKET'

    def test_handle_message(self):
        node = self.bound.Node('test_dispatch_from_message')

        @node.handler
        def my_handler_name(state, x=None, y=None):
            return x * y

        body = {'method': 'my_handler_name',
                'arguments': {'x': 64, 'y': 64}}

        assert node.handle_message(body, None) == 64 * 64

        # message not for me should not be processed.
        body['destination'] = ['some_other_node']
        assert node.handle_message(body, None) is None

        # message for me should be processed.
        body['destination'] = ['test_dispatch_from_message']
        assert node.handle_message(body, None) is not None

        # message not for me should not be processed.
        body.pop("destination")
        body['matcher'] = 'glob'
        body["pattern"] = "something*"
        assert node.handle_message(body, None) is None

        body["pattern"] = "test*"
        assert node.handle_message(body, None) is not None

    def test_handle_message_adjusts_clock(self):
        node = self.bound.Node('test_adjusts_clock')

        @node.handler
        def my_handler_name(state):
            return 10

        body = {'method': 'my_handler_name',
                'arguments': {}}
        message = Mock(name='message')
        message.headers = {'clock': 313}
        node.adjust_clock = Mock(name='adjust_clock')
        res = node.handle_message(body, message)
        node.adjust_clock.assert_called_with(313)
        assert res == 10

    def test_listen(self):
        consumer = self.node.listen()
        assert consumer.callbacks[0] == self.node.handle_message
        assert consumer.channel == self.default_chan

    def test_cast(self):
        self.bound.cast(['somenode'], 'mymethod')
        consumer = self.node.Consumer()
        assert is_cast(self.get_next(consumer))

    def test_abcast(self):
        self.bound.abcast('mymethod')
        consumer = self.node.Consumer()
        assert is_cast(self.get_next(consumer))

    def test_call_destination_must_be_sequence(self):
        with pytest.raises(ValueError):
            self.bound.call('some_node', 'mymethod')

    def test_call(self):
        assert self.bound.call(['some_node'], 'mymethod') == 'COLLECTED'
        consumer = self.node.Consumer()
        assert is_call(self.get_next(consumer))

    def test_multi_call(self):
        assert self.bound.multi_call('mymethod') == 'COLLECTED'
        consumer = self.node.Consumer()
        assert is_call(self.get_next(consumer))

    def get_next(self, consumer):
        m = consumer.queues[0].get()
        if m:
            return m.payload

    def test_mailbox_defaults_to_exclusive(self):
        mbox = pidbox.Mailbox('flagbox_default')(self.connection)

        for q in (mbox.get_queue('worker1'), mbox.get_reply_queue()):
            assert q.exclusive is True
            assert q.durable is False
            assert q.auto_delete is True

    def test_mailbox_queue_exclusive(self):
        mbox = pidbox.Mailbox(
            'flagbox_ex',
            queue_exclusive=True,
            queue_durable=False,
        )(self.connection)

        for q in (mbox.get_queue('worker1'), mbox.get_reply_queue()):
            assert q.exclusive is True
            assert q.durable is False
            assert q.auto_delete is True

    def test_mailbox_queue_durable(self):
        mbox = pidbox.Mailbox(
            'flagbox_dur',
            queue_exclusive=False,
            queue_durable=True,
        )(self.connection)

        for q in (mbox.get_queue('worker1'), mbox.get_reply_queue()):
            assert q.durable is True
            assert q.exclusive is False
            assert q.auto_delete is False

    def test_mailbox_invalid_flag_combo(self):
        with pytest.raises(ValueError):
            pidbox.Mailbox(
                'flagbox_bad',
                queue_exclusive=True,
                queue_durable=True,
            )(self.connection)


GLOBAL_PIDBOX = pidbox.Mailbox('global_unittest_mailbox')


def getoid():
    return GLOBAL_PIDBOX.oid


class test_PidboxOid:
    """Unittests checking oid consistency of Pidbox"""

    def test_oid_consistency(self):
        """Tests that oid is consistent in single process"""
        m1 = pidbox.Mailbox('mailbox1')
        m2 = pidbox.Mailbox('mailbox2')
        assert m1.oid == m1.oid
        assert m2.oid == m2.oid
        assert m1.oid != m2.oid

    def test_subprocess_oid(self):
        """Tests that subprocess will not share oid with parent process."""
        oid = GLOBAL_PIDBOX.oid
        mp_context = multiprocessing.get_context('forkserver')
        with ProcessPoolExecutor(mp_context=mp_context) as e:
            res = e.submit(getoid)
            subprocess_oid = res.result()
        assert subprocess_oid != oid

    def test_thread_oid(self):
        """Tests that threads will not share oid."""
        oid = GLOBAL_PIDBOX.oid
        with ThreadPoolExecutor() as e:
            res = e.submit(getoid)
            subprocess_oid = res.result()
            assert subprocess_oid != oid


class test_PidboxTicket:

    def ticket(self, **kwargs):
        return pidbox.PidboxTicket(
            kwargs.pop('id', 'T'), Mock(name='queue'), **kwargs,
        )

    def test_open_close_lifecycle(self):
        ticket = self.ticket()
        assert not ticket.is_open
        ticket.open()
        assert ticket.is_open
        assert ticket.close('complete')
        assert not ticket.is_open
        assert ticket.reason == 'complete'
        # Second close is a no-op; the first reason wins.
        assert not ticket.close('timeout')
        assert ticket.reason == 'complete'

    def test_deliver_collects_while_open(self):
        ticket = self.ticket(limit=2)
        ticket.open()
        assert ticket.deliver({'a': 1})
        assert ticket.responses == [{'a': 1}]
        # Limit reached on the second reply.
        assert not ticket.deliver({'a': 2})
        assert ticket.responses == [{'a': 1}, {'a': 2}]

    def test_deliver_after_close_is_late(self):
        ticket = self.ticket()
        ticket.open()
        ticket.close('timeout')
        assert not ticket.deliver({'late': True})
        assert ticket.responses == []
        assert ticket.late_responses == 1
        ticket.note_late()
        assert ticket.late_responses == 2

    def test_callback_exception_does_not_block_delivery(self):
        seen = []

        def callback(body):
            seen.append(body)
            raise RuntimeError('boom')

        ticket = self.ticket(callback=callback)
        ticket.open()
        # The callback raises, but the reply is still collected.
        assert ticket.deliver({'x': 1})
        assert seen == [{'x': 1}]
        assert ticket.responses == [{'x': 1}]

    def test_begin_finish_runs_once(self):
        ticket = self.ticket()
        assert ticket.begin_finish()
        assert not ticket.begin_finish()
