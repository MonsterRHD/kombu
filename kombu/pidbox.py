"""Generic process mailbox."""

from __future__ import annotations

import socket
import threading
import warnings
from contextlib import contextmanager
from copy import copy
from time import time

from . import Consumer, Exchange, Producer, Queue
from .clocks import LamportClock
from .common import maybe_declare, oid_from
from .exceptions import InconsistencyError
from .log import get_logger
from .matcher import match
from .transport.base import StdChannel
from .utils.functional import maybe_evaluate, reprcall
from .utils.objects import cached_property
from .utils.uuid import uuid

REPLY_QUEUE_EXPIRES = 10

#: terminal reasons recorded on a closed request ticket.
TICKET_COMPLETE = 'complete'
TICKET_TIMEOUT = 'timeout'
TICKET_CANCELLED = 'cancelled'
TICKET_CONNECTION_CLOSED = 'connection_closed'

W_PIDBOX_IN_USE = """\
A node named {node.hostname} is already using this process mailbox!

Maybe you forgot to shutdown the other node or did not do so properly?
Or if you meant to start multiple nodes on the same host please make sure
you give each node a unique node name!
"""

__all__ = ('Node', 'Mailbox', 'PidboxTicket')
logger = get_logger(__name__)
debug, error = logger.debug, logger.error


class Node:
    """Mailbox node."""

    #: hostname of the node.
    hostname = None

    #: the :class:`Mailbox` this is a node for.
    mailbox = None

    #: map of method name/handlers.
    handlers = None

    #: current context (passed on to handlers)
    state = None

    #: current channel.
    channel = None

    def __init__(self, hostname, state=None, channel=None,
                 handlers=None, mailbox=None):
        self.channel = channel
        self.mailbox = mailbox
        self.hostname = hostname
        self.state = state
        self.adjust_clock = self.mailbox.clock.adjust
        if handlers is None:
            handlers = {}
        self.handlers = handlers

    def Consumer(self, channel=None, no_ack=True, accept=None, **options):
        queue = self.mailbox.get_queue(self.hostname)

        def verify_exclusive(name, messages, consumers):
            if consumers:
                warnings.warn(W_PIDBOX_IN_USE.format(node=self))
        queue.on_declared = verify_exclusive

        conn = self.mailbox.connection
        channel_errors = conn.channel_errors if conn else ()
        try:
            return Consumer(
                channel or self.channel, [queue], no_ack=no_ack,
                accept=self.mailbox.accept if accept is None else accept,
                **options
            )
        except channel_errors as exc:
            if getattr(exc, 'code', 0) == 405:
                raise InconsistencyError(
                    W_PIDBOX_IN_USE.format(node=self)
                ) from exc
            raise

    def handler(self, fun):
        self.handlers[fun.__name__] = fun
        return fun

    def on_decode_error(self, message, exc):
        error('Cannot decode message: %r', exc, exc_info=1)

    def listen(self, channel=None, callback=None):
        consumer = self.Consumer(channel=channel,
                                 callbacks=[callback or self.handle_message],
                                 on_decode_error=self.on_decode_error)
        consumer.consume()
        return consumer

    def dispatch(self, method, arguments=None,
                 reply_to=None, ticket=None, **kwargs):
        arguments = arguments or {}
        debug('pidbox received method %s [reply_to:%s ticket:%s]',
              reprcall(method, (), kwargs=arguments), reply_to, ticket)
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, arguments)
        except SystemExit:
            raise
        except Exception as exc:
            error('pidbox command error: %r', exc, exc_info=1)
            reply = {'error': repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply},
                       exchange=reply_to['exchange'],
                       routing_key=reply_to['routing_key'],
                       ticket=ticket)
        return reply

    def handle(self, method, arguments=None):
        arguments = {} if not arguments else arguments
        return self.handlers[method](self.state, **arguments)

    def handle_call(self, method, arguments):
        return self.handle(method, arguments)

    def handle_cast(self, method, arguments):
        return self.handle(method, arguments)

    def handle_message(self, body, message=None):
        destination = body.get('destination')
        pattern = body.get('pattern')
        matcher = body.get('matcher')
        if message:
            self.adjust_clock(message.headers.get('clock') or 0)
        hostname = self.hostname
        run_dispatch = False
        if destination:
            if hostname in destination:
                run_dispatch = True
        elif pattern and matcher:
            if match(hostname, pattern, matcher):
                run_dispatch = True
        else:
            run_dispatch = True
        if run_dispatch:
            return self.dispatch(**body)
    dispatch_from_message = handle_message

    def reply(self, data, exchange, routing_key, ticket, **kwargs):
        self.mailbox._publish_reply(data, exchange, routing_key, ticket,
                                    channel=self.channel,
                                    serializer=self.mailbox.serializer)


class PidboxTicket:
    """Trackable ticket for a single pidbox reply request.

    A ticket ties together publishing the command, declaring the
    (temporary) reply queue and collecting the replies. Replies are only
    handed to the caller while the ticket is *open*. Once it has been
    closed -- reply limit reached, timeout, cancellation or connection
    close -- late replies are dropped and counted instead of extending
    the wait.
    """

    def __init__(self, ticket, queue, limit=None, timeout=1, callback=None):
        #: Ticket identifier (correlates replies with this request).
        self.ticket = ticket

        #: (bound or unbound) reply Queue used by this request.
        self.queue = queue

        #: Maximum number of replies to wait for (``None``: timeout only).
        self.limit = limit

        #: Timeout used while waiting for the next reply.
        self.timeout = timeout

        #: Optional per-reply callback.
        self.callback = callback

        #: Collected reply bodies.
        self.responses = []

        #: Number of replies that arrived after the ticket had closed.
        self.late_responses = 0

        #: Consumer collecting replies (set by :meth:`Mailbox._collect`).
        self.consumer = None

        self._open = False
        self._finished = False
        self._reason = None
        self._lock = threading.RLock()

    def open(self):
        """Open the ticket, accepting replies."""
        with self._lock:
            self._open = True
            self._reason = None

    def close(self, reason):
        """Close the ticket.

        The first close wins; return :const:`True` when the caller
        performed the transition. Subsequent calls are no-ops.
        """
        with self._lock:
            if not self._open:
                return False
            self._open = False
            self._reason = reason
            return True

    @property
    def is_open(self):
        return self._open

    @property
    def reason(self):
        """Terminal reason once the ticket has been closed."""
        return self._reason

    def begin_finish(self):
        """Mark the shutdown sequence as started.

        Return :const:`True` for the first caller only so consumer
        cancellation and queue deletion happen exactly once.
        """
        with self._lock:
            if self._finished:
                return False
            self._finished = True
            return True

    def note_late(self):
        """Record a reply that arrived after the ticket ended."""
        with self._lock:
            self.late_responses += 1

    def deliver(self, body):
        """Hand a matched reply to the waiting caller.

        Returns :const:`False` once the ticket is closed or the reply
        limit has been reached. Exceptions raised by the user callback
        are logged and never prevent further replies from being
        collected.
        """
        with self._lock:
            if not self._open:
                self.late_responses += 1
                return False
            if self.callback is not None:
                try:
                    self.callback(body)
                except Exception as exc:
                    logger.error(
                        'pidbox reply callback raised: %r', exc, exc_info=1,
                    )
            # The ticket may have been closed while the callback ran.
            if not self._open:
                self.late_responses += 1
                return False
            self.responses.append(body)
            return not (self.limit and len(self.responses) >= self.limit)


class Mailbox:
    """Process Mailbox."""

    node_cls = Node
    exchange_fmt = '%s.pidbox'
    reply_exchange_fmt = 'reply.%s.pidbox'

    #: Name of application.
    namespace = None

    #: Connection (if bound).
    connection = None

    #: Exchange type (usually direct, or fanout for broadcast).
    type = 'direct'

    #: mailbox exchange (init by constructor).
    exchange = None

    #: exchange to send replies to.
    reply_exchange = None

    #: Only accepts json messages by default.
    accept = ['json']

    #: Message serializer
    serializer = None

    def __init__(self, namespace,
                 type='direct', connection=None, clock=None,
                 accept=None, serializer=None, producer_pool=None,
                 queue_ttl=None, queue_expires=None,
                 queue_durable=False, queue_exclusive=True,
                 reply_queue_ttl=None, reply_queue_expires=10.0):
        self.namespace = namespace
        self.connection = connection
        self.type = type
        self.clock = LamportClock() if clock is None else clock
        self.exchange = self._get_exchange(self.namespace, self.type)
        self.reply_exchange = self._get_reply_exchange(self.namespace)
        # live request tickets: ticket id -> PidboxTicket
        self._tickets = {}
        self._tickets_lock = threading.Lock()
        # names of reply queues whose deletion failed and must be retried
        self._pending_cleanup = set()
        self._cleanup_lock = threading.Lock()
        self.accept = self.accept if accept is None else accept
        self.serializer = self.serializer if serializer is None else serializer
        self.queue_ttl = queue_ttl
        self.queue_expires = queue_expires
        self.queue_durable = queue_durable
        self.queue_exclusive = queue_exclusive
        self.reply_queue_ttl = reply_queue_ttl
        self.reply_queue_expires = reply_queue_expires
        self._producer_pool = producer_pool
        if queue_exclusive and queue_durable:
            raise ValueError(
                "queue_exclusive and queue_durable cannot both be True "
                "(exclusive queues are automatically deleted and cannot be durable).",
            )

    def __call__(self, connection):
        bound = copy(self)
        bound.connection = connection
        return bound

    def Node(self, hostname=None, state=None, channel=None, handlers=None):
        hostname = hostname or socket.gethostname()
        return self.node_cls(hostname, state, channel, handlers, mailbox=self)

    def call(self, destination, command, kwargs=None,
             timeout=None, callback=None, channel=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, destination,
                               reply=True, timeout=timeout,
                               callback=callback,
                               channel=channel)

    def cast(self, destination, command, kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, destination, reply=False)

    def abcast(self, command, kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, reply=False)

    def multi_call(self, command, kwargs=None, timeout=1,
                   limit=None, callback=None, channel=None):
        kwargs = {} if not kwargs else kwargs
        return self._broadcast(command, kwargs, reply=True,
                               timeout=timeout, limit=limit,
                               callback=callback,
                               channel=channel)

    def ticket_routing_key(self, ticket):
        """Routing key (and binding) of a ticket's reply queue."""
        return f'{self.oid}.{ticket}'

    def get_ticket_reply_queue(self, ticket):
        """Temporary reply queue dedicated to a single request ticket."""
        routing_key = self.ticket_routing_key(ticket)
        return Queue(
            f'{routing_key}.{self.reply_exchange.name}',
            exchange=self.reply_exchange,
            routing_key=routing_key,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.reply_queue_expires,
            message_ttl=self.reply_queue_ttl,
        )

    def get_reply_queue(self):
        oid = self.oid
        return Queue(
            f'{oid}.{self.reply_exchange.name}',
            exchange=self.reply_exchange,
            routing_key=oid,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.reply_queue_expires,
            message_ttl=self.reply_queue_ttl,
        )

    @cached_property
    def reply_queue(self):
        return self.get_reply_queue()

    def get_queue(self, hostname):
        return Queue(
            f'{hostname}.{self.namespace}.pidbox',
            exchange=self.exchange,
            durable=self.queue_durable,
            exclusive=self.queue_exclusive,
            auto_delete=not self.queue_durable,
            expires=self.queue_expires,
            message_ttl=self.queue_ttl,
        )

    @contextmanager
    def producer_or_acquire(self, producer=None, channel=None):
        if producer:
            yield producer
        elif self.producer_pool:
            with self.producer_pool.acquire() as producer:
                yield producer
        else:
            yield Producer(channel, auto_declare=False)

    def _publish_reply(self, reply, exchange, routing_key, ticket,
                       channel=None, producer=None, **opts):
        chan = channel or self.connection.default_channel
        exchange = Exchange(exchange, exchange_type='direct',
                            delivery_mode='transient',
                            durable=False)
        with self.producer_or_acquire(producer, chan) as producer:
            try:
                producer.publish(
                    reply, exchange=exchange, routing_key=routing_key,
                    declare=[exchange], headers={
                        'ticket': ticket, 'clock': self.clock.forward(),
                    }, retry=True,
                    **opts
                )
            except InconsistencyError:
                # queue probably deleted and no one is expecting a reply.
                pass

    def _publish(self, type, arguments, destination=None,
                 reply_ticket=None, channel=None, timeout=None,
                 serializer=None, producer=None, pattern=None, matcher=None):
        message = {'method': type,
                   'arguments': arguments,
                   'destination': destination,
                   'pattern': pattern,
                   'matcher': matcher}
        chan = channel or self.connection.default_channel
        exchange = self.exchange
        if reply_ticket:
            reply_queue = self.get_ticket_reply_queue(reply_ticket)
            maybe_declare(reply_queue(chan))
            message.update(ticket=reply_ticket,
                           reply_to={'exchange': self.reply_exchange.name,
                                     'routing_key': reply_queue.routing_key})
        serializer = serializer or self.serializer
        with self.producer_or_acquire(producer, chan) as producer:
            producer.publish(
                message, exchange=exchange.name, declare=[exchange],
                headers={'clock': self.clock.forward(),
                         'expires': time() + timeout if timeout else 0},
                serializer=serializer, retry=True,
            )

    def _broadcast(self, command, arguments=None, destination=None,
                   reply=False, timeout=1, limit=None,
                   callback=None, channel=None, serializer=None,
                   pattern=None, matcher=None):
        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError(
                'destination must be a list/tuple not {}'.format(
                    type(destination)))
        if (pattern is not None and not isinstance(pattern, str) and
                matcher is not None and not isinstance(matcher, str)):
            raise ValueError(
                'pattern and matcher must be '
                'strings not {}, {}'.format(type(pattern), type(matcher))
            )

        arguments = arguments or {}
        reply_ticket = reply and uuid() or None
        chan = channel or self.connection.default_channel

        # Set reply limit to number of destinations (if specified)
        if limit is None and destination:
            limit = destination and len(destination) or None

        serializer = serializer or self.serializer

        if not reply_ticket:
            # cast / abcast: no reply queue, no ticket to track.
            self._publish(command, arguments, destination=destination,
                          channel=chan, timeout=timeout,
                          serializer=serializer,
                          pattern=pattern, matcher=matcher)
            return

        # Retry queues left behind by earlier failed deletions, now that
        # the (possibly recovered/new) connection has a usable channel.
        self._retry_pending_cleanup(chan)

        # Publish, queue declaration and collection share one ticket that
        # is opened before any of them takes place.
        ticket = PidboxTicket(
            reply_ticket, self.get_ticket_reply_queue(reply_ticket)(chan),
            limit=limit, timeout=timeout, callback=callback,
        )
        self._register_ticket(ticket)
        ticket.open()
        try:
            maybe_declare(ticket.queue)
            self._publish(command, arguments, destination=destination,
                          reply_ticket=reply_ticket,
                          channel=chan,
                          timeout=timeout,
                          serializer=serializer,
                          pattern=pattern,
                          matcher=matcher)
        except BaseException:
            # Publication/declaration failure: terminate before unwinding.
            self._terminate(ticket, TICKET_CANCELLED, chan)
            raise

        return self._collect(ticket, channel=chan)

    def _collect(self, ticket, limit=None, timeout=1, callback=None,
                 channel=None, accept=None):
        # Allow callers to pass a raw ticket id: adopt it with a fresh
        # ticket-scoped reply queue.
        if isinstance(ticket, str):
            ticket = PidboxTicket(
                ticket, self.get_ticket_reply_queue(ticket),
                limit=limit, timeout=timeout, callback=callback,
            )
            self._register_ticket(ticket)
            ticket.open()

        if accept is None:
            accept = self.accept
        chan = channel or self.connection.default_channel
        if not ticket.queue.is_bound or ticket.queue.channel is not chan:
            ticket.queue = ticket.queue(chan)
        queue = ticket.queue
        consumer = Consumer(chan, [queue], accept=accept, no_ack=True)
        ticket.consumer = consumer
        adjust_clock = self.clock.adjust

        def on_message(body, message):
            # ticket header added in kombu 2.5
            header = message.headers.get
            adjust_clock(header('clock') or 0)
            expires = header('expires')
            if expires and time() > expires:
                return
            this_id = header('ticket')
            # Replies must carry our ticket id; an unknown/missing id is
            # never attributed to the current (next) command.
            if this_id is None or this_id != ticket.ticket:
                return
            if not ticket.is_open:
                # in-flight reply racing the consumer cancel: drop it.
                ticket.note_late()
                return
            if not ticket.deliver(body):
                # Limit reached (or ticket closed concurrently): close the
                # ticket only -- the drain loop exits and performs the
                # consumer cancel + queue deletion afterwards, so we never
                # issue a synchronous basic_cancel from inside a callback.
                ticket.close(TICKET_COMPLETE)

        consumer.register_callback(on_message)
        try:
            with consumer:
                while ticket.is_open:
                    try:
                        self.connection.drain_events(timeout=ticket.timeout)
                    except socket.timeout:
                        ticket.close(TICKET_TIMEOUT)
                        break
                    except self._collect_errors() as exc:
                        debug('pidbox connection lost while waiting for '
                              'replies: %r', exc)
                        ticket.close(TICKET_CONNECTION_CLOSED)
                        break
            return ticket.responses
        finally:
            # Runs exactly once: cancels the consumer and deletes (or
            # records for retry) the temporary reply queue. This also
            # covers caller cancellation, i.e. any other exception
            # unwinding through collect.
            ticket.close(TICKET_CANCELLED)
            self._finish(ticket, chan)

    def _collect_errors(self):
        conn = self.connection
        return conn.connection_errors + conn.channel_errors

    def _register_ticket(self, ticket):
        with self._tickets_lock:
            self._tickets[ticket.ticket] = ticket

    def _unregister_ticket(self, ticket):
        with self._tickets_lock:
            self._tickets.pop(ticket.ticket, None)

    def get_active_ticket(self, ticket_id):
        """Return the live ticket with the given id, if any."""
        with self._tickets_lock:
            return self._tickets.get(ticket_id)

    def _terminate(self, ticket, reason, chan=None):
        """Close ticket and run the shutdown sequence (idempotent)."""
        if ticket.close(reason):
            debug('pidbox ticket %s closed (%s)', ticket.ticket, reason)
        self._finish(
            ticket, chan if chan is not None else ticket.queue.channel,
        )

    def _finish(self, ticket, chan):
        """Stop consumer and delete the reply queue; runs exactly once.

        The ticket must already be closed, so replies racing the shutdown
        are dropped (and counted) instead of being collected.
        """
        if not ticket.begin_finish():
            return
        self._unregister_ticket(ticket)

        # Stop the consumer before deleting the temporary resource.
        consumer = ticket.consumer
        if consumer is not None:
            try:
                consumer.cancel()
            except Exception as exc:
                debug('pidbox consumer cancel failed: %r', exc)

        if chan is None or not self._delete_reply_queue(ticket.queue, chan):
            if chan is None:
                debug('pidbox reply queue %r left without a usable channel',
                      ticket.queue.name)
            with self._cleanup_lock:
                self._pending_cleanup.add(ticket.queue.name)
            logger.warning(
                'pidbox reply queue %r could not be deleted; it will be '
                'retried as soon as a connection is available.',
                ticket.queue.name,
            )

    def _delete_reply_queue(self, queue, chan):
        """Best-effort deletion of a temporary reply queue.

        Returns :const:`True` when the queue is deleted, or deletion is
        handled by the transport / cannot leak.
        """
        name = queue.name
        conn = self.connection
        try:
            chan.after_reply_message_received(name)
        except conn.connection_errors + conn.channel_errors as exc:
            debug('pidbox reply queue %r could not be deleted: %r',
                  name, exc)
            return False
        except Exception as exc:  # defensive: cleanup must never raise
            debug('pidbox reply queue %r cleanup failed: %r', name, exc)
            return False

        # AMQP transports leave after_reply_message_received as a no-op and
        # rely on queue flags; delete explicitly so a dropped connection
        # cannot leave the queue behind.
        if type(chan).after_reply_message_received is \
                StdChannel.after_reply_message_received:
            bound = queue if queue.is_bound else queue(chan)
            try:
                bound.delete()
            except conn.channel_errors as exc:
                if getattr(exc, 'code', None) == 404:
                    # Queue already gone (e.g. auto-delete after consumer
                    # cancel or exclusive removal with a dead connection).
                    return True
                debug('pidbox reply queue %r could not be deleted: %r',
                      name, exc)
                return False
            except conn.connection_errors as exc:
                debug('pidbox reply queue %r could not be deleted: %r',
                      name, exc)
                return False
            except Exception as exc:
                debug('pidbox reply queue %r cleanup failed: %r', name, exc)
                return False
        return True

    def _retry_pending_cleanup(self, chan):
        """Retry deleting queues whose earlier deletion failed.

        Called when the same connection has recovered or a new connection
        (with a fresh channel) is available.
        """
        with self._cleanup_lock:
            pending = list(self._pending_cleanup)
        if not pending:
            return
        conn = self.connection
        for name in pending:
            try:
                chan.queue_delete(queue=name)
            except conn.channel_errors as exc:
                if getattr(exc, 'code', None) == 404:
                    # Queue already gone.
                    pass
                else:
                    # The channel is suspect: keep the resource pending and
                    # wait for the next usable connection.
                    debug('pidbox pending cleanup aborted: %r', exc)
                    return
            except conn.connection_errors as exc:
                # Connection unusable again; keep everything pending.
                debug('pidbox pending cleanup aborted: %r', exc)
                return
            except Exception as exc:
                # Unknown failure: don't risk forgetting a real resource.
                debug('pidbox pending cleanup of %r failed: %r', name, exc)
                continue
            with self._cleanup_lock:
                self._pending_cleanup.discard(name)

    def _get_exchange(self, namespace, type):
        return Exchange(self.exchange_fmt % namespace,
                        type=type,
                        durable=False,
                        delivery_mode='transient')

    def _get_reply_exchange(self, namespace):
        return Exchange(self.reply_exchange_fmt % namespace,
                        type='direct',
                        durable=False,
                        delivery_mode='transient')

    @property
    def oid(self):
        return oid_from(self)

    @cached_property
    def producer_pool(self):
        return maybe_evaluate(self._producer_pool)
