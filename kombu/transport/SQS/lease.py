"""Consumer-scoped visibility lease for the SQS transport.

SQS hides a received message for the duration of the queue's visibility
timeout.  When processing takes longer than that timeout the message
becomes visible again and may be delivered to another worker even though
the original consumer is still busy with it.

This module implements an opt-in *visibility lease*: while a message is
unacknowledged the transport periodically calls
``ChangeMessageVisibility(Batch)`` to extend the visibility window on its
receipt handle.  The lease is

* **opt-in per consumer/queue** — disabled by default (see
  the ``visibility_lease`` transport option) and never applied to
  ``no_ack`` messages, which are deleted on receipt;
* **per message** — every unacked receipt handle owns an independent
  timer, so prefetch capacity and slow queues can never block renewal of
  messages consumed from another queue;
* **batched per queue** — renewals that fall due together are sent in a
  single ``ChangeMessageVisibilityBatch`` request (at most 10 entries per
  request, the SQS batch limit); a failure reported for a single batch
  entry is isolated to that message;
* **bounded** — transient AWS/credential/connection errors are retried
  with exponential backoff up to a limit, and an optional maximum lease
  duration caps how long processing may be protected;
* **deterministic on settlement** — ``ack``, ``reject``, ``basic_cancel``
  and channel close cancel the timer, and follow-up actions that also
  touch the receipt handle (e.g. the backoff policy on reject or message
  restore on close) are deferred until an in-flight renewal response has
  been observed, so they are never reordered behind a renewal.

When a receipt handle becomes invalid, renewal is permanently rejected,
retries are exhausted or the maximum lease duration is reached the lease
is simply stopped: the message is handed back to SQS's own redelivery and
to the transport's existing ack/reject recovery semantics.
"""

from __future__ import annotations

import threading
from time import monotonic

from kombu.log import get_logger

logger = get_logger(__name__)

#: SQS ChangeMessageVisibilityBatch accepts at most 10 entries/request.
BATCH_MAX_ENTRIES = 10

#: SQS allows a VisibilityTimeout between 0 and 43200 seconds.
MAX_VISIBILITY_TIMEOUT = 43200

# Terminal reasons reported when a lease stops without being acked.
REASON_MAX_DURATION = 'max-lease-duration-reached'
REASON_RETRIES_EXHAUSTED = 'renewal-retries-exhausted'
REASON_INVALID_RECEIPT_HANDLE = 'invalid-receipt-handle'
REASON_PERMANENT_FAILURE = 'permanent-renewal-failure'

#: Batch failure codes that can never succeed for the same receipt handle.
_PERMANENT_ERROR_CODES = frozenset({
    'ReceiptHandleIsInvalid',
    'InvalidParameterValue',
})

_UNSET = object()

_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_FACTOR = 2.0
_DEFAULT_BACKOFF_MAX = 30.0


class _LeaseEntry:
    """A single in-flight visibility lease."""

    __slots__ = (
        'tag', 'receipt_handle', 'queue', 'qname', 'queue_url', 'config',
        'started', 'next_due', 'timer', 'in_flight', 'stopped', 'retries',
        'settle_callbacks',
    )

    def __init__(self, tag, receipt_handle, queue, qname, queue_url,
                 config, started, next_due):
        self.tag = tag
        self.receipt_handle = receipt_handle
        # AMQP queue name (as consumed) / canonical SQS queue name / URL
        self.queue = queue
        self.qname = qname
        self.queue_url = queue_url
        self.config = config
        self.started = started
        self.next_due = next_due
        self.timer = None
        self.in_flight = False
        self.stopped = False
        self.retries = 0
        self.settle_callbacks = []


class VisibilityLease:
    """Track and renew SQS visibility timeouts for unacked messages."""

    def __init__(self, channel):
        self.channel = channel
        self.hub = getattr(channel, 'hub', None)
        self._entries = {}
        self._lock = threading.RLock()
        self.closed = False
        self._config_cache = {}

    # ------------------------------------------------------------------
    # configuration
    # ------------------------------------------------------------------

    def _raw_global_setting(self):
        return self.channel.transport_options.get('visibility_lease', False)

    def config_for(self, qname):
        """Resolve the lease configuration for a canonical queue name.

        Returns ``None`` when the lease is disabled for that queue.
        """
        cached = self._config_cache.get(qname, _UNSET)
        if cached is not _UNSET:
            return cached

        raw = self._raw_global_setting()
        qraw = _UNSET
        if self.channel.predefined_queues:
            qraw = self.channel.predefined_queues.get(qname, {}).get(
                'visibility_lease', _UNSET)

        if qraw is _UNSET:
            if not raw:
                config = None
            else:
                config = self._build_config(
                    raw if isinstance(raw, dict) else {})
        elif not qraw:
            # Explicitly disabled for this predefined queue.
            config = None
        else:
            merged = dict(raw if isinstance(raw, dict) else {})
            if isinstance(qraw, dict):
                merged.update(qraw)
            config = self._build_config(merged)

        self._config_cache[qname] = config
        return config

    def _build_config(self, overrides):
        timeout = overrides.get('timeout')
        if timeout is None:
            timeout = self.channel.visibility_timeout
        timeout = int(float(timeout))
        # 0 is only useful for immediate redelivery, never for a renewal.
        timeout = max(1, min(MAX_VISIBILITY_TIMEOUT, timeout))

        interval = overrides.get('interval')
        if interval is None:
            # Renew halfway through the visibility window.
            interval = max(1.0, timeout / 2.0)
        else:
            interval = max(1.0, float(interval))

        max_duration = overrides.get('max_duration')
        if max_duration is not None:
            max_duration = float(max_duration)
            if max_duration <= 0:
                max_duration = None

        max_retries = int(overrides.get(
            'max_retries', _DEFAULT_MAX_RETRIES))
        max_retries = max(0, max_retries)

        return {
            'interval': interval,
            'timeout': timeout,
            'max_duration': max_duration,
            'max_retries': max_retries,
            'backoff_factor': float(overrides.get(
                'backoff_factor', _DEFAULT_BACKOFF_FACTOR)),
            'backoff_max': float(overrides.get(
                'backoff_max', _DEFAULT_BACKOFF_MAX)),
        }

    # ------------------------------------------------------------------
    # lease lifecycle
    # ------------------------------------------------------------------

    def add(self, message, delivery_tag):
        """Start tracking the message that was just appended to QoS."""
        if self.closed or self.hub is None:
            return

        delivery_info = getattr(message, 'delivery_info', None)
        if not delivery_info:
            return
        sqs_message = delivery_info.get('sqs_message')
        routing_key = delivery_info.get('routing_key')
        if not sqs_message or not routing_key:
            return
        receipt_handle = sqs_message.get('ReceiptHandle')
        if not receipt_handle:
            return

        qname = self.channel.canonical_queue_name(routing_key)
        config = self.config_for(qname)
        if config is None:
            return

        now = monotonic()
        entry = _LeaseEntry(
            tag=delivery_tag,
            receipt_handle=receipt_handle,
            queue=routing_key,
            qname=qname,
            queue_url=delivery_info.get('sqs_queue'),
            config=config,
            started=now,
            next_due=now + config['interval'],
        )

        with self._lock:
            # A receipt handle should never be tracked twice; replace any
            # stale entry (its timer is cancelled).
            old = self._entries.get(delivery_tag)
            if old is not None and old is not entry:
                self._cancel_timer_locked(old)
                old.stopped = True
            self._entries[delivery_tag] = entry
            self._arm_locked(entry, config['interval'])

    def stop(self, delivery_tag, then=None):
        """Stop the lease for ``delivery_tag``.

        ``then`` (if provided) is invoked once the lease no longer owns
        the receipt handle: immediately when no renewal request is in
        flight, otherwise from the in-flight response callback.  This
        guarantees follow-up visibility changes (reject backoff, restore)
        cannot be reordered behind a renewal.
        """
        run_now = False
        with self._lock:
            entry = self._entries.get(delivery_tag)
            if entry is None:
                run_now = True
            else:
                entry.stopped = True
                self._cancel_timer_locked(entry)
                if entry.in_flight:
                    if then is not None:
                        entry.settle_callbacks.append(then)
                else:
                    self._entries.pop(delivery_tag, None)
                    run_now = True
        if run_now and then is not None:
            self._invoke(then)

    def stop_queue(self, queue):
        """Stop every active lease consumed from ``queue`` (basic_cancel)."""
        with self._lock:
            for entry in list(self._entries.values()):
                if entry.queue == queue:
                    entry.stopped = True
                    self._cancel_timer_locked(entry)
                    if not entry.in_flight:
                        self._entries.pop(entry.tag, None)

    def stop_all(self):
        """Stop all leases (channel close)."""
        with self._lock:
            self.closed = True
            for entry in list(self._entries.values()):
                entry.stopped = True
                self._cancel_timer_locked(entry)
                if not entry.in_flight:
                    self._entries.pop(entry.tag, None)

    def active_count(self):
        with self._lock:
            return len(self._entries)

    # ------------------------------------------------------------------
    # scheduling
    # ------------------------------------------------------------------

    def _arm_locked(self, entry, delay):
        entry.timer = self.hub.call_later(delay, self._on_due, entry)

    def _cancel_timer_locked(self, entry):
        if entry.timer is not None:
            try:
                entry.timer.cancel()
            except Exception:  # pragma: no cover - defensive
                logger.debug('Could not cancel lease timer', exc_info=True)
            entry.timer = None

    def _on_due(self, entry):
        with self._lock:
            if self.closed or entry.stopped:
                return
            entry.timer = None
            due = self._collect_due_locked(entry)

        # Group by queue and split into SQS-sized batches; every group and
        # chunk is an independent request so failures stay isolated.
        groups = {}
        for e in due:
            groups.setdefault(e.qname, []).append(e)
        for group in groups.values():
            for i in range(0, len(group), BATCH_MAX_ENTRIES):
                self._send_batch(group[i:i + BATCH_MAX_ENTRIES])

    def _collect_due_locked(self, entry):
        now = monotonic()
        grace = min(1.0, max(0.0, entry.config['interval']) / 4.0)
        selected = [entry]
        for candidate in self._entries.values():
            if candidate is entry:
                continue
            if candidate.qname != entry.qname:
                continue
            if candidate.stopped or candidate.in_flight:
                continue
            if candidate.timer is None:
                continue
            if candidate.next_due <= now + grace:
                selected.append(candidate)
        for e in selected:
            e.in_flight = True
            if e is not entry:
                self._cancel_timer_locked(e)
        return selected

    # ------------------------------------------------------------------
    # renewal requests
    # ------------------------------------------------------------------

    def _timeout_for(self, entry, now):
        """VisibilityTimeout to request, honoring the max lease duration."""
        timeout = entry.config['timeout']
        max_duration = entry.config['max_duration']
        if max_duration is None:
            return timeout
        remaining = entry.started + max_duration - now
        if remaining <= 0:
            return None
        return max(0, min(timeout, int(remaining + 0.999999)))

    def _send_batch(self, entries):
        now = monotonic()
        api_entries = []
        live = []
        for entry in entries:
            timeout = self._timeout_for(entry, now)
            if timeout is None:
                self._terminate(entry, REASON_MAX_DURATION)
                continue
            api_entries.append({
                'Id': str(len(api_entries)),
                'ReceiptHandle': entry.receipt_handle,
                'VisibilityTimeout': timeout,
            })
            live.append(entry)

        if not live:
            return

        id_to_entry = {str(i): entry for i, entry in enumerate(live)}
        qname = live[0].qname
        queue_url = live[0].queue_url

        def on_result(result, _ids=id_to_entry):
            self._on_batch_result(_ids, result)

        def on_error(exc, _entries=live):
            self._on_batch_error(_entries, exc)

        try:
            # Resolve the async connection per sweep so refreshed
            # (e.g. STS) credentials/connections are picked up and the
            # same lease simply continues.
            connection = self.channel.asynsqs(queue=qname)
            request = connection.change_message_visibility_batch_from_handles(
                queue_url, api_entries, callback=on_result)
            # vine invokes ``on_error(exc)`` when the response transform
            # raises (e.g. non-200/network error) and suppresses the
            # re-raise once an errback is attached.
            request.on_error = on_error
            # A mocked/synchronous transport may have already failed the
            # promise before we could attach the errback.
            if getattr(request, 'failed', False):
                on_error(request.reason)
        except Exception as exc:
            logger.warning(
                'Failed to issue SQS visibility lease renewal for %d '
                'message(s) from %r: %r', len(live), qname, exc)
            self._on_batch_error(live, exc)

    def _on_batch_result(self, id_to_entry, result):
        result = result or {}
        successful = result.get('Successful') or []
        failed = result.get('Failed') or []

        for item in successful:
            entry = id_to_entry.get(item.get('Id'))
            if entry is not None:
                self._renewed(entry)

        for item in failed:
            entry = id_to_entry.get(item.get('Id'))
            if entry is None:
                continue
            code = item.get('Code')
            if self._is_permanent_item_failure(item):
                reason = (REASON_INVALID_RECEIPT_HANDLE
                          if code in _PERMANENT_ERROR_CODES
                          else REASON_PERMANENT_FAILURE)
                logger.warning(
                    'SQS visibility lease for receipt handle %r rejected '
                    'permanently (%s: %s); stopping renewal and falling '
                    'back to SQS redelivery.',
                    entry.receipt_handle, code, item.get('Message'))
                self._terminate(entry, reason)
            else:
                logger.info(
                    'SQS visibility lease renewal for receipt handle %r '
                    'failed transiently (%s); retrying with backoff.',
                    entry.receipt_handle, code)
                self._retry(entry, str(code))

    @staticmethod
    def _is_permanent_item_failure(item):
        # Within a batch response SQS marks request faults (invalid
        # receipt handle, bad parameter) with SenderFault=True; throttling
        # and server-side failures report SenderFault=False.
        return bool(item.get('SenderFault')) or \
            item.get('Code') in _PERMANENT_ERROR_CODES

    def _on_batch_error(self, entries, exc):
        # The whole request failed (network/5xx/throttling/credentials).
        # Credential or connection refresh happens on the next sweep via
        # channel.asynsqs(); treat this as transient and retry bounded.
        logger.warning(
            'SQS visibility lease batch renewal failed (%r); retrying %d '
            'message(s) with bounded backoff.', exc, len(entries))
        for entry in entries:
            self._retry(entry, exc)

    # ------------------------------------------------------------------
    # per-entry continuation
    # ------------------------------------------------------------------

    def _renewed(self, entry):
        with self._lock:
            callbacks = self._finish_request_locked(entry)
            current = self._entries.get(entry.tag)
            if current is not entry:
                # Superseded while in flight; leave the current mapping
                # untouched.  Any settlement callback still runs below.
                pass
            elif self.closed or entry.stopped:
                self._entries.pop(entry.tag, None)
            else:
                entry.retries = 0
                config = entry.config
                if config['max_duration'] is not None and \
                        monotonic() - entry.started >= config['max_duration']:
                    callbacks += self._terminate_locked(
                        entry, REASON_MAX_DURATION)
                else:
                    entry.next_due = monotonic() + config['interval']
                    self._arm_locked(entry, config['interval'])
        self._run_callbacks(callbacks)

    def _retry(self, entry, cause=None):
        with self._lock:
            callbacks = self._finish_request_locked(entry)
            current = self._entries.get(entry.tag)
            if current is not entry:
                # Superseded while in flight; nothing to reschedule.
                pass
            elif self.closed or entry.stopped:
                self._entries.pop(entry.tag, None)
            else:
                config = entry.config
                if entry.retries >= config['max_retries']:
                    logger.warning(
                        'Giving up SQS visibility lease for receipt handle %r '
                        'after %d transient renewal failure(s) (%r); stopping '
                        'renewal and falling back to SQS redelivery.',
                        entry.receipt_handle, entry.retries, cause)
                    callbacks += self._terminate_locked(
                        entry, REASON_RETRIES_EXHAUSTED)
                else:
                    entry.retries += 1
                    delay = min(
                        config['backoff_max'],
                        config['backoff_factor'] * (2 ** (entry.retries - 1)),
                    )
                    entry.next_due = monotonic() + delay
                    logger.debug(
                        'Rescheduling SQS visibility lease renewal for %r in '
                        '%.1fs (retry %d/%d).',
                        entry.receipt_handle, delay, entry.retries,
                        config['max_retries'])
                    self._arm_locked(entry, delay)
        self._run_callbacks(callbacks)

    def _terminate(self, entry, reason):
        with self._lock:
            callbacks = self._terminate_locked(entry, reason)
        self._run_callbacks(callbacks)

    def _terminate_locked(self, entry, reason):
        """Stop and discard an entry while the lock is held.

        Returns any settlement callbacks queued by concurrent ``stop``
        calls so the caller can run them after releasing the lock.
        """
        callbacks = self._finish_request_locked(entry)
        if not entry.stopped:
            entry.stopped = True
            self._cancel_timer_locked(entry)
            logger.info(
                'Stopped SQS visibility lease for receipt handle %r: %s',
                entry.receipt_handle, reason)
        if self._entries.get(entry.tag) is entry:
            self._entries.pop(entry.tag, None)
        return callbacks

    def _finish_request_locked(self, entry):
        entry.in_flight = False
        callbacks = entry.settle_callbacks
        entry.settle_callbacks = []
        return callbacks

    @staticmethod
    def _invoke(callback):
        try:
            callback()
        except Exception:
            logger.exception(
                'Visibility lease settlement callback raised an exception')

    def _run_callbacks(self, callbacks):
        for callback in callbacks or ():
            self._invoke(callback)
