#
# In-process fake of the crawler-commons URL Frontier gRPC service.
#
# It implements the subset of the 2.3.1 API used by both the scheduler and the
# rebalancer, deliberately mirroring the reference service's semantics
# (blocked queues, held/in-process URLs, discovered-vs-known upserts, queue
# stats and pagination) so regression tests exercise the real protocol rather
# than a mock that always agrees with the caller.
#

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from urllib.parse import urlparse

import grpc

from urlfrontier.grpc import urlfrontier_pb2 as pb
from urlfrontier.grpc.urlfrontier_pb2_grpc import (
    URLFrontierServicer,
    add_URLFrontierServicer_to_server,
)


@dataclass
class FakeURL:
    """One stored URL item. Equality/identity is the canonical URL."""

    url: str
    crawl_id: str
    key: str
    metadata: dict
    discovered: bool = True
    next_fetch: int = 0
    # Epoch seconds until which GetURLs must not hand this URL out again.
    held_until: int = -1
    # Known items with refetchable_from_date=0 are "completed" (done forever).
    completed: bool = False

    def to_urlinfo(self) -> pb.URLInfo:
        return pb.URLInfo(
            url=self.url,
            key=self.key,
            crawlID=self.crawl_id,
            metadata=dict(self.metadata),
        )


@dataclass
class FakeQueue:
    key: str
    crawl_id: str
    urls: Dict[str, FakeURL] = field(default_factory=dict)
    blocked_until: int = -1
    delay: int = -1
    last_produced: int = 0

    def count_active(self, now: int) -> int:
        return sum(1 for u in self.urls.values() if not u.completed)

    def in_process(self, now: int) -> int:
        return sum(1 for u in self.urls.values() if u.held_until > now)

    def due(self, now: int) -> List[FakeURL]:
        out = [
            u
            for u in self.urls.values()
            if not u.completed and u.held_until <= now and u.next_fetch <= now
        ]
        out.sort(key=lambda u: (u.next_fetch, u.url))
        return out


class FakeURLFrontierServicer(URLFrontierServicer):
    """
    Thread-safe in-memory URL Frontier.  Stores state in
    ``self.queues[(crawl_id, key)]`` and records every mutation in
    ``self.events`` so tests can assert on the exact call sequence.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self.queues: Dict[tuple, FakeQueue] = {}
        self.active: Dict[str, bool] = {}
        self.events: List[str] = []
        # If set, BlockQueueUntil calls are blocked on this event (used to
        # test concurrent next_request behaviour deterministically).
        self.hold_blocks: Optional[threading.Event] = None

    # -- helpers -----------------------------------------------------------

    def _queue(self, crawl_id: str, key: str, create: bool = False) -> Optional[FakeQueue]:
        q = self.queues.get((crawl_id, key))
        if q is None and create:
            q = FakeQueue(key=key, crawl_id=crawl_id)
            self.queues[(crawl_id, key)] = q
        return q

    def snapshot(self) -> Dict[tuple, FakeQueue]:
        with self._lock:
            return dict(self.queues)

    def all_urls(self) -> List[FakeURL]:
        with self._lock:
            return [u for q in self.queues.values() for u in q.urls.values()]

    # -- service API -------------------------------------------------------

    def ListCrawls(self, request, context):
        with self._lock:
            values = sorted({q.crawl_id for q in self.queues.values()})
        return pb.StringList(values=values)

    def ListNodes(self, request, context):
        return pb.StringList(values=["fake-node"])

    def ListQueues(self, request, context):
        now = int(time.time())
        with self._lock:
            matching = [
                q
                for q in self.queues.values()
                if q.crawl_id == request.crawlID
                and (
                    request.include_inactive
                    or (q.blocked_until < now and q.count_active(now) > 0)
                )
            ]
            matching.sort(key=lambda q: q.key)
            total = len(matching)
            page = matching[request.start : request.start + request.size]
            return pb.QueueList(
                values=[q.key for q in page],
                total=total,
                start=request.start,
                size=len(page),
                crawlID=request.crawlID,
            )

    def GetStats(self, request, context):
        now = int(time.time())
        with self._lock:
            if request.key:
                queues = [q] if (q := self._queue(request.crawlID, request.key)) else []
            else:
                queues = [q for q in self.queues.values() if q.crawl_id == request.crawlID]
            size = sum(q.count_active(now) for q in queues)
            in_process = sum(q.in_process(now) for q in queues)
            num_queues = len(queues)
            completed = sum(
                1 for q in queues for u in q.urls.values() if u.completed
            )
        return pb.Stats(
            size=size,
            inProcess=in_process,
            numberOfQueues=num_queues,
            crawlID=request.crawlID,
            counts={"completed": completed, "active_queues": num_queues},
        )

    def BlockQueueUntil(self, request, context):
        if self.hold_blocks is not None:
            self.hold_blocks.wait(timeout=30)
        with self._lock:
            self.events.append(
                f"block:{request.crawlID}:{request.key}:{request.time}"
            )
            q = self._queue(request.crawlID, request.key, create=True)
            q.blocked_until = request.time
        return pb.Empty()

    def SetDelay(self, request, context):
        with self._lock:
            if not request.key:
                for q in self.queues.values():
                    if q.crawl_id == request.crawlID:
                        q.delay = request.delay_requestable
            else:
                q = self._queue(request.crawlID, request.key, create=True)
                q.delay = request.delay_requestable
        return pb.Empty()

    def SetActive(self, request, context):
        with self._lock:
            self.active[request.crawlID or ""] = request.state
        return pb.Empty()

    def GetActive(self, request, context):
        with self._lock:
            state = self.active.get(request.crawlID or "", True)
        return pb.Boolean(state=state)

    def DeleteQueue(self, request, context):
        with self._lock:
            self.events.append(f"delete:{request.crawlID}:{request.key}")
            q = self.queues.pop((request.crawlID, request.key), None)
            removed = q.count_active(int(time.time())) if q else 0
        return pb.Long(value=removed)

    def DeleteCrawl(self, request, context):
        with self._lock:
            doomed = [k for k in self.queues if k[0] == request.value]
            count = sum(len(self.queues[k].urls) for k in doomed)
            for k in doomed:
                del self.queues[k]
        return pb.Long(value=count)

    def GetURLs(self, request, context):
        now = int(time.time())
        # The reference service returns nothing while deactivated.
        if not self.active.get(request.crawlID or "", True):
            return
        delay = request.delay_requestable or 30
        with self._lock:
            if request.key:
                q = self._queue(request.crawlID, request.key)
                if q is None:
                    return
                # Mirrors AbstractFrontierService: a blocked queue is
                # invisible to GetURLs even when read by explicit key.
                if q.blocked_until >= now:
                    return
                # delay == -1 means "use service default"; with last_produced
                # initialized to 0 and now ~1.7e9, a fresh queue is always
                # immediately eligible, matching AbstractFrontierService.
                effective_delay = q.delay if q.delay >= 0 else 1
                if q.last_produced + effective_delay > now:
                    return
                candidates = q.due(now)[: request.max_urls_per_queue]
                if candidates:
                    q.last_produced = now
                for item in candidates:
                    item.held_until = now + delay
                    yield item.to_urlinfo()
                return

            # No specific key: round-robin over queues of the crawl.
            queues = [
                q
                for q in list(self.queues.values())
                if q.crawl_id == request.crawlID and q.blocked_until < now
            ]
            sent_queues = 0
            for q in queues:
                if sent_queues >= request.max_queues:
                    break
                effective_delay = q.delay if q.delay >= 0 else 1
                if q.last_produced + effective_delay > now:
                    continue
                candidates = q.due(now)[: request.max_urls_per_queue]
                if not candidates:
                    continue
                q.last_produced = now
                for item in candidates:
                    item.held_until = now + delay
                    yield item.to_urlinfo()
                sent_queues += 1

    def PutURLs(self, request_iterator, context):
        """
        Process the whole request stream first, then return the acks.

        A streaming gRPC handler that yields while the client is still
        writing the request stream deadlocks once the HTTP/2 flow-control
        window fills; collecting first mirrors a request/response-style
        handler and keeps the call deterministic.
        """
        now = int(time.time())
        acks = []
        for item in request_iterator:
            with self._lock:
                if item.HasField("discovered"):
                    info = item.discovered.info
                    discovered = True
                    next_fetch = now
                    completed = False
                else:
                    info = item.known.info
                    discovered = False
                    next_fetch = item.known.refetchable_from_date
                    completed = next_fetch == 0
                # Mirror the reference service: an unset/empty queue key
                # defaults to the URL's hostname (provideMissingKey).
                key = info.key or urlparse(info.url).hostname or ""
                if not key:
                    acks.append(pb.AckMessage(ID=item.ID, status=pb.AckMessage.SKIPPED))
                    continue
                q = self._queue(info.crawlID, key, create=True)
                existing = q.urls.get(info.url)
                if existing is not None:
                    if discovered:
                        # Already known: discovered items are skipped.
                        self.events.append(f"put:skip:{info.crawlID}:{info.url}")
                        acks.append(pb.AckMessage(ID=item.ID, status=pb.AckMessage.SKIPPED))
                        continue
                    # Known item: overwrite.
                    existing.metadata = dict(info.metadata)
                    existing.discovered = False
                    existing.next_fetch = next_fetch
                    existing.completed = completed
                    existing.held_until = -1
                else:
                    q.urls[info.url] = FakeURL(
                        url=info.url,
                        crawl_id=info.crawlID,
                        key=key,
                        metadata=dict(info.metadata),
                        discovered=discovered,
                        next_fetch=next_fetch,
                        completed=completed,
                    )
                self.events.append(f"put:ok:{info.crawlID}:{info.url}")
                acks.append(pb.AckMessage(ID=item.ID, status=pb.AckMessage.OK))
        return iter(acks)


def start_fake_frontier(host: str = "127.0.0.1", port: int = 0):
    """
    Start the fake service on a real gRPC server (in-process, random port by
    default).  Returns ``(server, servicer, target_address)``.
    """
    server = grpc.server(
        ThreadPoolExecutor(max_workers=8),
        options=(("grpc.so_reuseport", 0),),
    )
    servicer = FakeURLFrontierServicer()
    add_URLFrontierServicer_to_server(servicer, server)
    bound = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, servicer, f"{host}:{bound}"
