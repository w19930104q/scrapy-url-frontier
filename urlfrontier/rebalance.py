#
# Resumable queue-level rebalancing between spider partitions.
#
# When the number of partitions (``SPIDER_PARTITION_ID=I/N``) changes, simply
# swapping the hash ring means the old and new crawl IDs can both serve the
# same host at the same time.  This module implements an online, crash-safe
# migration protocol that moves one *queue* (one normalized host key) at a
# time from the old partitions to the new ones, using only the public URL
# Frontier 2.3.1 gRPC API.
#
# Important API fact this protocol is built on:
#   * A blocked queue (``BlockQueueUntil``) is invisible to *every*
#     ``GetURLs`` call, including explicit-key reads, so a queue cannot be
#     drained while it is blocked.
#   * A URL returned by ``GetURLs`` is held "in process": it cannot be handed
#     to any other caller until its hold expires.
#   * ``PutURLs`` is an upsert keyed on the canonical URL, so re-sending the
#     same URL is idempotent (discovered duplicates are skipped, known items
#     overwrite).
#
# Per-queue state machine (block-first, so the host never has two active
# owners handing out its URLs):
#
#   1. BLOCKED   - BlockQueueUntil locks the source queue immediately.  From
#                  this instant neither the old partition nor anyone else can
#                  hand out the host's URLs: the host has zero (and then, once
#                  copying starts, exactly one) active owner.
#   2. DRAINED   - first wait for any URLs already held in process by a spider
#                  to age out (the block prevents new holds), then repeatedly
#                  open a short unblock window in which GetURLs atomically
#                  claims one page of due URLs (holding them so no other caller
#                  can fetch them); each URLInfo is copied verbatim to the
#                  destination crawl via PutURLs and the queue is re-blocked.
#                  Because the claim hold outlasts the queue's politeness
#                  delay, successive pages walk the whole queue.  An empty page
#                  caused solely by the politeness window is retried for a
#                  bounded grace period; genuinely future-dated URLs leave the
#                  queue blocked for a later re-run.
#   3. QUIESCED  - GetStats confirms every active source URL is held in
#                  process by this migrator and therefore already safe on the
#                  destination.  Future-dated URLs that cannot be read yet
#                  leave the queue blocked for a later re-run instead of being
#                  deleted with data still in them.
#   4. DELETED   - only now is DeleteQueue called.  Nothing is deleted before
#                  the destination has acknowledged every item.
#
# Every transition is appended to an fsync-ed write-ahead journal.  If the
# process is killed, re-running the same command resumes from the last
# recorded state; idempotent PutURLs means replay never duplicates a URL, and
# because the source queue is only deleted after a fully-claimed check,
# replay never loses one either.
#

import os
import json
import time
import errno
import fcntl
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional

from urlfrontier.grpc.urlfrontier_pb2 import (
    GetParams,
    URLInfo,
    URLItem,
    DiscoveredURLItem,
    Pagination,
    BlockQueueParams,
    QueueWithinCrawlParams,
)
from urlfrontier.distribution import HashRingDistributor, normalize_queue_key


logger = logging.getLogger("urlfrontier-rebalance")

# Journal event / state names.  Order defines the state machine progression.
EVENT_PLANNED = "planned"
EVENT_BLOCKED = "blocked"
EVENT_DRAINED = "drained"
EVENT_QUIESCED = "quiesced"
EVENT_DELETED = "deleted"

# Default duration for which a drained URL is held in process.  Long enough
# to copy a batch and converge, short enough that a crash re-run does not wait
# long for the claims to release.  Matches the claim window used in tests.
DEFAULT_CLAIM_HOLD_SECONDS = 300

# A blocked queue stays blocked for this long and the block is renewed while
# the migrator is actively waiting on it.  If the migrator vanishes, the
# queue unlocks itself automatically after this TTL.
DEFAULT_BLOCK_TTL_SECONDS = 3600

# Maximum number of drain pages copied per queue (per process invocation).
DEFAULT_MAX_DRAIN_ROUNDS = 10000


class RebalanceError(Exception):
    pass


class RebalanceInProgress(RebalanceError):
    """Raised when another migrator already holds the journal lock."""


@dataclass
class QueueMigration:
    """The migration record for a single queue key."""

    key: str
    source_crawl: str
    target_crawl: str
    state: str = EVENT_PLANNED
    # Number of destination-side acks accumulated across (re)runs.
    copied: int = 0
    # Drain pages processed (helps detect stuck queues).
    rounds: int = 0

    def as_event(self) -> dict:
        return {
            "key": self.key,
            "source_crawl": self.source_crawl,
            "target_crawl": self.target_crawl,
            "state": self.state,
            "copied": self.copied,
            "rounds": self.rounds,
        }


class MigrationJournal:
    """
    Append-only, crash-safe record of a rebalance run.

    A JSON-lines file (one event per line) plus an advisory file lock that
    ensures a single migrator process per spider/job.  Writes are fsync-ed
    before the corresponding remote operation is considered complete.
    """

    def __init__(self, journal_dir: str, spider_name: str, old_n: int, new_n: int):
        self.spider_name = spider_name
        self.old_n = old_n
        self.new_n = new_n
        self.dir = os.path.join(journal_dir, self.job_id())
        self.path = os.path.join(self.dir, "journal.jsonl")
        self.lock_path = os.path.join(self.dir, "migrator.lock")
        self._lock_fh = None
        # Serializes journal appends when several worker threads migrate
        # different queues concurrently.
        self._write_lock = threading.Lock()
        self.queues: Dict[str, QueueMigration] = {}

    def job_id(self) -> str:
        return f"rebalance-{self.spider_name}-{self.old_n}to{self.new_n}"

    # -- locking -----------------------------------------------------------

    def acquire(self) -> None:
        os.makedirs(self.dir, exist_ok=True)
        fh = open(self.lock_path, "a+")
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise RebalanceInProgress(
                    f"Another rebalance process already holds {self.lock_path}"
                )
            raise
        self._lock_fh = fh
        fh.write(f"pid={os.getpid()} started={int(time.time())}\n")
        fh.flush()
        self._reload()

    def release(self) -> None:
        if self._lock_fh is not None:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            self._lock_fh.close()
            self._lock_fh = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.release()

    # -- event log ---------------------------------------------------------

    def _reload(self) -> None:
        """Rebuild migration state from the journal (crash recovery)."""
        self.queues = {}
        if not os.path.exists(self.path):
            return
        with open(self.path, "r") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                self.queues[event["key"]] = QueueMigration(
                    key=event["key"],
                    source_crawl=event["source_crawl"],
                    target_crawl=event["target_crawl"],
                    state=event["state"],
                    copied=event.get("copied", 0),
                    rounds=event.get("rounds", 0),
                )

    def record(self, migration: QueueMigration, extra: Optional[dict] = None) -> None:
        event = migration.as_event()
        event["ts"] = int(time.time())
        if extra:
            event.update(extra)
        with self._write_lock:
            with open(self.path, "a") as fh:
                fh.write(json.dumps(event, sort_keys=True) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            self.queues[migration.key] = migration

    def pending(self) -> List[QueueMigration]:
        return [q for q in self.queues.values() if q.state != EVENT_DELETED]

    def is_complete(self) -> bool:
        return bool(self.queues) and not self.pending()


@dataclass
class RebalanceConfig:
    spider_name: str
    old_partitions: int
    new_partitions: int
    separator: str = "."
    journal_dir: str = "./.urlfrontier-rebalance"
    # GetURLs page size per drain request.
    max_urls_per_queue: int = 1000
    # Hold duration claimed on every URL the migrator reads.
    claim_hold_seconds: int = DEFAULT_CLAIM_HOLD_SECONDS
    # TTL of source-queue blocks; renewed while the migrator is alive.
    block_ttl_seconds: int = DEFAULT_BLOCK_TTL_SECONDS
    # Upper bound on waiting for in-process URLs to quiesce.
    in_process_timeout: int = 12 * 60 * 60
    max_drain_rounds: int = DEFAULT_MAX_DRAIN_ROUNDS
    # Polling interval while waiting for held / politeness-delayed URLs.
    poll_interval: float = 1.1
    # How long to keep retrying a queue whose GetURLs page comes back empty
    # while active URLs remain.  A blocked-but-active queue only reads empty
    # because it is inside its per-queue politeness window (the reference
    # default delay is 1s); genuinely future-dated URLs remain unreadable far
    # beyond this grace period and are then deferred to a later re-run.
    empty_read_grace_seconds: float = 30.0
    # Number of queues migrated concurrently.  The journal lock guarantees one
    # migrator process per job; within it, independent host queues move in
    # parallel (each queue still has at most one active owner at any instant).
    migration_workers: int = 4
    # Inject faults for testing: called with (phase, QueueMigration); raising
    # simulates a process crash at that point.
    fault: Optional[object] = None
    # Allows tests to pin the destination ring independently.
    extra_target_ring: Optional[HashRingDistributor] = None


class Rebalancer:
    """
    Drives the queue migration protocol over a URL Frontier gRPC stub.

    Depends only on the public gRPC API, so it works against the real
    crawler-commons URL Frontier (2.3.1+) and the in-process test fake.
    """

    def __init__(self, stub, config: RebalanceConfig):
        self.stub = stub
        self.config = config
        self._old_ring = HashRingDistributor(
            spider_name=config.spider_name,
            spider_id=1 if config.old_partitions > 1 else None,
            num_spiders=config.old_partitions,
            separator=config.separator,
        )
        self._new_ring = config.extra_target_ring or HashRingDistributor(
            spider_name=config.spider_name,
            spider_id=1 if config.new_partitions > 1 else None,
            num_spiders=config.new_partitions,
            separator=config.separator,
        )
        self._stop = threading.Event()

    # -- planning ----------------------------------------------------------

    def plan_for_key(self, key: str) -> Optional[QueueMigration]:
        """Return the migration record for a queue key, or None if it stays."""
        norm = normalize_queue_key(key)
        source = self._old_ring.hash_ring.get_node(norm)
        target = self._new_ring.hash_ring.get_node(norm)
        if source == target:
            return None
        return QueueMigration(key=key, source_crawl=source, target_crawl=target)

    def list_source_crawls(self) -> List[str]:
        return list(self._old_ring.nodes)

    def list_all_queues(self) -> List[str]:
        """Enumerate every queue key across the old-partition crawls."""
        keys: List[str] = []
        page_size = 100
        for crawl in self.list_source_crawls():
            start = 0
            while True:
                page = self.stub.ListQueues(
                    Pagination(
                        start=start,
                        size=page_size,
                        include_inactive=True,
                        crawlID=crawl,
                        local=False,
                    )
                )
                values = list(page.values)
                keys.extend(values)
                start += len(values)
                if not values or start >= page.total:
                    break
        seen = set()
        unique = []
        for k in keys:
            if k not in seen:
                seen.add(k)
                unique.append(k)
        return unique

    def build_plan(self) -> List[QueueMigration]:
        return [m for m in (self.plan_for_key(k) for k in self.list_all_queues()) if m]

    # -- protocol primitives ----------------------------------------------

    def _block(self, migration: QueueMigration, until: int) -> None:
        self.stub.BlockQueueUntil(
            BlockQueueParams(
                key=migration.key,
                crawlID=migration.source_crawl,
                time=until,
                local=False,
            )
        )

    def _get_stats(self, crawl: str, key: str):
        return self.stub.GetStats(
            QueueWithinCrawlParams(key=key, crawlID=crawl, local=False)
        )

    def _read_page(self, migration: QueueMigration) -> List[URLInfo]:
        """
        Read one page of due URLs.  Every returned URL is held in process for
        ``claim_hold_seconds`` by the service, which is what prevents a spider
        from being handed the same URL concurrently.
        """
        params = GetParams(
            max_urls_per_queue=self.config.max_urls_per_queue,
            max_queues=1,
            delay_requestable=self.config.claim_hold_seconds,
            key=migration.key,
            crawlID=migration.source_crawl,
        )
        return list(self.stub.GetURLs(params))

    @staticmethod
    def _rewrite(uf: URLInfo, target_crawl: str) -> URLItem:
        """
        Re-wrap a source URLInfo for the destination crawl.

        The full metadata (encoded Scrapy request / meta / original URL) is
        copied verbatim, so dont_filter, codecs and custom state survive.
        On the read path a URLInfo cannot distinguish discovered from known
        items, so items are re-submitted as discovered; the frontier upserts
        on the canonical URL and preserves anything already stored.
        """
        info = URLInfo(
            url=uf.url,
            key=uf.key,
            crawlID=target_crawl,
            metadata=dict(uf.metadata),
        )
        return URLItem(discovered=DiscoveredURLItem(info=info))

    def _write_batch(self, items: List[URLItem]) -> int:
        acknowledged = 0
        for ack in self.stub.PutURLs(iter(items)):
            # OK and SKIPPED (already known at the destination) both mean the
            # URL is safely stored there; only FAIL would need a retry.
            if ack.status in (0, 1):
                acknowledged += 1
            else:
                logger.error("PutURLs failure for item ID=%s status=%s", ack.ID, ack.status)
        return acknowledged

    def _inject_fault(self, phase: str, migration: QueueMigration) -> None:
        if self.config.fault is not None:
            self.config.fault(phase, migration)

    # -- per-queue state machine ------------------------------------------

    def migrate_queue(self, journal: MigrationJournal, migration: QueueMigration) -> QueueMigration:
        if migration.state == EVENT_DELETED:
            return migration

        # 1. Block first: from this instant the host is not served by anyone.
        if migration.state == EVENT_PLANNED:
            self._block(migration, int(time.time()) + self.config.block_ttl_seconds)
            self._inject_fault("after_block", migration)
            migration.state = EVENT_BLOCKED
            journal.record(migration)

        # 2. Let in-flight holds expire, then copy every due URL to the target.
        if migration.state in (EVENT_BLOCKED, EVENT_DRAINED):
            if not self._drain_blocked_queue(journal, migration):
                return migration
            migration.state = EVENT_DRAINED
            journal.record(migration)

        # 3. Verify the source holds only migrator-held, already-copied URLs.
        if migration.state == EVENT_DRAINED:
            stats = self._get_stats(migration.source_crawl, migration.key)
            if not self._source_fully_claimed(stats):
                # Not safe to delete (e.g. future-dated items remain); the
                # queue stays blocked for a later re-run.
                logger.warning(
                    "Queue %s not fully claimed (size=%s, inProcess=%s); "
                    "leaving blocked for a later re-run",
                    migration.key, stats.size, stats.inProcess,
                )
                return migration
            migration.state = EVENT_QUIESCED
            journal.record(migration)

        # 4. Safe to remove the source queue.
        self._inject_fault("before_delete", migration)
        self.stub.DeleteQueue(
            QueueWithinCrawlParams(
                key=migration.key,
                crawlID=migration.source_crawl,
                local=False,
            )
        )
        migration.state = EVENT_DELETED
        journal.record(migration)
        self._inject_fault("after_delete", migration)
        logger.info(
            "Queue %s migrated %s -> %s (%i URLs copied)",
            migration.key,
            migration.source_crawl,
            migration.target_crawl,
            migration.copied,
        )
        return migration

    def _drain_blocked_queue(
        self, journal: MigrationJournal, migration: QueueMigration
    ) -> bool:
        """
        Copy every active URL out of a blocked source queue while preserving
        the single-owner invariant.

        Phase 1 (quiesce): while the queue is blocked, no *new* GetURLs hold
        can ever be created, so the only in-process URLs are ones a spider
        already held when the block was applied.  Poll GetStats until those
        holds expire (or the timeout fires), renewing the block each round.

        Phase 2 (page): briefly unblock and read one page.  GetURLs
        atomically claims the due URLs for ``claim_hold_seconds``, during
        which they are copied to the destination via idempotent PutURLs and
        the queue is re-blocked.  The claim hold is deliberately longer than
        the queue's politeness delay, so already-claimed URLs stay out of the
        due set and successive pages walk the whole queue instead of
        re-reading its head.

        An empty page while active URLs remain is only possible inside the
        per-queue politeness window (or because the leftovers are genuinely
        future-dated); it is retried until ``empty_read_grace_seconds``
        elapses, after which the queue stays blocked for a later re-run.

        Returns True once every active source URL is held in process by this
        migrator and therefore already safe at the destination.
        """
        # -- Phase 1: wait for pre-block spider holds to age out -----------
        deadline = time.time() + self.config.in_process_timeout
        while not self._stop.is_set():
            self._block(migration, int(time.time()) + self.config.block_ttl_seconds)
            stats = self._get_stats(migration.source_crawl, migration.key)
            if stats.inProcess == 0 or stats.size == 0:
                break
            if time.time() > deadline:
                logger.warning(
                    "Queue %s did not quiesce in time (size=%s, inProcess=%s); "
                    "leaving for re-run",
                    migration.key, stats.size, stats.inProcess,
                )
                return False
            time.sleep(self.config.poll_interval)

        stats = self._get_stats(migration.source_crawl, migration.key)
        if stats.size == 0:
            return True

        # -- Phase 2: page the queue, copying every claimed URL ------------
        empty_deadline = None
        rounds = 0
        while not self._stop.is_set():
            if rounds >= self.config.max_drain_rounds:
                logger.warning(
                    "Queue %s exceeded %i drain rounds; leaving for re-run",
                    migration.key, self.config.max_drain_rounds,
                )
                return False

            # The only unblocked window of the whole migration: claim a page,
            # then re-block before doing anything else.
            self._unblock(migration)
            try:
                page = self._read_page(migration)
                self._inject_fault("after_read", migration)
            finally:
                self._block(migration, int(time.time()) + self.config.block_ttl_seconds)

            if not page:
                stats = self._get_stats(migration.source_crawl, migration.key)
                if stats.size == 0:
                    return True
                # Active URLs remain but none is readable: either the queue is
                # inside its politeness window or its leftovers are genuinely
                # future-dated.  Retry through the grace period so a mere
                # politeness pause never strands a migratable queue.
                now = time.time()
                if empty_deadline is None:
                    empty_deadline = now + self.config.empty_read_grace_seconds
                if now > empty_deadline:
                    logger.warning(
                        "Queue %s retains %i unreadable URL(s) after %.0fs; "
                        "leaving it blocked for a later re-run",
                        migration.key, stats.size,
                        self.config.empty_read_grace_seconds,
                    )
                    return False
                time.sleep(self.config.poll_interval)
                continue

            # A readable page resets the politeness-retry budget.
            empty_deadline = None
            items = [self._rewrite(uf, migration.target_crawl) for uf in page]
            done = self._write_batch(items)
            self._inject_fault("after_write", migration)
            migration.copied += done
            migration.rounds += 1
            rounds += 1
            journal.record(migration, extra={"phase": "drain", "batch": len(page)})

            stats = self._get_stats(migration.source_crawl, migration.key)
            if self._source_fully_claimed(stats):
                return True
            # More requestable URLs may exist beyond the page; the next
            # iteration unblocks again and claims them.
            if time.time() > deadline:
                logger.warning(
                    "Queue %s did not reach fully-claimed state; leaving for re-run",
                    migration.key,
                )
                return False
        return False

    def _unblock(self, migration: QueueMigration) -> None:
        # time=0 unblocks the queue per the API contract.
        self.stub.BlockQueueUntil(
            BlockQueueParams(
                key=migration.key,
                crawlID=migration.source_crawl,
                time=0,
                local=False,
            )
        )

    @staticmethod
    def _source_fully_claimed(stats) -> bool:
        """
        True when every active URL still in the source queue is held in
        process.  Under the block-first protocol the only possible holder is
        this migrator, and every claimed URL has already been copied to the
        destination, so the queue can be deleted without losing data.
        """
        if stats.size == 0:
            return True
        return stats.inProcess > 0 and stats.inProcess >= stats.size

    # -- top level ---------------------------------------------------------

    def run(self, journal: MigrationJournal) -> List[QueueMigration]:
        """
        Execute (or resume) the full rebalance and return final states.

        Independent host queues are migrated concurrently (one gRPC call
        stream each); each individual queue still moves through the strict
        block-first state machine, so a host has at most one active owner at
        any instant while unaffected hosts keep serving throughout.
        """
        planned = {m.key: m for m in self.build_plan()}
        # Merge journaled progress so a re-run resumes rather than restarts.
        for key, previous in journal.queues.items():
            if key in planned:
                planned[key].state = previous.state
                planned[key].copied = previous.copied
                planned[key].rounds = previous.rounds
        if not journal.queues:
            for migration in planned.values():
                journal.record(migration, extra={"phase": "plan"})

        migrations = list(planned.values())
        if not migrations:
            return []

        workers = max(1, min(self.config.migration_workers, len(migrations)))
        if workers == 1:
            # Keep the simple, direct call path (and its tracebacks) for the
            # single-worker case, including fault injection in tests.
            results = []
            for migration in migrations:
                if self._stop.is_set():
                    break
                results.append(self.migrate_queue(journal, migration))
            return results

        results: Dict[str, QueueMigration] = {}
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self.migrate_queue, journal, migration): migration.key
                for migration in migrations
            }
            for future in as_completed(futures):
                # propagate any worker exception (e.g. an injected crash fault)
                migration = future.result()
                results[migration.key] = migration
        return [results[k] for k in planned if k in results]

    def stop(self) -> None:
        self._stop.set()
