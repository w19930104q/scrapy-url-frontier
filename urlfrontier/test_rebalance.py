#
# Regression tests for resumable partition rebalancing.
#
# Runs entirely against the in-process fake URL Frontier (see testing.py), no
# external service required:
#
#     python -m unittest urlfrontier.test_rebalance -v
#

import json
import os
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

import grpc
from scrapy.http.request import Request

from urlfrontier.grpc.urlfrontier_pb2 import (
    AckMessage,
    BlockQueueParams,
    GetParams,
    QueueWithinCrawlParams,
    StringList,
    URLInfo,
    URLItem,
    DiscoveredURLItem,
    KnownURLItem,
)
from urlfrontier.grpc.urlfrontier_pb2_grpc import URLFrontierStub
from urlfrontier.distribution import HashRingDistributor, normalize_queue_key
from urlfrontier.rebalance import (
    Rebalancer,
    RebalanceConfig,
    MigrationJournal,
    QueueMigration,
    EVENT_BLOCKED,
    EVENT_DRAINED,
    EVENT_QUIESCED,
    EVENT_DELETED,
    RebalanceInProgress,
)
from urlfrontier.testing import start_fake_frontier
from urlfrontier.scheduler import URLFrontierScheduler


SPIDER = "example"
# Small, fixed host set.  d.com / e.com / github.com move under BOTH 2->3
# and 3->2; a.com / b.com / example.org stay put under both, so every test
# has migrating and unaffected hosts without hashing dozens of queues.
HOSTS = [
    "a.com",
    "b.com",
    "example.org",
    "d.com",
    "e.com",
    "github.com",
]


def make_ring(n):
    return HashRingDistributor(
        spider_name=SPIDER,
        spider_id=1 if n > 1 else None,
        num_spiders=n,
    )


class _FakeStats:
    """Minimal stand-in for Scrapy's StatsCollector used by the scheduler."""

    def __init__(self):
        self._values = {}

    def inc_value(self, key, count=1, start=0):
        self._values[key] = self._values.get(key, start) + count

    def get_value(self, key, default=None):
        return self._values.get(key, default)

    def set_value(self, key, value):
        self._values[key] = value


class _FakeSettings:
    def __init__(self, settings):
        self._settings = settings

    def __getitem__(self, k):
        return self._settings[k]

    def get(self, k, default=None):
        return self._settings.get(k, default)

    def getbool(self, k, default=False):
        return bool(self._settings.get(k, default))

    def getint(self, k, default=0):
        return int(self._settings.get(k, default))


def make_scheduler_for(address, partition_id, spider_name=SPIDER):
    """Build a URLFrontierScheduler wired to the given (fake) endpoint."""
    from urllib.parse import urlparse

    class _Downloader:
        @staticmethod
        def _get_slot_key(request, spider):
            # Mirrors Scrapy: the per-host download slot key is the hostname.
            return urlparse(request.url).hostname

    class _Engine:
        downloader = _Downloader()

    class _Crawler:
        engine = _Engine()

    class FakeSpider:
        name = spider_name
        crawler = _Crawler()

    class FakeCrawler:
        stats = _FakeStats()
        spider = FakeSpider()

        class signals:
            @staticmethod
            def connect(*a, **kw):
                pass

    settings = {
        "SCHEDULER_URLFRONTIER_ENDPOINT": address,
        "SCHEDULER_DEBUG": False,
        "SPIDER_PARTITION_ID": partition_id,
        "SCHEDULER_URLFRONTIER_GETURLS_MAX_QUEUES": 1,
        # Keep the test scheduler's in-process hold short; the production
        # default is ten hours, which would stall migration convergence.
        "SCHEDULER_URLFRONTIER_DEFAULT_DELAY_REQUESTABLE": 2,
    }
    fake = FakeCrawler()
    fake.settings = _FakeSettings(settings)
    sched = URLFrontierScheduler(crawler=fake)
    sched._channel = grpc.insecure_channel(address)
    sched._stub = URLFrontierStub(sched._channel)
    return sched


def ring_owner(ring, key):
    return ring.hash_ring.get_node(normalize_queue_key(key))


def seed_queue(stub, crawl_id, key, urls, known=False):
    """Push a batch of URLs into one queue, retaining scrapy metadata."""
    items = []
    for i, url in enumerate(urls):
        request = Request(
            url=url,
            meta={"spiderid": crawl_id, "depth": i, "dont_filter": (i % 3 == 0)},
        )
        info = URLInfo(
            url=url,
            key=key,
            crawlID=crawl_id,
            metadata={
                "meta": StringList(values=[json.dumps(request.meta)]),
                "original_url": StringList(values=[request.url]),
            },
        )
        if known:
            items.append(
                URLItem(known=KnownURLItem(info=info, refetchable_from_date=int(time.time())))
            )
        else:
            items.append(URLItem(discovered=DiscoveredURLItem(info=info)))
    acks = list(stub.PutURLs(iter(items)))
    assert all(a.status in (AckMessage.OK, AckMessage.SKIPPED) for a in acks)


def all_queue_keys(servicer):
    return sorted({(q.crawl_id, q.key) for q in servicer.snapshot().values()})


class RebalanceTestBase(unittest.TestCase):
    def setUp(self):
        self.server, self.servicer, self.address = start_fake_frontier()
        self.channel = grpc.insecure_channel(self.address)
        self.stub = URLFrontierStub(self.channel)
        self.journal_dir = tempfile.mkdtemp(prefix="uf-rebalance-")

    def tearDown(self):
        self.channel.close()
        self.server.stop(None)
        shutil.rmtree(self.journal_dir, ignore_errors=True)

    def seed_partitioned_crawl(self, n, urls_per_host=4):
        """Populate every host queue under an N-partition layout."""
        ring = make_ring(n)
        expected = {}
        for host in HOSTS:
            crawl = ring_owner(ring, host)
            urls = [f"https://{host}/path/{i}?q={i}" for i in range(urls_per_host)]
            seed_queue(self.stub, crawl, host, urls)
            expected[host] = (crawl, urls)
        return expected

    def config(self, old_n, new_n, **kw):
        defaults = dict(
            spider_name=SPIDER,
            old_partitions=old_n,
            new_partitions=new_n,
            journal_dir=self.journal_dir,
            # Short timings so held-URL / quiesce paths run quickly in tests.
            in_process_timeout=30,
            # The claim hold must outlast the fake service's 1s per-queue
            # politeness delay, otherwise successive drain pages re-claim the
            # queue head instead of walking every URL.
            claim_hold_seconds=10,
            poll_interval=0.05,
            empty_read_grace_seconds=3,
        )
        defaults.update(kw)
        return RebalanceConfig(**defaults)

    def run_rebalance(self, old_n, new_n, **kw):
        cfg = self.config(old_n, new_n, **kw)
        journal = MigrationJournal(self.journal_dir, SPIDER, old_n, new_n)
        with journal:
            results = Rebalancer(self.stub, cfg).run(journal)
        return journal, results

    # -- assertions --------------------------------------------------------

    def assert_no_url_lost_or_duplicated(self, expected):
        """Every seeded canonical URL exists exactly once across the frontier."""
        seen = {}
        for queue in self.servicer.snapshot().values():
            for url, item in queue.urls.items():
                self.assertNotIn(url, seen, f"URL duplicated across queues: {url}")
                seen[url] = (item.crawl_id, item.key)
        self.assertEqual(
            sorted(seen), sorted(u for _, urls in expected.values() for u in urls),
            "set of URLs changed during migration",
        )

    def assert_all_on_new_ring(self, new_n):
        ring = make_ring(new_n)
        for queue in self.servicer.snapshot().values():
            for item in queue.urls.values():
                self.assertEqual(
                    item.crawl_id,
                    ring_owner(ring, item.key),
                    f"{item.url} still under old partition {item.crawl_id}",
                )

    def assert_metadata_preserved(self, expected):
        for host, (crawl, urls) in expected.items():
            for url in urls:
                found = None
                for queue in self.servicer.snapshot().values():
                    if url in queue.urls:
                        found = queue.urls[url]
                        break
                self.assertIsNotNone(found, f"{url} missing after migration")
                meta = json.loads(found.metadata["meta"].values[0])
                self.assertEqual(meta["spiderid"], crawl)
                self.assertIn("depth", meta)
                # original_url metadata must survive too:
                self.assertEqual(found.metadata["original_url"].values[0], url)


class ScaleUpTests(RebalanceTestBase):
    def test_two_to_three(self):
        expected = self.seed_partitioned_crawl(2)
        old_ring, new_ring = make_ring(2), make_ring(3)
        moving = [h for h in HOSTS if ring_owner(old_ring, h) != ring_owner(new_ring, h)]
        self.assertTrue(moving)

        journal, results = self.run_rebalance(2, 3)

        self.assertTrue(journal.is_complete())
        self.assertEqual(
            sorted(r.key for r in results), sorted(moving),
            "exactly the hosts whose ring placement changed must migrate",
        )
        self.assertTrue(all(r.state == EVENT_DELETED for r in results))
        # Unaffected hosts remain on their original crawl and were untouched:
        for host in HOSTS:
            if host not in moving:
                owner = ring_owner(old_ring, host)
                self.assertIn((owner, host), all_queue_keys(self.servicer))
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(3)
        self.assert_metadata_preserved(expected)

    def test_one_to_two(self):
        # Special case: old layout has no partition suffix at all.
        expected = self.seed_partitioned_crawl(1)
        journal, results = self.run_rebalance(1, 2)
        self.assertTrue(journal.is_complete())
        # Every host moves away from the bare spider-name crawl.
        self.assertEqual(sorted(r.key for r in results), sorted(HOSTS))
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(2)

    def test_noop_when_layout_unchanged(self):
        self.seed_partitioned_crawl(2)
        journal, results = self.run_rebalance(2, 2)
        self.assertEqual(results, [])
        # Nothing was blocked or deleted:
        self.assertFalse(any(e.startswith("block:") for e in self.servicer.events))
        self.assertFalse(any(e.startswith("delete:") for e in self.servicer.events))


class ScaleDownTests(RebalanceTestBase):
    def test_three_to_two(self):
        expected = self.seed_partitioned_crawl(3)
        old_ring, new_ring = make_ring(3), make_ring(2)
        moving = [h for h in HOSTS if ring_owner(old_ring, h) != ring_owner(new_ring, h)]
        self.assertTrue(moving)

        journal, results = self.run_rebalance(3, 2)

        self.assertTrue(journal.is_complete())
        self.assertEqual(sorted(r.key for r in results), sorted(moving))
        self.assertTrue(all(r.state == EVENT_DELETED for r in results))
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(2)
        self.assert_metadata_preserved(expected)
        # The removed partition's crawl must end up empty/gone:
        remaining_crawls = {q.crawl_id for q in self.servicer.snapshot().values()}
        self.assertNotIn(f"{SPIDER}.3", remaining_crawls)

    def test_two_to_one(self):
        expected = self.seed_partitioned_crawl(2)
        journal, results = self.run_rebalance(2, 1)
        self.assertTrue(journal.is_complete())
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(1)
        remaining_crawls = {q.crawl_id for q in self.servicer.snapshot().values()}
        self.assertEqual(remaining_crawls, {SPIDER})


class RepeatedRunTests(RebalanceTestBase):
    def test_repeat_after_completion_is_noop(self):
        self.seed_partitioned_crawl(2)
        journal1, results1 = self.run_rebalance(2, 3)
        events_after_first = list(self.servicer.events)

        journal2, results2 = self.run_rebalance(2, 3)

        self.assertTrue(journal1.is_complete())
        # Plan now finds nothing on the old ring that still needs moving.
        self.assertEqual(results2, [])
        self.assertEqual(self.servicer.events, events_after_first)

    def test_concurrent_migrators_rejected(self):
        self.seed_partitioned_crawl(2)
        journal = MigrationJournal(self.journal_dir, SPIDER, 2, 3)
        journal.acquire()
        try:
            with self.assertRaises(RebalanceInProgress):
                MigrationJournal(self.journal_dir, SPIDER, 2, 3).acquire()
        finally:
            journal.release()


class CrashRecoveryTests(RebalanceTestBase):
    """Simulate crashes at every phase and prove replay converges."""

    def _crash_once_at(self, phase, counter):
        def fault(where, migration):
            if where == phase and counter["n"] == 0:
                counter["n"] += 1
                raise RuntimeError(f"simulated crash at {phase}")
        return fault

    def test_crash_after_block_and_resume(self):
        self._crash_resume("after_block")

    def test_crash_mid_drain_and_resume(self):
        # Small batches so a multi-page queue is interrupted mid-drain.
        self._crash_resume("after_read", max_urls_per_queue=2)
        self._crash_resume("after_write", max_urls_per_queue=2)

    def test_crash_before_delete_and_resume(self):
        self._crash_resume("before_delete")

    def test_crash_after_delete_and_resume(self):
        self._crash_resume("after_delete")

    def _crash_resume(self, phase, max_urls_per_queue=1000):
        # Each crash scenario gets its own journal directory: reusing one
        # directory across two runs would reload the first run's DELETED
        # records and short-circuit the second run's state machine before the
        # injected fault can fire.
        journal_dir = tempfile.mkdtemp(dir=self.journal_dir, prefix=f"crash-{phase}-")
        expected = self.seed_partitioned_crawl(2, urls_per_host=6)
        counter = {"n": 0}
        cfg = self.config(
            2, 3,
            journal_dir=journal_dir,
            fault=self._crash_once_at(phase, counter),
            max_urls_per_queue=max_urls_per_queue,
        )
        journal = MigrationJournal(journal_dir, SPIDER, 2, 3)
        with journal:
            with self.assertRaises(RuntimeError):
                Rebalancer(self.stub, cfg).run(journal)

        # The journal survived and remembers partial progress:
        self.assertTrue(os.path.exists(journal.path))
        # Simulate a fresh process restarting: a new journal object acquires
        # the lock (the previous process's lock was released by `with`).
        reloaded = MigrationJournal(journal_dir, SPIDER, 2, 3)

        # Replay with no fault injection must converge:
        clean_cfg = self.config(
            2, 3,
            journal_dir=journal_dir,
            max_urls_per_queue=max_urls_per_queue,
        )
        with reloaded:
            results = Rebalancer(self.stub, clean_cfg).run(reloaded)
        self.assertTrue(reloaded.is_complete())
        self.assertTrue(all(r.state == EVENT_DELETED for r in results))
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(3)
        self.assert_metadata_preserved(expected)

    def test_drain_is_idempotent_under_replay(self):
        # Even if every copied URL is copied again from scratch (e.g. journal
        # lost between write and delete), the frontier dedupes by URL.
        expected = self.seed_partitioned_crawl(2)
        _, _ = self.run_rebalance(2, 3)
        # Wipe the journal and re-run against the same server state:
        shutil.rmtree(self.journal_dir, ignore_errors=True)
        os.makedirs(self.journal_dir)
        journal, results = self.run_rebalance(2, 3)
        # Remaining source queues are already empty/migrated; nothing lost:
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(3)


class FutureDatedURLTests(RebalanceTestBase):
    def test_future_dated_queue_is_left_blocked_not_deleted(self):
        # A URL whose refetchable_from_date is in the future cannot be returned
        # by any GetURLs.  The migrator must defer this queue instead of
        # deleting it and losing the work.
        old_ring, new_ring = make_ring(2), make_ring(3)
        moving_host = next(
            h for h in HOSTS if ring_owner(old_ring, h) != ring_owner(new_ring, h)
        )
        source = ring_owner(old_ring, moving_host)
        future = int(time.time()) + 3600
        # Known item with a future refetch date -> stored but never due.
        info = URLInfo(
            url=f"https://{moving_host}/later",
            key=moving_host,
            crawlID=source,
            metadata={"x": StringList(values=["y"])},
        )
        item = URLItem(known=KnownURLItem(info=info, refetchable_from_date=future))
        acks = list(self.stub.PutURLs(iter([item])))
        self.assertEqual(acks[0].status, AckMessage.OK)

        # A short grace period keeps the test fast: the item stays unreadable.
        journal, results = self.run_rebalance(
            2, 3, empty_read_grace_seconds=0.5, poll_interval=0.05
        )

        # The migration did not reach DELETED and remains journaled as pending.
        self.assertFalse(journal.is_complete())
        record = journal.queues[moving_host]
        self.assertNotEqual(record.state, EVENT_DELETED)
        self.assertIn(record, journal.pending())

        # Nothing was lost: the future-dated URL is still in the source queue,
        # and nothing was written to the destination.
        source_q = self.servicer.snapshot()[(source, moving_host)]
        self.assertIn(f"https://{moving_host}/later", source_q.urls)
        target = ring_owner(new_ring, moving_host)
        self.assertNotIn((target, moving_host), self.servicer.snapshot())

        # Re-running later (once the URL becomes due) must converge fully.
        info2 = URLInfo(
            url=f"https://{moving_host}/later",
            key=moving_host,
            crawlID=source,
            metadata={"x": StringList(values=["y"])},
        )
        due_item = URLItem(
            known=KnownURLItem(info=info2, refetchable_from_date=int(time.time()))
        )
        list(self.stub.PutURLs(iter([due_item])))  # overwrite -> now due
        journal2, _ = self.run_rebalance(2, 3)
        self.assertTrue(journal2.is_complete())
        self.assertNotIn((source, moving_host), self.servicer.snapshot())
        self.assertIn((target, moving_host), self.servicer.snapshot())


class InProcessHeldURLTests(RebalanceTestBase):
    def test_held_urls_are_not_lost(self):
        # A spider already fetched (holds) one URL when migration starts.
        expected = self.seed_partitioned_crawl(2, urls_per_host=3)
        old_ring, new_ring = make_ring(2), make_ring(3)
        moving_host = next(
            h for h in HOSTS if ring_owner(old_ring, h) != ring_owner(new_ring, h)
        )
        source = ring_owner(old_ring, moving_host)
        held = list(
            self.stub.GetURLs(
                GetParams(
                    max_urls_per_queue=1,
                    max_queues=1,
                    delay_requestable=2,
                    key=moving_host,
                    crawlID=source,
                )
            )
        )
        self.assertEqual(len(held), 1)

        journal, results = self.run_rebalance(2, 3, in_process_timeout=10)
        self.assertTrue(journal.is_complete())
        # The held URL ends up on the destination, not deleted with the queue:
        self.assert_no_url_lost_or_duplicated(expected)
        self.assert_all_on_new_ring(3)


class ConcurrentNextRequestTests(RebalanceTestBase):
    """
    The critical ownership guarantee, asserted deterministically against the
    fake service:

      * while a host is being claimed/blocked, its source partition hands out
        none of its URLs (no duplicate active owner);
      * queues whose ring placement did not change keep serving throughout;
      * after handoff only the destination partition serves the host.

    The migration is paused at a fixed journal phase so the assertions run
    against a stable state instead of racing the worker thread.
    """

    def _make_scheduler(self, partition_id):
        return make_scheduler_for(self.address, partition_id)

    def _get_urls(self, crawl_id, key=None):
        return list(
            self.stub.GetURLs(
                GetParams(
                    max_urls_per_queue=10,
                    max_queues=1,
                    delay_requestable=2,
                    key=key,
                    crawlID=crawl_id,
                )
            )
        )

    def test_blocked_queue_is_not_served_by_any_partition(self):
        self.seed_partitioned_crawl(2, urls_per_host=3)
        old_ring, new_ring = make_ring(2), make_ring(3)

        # Pause the migrator the instant the first source queue is blocked.
        reached = threading.Event()
        release = threading.Event()

        def fault(phase, migration):
            if phase == "after_block":
                reached.set()
                release.wait(timeout=15)

        cfg = self.config(2, 3, fault=fault)
        journal = MigrationJournal(self.journal_dir, SPIDER, 2, 3)
        migrator = Rebalancer(self.stub, cfg)

        def migrate():
            with journal:
                migrator.run(journal)

        worker = threading.Thread(target=migrate)
        worker.start()
        self.assertTrue(reached.wait(timeout=15))
        try:
            blocked = journal.pending()[0]
            target = ring_owner(new_ring, blocked.key)
            # The source queue is locked: neither a keyed read nor a crawl-wide
            # read from the source partition may surface its URLs.
            self.assertEqual(
                self._get_urls(blocked.source_crawl, blocked.key), []
            )
            served = {u.key for u in self._get_urls(blocked.source_crawl)}
            self.assertNotIn(blocked.key, served)
            # Block-first ordering means the destination has not received the
            # data yet either: during the locked phase the host has *no* active
            # owner, never two.  It becomes the single owner only after the
            # drain completes.
            self.assertEqual(self._get_urls(target, blocked.key), [])
        finally:
            release.set()
            worker.join(timeout=30)
        self.assertFalse(worker.is_alive())
        self.assertTrue(journal.is_complete())
        self.assert_all_on_new_ring(3)

        # Post-handoff: the destination is now the sole owner of the host.
        for qm in journal.queues.values():
            self.assertEqual(qm.state, EVENT_DELETED)
        self.assertTrue(self._get_urls(target, blocked.key))
        self.assertEqual(self._get_urls(blocked.source_crawl, blocked.key), [])

    def test_concurrent_next_request_never_double_serves_a_host(self):
        # Same guarantee, exercised through the real scheduler.next_request()
        # path with concurrent callers hammering both old partitions.
        self.seed_partitioned_crawl(2, urls_per_host=2)
        old_ring, new_ring = make_ring(2), make_ring(3)
        moving = [h for h in HOSTS if ring_owner(old_ring, h) != ring_owner(new_ring, h)]
        moving_sources = {ring_owner(old_ring, h) for h in moving}

        reached = threading.Event()
        release = threading.Event()

        def fault(phase, migration):
            if phase == "after_block":
                reached.set()
                release.wait(timeout=15)

        cfg = self.config(2, 3, fault=fault)
        journal = MigrationJournal(self.journal_dir, SPIDER, 2, 3)
        migrator = Rebalancer(self.stub, cfg)

        def migrate():
            with journal:
                migrator.run(journal)

        worker = threading.Thread(target=migrate)
        worker.start()
        self.assertTrue(reached.wait(timeout=15))

        # Only the queue that has reached the blocked phase is under the
        # single-owner guarantee at this instant; queues not yet processed are
        # still legitimately served by their old partitions.
        blocked_key = journal.pending()[0].key

        schedulers = [self._make_scheduler(f"{i}/2") for i in (1, 2)]
        violations = []

        def pull(scheduler):
            for _ in range(15):
                try:
                    req = scheduler.next_request()
                except grpc.RpcError:
                    continue
                if req is not None:
                    host = normalize_queue_key(req.url.split("/")[2])
                    if host == blocked_key:
                        violations.append((scheduler.spider_id, req.url))

        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(pull, s) for s in schedulers]
                [f.result(timeout=30) for f in futures]
        finally:
            release.set()
            worker.join(timeout=30)
            for s in schedulers:
                s._channel.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(
            violations, [],
            "the blocked/migrating host was still served by its old partition",
        )
        self.assertTrue(journal.is_complete())

    def test_unaffected_hosts_keep_serving_throughout(self):
        self.seed_partitioned_crawl(2, urls_per_host=2)
        old_ring, new_ring = make_ring(2), make_ring(3)
        staying = [h for h in HOSTS if ring_owner(old_ring, h) == ring_owner(new_ring, h)]
        self.assertTrue(staying)

        reached = threading.Event()
        release = threading.Event()

        def fault(phase, migration):
            if phase == "after_block":
                reached.set()
                release.wait(timeout=15)

        cfg = self.config(2, 3, fault=fault)
        journal = MigrationJournal(self.journal_dir, SPIDER, 2, 3)
        migrator = Rebalancer(self.stub, cfg)

        def migrate():
            with journal:
                migrator.run(journal)

        worker = threading.Thread(target=migrate)
        worker.start()
        self.assertTrue(reached.wait(timeout=15))
        try:
            # While another queue is mid-migration, every unaffected host must
            # still be readable from its (unchanged) owner partition.
            for host in staying:
                owner = ring_owner(old_ring, host)
                urls = self._get_urls(owner, host)
                self.assertTrue(urls, f"unaffected host {host} stopped serving")
        finally:
            release.set()
            worker.join(timeout=30)
        self.assertFalse(worker.is_alive())
        self.assertTrue(journal.is_complete())

    def test_only_destination_serves_host_after_handoff(self):
        expected = self.seed_partitioned_crawl(2, urls_per_host=2)
        old_ring, new_ring = make_ring(2), make_ring(3)
        moving = [h for h in HOSTS if ring_owner(old_ring, h) != ring_owner(new_ring, h)]

        journal, _ = self.run_rebalance(2, 3)
        self.assertTrue(journal.is_complete())

        for host in moving:
            old_owner = ring_owner(old_ring, host)
            new_owner = ring_owner(new_ring, host)
            # Old partition no longer holds the queue at all.
            self.assertEqual(self._get_urls(old_owner, host), [])
            # New partition owns and serves it.
            self.assertTrue(self._get_urls(new_owner, host))
        self.assert_no_url_lost_or_duplicated(expected)

    def test_different_hosts_migrate_in_parallel(self):
        # With several workers, multiple independent host queues must be in
        # the blocked/migrating phase at the same instant.  The fault hook
        # parks every queue right after it blocks its source; the gate only
        # opens once every moving queue has arrived.  A serial implementation
        # can never satisfy the gate (the first queue would wait forever for
        # queues it alone is preventing from starting), so the waits time out
        # and the test fails.
        self.seed_partitioned_crawl(2, urls_per_host=2)
        moving = [
            h for h in HOSTS if ring_owner(make_ring(2), h) != ring_owner(make_ring(3), h)
        ]
        self.assertGreaterEqual(len(moving), 2)

        arrived = set()
        arrival_lock = threading.Lock()
        all_arrived = threading.Event()

        def fault(phase, migration):
            if phase == "after_block":
                with arrival_lock:
                    arrived.add(migration.key)
                    if len(arrived) == len(moving):
                        all_arrived.set()
                # Park this queue until its peers have all blocked theirs.
                self.assertTrue(
                    all_arrived.wait(timeout=20),
                    "queues never blocked concurrently (migration looks serial)",
                )

        cfg = self.config(2, 3, fault=fault, migration_workers=len(moving))
        journal = MigrationJournal(self.journal_dir, SPIDER, 2, 3)
        with journal:
            Rebalancer(self.stub, cfg).run(journal)

        self.assertEqual(arrived, set(moving))
        self.assertTrue(journal.is_complete())
        self.assert_all_on_new_ring(3)


class SchedulerCompatTests(RebalanceTestBase):
    """dont_filter / known-item semantics still behave through the scheduler."""

    def _scheduler(self, partition_id):
        return make_scheduler_for(self.address, partition_id)

    def test_dont_filter_request_roundtrip(self):
        url = "https://dfilter.example/path"
        # Use the scheduler for whichever partition the ring routes the host
        # to, so the next_request() crawl ID actually owns the queue.
        owner = ring_owner(make_ring(2), "dfilter.example")
        partition_id = f"{owner.rsplit('.', 1)[1]}/2"
        sched = self._scheduler(partition_id)
        request = Request(url, dont_filter=True)
        self.assertTrue(sched.enqueue_request(request))
        # dont_filter items go in as known/immediately-refetchable, so they
        # must be returned by GetURLs:
        got = sched.next_request()
        self.assertIsNotNone(got)
        self.assertEqual(got.url, url)
        sched._channel.close()

    def test_discovered_request_roundtrip_and_partition(self):
        host = "partitioncheck.example"
        ring = make_ring(2)
        owner = ring_owner(ring, host)
        owner_idx = owner.rsplit(".", 1)[1]
        other_idx = "2" if owner_idx == "1" else "1"

        # The owning partition's scheduler enqueues and can read it back.
        owner_sched = self._scheduler(f"{owner_idx}/2")
        self.assertTrue(
            owner_sched.enqueue_request(Request(f"https://{host}/x"))
        )
        self.assertIsNotNone(owner_sched.next_request())

        # The non-owning partition must never see the host's queue.
        other_sched = self._scheduler(f"{other_idx}/2")
        self.assertIsNone(other_sched.next_request())
        owner_sched._channel.close()
        other_sched._channel.close()


class JournalTests(unittest.TestCase):
    def test_normalize_queue_key(self):
        self.assertEqual(normalize_queue_key("Example.COM "), "example.com")
        self.assertIsNone(normalize_queue_key(None))

    def test_journal_records_each_transition(self):
        tmp = tempfile.mkdtemp()
        try:
            journal = MigrationJournal(tmp, SPIDER, 2, 3)
            with journal:
                m = QueueMigration(key="h.com", source_crawl="example.1",
                                   target_crawl="example.3")
                for state in (EVENT_BLOCKED, EVENT_DRAINED, EVENT_QUIESCED, EVENT_DELETED):
                    m.state = state
                    journal.record(m)
            with open(journal.path) as fh:
                lines = [json.loads(l) for l in fh if l.strip()]
            self.assertEqual([l["state"] for l in lines],
                             [EVENT_BLOCKED, EVENT_DRAINED, EVENT_QUIESCED, EVENT_DELETED])
            # Reload reconstructs the final state:
            reloaded = MigrationJournal(tmp, SPIDER, 2, 3)
            reloaded.acquire()
            self.assertEqual(reloaded.queues["h.com"].state, EVENT_DELETED)
            self.assertTrue(reloaded.is_complete())
            reloaded.release()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
