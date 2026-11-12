'''
Command-line interface for URLFrontier
'''

import os
import sys
import json
import logging
import argparse

from scrapy.http.request import Request

import grpc
from urlfrontier.grpc.urlfrontier_pb2_grpc import URLFrontierStub
from urlfrontier.grpc.urlfrontier_pb2 import AnyCrawlID, GetParams, URLInfo, URLItem, DiscoveredURLItem, KnownURLItem, StringList, QueueWithinCrawlParams, Pagination, Local, DeleteCrawlMessage, Active

from urlfrontier.distribution import HashRingDistributor, urlInfo_to_request
from urlfrontier.rebalance import Rebalancer, RebalanceConfig, MigrationJournal, RebalanceInProgress

logging.basicConfig(level=logging.WARNING, format='%(asctime)s: %(levelname)s - %(name)s - %(message)s')

logger = logging.getLogger(__name__)

# Fix the local parameter for calls here for the time being.
DEFAULT_LOCAL=False


def main():
    # Set up a parser:
    parser = argparse.ArgumentParser(prog='urlfrontier')

    # Common arguments:
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument('-v', '--verbose',  action='count', default=0, help='Logging level; add more -v for more logging.')
    common_parser.add_argument('-u', '--urlfrontier-endpoint', type=str, help='The URLFrontier instance to talk to.', default='localhost:7071')

    crawlid_parser = argparse.ArgumentParser(add_help=False)
    crawlid_parser.add_argument('-C', '--crawl-id', default=None, help="The CrawlID to use.")

    # Use sub-parsers for different operations:
    subparsers = parser.add_subparsers(dest="op")
    subparsers.required = True

    # Add a parser a subcommand:
    parser_getstats = subparsers.add_parser(
        'get-stats', 
        help='Get stats from URLFrontier.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, crawlid_parser])
    #parser_getstats.add_argument('id', type=str, help='The record ID to look up, or "-" to read a list of IDs from STDIN.')


    # Add a parser a subcommand:
    parser_listcrawls = subparsers.add_parser(
        'list-crawls', 
        help='List crawls known to the URLFrontier.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])

    # Add a parser a subcommand:
    parser_delcrawl = subparsers.add_parser(
        'delete-crawl', 
        help='Delete an entire crawl from the URLFrontier',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, crawlid_parser])

    # Add a parser a subcommand:
    parser_listqueues = subparsers.add_parser(
        'list-queues', 
        help='List queues known to the URLFrontier.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser,crawlid_parser])

    # Add a parser a subcommand:
    parser_delqueue = subparsers.add_parser(
        'delete-queue', 
        help='Delete a queue from the URLFrontier',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, crawlid_parser])
    parser_delqueue.add_argument('queue', help="Key for the crawl queue to delete, e.g. 'example.com'.")

    # Add a parser a subcommand:
    parser_listurls = subparsers.add_parser(
        'list-urls', 
        help='List URLs from the URLFrontier (with delay_requestable=0 so this does not interfere with the crawl).',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, crawlid_parser])
    parser_listurls.add_argument('-q', '--queue', help="Key for the crawl queue to list URLs from, e.g. 'example.com'.")
    parser_listurls.add_argument('-M', '--max-queues', type=int, default=0, help='Maximum number of queues to list URLs from. 0 means no limit.')
    parser_listurls.add_argument('-N', '--max-urls', '--max-urls-per-queue', type=int, default=1, help='Maximum number of URLs to return per queue.')

    # Add a parser a subcommand:
    parser_puturls = subparsers.add_parser(
        'put-urls', 
        help='Put URLs into the URLFrontier.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, crawlid_parser])
    parser_puturls.add_argument('-q', '--queue', help="Key for the crawl queue to add the URL(s) to. If unset, uses the host name.")
    parser_puturls.add_argument('-N', '--num-partitions', type=int, default=None, help="Number of partitions to use. If set, Crawl ID (spider name) has a partition suffix added, eg. 'crawl.2'.")
    parser_puturls.add_argument('--partition-separator', default=".", help="Character to use to separate the Crawl ID (spider name) from the partition number.")
    parser_puturls.add_argument('-m','--meta',action='append',nargs=2, metavar=('name','value'),help='Metadata fields to add, as name/value pairs. Can be repeated.')
    parser_puturls.add_argument('urls', help="URL to enqueue, or a filename to read URLs from, or '-' to read from STDIN.")

    # Add a parser a subcommand:
    parser_getactive = subparsers.add_parser(
        'get-active', 
        help='Get the active status of the URLFrontier.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])

    # Add a parser a subcommand:
    parser_setactive = subparsers.add_parser(
        'set-active',
        help='Set the active status of the URLFrontier.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser])
    parser_setactive.add_argument('-A', '--active', action=argparse.BooleanOptionalAction, required=True, help="Activate/deactivate the getting URLs from frontier.")

    # Add a parser for the rebalance operation:
    parser_rebalance = subparsers.add_parser(
        'rebalance',
        help='Resumably migrate queues when the number of spider partitions changes (e.g. 2/2 -> 1/3).',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        parents=[common_parser, crawlid_parser])
    parser_rebalance.add_argument('--from-partitions', '-O', type=int, required=True,
        help="The old number of partitions (N in the current SPIDER_PARTITION_ID=I/N).")
    parser_rebalance.add_argument('--to-partitions', '-T', type=int, required=True,
        help="The new number of partitions to migrate to.")
    parser_rebalance.add_argument('--partition-separator', default=".",
        help="Character separating the spider name from the partition number.")
    parser_rebalance.add_argument('--journal-dir', default="./.urlfrontier-rebalance",
        help="Directory for the crash-recovery migration journal.")
    parser_rebalance.add_argument('--batch-size', type=int, default=100,
        help="Number of URLs to read/copy per drain page.")
    parser_rebalance.add_argument('--workers', type=int, default=4,
        help="Number of host queues migrated concurrently.")
    parser_rebalance.add_argument('--claim-hold-seconds', type=int, default=300,
        help="How long each drained URL is held in process while being copied.")
    parser_rebalance.add_argument('--empty-read-grace', type=float, default=30.0,
        help="Seconds to retry a queue that reads empty while URLs remain "
             "(politeness window), before deferring it to a later re-run.")

    # And PARSE it:
    args = parser.parse_args()

    # Set up verbose logging:
    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)

    # Set up URL Frontier connection:
    with grpc.insecure_channel(args.urlfrontier_endpoint) as channel:
        # Setup service interface
        stub = URLFrontierStub(channel)

        # Ops:
        logger.debug("Got args: %s" % args)
        if args.op == 'get-stats':
            q = QueueWithinCrawlParams(
                key="",
                crawlID=args.crawl_id,
                local=DEFAULT_LOCAL
            )
            stats = stub.GetStats(q)
            print(stats)
        elif args.op == 'list-queues':
            p = Pagination(
                start = 0,
                size = 100,
                include_inactive = True,
                crawlID = args.crawl_id,
                local = DEFAULT_LOCAL
            )
            queues = stub.ListQueues(p)
            print(queues)
        elif args.op == 'list-crawls':
            crawls = stub.ListCrawls(Local(local=DEFAULT_LOCAL))
            print(crawls)
        elif args.op == 'delete-crawl':
            deleted = stub.DeleteCrawl(
                    DeleteCrawlMessage(
                        value=args.crawl_id,
                        local=DEFAULT_LOCAL,
                    )
                )
            print(f"Deleted {deleted.value} URLs from crawl {args.crawl_id}.")
        elif args.op == 'list-urls':
            g = GetParams(
                max_urls_per_queue = args.max_urls,
                max_queues = args.max_queues,
                delay_requestable = 1,
                key = args.queue,
                crawlID = args.crawl_id, # Needed if querying a specific queue key, otherwise:
                #anyCrawlID = AnyCrawlID()
            )
            for uf_response in stub.GetURLs(g):
                print("url=%s, metadata=%s" % (uf_response.url, uf_response.metadata))
                #request = urlInfo_to_request(uf_response)
                #print(request)
        elif args.op == 'put-urls':
            # Support crawl distribution over crawl IDs
            if args.num_partitions is None:
                spider_id = None
                num_partitions = 1
            else:
                spider_id = 1 # Any value, to enable partitions
                num_partitions = args.num_partitions
            hr = HashRingDistributor(
                spider_id=spider_id,
                spider_name=args.crawl_id,
                num_spiders=num_partitions,
                separator=args.partition_separator,
            )
            # Gather any metadata fields:
            meta = None
            if args.meta:
                meta = {}
                for name, value in args.meta:
                    meta[name] = value
            # Process the URLs
            if args.urls == '-':
                def uf_generator():
                    for line in sys.stdin:
                        line = line.strip()
                        if line:
                            yield hr.request_to_put_url(line, meta)
            elif os.path.isfile(args.urls):
                def uf_generator():
                    with open(args.urls) as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                yield hr.request_to_put_url(line, meta)
            else:
                def uf_generator():
                    yield hr.request_to_put_url(args.urls, meta)
            for uf_response in stub.PutURLs(uf_generator()):
                # Status 0 OK, 1 Skipped, 2 FAILED
                logger.debug("PutURL ID=%s Status=%i" % (uf_response.ID, uf_response.status))

        elif args.op == 'get-active':
            response = stub.GetActive(Local(local=DEFAULT_LOCAL))
            print(response.state)

        elif args.op == 'set-active':
            print(f"Setting active status to {args.active}")
            response = stub.SetActive(
                Active(
                    state=args.active, 
                    local=DEFAULT_LOCAL
                    ))

        elif args.op == 'delete-queue':
            queue = args.queue
            crawlID = args.crawl_id
            d = QueueWithinCrawlParams(
                key = queue,
                crawlID = crawlID,
                local=DEFAULT_LOCAL,
            )
            deleted = stub.DeleteQueue(d)
            print(f"Deleted {deleted.value} URLs from queue {queue} of crawl {crawlID}.")
        elif args.op == 'rebalance':
            if args.crawl_id is None:
                parser.error("rebalance requires a crawl ID (-C/--crawl-id), i.e. the spider name.")
            config = RebalanceConfig(
                spider_name=args.crawl_id,
                old_partitions=args.from_partitions,
                new_partitions=args.to_partitions,
                separator=args.partition_separator,
                journal_dir=args.journal_dir,
                max_urls_per_queue=args.batch_size,
                migration_workers=args.workers,
                claim_hold_seconds=args.claim_hold_seconds,
                empty_read_grace_seconds=args.empty_read_grace,
            )
            journal = MigrationJournal(
                journal_dir=args.journal_dir,
                spider_name=args.crawl_id,
                old_n=args.from_partitions,
                new_n=args.to_partitions,
            )
            migrator = Rebalancer(stub, config)
            try:
                with journal:
                    results = migrator.run(journal)
            except RebalanceInProgress as e:
                raise SystemExit(f"ERROR: {e}")
            if not results:
                print("No queues need to move between these partition layouts.")
            for r in results:
                print(f"{r.key}: {r.source_crawl} -> {r.target_crawl} [{r.state}] "
                      f"({r.copied} URLs copied)")
            pending = journal.pending()
            if pending:
                raise SystemExit(
                    f"WARNING: {len(pending)} queue(s) not fully migrated; "
                    f"re-run the same command to resume."
                )
            print(f"Rebalance {args.from_partitions} -> {args.to_partitions} complete.")
        else:
            raise Exception("Unrecognized operation " + args.op)

if __name__ == "__main__":
    main()