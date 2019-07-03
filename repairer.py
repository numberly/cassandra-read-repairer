#!/usr/bin/env python3

import argparse
import sys
from collections import Counter
from multiprocessing import Pool
from ssl import CERT_NONE, PROTOCOL_TLSv1_2

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent


def get_token_ranges(partition_size=10000):
    max_size = sys.maxsize * 2
    range_size = max_size // partition_size

    start = -sys.maxsize
    should_break = False

    ranges = []

    while True:
        end = start + int(range_size)

        if end >= sys.maxsize:
            end = sys.maxsize
            should_break = True

        ranges.append((start, end))

        if should_break:
            break

        start = end + 1

    return ranges


def print_stats(keyspace, table, len_partitions, stats):
    percent = int(
        (stats["repaired_partitions"] + stats["failed_partitions"])
        / len_partitions
        * 100
    )
    if percent > stats["percent"]:
        print(
            f"{keyspace}.{table} repaired {stats['repaired_rows']} rows, "
            + f"{stats['repaired_partitions']}/{len_partitions} partitions "
            + f"({stats['failed_partitions']} failed) {percent}%"
        )
        stats["percent"] = percent


def repair_table(
    contact_points,
    auth_provider,
    ssl_opts,
    keyspace,
    table,
    partitions,
    concurrency,
    timeout,
):
    try:
        len_partitions = len(partitions)

        cluster = Cluster(
            auth_provider=auth_provider,
            compression=False,
            contact_points=contact_points,
            ssl_options=ssl_opts,
        )
        session = cluster.connect(keyspace)

        partition_key = ", ".join(
            [
                k.name
                for k in cluster.metadata.keyspaces[keyspace]
                .tables[table]
                .partition_key
            ]
        )
        print(f"{keyspace}.{table} partition key: {partition_key}")

        select_query = f"""SELECT COUNT(1) FROM \"{keyspace}\".{table}
                           WHERE token({partition_key}) >= ? AND token({partition_key}) <= ?"""
        select_token = session.prepare(select_query)
        select_token.consistency_level = ConsistencyLevel.ALL
        select_token.timeout = timeout

        stats = Counter()
        statements_and_params = []
        for start, stop in partitions:
            statements_and_params.append((select_token, (start, stop)))

        concurrent = execute_concurrent(
            session,
            statements_and_params,
            concurrency=concurrency,
            raise_on_first_error=False,
            results_generator=True,
        )
        for (success, result) in concurrent:
            if not success:
                stats["failed_partitions"] += 1
            else:
                for row in result.current_rows:
                    stats["repaired_rows"] += row.count
                    stats["repaired_partitions"] += 1
            print_stats(keyspace, table, len_partitions, stats)
        print_stats(keyspace, table, len_partitions, stats)
    except Exception as err:
        print(f"{keyspace}.{table} error: {err}")
        return False
    else:
        return True


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Performant Cassandra/Scylla cluster read repairer using consistency level = ALL"
    )
    parser.add_argument(
        "--timeout",
        action="store",
        default=60,
        dest="request_timeout",
        help="request timeout in seconds",
        type=int,
    )
    parser.add_argument(
        "--concurrency",
        action="store",
        default=100,
        dest="concurrency",
        help="cassandra-driver query execution concurrency",
        type=int,
    )
    parser.add_argument(
        "--processes",
        action="store",
        default=5,
        dest="processes",
        help="number or tables to repair in parallel",
        type=int,
    )
    parser.add_argument(
        "--partitionsize",
        action="store",
        default=10000,
        dest="partitionsize",
        help="size of repair partitions",
        type=int,
    )
    parser.add_argument(
        "--keyspaces",
        action="store",
        dest="keyspaces",
        help="comma separated keyspaces to repair",
        type=str,
    )
    parser.add_argument(
        "--tables",
        action="store",
        dest="tables",
        help="comma separated tables to repair",
        type=str,
    )
    parser.add_argument(
        "--username",
        action="store",
        default=None,
        dest="username",
        help="username to login as",
        type=str,
    )
    parser.add_argument(
        "--password",
        action="store",
        default=None,
        dest="password",
        help="user password",
        type=str,
    )
    parser.add_argument(
        "--hosts",
        action="store",
        dest="hosts",
        help="comma delimited target hosts to connect to",
        required=True,
        type=str,
    )
    parser.add_argument(
        "--cacert",
        action="store",
        dest="cacert",
        help="SSL CA certificates path",
        type=str,
    )
    options = parser.parse_args()

    contact_points = options.hosts.split(",")
    if options.username and options.password:
        auth_provider = PlainTextAuthProvider(
            username=options.username, password=options.password
        )
    else:
        auth_provider = None
    if options.cacert:
        ssl_opts = {
            "ca_certs": options.cacert,
            "ssl_version": PROTOCOL_TLSv1_2,
            "cert_reqs": CERT_NONE,
        }
    else:
        ssl_opts = None

    cluster = Cluster(
        contact_points=contact_points,
        auth_provider=auth_provider,
        ssl_options=ssl_opts,
        compression=False,
    )
    session = cluster.connect()

    partitions = get_token_ranges(partition_size=options.partitionsize)
    if options.keyspaces:
        keyspaces = options.keyspaces.split(",")
    else:
        keyspaces = cluster.metadata.keyspaces.keys()

    for keyspace in sorted(keyspaces):
        if options.tables:
            tables = options.table.split(",")
        else:
            tables = cluster.metadata.keyspaces[keyspace].tables.keys()

        print(f"repairing {len(tables)} tables on keyspace {keyspace}...")
        with Pool(options.processes) as pool:
            results = [
                pool.apply_async(
                    repair_table,
                    (
                        contact_points,
                        auth_provider,
                        ssl_opts,
                        keyspace,
                        table,
                        partitions,
                        options.concurrency,
                        options.request_timeout,
                    ),
                )
                for table in sorted(tables)
            ]
            if all([r.get() for r in results]):
                print(f"repaired {len(tables)} tables on keyspace {keyspace}...")
            else:
                print(f"failed to repair all tables on keyspace {keyspace}...")
