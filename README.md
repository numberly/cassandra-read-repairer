# Cassandra / Scylla Read Repairer tool

A performant tool to repair a [Scylla](https://scylladb.com/) or an [Apache Cassandra](https://cassandra.apache.org/) cluster/keyspace/table by reading every record in a table at Consistency Level = All.

This tool has been inspired by Christos Kalantzis' [cassTickler](https://github.com/ckalantzis/cassTickler). It is basically a rewrite to get it way more performant, support SSL and authentication enabled clusters as well as giving the possibility to repair all keyspaces or some selected keyspaces and tables.

## Why?

Sometimes the built-in repair tool in Scylla / Apache Cassandra does not run successfully. Or you need to repair a table, but you simply don't have the resources to allow the normal repair process of validation compactions, sstable copies, etc.

## How does it work?

You point the repairer to your Scylla / Apache Cassandra cluster you want to repair. It will read every record efficiently by selecting them by token ranges at a Consistency Level (CL) of ALL. In Scylla / Apache Cassandra this is the highest CL offered. An interesting side effect of reading a record at CL ALL is that if any copies of the record are not consistent (or missing) the correct data will be written out to the nodes that would have had the record.

- [Read more about the Read Repair mechanism](https://wiki.apache.org/cassandra/ReadRepair/)

## Usage example

- Repair the whole cluster (SSL + Auth):

`python repairer.py --hosts scylla1,scylla2 --username scylla --password coconut --cacert numberly-ca.crt`


- Repair `test` and `myproject` keyspaces:

`python repairer.py --hosts scylla1,scylla2 --keyspaces test,myproject`


- Repair `coconut` table of `myproject` keyspace:

`python repairer.py --hosts scylla1,scylla2 --keyspaces myproject --tables coconut`

## Help

```
usage: repairer.py [-h] [--timeout REQUEST_TIMEOUT]
                   [--concurrency CONCURRENCY] [--processes PROCESSES]
                   [--partitionsize PARTITIONSIZE] [--keyspaces KEYSPACES]
                   [--tables TABLES] [--username USERNAME]
                   [--password PASSWORD] --hosts HOSTS [--cacert CACERT]

Performant Cassandra/Scylla cluster read repairer using consistency level = ALL

optional arguments:
  -h, --help            show this help message and exit
  --timeout REQUEST_TIMEOUT
                        request timeout in seconds
  --concurrency CONCURRENCY
                        cassandra-driver query execution concurrency
  --processes PROCESSES
                        number or tables to repair in parallel
  --partitionsize PARTITIONSIZE
                        size of repair partitions
  --keyspaces KEYSPACES
                        comma separated keyspaces to repair
  --tables TABLES       comma separated tables to repair
  --username USERNAME   username to login as
  --password PASSWORD   user password
  --hosts HOSTS         comma delimited target hosts to connect to
  --cacert CACERT       SSL CA certificates path
```

## Installation

This tool is written for python3.6 or more and needs the `cassandra-driver` python library.

```bash
python3 -m venv cassandra-read-repairer

source cassandra-read-repairer/bin/activate

pip install -r requirements.txt
```
