#!/bin/bash


# By default, loglevel is INFO
loglevel="INFO"

# For one run of this script, we expect only one dump path.
# Hence declaring it as global var to return from functions.
dump_path=""
dump_txid=""

# current replication id at target before we begin replication
last_repl_id=""

# current replication id at target after we complete replication
post_load_repl_id=""
