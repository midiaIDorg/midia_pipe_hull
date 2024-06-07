#!/usr/bin/env python3
import argparse
import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from pprint import pprint

import pony.orm
import tomllib
from pony.orm import (
    Database,
    Json,
    Optional,
    PrimaryKey,
    Required,
    commit,
    db_session,
    set_sql_debug,
)


class args:
    results_json = Path("/tmp/test.json")
    results_json = Path("/tmp/full_summary.json")
    command = "test"
    config = Path("configs/resultsdb/lab.toml")
    config = None
    verbose = True
    reproducibility = "../config_initial.toml"


parser = argparse.ArgumentParser(description="Send a table to a db.")
parser.add_argument(
    "--results_json", help="A json to save in the DB, or a path to it.", required=True
)


parser.add_argument(
    "--config",
    help="A config for binding to a DB. If not given, will look into environement variables for `midia_results_db_provider`,`midia_results_db_host`,`midia_results_db_port`,`midia_results_db_user`,`midia_results_db_password`, and `midia_results_db_db`.",
    default=None,
)
parser.add_argument(
    "--command",
    help="Command used to prepare the entry.",
    default="",
)
parser.add_argument(
    "--reproducibility",
    help="Full config file for reproducibility.",
    default="../config_initial.toml",
)
parser.add_argument(
    "--full_drop",
    help="Fully drop a table into a json.",
    action="store_true",
)
parser.add_argument(
    "--verbose",
    help="Be verbose.",
    action="store_true",
)
args = parser.parse_args()

if __name__ == "__main__":
    expected_config_fields = ("provider", "port", "host", "user", "db", "password")
    if args.config is None:
        if args.verbose:
            print("ATTEMPTING TO USE ENV VARIABLES.")
        config = {}
        for _var in expected_config_fields:
            _value = os.getenv(f"midia_results_db_{_var}")
            if _value is None:
                if args.verbose:
                    print(
                        f"TERMINATING ATTEMPT TO SEND RESULS TO DB: no connection config nor environement variable {_var} being set."
                    )
                sys.exit(1)
            config[_var] = _value
        config["port"] = int(config["port"])

    else:
        try:
            with open(args.config, "rb") as f:
                config = tomllib.load(f)
            for _var in expected_config_fields:
                assert _var in config, f"MISSING VARIABLE {_var} SET IN CONFIG."
        except FileNotFoundError:
            print(traceback.format_exc())
            if args.verbose:
                print(
                    "TERMINATING ATTEMPT TO SEND RESULS TO DB: provided config path does not exist."
                )
            sys.exit(1)

    if args.verbose:
        print("DB CONNECTION CONFIG:")
        pprint(config)

    try:
        with open(args.results_json, "r") as f:
            results = json.load(f)
    except FileNotFoundError:
        results = json.loads(args.results_json)

    try:
        with open(args.reproducibility, "rb") as f:
            reproducibility = tomllib.load(f)
    except FileNotFoundError:
        reproducibility = {}

    db = Database()

    class Result(db.Entity):
        id = PrimaryKey(int, auto=True)
        date = Required(
            datetime, precision=0, default=lambda: datetime.now()
        )  # Date of the arrival of message.
        command = Optional(str, default="NA")
        result = Required(Json)
        reproducibility = Optional(Json)

    if args.verbose:
        set_sql_debug(True)

    db.bind(**config)
    # db.generate_mapping(create_tables=True)
    db.generate_mapping(create_tables=True)

    if args.verbose:
        print("SENDING RESULTS TO A REMOTE DATABASE OF RESULTS")

    with db_session():  # not necessary in interactive python session
        Result(
            command=args.command,
            result=results,
            reproducibility=reproducibility,
        )
        commit()

    if args.verbose:
        print("UPDATED REMOTE DB WITH NEW RESULTS.")
