# region External Imports
import os
import sys
import argparse
from datetime import datetime as dt, timedelta as td
from pathlib import Path
from typing import List

from loguru import logger

# endregion

# region Project Imports
script_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(script_dir))
os.chdir(script_dir)
from core import rrf, status, contacts, alerts, log, push

# endregion


@logger.catch()
def run_alerts(ss_ids: List[str], max_emails: int, dev_mode: bool):
    last_data = rrf.get_last_generation_data(ss_ids)
    to_send = status.get_alerts(last_data)
    to_send = contacts.assign(to_send)
    sent = alerts.send(to_send, max_emails, dev_mode)
    status.save(sent, dev_mode)
    push.upload_statuses(sent, dev_mode)


def get_params():
    parser = argparse.ArgumentParser(prog="Realtime Generation Data Client Alerts")
    parser.add_argument(
        "-s", "--ss-ids", nargs="+", help="SSXXXXX SSYYYYY", default=None
    )
    parser.add_argument(
        "-m",
        "--max-emails",
        type=int,
        help="Max emails to send in session",
        default=None,
    )
    parser.add_argument(
        "-d", "--dev-mode", action="store_const", const=True, default=False
    )
    args = vars(parser.parse_args())

    return args


if __name__ == "__main__":
    log.start()
    params = get_params()
    run_alerts(**params)
    log.end()
