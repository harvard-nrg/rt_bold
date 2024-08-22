#!/usr/bin/env python3 -u

import sys
import time
import logging
from pubsub import pub
from pathlib import Path
from argparse import ArgumentParser
from rtbold.watcher.directory import DirectoryWatcher
from rtbold.proc import Processor
from rtbold.proc.volreg import VolReg
from rtbold.proc.params import Params
from rtbold.view.dash import View
from rtbold.broker.redis import MessageBroker
from rtbold.config import Config

logger = logging.getLogger('main')
logging.basicConfig(level=logging.INFO)

def main():
    parser = ArgumentParser()
    parser.add_argument('-m', '--mock', action='store_true')
    parser.add_argument('-c', '--config', type=Path)
    parser.add_argument('--host', type=str, default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8080)
    parser.add_argument('--folder', type=Path, default='/tmp/rtbold')
    parser.add_argument('-v', '--verbose', action='store_true')
    args = parser.parse_args()

    config = Config(args.config)

    broker = MessageBroker()
    watcher = DirectoryWatcher(args.folder)
    processor = Processor()
    params = Params(
        broker=broker,
        config=config.section('params')
    )
    volreg = VolReg(mock=args.mock)
    view = View(host=args.host, port=args.port)

    if args.verbose:
        logging.getLogger('rtbold.proc').setLevel(logging.DEBUG)
        logging.getLogger('rtbold.proc.params').setLevel(logging.DEBUG)
        logging.getLogger('rtbold.proc.volreg').setLevel(logging.DEBUG)
        logging.getLogger('rtbold.view.dash').setLevel(logging.DEBUG)
   
    # logging from this module is useful, but noisy
    logging.getLogger('werkzeug').setLevel(logging.ERROR)

    # start the watcher and view
    watcher.start()
    view.forever()

if __name__ == '__main__':
    main()
