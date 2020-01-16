import argparse
import sys
import config as conf
import log

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Replicate databases using Timestamps in SQL Tables')
    parser.add_argument('--config', default='conf.json',
                        action='store', help='Set replication configuration file')

    args = parser.parse_args()
    log = log.Log()
    try:
        config = conf.Config(args.config)
    except conf.ConfigException as e:
        log.error(e)
