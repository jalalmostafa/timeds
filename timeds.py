import argparse
import sys
import config as conf
#from pid.decorator import pidfile
from log import Log
from replicator import SchemeReplicator

# @pidfile('timeds.pid')


def main():
    parser = argparse.ArgumentParser(
        description='Replicate databases using Timestamps in SQL Tables')
    parser.add_argument('--config', default='conf.json',
                        action='store', help='Set replication configuration file')
    parser.add_argument('-v', action='store_true',
                        help='Verbose Mode. Print config, etc')
    args = parser.parse_args()
    log = Log()
    try:
        config = conf.Config(args.config)
        if args.v:
            print(config)

        # for scheme in config:
        #     scheme = SchemeReplicator(scheme, config[scheme])
        #     scheme.run()
    except conf.ConfigException as e:
        log.error(e)


if __name__ == "__main__":
    main()
