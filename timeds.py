import argparse
import sys
import config as conf
from log import Log
from replicator import SchemeReplicator
from pid.decorator import pidfile


@pidfile('timeds.pid', piddir='/tmp')
def main():
    parser = argparse.ArgumentParser(
        description='Replicate databases using Timestamps in SQL Tables')
    parser.add_argument('--config', default='conf.json',
                        action='store', help='Set replication configuration file')
    parser.add_argument('-v', action='store_true',
                        help='Verbose Mode. Print config, etc')
    parser.add_argument('--only-dynamic-and-views', action='store_true',
                        help='Only replicate dynamic tables and views')
    parser.add_argument('-d', action='store_true',
                        help='Only replicate dynamic tables and views')

    args = parser.parse_args()
    log = Log()
    replicators = []

    try:
        config = conf.Config(args.config)
        if args.v:
            print(config)

        for scheme in config:
            scheme = SchemeReplicator(
                scheme, config[scheme], only_dynamic_and_views=args.only_dynamic_and_views or args.d)
            reps = scheme.run()
            replicators.extend(reps)

        log.info('Started for %s database(s)...' % (len(replicators)))
        for rep in replicators:
            rep.join()

    except conf.ConfigException as e:
        log.error(e)


if __name__ == "__main__":
    main()
