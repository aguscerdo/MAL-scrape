import MAL_api as mal
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--producers', action='store_true', default=False)
    parser.add_argument('-s', '--sleep', action='store_true', default=False)
    parser.add_argument('-v', '--verbose', action='store_true', default=False)
    parser.add_argument('-i', '--i_start', default=-1, type=int)

    args = parser.parse_args()

    ex = mal.Extracter(file=True)

    if args.producers:
        ex.update_producers(verbose=args.verbose)

    ex.retrieve(start_i=args.i_start, fail_limit=10000, sleep=args.sleep, verbose=args.verbose)


