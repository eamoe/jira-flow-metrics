import argparse


def main():

    parser = argparse.ArgumentParser(description='Analyze exported data')

    parser.add_argument('-f', '--file',
                        type=argparse.FileType('r'),
                        default='-',
                        help='Data file to analyze (default: stdin)')
    parser.add_argument('-o', '--output',
                        type=argparse.FileType('w'),
                        default='-',
                        help='File to output results (default: stdout)')

    parser.add_argument('-q', '--quiet',
                        action='store_true',
                        help='Quiet mode that only logs warnings to console')

    parser.add_argument('--exclude-weekends',
                        action='store_true',
                        help='Exclude weekends from cycle and lead time calculations')

    parser.add_argument('--exclude-type',
                        metavar='TYPE',
                        action='append',
                        help='Exclude one or more specific types from the analysis (e.g., "Epic", "Bug", etc)')

    parser.add_argument('--since',
                        help='Only process work items created since date (format: YYYY-MM-DD)')

    parser.add_argument('--until',
                        help='Only process work items created up until date (format: YYYY-MM-DD)')


if __name__ == '__main__':
    main()
