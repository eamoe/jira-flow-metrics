import argparse
import functools
import logging
import matplotlib
import pandas
from pandas.plotting import register_matplotlib_converters

logger = logging.getLogger(__file__)
if __name__ != '__main__':
    logging.basicConfig(level=logging.WARN)


class AnalysisException(Exception):
    pass


def init():
    register_matplotlib_converters()
    matplotlib.pyplot.style.use('fivethirtyeight')
    matplotlib.pyplot.rcParams['axes.labelsize'] = 14
    matplotlib.pyplot.rcParams['lines.linewidth'] = 1.5


def read_data(path, exclude_types=None, since='', until=''):

    # read csv changelog data with necessary fields:
    # issue_id - unique numeric id for this issue
    # issue_key - unique textual key for this issue
    # issue_type_name - category of issue type
    # issue_created_date - when the issue was created
    # issue_points - how many points were assigned to this issue (optional)
    # changelog_id - unique id for this particular change for this issue
    # status_change_date - when the change was made
    # status_from_name - from which status (optional)
    # status_to_name - to which status
    # status_from_category_name - from which status category (optional)
    # status_to_category_name - to which status category

    omit_issue_types = set(exclude_types) if exclude_types else None

    logger.info('Opening input file for reading...')

    data = pandas.read_csv(path)

    required_fields = ['issue_id',
                       'issue_key',
                       'issue_type_name',
                       'issue_created_date',
                       'changelog_id',
                       'status_change_date',
                       'status_from_name',
                       'status_to_name',
                       'status_from_category_name',
                       'status_to_category_name',
                       ]

    # Check for missing fields
    missing_fields = []
    for field in required_fields:
        if field not in data:
            missing_fields.append(field)
    if missing_fields:
        raise AnalysisException(f'Required fields `{", ".join(missing_fields)}` missing from the dataset')

    # parse the datetimes to utc and then localize them to naive datetimes
    # so _all_ date processing in pandas is naive in UTC
    data['issue_created_date'] = data['issue_created_date'].apply(pandas.to_datetime, utc=True).dt.tz_localize(None)
    data['status_change_date'] = data['status_change_date'].apply(pandas.to_datetime, utc=True).dt.tz_localize(None)

    # Check to make sure the data is sorted correctly by issue_id and status_change_date
    data = data.sort_values(['issue_id', 'status_change_date'])

    # Drop duplicates based on issue_id and changelog_id
    n1 = len(data)
    logger.info(f'-> {n1} changelog items read')
    data = data.drop_duplicates(subset=['issue_id', 'changelog_id'], keep='first')

    # Count how many changelog items were duplicates
    n2 = len(data)
    dupes = n1 - n2
    logger.info(f'-> {dupes} changelog items removed as duplicate')

    # Filter out specific issue types
    if omit_issue_types:
        data = data[~data['issue_type_name'].isin(omit_issue_types)]
        n3 = len(data)
        omitted = n2 - n3
        logger.info(f'-> {omitted} changelog items excluded by type')

    # Filter out issues before since date and after until
    if since:
        data = data[data['issue_created_date'] >= pandas.to_datetime(since)]
    if until:
        data = data[data['issue_created_date'] < pandas.to_datetime(until)]

    # Count how many changelog items were filtered
    n3 = len(data)
    filtered = n2 - n3
    logger.info(f'-> {filtered} changelog items filtered')

    # If issue_points does not exist, set them all to 1
    if 'issue_points' not in data:
        data['issue_points'] = 1

    if not data.empty:
        min_date = data['issue_created_date'].min().strftime('%Y-%m-%d')
        if not since:
            since = min_date
        max_date = data['issue_created_date'].max().strftime('%Y-%m-%d')
        if not until:
            until = max_date
        status_min_date = data['status_change_date'].min().strftime('%Y-%m-%d')
        status_max_date = data['status_change_date'].max().strftime('%Y-%m-%d')
        num_issues = data['issue_key'].nunique()
        logger.info(f'-> {n3} changelog items remain created from {min_date} to {max_date} '
                    f'with status changes from {status_min_date} to {status_max_date}')
        logger.info(f'-> {num_issues} unique work items loaded since {since} & until {until}, ready for analysis')

    logger.info('---')

    return data, dupes, filtered


def output_formatted_data(output,
                          title,
                          data,
                          output_exclude_title=False,
                          output_header='',
                          output_footer='',
                          output_columns=None,
                          output_format=None):
    if output_format is None:
        output_format = 'string'

    output.writelines(
        f'{line}\n' for line in [output_header if output_header else '',
                                 f'# {title}' if title and not output_exclude_title else '',
                                 data.to_string(columns=output_columns) if output_format == 'string' else
                                 data.to_csv(columns=output_columns) if output_format == 'csv' else
                                 data.to_html(columns=output_columns) if output_format == 'html' else '',
                                 output_footer if output_footer else '',
                                 ] if line)


def run(args):
    data, dupes, filtered = read_data(args.file,
                                      exclude_types=args.exclude_type,
                                      since=args.since,
                                      until=args.until)

    if data.empty:
        logger.warning('Data for analysis is empty')
        return

    exclude_weekends = args.exclude_weekends

    # If no since/until is provided, compute the range from the data
    since = args.since
    if not since:
        since = min(data['issue_created_date'].min(), data['status_change_date'].min())
    if hasattr(since, 'date'):
        since = since.date()
    until = args.until
    if not until:
        until = max(data['issue_created_date'].max(), data['status_change_date'].max()) + pandas.Timedelta(1, 'D')
    if hasattr(until, 'date'):
        until = until.date()

    output = args.output

    # preprocess issue data
    i, _ = process_issue_data(data, since=since, until=until, exclude_weekends=exclude_weekends)

    # Calc summary data
    if args.command == 'summary':
        cmd_summary(output, i, since=since, until=until)

    # Calc detail data
    if args.command == 'detail' and args.detail_type == 'flow':
        cmd_detail_flow(output,
                        data,
                        since=since,
                        until=until,
                        categorical=args.categorical,
                        plot=args.output_plot,
                        plot_trendline=args.output_plot_trendline,
                        columns=args.output_columns)

    if args.command == 'detail' and args.detail_type == 'wip':
        cmd_detail_wip(output, i, since=since, until=until, wip_type=args.type)

    if args.command == 'detail' and args.detail_type == 'throughput':
        cmd_detail_throughput(output, i, since=since, until=until, throughput_type=args.type)

    if args.command == 'detail' and args.detail_type == 'cycletime':
        cmd_detail_cycletime(output, i, since=since, until=until)

    if args.command == 'detail' and args.detail_type == 'leadtime':
        cmd_detail_leadtime(output, i, since=since, until=until)

    # Calc correlation data
    if args.command == 'correlation':
        cmd_correlation(output, i, since=since, until=until, plot=args.output_plot)

    # Calc survival data
    if args.command == 'survival' and args.survival_type == 'km':
        cmd_survival_km(output, i, since=since, until=until)

    if args.command == 'survival' and args.survival_type == 'wb':
        cmd_survival_wb(output, i, since=since, until=until)

    # Calc forecast data
    if args.command == 'forecast' and args.forecast_type == 'items' and args.n:
        cmd_forecast_items_n(output, i, since=since, until=until, n=args.n, simulations=args.simulations,
                             window=args.window)

    if args.command == 'forecast' and args.forecast_type == 'items' and args.days:
        cmd_forecast_items_days(output, i, since=since, until=until, days=args.days, simulations=args.simulations,
                                window=args.window)

    if args.command == 'forecast' and args.forecast_type == 'points' and args.n:
        cmd_forecast_points_n(output, i, since=since, until=until, n=args.n, simulations=args.simulations,
                              window=args.window)

    if args.command == 'forecast' and args.forecast_type == 'points' and args.days:
        cmd_forecast_points_days(output, i, since=since, until=until, days=args.days, simulations=args.simulations,
                                 window=args.window)

    # Calc shell data
    if args.command == 'shell':
        cmd_shell(output, data, i, since=since, until=until, args=args)


def main():
    global output_formatted_data

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

    subparsers = parser.add_subparsers(dest='command')

    def add_output_params(subparser):
        subparser.add_argument('--column',
                               dest='output_columns',
                               action='append',
                               help='Filter output to only include this (can accept more than one for ordering')

        subparser.add_argument('--format',
                               dest='output_format',
                               choices=('string', 'csv', 'html'),
                               default='string',
                               help='Which output format should be used (string, csv, html)')

        subparser.add_argument('--header',
                               dest='output_header',
                               help='Prepend each data table with header text')

        subparser.add_argument('--footer',
                               dest='output_footer',
                               default=' ',
                               help='Append each data table with footer text (default: \\n')

        subparser.add_argument('--exclude-title',
                               dest='output_exclude_title',
                               action='store_true',
                               help='Exclude title of data table in output')

    def add_output_plot_params(subparser):
        subparser.add_argument('--plot',
                               dest='output_plot',
                               type=argparse.FileType('wb'),
                               default=None,
                               help='File to output plot results')

    # Get metrics summary
    subparser_summary = subparsers.add_parser('summary',
                                              help='Generate a summary of metric data (cycle time, throughput, wip)')

    add_output_params(subparser_summary)

    # Get detailed metrics data
    subparser_detail = subparsers.add_parser('detail',
                                             help='Output detailed analysis data')

    subparser_detail_subparsers = subparser_detail.add_subparsers(dest='detail_type')

    subparser_flow = subparser_detail_subparsers.add_parser('flow',
                                                            help='Analyze cumulative flow and output detail')

    subparser_flow.add_argument('--categorical',
                                action='store_true',
                                help='Use status categories instead of statuses in flow analysis')

    subparser_flow.add_argument('--plot-trendline',
                                dest='output_plot_trendline',
                                action='store_true',
                                help='Output cumulative flow diagram as a scatterplot and trendline')

    add_output_params(subparser_flow)
    add_output_plot_params(subparser_flow)

    subparser_wip = subparser_detail_subparsers.add_parser('wip',
                                                           help='Analyze wip and output detail')

    subparser_wip.add_argument('type',
                               choices=('daily', 'weekly', 'aging'),
                               help='Type of wip data to output (daily, weekly, aging)')

    add_output_params(subparser_wip)

    subparser_throughput = subparser_detail_subparsers.add_parser('throughput',
                                                                  help='Analyze throughput and output detail')

    subparser_throughput.add_argument('type',
                                      choices=('daily', 'weekly'),
                                      help='Type of throughput data to output (daily, weekly)')

    add_output_params(subparser_throughput)

    subparser_cycletime = subparser_detail_subparsers.add_parser('cycletime',
                                                                 help='Analyze cycletime and output detail')
    add_output_params(subparser_cycletime)

    subparser_leadtime = subparser_detail_subparsers.add_parser('leadtime',
                                                                help='Analyze leadtime and output detail')

    add_output_params(subparser_leadtime)

    # Correlation subparser
    subparser_corrrelation = subparsers.add_parser('correlation',
                                                   help='Test correlation between issue_points and lead/cycle times')
    add_output_params(subparser_corrrelation)

    add_output_plot_params(subparser_corrrelation)

    # Survival subparser
    subparser_survival = subparsers.add_parser('survival',
                                               help='Analyze the survival of work items')

    subparser_survival_subparsers = subparser_survival.add_subparsers(dest='survival_type')

    subparser_survival_km = subparser_survival_subparsers.add_parser(
        'km',
        help='Analyze the survival of work items using Kaplan-Meier Estimation')

    add_output_params(subparser_survival_km)

    subparser_survival_wb = subparser_survival_subparsers.add_parser(
        'wb',
        help='Analyze the survival of work items using Weibull Estimation')

    add_output_params(subparser_survival_wb)

    # Forecast subparser
    subparser_forecast = subparsers.add_parser('forecast',
                                               help='Forecast the future using Monte Carlo simulation')

    subparser_forecast_subparsers = subparser_forecast.add_subparsers(dest='forecast_type')

    subparser_forecast_items = subparser_forecast_subparsers.add_parser('items',
                                                                        help='Forecast future work items')

    subparser_forecast_items_group = subparser_forecast_items.add_mutually_exclusive_group(required=True)

    subparser_forecast_items_group.add_argument(
        '-n', '--items',
        dest='n',
        type=int,
        help='Number of items to predict answering the question "within how many days can N items be completed?"')

    subparser_forecast_items_group.add_argument(
        '-d', '--days',
        type=int,
        help='Number of days to predict answering the question "how many items can be completed within N days?"')

    subparser_forecast_items.add_argument('--simulations',
                                          default=10000,
                                          help='Number of simulation iterations to run (default: 10000)')

    subparser_forecast_items.add_argument('--window',
                                          default=90,
                                          help='Window of historical data to use in the forecast (default: 90 days)')

    add_output_params(subparser_forecast_items)

    subparser_forecast_points = subparser_forecast_subparsers.add_parser('points',
                                                                         help='Forecast future points')

    subparser_forecast_points_group = subparser_forecast_points.add_mutually_exclusive_group(required=True)

    subparser_forecast_points_group.add_argument(
        '-n', '--points',
        dest='n',
        type=int,
        help='Number of points to predict answering the question "within how many days can N points be completed?"')

    subparser_forecast_points_group.add_argument(
        '-d', '--days',
        type=int,
        help='Number of days to predict answering the question "how many points can be completed within N days?"')

    subparser_forecast_points.add_argument('--simulations',
                                           default=10000,
                                           help='Number of simulation iterations to run (default: 10000)')

    subparser_forecast_points.add_argument('--window',
                                           default=90,
                                           help='Window of historical data to use in the forecast (default: 90 days)')

    add_output_params(subparser_forecast_points)

    # Shell subparser
    subparser_shell = subparsers.add_parser('shell',
                                            help='Load the data into an interactive Python shell')

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return

    if args.command == 'detail' and args.detail_type is None:
        subparser_detail.print_help()
        return

    if args.command == 'survival' and args.survival_type is None:
        subparser_survival.print_help()
        return

    if args.command == 'forecast' and args.forecast_type is None:
        subparser_forecast.print_help()
        return

    if args.output:
        args.output.reconfigure(line_buffering=True)

    format_args = ['output_exclude_title', 'output_header', 'output_footer', 'output_format', 'output_columns']
    if any(hasattr(args, arg) for arg in format_args):
        kw = {key: getattr(args, key) for key in format_args if hasattr(args, key)}
        output_formatted_data = functools.partial(output_formatted_data, **kw)

    try:
        init()
        run(args)
    except AnalysisException as e:
        logger.error('Error: %s', e)


if __name__ == '__main__':
    main()
