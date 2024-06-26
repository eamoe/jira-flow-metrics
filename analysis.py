import argparse
import functools
import logging
import matplotlib
import pandas
from pandas.plotting import register_matplotlib_converters
import collections
import numpy
import seaborn
import math
import pingouin
import lifelines
import code

logger = logging.getLogger(__file__)
if __name__ != '__main__':
    logging.basicConfig(level=logging.WARN)


class AnalysisException(Exception):
    pass


class Formatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.INFO:
            self._style._fmt = "%(message)s"
        else:
            self._style._fmt = "%(levelname)s: %(message)s"
        return super().format(record)


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


def process_issue_data(data,
                       since='',
                       until='',
                       exclude_weekends=False):
    if data.empty:
        logger.warning('Data for issue analysis is empty')
        return

    # Filter out issues before since date and after until date
    if since:
        data = data[data['issue_created_date'] >= pandas.to_datetime(since)]
    if until:
        data = data[data['issue_created_date'] < pandas.to_datetime(until)]

    issues = collections.defaultdict(list)
    issue_ids = dict()
    issue_keys = dict()
    issue_types = dict()
    issue_points = dict()
    for row, item in data.iterrows():
        issues[item.issue_id].append(item)
        issue_types[item.issue_id] = item.issue_type_name
        issue_ids[item.issue_key] = item.issue_id
        issue_keys[item.issue_id] = item.issue_key
        issue_points[item.issue_id] = item.issue_points

    categories = collections.defaultdict(set)

    issue_statuses = collections.defaultdict(dict)

    for issue_id, issue in issues.items():
        for update in issue:
            if update.changelog_id is None:
                continue

            if update.status_to_name:
                categories[update.status_to_category_name].add(update.status_to_name)
            if update.status_from_name:
                categories[update.status_from_category_name].add(update.status_from_name)

            # Find out when the issue was created
            if not issue_statuses[issue_id].get('first_created'):
                issue_statuses[issue_id]['first_created'] = update.issue_created_date
            issue_statuses[issue_id]['first_created'] = min(issue_statuses[issue_id]['first_created'],
                                                            update.issue_created_date)

            # Find out when the issue was first moved to in progress
            if update.status_to_category_name == 'In Progress':
                if not issue_statuses[issue_id].get('first_in_progress'):
                    issue_statuses[issue_id]['first_in_progress'] = update.status_change_date
                issue_statuses[issue_id]['first_in_progress'] = min(issue_statuses[issue_id]['first_in_progress'],
                                                                    update.status_change_date)

            # Find out when the issue was finally moved to completion
            if update.status_to_category_name == 'Complete' or update.status_to_category_name == 'Done':
                if not issue_statuses[issue_id].get('last_complete'):
                    issue_statuses[issue_id]['last_complete'] = update.status_change_date
                issue_statuses[issue_id]['last_complete'] = max(issue_statuses[issue_id]['last_complete'],
                                                                update.status_change_date)

            issue_statuses[issue_id]['prev_update'] = issue_statuses.get(issue_id, {}).get('last_update', {})
            issue_statuses[issue_id]['last_update'] = update

    # Create a new data set of each issue with the dates when the state changes happened.
    # Compute the lead and cycle times of each issue.
    issue_data = pandas.DataFrame(columns=['issue_key',
                                           'issue_type',
                                           'issue_points',
                                           'new',
                                           'new_day',
                                           'in_progress',
                                           'in_progress_day',
                                           'complete',
                                           'complete_day',
                                           'lead_time',
                                           'lead_time_days',
                                           'cycle_time',
                                           'cycle_time_days',
                                           ])

    for issue_id in issue_statuses:
        new = issue_statuses[issue_id].get('first_created')
        in_progress = issue_statuses[issue_id].get('first_in_progress')
        complete = issue_statuses[issue_id].get('last_complete')

        # Compute cycle time
        lead_time = pandas.Timedelta(days=0)
        cycle_time = pandas.Timedelta(days=0)

        if complete:
            lead_time = complete - new
            cycle_time = lead_time

            if in_progress:
                cycle_time = complete - in_progress
            else:
                cycle_time = pandas.Timedelta(days=0)

        # Adjust lead time and cycle time for weekend (non-working) days
        if complete and exclude_weekends:
            weekend_days = numpy.busday_count(new.date(), complete.date(), weekmask='Sat Sun')
            lead_time -= pandas.Timedelta(days=weekend_days)

            if in_progress:
                weekend_days = numpy.busday_count(in_progress.date(), complete.date(), weekmask='Sat Sun')
                cycle_time -= pandas.Timedelta(days=weekend_days)

        # Ensure there's no negative lead times / cycle times
        if lead_time / pandas.to_timedelta(1, unit='D') < 0:
            lead_time = pandas.Timedelta(days=0)

        if cycle_time / pandas.to_timedelta(1, unit='D') < 0:
            cycle_time = pandas.Timedelta(days=0)

        temp_dict = {'issue_key': [issue_keys.get(issue_id)],
                     'issue_type': [issue_types.get(issue_id)],
                     'issue_points': [issue_points.get(issue_id)],
                     'new': [new],
                     'new_day': [None],
                     'in_progress': [in_progress],
                     'in_progress_day': [None],
                     'complete': [complete],
                     'complete_day': [None],
                     'lead_time': [lead_time],
                     'lead_time_days': [None],
                     'cycle_time': [cycle_time],
                     'cycle_time_days': [None],
                     }
        temp_issue_data = pandas.DataFrame(temp_dict)
        issue_data = pandas.concat([issue_data, temp_issue_data], ignore_index=True)

    # Convert issue_points to float
    issue_data['issue_points'] = issue_data['issue_points'].astype(float)

    # Truncate days
    issue_data.loc[issue_data['new'].isnull(), 'new'] = None
    issue_data['new'] = pandas.to_datetime(issue_data['new'])
    issue_data['new_day'] = issue_data['new'].values.astype('<M8[D]')

    issue_data.loc[issue_data['in_progress'].isnull(), 'in progress'] = None
    issue_data['in_progress'] = pandas.to_datetime(issue_data['in_progress'])
    issue_data['in_progress_day'] = issue_data['in_progress'].values.astype('<M8[D]')

    issue_data.loc[issue_data['complete'].isnull(), 'complete'] = None
    issue_data['complete'] = pandas.to_datetime(issue_data['complete'])
    issue_data['complete_day'] = issue_data['complete'].values.astype('<M8[D]')

    # Add column for lead time represented as days
    issue_data['lead_time_days'] = issue_data['lead_time'] / pandas.to_timedelta(1, unit='D')

    # Round lead time less than 1 hour to zero
    issue_data.loc[issue_data['lead_time_days'] < 1 / 24.0, 'lead_time_days'] = 0

    # Add column for cycle time represented as days
    issue_data['cycle_time_days'] = issue_data['cycle_time'] / pandas.to_timedelta(1, unit='D')

    # Round cycle time less than 1 hour to zero
    issue_data.loc[issue_data['cycle_time_days'] < 1 / 24.0, 'cycle_time_days'] = 0

    # Add column for the previous statuses of this issue
    issue_data['prev_issue_status'] = [issue_statuses[issue_ids[key]].get('prev_update', {}).get('status_to_name') for
                                       key in issue_data['issue_key']]
    issue_data['prev_issue_status_change_date'] = [
        issue_statuses[issue_ids[key]].get('prev_update', {}).get('status_change_date') for key in
        issue_data['issue_key']]
    issue_data['prev_issue_status_category'] = [
        issue_statuses[issue_ids[key]].get('prev_update', {}).get('status_to_category_name') for key in
        issue_data['issue_key']]

    # Add column for the last statuses of this issue
    issue_data['last_issue_status'] = [issue_statuses[issue_ids[key]].get('last_update', {}).get('status_to_name') for
                                       key in issue_data['issue_key']]
    issue_data['last_issue_status_change_date'] = [
        issue_statuses[issue_ids[key]].get('last_update', {}).get('status_change_date') for key in
        issue_data['issue_key']]
    issue_data['last_issue_status_category'] = [
        issue_statuses[issue_ids[key]].get('last_update', {}).get('status_to_category_name') for key in
        issue_data['issue_key']]

    # Set the index
    issue_data = issue_data.set_index('issue_key')

    extra = (categories,
             issues,
             issue_ids,
             issue_keys,
             issue_types,
             issue_points,
             issue_statuses,
             )

    return issue_data, extra


def process_lead_data(issue_data, since='', until=''):
    if issue_data.empty:
        logger.warning('Data for lead time analysis is empty')
        return

    lead_data = issue_data.copy()
    lead_data = lead_data.sort_values(['complete'])

    if since:
        lead_data = lead_data[lead_data['complete_day'] >= pandas.to_datetime(since)]
    if until:
        lead_data = lead_data[lead_data['complete_day'] < pandas.to_datetime(until)]

    # Drop issues with a lead time less than 1 hour
    lead_data = lead_data[lead_data['lead_time_days'] > (1 / 24.0)]

    data = pandas.DataFrame()
    data['Create Date'] = lead_data['new_day']
    data['Complete Date'] = lead_data['complete_day']
    data['Lead Time'] = lead_data['lead_time_days']
    data['Moving Average (10 items)'] = lead_data['lead_time_days'].rolling(window=10).mean()
    data['Moving Standard Deviation (10 items)'] = lead_data['lead_time_days'].rolling(window=10).std()
    data['Average'] = lead_data['lead_time_days'].mean()
    data['Standard Deviation'] = lead_data['lead_time_days'].std()
    data = data.rename_axis('Work Item')

    return data


def process_cycle_data(issue_data, since='', until=''):
    if issue_data.empty:
        logger.warning('Data for cycle analysis is empty')
        return

    cycle_data = issue_data.copy()
    cycle_data = cycle_data.sort_values(['complete'])

    if since:
        cycle_data = cycle_data[cycle_data['complete_day'] >= pandas.to_datetime(since)]
    if until:
        cycle_data = cycle_data[cycle_data['complete_day'] < pandas.to_datetime(until)]

    # Drop issues with a cycle time less than 1 hour
    cycle_data = cycle_data[cycle_data['cycle_time_days'] > (1 / 24.0)]

    data = pandas.DataFrame()
    data['In Progress Date'] = cycle_data['in_progress_day']
    data['Complete Date'] = cycle_data['complete_day']
    data['Cycle Time'] = cycle_data['cycle_time_days']
    data['Moving Average (10 items)'] = cycle_data['cycle_time_days'].rolling(window=10).mean()
    data['Moving Standard Deviation (10 items)'] = cycle_data['cycle_time_days'].rolling(window=10).std()
    data['Average'] = cycle_data['cycle_time_days'].mean()
    data['Standard Deviation'] = cycle_data['cycle_time_days'].std()
    data = data.rename_axis('Work Item')

    return data


def process_throughput_data(issue_data, since='', until=''):
    if issue_data.empty:
        logger.warning('Data for throughput analysis is empty')
        return

    throughput_data = issue_data.copy()
    throughput_data = throughput_data.sort_values(['complete'])

    if since:
        throughput_data = throughput_data[throughput_data['complete_day'] >= pandas.to_datetime(since)]
    if until:
        throughput_data = throughput_data[throughput_data['complete_day'] < pandas.to_datetime(until)]

    points_data = pandas.pivot_table(throughput_data, values='issue_points', index='complete_day', aggfunc=numpy.sum)

    throughput = pandas.crosstab(throughput_data.complete_day, issue_data.issue_type, colnames=[None]).reset_index()

    date_range = pandas.date_range(start=since, end=until, inclusive='left', freq='D')

    cols = set(throughput.columns)
    if 'complete_day' in cols:
        cols.remove('complete_day')

    throughput['Throughput'] = 0
    for col in cols:
        throughput['Throughput'] += throughput[col]

    throughput = throughput.set_index('complete_day')
    throughput['Velocity'] = points_data['issue_points']

    throughput = throughput.reindex(date_range).fillna(0).astype(int).rename_axis('Date')

    throughput['Moving Average (10 days)'] = throughput['Throughput'].rolling(window=10).mean()
    throughput['Moving Standard Deviation (10 days)'] = throughput['Throughput'].rolling(window=10).std()
    throughput['Average'] = throughput['Throughput'].mean()
    throughput['Standard Deviation'] = throughput['Throughput'].std()

    throughput_per_week = pandas.DataFrame(
        throughput['Throughput'].resample('W-Mon').sum()
    )

    throughput_per_week['Moving Average (4 weeks)'] = throughput_per_week['Throughput'].rolling(window=4).mean()
    standard_deviation = throughput_per_week['Throughput'].rolling(window=4).std()
    throughput_per_week['Moving Standard Deviation (4 weeks)'] = standard_deviation
    throughput_per_week['Average'] = throughput_per_week['Throughput'].mean()
    throughput_per_week['Standard Deviation'] = throughput_per_week['Throughput'].std()

    return throughput, throughput_per_week


def process_wip_data(issue_data, since='', until=''):
    if issue_data.empty:
        logger.warning('Data for wip analysis is empty')
        return

    wip_data = issue_data[issue_data['in_progress_day'].notnull()]
    wip_data = wip_data[wip_data['last_issue_status_category'] != 'To Do']
    wip_data = wip_data.sort_values(['in_progress'])

    date_range = pandas.date_range(start=since, end=until, inclusive='left', freq='D')

    wip = pandas.DataFrame(columns=['Date', 'Work In Progress'])

    for date in date_range:
        date_changes = wip_data
        date_changes = date_changes[date_changes['in_progress_day'] <= date]
        date_changes = date_changes[(date_changes['complete_day'].isnull()) | (date_changes['complete_day'] > date)]

        row = dict()
        row['Date'] = [date]
        row['Work In Progress'] = [len(date_changes)]
        row_frame = pandas.DataFrame(row)
        wip = pandas.concat([wip, row_frame], ignore_index=True)

    wip = wip.set_index('Date')
    wip = wip.reindex(date_range).fillna(0).astype(int).rename_axis('Date')

    wip['Moving Average (10 days)'] = wip['Work In Progress'].rolling(window=10).mean()
    wip['Moving Standard Deviation (10 days)'] = wip['Work In Progress'].rolling(window=10).std()
    wip['Average'] = wip['Work In Progress'].mean()
    wip['Standard Deviation'] = wip['Work In Progress'].std()

    # Resample to also provide how much wip we have at the end of each week
    wip_per_week = pandas.DataFrame(wip['Work In Progress'].resample('W-Mon').last())

    wip_per_week['Moving Average (4 weeks)'] = wip_per_week['Work In Progress'].rolling(window=4).mean()
    wip_per_week['Moving Standard Deviation (4 weeks)'] = wip_per_week['Work In Progress'].rolling(window=4).std()
    wip_per_week['Average'] = wip_per_week['Work In Progress'].mean()
    wip_per_week['Standard Deviation'] = wip_per_week['Work In Progress'].std()

    return wip, wip_per_week


def process_wip_age_data(issue_data, since='', until=''):
    if issue_data.empty:
        logger.warning('Data for wip age analysis is empty')
        return

    age_data = issue_data[issue_data['in_progress_day'].notnull()]

    if since:
        age_data = age_data[age_data['in_progress_day'] >= pandas.to_datetime(since)]
    if until:
        age_data = age_data[age_data['in_progress_day'] < pandas.to_datetime(until)]

    # Compute ages for incomplete work
    age_data = age_data[(age_data['complete_day'].isnull()) | (age_data['complete_day'] >= pandas.to_datetime(until))]
    age_data = age_data[age_data['last_issue_status_category'] != 'To Do']
    age_data = age_data.sort_values(['in_progress'])

    today = pandas.to_datetime(until)

    age_data['First In Progress'] = age_data['in_progress_day']
    age_data['Stage'] = age_data['last_issue_status']
    age_data['Age in Stage'] = (today - age_data['last_issue_status_change_date']) / pandas.to_timedelta(1, unit='D')
    age_data['Age'] = (today - age_data['in_progress']) / pandas.to_timedelta(1, unit='D')
    age_data['Average'] = age_data['Age'].mean()
    age_data['Standard Deviation'] = age_data['Age'].std()
    age_data['P50'] = age_data['Age'].quantile(0.5)
    age_data['P75'] = age_data['Age'].quantile(0.75)
    age_data['P85'] = age_data['Age'].quantile(0.85)
    age_data['P95'] = age_data['Age'].quantile(0.95)
    age_data['P99'] = age_data['Age'].quantile(0.999)

    # Fix negative age in stages (because of an until that is set before completion date)
    age_data.loc[age_data['Age in Stage'] < 0, 'Stage'] = 'Unknown'
    age_data.loc[age_data['Age in Stage'] < 0, 'Age in Stage'] = age_data.loc[age_data['Age in Stage'] < 0, 'Age']

    return age_data


def cmd_summary(output, issue_data, since='', until=''):
    # Current lead time
    lt = process_lead_data(issue_data, since=since, until=until)

    # Current cycle time
    c = process_cycle_data(issue_data, since=since, until=until)

    # Current throughput
    t, tw = process_throughput_data(issue_data, since=since, until=until)

    # Current wip
    w, ww = process_wip_data(issue_data, since=since, until=until)
    a = process_wip_age_data(issue_data, since=since, until=until)

    lead_time = pandas.DataFrame.from_records([
        ('Average', lt['Average'].iat[-1]),
        ('Standard Deviation', lt['Standard Deviation'].iat[-1]),
        ('Moving Average (10 items)', lt['Moving Average (10 items)'].iat[-1]),
        ('Moving Standard Deviation (10 items)', lt['Moving Standard Deviation (10 items)'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    cycle_time = pandas.DataFrame.from_records([
        ('Average', c['Average'].iat[-1]),
        ('Standard Deviation', c['Standard Deviation'].iat[-1]),
        ('Moving Average (10 items)', c['Moving Average (10 items)'].iat[-1]),
        ('Moving Standard Deviation (10 items)', c['Moving Standard Deviation (10 items)'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    throughput = pandas.DataFrame.from_records([
        ('Average', t['Average'].iat[-1]),
        ('Standard Deviation', t['Standard Deviation'].iat[-1]),
        ('Moving Average (10 days)', t['Moving Average (10 days)'].iat[-1]),
        ('Moving Standard Deviation (10 days)', t['Moving Standard Deviation (10 days)'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    throughput_weekly = pandas.DataFrame.from_records([
        ('Average', tw['Average'].iat[-1]),
        ('Standard Deviation', tw['Standard Deviation'].iat[-1]),
        ('Moving Average (4 weeks)', tw['Moving Average (4 weeks)'].iat[-1]),
        ('Moving Standard Deviation (4 weeks)', tw['Moving Standard Deviation (4 weeks)'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    wip = pandas.DataFrame.from_records([
        ('Average', w['Average'].iat[-1]),
        ('Standard Deviation', w['Standard Deviation'].iat[-1]),
        ('Moving Average (10 days)', w['Moving Average (10 days)'].iat[-1]),
        ('Moving Standard Deviation (10 days)', w['Moving Standard Deviation (10 days)'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    wip_weekly = pandas.DataFrame.from_records([
        ('Average', ww['Average'].iat[-1]),
        ('Standard Deviation', ww['Standard Deviation'].iat[-1]),
        ('Moving Average (4 weeks)', ww['Moving Average (4 weeks)'].iat[-1]),
        ('Moving Standard Deviation (4 weeks)', ww['Moving Standard Deviation (4 weeks)'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    wip_age = pandas.DataFrame.from_records([
        ('Average', a['Average'].iat[-1]),
        ('50th Percentile', a['P50'].iat[-1]),
        ('75th Percentile', a['P75'].iat[-1]),
        ('85th Percentile', a['P85'].iat[-1]),
        ('95th Percentile', a['P95'].iat[-1]),
    ],
        columns=('Metric', 'Value'),
        index='Metric')

    output_formatted_data(output, 'Lead Time', lead_time)
    output_formatted_data(output, 'Cycle Time', cycle_time)
    output_formatted_data(output, 'Throughput (Daily)', throughput)
    output_formatted_data(output, 'Throughput (Weekly)', throughput_weekly)
    output_formatted_data(output, 'Work In Progress (Daily)', wip)
    output_formatted_data(output, 'Work In Progress (Weekly)', wip_weekly)
    output_formatted_data(output, f'Work In Progress Age (ending {until})', wip_age)


def process_flow_category_data(data, since='', until=''):
    if data.empty:
        logger.warning('Data for flow analysis is empty')
        return

    if not since or not until:
        raise AnalysisException('Flow analysis requires both `since` and `until` dates for processing')

    dates = pandas.date_range(start=since, end=until, inclusive='left', freq='D')

    flow_data = data.copy().reset_index()
    flow_data = flow_data.sort_values(['status_change_date'])

    statuses = set(flow_data['status_from_category_name']) | set(flow_data['status_to_category_name'])
    if numpy.nan in statuses:
        statuses.remove(numpy.nan)

    f = pandas.DataFrame(columns=['Date'] + list(statuses))

    last_counter = None

    for date in dates:
        tomorrow = date + pandas.Timedelta(days=1)
        date_changes = flow_data
        date_changes = date_changes[date_changes['status_change_date'] >= date]
        date_changes = date_changes[date_changes['status_change_date'] < tomorrow]

        if last_counter:
            counter = last_counter
        else:
            counter = collections.Counter()
        for item in date_changes['status_from_category_name']:
            if counter[item] > 0:
                counter[item] -= 1
        for item in date_changes['status_to_category_name']:
            counter[item] += 1

        row = dict(counter)
        row['Date'] = [date]
        row_frame = pandas.DataFrame(row)
        f = pandas.concat([f, row_frame], ignore_index=True)

        last_counter = counter

    f = f.fillna(0)
    if f.empty:
        return f

    f['Date'] = f['Date'].dt.normalize()
    f['Date'] = f['Date'].dt.date

    f = f.set_index('Date')

    return f


def process_flow_data(data, since='', until=''):
    if data.empty:
        logger.warning('Data for flow analysis is empty')
        return

    if not since or not until:
        raise AnalysisException('Flow analysis requires both `since` and `until` dates for processing')

    dates = pandas.date_range(start=since, end=until, inclusive='left', freq='D')

    flow_data = data.copy().reset_index()
    flow_data = flow_data.sort_values(['status_change_date'])

    statuses = set(flow_data['status_from_name']) | set(flow_data['status_to_name'])
    if numpy.nan in statuses:
        statuses.remove(numpy.nan)

    f = pandas.DataFrame(columns=['Date'] + list(statuses))

    last_counter = None

    for date in dates:
        tomorrow = date + pandas.Timedelta(days=1)
        date_changes = flow_data
        date_changes = date_changes[date_changes['status_change_date'] >= date]
        date_changes = date_changes[date_changes['status_change_date'] < tomorrow]

        if last_counter:
            counter = last_counter
        else:
            counter = collections.Counter()
        for item in date_changes['status_from_name']:
            if counter[item] > 0:
                counter[item] -= 1
        for item in date_changes['status_to_name']:
            counter[item] += 1

        row = dict(counter)
        row['Date'] = [date]
        row_frame = pandas.DataFrame(row)
        f = pandas.concat([f, row_frame], ignore_index=True)

        last_counter = counter

    f = f.fillna(0)
    if f.empty:
        return f

    f['Date'] = f['Date'].dt.normalize()
    f['Date'] = f['Date'].dt.date

    f = f.set_index('Date')

    return f


def plot_correlation(x, y, color='xkcd:muted blue', ax=None):
    # plot a Pearson regression between two sets (usually issue_points and cycle_time_days)
    return seaborn.regplot(x=x, y=y, color=color, ax=ax)


def plot_flow_trendlines(flow_data, status_columns=None, ax=None):
    if status_columns is None:
        status_columns = flow_data.columns

    flow_columns = list(reversed(status_columns))

    flow = flow_data[flow_columns]
    flow.index = pandas.to_datetime(flow.index)

    g = None
    lastly = 0
    for i, col in enumerate(reversed(flow_columns)):
        y = (flow[col] + lastly).astype(float)
        g = plot_correlation(flow.reset_index().index, y, color=f'C{i}', ax=ax)
        lastly = y

    if not g:
        return

    g.set_title(f"Cumulative Flow Since {flow.index.min().strftime('%Y-%m-%d')}",
                loc='left',
                fontdict={'fontsize': 18,
                          'fontweight': 'normal'})

    g.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(5))
    ticks_loc = g.get_xticks().tolist()
    g.xaxis.set_major_locator(matplotlib.ticker.FixedLocator(ticks_loc))
    labels = [flow.reset_index().iloc[min(int(x), len(flow) - 1)]['Date'].strftime('%Y-%m-%d') for x in g.get_xticks()]
    g.set_xticklabels(labels)

    g.set_ylabel('Items')
    g.set_xlabel('Timeline')
    g.set_ylim(ymin=0)

    g.legend(list(reversed(flow_columns)))

    return g


def plot_flow(flow_data, status_columns=None, ax=None):
    if status_columns is None:
        status_columns = flow_data.columns

    flow_columns = list(reversed(status_columns))
    last_col = flow_columns[-1]

    flow = flow_data[flow_columns]
    flow.index = pandas.to_datetime(flow.index)

    # y_min is the minimum of the last stage
    y_min = flow[last_col].min()

    # y_max is the maximum of the sums
    y_max = flow[flow_columns].sum(axis=1).max()

    # Create the individual area counts for each status
    flow_agg = flow.copy()

    ys = []
    for col in reversed(flow_columns):
        lastly = ys[-1] if ys else 0
        y = flow_agg[col] = (flow[col] + lastly).astype(float)
        ys.append(y)

    # Melt the data to be able to be sent to lineplot
    flow_melted = pandas.melt(flow_agg.reset_index(), ['Date'])
    flow_melted['value'] = flow_melted['value'].astype(float)

    # Plot the lines
    g = seaborn.lineplot(data=flow_melted,
                         x='Date',
                         y='value',
                         hue='variable',
                         palette=list(reversed([f'C{i}' for i in range(len(ys))])),
                         ax=ax)

    # Fill between the lines
    lastly = 0
    for i, y in enumerate(ys):
        g.fill_between(flow_agg.index, lastly, y, color=f'C{i}', alpha=0.7, interpolate=False)
        lastly = y

    # Label everything
    g.set_title(f"Cumulative Flow Since {flow_agg.index.min().strftime('%Y-%m-%d')}",
                loc='left',
                fontdict={'fontsize': 18,
                          'fontweight': 'normal'})

    g.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(5))
    ticks_loc = g.get_xticks().tolist()
    g.xaxis.set_major_locator(matplotlib.ticker.FixedLocator(ticks_loc))

    g.set_xlabel('Timeline')
    g.set_ylabel('Items')

    tenth = (y_max - y_min) * 0.1
    g.set_ylim([y_min - tenth, y_max + 2 * tenth])

    return g


def cmd_detail_flow(output,
                    data,
                    since='',
                    until='',
                    categorical=False,
                    plot=None,
                    plot_trendline=False,
                    columns=None):
    if categorical:
        flow_data = process_flow_category_data(data, since=since, until=until)
        output_formatted_data(output, 'Cumulative Flow (Categorical)', flow_data)
    else:
        flow_data = process_flow_data(data, since=since, until=until)
        output_formatted_data(output, 'Cumulative Flow', flow_data)

    if plot:
        fig, ax = matplotlib.pyplot.subplots(1, 1, dpi=150, figsize=(15, 10))

        if plot_trendline:
            plot_flow_trendlines(flow_data, status_columns=columns, ax=ax)
        else:
            plot_flow(flow_data, status_columns=columns, ax=ax)

        fig.savefig(plot)


def cmd_detail_wip(output, issue_data, wip_type='', since='', until=''):
    # Current wip
    w, ww = process_wip_data(issue_data, since=since, until=until)
    a = process_wip_age_data(issue_data, since=since, until=until)

    if wip_type == 'daily':
        output_formatted_data(output, 'Work In Progress (Daily)', w)

    if wip_type == 'weekly':
        output_formatted_data(output, 'Work In Progress (Weekly)', ww)

    if wip_type == 'aging':
        wa = a[['First In Progress', 'Age', 'Stage', 'Age in Stage']]
        output_formatted_data(output, f'Work In Progress Age (ending {until})', wa)


def cmd_detail_throughput(output, issue_data, since='', until='', throughput_type=''):
    # Current throughput
    t, tw = process_throughput_data(issue_data, since=since, until=until)

    if throughput_type == 'daily':
        output_formatted_data(output, 'Throughput (Daily)', t)

    if throughput_type == 'weekly':
        output_formatted_data(output, 'Throughput (Weekly)', tw)


def cmd_detail_cycletime(output, issue_data, since='', until=''):
    # Current cycle time
    c = process_cycle_data(issue_data, since=since, until=until)
    output_formatted_data(output, 'Cycle Time', c)


def cmd_detail_leadtime(output, issue_data, since='', until=''):
    # Current lead time
    c = process_lead_data(issue_data, since=since, until=until)
    output_formatted_data(output, 'Lead Time', c)


def process_correlation(x, y):
    # Run a pearson correlation analysis between two sets (usually issue_points and cycle_time_days)
    return pingouin.corr(x=x, y=y, method='pearson')


def cmd_correlation(output, issue_data, since='', until='', plot=None):
    points = issue_data['issue_points'].values.astype(float)
    lead_time = issue_data['lead_time_days'].values.astype(float)
    cycle_time = issue_data['cycle_time_days'].values.astype(float)
    cycle_result = process_correlation(points, cycle_time)
    lead_result = process_correlation(points, lead_time)

    point_summary = pandas.DataFrame.from_records([
        ('Observations', issue_data['issue_points'].count()),
        ('Min', issue_data['issue_points'].min()),
        ('Max', issue_data['issue_points'].max()),
        ('Average', issue_data['issue_points'].mean()),
        ('Standard Deviation', issue_data['issue_points'].std()),
        ],
        columns=('Metric', 'Value'),
        index='Metric')

    cycle_correlation_summary = pandas.DataFrame.from_records([
        ('Observations (n)', cycle_result['n'].iat[0]),
        ('Correlation Coefficient (r)', cycle_result['r'].iat[0]),
        ('Determination Coefficient (r^2)', math.pow(cycle_result['r'].iat[0], 2)),
        ('P-Value (p)', cycle_result['p-val'].iat[0]),
        ('Likelihood of detecting effect (power)', cycle_result['power'].iat[0]),
        ('Significance (p <= 0.05)', 'significant' if cycle_result['p-val'].iat[0] <= 0.05 else 'not significant'),
        ],
        columns=('Metric', 'Value'),
        index='Metric')

    lead_correlation_summary = pandas.DataFrame.from_records([
        ('Observations (n)', lead_result['n'].iat[0]),
        ('Correlation Coefficient (r)', lead_result['r'].iat[0]),
        ('Determination Coefficient (r^2)', math.pow(lead_result['r'].iat[0], 2)),
        ('P-Value (p)', lead_result['p-val'].iat[0]),
        ('Likelihood of detecting effect (power)', lead_result['power'].iat[0]),
        ('Significance (p <= 0.05)', 'significant' if lead_result['p-val'].iat[0] <= 0.05 else 'not significant'),
        ],
        columns=('Metric', 'Value'),
        index='Metric')

    output_formatted_data(output, 'Points', point_summary)
    output_formatted_data(output, 'Points', point_summary)
    output_formatted_data(output, 'Point Correlation to Cycle Time', cycle_correlation_summary)
    output_formatted_data(output, 'Point Correlation to Lead Time', lead_correlation_summary)

    if plot:
        fig, (ax1, ax2) = matplotlib.pyplot.subplots(1, 2, dpi=150, figsize=(15, 10))
        fig.suptitle(f'Point Correlation from {since} to {until}',
                     fontproperties={'size': 20,
                                     'weight': 'normal'})

        # Cycle time
        ax = plot_correlation(points, cycle_time, ax=ax1)
        ax.set_title('Point Correlation to Cycle Time', y=1.02, loc='left',
                     fontdict={'size': 18, 'weight': 'normal'})
        subtitle = '{} (n: {} r: {:.2f} p: {:.2f} α: 0.05)'.format(
            'Significant' if cycle_result['p-val'].iat[0] <= 0.05 else 'Not Significant',
            cycle_result['n'].iat[0],
            cycle_result['r'].iat[0],
            cycle_result['p-val'].iat[0])
        ax.text(x=0, y=1, s=subtitle, fontsize=14, ha='left', va='center', transform=ax.transAxes)
        ax.set_ylabel('Cycle Time (days)')
        ax.set_xlabel('Issue Points')
        ax.set_xlim((1, issue_data['issue_points'].max() + 0.1))

        # Lead time
        ax = plot_correlation(points, lead_time, ax=ax2)
        ax.set_title('Point Correlation to Lead Time', y=1.02, loc='left',
                     fontdict={'size': 18, 'weight': 'normal'})
        subtitle = '{} (n: {} r: {:.2f} p: {:.2f} α: 0.05)'.format(
            'Significant' if lead_result['p-val'].iat[0] <= 0.05 else 'Not Significant',
            lead_result['n'].iat[0],
            lead_result['r'].iat[0],
            lead_result['p-val'].iat[0])
        ax.text(x=0, y=1, s=subtitle, fontsize=14, ha='left', va='center', transform=ax.transAxes)
        ax.set_ylabel('Lead Time (days)')
        ax.set_xlabel('Issue Points')
        ax.set_xlim((1, issue_data['issue_points'].max() + 0.1))

        fig.savefig(plot)


def analyze_survival_km(issue_data, since='', until=''):
    # run a kaplan-meier survivability analysis on the issue data
    survivability_data = issue_data.copy()
    survivability_data = survivability_data[survivability_data['complete_day'] >= pandas.to_datetime(since)]
    survivability_data = survivability_data[survivability_data['complete_day'] < pandas.to_datetime(until)]
    survivability_data = survivability_data.sort_values(['complete_day'])

    durations = survivability_data['cycle_time_days']
    event_observed = [1 if c else 0 for c in survivability_data['cycle_time_days']]

    km = lifelines.KaplanMeierFitter()

    return km.fit(durations, event_observed, label='Kaplan Meier Estimate'), km


def cmd_survival_km(output, issue_data, since='', until=''):
    # Process survival analysis using Kaplan-Meier
    m, _ = analyze_survival_km(issue_data, since=since, until=until)

    km_summary = pandas.DataFrame.from_records([
        (f'{int(q * 100)}%', m.percentile(1 - q)) for q in (0.25, 0.5, 0.75, 0.85, 0.95, 0.999)
        ],
        columns=('Probability', 'Days'),
        index='Probability')

    output_formatted_data(output,
                          'Kaplan-Meier Estimation: Within how many days can a single work item be completed?',
                          km_summary)


def analyze_survival_wb(issue_data, since='', until=''):
    # Run a weibull survivability analysis on the issue data
    survivability_data = issue_data.copy()
    survivability_data = survivability_data[survivability_data['complete_day'] >= pandas.to_datetime(since)]
    survivability_data = survivability_data[survivability_data['complete_day'] < pandas.to_datetime(until)]
    survivability_data = survivability_data.sort_values(['complete_day'])

    durations = [c if c else 0.00001 for c in survivability_data['cycle_time_days']]
    event_observed = [1 if c else 0 for c in survivability_data['cycle_time_days']]

    wb = lifelines.WeibullFitter()

    return wb.fit(durations, event_observed, label='Weibull Estimate'), wb


def cmd_survival_wb(output, issue_data, since='', until=''):
    # process survival analysis using Weibull
    m, _ = analyze_survival_wb(issue_data, since=since, until=until)

    wb_summary = pandas.DataFrame.from_records([
        (f'{int(q * 100)}%', m.percentile(1 - q)) for q in (0.25, 0.5, 0.75, 0.85, 0.95, 0.999)
        ],
        columns=('Probability', 'Days'),
        index='Probability')

    output_formatted_data(output,
                          'Weibull Estimation: Within how many days can a single work item be completed?',
                          wb_summary)


def forecast_montecarlo_how_long_items(throughput_data, items=10, simulations=10000, window=90):
    # Forecast number of days it will take to complete n number of items based on historical throughput
    if throughput_data.empty:
        logger.warning('Data for Monte-Carlo analysis is empty')
        return

    SIMULATION_ITEMS = items
    SIMULATIONS = simulations
    LAST_DAYS = window

    logger.info('Running Monte-Carlo analysis...')

    def simulate_days(data, scope):
        days = 0
        total = 0
        while total <= scope:
            total += data.sample(n=1).iloc[0]['Throughput']
            days += 1
        return days

    dataset = throughput_data[['Throughput']].tail(LAST_DAYS).reset_index(drop=True)
    count = len(dataset)
    if count < window:
        logger.warning(f'Montecarlo window ({window}) is larger than throughput dataset ({count}). '
                       f'Try increasing your date filter to include more observations '
                       f'or decreasing the forecast window size.')

    samples = []
    for i in range(SIMULATIONS):
        if (i+1) % 1000 == 0:
            logger.info(f'-> {i+1} simulations run')
        samples.append(simulate_days(dataset, SIMULATION_ITEMS))
    logger.info('---')
    samples = pandas.DataFrame(samples, columns=['Days'])
    distribution_how_long = samples.groupby(['Days']).size().reset_index(name='Frequency')
    distribution_how_long = distribution_how_long.sort_index(ascending=False)
    frequency_sum = distribution_how_long.Frequency.cumsum()/distribution_how_long.Frequency.sum()
    distribution_how_long['Probability'] = 100 - 100 * frequency_sum

    return distribution_how_long, samples


def cmd_forecast_items_n(output, issue_data, since='', until='', n=10, simulations=10000, window=90):
    # Process forecast items n command
    # pre-req
    t, tw = process_throughput_data(issue_data, since=since, until=until)
    # analysis
    ml, s = forecast_montecarlo_how_long_items(t, items=n, simulations=simulations, window=window)

    forecast_summary = pandas.DataFrame.from_records([
        (f'{int(q*100)}%', s.Days.quantile(q)) for q in (0.25, 0.5, 0.75, 0.85, 0.95, 0.999)
        ],
        columns=('Probability', 'Days'),
        index='Probability')

    output_formatted_data(output,
                          f'Montecarlo Forecast: Within how many days can {n} work items be completed?',
                          forecast_summary)


def forecast_montecarlo_how_many_items(throughput_data, days=10, simulations=10000, window=90):
    # Forecast number of items to be completed in n days based on historical throughput
    if throughput_data.empty:
        logger.warning('Data for Montecarlo analysis is empty')
        return

    SIMULATION_DAYS = days
    SIMULATIONS = simulations
    LAST_DAYS = window

    logger.info('Running Montecarlo analysis...')

    dataset = throughput_data[['Throughput']].tail(LAST_DAYS).reset_index(drop=True)
    count = len(dataset)
    if count < window:
        logger.warning(f'Montecarlo window ({window}) is larger than throughput dataset ({count}). '
                       f'Try increasing your date filter to include more observations '
                       f'or decreasing the forecast window size.')

    samples = []
    for i in range(SIMULATIONS):
        if (i+1) % 1000 == 0:
            logger.info(f'-> {i+1} simulations run')
        samples.append(dataset.sample(n=SIMULATION_DAYS, replace=True).sum()['Throughput'])
    logger.info('---')
    samples = pandas.DataFrame(samples, columns=['Items'])
    distribution_how = samples.groupby(['Items']).size().reset_index(name='Frequency')
    distribution_how = distribution_how.sort_index(ascending=False)
    distribution_how['Probability'] = 100 * distribution_how.Frequency.cumsum()/distribution_how.Frequency.sum()

    return distribution_how, samples


def cmd_forecast_items_days(output, issue_data, since='', until='', days=10, simulations=10000, window=90):
    # Process forecast items days command

    # pre-req
    t, tw = process_throughput_data(issue_data, since=since, until=until)

    # analysis
    mh, s = forecast_montecarlo_how_many_items(t, days=days, simulations=simulations, window=window)

    forecast_summary = pandas.DataFrame.from_records([
        (f'{int(q*100)}%', s.Items.quantile(1-q)) for q in (0.25, 0.5, 0.75, 0.85, 0.95, 0.999)
        ],
        columns=('Probability', 'Items'),
        index='Probability')

    output_formatted_data(output,
                          f'Montecarlo Forecast: How many work items can be completed within {days} days?',
                          forecast_summary)


def forecast_montecarlo_how_long_points(throughput_data, points=10, simulations=10000, window=90):
    # Forecast number of days it will take to complete n number of points based on historical velocity
    if throughput_data.empty:
        logger.warning('Data for Montecarlo analysis is empty')
        return

    SIMULATION_ITEMS = points
    SIMULATIONS = simulations
    LAST_DAYS = window

    logger.info('Running Montecarlo analysis...')

    if (throughput_data['Velocity']/throughput_data['Throughput']).max() == 1:
        logger.warning('All velocity data is equal. Did you load data with points fields?')

    def simulate_days(data, scope):
        days = 0
        total = 0
        while total <= scope:
            total += data.sample(n=1).iloc[0]['Velocity']
            days += 1
        return days

    dataset = throughput_data[['Velocity']].tail(LAST_DAYS).reset_index(drop=True)

    count = len(dataset)
    if count < window:
        logger.warning(f'Montecarlo window ({window}) is larger than velocity dataset ({count}). '
                       f'Try increasing your date filter to include more observations '
                       f'or decreasing the forecast window size.')

    samples = []
    for i in range(SIMULATIONS):
        if (i+1) % 1000 == 0:
            logger.info(f'-> {i+1} simulations run')
        samples.append(simulate_days(dataset, SIMULATION_ITEMS))
    logger.info('---')
    samples = pandas.DataFrame(samples, columns=['Days'])
    distribution_how_long = samples.groupby(['Days']).size().reset_index(name='Frequency')
    distribution_how_long = distribution_how_long.sort_index(ascending=False)
    frequency_sum = distribution_how_long.Frequency.cumsum()/distribution_how_long.Frequency.sum()
    distribution_how_long['Probability'] = 100 - 100 * frequency_sum

    return distribution_how_long, samples


def cmd_forecast_points_n(output, issue_data, since='', until='', n=10, simulations=10000, window=90):
    # Process forecast points n command
    # pre-req
    t, tw = process_throughput_data(issue_data, since=since, until=until)
    # analysis
    ml, s = forecast_montecarlo_how_long_points(t, points=n, simulations=simulations, window=window)

    forecast_summary = pandas.DataFrame.from_records([
        (f'{int(q*100)}%', s.Days.quantile(q)) for q in (0.25, 0.5, 0.75, 0.85, 0.95, 0.999)
        ],
        columns=('Probability', 'Days'),
        index='Probability')

    output_formatted_data(output,
                          f'Montecarlo Forecast: Within how many days can {n} points be completed?',
                          forecast_summary)


def forecast_montecarlo_how_many_points(throughput_data, days=10, simulations=10000, window=90):
    # Forecast number of points to be completed in n days based on historical velocity
    if throughput_data.empty:
        logger.warning('Data for Montecarlo analysis is empty')
        return

    SIMULATION_DAYS = days
    SIMULATIONS = simulations
    LAST_DAYS = window

    logger.info('Running Montecarlo analysis...')

    if (throughput_data['Velocity']/throughput_data['Throughput']).max() == 1:
        logger.warning('All velocity data is equal. Did you load data with points fields?')

    dataset = throughput_data[['Velocity']].tail(LAST_DAYS).reset_index(drop=True)
    count = len(dataset)
    if count < window:
        logger.warning(f'Montecarlo window ({window}) is larger than velocity dataset ({count}). '
                       f'Try increasing your date filter to include more observations '
                       f'or decreasing the forecast window size.')

    samples = []
    for i in range(SIMULATIONS):
        if (i+1) % 1000 == 0:
            logger.info(f'-> {i+1} simulations run')
        samples.append(dataset.sample(n=SIMULATION_DAYS, replace=True).sum()['Velocity'])
    logger.info('---')
    samples = pandas.DataFrame(samples, columns=['Points'])
    distribution_how = samples.groupby(['Points']).size().reset_index(name='Frequency')
    distribution_how = distribution_how.sort_index(ascending=False)
    distribution_how['Probability'] = 100 * distribution_how.Frequency.cumsum()/distribution_how.Frequency.sum()

    return distribution_how, samples


def cmd_forecast_points_days(output, issue_data, since='', until='', days=10, simulations=10000, window=90):
    # Process forecast points days command
    # pre-req
    t, tw = process_throughput_data(issue_data, since=since, until=until)
    # analysis
    mh, s = forecast_montecarlo_how_many_points(t, days=days, simulations=simulations, window=window)

    forecast_summary = pandas.DataFrame.from_records([
        (f'{int(q*100)}%', s.Points.quantile(1-q)) for q in (0.25, 0.5, 0.75, 0.85, 0.95, 0.999)
    ], columns=('Probability', 'points'), index='Probability')

    output_formatted_data(output,
                          f'Montecarlo Forecast: How many points can be completed within {days} days?',
                          forecast_summary)


def cmd_shell(output, data, issue_data, since='', until='', args=None):
    logger.info('Creating interactive Python shell...')
    logger.info('-> locals: %s' % ', '.join(locals().keys()))
    logger.info('---')
    code.interact(local=locals())


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

    # Preprocess issue data
    i, _ = process_issue_data(data,
                              since=since,
                              until=until,
                              exclude_weekends=exclude_weekends)

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
        cmd_forecast_items_n(output,
                             i,
                             since=since,
                             until=until,
                             n=args.n,
                             simulations=args.simulations,
                             window=args.window)

    if args.command == 'forecast' and args.forecast_type == 'items' and args.days:
        cmd_forecast_items_days(output,
                                i,
                                since=since,
                                until=until,
                                days=args.days,
                                simulations=args.simulations,
                                window=args.window)

    if args.command == 'forecast' and args.forecast_type == 'points' and args.n:
        cmd_forecast_points_n(output,
                              i,
                              since=since,
                              until=until,
                              n=args.n,
                              simulations=args.simulations,
                              window=args.window)

    if args.command == 'forecast' and args.forecast_type == 'points' and args.days:
        cmd_forecast_points_days(output,
                                 i,
                                 since=since,
                                 until=until,
                                 days=args.days,
                                 simulations=args.simulations,
                                 window=args.window)

    # Calc shell data
    if args.command == 'shell':
        cmd_shell(output, data, i, since=since, until=until, args=args)


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


def make_parser():
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
    # Get metrics summary
    subparser_summary = subparsers.add_parser(name='summary',
                                              help='Generate a summary of metric data (cycle time, throughput, wip)')
    add_output_params(subparser_summary)
    # Get detailed metrics data
    subparser_detail = subparsers.add_parser(name='detail',
                                             help='Output detailed analysis data')
    subparser_detail_subparsers = subparser_detail.add_subparsers(dest='detail_type')
    subparser_flow = subparser_detail_subparsers.add_parser(name='flow',
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
    subparser_wip = subparser_detail_subparsers.add_parser(name='wip',
                                                           help='Analyze wip and output detail')
    subparser_wip.add_argument('type',
                               choices=('daily', 'weekly', 'aging'),
                               help='Type of wip data to output (daily, weekly, aging)')
    add_output_params(subparser_wip)
    subparser_throughput = subparser_detail_subparsers.add_parser(name='throughput',
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
    subparser_corrrelation = subparsers.add_parser(name='correlation',
                                                   help='Test correlation between issue_points and lead/cycle times')
    add_output_params(subparser_corrrelation)
    add_output_plot_params(subparser_corrrelation)
    # Survival subparser
    subparser_survival = subparsers.add_parser(name='survival',
                                               help='Analyze the survival of work items')
    subparser_survival_subparsers = subparser_survival.add_subparsers(dest='survival_type')
    subparser_survival_km = subparser_survival_subparsers.add_parser(name='km',
                                                                     help='Analyze the survival of work items '
                                                                          'using Kaplan-Meier Estimation')
    add_output_params(subparser_survival_km)
    subparser_survival_wb = subparser_survival_subparsers.add_parser(name='wb',
                                                                     help='Analyze the survival of work items '
                                                                          'using Weibull Estimation')
    add_output_params(subparser_survival_wb)
    # Forecast subparser
    subparser_forecast = subparsers.add_parser(name='forecast',
                                               help='Forecast the future using Monte Carlo simulation')
    subparser_forecast_subparsers = subparser_forecast.add_subparsers(dest='forecast_type')
    subparser_forecast_items = subparser_forecast_subparsers.add_parser(name='items',
                                                                        help='Forecast future work items')
    subparser_forecast_items_group = subparser_forecast_items.add_mutually_exclusive_group(required=True)
    subparser_forecast_items_group.add_argument('-n', '--items',
                                                dest='n',
                                                type=int,
                                                help='Number of items to predict answering the question '
                                                     '"within how many days can N items be completed?"')
    subparser_forecast_items_group.add_argument('-d', '--days',
                                                type=int,
                                                help='Number of days to predict answering the question '
                                                     '"how many items can be completed within N days?"')
    subparser_forecast_items.add_argument('--simulations',
                                          default=10000,
                                          help='Number of simulation iterations to run (default: 10000)')
    subparser_forecast_items.add_argument('--window',
                                          default=90,
                                          help='Window of historical data to use in the forecast (default: 90 days)')
    add_output_params(subparser_forecast_items)
    subparser_forecast_points = subparser_forecast_subparsers.add_parser(name='points',
                                                                         help='Forecast future points')
    subparser_forecast_points_group = subparser_forecast_points.add_mutually_exclusive_group(required=True)
    subparser_forecast_points_group.add_argument('-n', '--points',
                                                 dest='n',
                                                 type=int,
                                                 help='Number of points to predict answering the question '
                                                      '"within how many days can N points be completed?"')
    subparser_forecast_points_group.add_argument('-d', '--days',
                                                 type=int,
                                                 help='Number of days to predict answering the question '
                                                      '"how many points can be completed within N days?"')
    subparser_forecast_points.add_argument('--simulations',
                                           default=10000,
                                           help='Number of simulation iterations to run (default: 10000)')
    subparser_forecast_points.add_argument('--window',
                                           default=90,
                                           help='Window of historical data to use in the forecast (default: 90 days)')
    add_output_params(subparser_forecast_points)
    # Shell subparser
    subparser_shell = subparsers.add_parser(name='shell',
                                            help='Load the data into an interactive Python shell')
    return parser, subparser_detail, subparser_survival, subparser_forecast


def main():
    # noinspection PyGlobalUndefined
    global output_formatted_data

    (parser,
     subparser_detail,
     subparser_survival,
     subparser_forecast
     ) = make_parser()

    args = parser.parse_args()

    ch = logging.StreamHandler()
    ch.setLevel(logging.WARN if args.quiet else logging.INFO)
    ch.setFormatter(Formatter())
    logger.addHandler(ch)
    logger.setLevel(logging.WARN if args.quiet else logging.INFO)

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
