import csv
import datetime
import dateutil.parser
import pytz
import logging


class CSVReportGenerator:
    def __init__(self,
                 extractor,
                 csv_file,
                 project_key,
                 since,
                 custom_fields=None,
                 custom_field_names=None,
                 updates_only=False,
                 anonymize=False):
        self.extractor = extractor
        self.csv_file = csv_file
        self.project_key = project_key
        self.since = since
        self.custom_fields = custom_fields or []
        self.custom_field_names = custom_field_names or []
        self.updates_only = updates_only
        self.anonymize = anonymize
        self.field_names = self.__build_field_names()

    def __build_field_names(self):
        default_fields = ['project_id',
                          'project_key',
                          'issue_id',
                          'issue_key',
                          'issue_type_id',
                          'issue_type_name',
                          'issue_title',
                          'issue_created_date',
                          'changelog_id',
                          'status_from_id',
                          'status_from_name',
                          'status_to_id',
                          'status_to_name',
                          'status_from_category_name',
                          'status_to_category_name',
                          'status_change_date']
        # Append custom field names or ids
        return default_fields + (self.custom_field_names or self.custom_fields)

    def __map_custom_fields(self, record):
        """Map custom fields to their corresponding names if provided."""
        custom_field_map = dict(zip(self.custom_fields, self.custom_field_names))
        for field_id, field_name in custom_field_map.items():
            if field_id in record:
                record[field_name] = record.pop(field_id)
        return record

    @staticmethod
    def __anonymize_record(record):
        """Anonymize sensitive fields in the record."""
        record['issue_key'] = record['issue_key'].replace(record['project_key'], 'ANON')
        record['project_key'] = 'ANON'
        record['issue_title'] = 'Anonymized Title'
        return record

    @staticmethod
    def __parse_dates(record):
        """Convert date fields to ISO format."""
        for key, value in record.items():
            if 'date' in key and value and not isinstance(value, datetime.datetime):
                record[key] = dateutil.parser.parse(value).astimezone(pytz.UTC).isoformat()
        return record

    def generate(self, write_header=False):
        writer = csv.DictWriter(f=self.csv_file, fieldnames=self.field_names)
        if write_header:
            writer.writeheader()

        records = self.extractor.fetch(project_key=self.project_key,
                                       since=self.since,
                                       custom_fields=self.custom_fields,
                                       updates_only=self.updates_only)

        count = 0
        for record in records:
            record = self.__parse_dates(record)

            if self.anonymize:
                record = self.__anonymize_record(record)

            if self.custom_fields:
                record = self.__map_custom_fields(record)

            writer.writerow(record)
            count += 1

        logging.info(f'{count} records written')
