import boto3
import datetime
import pprint
import os
import time
from prometheus_client import start_http_server, Gauge, Enum
import re
import requests

class AWSScheduledEvents:
    health = None

    def __init__(self):
        session = boto3.session.Session()
        self.health = \
            session.client(service_name='health',
                           region_name='us-east-1')

    def get_aws_scheduled_events_and_affected_entities(self):
        if self.health:
            described_events = self.health.describe_events(
                filter={
                    'regions': [
                        'eu-west-1',
                        'global'
                    ],
                   'lastUpdatedTimes': [ {
                        'from': datetime.datetime(2021, 12, 1),
                        'to': datetime.datetime.now() },
                    ],
                    'eventTypeCategories': [
                        'issue',
                        'scheduledChange',
                    ],
                    'eventStatusCodes': [
                        'open',
                        'closed',
                        'upcoming',
                    ]
                })
            event_arns = []
            for event in described_events['events']:
                event_arns.append(event['arn'])
            if len(event_arns) != 0:
                affected_entities = self.health.describe_affected_entities(
                   filter={
                       'eventArns': event_arns,
                   },
                )
                return affected_entities['entities']
            else:
                return []
        else:
            raise ValueError

class AppMetrics:
    """
    Representation of Prometheus metrics and loop to fetch and transform
    application metrics into Prometheus metrics.
    """

    def __init__(self, app_url, polling_interval_seconds=3600):
        self.app_url = app_url
        self.polling_interval_seconds = polling_interval_seconds

        # Prometheus metrics to collect
        self.aws_health_number_of_events = Gauge("aws_health_number_of_events", "Current number of events")
        self.aws_health_event = Gauge("aws_health_event", "Event", labelnames=["event_arn", "last_update_time", "status_code", "entity_arn", "entity_value" ])
        self.aws_events = AWSScheduledEvents()

    def run_metrics_loop(self):
        """Metrics fetching loop"""

        while True:
            self.fetch()
            time.sleep(self.polling_interval_seconds)

    def fetch(self):
        """
        Get metrics from application and refresh Prometheus metrics with
        new values.
        """

        # Fetch raw status data from the application
        try:
            entities = self.aws_events.get_aws_scheduled_events_and_affected_entities()
            # Update Prometheus metrics with application metrics
            self.aws_health_number_of_events.set(len(entities))
            for entity in entities:
                self.aws_health_event.labels(event_arn = entity.get('eventArn'), last_update_time = entity.get('lastUpdatedTime'), status_code = entity.get('statusCode'), entity_arn = entity.get('entityArn'), entity_value = entity.get('entityValue')).set(1)

        except requests.exceptions.RequestException as e:
            self.aws_number_of_events.set("Nan")
            print("[ ERROR ] Something bad happened ...")

def main():
    """Main entry point"""

    polling_interval_seconds = int(os.getenv("POLLING_INTERVAL_SECONDS", "3600"))
    app_url = os.getenv("APP_URL", "")
    exporter_port = int(os.getenv("EXPORTER_PORT", "9876"))

    app_metrics = AppMetrics(
        app_url=app_url,
        polling_interval_seconds=polling_interval_seconds
    )
    start_http_server(exporter_port)
    app_metrics.run_metrics_loop()

if __name__ == "__main__":
    main()
