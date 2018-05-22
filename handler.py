import boto3
import json
import datetime
from datetime import timezone

def producer(event, context):
    sns = boto3.client('sns')

    context_parts = context.invoked_function_arn.split(':')
    topic_name = "my-topic-name"
    topic_arn = "arn:aws:sns:{region}:{account_id}:{topic}".format(
        region=context_parts[3], account_id=context_parts[4], topic=topic_name)

    now = datetime.datetime.now(timezone.utc)
    start_date = (now - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    params = {"startDate": start_date, "endDate": end_date, "tags": ["neo4j"]}

    sns.publish(TopicArn= topic_arn, Message= json.dumps(params))


def consumer(event, context):
    for record in event["Records"]:
        message = json.loads(record["Sns"]["Message"])

        start_date = message["startDate"]
        end_date = message["endDate"]
        tags = message["tags"]

        print("start_date: " + start_date)
        print("end_date: " + end_date)
        print("tags: " + str(tags))