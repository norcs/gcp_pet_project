import json
import settings

from google.auth import jwt
from google.cloud import pubsub_v1


service_account_info = json.load(open(settings.KEY_PATH))


def create_subscriber() -> pubsub_v1:
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"
    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )
    return pubsub_v1.SubscriberClient(credentials=credentials)


def create_publisher() -> pubsub_v1:
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    credentials = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )
    return pubsub_v1.PublisherClient(credentials=credentials)