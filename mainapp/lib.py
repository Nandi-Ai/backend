import logging
import os
from mainapp import settings
from rest_framework.authentication import TokenAuthentication
from rest_framework.exceptions import AuthenticationFailed
from datetime import datetime as dt,timedelta as td
import sqlparse


def startup():
    os.environ["AWS_ACCESS_KEY_ID"] = settings.aws_access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = settings.aws_secret_access_key
    os.environ["AWS_REGION"] = settings.aws_region

def validate_query(query, dataset):
    query_parsed = sqlparse(query)
    statement = query_parsed[0]

    if statement.get_type() == "SELECT":
        pass

    return True, False #validated, no reason..

class MyTokenAuthentication(TokenAuthentication):
    keyword = "Bearer"

    def authenticate_credentials(self, key):
        model = self.get_model()
        try:
            token = model.objects.select_related('user').get(key=key)
        except model.DoesNotExist:
            raise AuthenticationFailed('Invalid token.')

        if not token.user.is_active:
            raise AuthenticationFailed('User inactive or deleted')

        # This is required for the time comparison
        now = dt.now()

        if token.created < now - td(hours=settings.token_valid_hours):
            raise AuthenticationFailed('Token has expired')

        # if there are issues with multiple tokens for users uncomment
        # token.created = now
        # token.save()

        return token.user, token
