import os

from django.db import DatabaseError, IntegrityError, connection

from health_check.backends import BaseHealthCheckBackend
from health_check.exceptions import ServiceReturnedUnexpectedResult, ServiceUnavailable

from .models import TestModel


class DatabaseBackend(BaseHealthCheckBackend):

    def set_query_timeout(self, timeout, cursor):
        engine = connection.settings_dict['ENGINE']
        if 'postgresql' in engine:
            cursor.execute(f"SET statement_timeout = '{timeout}s'")
        elif 'mysql' in engine:
            cursor.execute(f"SET SESSION max_statement_time = {timeout}")
        else:
            return None


    def check_status(self):
        timeout = os.environ.get('QUERY_TIMEOUT', 2)
        try:
            with connection.cursor() as cursor:
                self.set_query_timeout(timeout, cursor)
                obj = TestModel.objects.create(title="test")
                obj.title = "newtest"
                obj.save()
                obj.delete()
        except IntegrityError:
            raise ServiceReturnedUnexpectedResult("Integrity Error")
        except DatabaseError as e:
            raise ServiceUnavailable("Database error {}".format(e))
