import tornado.web
from celery.result import AsyncResult
from celery import Celery

import waterbutler
from waterbutler.tasks import settings as tasks_settings

app = Celery()
app.config_from_object(tasks_settings)


class StatusHandler(tornado.web.RequestHandler):

    def get(self):
        """List information about waterbutler status"""
        self.write({
            'status': 'up',
            'version': waterbutler.__version__
        })


class PendingHandler(tornado.web.RequestHandler):

    def get(self, task_id, result_resource):
        # TKB need to cover all possible states
        app = Celery()
        app.config_from_object(tasks_settings)
        result = AsyncResult(id=task_id, app=app)
        if str(result.ready()) == 'True':
            if str(result.state) == 'SUCCESS':
                meta, created = result.get(timeout=3)
                self.set_status(303)
                self.add_header('Content-Location', '{}://{}/v1/resources/{}/providers/{}{}'.format(self.request.protocol, self.request.host, result_resource, meta.provider, meta.path))
                self.write({'data':
                    {'task_id': task_id,
                     'state': str(result.state),
                     'ready': str(result.ready()),
                     }
                })
            elif str(result.state) == 'FAILURE':
                self.set_status(200)
                self.write({'errors':
                    {'status': result.error_code,
                     'source': {'pointer': self.url},
                     'title': result.error_msg,
                     'detail': result.error_dtl
                     }
                })
        else:
            self.write({
                'task_id': task_id,
                'state': str(result.state),
            })
