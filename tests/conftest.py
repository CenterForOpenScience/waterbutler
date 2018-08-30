import aiohttpretty

from waterbutler import settings

# Clear all celery settings
settings.config['TASKS_CONFIG'] = {
    'WAIT_TIME_OUT': 30,
    'CELERY_ALWAYS_EAGER': True,
    'CELERY_RESULT_BACKEND': 'redis://'
}



def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'aiohttpretty: mark tests to activate aiohttpretty'
    )


def pytest_runtest_setup(item):
    marker = item.get_marker('aiohttpretty')
    if marker is not None:
        aiohttpretty.clear()
        aiohttpretty.activate()


def pytest_runtest_teardown(item, nextitem):
    marker = item.get_marker('aiohttpretty')
    if marker is not None:
        aiohttpretty.deactivate()
