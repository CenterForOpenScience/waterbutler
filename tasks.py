import os
import sys

from invoke import task

WHEELHOUSE_PATH = os.environ.get('WHEELHOUSE')


@task
def wheelhouse(ctx, develop=False, pty=True):
    extras = '--with dev' if develop else ''
    cmd = f'poetry export --format=requirements.txt {extras} | pip wheel --find-links={WHEELHOUSE_PATH} -r /dev/stdin --wheel-dir={WHEELHOUSE_PATH}'
    ctx.run(cmd, pty=pty)


@task
def install(ctx, develop=False, pty=True):
    extras = '--with dev' if develop else ''
    ctx.run(f'poetry install {extras}', pty=pty)


@task
def flake(ctx):
    ctx.run('poetry run flake8 .', pty=True)


@task
def mypy(ctx):
    ctx.run('poetry run mypy waterbutler/', pty=True)


@task
def test(ctx, verbose=False, types=False, nocov=False, provider=None, path=None):
    """Run full or customized tests for WaterButler.

    :param ctx: the ``invoke`` context
    :param verbose: the flag to increase verbosity
    :param types: the flag to enable ``mypy`` test
    :param nocov: the flag to disable coverage
    :param provider: limit the tests to the given provider only
    :param path: limit the tests to the given path only

    :return: None
    """

    flake(ctx)
    if types:
        mypy(ctx)

    # `--provider=` and `--path=` are mutually exclusive options
    assert not (provider and path)
    if path:
        path = f'/{path}'
    elif provider:
        path = f'/providers/{provider}/'
    else:
        path = ''

    coverage = ' --cov-report term-missing --cov waterbutler' if not nocov else ''
    verbosity = '-v' if verbose else ''
    cmd = f'poetry run pytest{coverage} tests{path} {verbosity}'
    ctx.run(cmd, pty=True)


@task
def celery(ctx, loglevel='INFO', hostname='%h'):
    from waterbutler.tasks.app import app
    command = ['worker']
    if loglevel:
        command.extend(['--loglevel', loglevel])
    if hostname:
        command.extend(['--hostname', hostname])
    app.worker_main(command)


@task
def rabbitmq(ctx):
    ctx.run('rabbitmq-server', pty=True)


@task
def server(ctx):
    if os.environ.get('REMOTE_DEBUG', None):
        import pydevd
        remote_parts = os.environ.get('REMOTE_DEBUG').split(':')
        pydevd.settrace(remote_parts[0], port=int(remote_parts[1]), suspend=False,
                        stdoutToServer=True, stderrToServer=True)

    from waterbutler.server.app import serve
    serve()


@task
def clean(ctx, verbose=False):
    cmd = 'find . -name "*.pyc" -delete'
    if verbose:
        print(cmd)
    ctx.run(cmd, pty=True)


@task
def newrelic_init(ctx, key=None, verbose=False):
    if key is None:
        sys.exit('No newrelic api key given.  Please generate one and rerun this command '
                 'with `invoke newrelic_init --key=$key`')

    cmd_tmpl = 'newrelic-admin generate-config {} newrelic.ini'
    cmd = cmd_tmpl.format(key)
    if verbose:
        print(cmd_tmpl.format('<redacted>'))
    ctx.run(cmd, pty=True)


@task
def newrelic_server(ctx, config='newrelic.ini', verbose=False):
    if not os.path.exists(config):
        sys.exit("Couldn't find config file '{}'.  Check path or run `invoke newrelic_init` "
                 "to generate it.".format(config))

    cmd = f'poetry run env NEW_RELIC_CONFIG_FILE={config} newrelic-admin run-program invoke server'
    if verbose:
        print(cmd)
    ctx.run(cmd, pty=True)
