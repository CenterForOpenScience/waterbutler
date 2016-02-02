import os

from invoke import task, run

WHEELHOUSE_PATH = os.environ.get('WHEELHOUSE')


@task
def wheelhouse(develop=False):
    req_file = 'dev-requirements.txt' if develop else 'requirements.txt'
    cmd = 'pip wheel --find-links={} -r {} --wheel-dir={}'.format(WHEELHOUSE_PATH, req_file, WHEELHOUSE_PATH)
    run(cmd, pty=True)


@task
def install(develop=False):
    run('python setup.py develop')
    req_file = 'dev-requirements.txt' if develop else 'requirements.txt'
    cmd = 'pip install --upgrade -r {}'.format(req_file)

    if WHEELHOUSE_PATH:
        cmd += ' --no-index --find-links={}'.format(WHEELHOUSE_PATH)
    run(cmd, pty=True)


@task
def flake():
    run('flake8 .', pty=True)


@task
def test(verbose=False):
    flake()
    cmd = 'py.test --cov-report term-missing --cov waterbutler tests'
    if verbose:
        cmd += ' -v'
    run(cmd, pty=True)


@task
def celery(loglevel='INFO', hostname='%h'):
    from waterbutler.tasks.app import app
    command = ['worker']
    if loglevel:
        command.extend(['--loglevel', loglevel])
    if hostname:
        command.extend(['--hostname', hostname])
    app.worker_main(command)


@task
def rabbitmq():
    run('rabbitmq-server', pty=True)


@task
def server():
    from waterbutler.server.app import serve
    serve()


@task
def clean(verbose=False):
    cmd = 'find . -name "*.pyc" -delete'
    if verbose:
        print(cmd)
    run(cmd, pty=True)
