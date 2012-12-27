import os
import sys

from fabric.api import env, run, cd


DEPLOYMENTS = {
    'prod': {
        'home':         '/var/www/',
        'host_string':  'bamboo@bamboo.io',
        'virtual_env':  'bamboo',
        'repo_name':    'current',
        'project':      'bamboo',
        'docs':         'docs',
        'branch':       'master',
        'key_filename': os.path.expanduser('~/.ssh/modilabs.pem'),
        'init_script':  'bamboo_uwsgi.sh',
        'celeryd':      'celeryd',
    }
}


def _run_in_virtualenv(command):
    run('source ~/.virtualenvs/%s/bin/activate && %s' % (env.virtual_env,
                                                         command))


def _check_key_filename(deployment_name):
    if 'key_filename' in DEPLOYMENTS[deployment_name] and \
            not os.path.exists(DEPLOYMENTS[deployment_name]['key_filename']):
        print 'Cannot find required permissions file: %s' % \
            DEPLOYMENTS[deployment_name]['key_filename']
        return False
    return True


def _setup_env(deployment_name):
    env.update(DEPLOYMENTS[deployment_name])
    if not _check_key_filename(deployment_name):
        sys.exit(1)
    env.project_directory = os.path.join(env.home, env.project)
    env.code_src = os.path.join(env.project_directory, env.repo_name)
    env.doc_src = os.path.join(env.project_directory, env.repo_name, env.docs)
    env.pip_requirements_file = os.path.join(env.code_src, 'requirements.pip')


def deploy(deployment_name):
    _setup_env(deployment_name)

    # update code
    with cd(env.code_src):
        run('git pull origin %(branch)s' % env)
        run('find . -name "*.pyc" -delete')

    # update docs
    with cd(env.doc_src):
        _run_in_virtualenv('make html')

    # install dependencies
    _run_in_virtualenv('pip install -r %s' % env.pip_requirements_file)

    # restart celery
    with cd(env.code_src):
        _run_in_virtualenv('../shared/%s restart' % env.celeryd)

    # restart the server
    with cd(env.code_src):
        _run_in_virtualenv('./scripts/%s restart' % env.init_script)
