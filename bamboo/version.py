
from subprocess import check_output

# versioning
VERSION_MAJOR = 0.5
VERSION_MINOR = 6
VERSION_NUMBER = '%.1f.%d' % (VERSION_MAJOR, VERSION_MINOR)
VERSION_DESCRIPTION = 'alpha'


def get_version():
    return {'version': VERSION_NUMBER,
            'version_major': VERSION_MAJOR,
            'version_minor': VERSION_MINOR,
            'description': VERSION_DESCRIPTION,
            'branch': check_output([
                'git', 'rev-parse', '--abbrev-ref', 'HEAD']).strip(),
            'commit': check_output(['git', 'rev-parse', 'HEAD']).strip()}
