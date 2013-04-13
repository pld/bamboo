from subprocess import check_output

# versioning
VERSION_MAJOR = 0.5
VERSION_MINOR = 9
VERSION_NUMBER = '%.1f.%d' % (VERSION_MAJOR, VERSION_MINOR)
VERSION_DESCRIPTION = 'alpha'


def safe_command_request(args):
    try:
        return check_output(args).strip()
    except:
        # might fail at least if git is not present
        # or if there's no git repository
        return ''


def get_version():
    return {'version': VERSION_NUMBER,
            'version_major': VERSION_MAJOR,
            'version_minor': VERSION_MINOR,
            'description': VERSION_DESCRIPTION,
            'branch': safe_command_request([
                'git', 'rev-parse', '--abbrev-ref', 'HEAD']),
            'commit': safe_command_request(['git', 'rev-parse', 'HEAD'])}
