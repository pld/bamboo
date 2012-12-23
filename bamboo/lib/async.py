import os

ASYNC_FLAG = 'BAMBOO_ASYNC_OFF'


def is_async():
    return not os.getenv(ASYNC_FLAG)


def set_async(on):
    if on:
        if not is_async():
            del os.environ[ASYNC_FLAG]
    else:
        os.environ[ASYNC_FLAG] = 'True'


def call_async(function, *args, **kwargs):
    """Potentially asynchronously call `function` with the arguments.

    :param function: The function to call.
    :param args: Arguments for the function.
    :param kwargs: Keyword arguments for the function.
    """
    countdown = kwargs.pop('countdown', 0)

    if is_async():
        function.__getattribute__('apply_async')(
            countdown=countdown, args=args, kwargs=kwargs)
    else:  # pragma: no cover
        function(*args, **kwargs)
