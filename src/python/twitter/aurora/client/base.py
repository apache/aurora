import functools
import sys
from urlparse import urljoin

from twitter.common import app, log

from gen.twitter.aurora.ttypes import ResponseCode


def die(msg):
  log.fatal(msg)
  sys.exit(1)


def check_and_log_response(resp):
  log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
  if resp.responseCode != ResponseCode.OK:
    sys.exit(1)


def deprecation_warning(text):
  log.warning('')
  log.warning('*' * 80)
  log.warning('* The command you ran is deprecated and will soon break!')
  for line in text.split('\n'):
    log.warning('* %s' % line)
  log.warning('*' * 80)
  log.warning('')


class requires(object):
  @staticmethod
  def wrap_function(fn, fnargs, comparator):
    @functools.wraps(fn)
    def wrapped_function(args):
      if not comparator(args, fnargs):
        help = 'Incorrect parameters for %s' % fn.__name__
        if fn.__doc__:
          help = '%s\n\nsee the help subcommand for more details.' % fn.__doc__.split('\n')[0]
        die(help)
      return fn(*args)
    return wrapped_function

  @staticmethod
  def exactly(*args):
    def wrap(fn):
      return requires.wrap_function(fn, args, (lambda want, got: len(want) == len(got)))
    return wrap

  @staticmethod
  def at_least(*args):
    def wrap(fn):
      return requires.wrap_function(fn, args, (lambda want, got: len(want) >= len(got)))
    return wrap

  @staticmethod
  def nothing(fn):
    @functools.wraps(fn)
    def real_fn(line):
      return fn(*line)
    return real_fn


def synthesize_url(scheduler_url, role=None, env=None, job=None):
  if not scheduler_url:
    log.warning("Unable to find scheduler web UI!")
    return None

  if env and not role:
    die('If env specified, must specify role')
  if job and not (role and env):
    die('If job specified, must specify role and env')

  scheduler_url = urljoin(scheduler_url, 'scheduler')
  if role:
    scheduler_url += '/' + role
    if env:
      scheduler_url += '/' + env
      if job:
        scheduler_url += '/' + job
  return scheduler_url


def handle_open(scheduler_url, role, env, job):
  url = synthesize_url(scheduler_url, role, env, job)
  if url:
    log.info('Job url: %s' % url)
    if app.get_options().open_browser:
      import webbrowser
      webbrowser.open_new_tab(url)
