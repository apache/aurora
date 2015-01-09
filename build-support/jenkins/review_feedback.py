#!/usr/bin/env python2.7
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Runs a CI script when new diffs are posted on Review Board.
# TODO(wfarner): Also add support for pinging stale reviews.

from __future__ import print_function

import argparse
import base64
import json
import subprocess
import sys
import urllib
import urllib2

class ReviewBoard(object):
  def __init__(self, host, user, password):
    self._host = host
    self.user = user
    self._password = password

  def api_url(self, api_path):
    return 'https://%s/api/%s/' % (self._host, api_path)

  def get_resource_data(self, href, args=None, accept='application/json', data=None):
    href = '%s?%s' % (href, urllib.urlencode(args)) if args else href
    print('Request: %s' % href)
    request = urllib2.Request(href)
    base64string = base64.encodestring('%s:%s' % (self.user, self._password)).replace('\n', '')
    request.add_header('Authorization', 'Basic %s' % base64string)
    request.add_header('Accept', accept)
    result = urllib2.urlopen(request, data=data)
    if result.getcode() / 100 != 2:
      print('Non-ok response: %s\n%s' % (result.getcode(), result))
      sys.exit(1)
    return result.read()

  def get_resource(self, href, args=None, data=None):
    return json.loads(self.get_resource_data(href, args=args, data=data))


# Thrown when the patch from a review diff could not be applied.
class PatchApplyError(Exception):
  pass


def _apply_patch(patch_data, clean_excludes):
  subprocess.check_call(['git', 'clean', '-fdx'] + ['--exclude=%s' % e for e in clean_excludes])
  subprocess.check_call(['git', 'reset', '--hard', 'origin/master'])

  patch_file = 'diff.patch'
  with open(patch_file, 'w') as f:
    f.write(patch_data)
  try:
    subprocess.check_call(['git', 'apply', patch_file])
  except subprocess.CalledProcessError:
    raise PatchApplyError()


def _get_latest_diff_time(server, request):
  diffs = server.get_resource(request['links']['diffs']['href'])['diffs']
  return diffs[-1]['timestamp']


REPLY_REQUEST = '@ReviewBot retry'


def _get_latest_user_request(reviews):
  reply_requests = [r for r in reviews if REPLY_REQUEST.lower() in r['body_top'].lower()]
  if reply_requests:
    return reply_requests[-1]['timestamp']


def _needs_reply(server, request):
  print('Inspecting review %d: %s' % (request['id'], request['summary']))
  reviews = server.get_resource(request['links']['reviews']['href'])['reviews']
  feedback_reviews = [r for r in reviews if r['links']['user']['title'] == server.user]
  if feedback_reviews:
    # Determine whether another round of feedback is necessary.
    latest_feedback_time = feedback_reviews[-1]['timestamp']
    latest_request = _get_latest_user_request(reviews)
    latest_diff = _get_latest_diff_time(server, request)
    return ((latest_request and (latest_request > latest_feedback_time))
            or (latest_diff and (latest_diff > latest_feedback_time)))
  return True


def _missing_tests(server, diff):
  # Get files that were modified by the change, flag if test coverage appears lacking.
  diff_files = server.get_resource(diff['links']['files']['href'])['files']
  paths = [f['source_file'] for f in diff_files]
  return (filter(lambda f: f.startswith('src/main/'), paths) and not
          filter(lambda f: f.startswith('src/test/'), paths))


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--server', dest='server', help='Review Board server.', required=True)
  parser.add_argument(
      '--reviewboard-credentials-file',
      type=argparse.FileType(),
      help='Review Board credentials file, formatted as <user>\\n<password>',
      required=True)
  parser.add_argument(
      '--repository',
      help='Inspect reviews posted for this repository.',
      required=True)
  parser.add_argument(
      '--command',
      help='Build verification command.',
      required=True)
  parser.add_argument(
      '--tail-lines',
      type=int,
      default=20,
      help='Number of lines of command output to include in red build reviews.',
      required=True)
  parser.add_argument(
    '--git-clean-exclude',
    help='Patterns to pass to git-clean --exclude.',
    nargs='*')
  args = parser.parse_args()

  credentials = args.reviewboard_credentials_file.readlines()
  server = ReviewBoard(
      host=args.server,
      user=credentials[0].strip(),
      password=credentials[1].strip())

  # Find the numeric ID for the repository, required by other API calls.
  repositories = server.get_resource(
      server.api_url('repositories'),
      args={'name': args.repository})['repositories']
  if not repositories:
    print('Failed to find repository %s' % args.repository)
    sys.exit(1)
  repository_id = repositories[0]['id']

  # Fetch all in-flight reviews. (Note: this does not do pagination, required when > 200 results.)
  requests = server.get_resource(server.api_url('review-requests'), args={
    'repository': repository_id,
    'status': 'pending'
  })['review_requests']
  print('Found %d review requests to inspect' % len(requests))
  for request in requests:
    if not _needs_reply(server, request):
      continue

    diffs = server.get_resource(request['links']['diffs']['href'])['diffs']
    if not diffs:
      continue

    latest_diff = diffs[-1]
    print('Applying diff %d' % latest_diff['id'])
    patch_data = server.get_resource_data(
        latest_diff['links']['self']['href'],
        accept='text/x-patch')
    sha = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip()
    ship = False
    try:
      _apply_patch(patch_data, args.git_clean_exclude)
      print('Running build command.')
      build_output = 'build_output'
      command = args.command
      # Pipe to a file in case output is large, also tee the output to simplify
      # debugging.  Since we pipe the output, we must set pipefail to ensure
      # a failing build command fails the bash pipeline.
      result = subprocess.call([
          'bash',
          '-c',
          'set -o pipefail; %s 2>&1 | tee %s' % (command, build_output)])
      if result == 0:
        review_text = 'Master (%s) is green with this patch.\n  %s' % (sha, command)
        if _missing_tests(server, latest_diff):
          review_text = '%s\n\nHowever, it appears that it might lack test coverage.' % review_text
        else:
          ship = True
      else:
        build_tail = subprocess.check_output(['tail', '-n', str(args.tail_lines), build_output])
        review_text = (
            'Master (%s) is red with this patch.\n  %s\n\n%s' % (sha, command, build_tail))
    except PatchApplyError:
      review_text = (
          'This patch does not apply cleanly on master (%s), do you need to rebase?' % sha)

    review_text = ('%s\n\nI will refresh this build result if you post a review containing "%s"'
                   % (review_text, REPLY_REQUEST))
    print('Replying to review %d:\n%s' % (request['id'], review_text))
    print(server.get_resource(
        request['links']['reviews']['href'],
        data=urllib.urlencode({
            'body_top': review_text,
            'public': 'true',
            'ship_it': 'true' if ship else 'false'
        })))


if __name__=="__main__":
  main()
