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
    self._base_url = 'https://%s' % host if not host.startswith('http') else host
    self.user = user
    self._password = password

  def api_url(self, api_path):
    return '%s/api/%s/' % (self._base_url, api_path)

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


class RequestProcessor(object):
  def __init__(self, server, command, git_clean_excludes=None, tail_lines=20):
    self._server = server
    self._command = command
    self._git_clean_excludes = git_clean_excludes or []
    self._tail_lines = tail_lines

  class PatchApplyError(Exception):
    """Indicates the patch from a review diff could not be applied."""

  def _apply_patch(self, patch_data, basis=None, reset=True):
    if reset:
      subprocess.check_call(
          ['git', 'clean', '-fdx'] + ['--exclude=%s' % e for e in self._git_clean_excludes])
      subprocess.check_call(['git', 'reset', '--hard', 'origin/master'])

    sha = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).strip()
    patch_file = 'diff.patch'
    with open(patch_file, 'w') as f:
      f.write(patch_data)
    try:
      subprocess.check_call(['git', 'apply', '-3', patch_file])
      return sha
    except subprocess.CalledProcessError:
      raise self.PatchApplyError(
          'This patch does not apply cleanly against %s (%s), do you need to rebase?'
          % (basis or 'master', sha))

  def _get_latest_diff_time(self, review_request):
    diffs = self._server.get_resource(review_request['links']['diffs']['href'])['diffs']
    return diffs[-1]['timestamp']

  REPLY_REQUEST = '@ReviewBot retry'

  def _get_latest_user_request(self, reviews):
    reply_requests = [r for r in reviews if self.REPLY_REQUEST.lower() in r['body_top'].lower()]
    if reply_requests:
      return reply_requests[-1]['timestamp']

  def _needs_reply(self, review_request):
    print('Inspecting review %d: %s' % (review_request['id'], review_request['summary']))
    reviews_response = self._server.get_resource(review_request['links']['reviews']['href'])
    reviews = reviews_response['reviews']
    # The default response limit is 25.  When the responses are limited, a 'next' link will be
    # included.  When that happens, continue to walk until there are no reviews left.
    while 'next' in reviews_response['links']:
      print('Fetching next page of reviews.')
      reviews_response = self._server.get_resource(reviews_response['links']['next']['href'])
      reviews.extend(reviews_response['reviews'])
    feedback_reviews = [r for r in reviews if r['links']['user']['title'] == self._server.user]
    if feedback_reviews:
      # Determine whether another round of feedback is necessary.
      latest_feedback_time = feedback_reviews[-1]['timestamp']
      latest_request = self._get_latest_user_request(reviews)
      latest_diff = self._get_latest_diff_time(review_request)
      print('Latest feedback was given at        %s' % latest_feedback_time)
      print('Latest build request from a user at %s' % latest_request)
      print('Latest diff was posted at           %s' % latest_diff)
      return ((latest_request and (latest_request > latest_feedback_time)) or
              (latest_diff and (latest_diff > latest_feedback_time)))
    return True

  def _missing_tests(self, diff):
    # Get files that were modified by the change, flag if test coverage appears lacking.
    diff_files = self._server.get_resource(diff['links']['files']['href'])['files']
    paths = [f['source_file'] for f in diff_files]
    return (filter(lambda p: p.startswith('src/main/'), paths) and not
            filter(lambda p: p.startswith('src/test/'), paths))

  def _patch(self, review_request, basis=None, reset=True):
    diffs = self._server.get_resource(review_request['links']['diffs']['href'])['diffs']
    if not diffs:
      return None

    dependencies = review_request['depends_on']
    if len(dependencies) > 1:
      raise self.PatchApplyError(
          'Can only test reviews with 1 parent, found %d:\n\t%s' %
          (len(dependencies), '\n\t'.join(d['href'] for d in dependencies)))
    elif len(dependencies) == 1:
      parent_request = self._server.get_resource(dependencies[0]['href'])['review_request']
      basis = 'RB#%d' % parent_request['id']
      parent_status = parent_request['status']
      if parent_status == 'submitted':
        print('Skipping application of %s diffs - dependency already submitted to master.' % basis)
      elif parent_status == 'pending':
        self._patch(parent_request, basis=basis, reset=reset)
        print('Applied patch for %s dependency.' % basis)
        reset = False
      elif parent_status == 'discarded':
        raise self.PatchApplyError(
            "RB#%d depends on discarded %s. Please adjust the 'depends-on' field and rebase "
            "as-needed." % (review_request['id'], basis))
      else:
        print('WARNING: unexpected RB status %s for %s' % (parent_status, basis))

    latest_diff = diffs[-1]
    print('Applying diff %d' % latest_diff['id'])
    patch_data = self._server.get_resource_data(
        latest_diff['links']['self']['href'],
        accept='text/x-patch')
    sha = self._apply_patch(patch_data, basis=basis, reset=reset)
    return sha, latest_diff

  def process(self, review_request):
    if not self._needs_reply(review_request):
      return

    ship = False
    try:
      result = self._patch(review_request)
      if not result:
        return
      sha, latest_diff = result

      print('Running build command.')
      build_output = 'build_output'
      # Pipe to a file in case output is large, also tee the output to simplify
      # debugging.  Since we pipe the output, we must set pipefail to ensure
      # a failing build command fails the bash pipeline.
      # AURORA-1961: Convert carriage returns to newlines to prevent shell cmds
      # seing spotbugs output on a single line, which is then translated to
      # multiple lines by Python subprocess.
      result = subprocess.call([
        'bash',
        '-c',
        'set -o pipefail; %s 2>&1 | tr "\r" "\n" | tee %s' % (self._command, build_output)])
      if result == 0:
        review_text = 'Master (%s) is green with this patch.\n  %s' % (sha, self._command)
        if self._missing_tests(latest_diff):
          review_text = '%s\n\nHowever, it appears that it might lack test coverage.' % review_text
        else:
          ship = True
      else:
        build_tail = subprocess.check_output(['tail', '-n', str(self._tail_lines), build_output])
        review_text = (
          'Master (%s) is red with this patch.\n  %s\n\n%s' % (sha, self._command, build_tail))
    except self.PatchApplyError as e:
      review_text = str(e)

    review_text = ('%s\n\nI will refresh this build result if you post a review containing "%s"'
                   % (review_text, self.REPLY_REQUEST))
    print('Replying to review %d:\n%s' % (review_request['id'], review_text))
    print(self._server.get_resource(
        review_request['links']['reviews']['href'],
        data=urllib.urlencode({
          'body_top': review_text,
          'public': 'true',
          'ship_it': 'true' if ship else 'false'
        })))


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
      help='Number of lines of command output to include in red build reviews.')
  parser.add_argument(
      '--git-clean-exclude',
      help='Patterns to pass to git-clean --exclude.',
      nargs='*',
      default=[])
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

  request_processor = RequestProcessor(
      server,
      args.command,
      args.git_clean_exclude,
      args.tail_lines)

  for review_request in requests:
    request_processor.process(review_request)


if __name__ == "__main__":
  main()
