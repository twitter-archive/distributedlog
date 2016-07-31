#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Utility for creating well-formed pull request merges and pushing
# them to Apache. This script is a modified version of the one created
# by the Spark project
# (https://github.com/apache/spark/blob/master/dev/merge_spark_pr.py).
#
# Usage: ./dl-merge-pr.py (see config env vars below)
#
# This utility assumes you already have local a distributedlog git folder and that you
# have added remotes corresponding to both:
# (i) the github apache distributedlog mirror and
# (ii) the apache distributedlog git repo.

import json
import os
import re
import subprocess
import sys
import urllib2

try:
  import jira.client
  JIRA_IMPORTED = True
except ImportError:
  JIRA_IMPORTED = False

PROJECT_NAME = 'incubator-distributedlog'

CAPITALIZED_PROJECT_NAME = 'distributedlog'.upper()

# Location of the local git repository
REPO_HOME = os.environ.get('{0}_HOME'.format(CAPITALIZED_PROJECT_NAME), os.getcwd())

# Remote name which points to the GitHub site
PR_REMOTE_NAME = os.environ.get('PR_REMOTE_NAME', 'apache-github')

# Remote name which points to Apache git
PUSH_REMOTE_NAME = os.environ.get('PUSH_REMOTE_NAME', 'apache')

# ASF JIRA username
JIRA_USERNAME = os.environ.get('JIRA_USERNAME', '')

# ASF JIRA password
JIRA_PASSWORD = os.environ.get('JIRA_PASSWORD', '')

# OAuth key used for issuing requests against the GitHub API. If this
# is not defined, then requests will be unauthenticated. You should
# only need to configure this if you find yourself regularly exceeding
# your IP's unauthenticated request rate limit. You can create an
# OAuth key at https://github.com/settings/tokens. This script only
# requires the "public_repo" scope.
GITHUB_OAUTH_KEY = os.environ.get('GITHUB_OAUTH_KEY')

GITHUB_USER = os.environ.get('GITHUB_USER', 'apache')
GITHUB_BASE = 'https://github.com/{0}/{1}/pull'.format(GITHUB_USER, PROJECT_NAME)
GITHUB_PR_REMOTE = 'https://github.com/{0}/{1}.git'.format(GITHUB_USER, PROJECT_NAME)
GITHUB_API_URL  = 'https://api.github.com'
GITHUB_API_BASE = '{0}/repos/{1}/{2}'.format(GITHUB_API_URL, GITHUB_USER, PROJECT_NAME)
JIRA_BASE = 'https://issues.apache.org/jira/browse'
JIRA_API_BASE = 'https://issues.apache.org/jira'

PUSH_PR_REMOTE = 'git://git.apache.org/{0}.git'.format(PROJECT_NAME)

# Prefix added to temporary branches
TEMP_BRANCH_PREFIX = 'PR_TOOL'
RELEASE_BRANCH_PREFIX = ''

DEV_BRANCH_NAME = 'master'

DEFAULT_FIX_VERSION = os.environ.get('DEFAULT_FIX_VERSION', '0.9.1.0')

def get_json(url):
  """
  Returns parsed JSON from an API call to the GitHub API.
  """
  try:
    request = urllib2.Request(url)
    if GITHUB_OAUTH_KEY:
      request.add_header('Authorization', 'token {0}'.format(GITHUB_OAUTH_KEY))
    return json.load(urllib2.urlopen(request))
  except urllib2.HTTPError as e:
    if 'X-RateLimit-Remaining' in e.headers and e.headers['X-RateLimit-Remaining'] == '0':
      print('Exceeded the GitHub API rate limit; see the instructions in ',
        'dl-merge-pr.py to configure an OAuth token for making authenticated ',
        'GitHub requests.')
    else:
      print('Unable to fetch URL, exiting: {0}'.format(url))
      sys.exit(-1)


def fail(msg):
  """
  Prints a message, cleans up and exits.
  """
  print(msg)
  clean_up()
  sys.exit(-1)


def run_cmd(cmd, verbose=False):
  """
  Runs a command.
  """
  if verbose is True:
    print(cmd)
  return subprocess.check_output(cmd)


def continue_maybe(prompt):
  """
  Asks confirmation before continuing to run.
  """
  result = raw_input('\n{0} (y/n): '.format(prompt))
  if result.lower() != 'y':
    fail('Okay, exiting')


def clean_up():
  """
  Cleans up the repository to put it back to it's original state.
  """
  if original_head != get_current_branch():
    print('Restoring head pointer to {0}'.format(original_head))
    run_cmd(['git', 'checkout', original_head])

  branches = run_cmd(['git', 'branch']).rstrip().split('\n')

  for branch in filter(lambda x: x.startswith(TEMP_BRANCH_PREFIX), branches):
    print('Deleting local branch {0}'.format(branch))
    run_cmd(['git', 'branch', '-D', branch])


def get_current_branch():
  """
  Returns the current branch
  """
  return run_cmd(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).rstrip()


def merge_pr(pr_num, target_ref, title, body, default_pr_reviewers, pr_repo_desc):
  """
  Merges the requested PR and return the merge hash.
  """
  pr_branch_name = '{0}_MERGE_PR_{1}'.format(TEMP_BRANCH_PREFIX, pr_num)
  target_branch_name = '{0}_MERGE_PR_{1}_{2}'.format(TEMP_BRANCH_PREFIX, pr_num, target_ref.upper())
  run_cmd(['git', 'fetch', PR_REMOTE_NAME, 'pull/{0}/head:{1}'.format(pr_num, pr_branch_name)])
  run_cmd(['git', 'fetch',PUSH_PR_REMOTE, '{0}:{1}'.format(target_ref, target_branch_name)])
  run_cmd(['git', 'checkout', target_branch_name])

  had_conflicts = False
  try:
    run_cmd(['git', 'merge', pr_branch_name, '--squash'])
  except Exception as e:
    msg = 'Error merging: {0}\nWould you like to manually fix-up this merge?'.format(e)
    continue_maybe(msg)
    msg = 'Okay, please fix any conflicts and \'git add\' conflicting files... Finished?'
    continue_maybe(msg)
    had_conflicts = True

  # Offer to run unit tests before committing
  result = raw_input('Do you want to validate unit tests after the merge? (y/n): ')
  if result.lower() == 'y':
    test_res = subprocess.call('mvn clean install'.split())
    if test_res == 0:
      print('Unit tests execution succeeded')
    else:
      continue_maybe('Unit tests execution FAILED. Do you want to continue with the merge anyway?')

  commit_authors = run_cmd(['git', 'log', 'HEAD..{0}'.format(pr_branch_name), '--pretty=format:%an <%ae>']).split("\n")
  distinct_authors = sorted(set(commit_authors), key=lambda x: commit_authors.count(x), reverse=True)
  primary_author = raw_input('Enter primary author in the format of \'name <email>\' [{0}]: '.format(distinct_authors[0]))
  if primary_author == '':
    primary_author = distinct_authors[0]

  reviewers = raw_input('Enter reviewers [{0}]: '.format(default_pr_reviewers)).strip()
  if reviewers == '':
    reviewers = default_pr_reviewers

  commits = run_cmd(['git', 'log', 'HEAD..{0}'.format(pr_branch_name), '--pretty=format:%h [%an] %s']).split('\n')

  if len(commits) > 1:
    result = raw_input('List pull request commits in squashed commit message? (y/n): ')
    if result.lower() == 'y':
      should_list_commits = True
    else:
      should_list_commits = False
  else:
    should_list_commits = False

  merge_message_flags = []

  merge_message_flags += ['-m', title]
  if body is not None:
    # We remove @ symbols from the body to avoid triggering e-mails
    # to people every time someone creates a public fork of the project.
    merge_message_flags += ['-m', body.replace('@', '')]

  authors = '\n'.join(['Author: {0}'.format(a) for a in distinct_authors])

  merge_message_flags += ['-m', authors]

  if (reviewers != ''):
    merge_message_flags += ['-m', 'Reviewers: {0}'.format(reviewers)]

  if had_conflicts:
    committer_name = run_cmd(['git', 'config', '--get', 'user.name']).strip()
    committer_email = run_cmd(['git', 'config', '--get', 'user.email']).strip()
    message = 'This patch had conflicts when merged, resolved by\nCommitter: {0} <{1}>'.format(committer_name, committer_email)
    merge_message_flags += ['-m', message]

  # The string "Closes #%s" string is required for GitHub to correctly close the PR
  close_line = 'Closes #{0} from {1}'.format(pr_num, pr_repo_desc)
  if should_list_commits:
    close_line += ' and squashes the following commits:'
    merge_message_flags += ['-m', close_line]

  if should_list_commits:
    merge_message_flags += ['-m', '\n'.join(commits)]

  run_cmd(['git', 'commit', '--author="%s"' % primary_author] + merge_message_flags)

  continue_maybe('Merge complete (local ref {0}). Push to {1}?'.format(target_branch_name, PUSH_REMOTE_NAME))

  try:
    run_cmd(['git', 'push', PUSH_REMOTE_NAME, target_branch_name, target_ref])
  except Exception as e:
    clean_up()
    fail('Exception while pushing: {0}'.format(e))

  merge_hash = run_cmd(['git', 'rev-parse', target_branch_name])[:8]
  merge_log = run_cmd(['git', 'show', '--format=fuller', '-q', target_branch_name])
  clean_up()
  print('Pull request #{0} merged!'.format(pr_num))
  print('Merge hash: {0}'.format(merge_hash))
  return merge_hash, merge_log


def cherry_pick(pr_num, merge_hash, default_branch):
  pick_ref = raw_input('Enter a branch name [{0}]: '.format(default_branch))
  if pick_ref == '':
    pick_ref = default_branch

  pick_branch_name = '{0}_PICK_PR_{1}_{2}'.format(TEMP_BRANCH_PREFIX, pr_num, pick_ref.upper())

  run_cmd(['git', 'fetch', PUSH_REMOTE_NAME, '{0}:{1}'.format(pick_ref, pick_branch_name)])
  run_cmd(['git', 'checkout', pick_branch_name])

  try:
    run_cmd(['git', 'cherry-pick', '-sx', merge_hash])
  except Exception as e:
    msg = 'Error cherry-picking: {0}\nWould you like to manually fix-up this merge?'.format(e)
    continue_maybe(msg)
    msg = 'Okay, please fix any conflicts and finish the cherry-pick. Finished?'
    continue_maybe(msg)

  continue_maybe('Pick complete (local ref {0}). Push to {1}?'.format(pick_branch_name, PUSH_REMOTE_NAME))

  try:
    run_cmd(['git', 'push', PUSH_REMOTE_NAME, '{0}:{1}'.format(pick_branch_name, pick_ref)])
  except Exception as e:
    clean_up()
    fail('Exception while pushing: {0}'.format(e))

  pick_hash = run_cmd(['git', 'rev-parse', pick_branch_name])[:8]
  clean_up()

  print('Pull request #{0} picked into {1}!'.format(pr_num, pick_ref))
  print('Pick hash: {0}'.format(pick_hash))
  return pick_ref


def fix_version_from_branch(branch, versions):
  # Note: Assumes this is a sorted (newest->oldest) list of un-released versions
  if branch == DEV_BRANCH_NAME:
    versions = filter(lambda x: x == DEFAULT_FIX_VERSION, versions)
    if len(versions) > 0:
      return versions[0]
    else:
      return None
  else:
    versions = filter(lambda x: x.startswith(branch), versions)
    if len(versions) > 0:
      return versions[-1]
    else:
      return None


def resolve_jira_issue(merge_branches, comment, jira_id):
  """
  Resolves the JIRA issue associated with the pull request.
  """
  asf_jira = jira.client.JIRA({'server': JIRA_API_BASE},
                              basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

  result = raw_input('Resolve JIRA {0} ? (y/n): '.format(jira_id))
  if result.lower() != 'y':
    return

  try:
    issue = asf_jira.issue(jira_id)
  except Exception as e:
    fail('ASF JIRA could not find {0}\n{1}'.format(jira_id, e))

  cur_status = issue.fields.status.name
  cur_summary = issue.fields.summary
  cur_assignee = issue.fields.assignee
  if cur_assignee is None:
    cur_assignee = 'NOT ASSIGNED!!!'
  else:
    cur_assignee = cur_assignee.displayName

  if cur_status == 'Resolved' or cur_status == 'Closed':
    fail('JIRA issue {0} already has status \'{1}\''.format(jira_id, cur_status))
    print ('=== JIRA {0} ==='.format(jira_id))
    print ('summary\t\t{0}\nassignee\t{1}\nstatus\t\t{2}\nurl\t\t{3}/{4}\n'.format(
      cur_summary, cur_assignee, cur_status, JIRA_BASE, jira_id))

  versions = asf_jira.project_versions(CAPITALIZED_PROJECT_NAME)
  versions = sorted(versions, key=lambda x: x.name, reverse=True)
  versions = filter(lambda x: x.raw['released'] is False, versions)

  version_names = map(lambda x: x.name, versions)
  default_fix_versions = map(lambda x: fix_version_from_branch(x, version_names), merge_branches)
  default_fix_versions = filter(lambda x: x != None, default_fix_versions)
  default_fix_versions = ','.join(default_fix_versions)

  fix_versions = raw_input('Enter comma-separated fix version(s) [{0}]: '.format(default_fix_versions))
  if fix_versions == '':
    fix_versions = default_fix_versions
    fix_versions = fix_versions.replace(' ', '').split(',')

  def get_version_json(version_str):
    return filter(lambda v: v.name == version_str, versions)[0].raw

  jira_fix_versions = map(lambda v: get_version_json(v), fix_versions)

  resolve = filter(lambda a: a['name'] == 'Resolve Issue', asf_jira.transitions(jira_id))[0]
  resolution = filter(lambda r: r.raw['name'] == 'Fixed', asf_jira.resolutions())[0]
  asf_jira.transition_issue(
    jira_id, resolve['id'], fixVersions = jira_fix_versions,
    comment = comment, resolution = {'id': resolution.raw['id']})

  print 'Successfully resolved {0} with fixVersions={1}!'.format(jira_id, fix_versions)


def resolve_jira_issues(title, merge_branches, comment):
  """
  Resolves a list of jira issues.
  """
  jira_ids = re.findall('%s-[0-9]{3,6}' % CAPITALIZED_PROJECT_NAME, title)

  if len(jira_ids) == 0:
    print 'No JIRA issue found to update'
  for jira_id in jira_ids:
    resolve_jira_issue(merge_branches, comment, jira_id)


def standardize_jira_ref(text):
  """
  Standardize the jira reference commit message prefix to "PROJECT_NAME-XXX: Issue"

  >>> standardize_jira_ref("%s-877: Script for generating patch for reviews" % CAPITALIZED_PROJECT_NAME)
  'DISTRIBUTEDLOG-877: Script for generating patch for reviews'
  """
  jira_refs = []
  components = []

  # Extract JIRA ref(s):
  pattern = re.compile(r'(%s[-\s]*[0-9]{3,6})+' % CAPITALIZED_PROJECT_NAME, re.IGNORECASE)
  for ref in pattern.findall(text):
    # Add brackets, replace spaces with a dash, & convert to uppercase
    jira_refs.append(re.sub(r'\s+', '-', ref.upper()))
    text = text.replace(ref, '')

  # Extract project name component(s):
  # Look for alphanumeric chars, spaces, dashes, periods, and/or commas
  pattern = re.compile(r'(\[[\w\s,-\.]+\])', re.IGNORECASE)
  for component in pattern.findall(text):
    components.append(component.upper())
    text = text.replace(component, '')

  # Cleanup any remaining symbols:
  pattern = re.compile(r'^\W+(.*)', re.IGNORECASE)
  if (pattern.search(text) is not None):
    text = pattern.search(text).groups()[0]

  # Assemble full text (JIRA ref(s), module(s), remaining text)
  jira_prefix = ' '.join(jira_refs).strip()
  if jira_prefix:
    jira_prefix = jira_prefix + ": "

  clean_text = jira_prefix + ' '.join(components).strip() + " " + text.strip()

  # Replace multiple spaces with a single space, e.g. if no jira refs and/or components were included
  clean_text = re.sub(r'\s+', ' ', clean_text.strip())

  return clean_text


def get_reviewers(pr_num):
  """
  Gets a candidate list of reviewers that have commented on the PR with '+1' or 'LGTM'
  """
  approval_msgs = ['+1', 'lgtm']

  pr_comments = get_json('{0}/issues/{1}/comments'.format(GITHUB_API_BASE, pr_num))

  reviewers_ids = set()
  for comment in pr_comments:
    for approval_msg in approval_msgs:
      if approval_msg in comment['body'].lower():
        reviewers_ids.add(comment['user']['login'])

  reviewers_emails = []
  for reviewer_id in reviewers_ids:
    user = get_json('{0}/users/{1}'.format(GITHUB_API_URL, reviewer_id))
    reviewers_emails += ['{0} <{1}>'.format(user['name'].strip(), user['email'].strip())]
  return ', '.join(reviewers_emails)


def check_remote_repos():
  """
  Checks that we have the remote repository configured.
  """
  remotes = run_cmd(['git', 'remote', 'show']).rstrip().split('\n')
  if PR_REMOTE_NAME not in remotes:
    _add_remote_repos(PR_REMOTE_NAME, GITHUB_PR_REMOTE)
  if PUSH_REMOTE_NAME not in remotes:
    _add_remote_repos(PUSH_REMOTE_NAME, PUSH_PR_REMOTE)


def _add_remote_repos(remote_name, remote_url):
  """
  Asks if we want to configure the remote repository.
  """
  result = raw_input('You don\'t have {0} set as a remote, do you want me to set this up for you? [y/n]: '.format(remote_name))
  if result.lower() != 'y':
    print('You need to set {0} before continuing'.format(PR_REMOTE_NAME))
    sys.exit(1)
  else:
    run_cmd(['git', 'remote', 'add', remote_name, remote_url])


def main():
  global original_head

  # we need the remote repository to be set
  check_remote_repos()

  original_head = get_current_branch()

  branches = get_json('{0}/branches'.format(GITHUB_API_BASE))

  branch_names = filter(lambda x: x.startswith(RELEASE_BRANCH_PREFIX), [x['name'] for x in branches])

  if len(branch_names) == 0:
    print('No remote branch.')
    sys.exit(0)

  # Assumes branch names can be sorted lexicographically
  latest_branch = sorted(branch_names, reverse=True)[0]

  pr_num = raw_input('Which pull request would you like to merge? (e.g. 34): ')
  pr = get_json('{0}/pulls/{1}'.format(GITHUB_API_BASE, pr_num))
  pr_events = get_json('{0}/issues/{1}/events'.format(GITHUB_API_BASE, pr_num))
  pr_reviewers = get_reviewers(pr_num)

  url = pr['url']

  pr_title = pr['title']
  commit_title = raw_input('Commit title [{0}]: '.format(pr_title.encode('utf-8')).decode('utf-8'))
  if commit_title == '':
    commit_title = pr_title

  # Decide whether to use the modified title or not
  modified_title = standardize_jira_ref(commit_title)
  if modified_title != commit_title:
    print 'I\'ve re-written the title as follows to match the standard format:'
    print 'Original: {0}'.format(commit_title)
    print 'Modified: {1}'.format(modified_title)
    result = raw_input('Would you like to use the modified title? (y/n): ')
    if result.lower() == 'y':
      commit_title = modified_title
      print 'Using modified title:'
    else:
      print 'Using original title:'
      print commit_title

  body = pr['body']
  target_ref = pr['base']['ref']
  user_login = pr['user']['login']
  base_ref = pr['head']['ref']
  pr_repo_desc = '{0}/{1}'.format(user_login, base_ref)

  # Merged pull requests don't appear as merged in the GitHub API;
  # Instead, they're closed by asfgit.
  merge_commits = [
    e for e in pr_events if e['actor']['login'] == 'asfgit' and e['event'] == 'closed'
  ]

  if merge_commits:
    merge_hash = merge_commits[0]['commit_id']
    message = get_json('{0}/commits/{1}'.format(GITHUB_API_BASE, merge_hash))['commit']['message']

    print 'Pull request {0} has already been merged, assuming you want to backport'.format(pr_num)
    commit_is_downloaded = run_cmd(
      ['git', 'rev-parse', '--quiet', '--verify', '%s^{commit}' % merge_hash]
    ).strip() != ''
    if not commit_is_downloaded:
      fail('Could not find any merge commit for #{0}, you may need to update HEAD.'.format(pr_num))

    print 'Found commit %s:\n%s' % (merge_hash, message)
    cherry_pick(pr_num, merge_hash, latest_branch)
    sys.exit(0)

  if not bool(pr['mergeable']):
    msg = 'Pull request {0} is not mergeable in its current form.\n'.format(pr_num) + \
          'Continue? (experts only!)'
    continue_maybe(msg)

  print ('\n=== Pull Request #{0} ==='.format(pr_num))
  print ('PR title\t{0}\nCommit title\t{1}\nSource\t\t{2}\nTarget\t\t{3}\nURL\t\t{4}'.format(
    pr_title, commit_title, pr_repo_desc, target_ref, url
  ))
  continue_maybe('Proceed with merging pull request #{0}?'.format(pr_num))

  merged_refs = [target_ref]

  merge_hash, merge_commit_log = merge_pr(pr_num, target_ref, commit_title, body, pr_reviewers, pr_repo_desc)

  pick_prompt = 'Would you like to pick %s into another branch?' % merge_hash
  while raw_input('\n%s (y/n): ' % pick_prompt).lower() == 'y':
    merged_refs = merged_refs + [cherry_pick(pr_num, merge_hash, latest_branch)]

  if JIRA_IMPORTED:
    if JIRA_USERNAME and JIRA_PASSWORD:
      jira_comment = '''Issue resolved by merging pull request {0}
      [{1}/{2}]

      {noformat}
      {3}
      {noformat}
      '''.format(pr_num, GITHUB_BASE, pr_num, merge_commit_log)
      resolve_jira_issues(commit_title, merged_refs, jira_comment)
    else:
      print 'JIRA_USERNAME and JIRA_PASSWORD are not set'
      print 'Exiting without trying to close the associated JIRA.'
  else:
    print 'Could not find jira-python library. Run \'sudo pip install jira\' to install.'
    print 'Exiting without trying to close the associated JIRA.'


if __name__ == "__main__":
  import doctest
  (failure_count, test_count) = doctest.testmod()
  if (failure_count):
    exit(-1)

  main()
