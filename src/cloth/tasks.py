#! /usr/bin/env python

import pprint

from collections import defaultdict

import fabric.api
from fabric.api import run, env, sudo, task, runs_once, roles, parallel

from cloth.utils import instances, use, unuse

env.nodes = []
env.roledefs = defaultdict(list)

# setup global results
env['results'] = {}

@task
def all():
    "All nodes"
    for node in instances():
        use(node)

def clean_results(results,group,count,prettyPrint=True):
  """ Cleanup and print results from a @runs_once exec. """
  pp = pprint.PrettyPrinter(indent=2)
  cleaned_results = {}
  failed_hosts = []
  for k,v in results.items():
    if not v:
      failed_hosts.append(k)
    else:
      for kk,vv in v.items():
        cleaned_results[kk] = vv
  if group:
    cleaned_results = group_by_values(cleaned_results)
  elif count:
    cleaned_results = count_by_values(cleaned_results)
  else:
    cleaned_results = cleaned_results
  if prettyPrint:
    if len(failed_hosts) > 0:
      print "Failed Hosts:"
      pp.pprint(failed_hosts)
      print "Results:"
    pp.pprint(cleaned_results)

def count_by_values(dictionary):
    """ Return a dictionary of values, a count of matches."""
    results = {}
    for k,v in dictionary.items():
        if v not in results:
            results[v] = 0
        results[v] += 1
    return results

@parallel
def do_cmd(c):
    hostname = run('hostname',quiet=True)
    result = run('{0}'.format(c),quiet=True)
    return {
        hostname: result
    }

@task
def exclude(exp):
    "Exclude nodes based on a regular expression"
    for node in instances(exp,env.nodes):
        unuse(node)

def group_by_values(dictionary):
    """ Return a dictionary of values, with keys that match them."""
    results = {}
    for k,v in dictionary.items():
        if v not in results:
            results[v] = []
        results[v].append(k)
    return results

@task
@runs_once
def list():
    "List EC2 name and public and private ip address"
    for node in env.nodes:
        print "%s (%s, %s)" % (node.tags["Name"], node.ip_address,
            node.private_ip_address)

@task
def nodes(exp):
    "Select nodes based on a regular expression"
    for node in instances(exp):
        use(node)

@task
@runs_once
def execute(c,group=False,count=False):
    """
    Executes a shell command on a remote host.

        c = command to execute.
        group = if true, group by the result value.
        count = if true, group by result value, and count.

    """
    results = fabric.api.execute(do_cmd,c=c)
    clean_results(results,group,count)
