#! /usr/bin/env python

import re
import os

import boto.ec2
from fabric.api import env

REGION = os.environ.get("AWS_EC2_REGION")

def ec2_instances():
    "Use the EC2 API to get a list of all machines"
    region = boto.ec2.get_region(REGION)
    reservations = region.connect().get_all_instances()
    instances = []
    for reservation in reservations:
        instances += reservation.instances
    return instances

def ip(node):
    if node.dns_name:
        return node.dns_name
    else:
        return node.private_ip_address

def instances(exp=".*"):
    "Filter list of machines matching an expression"
    expression = re.compile(exp)
    instances = []
    for node in ec2_instances():
        if node.tags and ip(node):
            try:
                if expression.match(node.tags.get("Name")):
                    instances.append(node)
            except TypeError:
                pass
    return instances

def use(node):
    "Set the fabric environment for the specifed node"
    try:
        role = node.tags.get("Name").split('-')[1]
        env.roledefs[role] += [ip(node)]
    except IndexError:
        pass
    env.nodes += [node]
    env.hosts += [ip(node)]

def unuse(node):
    "Remove the fabric environment for the specifed node"
    node_ip = ip(node)
    env.nodes = filter(lambda x: x.dns_name != node.dns_name, env.nodes)
    env.hosts = filter(lambda x: x != node_ip, env.hosts)
