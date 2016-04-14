from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import Bigtop, get_layer_opts
from charmhelpers.core import hookenv
import subprocess


@when('puppet.available')
@when_not('namenode.installed')
def install_hadoop():
    hookenv.status_set('maintenance', 'installing namenode')
    bigtop = Bigtop()
    bigtop.install()
    set_state('namenode.installed')
    hookenv.status_set('maintenance', 'namenode installed')


@when('namenode.installed')
@when_not('namenode.started')
def start_namenode():
    hookenv.status_set('maintenance', 'starting namenode')
    for port in get_layer_opts().exposed_ports('namenode'):
        hookenv.open_port(port)
    set_state('namenode.started')
    hookenv.status_set('active', 'ready')


@when('namenode.started', 'datanode.joined')
def send_info(datanode):
    '''Send datanodes our FQDN so they can install as slaves.'''
    hostname = subprocess.check_output(['hostname', '-f']).strip().decode()
    datanode.send_namenodes([hostname])

    slaves = [node['host'] for node in datanode.nodes()]
    hookenv.status_set('active', 'ready ({count} dataNode{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
