from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv
import subprocess


@when_not('namenode.installed')
def install_hadoop():
    hookenv.status_set('maintenance', 'installing namenode')
    bigtop = get_bigtop_base()
    nn_fqdn = subprocess.check_output(['hostname', '-f']).strip().decode()
    bigtop.install(NN=nn_fqdn)
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


@when('datanode.joined')
def send_fqdn_to_dn(datanode):
    '''Send datanodes our FQDN so they can install as slaves.'''
    hostname = subprocess.check_output(['hostname', '-f']).strip().decode()
    datanode.send_namenodes([hostname])

    slaves = [node['host'] for node in datanode.nodes()]
    hookenv.status_set('active', 'ready ({count} dataNode{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))


@when('client.joined')
def send_fqdn_to_client(client):
    '''Send clients our FQDN so they can install as slaves.'''
    hostname = subprocess.check_output(['hostname', '-f']).strip().decode()
    client.send_namenodes([hostname])


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
