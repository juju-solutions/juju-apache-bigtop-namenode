from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv
from jujubigdata import utils
import subprocess


@when('puppet.available')
@when_not('namenode.installed')
def install_hadoop():
    hookenv.status_set('maintenance', 'installing namenode')
    bigtop = get_bigtop_base()
    # nn_host = utils.resolve_private_address(hookenv.unit_private_ip())
    nn_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    hosts = {'namenode': nn_host}
    bigtop.install(hosts=hosts, roles='namenode')
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
    # nn_host = utils.resolve_private_address(hookenv.unit_private_ip())
    nn_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    datanode.send_namenodes([nn_host])


@when('namenode.clients')
def send_fqdn_to_client(client):
    '''Send clients our FQDN so they can install as slaves.'''
    # nn_host = utils.resolve_private_address(hookenv.unit_private_ip())
    nn_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    client.send_namenodes([nn_host])


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
