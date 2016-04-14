from charms.reactive import when
from charms.reactive import when_not
from charms.reactive import set_state
from charms.layer.apache_bigtop_base import get_bigtop_base
from jujubigdata import utils
from charmhelpers.core import hookenv


@when_not('namenode.installed')
def install_hadoop():
    bigtop = get_bigtop_base()
    bigtop.install()
    set_state('namenode.installed')

@when('namenode.installed')
@when_not('namenode.started')
def configure_namenode():
    bigtop = get_bigtop_base()

    # use layer options someday, for now, hard code ports
    hookenv.open_port('8020')
    hookenv.open_port('50070')

    set_state('namenode.started')


# TODO the following reactive condition should be removed as NN install
## doesn't expect a presence of the DNs
# @when('namenode.started')
# @when_not('datanode.joined')
# def blocked():
#     hookenv.status_set('blocked', 'Waiting for relation to DataNodes')


@when('namenode.started', 'datanode.joined')
def send_info(datanode):
    hadoop = get_bigtop_base()
    # hdfs = HDFS(hadoop)
    # local_hostname = hookenv.local_unit().replace('/', '-')
    # hdfs_port = hadoop.dist_config.port('namenode')
    # webhdfs_port = hadoop.dist_config.port('nn_webapp_http')

    utils.update_kv_hosts({node['ip']: node['host']
                           for node in datanode.nodes()})
    utils.manage_etc_hosts()

    # datanode.send_spec(hadoop.spec())
    # datanode.send_namenodes([local_hostname])
    # datanode.send_ports(hdfs_port, webhdfs_port)
    # datanode.send_ssh_key(utils.get_ssh_key('hdfs'))
    datanode.send_hosts_map(utils.get_kv_hosts())

    # slaves = [node['host'] for node in datanode.nodes()]
    # if data_changed('namenode.slaves', slaves):
    #     unitdata.kv().set('namenode.slaves', slaves)
    #     hdfs.register_slaves(slaves)

    # hookenv.status_set('active', 'Ready ({count} DataNode{s})'.format(
    #     count=len(slaves),
    #     s='s' if len(slaves) > 1 else '',
    # ))
    set_state('namenode.ready')
    hookenv.status_set('active', 'ready')

# TODO a client should be unblocked once the NN && DNs are ready. I.g. when the whole HDFS as the service
## is up and running
@when('namenode.clients')
@when_not('namenode.ready')
def reject_clients(clients):
    clients.send_ready(False)


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
