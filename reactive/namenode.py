from charms.reactive import is_state, remove_state, set_state, when, when_not
from charms.layer.apache_bigtop_base import get_bigtop_base, get_layer_opts
from charmhelpers.core import hookenv, host
from jujubigdata import utils
import subprocess


@when('puppet.available')
@when_not('apache-bigtop-namenode.installed')
def install_namenode():
    hookenv.status_set('maintenance', 'installing namenode')
    bigtop = get_bigtop_base()
    nn_host = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    hosts = {'namenode': nn_host}
    bigtop.install(hosts=hosts, roles='namenode')

    # /etc/hosts entries from the KV are not currently used for bigtop,
    # but a hosts_map attribute is required by some interfaces (eg: dfs-slave)
    # to signify NN's readiness. Set our NN info in the KV to fulfill this
    # requirement.
    kv_ip = utils.resolve_private_address(hookenv.unit_private_ip())
    kv_hostname = hookenv.local_unit().replace('/', '-')
    utils.update_kv_host(kv_ip, kv_hostname)

    set_state('apache-bigtop-namenode.installed')
    hookenv.status_set('maintenance', 'namenode installed')


@when('apache-bigtop-namenode.installed')
@when_not('apache-bigtop-namenode.started')
def start_namenode():
    hookenv.status_set('maintenance', 'starting namenode')
    # NB: service should be started by install, but this may be handy in case
    # we have something that removes the .started state in the future. Also
    # note we restart here in case we modify conf between install and now.
    host.service_restart('hadoop-hdfs-namenode')
    for port in get_layer_opts().exposed_ports('namenode'):
        hookenv.open_port(port)
    set_state('apache-bigtop-namenode.started')
    hookenv.status_set('maintenance', 'namenode started')


@when('datanode.joined')
@when_not('apache-bigtop-namenode.installed')
def send_dn_install_info(datanode):
    """Send datanodes enough info to start their install.

    If a datanode joins before the namenode is installed, we can still provide
    enough info to start their installation. This will help parallelize
    installation among our cluster.

    NOTE: Datanodes can safely install early, but should not start until
    the dfs-slave interface has set the 'namenode.ready' state.
    """
    fqdn = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    hdfs_port = get_layer_opts().port('namenode')
    webhdfs_port = get_layer_opts().port('nn_webapp_http')

    datanode.send_namenodes([fqdn])
    datanode.send_ports(hdfs_port, webhdfs_port)


@when('apache-bigtop-namenode.started', 'datanode.joined')
def send_dn_all_info(datanode):
    """Send datanodes all dfs-slave relation data.

    At this point, the namenode is ready to serve datanodes. Send all
    dfs-slave relation data so that our 'namenode.ready' state becomes set.
    """
    bigtop = get_bigtop_base()
    fqdn = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    hdfs_port = get_layer_opts().port('namenode')
    webhdfs_port = get_layer_opts().port('nn_webapp_http')

    datanode.send_spec(bigtop.spec())
    datanode.send_namenodes([fqdn])
    datanode.send_ports(hdfs_port, webhdfs_port)

    # hosts_map and ssh_key are required by the dfs-slave interface to signify
    # NN's readiness. Send them, even though they are not utilized by bigtop.
    # NB: update KV hosts with all datanodes prior to sending the hosts_map
    # because dfs-slave gates readiness on a DN's presence in the hosts_map.
    utils.update_kv_hosts({node['ip']: node['host']
                           for node in datanode.nodes()})
    datanode.send_hosts_map(utils.get_kv_hosts())
    datanode.send_ssh_key('invalid')

    # update status with slave count and report ready for hdfs
    slaves = [node['host'] for node in datanode.nodes()]
    hookenv.status_set('active', 'ready ({count} datanode{s})'.format(
        count=len(slaves),
        s='s' if len(slaves) > 1 else '',
    ))
    set_state('apache-bigtop-namenode.ready')


@when('apache-bigtop-namenode.started', 'datanode.departing')
def remove_dn(datanode):
    """Handle a departing datanode.

    This simply logs a message about a departing datanode and removes
    the entry from our KV hosts_map. The hosts_map is not used by bigtop, but
    it is required for the 'namenode.ready' state, so we may as well keep it
    accurate.
    """
    nodes_leaving = datanode.nodes()  # only returns nodes in "departing" state
    slaves_leaving = [node['host'] for node in nodes_leaving]
    hookenv.log('Datanodes leaving: {}'.format(slaves_leaving))
    utils.remove_kv_hosts(slaves_leaving)
    datanode.dismiss()


@when('apache-bigtop-namenode.started')
@when_not('datanode.joined')
def wait_for_dn():
    remove_state('apache-bigtop-namenode.ready')
    # NB: we're still active since a user may be interested in our web UI
    # without any DNs, but let them know hdfs is caput without a DN relation.
    hookenv.status_set('active', 'hdfs requires a datanode relation')


@when('namenode.clients')
@when_not('apache-bigtop-namenode.installed')
def send_client_install_info(client):
    """Send clients (plugin, RM, non-DNs) enough info to start their install.

    If a client joins before the namenode is installed, we can still provide
    enough info to start their installation. This will help parallelize
    installation among our cluster.

    NOTE: Clients can safely install early, but should not start until
    the dfs interface has set the 'namenode.ready' state.
    """
    fqdn = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    hdfs_port = get_layer_opts().port('namenode')
    webhdfs_port = get_layer_opts().port('nn_webapp_http')

    client.send_namenodes([fqdn])
    client.send_ports(hdfs_port, webhdfs_port)


@when('apache-bigtop-namenode.started', 'namenode.clients')
def send_client_all_info(client):
    """Send clients (plugin, RM, non-DNs) all dfs relation data.

    At this point, the namenode is ready to serve clients. Send all
    dfs relation data so that our 'namenode.ready' state becomes set.
    """
    bigtop = get_bigtop_base()
    fqdn = subprocess.check_output(['facter', 'fqdn']).strip().decode()
    hdfs_port = get_layer_opts().port('namenode')
    webhdfs_port = get_layer_opts().port('nn_webapp_http')

    client.send_spec(bigtop.spec())
    client.send_namenodes([fqdn])
    client.send_ports(hdfs_port, webhdfs_port)
    # namenode.ready implies we have at least 1 datanode, which means hdfs
    # is ready for use. Inform clients of that with send_ready().
    if is_state('apache-bigtop-namenode.ready'):
        client.send_ready(True)
    else:
        client.send_ready(False)

    # hosts_map is required by the dfs interface to signify
    # NN's readiness. Send it, even though it is not utilized by bigtop.
    client.send_hosts_map(utils.get_kv_hosts())


@when('benchmark.joined')
def register_benchmarks(benchmark):
    benchmark.register('nnbench', 'testdfsio')
