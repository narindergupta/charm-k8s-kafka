import ipaddress
import json
import logging

from ops.framework import EventBase, EventSource, EventSetBase, StoredState
from ops.framework import Object

logger = logging.getLogger(__name__)


class KafkaCluster(Object):

    def __init__(self, charm, relation_name, listen_on_all_addresses):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        if listen_on_all_addresses:
            # This will create a listening socket for all IPv4 and IPv6 addresses.
            self._listen_address = ipaddress.ip_address('0.0.0.0')
            self._ingress_address = ipaddress.ip_address('0.0.0.0')
        else:
            self._listen_address = None
            self._ingress_address = None

    @property
    def is_joined(self):
        return self.framework.model.get_relation(self._relation_name) is not None

    @property
    def relation(self):
        return self.framework.model.get_relation(self._relation_name)

    @property
    def peer_addresses(self):
        addresses = []
        relation = self.relation
        if relation:
            for u in relation.units:
                addresses.append(relation.data[u]['ingress-address'])
        return addresses

    @property
    def listen_address(self):
        if self._listen_address is None:
            self._listen_address = self.model.get_binding(self._relation_name).network.bind_address
        return self._listen_address

    @property
    def ingress_address(self):
        if self._ingress_address is None:
            self._ingress_address = self.model.get_binding(self._relation_name).network.ingress_address
        return self._ingress_address

    def init_state(self, event):
        self.state.apps = []
        pass


class KafkaClient(Object):

    state = StoredState()

    def __init__(self, charm, relation_name, listen_on_all_addresses, client_port):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self._client_port = client_port
        self._listen_on_all_addresses = listen_on_all_addresses
        if listen_on_all_addresses:
            # This will create a listening socket for all IPv4 and IPv6 addresses.
            self._listen_address = ipaddress.ip_address('0.0.0.0')
        else:
            self._listen_address = None
        self._ingress_addresses = None
        self.state.set_default(host=None, port=None)

    @property
    def listen_address(self):
        if self._listen_address is None:
            addresses = set()
            for relation in self.model.relations[self._relation_name]:
                address = self.model.get_binding(relation).network.bind_address
                addresses.add(address)
            if len(addresses) > 1:
                raise Exception('Multiple potential listen addresses detected: Kafka does not support that')
            elif addresses == 1:
                self._listen_address = addresses.pop()
            else:
                # Default to network information associated with an endpoint binding itself in absence of relations.
                self._listen_address = self.model.get_binding(self._relation_name).network.bind_address
        return self._listen_address

    def set_host(self, host):
        self.state.host = host

    def set_port(self, port):
        self.state.port = port

    def expose_kafka(self):
        rel = self.model.get_relation[self._relation_name]
        if rel is not None:
            if self.model.unit.is_leader() and self.state.host is not None:
                rel.data[self.model.unit]['host'] = self.state.host
                rel.data[self.model.unit]['port'] = str(self.state.port)

    @property
    def ingress_addresses(self):
        # Even though Kafka does not support multiple listening addresses that does not mean there
        # cannot be multiple ingress addresses clients would use.
        if self._ingress_addresses is None:
            self._ingress_addresses = set()
            for relation in self.model.relations[self._relation_name]:
                self._ingress_addresses.add(self.model.get_binding(relation).network.ingress_address)
        return self._ingress_addresses


class ZookeeperAvailable(EventBase):
    pass


class ZookeeperReady(EventBase):
    pass


class ZookeeperClientEvents(EventSetBase):
    zookeeper_available = EventSource(ZookeeperAvailable)
    zookeeper_ready = EventSource(ZookeeperReady)


class ZookeeperClient(Object):
    on = ZookeeperClientEvents()
    state = StoredState()

    def __init__(self, charm, relation_name):
        super().__init__(charm, relation_name)
        self._relation_name = relation_name
        self.state.set_default(zkhost=None, zkport=None, zkrest_port=None)

        self.framework.observe(charm.on[relation_name].relation_joined, self.on_relation_joined)
        self.framework.observe(charm.on[relation_name].relation_changed, self.on_relation_changed)

    @property
    def is_joined(self):
        return self.framework.model.get_relation(self._relation_name) is not None

    @property
    def is_ready(self):
        return all(p is not None for p in (self.state.host, self.state.port, self.state.rest_port))

    @property
    def host(self):
        return self.state.zkhost

    @property
    def port(self):
        return self.state.zkport

    @property
    def rest_port(self):
        return self.state.zkrest_port

    def on_relation_joined(self, event):
        self.on.zookeeper_available.emit()

    def on_relation_changed(self, event):
        # easy-rsa is not HA so there is only one unit to work with and Vault uses one leader unit to
        # write responses and does not (at the time of writing) rely on app relation data.
        remote_data = event.relation.data[event.unit]

        host = remote_data.get('host')
        port = remote_data.get('port')
        rest_port = remote_data.get('rest_port')
        if host is None or port is None or rest_port is None:
            logger.info('A Zookeeper has not yet exposed a requested host, port and rest port.')
            return
        self.state.zkhost = host
        self.state.zkport = port
        self.state.zkrest_port = rest_port
        self.on.zookeeper_ready.emit()
