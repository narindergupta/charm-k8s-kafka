#!/usr/bin/env python3

import sys
import json
import subprocess
sys.path.append('lib')

from ops.charm import EventSource, EventBase, CharmBase, CharmEvents
from ops.main import main
from ops.framework import StoredState, Object
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    UnknownStatus,
    WaitingStatus,
    ModelError,
)

from interface import KafkaCluster, KafkaClient, ZookeeperClient
from k8s import K8sPod

import logging
import subprocess

import yaml

logging.basicConfig(level=logging.DEBUG)


class KafkaStartedEvent(EventBase):
     pass


class KafkaCharmEvents(CharmEvents):
     kafka_started = EventSource(KafkaStartedEvent)


class KafkaCharm(CharmBase):
    on = KafkaCharmEvents()
    state = StoredState()

    def __init__(self, framework, key):
        super().__init__(framework, key)

        self.framework.observe(self.on.start, self)
        self.framework.observe(self.on.stop, self)
        self.framework.observe(self.on.update_status, self)
        self.framework.observe(self.on.upgrade_charm, self)
        self.framework.observe(self.on.config_changed, self)
        self.framework.observe(self.on.cluster_relation_changed, self)
        self.framework.observe(self.on.cluster_relation_joined, self)
        self.framework.observe(self.on.kafka_relation_changed, self.expose_relation_data)

        self._unit = 1
        self._zookeeperuri = ""
        self._pod = K8sPod(self.framework.model.app.name)
        listen_on_all_addresses = self.model.config['listen-on-all-addresses']

        self.cluster = KafkaCluster(self, 'cluster', listen_on_all_addresses)
        self.client = KafkaClient(self, 'kafka', listen_on_all_addresses, self.model.config['client-port'])

        self.state.set_default(isStarted=False)
        self.zookeeper = ZookeeperClient(self, 'zookeeper')
        self.framework.observe(self.zookeeper.on.zookeeper_ready, self)
        self.framework.observe(self.zookeeper.on.zookeeper_available, self)

    def on_start(self, event):
        logging.info('START: do nothing wait for zookeeper')
        if (self.model.pod._backend.is_leader()):
            self.model.app.status = WaitingStatus('Waiting for Zookeeper to be ready')

    def on_stop(self, event):
        logging.info('STOP')

    def on_upgrade_charm(self, event):
        logging.info('UPGRADE')
        self.on.config_changed.emit()

    def on_cluster_relation_changed(self, event):
        self.getUnits()

    def on_cluster_relation_joined(self, event):
        self.getUnits()

    def on_leader_elected(self, event):
        logging.info('LEADER ELECTED')

    def on_zookeeper_available(self, event):
        self.getZookeeperURI()

    def expose_relation_data(self, event):
        logging.info('Data Exposed')
        fqdn = socket.getnameinfo((str(self.cluster.ingress_address), 0), socket.NI_NAMEREQD)[0]
        logging.info(fqdn)
        self.client.set_host(fqdn)
        self.client.set_port(self.model.config['client-port'])
        self.client.expose_kafka()

    def getUnits(self):
        logging.info('get_units')
        peer_relation = self.model.get_relation('cluster')
        units = self._unit
        if peer_relation is not None:
            self._unit =  len(peer_relation.units) + 1
        #if self._unit != units:
        self.on.config_changed.emit()

    def on_cluster_modified(self, event):
        logging.info('on_cluster_modified')
        self.getUnits()

    def on_update_status(self, event):
        logging.info('UPDATE STATUS')
        if self._pod.is_ready:
            logging.info('Pod is ready')
            self.state.isStarted = True
            if (self.model.pod._backend.is_leader()):
                self.model.unit.status = ActiveStatus('ready')
            else:
                self.model.unit.status = ActiveStatus('ready Not a Leader')

    def on_config_changed(self, event):
        logging.info('CONFIG CHANGED')
        if self._pod.is_ready:
            if (self.model.pod._backend.is_leader()):
                podSpec = self.makePodSpec()
                if self.state.podSpec != podSpec:
                    self.model.pod.set_spec(podSpec)
                    self.state.podSpec = podSpec
        self.on.update_status.emit()

    def on_zookeeper_ready(self, event):
        logging.info('on_zookeeper_ready')
        if (self.model.pod._backend.is_leader()):
            podSpec = self.makePodSpec()
            self.model.pod.set_spec(podSpec)
            self.state.podSpec = podSpec
        self.on.update_status.emit()

    def getZookeeperURI(self):
        if self.zookeeper.is_joined:
            zk_host = self.zookeeper.state.zkhost
            zk_port = self.zookeeper.state.zkport
            zk_rest_port = self.zookeeper.state.zkrest_port
            self._zookeeperuri = "{}:{}".format(zk_host, zk_port)

    def makePodSpec(self):
        logging.info('MAKING POD SPEC')
        self.getZookeeperURI()
        with open("templates/spec_template.yaml") as spec_file:
            podSpecTemplate = spec_file.read()
        dockerImage = self.model.config['image']
        logging.info(self._unit)
        data = {
            "name": self.model.app.name,
            "kafka-thread": self.model.config['default-net-threads'],
            "kafka-partition": self.model.config['default-partitions'],
            "kafka-replication": self.model.config['default-replication-factor'],
            "docker_image": dockerImage,
            "advertised-port": self.model.config['client-port'],
            "advertised-hostname": self.cluster.listen_address,
            "zookeeper_uri": self._zookeeperuri,
        }
        logging.info(data)
        podSpec = podSpecTemplate % data
        podSpec = yaml.load(podSpec)
        return podSpec

if __name__ == "__main__":
    main(KafkaCharm)
