from collections import defaultdict
import topic as tp
import simpy
import random
import functools
import copy
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt


class Packet(object):
    """ A very simple class that represents a packet.
        This packet will run through a queue at a switch output port.
        We use a float to represent the size of the packet in bytes so that
        we can compare to ideal M/M/1 queues.

        Parameters
        ----------
        time : float
            the time the packet is generated.
        size : float
            the size of the packet in bytes
        id : int
            an identifier for the packet
        src : string
            identifiers for source
        topic : string
            topic the content is under
        pkt_type : pub or sub, publication or subscription
    """

    def __init__(self, time, size, id, src="a", topic=None, pkt_type=None):
        self.time = time
        self.size = size
        self.id = id
        self.src = src
        # list of sp_id, travel path of the packet
        self.trace = []

        self.topic = topic
        # pkt_type: pub sub
        self.pkt_type = pkt_type

    def __repr__(self):
        return "id: {}, src: {}, trace: {}, time: {}, size: {}, topic:{}, pkt_type:{}". \
            format(self.id, self.src, self.trace, self.time, self.size, self.topic, self.pkt_type)


class Client(object):
    """ A class that represents a client: a publisher or a subscriber .
            publisher sends out publication.
            subscriber sends out subscription, and receives publication.

            Parameters
            ----------
            env : simpy.Environment
                the simulation environment
            adist : function
                a no parameter function that returns the successive inter-arrival times of the packets
            sdist : function
                a no parameter function that returns the successive sizes of the packets
            client_type : 'pub' or 'sub'
                identifies publisher or subscriber
            client_id : string
                id of a client
            topic_list : array of string
                the list of topics that the client will subscribe to or publish under
            debug : boolean
                if true then the contents of each packet will be printed as it is received.
            finish : float
                lifetime of the client

        """

    def __init__(self, env, adist, sdist, client_type, client_id, topic_list, finish=float("inf"), debug=False):
        self.client_id = client_id
        self.env = env
        # packet generator
        self.adist = adist
        self.sdist = sdist
        self.finish = finish
        self.out = None
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.client_type = client_type
        self.topic_list = topic_list
        # packet sink
        self.store = simpy.Store(env)
        self.waits = []
        self.arrivals = []
        self.debug = debug
        self.packets_rec = 0
        self.bytes_rec = 0
        self.packets_sent = 0
        self.bytes_sent = 0
        self.last_arrival = 0.0

    def run(self):
        """The generator function used in simulations.
        """
        while self.env.now < self.finish:
            # wait for next transmission
            yield self.env.timeout(self.adist())
            topic = self.topic_list[random.randint(0, len(self.topic_list)) - 1]
            p = Packet(self.env.now, self.sdist(), self.packets_sent, pkt_type=self.client_type, topic=topic,
                       src=self.client_id)
            p.trace.append(self.client_id)
            self.packets_sent += 1
            self.bytes_sent += p.size
            self.out.put(p)

    def put(self, pkt):
        now = self.env.now
        self.waits.append(self.env.now - pkt.time)
        self.arrivals.append(now - self.last_arrival)
        self.last_arrival = now
        self.packets_rec += 1
        self.bytes_rec += pkt.size
        if self.debug:
            print("client id: " + self.client_id)
            print(pkt)
            print()


class SwitchPort(object):
    """ Models a switch output port with a given rate and buffer size limit in bytes.
        Set the "out" member variable to the entity to receive the packet.

        Parameters
        ----------
        env : simpy.Environment
            the simulation environment
        rate : float
            the bit rate of the port
        sp_id : string
            id of a switch_port
        qlimit : integer (or None)
            a buffer size limit in bytes or packets for the queue (including items
            in service).
        limit_bytes : If true, the queue limit will be based on bytes if false the
            queue limit will be based on packets.

    """
    mode = "PF"

    def __init__(self, env, rate, sp_id, qlimit=None, limit_bytes=True, debug=False):
        self.sp_id = sp_id
        self.store = simpy.Store(env)
        self.rate = rate
        self.env = env
        self.outs = []
        self.packets_rec = 0
        self.packets_sent = 0
        self.packets_drop = 0
        self.qlimit = qlimit
        self.limit_bytes = limit_bytes
        self.bytes_rec = 0  # cumulative byte received
        self.bytes_sent = 0  # cumulative byte send
        self.byte_size = 0  # Current size of the queue in bytes
        self.debug = debug
        self.busy = 0  # Used to track if a packet is currently being sent
        self.action = env.process(self.run())  # starts the run() method as a SimPy process
        self.topic_tree = tp.TopicTree()
        self.sending_tree = tp.TopicTree()

    def run(self):
        while True:
            msg = (yield self.store.get())
            self.busy = 1
            self.byte_size -= msg.size

            yield self.env.timeout(msg.size / self.rate)

            yield self.env.process(self.inbound(msg))

            yield self.env.process(self.outbound(msg))

            self.busy = 0

            if self.debug:
                print(msg)

    def outbound(self, msg):
        outbound_delay = 0
        if SwitchPort.mode == 'PF':
            if msg.pkt_type == 'sub':
                pass
            elif msg.pkt_type == 'pub':
                for out in self.outs:
                    if isinstance(out, SwitchPort) and out.sp_id != msg.trace[-1]:
                        # flood to all neighbour brokers
                        msg_copy = copy.deepcopy(msg)
                        msg_copy.trace.append(self.sp_id)
                        out.put(msg_copy)
                        self.packets_sent += 1
                        self.bytes_sent += msg.size

                    elif isinstance(out, Client) and out.client_id != msg.trace[-1]:
                        # send message to interested client
                        target_nodes, delay = self.topic_tree.match_branch(msg.topic)
                        outbound_delay += delay
                        for node in target_nodes:
                            # if target_node is not None:
                            if out.client_id in node.broker_ids:
                                msg_copy = copy.deepcopy(msg)
                                msg_copy.trace.append(self.sp_id)
                                out.put(msg_copy)
                                self.packets_sent += 1
                                self.bytes_sent += msg.size
                                break
            yield self.env.timeout(outbound_delay)
        elif SwitchPort.mode == 'SF':
            if msg.pkt_type == 'sub':
                for out in self.outs:
                    if isinstance(out, SwitchPort) and out.sp_id != msg.trace[-1]:
                        # flood to all neighbour brokers
                        msg_copy = copy.deepcopy(msg)
                        msg_copy.trace.append(self.sp_id)
                        out.put(msg_copy)
            elif msg.pkt_type == 'pub':
                target_nodes, match_branch_delay = self.topic_tree.match_branch(msg.topic)
                outbound_delay += match_branch_delay
                for out in self.outs:
                    if isinstance(out, SwitchPort) and out.sp_id != msg.trace[-1]:
                        for node in target_nodes:
                            if out.sp_id in node.broker_ids:
                                msg_copy = copy.deepcopy(msg)
                                msg_copy.trace.append(self.sp_id)
                                out.put(msg_copy)
                                self.packets_sent += 1
                                self.bytes_sent += msg.size
                                break

                    elif isinstance(out, Client) and out.client_id != msg.trace[-1]:
                        for node in target_nodes:
                            if out.client_id in node.broker_ids:
                                msg_copy = copy.deepcopy(msg)
                                msg_copy.trace.append(self.sp_id)
                                out.put(msg_copy)
                                self.packets_sent += 1
                                self.bytes_sent += msg.size
                                break
            yield self.env.timeout(outbound_delay)
        elif SwitchPort.mode == 'SSF':
            if msg.pkt_type == 'sub':
                subscribed_nodes, match_branch_delay = self.sending_tree.match_branch(msg.topic)
                outbound_delay += match_branch_delay
                flooding_group = []
                for out in self.outs:
                    if isinstance(out, SwitchPort) and out.sp_id != msg.trace[-1]:
                        is_necessary = True
                        for node in subscribed_nodes:
                            if out.sp_id in node.broker_ids:
                                is_necessary = False
                                break
                        if is_necessary:
                            # add necessary broker into flood group
                            flooding_group.append(out)
                            # update sending tree
                            add_branch_delay = self.sending_tree.add_branch(msg.topic, out.sp_id)
                            outbound_delay += add_branch_delay
                # do flood
                for out in flooding_group:
                    msg_copy = copy.deepcopy(msg)
                    msg_copy.trace.append(self.sp_id)
                    out.put(msg_copy)
                    self.packets_sent += 1
                    self.bytes_sent += msg.size
            elif msg.pkt_type == 'pub':
                target_nodes, match_branch_delay = self.topic_tree.match_branch(msg.topic)
                outbound_delay += match_branch_delay
                for out in self.outs:
                    if isinstance(out, SwitchPort) and out.sp_id != msg.trace[-1]:
                        for node in target_nodes:
                            if out.sp_id in node.broker_ids:
                                msg_copy = copy.deepcopy(msg)
                                msg_copy.trace.append(self.sp_id)
                                out.put(msg_copy)
                                self.packets_sent += 1
                                self.bytes_sent += msg.size
                                break
                    elif isinstance(out, Client) and out.client_id != msg.trace[-1]:
                        for node in target_nodes:
                            if out.client_id in node.broker_ids:
                                msg_copy = copy.deepcopy(msg)
                                msg_copy.trace.append(self.sp_id)
                                out.put(msg_copy)
                                self.packets_sent += 1
                                self.bytes_sent += msg.size
                                break
            yield self.env.timeout(outbound_delay)
        else:
            raise Exception(SwitchPort.mode + " unknown flooding mode")

    def put(self, pkt):
        self.packets_rec += 1
        tmp_byte_count = self.byte_size + pkt.size

        if self.qlimit is None:
            self.byte_size = tmp_byte_count
            # return self.store.put(pkt)
            self.bytes_rec += pkt.size
            return self.store.put(pkt)
        if self.limit_bytes and tmp_byte_count >= self.qlimit:
            self.packets_drop += 1
            return
        elif not self.limit_bytes and len(self.store.items) >= self.qlimit - 1:
            self.packets_drop += 1
        else:
            self.byte_size = tmp_byte_count
            # return self.store.put(pkt)
            self.bytes_rec += pkt.size
            return self.store.put(pkt)

    def inbound(self, pkt):
        inbound_delay = 0
        if SwitchPort.mode == 'PF':
            if pkt.pkt_type == 'sub':
                last_hop = pkt.trace[-1]
                # add interests
                add_branch_delay = self.topic_tree.add_branch(pkt.topic, last_hop)
                inbound_delay += add_branch_delay
            yield self.env.timeout(inbound_delay)
        elif SwitchPort.mode == 'SF':
            if pkt.pkt_type == 'sub':
                last_hop = pkt.trace[-1]
                # add interests
                add_branch_delay = self.topic_tree.add_branch(pkt.topic, last_hop)
                inbound_delay += add_branch_delay
            yield self.env.timeout(inbound_delay)
        elif SwitchPort.mode == 'SSF':
            if pkt.pkt_type == 'sub':
                last_hop = pkt.trace[-1]
                # add interests
                add_branch_delay = self.topic_tree.add_branch(pkt.topic, last_hop)
                inbound_delay += add_branch_delay
            yield self.env.timeout(inbound_delay)
        else:
            raise Exception(SwitchPort.mode + " unknown flooding mode")


class PortMonitor(object):
    """ A monitor for an SwitchPort. Looks at the number of items in the SwitchPort
           in service + in the queue and records that info in the sizes[] list. The
           monitor looks at the port at time intervals given by the distribution dist.

           Parameters
           ----------
           env : simpy.Environment
               the simulation environment
           port : SwitchPort
               the switch port object to be monitored.
           dist : function
               a no parameter function that returns the successive inter-arrival times of the
               packets
       """

    def __init__(self, env, port, dist):
        self.port = port
        self.env = env
        self.dist = dist
        self.bytes_sent = []
        self.queue_length = []
        self.queue_size = []
        self.output_rate_cumulative = []
        self.output_rate = []
        self.input_rate = []
        self.action = env.process(self.run())

    def run(self):
        while True:
            yield self.env.timeout(self.dist())
            # update queue size
            self.queue_size.append(self.port.byte_size)
            self.queue_length.append(len(self.port.store.items) + self.port.busy)

            # update input rate
            self.input_rate.append(self.port.bytes_rec / self.env.now)

            # update cumulative average output rate
            self.output_rate_cumulative.append(self.port.bytes_sent / self.env.now)

            self.bytes_sent.append(self.port.bytes_sent)


class ClientMonitor(object):
    """ A monitor for an Client.

              Parameters
              ----------
              env : simpy.Environment
                  the simulation environment
              client : Client
                  the Client object to be monitored.
              dist : function
                  a no parameter function that returns the successive inter-arrival times of the
                  packets
          """

    def __init__(self, env, client, dist):
        self.client = client
        self.env = env
        self.dist = dist
        self.tot_waits = []
        self.packets_rec = []

        self.action = env.process(self.run())

    def run(self):
        while True:
            yield self.env.timeout(self.dist())
            self.packets_rec.append(self.client.packets_rec)
            # wait = None
            # if self.client.packets_rec != 0:
            wait = sum(self.client.waits)
            self.tot_waits.append(wait)


class Network(object):
    # total_topic is TopicTree
    def __init__(self, total_topic: tp.TopicTree, avg_sub_size, avg_pub_size, qlimit=None):

        self.env = simpy.Environment()
        self.broker_list = []
        self.broker_monitor_list = []
        self.sub_list = []
        self.sub_monitor_list = []
        self.pub_list = []
        self.pub_monitor_list = []
        self.total_topic = total_topic
        self.avg_sub_size = avg_sub_size
        self.avg_pub_size = avg_pub_size
        self.qlimit = qlimit
        self.edge_set = None

        # construct broker list

    def initialize_nodes(self, broker_rates, sub_rates, pub_rates, monitor_rate, seed=None):

        if seed is not None:
            random.seed(seed)

        mdist = functools.partial(constarrival, monitor_rate)

        for i in range(len(broker_rates)):
            # sp id starts at sp1
            sp_id = 'sp' + str(i + 1)
            broker = SwitchPort(self.env, broker_rates[i], sp_id, qlimit=self.qlimit)
            broker_monitor = PortMonitor(self.env, broker, mdist)
            self.broker_list.append(broker)
            self.broker_monitor_list.append(broker_monitor)

        # construct sub list
        sub_sdist = functools.partial(random.expovariate, 1 / self.avg_sub_size)
        for i in range(len(sub_rates)):
            # sub id starts at sub1
            sub_id = 'sub' + str(i + 1)
            sub_adist = functools.partial(random.expovariate, sub_rates[i])
            # set subscriber interested topic
            # num_topic = random.randint(1, len(self.total_topic))
            # topic_list = random.sample(self.total_topic, num_topic)
            topic_list = tp.get_topic_random(self.total_topic)
            sub = Client(self.env, adist=sub_adist, sdist=sub_sdist, client_type='sub', client_id=sub_id,
                         topic_list=topic_list)
            sub_monitor = ClientMonitor(self.env, sub, mdist)
            self.sub_list.append(sub)
            self.sub_monitor_list.append(sub_monitor)

        # construct pub list
        pub_sdist = functools.partial(random.expovariate, 1 / self.avg_pub_size)
        for i in range(len(pub_rates)):
            pub_id = 'pub' + str(i + 1)
            pub_adist = functools.partial(random.expovariate, pub_rates[i])
            # set publisher interested topic
            # num_topic = random.randint(1, len(self.total_topic))
            # topic_list = random.sample(self.total_topic, num_topic)
            topic_list = tp.get_topic_random(self.total_topic, wild=False)
            pub = Client(self.env, adist=pub_adist, sdist=pub_sdist, client_type='pub', client_id=pub_id,
                         topic_list=topic_list)
            pub_monitor = ClientMonitor(self.env, pub, mdist)
            self.pub_list.append(pub)
            self.pub_monitor_list.append(pub_monitor)

    def establish_topology(self, seed=None):

        num_broker = len(self.broker_list)
        self.edge_set = get_edges(num_broker, seed)
        for id1, id2 in self.edge_set:
            broker1 = self.broker_list[id1 - 1]
            broker2 = self.broker_list[id2 - 1]
            broker1.outs.append(broker2)
            broker2.outs.append(broker1)

    def connect_client(self, connection_style=None, seed=None):

        if seed is not None:
            random.seed(seed)

        if connection_style is None:
            num_sub = len(self.sub_list)
            num_pub = len(self.pub_list)
            sub_brokers = random.sample(self.broker_list, num_sub)
            pub_brokers = random.sample(self.broker_list, num_pub)
            for i in range(num_sub):
                self.sub_list[i].out = sub_brokers[i]
                sub_brokers[i].outs.append(self.sub_list[i])
            for i in range(num_pub):
                self.pub_list[i].out = pub_brokers[i]
                pub_brokers[i].outs.append(self.pub_list[i])
        elif connection_style == "non_overlap":
            num_sub = len(self.sub_list)
            num_pub = len(self.pub_list)
            target_brokers = random.sample(self.broker_list, num_sub + num_pub)
            for i in range(num_sub):
                self.sub_list[i].out = target_brokers[i]
                target_brokers[i].outs.append(self.sub_list[i])
            for i in range(num_pub):
                self.pub_list[i].out = target_brokers[i + num_sub]
                target_brokers[i + num_sub].outs.append(self.pub_list[i])
        else:
            raise Exception("not implemented")

    def visualize(self):
        G = nx.Graph()
        broker_nodes = []
        sub_nodes = []
        pub_nodes = []
        broker_connections = []
        sub_connections = []
        pub_connections = []
        color_map = []
        for broker in self.broker_list:
            broker_nodes.append((broker.sp_id, {"node_color": "blue"}))
            color_map.append('#1798E6')
        for sub in self.sub_list:
            sub_nodes.append((sub.client_id, {"color": "red"}))
            color_map.append('#B84B44')
        for pub in self.pub_list:
            pub_nodes.append((pub.client_id, {"color": "yellow"}))
            color_map.append('#E6D317')
        G.add_nodes_from(broker_nodes)
        G.add_nodes_from(sub_nodes)
        G.add_nodes_from(pub_nodes)

        for id1, id2 in self.edge_set:
            broker_connections.append((self.broker_list[id1 - 1].sp_id, self.broker_list[id2 - 1].sp_id))
        for sub in self.sub_list:
            sub_connections.append((sub.client_id, sub.out.sp_id))
        for pub in self.pub_list:
            pub_connections.append((pub.client_id, pub.out.sp_id))

        G.add_edges_from(broker_connections)
        G.add_edges_from(sub_connections)
        G.add_edges_from(pub_connections)

        nx.draw_networkx(G, node_color=color_map)
        plt.show()


def constarrival(t):
    return t


def get_edges(m, seed=None):
    np.random.seed(seed)

    edge_set = []
    # getPrufer
    length = m - 2

    vertices = m

    prufer = np.random.randint(1, high=vertices + 1, size=length)

    # Initialize the array of vertices
    vertex_set = [0] * vertices

    # Number of occurrences of vertex in code
    for i in range(vertices - 2):
        vertex_set[prufer[i] - 1] += 1

    # print("The edge set E(G) is :")

    # Find the smallest label not present in prufer.

    j = 0
    for i in range(vertices - 2):
        for j in range(vertices):

            # If j+1 is not present in prufer set
            if (vertex_set[j] == 0):
                # Remove from Prufer set and print
                # pair.
                vertex_set[j] = -1
                edge_set.append((j + 1, prufer[i]))
                # print("(", (j + 1), ", ", prufer[i], ") ", sep="", end="")
                vertex_set[prufer[i] - 1] -= 1
                break

    j = 0

    # For the last element
    first_el = 0
    second_el = 0
    for i in range(vertices):
        if vertex_set[i] == 0 and j == 0:
            first_el = i + 1
            # print("(", (i + 1), ", ", sep="", end="")
            j += 1
        elif vertex_set[i] == 0 and j == 1:
            second_el = i + 1
            # print((i + 1), ")")

    edge_set.append((first_el, second_el))

    return edge_set


if __name__ == '__main__':
    mdist = functools.partial(constarrival, 2.5)

    mean_pkt_size = 100.0  # in bytes
    adist1 = functools.partial(random.expovariate, 2.5)
    sdist = functools.partial(random.expovariate, 1.0 / mean_pkt_size)
    port_rate = 3 * mean_pkt_size
    samp_dist = functools.partial(random.expovariate, 0.50)

    env = simpy.Environment()
    sub1 = Client(env, client_type="sub", client_id="sub1", adist=adist1, sdist=sdist,
                  topic_list=['t1', 't2', 't3', 't4'], debug=False)
    pub1 = Client(env, client_type="pub", client_id="pub1", adist=adist1, sdist=sdist, topic_list=['t4', 't5'],
                  debug=False)
    switch_port1 = SwitchPort(env, port_rate, sp_id="sp1", debug=False)
    switch_port2 = SwitchPort(env, port_rate, sp_id="sp2", debug=False)

    sub1.out = switch_port1
    switch_port1.outs.append(sub1)
    switch_port1.outs.append(switch_port2)
    switch_port2.outs.append(switch_port1)
    switch_port2.outs.append(pub1)
    pub1.out = switch_port2

    pm1 = PortMonitor(env, switch_port1, mdist)
    pm2 = PortMonitor(env, switch_port2, mdist)
    cm1 = ClientMonitor(env, sub1, mdist)
    # print(switch_port1.outs[pub1])

    env.run(until=20)

    print('sub1: ')
    print(cm1.avg_waits)

    print('pm1: ')
    print(list(map(int, pm1.queue_size)))

    print('pm2: ')
    print(list(map(int, pm2.queue_size)))
