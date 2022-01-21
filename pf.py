import myComponents as tc
import topic as tp
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import random
import os
import datetime

# network parameter
mode = 'PF'
delay_unit = 1

num_broker = 1000
num_sub = 8000
num_pub = 2000

broker_r = 3000
broker_rates = [broker_r] * num_broker
topic_dist = [5, 5, 5, 5, 5, 5, 5]
monitor_rate = 1
runtime = 650
sub_start = 5
sub_end = 35
pub_start = 100
pub_end = 130
prop_delay = 0.0
connection_style = None

# sub parameter
avg_sub_size = 10.0
sub_r = 0.1

sub_rates = [sub_r] * num_sub
sub_num_topic = [100] * num_sub

# pub parameter
avg_pub_size = 20.0
pub_r = 1

pub_rates = [pub_r] * num_pub
pub_num_topic = [100] * num_pub
pub_diameter = [14] * num_pub

wildcard_rate = 1
plus_rate = 0.9
hash_rate = 0.09

seed = 1

diameter = 10
sub_diameter = [diameter] * num_sub

dir_name = str(datetime.date.today()) + '_' + str(diameter) + '_' + str(num_broker) + '_' + str(broker_r) + '_' + str(
    prop_delay) + '_' + str(wildcard_rate * plus_rate) + '_' + str(wildcard_rate * hash_rate)
os.makedirs('./mqtt_data/' + dir_name, exist_ok=True)

sub_waits = open('./mqtt_data/' + dir_name + '/' + mode + '_' + 'sub_waits.csv', 'w')
sub_pkt = open('./mqtt_data/' + dir_name + '/' + mode + '_' + 'sub_pkt.csv', 'w')
broker_queue = open('./mqtt_data/' + dir_name + '/' + mode + '_' + 'broker_queue.csv', 'w')
broker_output = open('./mqtt_data/' + dir_name + '/' + mode + '_' + 'broker_output.csv', 'w')
broker_sub = open('./mqtt_data/' + dir_name + '/' + mode + '_' + 'sub_queue.csv', 'w')
broker_pub = open('./mqtt_data/' + dir_name + '/' + mode + '_' + 'pub_queue.csv', 'w')

tc.SwitchPort.mode = mode
tc.SwitchPort.delay_unit = delay_unit

total_topic = tp.TopicTree(wildcard_rate, plus_rate, hash_rate)
total_topic.random_construct(topic_dist, seed)
# total_topic.visualize(total_topic.root)
net = tc.Network(total_topic, avg_sub_size, avg_pub_size, sub_waits, sub_pkt, broker_queue,
                 broker_output, broker_sub, broker_pub, qlimit=None, debug=False)
net.initialize_nodes(broker_rates, sub_rates, sub_num_topic, sub_diameter, pub_rates, pub_num_topic, pub_diameter,
                     monitor_rate,
                     sub_start=sub_start, sub_end=sub_end, pub_start=pub_start, pub_end=pub_end, prop_delay=prop_delay,
                     seed=seed)
net.establish_topology(seed)
net.connect_client(connection_style, seed)
print()
print(tc.SwitchPort.mode)
print(plus_rate)
print(hash_rate)

net.env.run(runtime)

sub_waits.close()
sub_pkt.close()
broker_queue.close()
broker_output.close()
broker_sub.close()
broker_pub.close()

#######################################