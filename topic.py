import random
import numpy as np


class TopicNode(object):

    def __init__(self, name, isroot=False):
        self.name = name
        self.isroot = isroot
        self.parent = None
        self.children = []
        self.broker_ids = []

    def visualize(self):
        print('TopicNode:', end=" ")
        node_str = ''
        current_node = self
        while current_node.parent is not None:
            node_str = ' --> ' + current_node.name + node_str
            # print(current_node.name, end='-->')
            current_node = current_node.parent
        # print('root')
        node_str = current_node.name + node_str
        print(node_str, end=" ")
        print(self.broker_ids)


class TopicTree(object):

    def __init__(self, wildcard_rate=0.5, plus_rate=0.7, hash_rate=0.5):

        self.level = 0
        # all nodes except root
        self.all_nodes = []
        self.root = TopicNode('root', isroot=True)
        self.transmission_delay = None

        self.wildcard_rate = wildcard_rate
        self.plus_rate = plus_rate
        self.hash_rate = hash_rate

    def add_branch(self, topic, broker_id=None):
        # add broker id to topic, if topic not exist add topic to topic tree
        name_list = str.split(topic, '/')
        parent = self.root
        for name in name_list:
            child = find_child(parent, name)
            if child is None:
                new_child = TopicNode(name)
                new_child.parent = parent
                parent.children.append(new_child)
                parent = new_child
            else:
                parent = child
        if broker_id:
            if broker_id not in parent.broker_ids:
                parent.broker_ids.append(broker_id)
        delay = 0.01
        return delay

    def match_branch(self, topic):
        # return list of nodes, matches all nodes that contains input topic
        name_list = str.split(topic, '/')
        parent_list = [self.root]
        result = []
        for name in name_list:
            # get next layer parent and update result when encounter #
            children = []
            for parent in parent_list:
                for child in parent.children:
                    if child.name == '#':
                        result.append(child)
                    elif child.name == '+' and name != '#':
                        children.append(child)
                    elif child.name == name:
                        children.append(child)
            parent_list = children
        result += parent_list
        delay = 0.01
        return result, delay

    def visualize(self):
        visualize_helper(self.root, 0)

    def random_construct(self, layer_branch_list, seed=None):

        if seed is not None:
            random.seed(seed)

        self.level = len(layer_branch_list)

        parent_layer = [self.root]
        for i in range(len(layer_branch_list)):
            children_layer = []
            # mean of num_child of this layer
            mean = layer_branch_list[i]
            layer_name = chr(i + 97)
            node_id_list = np.arange(mean * 2)
            for node in parent_layer:
                # num_child = mean - random.randint(-mean // 2, mean // 2)
                num_child = mean
                child_id_list = random.sample(np.ndarray.tolist(node_id_list), num_child)
                for j in range(num_child):
                    child_name = layer_name + str(child_id_list[j])
                    child_node = TopicNode(child_name)
                    child_node.parent = node
                    node.children.append(child_node)
                    children_layer.append(child_node)
                    self.all_nodes.append(child_node)

            parent_layer = children_layer


    def get_topic_within_distance(self, num_topic, diameter=None):
        if diameter is None:
            diameter = self.level * 2 - 1
        idx_list = [*range(len(self.all_nodes))]
        topic_str_list = []
        while len(topic_str_list) < num_topic and len(idx_list) > 0:
            idx = random.choice(idx_list)
            idx_list.remove(idx)
            if(len(idx_list)==0):
                print("1 unable to generate enough topic\n")
                print("2 unable to generate enough topic\n")
            node_str = node_to_str(self.all_nodes[idx])
            conflict = False
            for topic_str in topic_str_list:
                if topic_distance(topic_str, node_str) > diameter:
                    conflict = True
                    break
            if not conflict:
                topic_str_list.append(node_str)

        return topic_str_list

    def get_topic_random(self, num_topic):
        topic_nodes = random.sample(self.all_nodes, num_topic)
        topic_list = []
        for node in topic_nodes:
            topic_str = node_to_str(node)
            topic_list.append(topic_str)
        return topic_list

    def make_wild(self, topic_str_list):
        wild_list = []
        for topic_str in topic_str_list:
            wild_str = add_wildcard(topic_str, wildcard_rate=self.wildcard_rate, plus_rate=self.plus_rate,
                                    hash_rate=self.hash_rate)
            wild_list.append(wild_str)
        return wild_list


def find_child(parent, name):
    if not parent.children:
        return None
    for child in parent.children:
        if child.name == name:
            return child
    return None


def visualize_helper(node: TopicNode, layer_num):
    if not node.children:
        print('   ' * layer_num, end='')
        if layer_num:
            print('|__', end='')
        print(node.name, end=': ')
        print(node.broker_ids)
    else:
        print('   ' * layer_num, end='')
        if layer_num:
            print('|__', end='')
        print(node.name, end=': ')
        print(node.broker_ids)
        for child in node.children:
            visualize_helper(child, layer_num + 1)


def node_to_str(node):
    topic_str = ''
    done = False
    while not done:
        topic_str = node.name + topic_str
        if node.parent.isroot:
            done = True
        else:
            topic_str = '/' + topic_str
        node = node.parent

    return topic_str


# at most add one + and one #
def add_wildcard(topic_str, wildcard_rate=0.5, plus_rate=0.9, hash_rate=0.8):
    if random.uniform(0, 1) < wildcard_rate:  # add wildcard
        name_list = str.split(topic_str, '/')
        if random.uniform(0, 1) < plus_rate:
            # add +
            idx = random.randint(0, len(name_list) - 1)
            name_list[idx] = '+'
        if random.uniform(0, 1) < hash_rate:
            # add #
            if len(name_list) >= 2:
                if name_list[-2] != '+':
                    name_list[-1] = '#'
        topic_str = '/'.join(name_list)
    return topic_str


def get_topic_random(total_topic: TopicTree, wild=True):
    num_topic = random.randint(1, len(total_topic.all_nodes))
    topic_nodes = random.sample(total_topic.all_nodes, num_topic)
    topic_list = []
    for node in topic_nodes:
        topic_str = node_to_str(node)
        if wild:
            topic_str = add_wildcard(topic_str, total_topic.wildcard_rate, total_topic.plus_rate, total_topic.hash_rate)
        topic_list.append(topic_str)
    return topic_list


def topic_distance(topic1_str: str, topic2_str: str):
    topic1 = str.split(topic1_str, '/')
    topic2 = str.split(topic2_str, '/')
    len1 = len(topic1)
    len2 = len(topic2)
    len_diff = np.abs(len1 - len2)
    min_len = min(len1, len2)
    name_diff = 0
    for i in range(min_len):
        if topic1[i] != topic2[i]:
            name_diff += 1
    return len_diff * 2 + name_diff
