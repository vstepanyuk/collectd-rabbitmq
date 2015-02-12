import urllib2
import urllib
import json
import threading
import Queue
import time
import random

RABBIT_API_URL = "http://{host}:{port}/api/"
PORT = "15672"
USERNAME = "shopclub"
PASSWORD = "santinela"
REALM = "RabbitMQ Management"

class ParseThread(threading.Thread):
    def __init__(self, host, port, realm, username, password, options={}):
        super(ParseThread, self).__init__()
        self.host = host
        self.data = None
        self.base_url = RABBIT_API_URL.format(host=host, port=port)
        self.options = options
        
        # HTTP Basic Auth handler
        auth_handler = urllib2.HTTPBasicAuthHandler()
        auth_handler.add_password(realm=realm, uri=self.base_url, user=username, passwd=password)
        
        self.opener = urllib2.build_opener(auth_handler)

    def quote(self, value):
        '''
        Quote value
        '''

        return urllib.quote(value, '')

    def value(self, obj, path, default=None):
        '''
        Get dict value by path
        '''

        for part in path.split('.'):
            if obj is not None and part in obj:
                obj = obj[part]
            else:
                return default

        return obj

    def get_info(self, path):
        '''
        Get rabbitmq info
        '''

        url = self.base_url + path
        print url
        try:
            info = self.opener.open(url)
        except urllib2.HTTPError as http_error:
            return None
        except urllib2.URLError as url_error:
            return None

        return json.load(info)

    def get_metrics(self, obj, metrics):
        '''
        Get metrics from object
        '''

        tmp = {}
        for key, path in metrics.iteritems():
            tmp[key] = self.value(obj, path, 0) if type(path) is str else self.value(obj, path[0], path[1])

        return tmp

    def dispatch_metrics(self, name, data):
        print '-' * 80
        print "%s\n\t%s = %s\n" % (self.host, name, data)

    def dispatch_vhost_metrics(self, vhost):
        '''
        Dispatch vhost metrics
        '''

        print 'vhost: %s %s' % (vhost['name'], self.get_metrics(vhost, self.options['vhost_metrics']))
        
    def consumers_count_per_vhost(self, consumers):
        '''
        Dispatch consumer metrics
        '''

        if consumers is None:
            return {}

        stats = {}
        for consumer in consumers:
            vhost = consumer["queue"]["vhost"]
            stats[vhost] = stats[vhost] + 1 if vhost in stats else 1

        return stats

    def collect_queues_stats(self, vhost):
        '''
        Collect queues information
        '''

        vhost_name = self.quote(vhost['name'])
        queues = self.get_info('queues/%s' % vhost_name)
        
        if queues is None:
            return {}

        stats = {}
        for queue in queues:
            queue_name = self.quote(queue['name'])
            queue = self.get_info("queues/%s/%s" % (vhost_name, queue_name))
            print 'queue: %s %s %s' % (vhost_name, queue_name, self.get_metrics(queue, self.options['queue_metrics']))
        
        return stats

    def collect_exchanges_stats(self, vhost):
        '''
        Collect exchanges information
        '''
        vhost_name = self.quote(vhost['name'])
        exchanges = self.get_info('exchanges/%s' % vhost_name)

        if exchanges is None:
            return {}

        stats = {}
        for exchange in exchanges:
            exchange_name = self.quote(exchange['name'])
            exchange = self.get_info('exchanges/%s/%s' % (vhost_name, exchange_name))
            print self.get_metrics(exchange, self.options['exchange_metrics'])


    def run(self):
        '''
        Run collect information
        '''

        vhosts = self.get_info('vhosts')
        consumers = self.consumers_count_per_vhost(self.get_info('consumers'))

        if vhosts is not None:
            for vhost in vhosts:
                vhost['consumers'] = consumers[vhost['name']] if vhost['name'] in consumers else 0
                self.dispatch_vhost_metrics(vhost)

                if self.options.get('collect_queues_stats', False):
                    self.collect_queues_stats(vhost)

                if self.options.get('collect_exchanges_stats', False):
                    self.collect_exchanges_stats(vhost)

hosts = ['stage-rabbitmq1.wwdus.local', 'localhost']

threads = []

options = {
    'collect_queues_stats': True,
    'collect_exchanges_stats': True,
    'queue_metrics': {
        'msg_total': 'messages',
        'msg_ready': 'messages_ready',
        'msg_unack': 'messages_unacknowledged',
        'consumers': 'consumers',
        'ack': 'message_stats.ack',
        'get': 'message_stats.get',
        'deliver': 'message_stats.deliver',
        'publish': 'message_stats.publish',

        # Rates
        'msg_rate_total': 'messages_details.rate',
        'msg_rate_ready': 'messages_ready_details.rate',
        'msg_rate_unack': 'messages_unacknowledged_details.rate',
        'publish_rate': ('message_stats.publish_details.rate', 0.0),
        'deliver_rate': ('message_stats.deliver_details.rate', 0.0),
        'ack_rate': ('message_stats.ack_details.rate', 0.0),
        'get_rate': ('message_stats.get_details.rate', 0.0)
    },
    'exchange_metrics': {
        'confirm': 'message_stats.confirm',
        'publish_in': 'message_stats.publish_in',
        'publish_out': 'message_stats.publish_out',
        'return_unroutable': 'message_stats.return_unroutable',
        
        # Rates
        'confirm_rate': ('message_stats.confirm_details.rate', 0.0),
        'publish_in_rate': ('message_stats.publish_in_details.rate', 0.0),
        'publish_out_rate': ('message_stats.publish_out_details.rate', 0.0),
        'return_unroutable_rate': ('message_stats.return_unroutable_details.rate', 0.0),
    },
    'vhost_metrics': {
        'msg_total': 'messages',
        'msg_ready': 'messages_ready',
        'msg_unack': 'messages_unacknowledged',
        'consumers': 'consumers'
    }
}

# Create parse threads
for host in hosts:
    thread = ParseThread(host, PORT, REALM, USERNAME, PASSWORD, options)
    threads.append(thread)
    thread.start()

# Wait all threads
for t in threads:
    t.join()

# Push results
for t in threads:
    print t.data