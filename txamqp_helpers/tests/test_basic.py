
import os

from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.trial.unittest import TestCase
from amqp import AmqpFactory
from amqp import AmqpProtocol


class BasicAmqTest(TestCase):

    def setUp(self):
        self.f = AmqpFactory(spec_file='file:../tests/amqp0-8.xml')

    def test_001_connect(self):
        return self.f.deferred.addCallback(lambda _:self.f.stopFactory())

    def test_002_basic_send(self):
        def _send_message(par):
            print 'send message'
            return self.f.send_message('test_exchange', 'route', 'msg')
        return self.f.deferred.addCallback(_send_message)

    def test_002_basic_send_second(self):
        def _send_message(par):
            return self.f.send_message('test_exchange', 'route', 'msg')
        for i in xrange(1000):
            self.f.deferred.addCallback(_send_message)
        return self.f.deferred

    def test_003_basic_receive(self):
        def _send_message(par):
            print 'send message'
            return self.f.send_message('test_exchange', 'route', 'msg')
        d2 = Deferred()
        def _receive_message(mess):
            print 'received %r'%mess
            assert mess.content.body == 'msg', mess
            if not d2.called:
                d2.callback(None)
            print 'all done'
        d = self.f.deferred.addCallback(_send_message)
        self.f.read('test_exchange', 'route', _receive_message)
        return DeferredList([d, d2])
        

    def tearDown(self):
        print 'shutdown'
        
        return self.f.shutdown()
