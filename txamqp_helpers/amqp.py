###
# amqp.py
# AMQPFactory based on txamqp.
#
# Dan Siemon <dan@coverfire.com>
# March 2010
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
##
from twisted.internet import reactor, defer, protocol
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import maybeDeferred

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate
from txamqp.content import Content
import txamqp


SHUTDOWN_TIMEOUT = 0.1
MESSAGE_TIMEOUT = 1

class AmqpProtocol(AMQClient):
    """The protocol is created and destroyed each time a connection is created and lost."""

    def connectionMade(self):
        """Called when a connection has been made."""
        print 'connection'
        AMQClient.connectionMade(self)

        # Flag that this protocol is not connected yet.
        self.connected = False

        # Flag that we sending message
        self._sending = False

        # Authenticate.
        deferred = self.start({"LOGIN": self.factory.user, "PASSWORD": self.factory.password})
        deferred.addCallback(self._authenticated)
        deferred.addErrback(self._authentication_failed)
        return deferred


    def _authenticated(self, ignore):
        """Called when the connection has been authenticated."""
        # Get a channel.
        d = self.channel(1)
        d.addCallback(self._got_channel)
        d.addErrback(self._got_channel_failed)

        d2 = self.channel(2)
        d2.addCallback(self._got_send_channel)
        d2.addErrback(self._got_channel_failed)

        dl = DeferredList([d, d2])
        dl.addCallback(self._all_connected)
        return dl


    def _got_channel(self, chan):
        print 'C: %r'%chan
        self.chan = chan
        def _after_qos(*args):
            print 'af args: %r'%args
        def _set_prefetch(*args):
            return self.chan.basic_qos(prefetch_count=1).addCallback(_after_qos)

        d = self.chan.channel_open()
        d.addCallback(_set_prefetch)
        d.addCallback(self._read_channel_open)
        d.addErrback(self._channel_open_failed)

    def _got_send_channel(self, chan):
        print 'C2: %r'%chan
        self.chan_send = chan
        def _call_send(_):
            # Send any messages waiting to be sent.
            self.send()
        d = self.chan_send.channel_open()
        d.addCallback(_call_send)
        d.addErrback(self._channel_open_failed)
        return d

    def _all_connected(self, dl_return):
        '''called after both channels are opened'''
        # Flag that the connection is open.
        self.connected = True

        # Fire the factory's 'initial connect' deferred if it hasn't already
        if not self.factory.initial_deferred_fired:
            self.factory.deferred.callback(self)
            self.factory.initial_deferred_fired = True


    def _read_channel_open(self, arg):
        """Called when the channel is open."""
        # Now that the channel is open add any readers the user has specified.
        for l in self.factory.read_list:
            self.setup_read(l[0], l[1], l[2])


    def read(self, exchange, routing_key, callback):
        """Add an exchange to the list of exchanges to read from."""
        if self.connected:
            # Connection is already up. Add the reader.
            self.setup_read(exchange, routing_key, callback)
        else:
            # Connection is not up. _channel_open will add the reader when the
            # connection is up.
            pass

    # Send all messages that are queued in the factory.
    @inlineCallbacks
    def send(self):
        """If connected, send all waiting messages."""
        if self.connected:
            #print 'send messages'
            while len(self.factory.queued_messages) > 0:
                m = self.factory.queued_messages.pop(0)
                self._sending = True
                yield self._send_message(m[0], m[1], m[2])
            #print 'sended'


    # Do all the work that configures a listener.
    @inlineCallbacks
    def setup_read(self, exchange, routing_key, callback):
        """This function does the work to read from an exchange."""
        queue = exchange # For now use the exchange name as the queue name.
        consumer_tag = exchange # Use the exchange name for the consumer tag for now.
        self.chan.tag = consumer_tag

        # Declare the exchange in case it doesn't exist.
        yield self.chan.exchange_declare(exchange=exchange, type="direct", durable=True, auto_delete=False)

        # Declare the queue and bind to it.
        yield self.chan.queue_declare(queue=queue, durable=True, exclusive=False, auto_delete=False)
        yield self.chan.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)

        # Consume.
        yield self.chan.basic_consume(queue=queue, no_ack=True, consumer_tag=consumer_tag)
        queue = yield self.queue(consumer_tag)

        # Now setup the readers.
        yield self.read_loop(queue, callback).addErrback(self._read_item_err)
        #d = queue.get()
        #d.addCallback(self._read_item, queue, callback)
        #d.addErrback(self._read_item_err)


    def _channel_open_failed(self, error):
        print "Channel open failed:", error


    def _got_channel_failed(self, error):
        print "Error getting channel:", error


    def _authentication_failed(self, error):
        print "AMQP authentication failed:", error


    @inlineCallbacks
    def _send_message(self, exchange, routing_key, msg):
        """Send a single message."""
        # First declare the exchange just in case it doesn't exist.
        #print 'declare'
        #yield self.chan_send.exchange_declare(exchange=exchange, type="direct", durable=True, auto_delete=False)
        #print 'declared'

        msg = Content(msg)
        msg["delivery mode"] = 2 # 2 = persistent delivery.
        d = self.chan_send.basic_publish(exchange=exchange, routing_key=routing_key, content=msg)
        d.addErrback(self._send_message_err)
        yield d
        self._sending = False


    def _send_message_err(self, error):
        print "Sending message failed", error


    @inlineCallbacks
    def read_loop(self, queue, callback):
        """Callback function which is called when an item is read."""
        # Setup another read of this queue.
        print 'read loop'
        while self.connected:
            try:
                msg = yield queue.get(MESSAGE_TIMEOUT)
                print '!get msg %r'%msg
                #d.addCallback(self._read_item, queue, callback)
                #d.addErrback(self._read_item_err)
                # Process the read item by running the callback.
                print 'CB: %r'%callback
                yield maybeDeferred(callback, msg)\
                      .addErrback(self._read_callback_err)
                print 'runned callback'
            except txamqp.queue.Empty:
                print 'cont'
                continue
            except txamqp.queue.Closed:
                print 'break'
                break

    def _read_callback_err(self, failure):
        print 'Error read_callback: ', failure


    def _read_item_err(self, error):
        print "Error reading item: ", error

    def close_connection(self, *args):
        if self._sending:
            print 'on sending'
            d = Deferred()
            d.addCallback(self.close_connection)
            reactor.callLater(SHUTDOWN_TIMEOUT, d.callback, None)
            return d
        print 'closing'
        def _close_zero_connection(chan0):
            return chan0.connection_close()
        def _get_zero_channel(_):
            return self.channel(0).addCallback(_close_zero_connection)
        def _close_chan(*args):
            return self.chan.channel_close()
        def _unsubscribe():
            if hasattr(self.chan, 'tag'):
                d = self.chan.basic_cancel(self.chan.tag)
                d.addCallback(_close_chan)
                return d
            return _close_chan()
        def _close(*args):
            self.close('Shutdown')
            return self.transport.loseConnection()
        dl = DeferredList([_unsubscribe(),
                           self.chan_send.channel_close()])
        dl.addCallback(_get_zero_channel)
        dl.addCallback(_close)
        dl.addErrback(self.close_error)
        self.connected = False
        return dl

    def close_error(self, failure):
        raise failure


class AmqpFactory(protocol.ReconnectingClientFactory):
    protocol = AmqpProtocol


    def __init__(self, spec_file=None, vhost=None, host=None, port=None, user=None, password=None):
        spec_file = spec_file or 'amqp0-8.xml'
        self.spec = txamqp.spec.load(spec_file)
        self.user = user or 'guest'
        self.password = password or 'guest'
        self.vhost = vhost or '/'
        self.host = host or 'localhost'
        self.port = port or 5672
        self.delegate = TwistedDelegate()
        self.deferred = Deferred()
        self.initial_deferred_fired = False

        self.p = None # The protocol instance.
        self.client = None # Alias for protocol instance

        self.queued_messages = [] # List of messages waiting to be sent.
        self.read_list = [] # List of queues to listen on.

        # Make the TCP connection.
        reactor.connectTCP(self.host, self.port, self)


    def buildProtocol(self, addr):
        p = self.protocol(self.delegate, self.vhost, self.spec)
        p.factory = self # Tell the protocol about this factory.

        self.p = p # Store the protocol.
        self.client = p

        # Reset the reconnection delay since we're connected now.
        self.resetDelay()

        return p


    def clientConnectionFailed(self, connector, reason):
        print "Connection failed."
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)


    def clientConnectionLost(self, connector, reason):
        print "Client connection lost."
        self.p = None

        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)


    def send_message(self, exchange=None, routing_key=None, msg=None):
        """Send a message."""
        # Add the new message to the queue.
        self.queued_messages.append((exchange, routing_key, msg))

        # This tells the protocol to send all queued messages.
        if self.p != None and self.p.connected:
            return self.p.send()


    def read(self, exchange=None, routing_key=None, callback=None):
        """Configure an exchange to be read from."""
        assert(exchange != None and routing_key != None and callback != None)

        # Add this to the read list so that we have it to re-add if we lose the connection.
        self.read_list.append((exchange, routing_key, callback))

        # Tell the protocol to read this if it is already connected.
        if self.p != None:
            self.p.read(exchange, routing_key, callback)

    def shutdown(self, *args):
        print 'len: %r'%len(self.queued_messages)
        if len(self.queued_messages) != 0:
            d = Deferred()
            d.addCallback(self.shutdown)
            reactor.callLater(SHUTDOWN_TIMEOUT, d.callback)
            return d
        return self._shutdown()

    def _shutdown(self):
        self.stopTrying()
        d = self.p.close_connection()
        #d.addErrback(self.shutdown_error)
        self.p = None
        return d

    def shutdown_error(self, failure):
        raise failure
