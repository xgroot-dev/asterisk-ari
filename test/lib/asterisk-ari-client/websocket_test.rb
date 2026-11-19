require_relative '../../test_helper'
require 'socket'
require 'timeout'
require 'minitest/mock'

class TestWebsocket < Minitest::Test
  def setup
    @server = TCPServer.new('127.0.0.1', 0)
    @client = Ari::Client.new(
      url: "http://127.0.0.1:#{@server.addr[1]}/ari",
      api_key: 'asterisk:asterisk',
      app: 'dialplan'
    )
  end

  def teardown
    @client.ws&.close
    @server.close
  end

  # Answers the upgrade request like Asterisk; returns the server end of the websocket and the request.
  def accept_websocket
    Timeout.timeout(5) do
      socket = @server.accept
      handshake = WebSocket::Handshake::Server.new
      request = +''
      until handshake.finished?
        chunk = socket.readpartial(4096)
        request << chunk
        handshake << chunk
      end
      @protocol_version = handshake.version
      socket.write handshake.to_s
      [socket, request]
    end
  end

  # Queue#pop takes a timeout only from Ruby 3.2 on.
  def wait_for(queue)
    Timeout.timeout(5) { queue.pop }
  end

  def text_frame(data)
    WebSocket::Frame::Outgoing::Server.new(version: @protocol_version, data: data, type: :text).to_s
  end

  def test_connect_opens_the_events_websocket_and_passes_events_on
    opened = Queue.new
    calls = Queue.new
    @client.on(:websocket_open) { opened << ws }
    @client.on(:stasis_start) { |event| calls << event.args }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    socket, request = server.value

    assert_same @client.ws, wait_for(opened), 'websocket_open ran before #ws was the new websocket'
    assert @client.ws.open?
    assert_equal 'GET /ari/events?api_key=asterisk%3Aasterisk&app=dialplan HTTP/1.1', request.lines.first.chomp

    socket.write text_frame(MultiJson.dump(type: 'StasisStart', args: ['in'], channel: { id: '1794990944.1' }))
    assert_equal ['in'], wait_for(calls)
  end

  def test_reconnect_closes_the_previous_websocket_quietly
    closed = false
    opened = Queue.new
    @client.on(:websocket_close) { closed = true }
    @client.on(:websocket_open) { opened << true }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    previous, = server.value
    assert wait_for(opened)

    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value
    Timeout.timeout(5) { previous.read }

    assert wait_for(opened)
    assert @client.ws.open?
    refute closed
  end

  # websocket-client-simple starts its reader thread before it writes the upgrade request.
  def test_failed_connect_closes_what_it_opened
    closed = false
    @client.on(:websocket_close) { closed = true }
    handshake = Object.new
    def handshake.to_s
      raise Errno::EPIPE
    end
    threads = Thread.list

    WebSocket::Handshake::Client.stub(:new, handshake) do
      assert_raises(Errno::EPIPE) { @client.connect_websocket }
    end
    accepted = @server.accept

    assert_nil @client.ws
    refute closed, 'a failed attempt reports a closed websocket'
    assert accepted.wait_readable(5), 'the socket of the failed attempt stays open'
    assert_nil accepted.read(1)
    (Thread.list - threads).each { |thread| thread.join(5) }
    assert_empty((Thread.list - threads).select(&:alive?))
  end

  def test_reconnect_does_not_wait_for_a_close_that_hangs
    opened = Queue.new
    release = Queue.new
    @client.on(:websocket_open) { opened << true }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value
    assert wait_for(opened)
    previous = @client.ws
    previous.define_singleton_method(:close) { release.pop }

    server = Thread.new { accept_websocket }
    Timeout.timeout(5) { @client.connect_websocket }
    server.value

    assert wait_for(opened)
    refute_same previous, @client.ws
    assert @client.ws.open?
  ensure
    release << true
  end

  def test_failed_reconnect_leaves_no_websocket
    opened = Queue.new
    @client.on(:websocket_open) { opened << true }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value
    assert wait_for(opened)
    previous = @client.ws
    @server.close

    assert_raises(Errno::ECONNREFUSED) { @client.connect_websocket }

    assert_nil @client.ws
    refute previous.open?
  ensure
    @server = TCPServer.new('127.0.0.1', 0)
  end

  def test_reconnect_goes_on_when_the_close_of_the_previous_websocket_raises
    opened = Queue.new
    @client.on(:websocket_open) { opened << true }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value
    assert wait_for(opened)
    previous = @client.ws
    # The write of the close frame finds the connection reset.
    previous.define_singleton_method(:send) { |*| raise Errno::ECONNRESET }

    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value

    assert wait_for(opened)
    refute_same previous, @client.ws
    assert @client.ws.open?
    previous.thread.join(5)
    refute previous.thread.alive?
  end

  def test_unexpected_error_of_a_close_still_ends_the_reader_thread
    opened = Queue.new
    @client.on(:websocket_open) { opened << true }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value
    assert wait_for(opened)
    previous = @client.ws
    previous.define_singleton_method(:close) { raise ArgumentError, 'unexpected' }

    assert_raises(ArgumentError) { @client.connect_websocket }

    assert_nil @client.ws
    previous.thread.join(5)
    refute previous.thread.alive?
  end

  def test_close_that_hangs_is_ended_with_the_reader_thread
    opened = Queue.new
    release = Queue.new
    @client.on(:websocket_open) { opened << true }
    server = Thread.new { accept_websocket }
    @client.connect_websocket
    server.value
    assert wait_for(opened)
    previous = @client.ws
    previous.define_singleton_method(:close) { release.pop }
    @server.close
    Timeout.timeout(5) { nil } # the timeout's own thread starts with its first use
    threads = Thread.list

    assert_raises(Errno::ECONNREFUSED) { Timeout.timeout(5) { @client.connect_websocket } }

    assert_nil @client.ws
    (Thread.list - threads + [previous.thread]).each { |thread| thread.join(5) }
    refute previous.thread.alive?
    assert_empty((Thread.list - threads).select(&:alive?))
  ensure
    3.times { release << true }
    @server = TCPServer.new('127.0.0.1', 0)
  end
end
