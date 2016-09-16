require 'securerandom'
require 'socket'
require 'json'
require 'uri'

module RubyNats
  class Connection
    class ConnectionFailedError < StandardError; end
    class CertificateError < StandardError; end

    attr_reader :uri, :options, :details

    PROTOCOL_VERSION = 1

    DEFAULT_URI = 'nats://127.0.0.1:4222'.freeze
    MAX_RECONNECT_ATTEMPTS = 10
    RECONNECT_TIME_WAIT = 2

    CONNECTED_PATTERN = /INFO (?<details>.+)/
    MSG_PATTERN = /MSG\s+(?<queue>[^\s]+)\s+(?<sid>[^\s]+)\s+(?<reply_to>([^\s]+)[^\S\r\n]+)?(?<bytes>\d+)/
    PING_PATTERN = /PING.+/

    def initialize(opts = {})
      @callbacks = {}
      @options = {}
      @ssid, @subscribers = 1, {}
      @reconnect_attempts = Hash.new(0)

      @socket_mutex = Mutex.new
      @subscribers_mutex = Mutex.new

      @options[:uris] = opts.fetch(:uris, ENV.fetch('NATS_URIS', DEFAULT_URI).split(','))
      @options[:verbose] = opts.fetch(:verbose, ENV.fetch('NATS_VERBOSE', 'false').downcase == 'true')
      @options[:pedantic] = opts.fetch(:pedantic, ENV.fetch('NATS_PEDANTIC', 'false').downcase == 'true')
      @options[:ssl_required] = opts.fetch(:ssl_required, ENV.fetch('NATS_SSL_REQUIRED', 'false').downcase == 'true')
      @options[:name] = opts.fetch(:name, ENV.fetch('NATS_NAME', ''))
      @options[:max_reconnect_attempts] = opts.fetch(:max_reconnect_attempts, ENV.fetch('MAX_RECONNECT_ATTEMPTS', MAX_RECONNECT_ATTEMPTS)).to_i
      @options[:reconnect_time_wait] = opts.fetch(:reconnect_time_wait, ENV.fetch('RECONNECT_TIME_WAIT', RECONNECT_TIME_WAIT)).to_i
      @options[:user] = opts.fetch(:user, ENV.fetch('NATS_USER', ''))
      @options[:pass] = opts.fetch(:pass, ENV.fetch('NATS_PASS', ''))
      @options[:ping_interval] = opts.fetch(:ping_interval, ENV.fetch('NATS_PING_INTERVAL', '')).to_i
      @options[:max_outstanding_pings] = opts.fetch(:max_outstanding_pings, ENV.fetch('NATS_MAX_OUTSTANDING_PINGS', '')).to_i

      # encrypted connection
      if @options[:tls]
        case
        when @options[:tls][:ca_file]
          if !File.readable?(@options[:tls][:ca_file])
            raise CertificateError, "TLS verification is enabled but ca_file #{@options[:tls][:ca_file]} is not readable."
          end
          @options[:tls][:verify_peer] = true
        when @options[:tls][:verify_peer] && !@options[:tls][:ca_file]
          raise CertificateError, "TLS verification is enabled but no ca_file is set."
        else
          @options[:tls][:verify_peer] = false
        end
      end
    end

    def start
      create_socket(uri_to_connect)
      start_reactor
      yield(self)
      @reactor.join
    end

    def stop
      callback(:on_close).call
      @reactor.exit if @reactor.alive?
      @socket.close unless @socket.closed?
    end

    def on_connect(&callback)
      callback(:on_connect, callback)
    end

    def on_reconnect(&callback)
      callback(:on_reconnect, callback)
    end

    def on_close(&callback)
      callback(:on_close, callback)
    end

    def on_error(&callback)
      callback(:on_error, callback)
    end

    def subscribe(subject, group: nil, max_messages: nil, &callback)
      sid = (@ssid += 1)
      subscriber(sid, callback, max_messages: max_messages)
      send("SUB #{subject} #{group} #{sid}")
      unsubscribe(sid, max_messages) if max_messages != nil
      sid
    end

    def unsubscribe(sid, max_messages=nil)
      send("UNSUB #{sid} #{max_messages}")
    end

    def publish(subject, message, reply_to: nil)
      send("PUB #{subject} #{reply_to} #{message.bytesize}\r\n#{message}\r")
    end

    def request(subject, message, max_messages: 1, &callback)
      inbox = inbox_hash
      response_subscriber_id = subscribe(inbox, max_messages: max_messages) do |response|
        callback.call(response)
      end
      publish(subject, message, reply_to: inbox)
      response_subscriber_id
    end

    private

    def start_reactor
      @reactor = Thread.new { listen }
      @reactor.abort_on_exception = true
    end

    def uri_to_connect
      URI(@options[:uris].shuffle.first)
    end

    def create_socket(current_connection_uri)
      @uri = current_connection_uri
      @socket = TCPSocket.new(uri.host, uri.port)
      initialize_connection

      # connection is successful at this point
      @reconnect_attempts[@uri] = 0
      callback(:on_connect).call(@uri)
    rescue Errno::ECONNREFUSED
      return try_reconnect if @reconnect_attempts[@uri] < @options[:max_reconnect_attempts]
      raise ConnectionFailedError, "Unable to connect to: #{@uri}"
    end

    def try_reconnect
      new_uri = uri_to_connect
      @reconnect_attempts[@uri] += 1
      sleep(RECONNECT_TIME_WAIT)

      callback(:on_reconnect).call(new_uri)
      create_socket(new_uri)
      listen
    end

    def send(command)
      @socket_mutex.synchronize do
        @socket.puts(command) if connected?
      end
    end

    def receive
      @socket.gets
    end

    def connected?
      @socket && !@socket.closed?
    end

    def receive_loop
      while message = receive
        yield(message)
      end
      try_reconnect
    end

    def callback(key, callable = nil)
      return @callbacks[key] || Proc.new {} if callable == nil
      @callbacks[key] = callable
    end

    def subscriber(sid, callable = nil, max_messages: nil)
      @subscribers_mutex.synchronize do
        if callable == nil
          sub = @subscribers[sid.to_s]
          if (sub[:subscribed] += 1) >= sub[:max_messages].to_i && sub[:max_messages] != nil
            @subscribers.delete(sid.to_s)
          end
          return sub[:callable]
        end
        @subscribers[sid.to_s] = {subscribed: 0, max_messages: max_messages, callable: callable}
      end
    end

    def inbox_hash
      "_INBOX.#{SecureRandom.hex(13)}"
    end

    def set_current_connection_details(line)
      @details = JSON.parse(line, symbolize_keys: true, symbolize_names: true, symbol_keys: true)
    end

    def auth_required?
      details[:auth_required]
    end

    def send_connection_response
      connection_response = {
        verbose: options[:verbose],
        pedantic: options[:pedantic],
        ssl_required: options[:ssl_required],
        name: options[:name],
        lang: :ruby,
        version: RubyNats::VERSION,
        protocol: PROTOCOL_VERSION
      }
      if auth_required?
        connection_response[:user] = options[:user]
        connection_response[:pass] = options[:pass]
      end
      send("CONNECT #{connection_response.to_json}")
    end

    def initialize_connection
      while connection_info = CONNECTED_PATTERN.match(receive)
        set_current_connection_details(connection_info[:details])
        break
      end
      send_connection_response
    end

    def listen
      receive_loop do |line|
        if message = MSG_PATTERN.match(line)
          payload = receive.byteslice(0, message[:bytes].to_i)
          subscriber(message[:sid]).call(payload, message[:reply_to])
        end
        if ping = PING_PATTERN.match(line)
          send('PONG')
        end
      end
    end
  end
end
