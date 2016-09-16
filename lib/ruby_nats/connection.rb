require 'socket'
require 'uri'
require 'json'
require 'securerandom'

module RubyNats
  class Connection
    attr_reader :uri, :options, :current_connection

    PROTOCOL_VERSION = 1

    DEFAULT_URI = 'nats://127.0.0.1:4222'.freeze

    CONNECTED_PATTERN = /INFO (?<details>.+)/
    MSG_PATTERN = /MSG\s+(?<queue>[^\s]+)\s+(?<sid>[^\s]+)\s+(?<reply_to>([^\s]+)[^\S\r\n]+)?(?<bytes>\d+)/

    def initialize(opts = {})
      @callbacks = {}
      @options = {}
      @ssid, @subscribers = 1, {}
      @options[:uris] = opts.fetch(:uris, ENV.fetch('NATS_URIS', DEFAULT_URI).split(','))
      @options[:verbose] = opts.fetch(:verbose, ENV.fetch('NATS_VERBOSE', 'false').downcase == 'true')
      @options[:pedantic] = opts.fetch(:pedantic, ENV.fetch('NATS_PEDANTIC', 'false').downcase == 'true')
    end

    def start(&connected)
      callback(:on_connect, connected)
      create_socket(uri_to_connect)
      new_listener_thread
    end

    def stop
      @socket.close
    end

    def subscribe(subject, group: nil, &callback)
      sid = (@ssid += 1)
      subscriber(sid, callback)
      send("SUB #{subject} #{group} #{sid}")
      sid
    end

    def publish(subject, message, reply_to: nil)
      send("PUB #{subject} #{reply_to} #{message.bytesize}\r\n#{message}\r")
    end

    def request(subject, message, &callback)
      inbox = inbox_hash
      response_subscriber = subscribe(inbox) do |response|
        callback.call(response)
      end
      publish(subject, message, reply_to: inbox)
      response_subscriber
    end

    private

    def uri_to_connect
      URI(@options[:uris].first)
    end

    def create_socket(current_connection_uri)
      @uri = current_connection_uri
      @socket = TCPSocket.new(uri.host, uri.port)
    end

    def send(command)
      @socket.puts(command)
    end

    def receive
      @socket.gets
    end

    def receive_loop
      while message = receive
        yield(message)
      end
    end

    def callback(key, callable = nil)
      return @callbacks[key] if callable == nil
      @callbacks[key] = callable
    end

    def subscriber(sid, callable = nil)
      return @subscribers[sid.to_s] if callable == nil
      @subscribers[sid.to_s] = callable
    end

    def inbox_hash
      "_INBOX.#{SecureRandom.hex(13)}"
    end

    def set_current_connection_details(line)
      @current_connection = JSON.parse(line).map { |k, v| [k.to_sym, v] }.to_h
    end

    def send_connection_response
      connection_response = {
        verbose: options[:verbose],
        pedantic: options[:pedantic],
        ssl_required: false,
        name: '',
        lang: :ruby,
        version: RubyNats::VERSION,
        protocol: PROTOCOL_VERSION
      }
      send("CONNECT #{connection_response.to_json}")
    end

    def new_listener_thread
      receive_loop do |line|
        if conn = CONNECTED_PATTERN.match(line)
          set_current_connection_details(conn[:details])
          send_connection_response
          callback(:on_connect).call(self)
        end
        if message = MSG_PATTERN.match(line)
          payload = receive.byteslice(0, message[:bytes].to_i)
          subscriber(message[:sid]).call(payload, message[:reply_to])
        end
      end
    end
  end
end
