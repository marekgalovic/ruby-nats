require "bundler/setup"
require "ruby_nats"
require 'securerandom'

node_id = SecureRandom.hex(5)

conn = RubyNats::Connection.new(brokers: ['nats://127.0.0.1:4222'], ssl_required: true)
conn.start do |connection|
  connection.subscribe('services.service_a', group: 'service_a') do |message, reply_to|
    connection.publish(reply_to, "#{message}, node: #{node_id}")
  end
end