require "bundler/setup"
require "ruby_nats"

conn = RubyNats::Connection.new(brokers: ['nats://127.0.0.1:4222', 'nats://127.0.0.1:4222'])
conn.on_connect do |uri|
  puts "Connected: #{uri}"
end
conn.on_reconnect do |uri|
  puts "Reconnect attempt: #{uri}"
end
conn.on_close do
  puts "closing"
end

def request(connection)
  m = Mutex.new
  cv = ConditionVariable.new
  at = Time.now.utc
  connection.request('services.service_a', '') do |response|
    m.synchronize{ cv.signal }
  end
  m.synchronize { cv.wait(m) }
  puts (Time.now.utc - at)*1000
end

conn.start do |connection|
  response_times = []
  10.times do
    request(connection)
  end
  connection.stop
end
