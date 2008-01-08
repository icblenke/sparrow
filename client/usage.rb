require 'mq3'
require 'protocols/memcache'

class MyQueue < MQ3::Queue
  def on_message(msg)
    @@num ||= -1
    @@num += 1
    p @@num
  end
end

MyQueue.servers = [MQ3::Protocols::Memcache.new({:host => '127.0.0.1', :port => 11212, :weight => 1})]

1000.times do |i|
  p i
  MyQueue.publish i
end

MyQueue.run
