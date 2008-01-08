require 'mq3'
require 'protocols/memcache'
require 'benchmark'

class MyQueue < MQ3::Queue
  def on_message(msg)
    @@num ||= -1
    @@num += 1
    p @@num
  end
end

MyQueue.servers = [
  MQ3::Protocols::Memcache.new({:host => '127.0.0.1', :port => 11212, :weight => 1})
]

b =  Benchmark.measure {
  10000.times do |i|
    p i
    MyQueue.publish i
  end
}

b2 = Benchmark.measure {
  10000.times do |i|
    @@num ||= -1
    @@num += 1
    MyQueue.poll
    p "#{@@num} #{i}"
  end
}

puts b
puts
puts b2