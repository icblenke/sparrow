
# See client/mq2.rb for a better client

require 'mem_cache_queue'

class MyQueue < MemCacheQueue
  def on_message
    logger.info "Received msg with args: #{args.inspect}"
  end
  
  def delay
    2
  end
end

MemCacheQueue.connect('127.0.0.1:11211')

MyQueue.publish('test msg1')
MyQueue.publish('test msg2')
MyQueue.publish('test msg3')


MyQueue.run(true)


