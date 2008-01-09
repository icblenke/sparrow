#!/usr/bin/env ruby
require 'rubygems'
require 'mq3'
require 'protocols/memcache'

class MyQueue < MQ3::Queue
  def on_message(msg)
    @@num ||= 0
    @@num += 1
    puts @@num
  end
end

MyQueue.servers = [
  MQ3::Protocols::Memcache.new({:host => '127.0.0.1', :port => 11212, :weight => 1})
]

MyQueue.run