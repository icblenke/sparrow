#!/usr/bin/env ruby
require 'rubygems'
require 'mq3'
require 'protocols/memcache'

class MyQueue < MQ3::Queue
end

MyQueue.servers = [
  MQ3::Protocols::Memcache.new({:host => '127.0.0.1', :port => 11212, :weight => 1})
]

CANNED_MESSAGE = <<-LOREM
  Lorem ipsum dolor sit amet, consectetuer adipiscing elit. Donec eleifend. Vivamus fringilla. Cras nunc est, laoreet mollis, sollicitudin sed, placerat ac, eros. Aenean dignissim, orci eu adipiscing scelerisque, nisl libero sollicitudin nisi, luctus varius lectus orci at justo. Etiam placerat augue et metus.
LOREM

loop do 
  @num ||= 0
  @num += 1
  puts @num
  MyQueue.publish(CANNED_MESSAGE)
end