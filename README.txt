Sparrow
    by Alex MacCaw
    http://code.google.com/p/sparrow/

== DESCRIPTION:

  # Sparrow is a really fast lightweight queue written in Ruby that speaks memcached. 
  # That means you can use Sparrow with any memcached client library (Ruby or otherwise). 
  # 
  # Basic tests shows that Sparrow processes messages at a rate of 850-900 per second. 
  # The load Sparrow can cope with increases exponentially as you add to the cluster. 
  # Sparrow also takes advantage of eventmachine, which uses a non-blocking io, offering great performance.
  # 
  # Sparrow comes with built in support for daemonization and clustering. 
  # Also included are example libraries and clients. For example:
  # 
  # require 'memcache'
  # m = MemCache.new('127.0.0.1:11212')
  # m['queue_name'] = '1' # Publish to queue
  # m['queue_name']       #=> 1 Pull next msg from queue
  # m['queue_name']       #=> nil
  # m.delete('queue_name) # Delete queue
  # 
  # # or using the included client:
  # 
  # class MyQueue < MQ3::Queue
  #   def on_message
  #     logger.info "Received msg with args: #{args.inspect}"
  #   end
  # end
  # 
  # MyQueue.servers = [
  #   MQ3::Protocols::Memcache.new({:host => '127.0.0.1', :port => 11212, :weight => 1})
  # ]
  # MyQueue.publish('test msg')
  # MyQueue.run
  # 
  # Messages are deleted as soon as they're read and the order you add messages to the queue probably won't 
  # be the same order when they're removed.
  # 
  # Additional memcached commands that are supported are:
  # flush_all # Deletes all queues
  # version
  # quit
  # The memcached commands 'add', and 'replace' just call 'set'.
  # 
  # Command line options are:
  # -p, --port [number]              Specify port                   # Defaults to 11212
  # -c, --cluster [cluster size]     Create a cluster of daemons  
  # -h, --host [string]              Specify host                   # Defaults to '0.0.0.0'
  # -l, --debug                      Run in debug mode
  # -d, --daemon                     Daemonize mode
  # -k, --kill [<name>/all]          Kill specified running daemons # Defaults to '*' (all daemons)
  # 
  # For example, creating a cluster of three daemons running on consecutive ports (starting on 11212) is as easy as this:
  # ./sparrow -c 3
  # 
  # The daemonization won't work on Windows. 
  # 
  # Check out the code:
  # svn checkout http://sparrow.googlecode.com/svn/trunk/ sparrow
  # 
  # Sparrow was inspired by Twitter's Starling

== REQUIREMENTS:

* eventmachine

== INSTALL:

* sudo gem install sparrow

== LICENSE:

(The MIT License)

Copyright (c) 2008 FIX

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
