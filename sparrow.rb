#!/usr/local/bin/ruby

# Copyright (c) 2007 Alexander MacCaw (info@eribium.org, www.eribium.org)
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# 
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# =DESCRIPTION
# Sparrow is a super fast and scalable ruby queue that speaks memcached (get, set)

# Options available:
# -p, --port [number]              Specify Port                   # Defaults to 11211
# -c, --cluster [cluster size]     Create a cluster or daemons  
# -h, --host [string]              Specify Host                   # Defaults to '0.0.0.0'
# -t, --cleanup-timer [seconds]    Specify Cleanup Timer          # Defaults to 30 seconds
# -l, --debug                      Run in debug mode
# -d, --daemon                     Daemonize mode
# -k, --kill [<name>/all]          Kill specified running daemons # Defaults to '*' (all daemons)

# Prerequisites:
#  - Eventmachine

# How to use with a ruby client?
# 1) Install the client
#     preferably install from http://www.deveiate.org/code/Ruby-MemCache-0.0.4.zip
#   or
#     gem install memcache-client
#   (The gem version of Ruby-MemCache is outdated)
# 2) 
#   Example client usage for a sparrow cluster (./sparrow.rb -c 3):
# 
#   require 'memcached'
#   m = MemCache.new('127.0.0.1:11211', '127.0.0.1:11212', '127.0.0.1:11213')
#   m['queue_name'] = '1' # Publish to queue
#   m['queue_name']       #=> 1 Pull next msg from queue
#   m['queue_name']       #=> nil
#   m.delete('queue_name) # Delete queue

# Write - 10000 messages send at a rate of 850-900 msgs/s
# Read  - 10000 messages read at a rate of 317 msgs/s

require 'rubygems'
require 'eventmachine'
require 'uuid'
require 'fileutils'
require 'optparse'

module Sparrow
  
  class SparrowError < StandardError #:nodoc:
  end
  
  class NoMoreMessages < SparrowError #:nodoc:
  end
  
  class ClientError < SparrowError #:nodoc:
  end
  
  class StatementInvalid < ClientError #:nodoc:
  end
  
  class InvalidBodyLength < ClientError #:nodoc:
  end
  
  BASE_DIR        = File.join(File.dirname(__FILE__), 'base')
  PROCESSING_DIR  = File.join(File.dirname(__FILE__), 'processing')
  LOG_DIR         = File.join(File.dirname(__FILE__), 'log')
  
  CR              = "\r\n"
  ERROR           = "ERROR"
  OK              = "OK"
  EOF             = "END"
               
  CLIENT_ERROR    = "CLIENT_ERROR"
  SERVER_ERROR    = "SERVER_ERROR"
               
  STORED          = "STORED"
  NOT_STORED      = "NOT_STORED"
               
  DELETED         = "DELETED"
  NOT_FOUND       = "NOT_FOUND"
  
  VALUE           = "VALUE"
  
  VERSION         = "VERSION"
    
  SET_REGEX       = /\ASET\s/i
  ADD_REGEX       = /\AADD\s/i
  REPLACE_REGEX   = /\AREPLACE\s/i
  DELETE_REGEX    = /\ADELETE\s/i
  GET_REGEX       = /\AGET\s/i
  QUIT_REGEX      = /\AQUIT/i
  FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
  VERSION_REGEX   = /\AVERSION/i

  def post_init
    @data = ""
    @current_queue = nil
    @expecting_body = false
    @expected_bytes = 0
    logger.debug "New client"
  end
  
  def receive_data ln
    logger.debug "Receiving data: #{ln}"
    if ln[-2..-1].include?(CR)
      ln.split(CR).each do |ln|
        @data << ln
        if ln =~ SET_REGEX
          set_command
        elsif ln =~ ADD_REGEX
          add_command
        elsif ln =~ REPLACE_REGEX
          replace_command
        elsif ln =~ GET_REGEX
          get_command
        elsif ln =~ DELETE_REGEX
          delete_command
        elsif ln =~ QUIT_REGEX
          quit_command
        elsif ln =~ VERSION_REGEX
          version_command
        elsif ln =~ FLUSH_ALL_REGEX
          flush_all_command
        elsif @expecting_body
          process_body
        else
          raise StatementInvalid
        end
        @data = ''
        @split_args = nil
      end
    else
      raise StatementInvalid
    end

  rescue ClientError => e
    logger.error e
    publish [CLIENT_ERROR, e]
    publish ERROR
  rescue => e
    logger.error e
    publish [SERVER_ERROR, e]
  end
  
  def publish d
    send_data d.to_a.join(' ') + CR
  end
  
  # Storage commands

  # <command name> <key> <flags> <exptime> <bytes>\r\n
  def set_command
    @current_queue = args[1]
    raise ClientError unless @current_queue
    @expected_bytes = args[4].to_i || 0
    @expecting_body = true
  end
  alias add_command set_command
  alias replace_command set_command
  
  def process_body
    if @data.length != @expected_bytes
     raise InvalidBodyLength
    end
    logger.debug "Adding message to queue - #{@current_queue}"
    add_message(@current_queue, @data)
    @expected_bytes = 0
    @current_queue = nil
    @expecting_body = false
    publish STORED
  end

  # Retrieval commands
  
  # GET <key>*r\n
  def get_command
    args.shift # get rid of the command
    raise ClientError if args.empty?
    args.each do |queue|
      begin
        logger.debug "Getting message from queue - #{queue}"
        msg = next_message(queue)
      rescue NoMoreMessages
        next
      end
      publish [VALUE, queue, 0, msg.length]
      publish msg
    end
    publish EOF
  end
  
  # Other commands
  
  # DELETE <key> <time>\r\n
  def delete_command
    path  = File.join(BASE_DIR, args[1])
    if File.exists?(path) or !args[1]
      logger.info "Deleting queue - #{args[1]}"
      FileUtils.rm_rf path
      publish DELETED
    else
      publish NOT_FOUND
    end
  end
  
  # FLUSH_ALL
  def flush_all_command
    logger.info "Flushing all queues"
    FileUtils.rm_rf BASE_DIR
    publish OK
  end
  
  # VERSION
  def version_command
    publish [VERSION, '0.1']
  end
  
  # QUIT
  def quit_command
    logger.debug "Closing connection"
    close_connection
  end

  protected
  
  # Queue methods
  
  def next_message(queue_name)
    Dir.glob(File.join(BASE_DIR, queue_name, '*', '*', '*', '*', '*')).each do |path|
      begin
        next unless File.extname(path) == '.msg'
        mv_path = File.join(PROCESSING_DIR, new_file_name(queue_name))
        FileUtils.mkdir_p(File.dirname(mv_path))
        FileUtils.mv path, mv_path
        data = File.read(mv_path)
        FileUtils.rm mv_path
        return data
      rescue => e
        logger.debug "Error reading next message:" + e
        next 
      end
    end
    raise NoMoreMessages
  end
  
  def add_message(queue_name, msg)
    name = File.join(BASE_DIR, new_file_name(queue_name))
    FileUtils.mkdir_p(File.dirname(name))
    File.open(name, 'w+') do |file|
      file.write msg
    end
    true
  end
    
  def logger
    return @@logger if defined?(@@loggger)
    FileUtils.mkdir_p(LOG_DIR)
    @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
    @@logger.level = Logger::INFO if !SparrowRunner.debug
    @@logger
  rescue
      @@logger = Logger.new(STDOUT)
  end
  
  def self.clear!
    logger.info "Clearing queue"
    flush_all_command
    FileUtils.rm_rf PROCESSING_DIR
    true
  end
  
  def self.cleanup(queue_name = '*')
    # Is there a better way of doing this?
    Dir.glob(File.join(BASE_DIR, queue_name, '**/')).each do |dir|
      FileUtils.rmdir(dir) rescue nil
    end
    Dir.glob(File.join(PROCESSING_DIR, queue_name, '**/')).each do |dir|
      FileUtils.rmdir(dir) rescue nil
    end
  end

  private
  
  def new_file_name(queue_name)
    guid = UUID.new
    hex = Digest::MD5.hexdigest(guid.to_s)
    time = Time.now
    path = time.strftime("%Y%m%d%H%M").scan(/..../)
    File.join(queue_name, path, time.strftime("%S"), hex + '.msg')
  end
  
  def args
    @split_args ||= @data.split(' ')
  end

end

class SparrowRunner
  attr_accessor :options
  PID_DIR = File.join(File.dirname(__FILE__), 'pids')
  @@debug = false

  def initialize(ops = {})
    parse_options(ops)

    self.options[:port] ||= 11211
    self.options[:host] ||= "0.0.0.0"
    self.options[:cleanup_timer] ||= 30
    self.options[:cluster] ||= 1

    @@debug = self.options[:debug] || false

    kill_pid(options[:kill]) if self.options[:kill]
    
    if !options[:daemonize] and !(self.options[:cluster] > 1)
      start(self.options[:port], self.options)
    else
      self.options[:port].upto(self.options[:port] + self.options[:cluster] - 1) do |n|
        daemonize(n, self.options)
      end
    end
  end
  
  def self.debug
    @@debug
  end
  
  protected

  def parse_options(ops = {})
    self.options = {}
    OptionParser.new do |opts|
      opts.banner = "Usage: sparrow.rb [-p port] [-h host] [-c cleanup-timer]"

      opts.on("-p", "--port [number]", Integer, "Specify Port") do |v|
        self.options[:port] = v
      end
      
      opts.on("-c", "--cluster [cluster size]", Integer, "Create a cluster or daemons") do |v|
        self.options[:cluster] = v 
      end
  
      opts.on("-h", "--host [string]", String, "Specify Host") do |v|
        self.options[:host] = v 
      end
  
      opts.on("-t", "--cleanup-timer [seconds]", Integer, "Specify Cleanup Timer") do |v|
        self.options[:cleanup_timer] = v
      end
  
      opts.on("-l", "--debug", "Run in debug mode") do |v|
        self.options[:debug] = v
      end
  
      opts.on("-d", "--daemon", "Daemonize mode") do |v|
        self.options[:daemonize] = v
      end
  
      opts.on("-k", "--kill [<name>/all]", String, "Kill specified running daemons") do |v|
        self.options[:kill] = v || '*'
      end
    end.parse!
    self.options.merge!(ops)
    self.options
  end
    
  def start(port, options = {})
   EventMachine::run {
     EventMachine::add_periodic_timer( options[:cleanup_timer].to_i ) { Sparrow.cleanup }
     EventMachine::start_server options[:host], port.to_i, Sparrow
   }
  end
  
  private

  def store_pid(pid, port)
   FileUtils.mkdir_p(PID_DIR)
   File.open(File.join(PID_DIR, "sparrow.#{port}.pid"), 'w'){|f| f.write("#{pid}\n")}
  end
  
  def kill_pid(k)
    begin
      Dir[File.join(PID_DIR, "sparrow.#{k}.pid")].each do |f|
        puts f
        pid = IO.read(f).chomp.to_i
        Process.kill(9, pid)
        FileUtils.rm f
        puts "killed PID: #{pid}"
      end
    rescue
      puts "Failed to kill! #{k}"
    ensure  
      exit
    end
  end

  def daemonize(port, options)
   fork do
     Process.setsid
     exit if fork
     store_pid(Process.pid, port)
     Dir.chdir File.dirname(__FILE__)
     File.umask 0000
     STDIN.reopen "/dev/null"
     STDOUT.reopen "/dev/null", "a"
     STDERR.reopen STDOUT
     trap("TERM") { exit }
     start(port, options)
   end
  end
end

SparrowRunner.new