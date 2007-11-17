require 'eventmachine'
require 'socket'
require 'sqs'
require 'active_support'

RAILS_ROOT = File.join(File.dirname(__FILE__), '..') unless defined?(RAILS_ROOT)
RAILS_DEFAULT_LOGGER = Logger.new('log/mq2.log') unless defined?(RAILS_DEFAULT_LOGGER)
# This lib was originally designed for Rails


module EventMachine
  
  module Deferrable
    
    def set_deferred_status_without_popping_callbacks status, *args
      cancel_timeout
      @deferred_status = status
      @deferred_args = args
      case @deferred_status
      when :succeeded
        if @callbacks
          @callbacks.each do |cb|
            cb.call(*@deferred_args)
          end
        end
        @errbacks.clear if @errbacks
      when :failed
        if @errbacks
          while eb = @errbacks.pop
            eb.call(*@deferred_args)
          end
        end
        @callbacks.clear if @callbacks
      end
    end
    
  end
  
  # We want to connect from inside instances 
  # (so when don't have to copy loads of server instances)
  def EventMachine::connect_from_instance server, port, handler
    s = connect_server server, port
    handler.signature = s
    @conns[s] = handler
    block_given? and yield handler
    handler
  end
  
  module Protocols    
    class Memcache < Connection
      include EventMachine::Deferrable
    
      class MemcacheError < StandardError #:nodoc:
      end
  
      class ClientError < MemcacheError #:nodoc:
      end
  
      class ServerError < MemcacheError #:nodoc:
      end
      
      class ConnectionError < MemcacheError #:nodoc:
      end
  
      LOG_DIR             = File.join(RAILS_ROOT, 'log')
  
      CR                  = "\r\n"     
      ERROR               = "ERROR"    
                                       
      GET                 = "GET"      
      SET                 = "SET"      
      DELETE              = "DELETE"   
      FLUSH_ALL           = "FLUSH_ALL"
      VERSION             = "VERSION"  
            
      CLIENT_ERROR_REGEX  = /\ACLIENT_ERROR\s/i
      SERVER_ERROR_REGEX  = /\ASERVER_ERROR\s/i
      ERROR_REGEX         = /\AERROR\s/i
               
  
      attr_reader :options
      attr_reader :connected
      attr_reader :retry
      attr_reader :expects_proc
      attr_reader :status
      
      attr_accessor :fire_and_forget
      
      def connect!
        # We have to call the custom method 'connect_from_instance' since we want to keep the current instance
        return if connected?
        reconnect! and return if been_connected?
        EventMachine::connect_from_instance(@options[:host], @options[:port], self) {|c|
          c.instance_eval {
            @connected = true
            @status = 'connected'
          }
        }
      # rescue => e
      #   mark_dead e
      end
      
      def reconnect!
        return if connected?
        connect! and return unless been_connected?
        EventMachine::reconnect(@options[:host], @options[:port], self) {|c|
          c.instance_eval {
            @connected = true
            @status = 'connected'
          }
        }
      # rescue => e
      #   mark_dead e
      end
      
      def initialize(options = {})
        @options = options
        @data = ''
        @expects_proc = []
      end
      
      def receive_data ln
        exp = @expects_proc.shift
        return unless exp
        exp.call(ln)
        @data = ''
      end
      
      def [](q)
        send_msg GET, q
        expects q do |q, body|
          body.split(CR).each do |d|
            next if d =~ /^VALUE (.+) (.+) (.+)/
            next if d =~ /\AEND/i
            set_deferred_status_without_popping_callbacks :succeeded, q, d
          end
        end
      end
  
      def []=(q, msg)
        send_msg SET, q, 0, 0, msg.length, CR + msg
        # expects q do |q, body|
        #   return true if body =~ /\ASTORED/i
        #   return false if body =~ /\ANOT_STORED/i
        #   # This doesn't actually go anywhere...
        # end
        msg
      end
  
      def delete(q)
        send_msg DELETE, q
        # expects q do |q, body| 
        #   return true if body =~ /\ADELETED/i
        #   return false if body =~ /\ANOT_FOUND/i
        #   # Catch for something else?
        # end
      end
  
      def delete!
        send_msg FLUSH_ALL
        # expects q do |q, body|
        #   return false unless body =~ /\AOK/i
        #   true
        # end
      end
      
      def inspect
  			return "<EventMachine::Protocols::Memcached %s:%d [%d] (%s)>" % [
  				@options[:host],
  				@options[:port],
  				weight,
  				@connected ? 'connected' : (@status || 'not connected')
  			]
  		end
  		
  		def weight
  		  @options[:weight] || 0
  		end
  		
  		def unbind
    	  mark_dead('Unbind called')
    	end
    	
    	def been_connected?
    	 self.signature ? true : false
    	end
    	
    	def alive?
        (!@retry or @retry < Time.now)
      end
      
      def connected?
        @connected == true
      end
      
      def retry?
        ((@retry and @retry < Time.now) and !@connected) ? true : false
      end
      
      def mark_dead( reason="Unknown error" )
        @connected = false
        @tcp_socket.close if @tcp_socket && !@tcp_socket.closed?
        @tcp_socket = nil
        @retry = Time::now + ( 30 + rand(10) )
  			@status = "DEAD: %s: Will reincarnate at %s" %
  				[ reason, @retry ]
  		end
      
  		
      # private
    
      def send_msg(*msg)
        if self.fire_and_forget
          if !@tcp_socket
            # We use a tcpsocket since this fails if you try to write
            # to it if the socket isn't open. Eventmachine buffers
            # send_msg, so doesn't fails. It also doesn't call unbind
            # for a couple of seconds after failing - potentially
            # losing messages
            @tcp_socket = TCPSocket.open(options[:host], options[:port])
            # Speed up socket...
            if Socket.constants.include? 'TCP_NODELAY' then
              @tcp_socket.setsockopt Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1
            end
          end
          @tcp_socket.print(msg.join(' ') + CR)
          @tcp_socket.flush
        else
          raise 'Attempted write to a non-connected server' unless connected?
          send_data msg.join(' ') + CR
        end
      rescue => e
        mark_dead(e)
        raise ConnectionError
      ensure
        self.fire_and_forget = false
      end
      
      def expects(q, &block)
        @expects_proc ||= []
        @expects_proc << Proc.new { |body|
          yield(q, body)
        }
      end
  		
    end

    # Call it something other than SQS so it doesn't conflict
    class SQSWrapper
      include EventMachine::Deferrable
      attr_reader :options
      attr_accessor :fire_and_forget
  
      class SQSError < StandardError #:nodoc:
      end
  
      class ConnectionError < SQSError #:nodoc:
      end
      
      
      # To mirror the existing api
      def connect!
      end
      
      def reconnect!
      end
      
      def connected?
        true
      end
      
      def alive?
        true # Presume internet access
      end
      
      def retry?
        false
      end
      
      def initialize(options = {})
        @options = options
      end

      def []=(q, msg)
        puts 'Writing to sqs'
        queue(q).send_message(msg)
        msg
      rescue => e
        raise ConnectionError
      end
      
      def inspect
  			return "<EventMachine::Protocols::SQSWrapper [%d] (connected)>" % [
  			  weight
			  ]
      end

      def delete(q)
        queue(q).delete!
      rescue => e
      end

      def delete!
        SQS.each_queue do |q|
          q.delete!
        end
      rescue => e
      end
  
      def [](q)
        msg = queue(q).receive_message
        set_deferred_status_without_popping_callbacks :succeeded, q, msg.body if msg
      rescue => e
        raise ConnectionError
      end
      
      def weight
        @options[:weight] || 0
      end
  
      private

      def queue(q)
        @@queues ||= {}
        @@queues[q] ||=
          begin
            SQS.get_queue(q)
          rescue SQS::UnavailableQueue
            SQS.create_queue(q)
          end
      end

    end

  end
end
  
class MQ2
  
  class MQ2Error < StandardError #:nodoc:
  end

  class NoServersLeft < MQ2Error #:nodoc:
  end
  
  class ConnectionError < MQ2Error #:nodoc:
  end
  
  PID_DIR = "#{RAILS_ROOT}/tmp/pids"
  
  class << self
    
    def inherited(klass)
      @@processors ||= []
      @@processors << klass
    end
    
    def processors
      if !defined?(@@processors) or !@@processors
        # Force loading, better way of doing this?
        Dir[File.join(RAILS_ROOT, 'app', 'processors') + '/*.rb'].each {|r| File.basename(r, '.*').camelize.constantize }
      end
      @@processors
    end
    
    def run_all
      processors.each do |p|
        p.run(true)
      end
      true
    end
    
    def run(use_daemonize = false)
      puts "Starting #{queue_name} up..."
      could_daemonize(use_daemonize) {
        EventMachine::run {
          @@servers.each do |server|
            server.connect!
          end
          @@servers.each do |server|
            server.callback {|queue, response|
              process(response) if queue == queue_name
            }
          end
          EventMachine::add_periodic_timer(delay){
            poll
          }
        }
      }
    end

    def publish(msg)
      send_to_server {|server| 
        connect_server(server){
          server[queue_name] = msg.to_yaml
        }
      }
      msg
    end
    
    def destroy!
      processed_servers.each {|e| connect_server(e){ e.delete(queue_name) } }
    end
    
    def destroy_all!
      processed_servers.each {|e| connect_server(e){ e.delete! } }
    end
    
    def poll
      # assume connected
      send_to_server(true) {|server|
        server.reconnect! unless server.connected?
        server[queue_name]
      }
    end
    
    def process(msg)
      self.new.process(msg)
    end
    
    def queue_name
      self.name.underscore
    end
    
    def delay
      1
    end
    
    def inspect
			return "<MQ2 %s (%s/%s)>" % [
				queue_name,
				processed_servers.length,
				servers.length
			]
		end
    
    # private
    
    def processed_servers
      @@servers ||= []
      @@servers.select(&:alive?).sort_by{ rand }.sort_by(&:weight).reverse
    end
    
    def servers
      @@servers ||= []
    end
    
    def servers= srvs
      @@servers = srvs
    end
    
    def connect_server(server, &block)
      # Make sure uses TCPSocket instead of eventmachine
      server.fire_and_forget = true
      yield
    end
    
    def send_to_server(random = false, &block)
      processed_servers.sort_by { random ? rand : 1 }.each do |server|
        begin
          return yield(server)
        rescue EventMachine::Protocols::Memcache::ConnectionError
          # Server will be marked dead
          next
        end
      end
      raise NoServersLeft
    end
    
    def store_pid(pid)
     FileUtils.mkdir_p(PID_DIR)
     File.open(File.join(PID_DIR, "#{queue_name}.#{pid}.pid"), 'w'){|f| f.write("#{pid}\n")}
    end

    def kill_pid(k, name = queue_name)
      Dir[File.join(PID_DIR, "#{name}.#{k}.pid")].each do |f|
        begin
          puts f
          pid = IO.read(f).chomp.to_i
          FileUtils.rm f
          Process.kill(9, pid)
          puts "killed PID: #{pid}"
        rescue => e
          puts "Failed to kill! #{f}: #{e}"
        end
      end
      true
    end
    alias kill! kill_pid
    
    def kill_all!
      kill_pid('*', '*')
    end
    
    def could_daemonize(use_daemonize, &block)
      return yield unless use_daemonize
      fork do
        Process.setsid
        exit if fork
        store_pid(Process.pid)
        Dir.chdir File.dirname(__FILE__)
        File.umask 0000
        STDIN.reopen "/dev/null"
        STDOUT.reopen "/dev/null", "a"
        STDERR.reopen STDOUT
        trap("TERM") { exit }
        yield
      end
    end
    
  end
  
  def process(msg)
    1.upto(retry_attempts + 1) do |n|
      begin
        self.args = YAML.load(msg)
        on_message
        break
      rescue => e
        if n == retry_attempts + 1
          logger.fatal "Permanently failed: " + e
        else
          logger.error "Retry number #{n}: " + e
        end
      end
    end
  end
  
  def logger
    RAILS_DEFAULT_LOGGER
  end
  
  def retry_attempts
    0
  end
  
  attr_accessor :args
  def on_message
    raise 'You must implement on_message.'
  end
  
  # private
  # 
  # def fix_loggers
  #   ActiveRecord::Base.logger = logger
  #   ActionMailer::Base.logger = logger
  # end
  
end

# class MyProcessor < MQ2
#   def on_message
#     puts 'On message: ' + self.args.inspect
#   end
# end

# SQS.access_key_id = 'YOURACCESSKEYID'
# SQS.secret_access_key = 'YOURSECRETACCESSKEY'
   
# servers = [
#   EventMachine::Protocols::Memcache.new({:host => 'localhost', :port => 11211, :weight => 1}),
#   EventMachine::Protocols::Memcache.new({:host => 'localhost', :port => 11212, :weight => 1}),
#   EventMachine::Protocols::Memcache.new({:host => 'localhost', :port => 11213, :weight => 1}),
#   EventMachine::Protocols::Memcache.new({:host => 'localhost', :port => 11214, :weight => 1}),
#   EventMachine::Protocols::SQSWrapper.new({:weight => 0})
# ]

# 
# MyProcessor.servers = servers
# MyProcessor.run
