require 'socket'

module MQ3
  MQ3_ROOT = defined?(RAILS_ROOT) ? RAILS_ROOT : File.join(File.dirname(__FILE__))
  
  module Protocols
    class ConnectionError < StandardError #:nodoc:
    end
    
    class Memcache
      attr_accessor :options

      LOG_DIR             = File.join(MQ3_ROOT, 'log')

      CR                  = "\r\n"     
      ERROR               = "ERROR"    
                                   
      GET                 = "GET"      
      SET                 = "SET"      
      DELETE              = "DELETE"   
      FLUSH_ALL           = "FLUSH_ALL"
        
      CLIENT_ERROR_REGEX  = /\ACLIENT_ERROR\s/i
      SERVER_ERROR_REGEX  = /\ASERVER_ERROR\s/i
      ERROR_REGEX         = /\AERROR\s/i
      VALUE_REGEX         = /^VALUE (.+) (.+) (.+)/
      
      def initialize(opts = {})
        self.options = opts
      end
  
      def []=(queue_name, msg)
        send_msg SET, queue_name, 0, 0, msg.length, CR + msg
      end
  
      def [](queue_name)
        send_msg GET, queue_name
        return unless @socket
        rsp = @socket.gets
        return unless rsp =~ VALUE_REGEX
        bytes = rsp.split(' ').last.to_i
        msg = @socket.gets
        eof = @socket.gets # END
        msg
      rescue => e
        mark_dead(e)
        nil
      end
  
      def delete(queue_name)
        send_msg DELETE, queue_name
      end
  
      def delete!
        send_msg FLUSH_ALL
      end
      
      # MQ3 Protocol API
  
      def alive?
        (!@retry or @retry < Time.now)
      end
      
      def weight
  		  @options[:weight] || 0  
      end
  
      private
      
      def retry?
        ((@retry and @retry < Time.now) and !@connected) ? true : false
      end

      def send_msg(*msg)
        @socket ||= TCPSocket.open(options[:host], options[:port])
        @socket.print(msg.join(' ') + CR)
        @socket.flush
        sleep 0.00001
      rescue => e
        mark_dead(e)
        raise ConnectionError
      end

      def mark_dead( reason="Unknown error" )
        @socket.close if @socket && !@socket.closed?
        @socket = nil
        @retry = Time::now + ( 30 + rand(10) )
    		@status = "DEAD: %s: Will reincarnate at %s" %
    			[ reason, @retry ]
    	end
    end # Memcache
    
  end # Protocols
  
  class Queue
    class MQ3Error < StandardError; end #:nodoc:
    class NoServersLeft < MQ3Error; end #:nodoc:
    
    PID_DIR = File.join(MQ3_ROOT, 'tmp', 'pids')
    
    class << self
      def run(use_daemonize = false)
        puts "Starting #{queue_name} up..."
        could_daemonize(use_daemonize) {
          loop do
            msg = poll
            self.new.process(msg) if msg
          end
        }
      end
    
      def publish(msg)
        send_to_server {|server|
          server[queue_name] = msg.to_yaml
        }
      end
    
      def poll
        send_to_server {|server|
          server[queue_name]
        }
      end
    
      def queue_name
        self.name
      end
      
      def servers=(srvs)
        @@servers = srvs
      end
      
      def servers
        @@servers ||= []
      end
      
      private
      
      def send_to_server(random = false, &block)
        alive_servers.sort_by{ random ? rand : 1 }.each do |server|
          begin
            return yield(server)
          rescue MQ3::Protocols::ConnectionError
            # Server will be marked dead
            next
          end
        end
        raise NoServersLeft
      end
      
      # Daemon stuff
      
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
      
      def alive_servers
        servers.select {|a| a.alive? }.sort_by {|b| b.weight }.reverse
      end

    end # self
    
    def process(msg)
      1.upto(retry_attempts + 1) do |n|
        begin
          on_message(YAML.load(msg))
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
      defined?(RAILS_DEFAULT_LOGGER) ? RAILS_DEFAULT_LOGGER : Logger.new(STDOUT)
    end

    def retry_attempts
      0
    end
    
    def on_message
      raise 'You must implement on_message.'
    end
    
  end # Queue
end # MQ3
  
  