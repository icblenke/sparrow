require 'socket'

module MQueue
  module Protocols
    class Sparrow
      attr_accessor :options

      LOG_DIR             = File.join(MQueue::MQUEUE_ROOT, 'log')

      CR                  = "\r\n"     
      ERROR               = "error"    
                               
      GET                 = "get"      
      SET                 = "set"      
      DELETE              = "delete"   
      FLUSH_ALL           = "flush_all"
    
      CLIENT_ERROR_REGEX  = /\ACLIENT_ERROR\s/i
      SERVER_ERROR_REGEX  = /\ASERVER_ERROR\s/i
      ERROR_REGEX         = /\AERROR\s/i
      VALUE_REGEX         = /^VALUE (.+) (.+) (.+)/
  
      def initialize(opts = {})
        self.options = opts
      end

      def []=(queue_name, msg)
        send_msg SET, queue_name, 0, 0, msg.length, CR + msg
        return unless @socket
        @socket.gets # STORED
      end

      def [](queue_name)
        send_msg GET, queue_name
        return unless @socket
        rsp = @socket.gets
        return unless rsp =~ VALUE_REGEX
        bytes = rsp.split(' ').last.to_i
        rsp =~ /(\d+)\r/
        msg = @socket.read $1.to_i
        @socket.gets # CR
        @socket.gets # END
        msg
      rescue => e
        mark_dead(e)
        nil
      end

      def delete(queue_name)
        send_msg DELETE, queue_name
        return unless @socket
        @socket.gets
      end

      def delete!
        send_msg FLUSH_ALL
        return unless @socket
        @socket.gets
      end
  
      # MQueue Protocol API

      def alive?
        (!@retry or @retry < Time.now)
      end
  
      def weight
    	  options[:weight] || 0  
      end
      
      def reload!
        @retry = nil
        @socket.close if @socket && !@socket.closed?
        @socket = nil
        @status = "Reloading"
      end

      private
  
      def retry?
        ((@retry and @retry < Time.now) and !@connected) ? true : false
      end

      def send_msg(*msg)
        @socket ||= TCPSocket.open(options[:host], options[:port])
        @socket.print(msg.join(' ') + CR)
        @socket.flush
        # sleep 0.00001
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
    end
  end
end