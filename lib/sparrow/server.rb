require 'socket'
module Sparrow
  module Server
    include Sparrow::Miscel
    
    class NoMoreMessages < SparrowError #:nodoc:
    end

    class ClientError < SparrowError #:nodoc:
    end

    class StatementInvalid < ClientError #:nodoc:
    end

    class InvalidBodyLength < ClientError #:nodoc:
    end
  
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
    
    STATS           = "STATS"
    
    SET_REGEX       = /\ASET\s/i
    ADD_REGEX       = /\AADD\s/i
    REPLACE_REGEX   = /\AREPLACE\s/i
    DELETE_REGEX    = /\ADELETE\s/i
    GET_REGEX       = /\AGET\s/i
    QUIT_REGEX      = /\AQUIT/i
    FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
    VERSION_REGEX   = /\AVERSION/i
    STATS_REGEX     = /\ASTATS/i
    
    mattr_accessor :bytes_read
    mattr_accessor :bytes_written
    mattr_accessor :connections_made
    mattr_accessor :connections_lost
    mattr_accessor :get_count
    mattr_accessor :set_count
    
    self.bytes_read = 0
    self.bytes_written = 0
    self.connections_made = 0
    self.connections_lost = 0
    self.get_count = 0
    self.set_count = 0

    def post_init
      @current_queue = nil
      @expecting_body = false
      @expected_bytes = 0
      @current_flag = nil
      @buffer = ''
      self.connections_made += 1
      logger.debug "New client [#{client_ip}]"
    end
    
    def unbind
      self.connections_lost += 1
      logger.debug "Lost client"
    end
    
    def send_data(data)
      self.bytes_written += 1
      super(data)
    end
    
    def receive_data(data)
      logger.debug "Receiving data: #{data}"
      self.bytes_read += 1
      @buffer << data
      @buffer = process_whole_messages(@buffer)
    end
    
    # process any whole messages in the buffer,
    # and return the new contents of the buffer
    def process_whole_messages(data)
      return data if data !~ /\r\n/i # only process if data contains a CR
      messages = data.split(CR)
      if data =~ /\r\n$/i
        data = ''
      else
        # remove the last message from the list (because it is incomplete) before processing
        data = messages.pop
      end
      messages.each {|message| process_message(message) }
      return data
    end
  
    def process_message ln
      @data = ln
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
      elsif ln =~ STATS_REGEX
        stats_command
      elsif @expecting_body
        process_body
      else
        raise StatementInvalid
      end
      @data = nil
      @split_args = nil

    rescue ClientError => e
      logger.error e
      publish CLIENT_ERROR, e
      publish ERROR
    rescue => e
      logger.error e
      publish SERVER_ERROR, e
    end
  
    def publish *args
      send_data args.join(' ') + CR
    end
  
    # Storage commands

    # <command name> <key> <flags> <exptime> <bytes>\r\n
    def set_command
      @current_queue = args[1]
      @current_flag = args[2] || 0
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
      @data << @current_flag
      logger.debug "Adding message to queue - #{@current_queue}"
      Sparrow::Queue.add_message(@current_queue, @data)
      self.set_count += 1
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
      rsp = []
      args.each do |queue|
        logger.debug "Getting message from queue - #{queue}"
        begin
          msg = Sparrow::Queue.next_message(queue)
          next unless msg
        rescue NoMoreMessages
          next
        end
        flag = msg[-1..-1]
        msg = msg[0..-2]
        rsp << [VALUE, queue, flag, msg.length].join(' ')
        rsp << msg
        self.get_count += 1
      end
      rsp << EOF
      send_data(rsp.join(CR) + CR)
    end
  
    # Other commands
  
    # DELETE <key> <time>\r\n
    def delete_command
      if Sparrow::Queue.delete(!args[1])
        logger.info "Deleting queue - #{args[1]}"
        publish DELETED
      else
        publish NOT_FOUND
      end
    end
    
    def stats_command
      rsp = []
      stats_hash = Sparrow::Queue.get_stats(args[1])
      stats_hash.merge!({
        :curr_connections   => (self.connections_made - self.connections_lost),
        :total_connections  => self.connections_made,
        :bytes_read         => self.bytes_read,
        :bytes_written      => self.bytes_written,
        :get_count          => self.get_count,
        :set_count          => self.set_count
      })
      stats_hash.each do |key, value|
        rsp << [STATS, key, value].join(' ')
      end
      rsp << EOF
      send_data(rsp.join(CR) + CR)
    end
  
    # FLUSH_ALL
    def flush_all_command
      logger.info "Flushing all queues"
      Sparrow::Queue.delete_all
      publish OK
    end
  
    # VERSION
    def version_command
      publish VERSION, Sparrow::Version
    end
  
    # QUIT
    def quit_command
      logger.debug "Closing connection"
      close_connection
    end
  
    private
  
    def args
      @split_args ||= @data.split(' ')
    end
    
    def client_ip
      Socket.unpack_sockaddr_in(get_peername)[1]
    end
  
  end
end