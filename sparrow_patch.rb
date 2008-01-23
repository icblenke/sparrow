Index: sparrow
===================================================================
--- sparrow	(revision 24)
+++ sparrow	(working copy)
@@ -82,389 +82,355 @@
 
 require 'rubygems'
 require 'eventmachine'
-require 'uuid'
 require 'fileutils'
 require 'optparse'
 
-module Sparrow
-  
-  class SparrowError < StandardError #:nodoc:
+# Extends the class object with class and instance accessors for class attributes,
+# just like the native attr* accessors for instance attributes.
+class Class # :nodoc:
+  def cattr_reader(*syms)
+    syms.flatten.each do |sym|
+      next if sym.is_a?(Hash)
+      class_eval(<<-EOS, __FILE__, __LINE__)
+        unless defined? @@#{sym}
+          @@#{sym} = nil
+        end
+
+        def self.#{sym}
+          @@#{sym}
+        end
+
+        def #{sym}
+          @@#{sym}
+        end
+      EOS
+    end
   end
-  
-  class NoMoreMessages < SparrowError #:nodoc:
+
+  def cattr_writer(*syms)
+    options = syms.extract_options!
+    syms.flatten.each do |sym|
+      class_eval(<<-EOS, __FILE__, __LINE__)
+        unless defined? @@#{sym}
+          @@#{sym} = nil
+        end
+
+        def self.#{sym}=(obj)
+          @@#{sym} = obj
+        end
+
+        #{"
+        def #{sym}=(obj)
+          @@#{sym} = obj
+        end
+        " unless options[:instance_writer] == false }
+      EOS
+    end
   end
-  
-  class ClientError < SparrowError #:nodoc:
+
+  def cattr_accessor(*syms)
+    cattr_reader(*syms)
+    cattr_writer(*syms)
   end
+end
+
+module Sparrow
   
-  class StatementInvalid < ClientError #:nodoc:
+  class SparrowError < StandardError #:nodoc:
   end
   
-  class InvalidBodyLength < ClientError #:nodoc:
-  end
-  
   BASE_DIR        = File.join(File.dirname(__FILE__), 'base')
-  PROCESSING_DIR  = File.join(File.dirname(__FILE__), 'processing')
   LOG_DIR         = File.join(File.dirname(__FILE__), 'log')
   
-  CR              = "\r\n"
-  ERROR           = "ERROR"
-  OK              = "OK"
-  EOF             = "END"
+  module Server
+    
+    class NoMoreMessages < SparrowError #:nodoc:
+    end
+
+    class ClientError < SparrowError #:nodoc:
+    end
+
+    class StatementInvalid < ClientError #:nodoc:
+    end
+
+    class InvalidBodyLength < ClientError #:nodoc:
+    end
+  
+    CR              = "\r\n"
+    ERROR           = "ERROR"
+    OK              = "OK"
+    EOF             = "END"
                
-  CLIENT_ERROR    = "CLIENT_ERROR"
-  SERVER_ERROR    = "SERVER_ERROR"
+    CLIENT_ERROR    = "CLIENT_ERROR"
+    SERVER_ERROR    = "SERVER_ERROR"
                
-  STORED          = "STORED"
-  NOT_STORED      = "NOT_STORED"
+    STORED          = "STORED"
+    NOT_STORED      = "NOT_STORED"
                
-  DELETED         = "DELETED"
-  NOT_FOUND       = "NOT_FOUND"
+    DELETED         = "DELETED"
+    NOT_FOUND       = "NOT_FOUND"
   
-  VALUE           = "VALUE"
+    VALUE           = "VALUE"
   
-  VERSION         = "VERSION"
+    VERSION         = "VERSION"
     
-  SET_REGEX       = /\ASET\s/i
-  ADD_REGEX       = /\AADD\s/i
-  REPLACE_REGEX   = /\AREPLACE\s/i
-  DELETE_REGEX    = /\ADELETE\s/i
-  GET_REGEX       = /\AGET\s/i
-  QUIT_REGEX      = /\AQUIT/i
-  FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
-  VERSION_REGEX   = /\AVERSION/i
+    SET_REGEX       = /\ASET\s/i
+    ADD_REGEX       = /\AADD\s/i
+    REPLACE_REGEX   = /\AREPLACE\s/i
+    DELETE_REGEX    = /\ADELETE\s/i
+    GET_REGEX       = /\AGET\s/i
+    QUIT_REGEX      = /\AQUIT/i
+    FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
+    VERSION_REGEX   = /\AVERSION/i
+  
+    cattr_accessor :queues
+    cattr_accessor :options
+  
+    def max_log_size
+      @@max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
+    end
 
-  def post_init
-    @data = ""
-    @current_queue = nil
-    @expecting_body = false
-    @expected_bytes = 0
-    @current_flag = nil
-    logger.debug "New client"
-  end
+    def post_init
+      @current_queue = nil
+      @expecting_body = false
+      @expected_bytes = 0
+      @current_flag = nil
+      logger.debug "New client"
+    end
   
-  def receive_data ln
-    logger.debug "Receiving data: #{ln}"
-    if ln[-2..-1].include?(CR)
-      ln.split(CR).each do |ln|
-        @data << ln
-        if ln =~ SET_REGEX
-          set_command
-        elsif ln =~ ADD_REGEX
-          add_command
-        elsif ln =~ REPLACE_REGEX
-          replace_command
-        elsif ln =~ GET_REGEX
-          get_command
-        elsif ln =~ DELETE_REGEX
-          delete_command
-        elsif ln =~ QUIT_REGEX
-          quit_command
-        elsif ln =~ VERSION_REGEX
-          version_command
-        elsif ln =~ FLUSH_ALL_REGEX
-          flush_all_command
-        elsif @expecting_body
-          process_body
-        else
-          raise StatementInvalid
+    def receive_data ln
+      logger.debug "Receiving data: #{ln}"
+      if ln[-2..-1].include?(CR)
+        ln.split(CR).each do |ln|
+          @data = ln
+          if ln =~ SET_REGEX
+            set_command
+          elsif ln =~ ADD_REGEX
+            add_command
+          elsif ln =~ REPLACE_REGEX
+            replace_command
+          elsif ln =~ GET_REGEX
+            get_command
+          elsif ln =~ DELETE_REGEX
+            delete_command
+          elsif ln =~ QUIT_REGEX
+            quit_command
+          elsif ln =~ VERSION_REGEX
+            version_command
+          elsif ln =~ FLUSH_ALL_REGEX
+            flush_all_command
+          elsif @expecting_body
+            process_body
+          else
+            raise StatementInvalid
+          end
+          @data = nil
+          @split_args = nil
         end
-        @data = ''
-        @split_args = nil
+      else
+        raise StatementInvalid
       end
-    else
-      raise StatementInvalid
-    end
 
-  rescue ClientError => e
-    logger.error e
-    publish CLIENT_ERROR, e
-    publish ERROR
-  rescue => e
-    logger.error e
-    publish SERVER_ERROR, e
-  end
+    rescue ClientError => e
+      logger.error e
+      publish CLIENT_ERROR, e
+      publish ERROR
+    rescue => e
+      logger.error e
+      publish SERVER_ERROR, e
+    end
   
-  def publish *args
-    send_data args.join(' ') + CR
-  end
+    def publish *args
+      send_data args.join(' ') + CR
+    end
   
-  # Storage commands
+    # Storage commands
 
-  # <command name> <key> <flags> <exptime> <bytes>\r\n
-  def set_command
-    @current_queue = args[1]
-    @current_flag = args[2] || 0
-    raise ClientError unless @current_queue
-    @expected_bytes = args[4].to_i || 0
-    @expecting_body = true
-  end
-  alias add_command set_command
-  alias replace_command set_command
+    # <command name> <key> <flags> <exptime> <bytes>\r\n
+    def set_command
+      @current_queue = args[1]
+      @current_flag = args[2] || 0
+      raise ClientError unless @current_queue
+      @expected_bytes = args[4].to_i || 0
+      @expecting_body = true
+    end
+    alias add_command set_command
+    alias replace_command set_command
   
-  def process_body
-    if @data.length != @expected_bytes
-     raise InvalidBodyLength
+    def process_body
+      if @data.length != @expected_bytes
+       raise InvalidBodyLength
+      end
+      @data << @current_flag
+      logger.debug "Adding message to queue - #{@current_queue}"
+      add_message(@current_queue, @data)
+      @expected_bytes = 0
+      @current_queue = nil
+      @expecting_body = false
+      publish STORED
     end
-    @data << @current_flag
-    logger.debug "Adding message to queue - #{@current_queue}"
-    add_message(@current_queue, @data)
-    @expected_bytes = 0
-    @current_queue = nil
-    @expecting_body = false
-    publish STORED
-  end
 
-  # Retrieval commands
+    # Retrieval commands
   
-  # GET <key>*r\n
-  def get_command
-    args.shift # get rid of the command
-    raise ClientError if args.empty?
-    rsp = []
-    args.each do |queue|
-      begin
-        logger.debug "Getting message from queue - #{queue}"
-        msg = next_message(queue)
-      rescue NoMoreMessages
-        next
+    # GET <key>*r\n
+    def get_command
+      args.shift # get rid of the command
+      raise ClientError if args.empty?
+      rsp = []
+      args.each do |queue|
+        begin
+          logger.debug "Getting message from queue - #{queue}"
+          msg = next_message(queue)
+        rescue NoMoreMessages
+          next
+        end
+        flag = msg[-1..-1]
+        msg = msg[0..-2]
+        rsp << [VALUE, queue, flag, msg.length].join(' ')
+        rsp << msg
       end
-      flag = msg[-1..-1]
-      msg = msg[0..-2]
-      rsp << [VALUE, queue, flag, msg.length].join(' ')
-      rsp << msg
+      rsp << EOF
+      send_data(rsp.join(CR) + CR)
     end
-    rsp << EOF
-    send_data(rsp.join(CR) + CR)
-  end
   
-  # Other commands
+    # Other commands
   
-  # DELETE <key> <time>\r\n
-  def delete_command
-    path  = File.join(BASE_DIR, args[1])
-    if File.exists?(path) or !args[1]
-      logger.info "Deleting queue - #{args[1]}"
-      FileUtils.rm_rf path
-      publish DELETED
-    else
-      publish NOT_FOUND
+    # DELETE <key> <time>\r\n
+    def delete_command
+      path  = File.join(BASE_DIR, args[1])
+      if File.exists?(path) or !args[1]
+        logger.info "Deleting queue - #{args[1]}"
+        FileUtils.rm_rf path
+        publish DELETED
+      else
+        publish NOT_FOUND
+      end
     end
-  end
   
-  # FLUSH_ALL
-  def flush_all_command
-    logger.info "Flushing all queues"
-    FileUtils.rm_rf BASE_DIR
-    publish OK
-  end
+    # FLUSH_ALL
+    def flush_all_command
+      logger.info "Flushing all queues"
+      FileUtils.rm_rf BASE_DIR
+      publish OK
+    end
   
-  # VERSION
-  def version_command
-    publish VERSION, '0.1'
-  end
+    # VERSION
+    def version_command
+      publish VERSION, '0.1'
+    end
   
-  # QUIT
-  def quit_command
-    logger.debug "Closing connection"
-    close_connection
-  end
+    # QUIT
+    def quit_command
+      logger.debug "Closing connection"
+      close_connection
+    end
 
-  protected
+    protected
   
-  # Queue methods
+    # Queue methods
   
-  def next_message(queue_name)
-    while path = find_next_file(queue_name)
-      next unless File.exists?(path)
-      begin
-        mv_path = File.join(PROCESSING_DIR, queue_name, File.basename(path))
-        FileUtils.mkdir_p(File.dirname(mv_path))
-        FileUtils.mv path, mv_path
-        data = File.read(mv_path)
-        FileUtils.rm mv_path
-        cleanup(File.dirname(path))
-        return data
-      rescue => e
-        # Usually would happen when a message is being processed 
-        # simultaneously by another Sparrow instance
-        logger.debug "Error reading next message:" + e
-        next 
-      end
+    def next_message(queue_name)
+      Queue.get_queue(queue_name).pop
     end
-  end
   
-  def find_next_file(queue_name)
-    @@files ||= []
-    return @@files.pop unless @@files.empty?
-    @@files = Dir.glob(File.join(BASE_DIR, queue_name, '*', '*', '*', '*', '*.msg'))
-    raise NoMoreMessages if @@files.empty?
-    @@files.pop
-  end
-  
-  def add_message(queue_name, msg)
-    name = File.join(BASE_DIR, new_file_name(queue_name))
-    FileUtils.mkdir_p(File.dirname(name))
-    File.open(name, 'w+') do |file|
-      file.write msg
+    def add_message(queue_name, value)
+      Queue.get_queue(queue_name).push(value)
     end
-    true
-  end
     
-  def logger
-    return @@logger if defined?(@@loggger)
-    FileUtils.mkdir_p(LOG_DIR)
-    @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
-    @@logger.level = Logger::INFO if !SparrowRunner.debug
-    @@logger
-  rescue
-    @@logger = Logger.new(STDOUT)
-  end
+    def logger
+      return @@logger if defined?(@@loggger)
+      FileUtils.mkdir_p(LOG_DIR)
+      @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
+      @@logger.level = Logger::INFO if !options[:debug]
+      @@logger
+    rescue
+      @@logger = Logger.new(STDOUT)
+    end
   
-  def self.clear!
-    logger.info "Clearing queue"
-    flush_all_command
-    FileUtils.rm_rf PROCESSING_DIR
-    true
-  end
+    private
   
-  def cleanup(folder_path)
-    dir = Dir.new(folder_path)
-    # first two are ['.', '..']
-    dir.read; dir.read
-    return if dir.read
-    return if [File.expand_path(BASE_DIR), File.expand_path(PROCESSING_DIR)].include?(File.expand_path(folder_path))
-    logger.info "Cleaning up: #{folder_path}"
-    FileUtils.rm_r folder_path
-    cleanup(File.dirname(folder_path))
-  rescue => e
-    logger.debug "Error cleaning up queue: #{e}"
-    # Usually would happen if cleanup is done
-    # simultaneously by another Sparrow instance
-  end
-
-  private
+    def args
+      @split_args ||= @data.split(' ')
+    end
   
-  def new_file_name(queue_name)
-    guid = UUID.new
-    hex = Digest::MD5.hexdigest(guid.to_s)
-    time = Time.now
-    path = time.strftime("%Y%m%d%H%M").scan(/..../)
-    File.join(queue_name, path, time.strftime("%S"), hex + '.msg')
   end
   
-  def args
-    @split_args ||= @data.split(' ')
-  end
+  class Queue
+    # Shamelessly utilised from Starling
+    TRX_CMD_PUSH = "\000".freeze
+    TRX_CMD_POP = "\001".freeze
 
-end
-
-class SparrowRunner
-  attr_accessor :options
-  PID_DIR = File.join(File.dirname(__FILE__), 'pids')
-  @@debug = false
-
-  def initialize(ops = {})
-    parse_options(ops)
-
-    self.options[:port] ||= 11212
-    self.options[:host] ||= "0.0.0.0"
-    self.options[:cluster] ||= 1
-
-    @@debug = self.options[:debug] || false
-
-    kill_pid(options[:kill]) if self.options[:kill]
+    TRX_PUSH = "\000%s%s".freeze
+    TRX_POP = "\001".freeze
     
-    if !options[:daemonize] and !(self.options[:cluster] > 1)
-      start(self.options[:port], self.options)
-    else
-      self.options[:port].upto(self.options[:port] + self.options[:cluster] - 1) do |n|
-        daemonize(n, self.options)
+    cattr_accessor :queues
+    attr_accessor :queue
+    attr_accessor :trx
+    attr_accessor :data
+    
+    class << self
+      def get_queue(queue_name)
+        @@queues[queue_name] ||= []
       end
     end
-  end
-  
-  def self.debug
-    @@debug
-  end
-  
-  protected
-
-  def parse_options(ops = {})
-    self.options = {}
-    OptionParser.new do |opts|
-      opts.banner = "Usage: sparrow.rb [-p port] [-h host] [-c cleanup-timer]"
-
-      opts.on("-p", "--port [number]", Integer, "Specify Port") do |v|
-        self.options[:port] = v
-      end
-      
-      opts.on("-c", "--cluster [cluster size]", Integer, "Create a cluster of daemons") do |v|
-        self.options[:cluster] = v 
-      end
-  
-      opts.on("-h", "--host [string]", String, "Specify host") do |v|
-        self.options[:host] = v 
-      end
-  
-      opts.on("-l", "--debug", "Run in debug mode") do |v|
-        self.options[:debug] = v
-      end
-  
-      opts.on("-d", "--daemon", "Daemonize mode") do |v|
-        self.options[:daemonize] = v
-      end
-  
-      opts.on("-k", "--kill [<name>/all]", String, "Kill specified running daemons") do |v|
-        self.options[:kill] = v || '*'
-      end
-    end.parse!
-    self.options.merge!(ops)
-    self.options
-  end
     
-  def start(port, options = {})
-   EventMachine::run {
-     EventMachine::start_server options[:host], port.to_i, Sparrow
-   }
-  end
-  
-  private
+    def initialize(queue_name)
+      self.queue = queue_name
+      self.data = []
+      reopen_queue
+    end
+    
+    def reopen_queue
+      self.trx = File.new(File.join(BASE_DIR, queue), File::CREAT|File::RDWR)
+    end
 
-  def store_pid(pid, port)
-   FileUtils.mkdir_p(PID_DIR)
-   File.open(File.join(PID_DIR, "sparrow.#{port}.pid"), 'w'){|f| f.write("#{pid}\n")}
-  end
-  
-  def kill_pid(k)
-    Dir[File.join(PID_DIR, "sparrow.#{k}.pid")].each do |f|
-      begin
-        puts f
-        pid = IO.read(f).chomp.to_i
-        FileUtils.rm f
-        Process.kill(9, pid)
-        puts "killed PID: #{pid}"
-      rescue => e
-        puts "Failed to kill! #{k}: #{e}"
+    # def rotate_queue(queue_name)
+    #   @queue.close
+    #   File.rename(File.join(BASE_DIR, queue_name), "#{queue_name}.#{Time.now.to_i}")
+    #   reopen_queue(queue_name)
+    # end
+    
+    def push(value)
+      size = [value.size].pack("I")
+      data_to_write = sprintf(TRX_PUSH, size, value)
+      self.data.push value
+      self.trx.write data_to_write
+      self.trx.fsync
+    end
+    
+    def pop
+      value = self.data.pop
+      @trx.write "\001"
+      @trx.fsync
+      value
+    end
+    
+    def replay_queue(queue_name)
+      bytes_read = 0
+    
+      while !trx.eof?
+        cmd = trx.read(1)
+        case cmd
+        when TRX_CMD_PUSH
+          logger.debug ">"
+          raw_size = trx.read(4)
+          next unless raw_size
+          size = raw_size.unpack("I").first
+          value = self.trx.read(size)
+          next unless value
+          self.data.push(value)
+          bytes_read += value.size
+        when TRX_CMD_POP
+          logger.debug "<"
+          bytes_read -= self.data.pop.size
+        else
+          logger.error "Error reading queue: " +
+                       "I don't understand '#{cmd}' (skipping)."
+        end
       end
+    
+      bytes_read
     end
-    exit
+    
   end
-
-  def daemonize(port, options)
-   fork do
-     Process.setsid
-     exit if fork
-     store_pid(Process.pid, port)
-     Dir.chdir File.dirname(__FILE__)
-     File.umask 0000
-     STDIN.reopen "/dev/null"
-     STDOUT.reopen "/dev/null", "a"
-     STDERR.reopen STDOUT
-     trap("TERM") { exit }
-     start(port, options)
-   end
-  end
-end
-
-SparrowRunner.new
\ No newline at end of file
+end
\ No newline at end of file
Index: sparrow_patch.rb
===================================================================
--- sparrow_patch.rb	(revision 33)
+++ sparrow_patch.rb	(working copy)
@@ -2,7 +2,7 @@
 ===================================================================
 --- sparrow	(revision 24)
 +++ sparrow	(working copy)
-@@ -82,7 +82,6 @@
+@@ -82,389 +82,355 @@
  
  require 'rubygems'
  require 'eventmachine'
@@ -10,19 +10,68 @@
  require 'fileutils'
  require 'optparse'
  
-@@ -91,380 +90,306 @@
-   class SparrowError < StandardError #:nodoc:
+-module Sparrow
+-  
+-  class SparrowError < StandardError #:nodoc:
++# Extends the class object with class and instance accessors for class attributes,
++# just like the native attr* accessors for instance attributes.
++class Class # :nodoc:
++  def cattr_reader(*syms)
++    syms.flatten.each do |sym|
++      next if sym.is_a?(Hash)
++      class_eval(<<-EOS, __FILE__, __LINE__)
++        unless defined? @@#{sym}
++          @@#{sym} = nil
++        end
++
++        def self.#{sym}
++          @@#{sym}
++        end
++
++        def #{sym}
++          @@#{sym}
++        end
++      EOS
++    end
    end
-   
+-  
 -  class NoMoreMessages < SparrowError #:nodoc:
--  end
++
++  def cattr_writer(*syms)
++    options = syms.extract_options!
++    syms.flatten.each do |sym|
++      class_eval(<<-EOS, __FILE__, __LINE__)
++        unless defined? @@#{sym}
++          @@#{sym} = nil
++        end
++
++        def self.#{sym}=(obj)
++          @@#{sym} = obj
++        end
++
++        #{"
++        def #{sym}=(obj)
++          @@#{sym} = obj
++        end
++        " unless options[:instance_writer] == false }
++      EOS
++    end
+   end
 -  
 -  class ClientError < SparrowError #:nodoc:
--  end
--  
++
++  def cattr_accessor(*syms)
++    cattr_reader(*syms)
++    cattr_writer(*syms)
+   end
++end
++
++module Sparrow
+   
 -  class StatementInvalid < ClientError #:nodoc:
--  end
--  
++  class SparrowError < StandardError #:nodoc:
+   end
+   
 -  class InvalidBodyLength < ClientError #:nodoc:
 -  end
 -  
@@ -91,9 +140,12 @@
 +    FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
 +    VERSION_REGEX   = /\AVERSION/i
 +  
-+    # Shamelessly utilised from Starling
-+    TRX_CMD_PUSH = "\000".freeze
-+    TRX_CMD_POP = "\001".freeze
++    cattr_accessor :queues
++    cattr_accessor :options
++  
++    def max_log_size
++      @@max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
++    end
  
 -  def post_init
 -    @data = ""
@@ -103,8 +155,13 @@
 -    @current_flag = nil
 -    logger.debug "New client"
 -  end
-+    TRX_PUSH = "\000%s%s".freeze
-+    TRX_POP = "\001".freeze
++    def post_init
++      @current_queue = nil
++      @expecting_body = false
++      @expected_bytes = 0
++      @current_flag = nil
++      logger.debug "New client"
++    end
    
 -  def receive_data ln
 -    logger.debug "Receiving data: #{ln}"
@@ -131,30 +188,6 @@
 -          process_body
 -        else
 -          raise StatementInvalid
-+    @@queues = {}
-+  
-+    @@options = {}
-+  
-+    def options
-+      @@options
-+    end
-+  
-+    def self.options=(obj)
-+      @@options = obj
-+    end
-+  
-+    def max_log_size
-+      @@max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
-+    end
-+
-+    def post_init
-+      @current_queue = nil
-+      @expecting_body = false
-+      @expected_bytes = 0
-+      @current_flag = nil
-+      logger.debug "New client"
-+    end
-+  
 +    def receive_data ln
 +      logger.debug "Receiving data: #{ln}"
 +      if ln[-2..-1].include?(CR)
@@ -470,7 +503,11 @@
 -  def args
 -    @split_args ||= @data.split(' ')
 -  end
--
++  class Queue
++    # Shamelessly utilised from Starling
++    TRX_CMD_PUSH = "\000".freeze
++    TRX_CMD_POP = "\001".freeze
+ 
 -end
 -
 -class SparrowRunner
@@ -488,14 +525,19 @@
 -    @@debug = self.options[:debug] || false
 -
 -    kill_pid(options[:kill]) if self.options[:kill]
--    
++    TRX_PUSH = "\000%s%s".freeze
++    TRX_POP = "\001".freeze
+     
 -    if !options[:daemonize] and !(self.options[:cluster] > 1)
 -      start(self.options[:port], self.options)
 -    else
 -      self.options[:port].upto(self.options[:port] + self.options[:cluster] - 1) do |n|
 -        daemonize(n, self.options)
-+  class Queue
-+    @@queues = {}
++    cattr_accessor :queues
++    attr_accessor :queue
++    attr_accessor :trx
++    attr_accessor :data
++    
 +    class << self
 +      def get_queue(queue_name)
 +        @@queues[queue_name] ||= []
@@ -508,12 +550,7 @@
 -  end
 -  
 -  protected
-+    
-+    def reopen_queue(queue_name)
-+      @queue = File.new(File.join(BASE_DIR, queue_name), File::CREAT|File::RDWR)
-+      @queue_size = File.size()
-+    end
- 
+-
 -  def parse_options(ops = {})
 -    self.options = {}
 -    OptionParser.new do |opts|
@@ -521,54 +558,7 @@
 -
 -      opts.on("-p", "--port [number]", Integer, "Specify Port") do |v|
 -        self.options[:port] = v
-+    def rotate_queue(queue_name)
-+      @queue.close
-+      File.rename(File.join(BASE_DIR, queue_name), "#{queue_name}.#{Time.now.to_i}")
-+      reopen_queue(queue_name)
-+    end
-+    
-+    def push
-+      size = [value.size].pack("I")
-+      data = sprintf(TRX_PUSH, size, value)
-+      @@trxs[queue_name] ||= open_queue(queue_name)
-+      @@queues[queue_name].push value
-+      @@trxs[queue_name].write data
-+      @@trxs[queue_name].fsync
-+    end
-+    
-+    def pop
-+      @@trxs[queue_name] ||= open_queue(queue_name)
-+      data = @@queues[queue_name].pop
-+      @@trxs[queue_name].write "\001"
-+      @@trxs[queue_name].fsync
-+      data
-+    end
-+    
-+    def replay_queue(queue_name)
-+      trx = open_queue(queue_name)
-+      bytes_read = 0
-+      @@queues[queue_name] ||= []
-+    
-+      while !trx.eof?
-+        cmd = trx.read(1)
-+        case cmd
-+        when TRX_CMD_PUSH
-+          logger.debug ">"
-+          raw_size = trx.read(4)
-+          next unless raw_size
-+          size = raw_size.unpack("I").first
-+          data = trx.read(size)
-+          next unless data
-+          @@queues[queue_name].push(data)
-+          bytes_read += data.size
-+        when TRX_CMD_POP
-+          logger.debug "<"
-+          bytes_read -= @@queue[queue_name].pop.size
-+        else
-+          logger.error "Error reading queue: " +
-+                       "I don't understand '#{cmd}' (skipping)."
-+        end
-       end
+-      end
 -      
 -      opts.on("-c", "--cluster [cluster size]", Integer, "Create a cluster of daemons") do |v|
 -        self.options[:cluster] = v 
@@ -599,11 +589,18 @@
 -     EventMachine::start_server options[:host], port.to_i, Sparrow
 -   }
 -  end
-+      bytes_read
-+    end
-   
+-  
 -  private
--
++    def initialize(queue_name)
++      self.queue = queue_name
++      self.data = []
++      reopen_queue
++    end
++    
++    def reopen_queue
++      self.trx = File.new(File.join(BASE_DIR, queue), File::CREAT|File::RDWR)
++    end
+ 
 -  def store_pid(pid, port)
 -   FileUtils.mkdir_p(PID_DIR)
 -   File.open(File.join(PID_DIR, "sparrow.#{port}.pid"), 'w'){|f| f.write("#{pid}\n")}
@@ -619,9 +616,52 @@
 -        puts "killed PID: #{pid}"
 -      rescue => e
 -        puts "Failed to kill! #{k}: #{e}"
--      end
-+    def open_queue(queue_name)
-+      File.new(File.join(BASE_DIR, queue_name), File::CREAT|File::RDWR)
++    # def rotate_queue(queue_name)
++    #   @queue.close
++    #   File.rename(File.join(BASE_DIR, queue_name), "#{queue_name}.#{Time.now.to_i}")
++    #   reopen_queue(queue_name)
++    # end
++    
++    def push(value)
++      size = [value.size].pack("I")
++      data_to_write = sprintf(TRX_PUSH, size, value)
++      self.data.push value
++      self.trx.write data_to_write
++      self.trx.fsync
++    end
++    
++    def pop
++      value = self.data.pop
++      @trx.write "\001"
++      @trx.fsync
++      value
++    end
++    
++    def replay_queue(queue_name)
++      bytes_read = 0
++    
++      while !trx.eof?
++        cmd = trx.read(1)
++        case cmd
++        when TRX_CMD_PUSH
++          logger.debug ">"
++          raw_size = trx.read(4)
++          next unless raw_size
++          size = raw_size.unpack("I").first
++          value = self.trx.read(size)
++          next unless value
++          self.data.push(value)
++          bytes_read += value.size
++        when TRX_CMD_POP
++          logger.debug "<"
++          bytes_read -= self.data.pop.size
++        else
++          logger.error "Error reading queue: " +
++                       "I don't understand '#{cmd}' (skipping)."
++        end
+       end
++    
++      bytes_read
      end
 -    exit
 +    
@@ -647,655 +687,3 @@
 \ No newline at end of file
 +end
 \ No newline at end of file
-Index: sparrow_patch.rb
-===================================================================
---- sparrow_patch.rb	(revision 32)
-+++ sparrow_patch.rb	(working copy)
-@@ -10,67 +10,368 @@
-  require 'fileutils'
-  require 'optparse'
-  
--@@ -104,7 +103,6 @@
-+@@ -91,380 +90,306 @@
-+   class SparrowError < StandardError #:nodoc:
-    end
-    
-+-  class NoMoreMessages < SparrowError #:nodoc:
-+-  end
-+-  
-+-  class ClientError < SparrowError #:nodoc:
-+-  end
-+-  
-+-  class StatementInvalid < ClientError #:nodoc:
-+-  end
-+-  
-+-  class InvalidBodyLength < ClientError #:nodoc:
-+-  end
-+-  
-    BASE_DIR        = File.join(File.dirname(__FILE__), 'base')
- -  PROCESSING_DIR  = File.join(File.dirname(__FILE__), 'processing')
-    LOG_DIR         = File.join(File.dirname(__FILE__), 'log')
-    
--   CR              = "\r\n"
--@@ -133,9 +131,29 @@
--   QUIT_REGEX      = /\AQUIT/i
--   FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
--   VERSION_REGEX   = /\AVERSION/i
-+-  CR              = "\r\n"
-+-  ERROR           = "ERROR"
-+-  OK              = "OK"
-+-  EOF             = "END"
-++  module Server
-++    
-++    class NoMoreMessages < SparrowError #:nodoc:
-++    end
-++
-++    class ClientError < SparrowError #:nodoc:
-++    end
-++
-++    class StatementInvalid < ClientError #:nodoc:
-++    end
-++
-++    class InvalidBodyLength < ClientError #:nodoc:
-++    end
- +  
--+  # Shamelessly utilised from Starling
--+  TRX_CMD_PUSH = "\000".freeze
--+  TRX_CMD_POP = "\001".freeze
-++    CR              = "\r\n"
-++    ERROR           = "ERROR"
-++    OK              = "OK"
-++    EOF             = "END"
-+                
-+-  CLIENT_ERROR    = "CLIENT_ERROR"
-+-  SERVER_ERROR    = "SERVER_ERROR"
-++    CLIENT_ERROR    = "CLIENT_ERROR"
-++    SERVER_ERROR    = "SERVER_ERROR"
-+                
-+-  STORED          = "STORED"
-+-  NOT_STORED      = "NOT_STORED"
-++    STORED          = "STORED"
-++    NOT_STORED      = "NOT_STORED"
-+                
-+-  DELETED         = "DELETED"
-+-  NOT_FOUND       = "NOT_FOUND"
-++    DELETED         = "DELETED"
-++    NOT_FOUND       = "NOT_FOUND"
-+   
-+-  VALUE           = "VALUE"
-++    VALUE           = "VALUE"
-+   
-+-  VERSION         = "VERSION"
-++    VERSION         = "VERSION"
-+     
-+-  SET_REGEX       = /\ASET\s/i
-+-  ADD_REGEX       = /\AADD\s/i
-+-  REPLACE_REGEX   = /\AREPLACE\s/i
-+-  DELETE_REGEX    = /\ADELETE\s/i
-+-  GET_REGEX       = /\AGET\s/i
-+-  QUIT_REGEX      = /\AQUIT/i
-+-  FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
-+-  VERSION_REGEX   = /\AVERSION/i
-++    SET_REGEX       = /\ASET\s/i
-++    ADD_REGEX       = /\AADD\s/i
-++    REPLACE_REGEX   = /\AREPLACE\s/i
-++    DELETE_REGEX    = /\ADELETE\s/i
-++    GET_REGEX       = /\AGET\s/i
-++    QUIT_REGEX      = /\AQUIT/i
-++    FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
-++    VERSION_REGEX   = /\AVERSION/i
-++  
-++    # Shamelessly utilised from Starling
-++    TRX_CMD_PUSH = "\000".freeze
-++    TRX_CMD_POP = "\001".freeze
-  
--+  TRX_PUSH = "\000%s%s".freeze
--+  TRX_POP = "\001".freeze
-+-  def post_init
-+-    @data = ""
-+-    @current_queue = nil
-+-    @expecting_body = false
-+-    @expected_bytes = 0
-+-    @current_flag = nil
-+-    logger.debug "New client"
-+-  end
-++    TRX_PUSH = "\000%s%s".freeze
-++    TRX_POP = "\001".freeze
-+   
-+-  def receive_data ln
-+-    logger.debug "Receiving data: #{ln}"
-+-    if ln[-2..-1].include?(CR)
-+-      ln.split(CR).each do |ln|
-+-        @data << ln
-+-        if ln =~ SET_REGEX
-+-          set_command
-+-        elsif ln =~ ADD_REGEX
-+-          add_command
-+-        elsif ln =~ REPLACE_REGEX
-+-          replace_command
-+-        elsif ln =~ GET_REGEX
-+-          get_command
-+-        elsif ln =~ DELETE_REGEX
-+-          delete_command
-+-        elsif ln =~ QUIT_REGEX
-+-          quit_command
-+-        elsif ln =~ VERSION_REGEX
-+-          version_command
-+-        elsif ln =~ FLUSH_ALL_REGEX
-+-          flush_all_command
-+-        elsif @expecting_body
-+-          process_body
-+-        else
-+-          raise StatementInvalid
-++    @@queues = {}
- +  
--+  @@options = {}
-++    @@options = {}
- +  
--+  def options
--+    @@options
--+  end
-++    def options
-++      @@options
-++    end
- +  
--+  def self.options=(obj)
--+    @@options = obj
--+  end
-++    def self.options=(obj)
-++      @@options = obj
-++    end
- +  
--+  def max_log_size
--+    @@max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
--+  end
-++    def max_log_size
-++      @@max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
-++    end
- +
--   def post_init
---    @data = ""
--     @current_queue = nil
--     @expecting_body = false
--     @expected_bytes = 0
--@@ -147,7 +165,7 @@
--     logger.debug "Receiving data: #{ln}"
--     if ln[-2..-1].include?(CR)
--       ln.split(CR).each do |ln|
---        @data << ln
--+        @data = ln
--         if ln =~ SET_REGEX
--           set_command
--         elsif ln =~ ADD_REGEX
--@@ -169,7 +187,7 @@
--         else
--           raise StatementInvalid
-++    def post_init
-++      @current_queue = nil
-++      @expecting_body = false
-++      @expected_bytes = 0
-++      @current_flag = nil
-++      logger.debug "New client"
-++    end
-++  
-++    def receive_data ln
-++      logger.debug "Receiving data: #{ln}"
-++      if ln[-2..-1].include?(CR)
-++        ln.split(CR).each do |ln|
-++          @data = ln
-++          if ln =~ SET_REGEX
-++            set_command
-++          elsif ln =~ ADD_REGEX
-++            add_command
-++          elsif ln =~ REPLACE_REGEX
-++            replace_command
-++          elsif ln =~ GET_REGEX
-++            get_command
-++          elsif ln =~ DELETE_REGEX
-++            delete_command
-++          elsif ln =~ QUIT_REGEX
-++            quit_command
-++          elsif ln =~ VERSION_REGEX
-++            version_command
-++          elsif ln =~ FLUSH_ALL_REGEX
-++            flush_all_command
-++          elsif @expecting_body
-++            process_body
-++          else
-++            raise StatementInvalid
-++          end
-++          @data = nil
-++          @split_args = nil
-          end
- -        @data = ''
--+        @data = nil
--         @split_args = nil
-+-        @split_args = nil
-++      else
-++        raise StatementInvalid
-        end
--     else
--@@ -275,196 +293,39 @@
--   # Queue methods
-+-    else
-+-      raise StatementInvalid
-+-    end
-+ 
-+-  rescue ClientError => e
-+-    logger.error e
-+-    publish CLIENT_ERROR, e
-+-    publish ERROR
-+-  rescue => e
-+-    logger.error e
-+-    publish SERVER_ERROR, e
-+-  end
-++    rescue ClientError => e
-++      logger.error e
-++      publish CLIENT_ERROR, e
-++      publish ERROR
-++    rescue => e
-++      logger.error e
-++      publish SERVER_ERROR, e
-++    end
-    
--   def next_message(queue_name)
-+-  def publish *args
-+-    send_data args.join(' ') + CR
-+-  end
-++    def publish *args
-++      send_data args.join(' ') + CR
-++    end
-+   
-+-  # Storage commands
-++    # Storage commands
-+ 
-+-  # <command name> <key> <flags> <exptime> <bytes>\r\n
-+-  def set_command
-+-    @current_queue = args[1]
-+-    @current_flag = args[2] || 0
-+-    raise ClientError unless @current_queue
-+-    @expected_bytes = args[4].to_i || 0
-+-    @expecting_body = true
-+-  end
-+-  alias add_command set_command
-+-  alias replace_command set_command
-++    # <command name> <key> <flags> <exptime> <bytes>\r\n
-++    def set_command
-++      @current_queue = args[1]
-++      @current_flag = args[2] || 0
-++      raise ClientError unless @current_queue
-++      @expected_bytes = args[4].to_i || 0
-++      @expecting_body = true
-++    end
-++    alias add_command set_command
-++    alias replace_command set_command
-+   
-+-  def process_body
-+-    if @data.length != @expected_bytes
-+-     raise InvalidBodyLength
-++    def process_body
-++      if @data.length != @expected_bytes
-++       raise InvalidBodyLength
-++      end
-++      @data << @current_flag
-++      logger.debug "Adding message to queue - #{@current_queue}"
-++      add_message(@current_queue, @data)
-++      @expected_bytes = 0
-++      @current_queue = nil
-++      @expecting_body = false
-++      publish STORED
-+     end
-+-    @data << @current_flag
-+-    logger.debug "Adding message to queue - #{@current_queue}"
-+-    add_message(@current_queue, @data)
-+-    @expected_bytes = 0
-+-    @current_queue = nil
-+-    @expecting_body = false
-+-    publish STORED
-+-  end
-+ 
-+-  # Retrieval commands
-++    # Retrieval commands
-+   
-+-  # GET <key>*r\n
-+-  def get_command
-+-    args.shift # get rid of the command
-+-    raise ClientError if args.empty?
-+-    rsp = []
-+-    args.each do |queue|
-+-      begin
-+-        logger.debug "Getting message from queue - #{queue}"
-+-        msg = next_message(queue)
-+-      rescue NoMoreMessages
-+-        next
-++    # GET <key>*r\n
-++    def get_command
-++      args.shift # get rid of the command
-++      raise ClientError if args.empty?
-++      rsp = []
-++      args.each do |queue|
-++        begin
-++          logger.debug "Getting message from queue - #{queue}"
-++          msg = next_message(queue)
-++        rescue NoMoreMessages
-++          next
-++        end
-++        flag = msg[-1..-1]
-++        msg = msg[0..-2]
-++        rsp << [VALUE, queue, flag, msg.length].join(' ')
-++        rsp << msg
-+       end
-+-      flag = msg[-1..-1]
-+-      msg = msg[0..-2]
-+-      rsp << [VALUE, queue, flag, msg.length].join(' ')
-+-      rsp << msg
-++      rsp << EOF
-++      send_data(rsp.join(CR) + CR)
-+     end
-+-    rsp << EOF
-+-    send_data(rsp.join(CR) + CR)
-+-  end
-+   
-+-  # Other commands
-++    # Other commands
-+   
-+-  # DELETE <key> <time>\r\n
-+-  def delete_command
-+-    path  = File.join(BASE_DIR, args[1])
-+-    if File.exists?(path) or !args[1]
-+-      logger.info "Deleting queue - #{args[1]}"
-+-      FileUtils.rm_rf path
-+-      publish DELETED
-+-    else
-+-      publish NOT_FOUND
-++    # DELETE <key> <time>\r\n
-++    def delete_command
-++      path  = File.join(BASE_DIR, args[1])
-++      if File.exists?(path) or !args[1]
-++        logger.info "Deleting queue - #{args[1]}"
-++        FileUtils.rm_rf path
-++        publish DELETED
-++      else
-++        publish NOT_FOUND
-++      end
-+     end
-+-  end
-+   
-+-  # FLUSH_ALL
-+-  def flush_all_command
-+-    logger.info "Flushing all queues"
-+-    FileUtils.rm_rf BASE_DIR
-+-    publish OK
-+-  end
-++    # FLUSH_ALL
-++    def flush_all_command
-++      logger.info "Flushing all queues"
-++      FileUtils.rm_rf BASE_DIR
-++      publish OK
-++    end
-+   
-+-  # VERSION
-+-  def version_command
-+-    publish VERSION, '0.1'
-+-  end
-++    # VERSION
-++    def version_command
-++      publish VERSION, '0.1'
-++    end
-+   
-+-  # QUIT
-+-  def quit_command
-+-    logger.debug "Closing connection"
-+-    close_connection
-+-  end
-++    # QUIT
-++    def quit_command
-++      logger.debug "Closing connection"
-++      close_connection
-++    end
-+ 
-+-  protected
-++    protected
-+   
-+-  # Queue methods
-++    # Queue methods
-+   
-+-  def next_message(queue_name)
- -    while path = find_next_file(queue_name)
- -      next unless File.exists?(path)
- -      begin
-@@ -87,9 +388,10 @@
- -        logger.debug "Error reading next message:" + e
- -        next 
- -      end
---    end
--+    
--   end
-++    def next_message(queue_name)
-++      Queue.get_queue(queue_name).pop
-+     end
-+-  end
-    
- -  def find_next_file(queue_name)
- -    @@files ||= []
-@@ -97,41 +399,46 @@
- -    @@files = Dir.glob(File.join(BASE_DIR, queue_name, '*', '*', '*', '*', '*.msg'))
- -    raise NoMoreMessages if @@files.empty?
- -    @@files.pop
--+  def add_message(queue_name, value)
--+    size = [value.size].pack("I")
--+    sprintf(TRX_PUSH, size, value)
--   end
-+-  end
- -  
- -  def add_message(queue_name, msg)
- -    name = File.join(BASE_DIR, new_file_name(queue_name))
- -    FileUtils.mkdir_p(File.dirname(name))
- -    File.open(name, 'w+') do |file|
- -      file.write msg
---    end
-++    def add_message(queue_name, value)
-++      Queue.get_queue(queue_name).push(value)
-+     end
- -    true
- -  end
-      
--   def logger
--     return @@logger if defined?(@@loggger)
--     FileUtils.mkdir_p(LOG_DIR)
--     @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
-+-  def logger
-+-    return @@logger if defined?(@@loggger)
-+-    FileUtils.mkdir_p(LOG_DIR)
-+-    @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
- -    @@logger.level = Logger::INFO if !SparrowRunner.debug
--+    @@logger.level = Logger::INFO if !options[:debug]
--     @@logger
--   rescue
--     @@logger = Logger.new(STDOUT)
--   end
-+-    @@logger
-+-  rescue
-+-    @@logger = Logger.new(STDOUT)
-+-  end
-++    def logger
-++      return @@logger if defined?(@@loggger)
-++      FileUtils.mkdir_p(LOG_DIR)
-++      @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
-++      @@logger.level = Logger::INFO if !options[:debug]
-++      @@logger
-++    rescue
-++      @@logger = Logger.new(STDOUT)
-++    end
-    
- -  def self.clear!
- -    logger.info "Clearing queue"
- -    flush_all_command
- -    FileUtils.rm_rf PROCESSING_DIR
- -    true
--+  def reopen_queue(queue_name)
--+    @queue = File.new(File.join(BASE_DIR, queue_name), File::CREAT|File::RDWR)
--+    @queue_size = File.size()
--   end
---  
-+-  end
-++    private
-+   
- -  def cleanup(folder_path)
- -    dir = Dir.new(folder_path)
- -    # first two are ['.', '..']
-@@ -146,26 +453,23 @@
- -    # Usually would happen if cleanup is done
- -    # simultaneously by another Sparrow instance
- -  end
-- 
-+-
- -  private
---  
-++    def args
-++      @split_args ||= @data.split(' ')
-++    end
-+   
- -  def new_file_name(queue_name)
- -    guid = UUID.new
- -    hex = Digest::MD5.hexdigest(guid.to_s)
- -    time = Time.now
- -    path = time.strftime("%Y%m%d%H%M").scan(/..../)
- -    File.join(queue_name, path, time.strftime("%S"), hex + '.msg')
--+  def rotate_queue(queue_name)
--+    @queue.close
--+    File.rename(File.join(BASE_DIR, queue_name), "#{queue_name}.#{Time.now.to_i}")
--+    reopen_queue(queue_name)
-    end
-    
--+  private
--+  
--   def args
--     @split_args ||= @data.split(' ')
--   end
-+-  def args
-+-    @split_args ||= @data.split(' ')
-+-  end
- -
- -end
- -
-@@ -190,16 +494,26 @@
- -    else
- -      self.options[:port].upto(self.options[:port] + self.options[:cluster] - 1) do |n|
- -        daemonize(n, self.options)
---      end
---    end
-++  class Queue
-++    @@queues = {}
-++    class << self
-++      def get_queue(queue_name)
-++        @@queues[queue_name] ||= []
-+       end
-+     end
- -  end
--   
-+-  
- -  def self.debug
- -    @@debug
- -  end
- -  
- -  protected
---
-++    
-++    def reopen_queue(queue_name)
-++      @queue = File.new(File.join(BASE_DIR, queue_name), File::CREAT|File::RDWR)
-++      @queue_size = File.size()
-++    end
-+ 
- -  def parse_options(ops = {})
- -    self.options = {}
- -    OptionParser.new do |opts|
-@@ -207,7 +521,54 @@
- -
- -      opts.on("-p", "--port [number]", Integer, "Specify Port") do |v|
- -        self.options[:port] = v
---      end
-++    def rotate_queue(queue_name)
-++      @queue.close
-++      File.rename(File.join(BASE_DIR, queue_name), "#{queue_name}.#{Time.now.to_i}")
-++      reopen_queue(queue_name)
-++    end
-++    
-++    def push
-++      size = [value.size].pack("I")
-++      data = sprintf(TRX_PUSH, size, value)
-++      @@trxs[queue_name] ||= open_queue(queue_name)
-++      @@queues[queue_name].push value
-++      @@trxs[queue_name].write data
-++      @@trxs[queue_name].fsync
-++    end
-++    
-++    def pop
-++      @@trxs[queue_name] ||= open_queue(queue_name)
-++      data = @@queues[queue_name].pop
-++      @@trxs[queue_name].write "\001"
-++      @@trxs[queue_name].fsync
-++      data
-++    end
-++    
-++    def replay_queue(queue_name)
-++      trx = open_queue(queue_name)
-++      bytes_read = 0
-++      @@queues[queue_name] ||= []
-++    
-++      while !trx.eof?
-++        cmd = trx.read(1)
-++        case cmd
-++        when TRX_CMD_PUSH
-++          logger.debug ">"
-++          raw_size = trx.read(4)
-++          next unless raw_size
-++          size = raw_size.unpack("I").first
-++          data = trx.read(size)
-++          next unless data
-++          @@queues[queue_name].push(data)
-++          bytes_read += data.size
-++        when TRX_CMD_POP
-++          logger.debug "<"
-++          bytes_read -= @@queue[queue_name].pop.size
-++        else
-++          logger.error "Error reading queue: " +
-++                       "I don't understand '#{cmd}' (skipping)."
-++        end
-+       end
- -      
- -      opts.on("-c", "--cluster [cluster size]", Integer, "Create a cluster of daemons") do |v|
- -        self.options[:cluster] = v 
-@@ -232,13 +593,15 @@
- -    self.options.merge!(ops)
- -    self.options
- -  end
---    
-+     
- -  def start(port, options = {})
- -   EventMachine::run {
- -     EventMachine::start_server options[:host], port.to_i, Sparrow
- -   }
- -  end
---  
-++      bytes_read
-++    end
-+   
- -  private
- -
- -  def store_pid(pid, port)
-@@ -257,9 +620,12 @@
- -      rescue => e
- -        puts "Failed to kill! #{k}: #{e}"
- -      end
---    end
-++    def open_queue(queue_name)
-++      File.new(File.join(BASE_DIR, queue_name), File::CREAT|File::RDWR)
-+     end
- -    exit
---  end
-++    
-+   end
- -
- -  def daemonize(port, options)
- -   fork do
