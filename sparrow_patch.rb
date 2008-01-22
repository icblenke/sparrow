Index: sparrow
===================================================================
--- sparrow	(revision 24)
+++ sparrow	(working copy)
@@ -82,7 +82,6 @@
 
 require 'rubygems'
 require 'eventmachine'
-require 'uuid'
 require 'fileutils'
 require 'optparse'
 
@@ -104,7 +103,6 @@
   end
   
   BASE_DIR        = File.join(File.dirname(__FILE__), 'base')
-  PROCESSING_DIR  = File.join(File.dirname(__FILE__), 'processing')
   LOG_DIR         = File.join(File.dirname(__FILE__), 'log')
   
   CR              = "\r\n"
@@ -133,9 +131,29 @@
   QUIT_REGEX      = /\AQUIT/i
   FLUSH_ALL_REGEX = /\AFLUSH_ALL/i
   VERSION_REGEX   = /\AVERSION/i
+  
+  # Shamelessly utilised from Starling
+  TRX_CMD_PUSH = "\000".freeze
+  TRX_CMD_POP = "\001".freeze
 
+  TRX_PUSH = "\000%s%s".freeze
+  TRX_POP = "\001".freeze
+  
+  @@options = {}
+  
+  def options
+    @@options
+  end
+  
+  def self.options=(obj)
+    @@options = obj
+  end
+  
+  def max_log_size
+    @@max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
+  end
+
   def post_init
-    @data = ""
     @current_queue = nil
     @expecting_body = false
     @expected_bytes = 0
@@ -147,7 +165,7 @@
     logger.debug "Receiving data: #{ln}"
     if ln[-2..-1].include?(CR)
       ln.split(CR).each do |ln|
-        @data << ln
+        @data = ln
         if ln =~ SET_REGEX
           set_command
         elsif ln =~ ADD_REGEX
@@ -169,7 +187,7 @@
         else
           raise StatementInvalid
         end
-        @data = ''
+        @data = nil
         @split_args = nil
       end
     else
@@ -275,196 +293,39 @@
   # Queue methods
   
   def next_message(queue_name)
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
-    end
+    
   end
   
-  def find_next_file(queue_name)
-    @@files ||= []
-    return @@files.pop unless @@files.empty?
-    @@files = Dir.glob(File.join(BASE_DIR, queue_name, '*', '*', '*', '*', '*.msg'))
-    raise NoMoreMessages if @@files.empty?
-    @@files.pop
+  def add_message(queue_name, value)
+    size = [value.size].pack("I")
+    sprintf(TRX_PUSH, size, value)
   end
-  
-  def add_message(queue_name, msg)
-    name = File.join(BASE_DIR, new_file_name(queue_name))
-    FileUtils.mkdir_p(File.dirname(name))
-    File.open(name, 'w+') do |file|
-      file.write msg
-    end
-    true
-  end
     
   def logger
     return @@logger if defined?(@@loggger)
     FileUtils.mkdir_p(LOG_DIR)
     @@logger = Logger.new(File.join(LOG_DIR, 'sparrow.log'))
-    @@logger.level = Logger::INFO if !SparrowRunner.debug
+    @@logger.level = Logger::INFO if !options[:debug]
     @@logger
   rescue
     @@logger = Logger.new(STDOUT)
   end
   
-  def self.clear!
-    logger.info "Clearing queue"
-    flush_all_command
-    FileUtils.rm_rf PROCESSING_DIR
-    true
+  def reopen_queue(queue_name)
+    @queue = File.new(File.join(BASE_DIR, queue_name), File::CREAT|File::RDWR)
+    @queue_size = File.size()
   end
-  
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
 
-  private
-  
-  def new_file_name(queue_name)
-    guid = UUID.new
-    hex = Digest::MD5.hexdigest(guid.to_s)
-    time = Time.now
-    path = time.strftime("%Y%m%d%H%M").scan(/..../)
-    File.join(queue_name, path, time.strftime("%S"), hex + '.msg')
+  def rotate_queue(queue_name)
+    @queue.close
+    File.rename(File.join(BASE_DIR, queue_name), "#{queue_name}.#{Time.now.to_i}")
+    reopen_queue(queue_name)
   end
   
+  private
+  
   def args
     @split_args ||= @data.split(' ')
   end
-
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
-    
-    if !options[:daemonize] and !(self.options[:cluster] > 1)
-      start(self.options[:port], self.options)
-    else
-      self.options[:port].upto(self.options[:port] + self.options[:cluster] - 1) do |n|
-        daemonize(n, self.options)
-      end
-    end
-  end
   
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
-    
-  def start(port, options = {})
-   EventMachine::run {
-     EventMachine::start_server options[:host], port.to_i, Sparrow
-   }
-  end
-  
-  private
-
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
-      end
-    end
-    exit
-  end
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
