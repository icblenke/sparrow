require 'memcache'
require 'logger'
require 'fileutils'

# See mq3.rb for a better client

class MemCacheQueue
  PID_DIR = File.join(File.dirname(__FILE__), '..', 'pids')
  LOG_DIR = File.join(File.dirname(__FILE__), '..', 'log')
  
  class << self
    def publish(msg)
      @@memcache[queue_name] = msg.to_yaml
      msg
    end
    
    def connect(*hosts)
      @@memcache = MemCache.new(hosts)
    end
    
    def run(use_demonize = false)
      child.run(use_demonize)
    end
    
    def destroy!
      @@memcache.delete(queue_name)
    end
    
    def destroy_all!
      @@memcache.flush_all
    end
    
    def queue_name
      self.name.gsub(/::/, '/').
        gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
        gsub(/([a-z\d])([A-Z])/,'\1_\2').
        tr("-", "_").
        downcase
    end
    
    def kill!
      child.kill_pid('*')
    end
    
    private
    
    def child
      @@child ||= {}
      @@child[queue_name] ||= self.new
    end
  end
  
  def receive(msg)
    1.upto(retry_attempts + 1) do |n|
      begin
        self.args = YAML.load(msg)
        on_message
        break
      rescue => e
        if n == retry_attempts + 1
          logger.fatal "PID #{Process.pid}: Permanently failed: " + e
        else
          logger.error "PID #{Process.pid}: Retry number #{n}: " + e
        end
      end
    end
  end
  
  def run(use_daemonize = false)
    could_daemonize(use_daemonize) do 
      logger.info "Receiving messages..."
      loop do
        msg = next_message
        if msg
          logger.debug "Processing message"
          receive(msg)
          logger.debug "Processed message"
        end
        sleep(delay) if delay
      end
     end
  end
  
  def next_message
    @@memcache[queue_name]
  end
  
  def retry_attempts
    0
  end
  
  def delay
    nil
  end
  
  def logger
    return @logger if @logger
    FileUtils.mkdir_p(LOG_DIR)
    @logger = Logger.new(File.join(LOG_DIR, "#{log_name}.log"))
    @logger.level = logger_level
    @logger
  rescue
    @logger = Logger.new(STDOUT)
  end
  
  attr_accessor :args

  def on_message
    raise 'You must implement on_message.'
  end
  
  def queue_name
    self.class.queue_name
  end
  alias log_name queue_name
  
  def logger_level
    Logger::INFO
  end
  
  def store_pid(pid)
   FileUtils.mkdir_p(PID_DIR)
   File.open(File.join(PID_DIR, "#{queue_name}.#{pid}.pid"), 'w'){|f| f.write("#{pid}\n")}
  end
  
  def kill_pid(k)
    begin
      Dir[File.join(PID_DIR, "#{queue_name}.#{k}.pid")].each do |f|
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
  
  protected
  
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