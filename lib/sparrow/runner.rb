require 'optparse'
require 'eventmachine'

module Sparrow
  class Runner
    include Sparrow::Miscel
    
    class << self
      def run
        self.new
      end
    end
    
    def initialize
      self.options = {
        :host => "0.0.0.0",
        :port => 11212,
        :debug => false,
        :base_dir => base_dir,
        :pid_path => pid_path,
        :log_path => log_path,
        :type => 'disk'
      }
      
      parse_options
      
      if options.include?(:kill)
        kill_pid(options[:kill] || '*')
      end
      
      if !options[:daemonize]
        start
      else
        daemonize
      end
    end
    
    def start
      puts "Starting Sparrow server on port: #{options[:port]}..."
      
      trap("INT") {
        stop
        exit
      }
      trap("TERM"){
        stop
        exit
      }
      
      EventMachine::run {
        EventMachine::start_server(options[:host], options[:port].to_i, Sparrow::Server)
      }
    end
    
    def stop
      puts "Stopping Eventmachine Server"
      EventMachine::stop
    end
    
    def parse_options
      OptionParser.new do |opts|
        opts.summary_width = 25
        opts.banner = "Sparrow (#{VERSION})\n\n",
                      "Usage: sparrow [-b path] [-t type] [-h host] [-p port] [-P file]\n",
                      "               [-d] [-k port] [-l file] [-e]\n",
                      "       sparrow --help\n",
                      "       sparrow --version\n"
        
        opts.separator ""
        opts.separator ""; opts.separator "Configuration:"
        
        opts.on("-b", "--base PATH", String, "Path to queue data store.", "(default: #{options[:base_dir]})") do |v|
          options[:base_dir] = File.expand_path(v)
        end
        
        opts.on("-t", "--type QUEUE_TYPE", String, "Type of queue (disk/memory/sqlite).", "(default: #{options[:type]})") do |v|
          options[:type] = v
        end
        
        opts.separator ""; opts.separator "Network:"
        
        opts.on("-h", "--host HOST", String, "Specify host", "(default: #{options[:host]})") do |v|
          options[:host] = v
        end
        
        opts.on("-p", "--port PORT", Integer, "Specify port", "(default: #{options[:port]})") do |v|
          options[:port] = v
        end
        
        opts.separator ""; opts.separator "Daemonization:"
        
        opts.on("-P", "--pid FILE", String, "save PID in FILE when using -d option.", "(default: #{options[:pid_path]})") do |v|
          options[:pid_path] = File.expand_path(v)
        end
        
        opts.on("-d", "--daemon", "Daemonize mode") do |v|
          options[:daemonize] = v
        end

        opts.on("-k", "--kill PORT", String, "Kill specified running daemons - leave blank to kill all.") do |v|
          options[:kill] = v
        end
        
        opts.separator ""; opts.separator "Logging:"
        
        opts.on("-l", "--log [FILE]", String, "Path to print debugging information.") do |v|
          options[:log_path] = File.expand_path(v)
        end
        
        opts.on("-e", "--debug", "Run in debug mode", "(default: #{options[:debug]})") do |v|
          options[:debug] = v
        end
        
        opts.separator ""; opts.separator "Miscellaneous:"
        
        opts.on_tail("-?", "--help", "Display this usage information.") do
          puts "#{opts}\n"
          exit
        end
        
        opts.on_tail("-v", "--version", "Display version") do |v|
          puts "Sparrow #{VERSION}"
          exit
        end
      end.parse!
      options
    end
    
    private
    
    def store_pid(pid)
     FileUtils.mkdir_p(File.dirname(pid_path))
     File.open(pid_path, 'w'){|f| f.write("#{pid}\n")}
    end

    def kill_pid(k)
      Dir[options[:pid_path]||File.join(File.dirname(pid_dir), "sparrow.#{k}.pid")].each do |f|
        begin
        puts f
        pid = IO.read(f).chomp.to_i
        FileUtils.rm f
        Process.kill(9, pid)
        puts "killed PID: #{pid}"
        rescue => e
          puts "Failed to kill! #{k}: #{e}"
        end
      end
      exit
    end

    def daemonize
     fork do
       Process.setsid
       exit if fork
       store_pid(Process.pid)
       # Dir.chdir "/" # Mucks up logs
       File.umask 0000
       STDIN.reopen "/dev/null"
       STDOUT.reopen "/dev/null", "a"
       STDERR.reopen STDOUT
       start
     end
    end
    
  end
end