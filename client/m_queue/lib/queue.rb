module MQueue
  class Queue
    class MQueueError < StandardError; end #:nodoc:
    class NoServersLeft < MQueueError; end #:nodoc:
    
    PID_DIR = File.join(MQUEUE_ROOT, 'tmp', 'pids')
    LOG_DIR = File.join(MQUEUE_ROOT, 'log')
    
    class << self
      def run(use_daemonize = false)
        puts "Starting #{queue_name} up..."
        could_daemonize(use_daemonize) {
          num_sleeps = 0
          loop do
            msg = poll
            self.new.process(msg) if msg
            if !msg
              if num_sleeps < 50
                num_sleeps += 1
                num_sleeps *= 2
              end
              sleep num_sleeps
            else
              num_sleeps = 0
            end
          end
        }
      end
    
      def publish(msg)
        send_to_server {|server|
          server[queue_name] = msg.to_yaml
        }
        msg
      end
    
      def poll
        send_to_server(true) {|server|
          begin
            server[queue_name]
          rescue => e
            puts e
          end
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
      
      def reload!
        servers.each do |s|
          s.reload! if s.respond_to?('reload!')
        end
      end
      
      private
      
      def send_to_server(random = false, &block)
        srvs = random ? unsorted_servers : sorted_servers
        srvs.each do |server|
          begin
            return yield(server)
          rescue => e
            # Server will be marked dead
            next
          end
        end
        raise NoServersLeft
      end
      
      # Daemon stuff
      
      def store_pid(pid)
       FileUtils.mkdir_p(PID_DIR)
       File.open(File.join(PID_DIR, "poller.#{queue_name}.pid"), 'w'){|f| f.write("#{pid}\n")}
      end

      def kill_pid(name = queue_name)
        Dir[File.join(PID_DIR, "poller.#{queue_name}.pid")].each do |f|
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
      
      def kill_all!
        kill_pid('*')
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
        servers.select {|a| a.alive? }.sort_by {|b| rand }
      end
      alias unsorted_servers alive_servers
      
      def sorted_servers
        alive_servers.sort_by {|b| b.weight }.reverse
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
     return @logger if @logger
     FileUtils.mkdir_p(LOG_DIR)
     @logger = Logger.new(File.join(LOG_DIR, self.class.queue_name + '.log'))     
     @logger
    end

    def retry_attempts
      0
    end
    
    def on_message(msg)
      raise 'You must implement on_message.'
    end
    
  end
end