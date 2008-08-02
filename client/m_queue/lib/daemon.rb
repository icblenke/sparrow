module MQueue
  class Daemon
    PID_DIR = File.join(MQUEUE_ROOT, 'tmp', 'pids')
    class << self
      
      def daemonize!(name)
        fork do
          Process.setsid
          exit if fork
          store_pid(Process.pid, name)
          Dir.chdir File.dirname(__FILE__)
          File.umask 0000
          STDIN.reopen "/dev/null"
          STDOUT.reopen "/dev/null", "a"
          STDERR.reopen STDOUT
          trap("TERM") { exit }
          yield if block_given?
        end
      end
      
      def kill!(name)
        Dir[pid_path(name)].each do |f|
          begin
            puts f
            pid = IO.read(f).chomp.to_i
            FileUtils.rm f
            Process.kill(15, pid) # TERM
            puts "killed PID: #{pid}"
          rescue => e
            puts "Failed to kill! #{f}: #{e}"
          end
        end
        true
      end
      
      def kill_all!
        kill!('*')
      end
      
    private
      
      def store_pid(pid, name)
       FileUtils.mkdir_p(PID_DIR)
       File.open(pid_path(name), 'w'){|f| 
         f.write("#{pid}\n")
       }
      end
      
      def pid_path(queue_name)
        File.join(PID_DIR, "poller.#{queue_name}.pid")
      end
      
    end
  end
end