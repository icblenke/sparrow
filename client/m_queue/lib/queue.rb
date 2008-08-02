module MQueue
  class Queue
    class MQueueError < StandardError; end #:nodoc:
    class NoServersLeft < MQueueError; end #:nodoc:
    @@queues = []
    
    LOG_DIR = File.join(MQUEUE_ROOT, 'log')
    
    class << self
      def inherited(subclass)
        @@queues << subclass
      end
      
      def queues
        @@queues
      end
      
      def run
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
      end
    
      def publish(msg)
        if defined?(RAILS_ENV) && 
          (RAILS_ENV == 'development' or
            RAILS_ENV == 'test')
              return self.new.on_message(msg)
        end
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
      
      def alive_servers
        servers.select {|a| a.alive? }.sort_by {|b| rand }
      end
      alias unsorted_servers alive_servers
      
      def sorted_servers
        alive_servers.sort_by {|b| b.weight }.reverse
      end

    end # self
    
    def process(msg)
      reload!
      begin
        on_message(YAML.load(msg))
      rescue => e
        logger.error  "\n#{ e.message } - (#{ e.class })\n" <<  
                      "#{(e.backtrace or []).join("\n")}"
      end
    end

    def logger
     return @logger if @logger
     FileUtils.mkdir_p(LOG_DIR)
     @logger = Logger.new(File.join(LOG_DIR, self.class.queue_name + '.log'))     
     @logger
    end
    
    def on_message(msg)
      raise 'You must implement on_message.'
    end
    
    def reload!
      ActiveRecord::Base.verify_active_connections! if defined?(ActiveRecord)
    end
    
  end
end