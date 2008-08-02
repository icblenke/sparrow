require 'fileutils'
module Sparrow
  class Queue
    
    cattr_accessor :queues
    self.queues = {}
    
    class << self
      def get_queue(queue_name)
        @@queues[queue_name] ||= Sparrow::Queues::Memory.new(queue_name)
      end
    
      def next_message(queue_name)
        self.get_queue(queue_name).pop
      end
    
      def add_message(queue_name, value)
        self.get_queue(queue_name).push(value)
      end
      
      def delete(queue_name)
        queue = self.get_queue(queue_name)
        queue.clear
        @@queues.delete(queue_name)
        true        
      end
      
      def delete_all
        @@queues.each {|name, q| q.clear! }
        @@queues = {}
      end
      
      def shutdown!
        @@queues.each {|name, q| q.shutdown! }
      end
      
      def get_stats(queue_name)
        stats = {
          :type =>              'memory',
          :total_bytes =>       (File.size?(Sparrow.base_dir) || 0),
          :queues  =>           Dir.glob(File.join(Sparrow.base_dir, '*')).collect {|s| File.basename(s) }.join(','),
          :number_of_queues =>  queues.keys.length,
          :debug =>             Sparrow.options[:debug],
          :pid =>               Process.pid,
          :uptime =>            Time.now - Sparrow.options[:start_time],
          :time =>              Time.now.to_i,
          :version =>           Sparrow::VERSION,
          :rusage_user =>       Process.times.utime,
          :rusage_system =>     Process.times.stime
        }
        if queue_name
          queue = get_queue(queue_name)
          stats.merge!({
            :total_items =>     queue.count_push, 
            :curr_items =>      queue.count
          })
        end
        stats
      end  
    end
    
  end
end