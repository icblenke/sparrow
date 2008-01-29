require 'fileutils'
module Sparrow
  class Queue
    
    cattr_accessor :queues
    self.queues = {}
    
    class << self
      def get_queue(queue_name)
        @@queues[queue_name] ||= case Sparrow.options[:type]
          when 'memory': Sparrow::Queues::Memory.new(queue_name)
          when 'sqlite': Sparrow::Queues::Sqlite.new(queue_name)
          else
            Sparrow::Queues::Disk.new(queue_name)
          end
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
        @@queues = {}
        FileUtils.rm_rf base_dir
        FileUtils.mkdir_p base_dir
      end
      
      def get_stats(queue_name)
        stats = {
          :type =>              Sparrow.options[:type],
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
            :bytes =>           Dir.glob(File.join(Sparrow.base_dir, queue_name + '**')).inject(0){|a, b| a += (File.size?(b) || 0); a },
            :total_items =>     queue.count_push, 
            :curr_items =>      queue.count
          })
        end
        stats
      end  
    end
    
  end
end