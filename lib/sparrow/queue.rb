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
    end
    
  end
end