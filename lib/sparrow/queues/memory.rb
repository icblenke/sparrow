module Sparrow
  module Queues
    class Memory
      include Sparrow::Miscel
      
      attr_accessor :queue_name
      attr_accessor :queue_data
      attr_accessor :count_all
      
      def initialize(queue_name)
        self.queue_name = queue_name
        self.queue_data = []
        self.count_all = 0
      end
      
      def pop
        queue_data.shift
      end
      
      def push(value)
        self.count_all += 1
        queue_data.push(value)
      end
      
      def clear
        self.queue_data = []
      end
      
      def count
        queue_data.length
      end
         
    end
  end
end