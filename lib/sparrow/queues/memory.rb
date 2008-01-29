module Sparrow
  module Queues
    class Memory
      include Sparrow::Miscel
      
      attr_accessor :queue_name
      attr_accessor :queue_data
      attr_accessor :count_pop
      attr_accessor :count_push
      
      def initialize(queue_name)
        self.queue_name = queue_name
        self.queue_data = []
        self.count_pop = 0
        self.count_push = 0
      end
      
      def pop
        self.count_pop += 1
        queue_data.shift
      end
      
      def push(value)
        self.count_push += 1
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