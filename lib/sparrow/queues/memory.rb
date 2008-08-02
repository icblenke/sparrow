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
        recover!
      end
      
      def pop
        self.count_pop += 1
        self.queue_data.shift
      end
      
      def push(value)
        self.count_push += 1
        self.queue_data.push(value)
      end
      
      def clear!
        self.queue_data = []
        self.sqlite.clear!
      end
      
      def count
        queue_data.length
      end
      
      def to_disk!
        copy = self.queue_data.dup
        self.sqlite.insert(copy)
        self.queue_data = self.queue_data - copy
      end
      
      def shutdown!
        self.to_disk!
      end
      
      def recover!
        logger.debug "Recovering queue"
        self.queue_data.concat(self.sqlite.all)
      end
      
      def sqlite
        @sqlite ||= Sparrow::Queues::Sqlite.new(self.queue_name)
      end
      
    end
  end
end