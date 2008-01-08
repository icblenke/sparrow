module MQ3
  module Protocols
    class SQS
      attr_accessor :options
      
      def initialize(opts = {})
        self.options = opts
      end
      
      def []=(queue_name, msg)
        queue(queue_name).send_message(msg)
        msg
      rescue => e
        raise ConnectionError
      end
      
      def []
        msg = queue(q).receive_message
        msg.body if msg
      end
      
      def delete(queue_name)
        queue(queue_name).delete!
      end
  
      def delete!
        SQS.each_queue do |q|
          q.delete!
        end
      end
      
      # MQ3 Protocol API
  
      def alive?
        true
      end
      
      def weight
  		  options[:weight] || 0  
      end
      
      private
      
      def queue(queue_name)
        @queues ||= {}
        @queues[queue_name] ||=
          begin
            SQS.get_queue(q)
          rescue SQS::UnavailableQueue
            SQS.create_queue(q)
          end
      end
    
  end
end