require 'sqs'

module MQueue
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
      
      def [](queue_name)
        msg = queue(queue_name).receive_message
        msg.body if msg
      end
            
      def delete(queue_name)
        queue(queue_name).delete!
      end
  
      def delete!
        ::SQS.each_queue do |q|
          q.delete!
        end
      end
      
      # MQueue Protocol API
  
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
            ::SQS.get_queue(queue_name)
          rescue SQS::UnavailableQueue
            ::SQS.create_queue(queue_name)
          end
      end
    end
  end
end