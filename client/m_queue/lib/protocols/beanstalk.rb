require 'beanstalk-client'

module MQueue
  module Protocols
    class Beanstalk
      attr_accessor :options
      
      # MQueue::Protocols::Beanstalk.new([
      #   {
      #     :queue_name => 'EmailProcessor',
      #     :pool => 'localhost:9000'
      #   },
      #   {
      #     :queue_name => 'AssetProcessor',
      #     :pool => 'localhost:9001'
      #   }
      # ])
      
      def initialize(options)
        self.options = options
      end

      def []=(queue_name, msg)
        queue = select_queue(queue_name)
        raise ConnectionError unless queue # try another queue
        server(queue).put(msg)
      end

      def [](queue_name)
        queue = select_queue(queue_name)
        return unless queue
        job = server(queue).peek
        return unless job
        rsp = job.body
        job.delete
        rsp
      end

      def delete(queue_name)
        queue = select_queue(queue_name)
        return unless queue
        queue.delete
      end
      
      def delete!
        self.options.each do |a|
          server(queue).delete
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
      
      def select_queue(queue_name)
        self.options.select {|a| a[:queue_name] == a[:queue_name] }.first
      end
      
      def server(queue)
        @servers ||= {}
        @servers[queue[:pool]] ||= Beanstalk::Pool.new(hostname)
      end
      
    end
  end
end