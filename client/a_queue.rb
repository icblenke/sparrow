require 'socket'

module AQueue
  AQUEUE_ROOT = defined?(RAILS_ROOT) ? RAILS_ROOT : File.join(File.dirname(__FILE__))
end
  
  