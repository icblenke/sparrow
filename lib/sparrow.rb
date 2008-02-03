require 'logger'

$:.unshift(File.dirname(__FILE__))
require 'sparrow/utils'

module Sparrow
  
  class SparrowError < StandardError #:nodoc:
  end
  
  VERSION   = '0.3.1'
  
  @@options = {}
  
  class << self
    def options
      @@options
    end
  
    def options=(val)
      @@options = val
    end
  
    def logger
      return @@logger if defined?(@@loggger)
      FileUtils.mkdir_p(File.dirname(log_path))
      @@logger = Logger.new(log_path)
      @@logger.level = Logger::INFO if options[:debug] == false
      @@logger
    rescue
      @@logger = Logger.new(STDOUT)
    end
  
    def base_dir
      options[:base_dir] || File.join(%w( / var spool sparrow base ))
    end
  
    def log_path
      options[:log_path] || File.join(%w( / var run sparrow.log ))
    end
  
    def pid_dir
      options[:pid_dir] || File.join(%w( / var run sparrow pids ))
    end
  end
end

require 'sparrow/server'
require 'sparrow/queues/sqlite' rescue LoadError nil
require 'sparrow/queues/memory'
require 'sparrow/queues/disk'
require 'sparrow/queue'
require 'sparrow/runner'