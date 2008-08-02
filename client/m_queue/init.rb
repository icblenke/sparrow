# Include hook code here
require 'yaml'
require File.join(File.dirname(__FILE__), 'lib', 'm_queue')

require File.join(File.dirname(__FILE__), 'lib', 'protocols')
require File.join(File.dirname(__FILE__), 'lib', 'queue')

Dir.glob(File.join(File.dirname(__FILE__), 'lib', 'protocols', '*.rb')) {|i| 
  begin
    require i
  rescue LoadError
   # Might not have sqs/beanstalk-client
  end
}