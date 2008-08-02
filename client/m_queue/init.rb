# Include hook code here
require 'yaml'

$:.unshift(File.dirname(__FILE__))

require 'lib/m_queue'
require 'lib/daemon'
require 'lib/protocols'
require 'lib/queue'

Dir.glob(File.join(File.dirname(__FILE__), 'lib', 'protocols', '*.rb')) {|i| 
  begin
    require i
  rescue LoadError
   # Might not have sqs/beanstalk-client
  end
}