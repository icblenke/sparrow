# Include hook code here
require File.join(File.dirname(__FILE__), 'lib', 'm_queue')

require File.join(File.dirname(__FILE__), 'lib', 'protocols')
require File.join(File.dirname(__FILE__), 'lib', 'queue')

Dir.glob(File.join(File.dirname(__FILE__), 'lib', 'protocols', '*.rb')) {|i| require i rescue nil }