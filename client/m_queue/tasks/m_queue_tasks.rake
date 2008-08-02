namespace :queue do
  task :start => [:environment] do
    if qn = ENV['QUEUE_NAME']
      queue = qn.classify.constantize
      puts "Starting up #{queue.queue_name}"
      MQueue::Daemon.daemonize!(queue.queue_name) { queue.run }
    else
      MQueue::Queue.queues.each do |queue|
        puts "Starting up #{queue.queue_name}"
        MQueue::Daemon.daemonize!(queue.queue_name) { queue.run }
      end
    end
  end
  
  task :stop  => [:environment] do
    if qn = ENV['QUEUE_NAME']
      queue = qn.classify.constantize
      puts "Killing #{queue.queue_name}"
      MQueue::Daemon.kill!(queue.queue_name)
    else
      puts "Killing all queues"
      MQueue::Daemon.kill_all!
    end
  end
  
  task :restart => [:stop, :start] do
  end
end