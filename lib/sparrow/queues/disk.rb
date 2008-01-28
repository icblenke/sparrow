require 'fileutils'
module Sparrow
  module Queues
    class Disk
      include Sparrow::Miscel
      
      TRX_CMD_PUSH = "\000".freeze
      TRX_CMD_POP = "\001".freeze

      TRX_PUSH = "\000%s%s".freeze
      TRX_POP = "\001".freeze
    
      attr_accessor :queue_name
      attr_accessor :trxr
      attr_accessor :trxw
      attr_accessor :count_all
    
      def initialize(queue_name)
        self.queue_name = queue_name
        self.count_all = 0
        open_queue
      end
    
      def push(value)
        value = value.to_s
        size = [value.size].pack("I")
        data = sprintf(TRX_PUSH, size, value)
        trxw.seek(0, IO::SEEK_END)
        trxw.write data
        # trxw.fsync
        rotate_queue if trxw.pos > max_log_size
        self.count_all += 1
        value
      end
    
      def pop        
        while !trxr.eof?
          s_pos = trxr.pos
          cmd = trxr.read(1)
          if cmd != TRX_CMD_POP and cmd != TRX_CMD_PUSH
            logger.fatal 'Corrupt queue'
            return
          end
          raw_size = trxr.read(4)
          size = raw_size.unpack("I").first
          value = trxr.read(size)
          next if cmd == TRX_CMD_POP
          e_pos = trxr.pos
          trxr.seek(s_pos, IO::SEEK_SET)
          trxr.write(TRX_POP)
          # trxr.fsync
          trxr.pos = e_pos
          next unless value
          return value
        end
        
        if trxr.path == queue_path
          File.truncate(trxr.path, 0)
        else
          FileUtils.rm_rf trxr.path
        end
        open_reader
        nil
      end
      
      def clear
        dirs = Dir.glob(queue_path) | Dir.glob(queue_path + '.*')
        FileUtils.rm_rf(dirs) unless dirs.empty?
      end
      
      def count
      end
      
      private
      
      def queue_path
        File.join(base_dir, queue_name)
      end
      
      def rotate_queue
        File.rename(queue_path, File.join(base_dir, "#{queue_name}.#{Time.now.to_i}"))
        open_writer
      end
      
      def open_writer
        self.trxw = File.open(queue_path, 'a+')
      end
      
      def open_reader    
        old_queue = Dir.glob(queue_path + '.*').first
        self.trxr = File.open(old_queue||queue_path, 'r+')
      end
    
      def open_queue
        open_writer
        open_reader
      end
      
      def max_log_size
        @max_log_size ||= (options[:log_size] || 16) * (1024**2) # 16mb
      end

    end
  end
end