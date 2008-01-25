require 'sqlite3'
module Sparrow
  module Queues
    class Sqlite
      include Sparrow::Miscel
    
      attr_accessor :queue_name
      attr_accessor :db
      attr_accessor :count_all
    
      def initialize(queue_name)
        self.queue_name = queue_name
        self.count_all = 0
        db_exists = File.exists?(db_path)
        self.db = SQLite3::Database.new( db_path )
        if !db_exists
          self.db.execute_batch <<-SQL
           CREATE TABLE queues (
            id INTEGER PRIMARY KEY,
            data VARCHAR(255)
           );
           PRAGMA default_synchronous=OFF;
           PRAGMA count_changes=OFF;
          SQL
        end
      end
        
      def push(value)
        self.count_all += 1
        db.execute("INSERT INTO queues (data) VALUES (?);", value)
        value
      end
    
      def pop
        id, value = db.get_first_row("SELECT * FROM queues LIMIT 1;")
        db.execute("DELETE FROM queues WHERE id = ?", id)
        value
      end
    
      def count
        db.get_first_value("SELECT COUNT FROM queues")
      end
      
      def clear
        db.execute("DELETE FROM queues")
      end
      
      private 
      
      def db_path
        File.join(base_dir, queue_name)
      end
    
    end
  end
end