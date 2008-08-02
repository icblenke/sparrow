require 'sqlite3'
module Sparrow
  module Queues
    class Sqlite
      include Sparrow::Miscel
    
      attr_accessor :queue_name
      attr_accessor :db
      attr_accessor :count_pop
      attr_accessor :count_push
    
      def initialize(queue_name)
        self.queue_name = queue_name
        self.count_pop = 0
        self.count_push = 0
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
        db.execute("INSERT INTO queues (data) VALUES (?);", value)
        self.count_push += 1
        value
      end
    
      def pop
        id, value = db.get_first_row("SELECT * FROM queues LIMIT 1;")
        return unless id and value
        db.execute("DELETE FROM queues WHERE id = ?", id)
        self.count_pop += 1
        value
      end
    
      def count
        db.get_first_value("SELECT count(*) FROM queues").to_i
      end
      
      def clear!
        db.execute("DELETE FROM queues")
      end
      
      def all
        db.execute("SELECT * FROM queues").collect {|v| v[1] }
      end
      
      def insert(values)
        db.transaction do |d|
          values.each do |v|
            db.execute("INSERT INTO queues (data) VALUES (?)", v)
          end
        end
      end
      
      private 
      
      def db_path
        File.join(base_dir, queue_name)
      end
    
    end
  end
end