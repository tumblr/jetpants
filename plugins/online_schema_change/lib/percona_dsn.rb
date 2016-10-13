module Jetpants
  module Percona
    class DSNTable
      SCHEMA_NAME = 'percona_schema'

      # If we change this from a constant to something dynamic, we should consider using
      # @server.escape or some form of validation where it is used in queries.
      TABLE_NAME = 'dsns';

      def initialize server
        @server = server
        @table_name = 'dsns'
      end

      def create_with_nodes nodes
        create_table
        load_nodes nodes
      end

      def dsn
        "h=#{@server.ip},D=#{SCHEMA_NAME},t=#{TABLE_NAME}"
      end

      def destroy!
        @server.mysql_root_cmd "SET SESSION sql_log_bin = 0; DROP DATABASE IF EXISTS #{SCHEMA_NAME}"
      end

      def create_table
        puts "pre"
        @server.mysql_root_cmd "SET SESSION sql_log_bin = 0; CREATE DATABASE IF NOT EXISTS #{SCHEMA_NAME}"
        puts "mid"
        puts schema
        @server.mysql_root_cmd "SET SESSION sql_log_bin = 0; #{schema}"
        puts "post"
      end

      def load_nodes nodes
        if nodes.length > 0
          values = nodes.map { |node| "('h=#{node.ip}')" }.join(', ')
          myquery = "INSERT INTO #{SCHEMA_NAME}.#{TABLE_NAME} (dsn) VALUES #{values}"
          puts "QUERY TO RUN: #{myquery}"
          @server.mysql_root_cmd("SET SESSION sql_log_bin = 0; #{myquery}")
        end
      end

      def schema
        dsn = <<-DSNS
          CREATE TABLE #{SCHEMA_NAME}.#{TABLE_NAME} (
            id int(11) NOT NULL AUTO_INCREMENT,
            parent_id int(11) DEFAULT NULL,
            dsn varchar(255) NOT NULL,
            PRIMARY KEY (id)
          )
        DSNS
        dsn.gsub("\n", " ")
      end
    end
  end
end
