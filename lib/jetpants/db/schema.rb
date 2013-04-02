require 'table'

module Jetpants

  #--
  # Table probing methods
  #++

  class DB
    # Query the database for information about the table schema, including
    # primary key, create statement, and columns
    def detect_table_schema(table_name)
      table_sql = "SHOW CREATE TABLE `#{table_name}`"
      create_statement = query_return_first(table_sql).values.last
      pk_sql = "SHOW INDEX IN #{table_name} WHERE Key_name = 'PRIMARY'"
      pk_fields = query_return_array(pk_sql)
      pk_fields.sort_by!{|pk| pk[:Seq_in_index]}

      params = {
        'primary_key' => pk_fields.map{|pk| pk[:Column_name] },
        'create_table' => create_statement,
        'indexes' => connection.indexes(table_name),
        'pool' => pool,
        'columns' => connection.schema(table_name).map{|schema| schema[0]} 
      }

      Table.new(table_name, params)
    end

    # Deletages check for a table existing by name up to the pool
    def has_table?(table)
      pool.has_table? table
    end

    # List of tables (as defined by the pool)
    def tables
      pool(true).tables
    end
  end
end
