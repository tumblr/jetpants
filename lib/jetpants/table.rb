module Jetpants
  
  # The Table class associates a table name with a column (or list of columns)
  # making up the table's sharding key or primary key. It is geared towards
  # generating SQL for importing/exporting a table, NOT for representing an
  # application data model.
  # 
  # None of these methods actually *execute* the SQL they generate, since the
  # Table class isn't tied to a specific DB. This allows us to represent the set
  # of all sharded tables with a single set of Table objects, without having to
  # duplicate those objects for every Shard or DB instance. If you want to run
  # the generated SQL on a database, use one of the DB#query* methods.
  class Table
    include CallbackHandler
    
    # Name of the table as it exists in your database.
    attr_reader :name
    
    # Your application's sharding_key is the column used to determine which rows
    # live on which shard. Generally this should be the same logical value for your
    # entire application (example: id column of the User table), although the column
    # name need not be identical across tables (one may call it 'user_id', another
    # could call it 'target_user_id' or 'from_user_id'.) The sharding_keys attribute
    # stores the name of that column for this particular table.
    # 
    # For a sharded table, sharding_keys should generally be a single column,
    # represented here as a single string.
    #
    # Jetpants supports mapping-tables with multiple sharding key columns (for
    # instance, if user_id is your app's sharding key, a "following" table mapping
    # one user_id to another). However this makes exports and cleanup extremely
    # inefficient, so its use is not recommended.
    #
    # For a non-sharded table, simply set sharding_keys to the first column of
    # the table's primary key. This is sufficient to make chunked exports/imports
    # work properly.
    attr_reader :sharding_keys
    
    # Jetpants supports doing import and export operations in parallel "chunks" of
    # the data set. For tables with few rows, this is irrelevant and can be left at
    # the default of 1 (meaning no chunking). For tables with hundreds of millions
    # of rows, you may want to do exports/imports in a few hundred chunks to speed
    # things up and keep the transactions smaller.
    attr_accessor :chunks
    
    # The SQL statement read from the DB via SHOW CREATE TABLE
    attr_reader :create_table_sql

    # The primary key of the table, returns an array on a multi-
    # column PK
    attr_reader :primary_key

    # A list of indexes mapped to the columns in them
    attr_reader :indexes

    # A list of the table column names
    attr_reader :columns 

    # Pool object this Table is related to
    attr_reader :pool

    # Create a Table. Possible keys include 'sharding_key', 'chunks', 'order_by',
    # 'create_table', 'pool', 'indexes', and anything else handled by plugins
    def initialize(name, params={})
      @name = name
      parse_params(params)
    end

    def parse_params(params = {})
      # Convert symbols to strings
      params.keys.select {|k| k.is_a? Symbol}.each do |symbol_key|
        params[symbol_key.to_s] = params[symbol_key]
        params.delete symbol_key
      end
      
      # accept singular or plural for some params
      params['sharding_key'] ||= params['sharding_keys']
      params['primary_key']  ||= params['primary_keys']
      
      @sharding_keys = (params['sharding_key'].is_a?(Array) ? params['sharding_key'] : [params['sharding_key']]) if params['sharding_key']
      @sharding_keys ||= []
      
      @primary_key = params['primary_key']
      @chunks = params['chunks'] || 1
      @order_by = params['order_by']
      @create_table_sql = params['create_table'] || params['create_table_sql']
      @pool = params['pool']
      @indexes = params['indexes']
      @columns = params['columns'] 
    end
    
    # Returns the current maximum primary key value, returns
    # the values of the record when ordered by the key fields
    # in order, descending on a multi-value PK
    def max_pk_val_query
      if @primary_key.is_a?(Array)
        pk_str = @primary_key.join(",")
        pk_ordering = @primary_key.map{|key| "#{key} DESC"}.join(',')
        sql = "SELECT #{pk_str} FROM #{@name} ORDER BY #{pk_ordering} LIMIT 1"
      else
        sql = "SELECT MAX(#{@primary_key}) FROM #{@name}"
      end
      return sql
    end
    
    # Returns the first column of the primary key, or nil if there isn't one
    def first_pk_col
      if @primary_key.is_a? Array
        @primary_key.first
      else
        @primary_key
      end
    end
    
    # Returns true if the table is associated with the supplied pool
    def belongs_to?(pool)
      return @pool == pool
    end

    # Return an array of Table objects based on the contents of Jetpants' config file entry
    # of the given label.
    # TODO: integrate better with table schema detection code. Consider auto-detecting chunk
    # count based on file size and row count estimate.
    def Table.from_config(label)
      result = []
      Jetpants.send(label).map {|name, attributes| Table.new name, attributes}
    end
    
    def to_s
      return @name
    end
    
    # Returns the SQL for performing a data export of a given ID range
    def sql_export_range(min_id=false, max_id=false)
      outfile = export_file_path min_id, max_id
      sql = "SELECT * FROM #{@name} "
      
      if min_id || max_id
        clauses = case
                  when min_id && max_id then @sharding_keys.collect {|col| "(#{col} >= #{min_id} AND #{col} <= #{max_id}) "}
                  when min_id           then @sharding_keys.collect {|col| "#{col} >= #{min_id} "}
                  when max_id           then @sharding_keys.collect {|col| "#{col} <= #{max_id} "}
                  end
        sql << "WHERE " + clauses.join('OR ')
      end
      
      sql << "ORDER BY #{@order_by} " if @order_by
      sql << "INTO OUTFILE '#{outfile}'"
    end
    alias sql_export_all sql_export_range
    
    # Returns the SQL necessary to load the table's data.
    # Note that we use an IGNORE on multi-sharding-key tables. This is because
    # we get duplicate rows between export chunk files in this case.
    def sql_import_range(min_id=false, max_id=false)
      outfile = export_file_path min_id, max_id
      ignore = (@sharding_keys.count > 1 && (min_id || max_id) ? ' IGNORE' : '')
      sql = "LOAD DATA INFILE '#{outfile}'#{ignore} INTO TABLE #{@name} CHARACTER SET binary"
    end
    alias sql_import_all sql_import_range
    
    # Returns the SQL necessary to iterate over a given sharding key by ID -- returns
    # the next ID desired.  Useful when performing a cleanup operation over a sparse
    # ID range.
    def sql_cleanup_next_id(sharding_key, id, direction)
      if direction == :asc
        "SELECT MIN(#{sharding_key}) FROM #{@name} WHERE #{sharding_key} > #{id}"
      elsif direction == :desc
        "SELECT MAX(#{sharding_key}) FROM #{@name} WHERE #{sharding_key} < #{id}"
      else
        raise "Unknown direction parameter #{direction}"
      end
    end
    
    # Returns the SQL necessary to clean rows that shouldn't be on this shard.
    # Pass in a sharding key and the min/max allowed ID on the shard, and get back
    # a SQL DELETE statement.  When running that statement, pass in an ID (obtained
    # from sql_cleanup_next_id) as a bind variable.
    def sql_cleanup_delete(sharding_key, min_keep_id, max_keep_id)
      sql = "DELETE FROM #{@name} WHERE #{sharding_key} = ?"
      
      # if there are multiple sharding cols, we need to be more careful to keep rows
      # where the OTHER sharding col(s) do fall within the shard's range
      @sharding_keys.each do |other_col|  
        next if other_col == sharding_key
        sql << " AND NOT (#{other_col} >= #{min_keep_id} AND #{other_col} <= #{max_keep_id})"
      end
      
      return sql
    end
    
    # Returns SQL to counts number of rows between the given ID ranges.
    # Warning: will give potentially misleading counts on multi-sharding-key tables.
    def sql_count_rows(min_id, max_id)
      sql = "SELECT COUNT(*) FROM #{@name}"
      return sql unless min_id && max_id
      
      wheres = []
      
      if @sharding_keys.size > 0
        @sharding_keys.each {|col| wheres << "(#{col} >= #{min_id} AND #{col} <= #{max_id})"}
        sql << ' WHERE ' + wheres.join(" OR ")
      elsif first_pk_col
        sql << " WHERE #{first_pk_col} >= #{min_id} AND #{first_pk_col} <= #{max_id}"
      end
      sql
    end
    
    # Returns a file path (as a String) for the export dumpfile of the given ID range.
    def export_file_path(min_id=false, max_id=false)
      case
      when min_id && max_id then  "#{Jetpants.export_location}/#{@name}#{min_id}-#{max_id}.out"
      when min_id           then  "#{Jetpants.export_location}/#{@name}#{min_id}-and-up.out"
      when max_id           then  "#{Jetpants.export_location}/#{@name}start-#{max_id}.out"
      else                        "#{Jetpants.export_location}/#{@name}-full.out"
      end
    end
    
  end
end
