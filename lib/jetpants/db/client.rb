module Jetpants
  
  #--
  # Connection and query methods ###############################################
  #++
  
  class DB
    # Runs the provided SQL statement as root, locally via an SSH command line, and
    # returns the response as a single string.
    # Available options:
    # :terminator:: how to terminate the query, such as '\G' or ';'. (default: '\G')
    # :parse:: parse a single-row, vertical-format result (:terminator must be '\G') and return it as a hash
    # :schema:: name of schema to use, or true to use this DB's default. This may have implications when used with filtered replication! (default: nil, meaning no schema)
    # :attempts:: by default, queries will be attempted up to 3 times. set this to 0 or false for non-idempotent queries.
    def mysql_root_cmd(cmd, options={})
      terminator = options[:terminator] || '\G'
      attempts = (options[:attempts].nil? ? 3 : (options[:attempts].to_i || 1))
      schema = (options[:schema] == true ? app_schema : options[:schema])
      failures = 0
      
      begin
        raise "MySQL is not running" unless running?
        supply_root_pw = (Jetpants.mysql_root_password ? "-p#{Jetpants.mysql_root_password}" : '')
        supply_port = (@port == 3306 ? '' : "-h 127.0.0.1 -P #{@port}")
        real_cmd = %Q{mysql #{supply_root_pw} #{supply_port} -ss -e "#{cmd}#{terminator}" #{schema}}
        real_cmd.untaint
        result = ssh_cmd!(real_cmd)
        raise result if result && result.downcase.start_with?('error ')
        result = parse_vertical_result(result) if options[:parse] && terminator == '\G'
        return result
      rescue => ex
        failures += 1
        raise if failures >= attempts
        output "Root query \"#{cmd}\" failed: #{ex.message}, re-trying after delay"
        sleep 3 * failures
        retry
      end
    end
    
    # Returns a Sequel database object for use in sending queries to the DB remotely.
    # Initializes (or re-initializes) the connection pool upon first use or upon
    # requesting a different user or schema. Note that we only maintain one connection
    # pool per DB.
    # Valid options include :user, :pass, :schema, :max_conns, :after_connect or omit
    # these to use defaults.
    def connect(options={})
      if @options.include? '--skip-networking'
        output 'Skipping connection attempt because server started with --skip-networking'
        return nil
      end
      
      options[:user]    ||= app_credentials[:user]
      options[:schema]  ||= app_schema
      
      return @db if @db && @user == options[:user] && @schema == options[:schema]
      
      disconnect if @db
      
      @db = Sequel.connect(
        :adapter          =>  'mysql2',
        :host             =>  @ip,
        :port             =>  @port,
        :user             =>  options[:user],
        :password         =>  options[:pass] || app_credentials[:pass],
        :database         =>  options[:schema],
        :max_connections  =>  options[:max_conns] || Jetpants.max_concurrency,
        :after_connect    =>  options[:after_connect] )
      @user = options[:user]
      @schema = options[:schema]
      @db.convert_tinyint_to_bool = false
      @db
    end
    
    # Closes the database connection(s) in the connection pool.
    def disconnect
      if @db
        @db.disconnect rescue nil
        @db = nil
      end
      @user = nil
      @schema = nil
    end
    
    # Disconnects and reconnects to the database.
    def reconnect(options={})
      disconnect # force disconnection even if we're not changing user or schema
      connect(options)
    end
    
    # Returns a Sequel database object representing the current connection. If no
    # current connection, this will automatically connect with default options.
    def connection
      @db || connect
    end
    alias mysql connection
    
    # Returns a hash containing :user and :pass indicating how the application connects to
    # this database instance.  By default this just delegates to Jetpants.application_credentials,
    # which obtains credentials from the Jetpants config file. Plugins may override this
    # to use different credentials for particular hosts or in certain situations.
    def app_credentials
      Jetpants.app_credentials
    end
    
    # Returns the schema name ("database name" in MySQL parlance) to use for connections.
    # Defaults to just calling Jetpants.mysql_schema, but plugins may override.
    def app_schema
      Jetpants.mysql_schema
    end
    
    # Execute a write (INSERT, UPDATE, DELETE, REPLACE, etc) query.
    # If the query is an INSERT, returns the last insert ID (if an auto_increment
    # column is involved).  Otherwise returns the number of affected rows.
    def query(sql, *binds)
      ds = connection.fetch(sql, *binds)
      connection.execute_dui(ds.update_sql) {|c| return c.last_id > 0 ? c.last_id : c.affected_rows}
    end
    
    # Execute a read (SELECT) query. Returns an array of hashes.
    def query_return_array(sql, *binds)
      connection.fetch(sql, *binds).all
    end
    
    # Execute a read (SELECT) query. Returns a hash of the first row only.
    def query_return_first(sql, *binds)
      connection.fetch(sql, *binds).first
    end
    
    # Execute a read (SELECT) query. Returns the value of the first column of the first row only.
    def query_return_first_value(sql, *binds)
      connection.fetch(sql, *binds).single_value
    end
    
    # Parses the result of a MySQL query run with a \G terminator. Useful when
    # interacting with MySQL via the command-line client (for secure access to
    # the root user) instead of via the MySQL protocol.
    def parse_vertical_result(text)
      results = {}
      return results unless text
      raise text.chomp if text =~ /^ERROR/
      lines = text.split("\n")
      lines.each do |line|
        col, val = line.split ':'
        next unless val
        results[col.strip.downcase.to_sym] = val.strip
      end
      results
    end
    
  end
end
