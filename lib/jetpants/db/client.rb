module Jetpants
  
  #--
  # Connection and query methods ###############################################
  #++
  
  class DB
    # Runs the provided SQL statement as root, and returns the response as a single string.
    # Available options:
    # :terminator:: how to terminate the query, such as '\G' or ';'. (default: '\G')
    # :parse:: parse a single-row, vertical-format result (:terminator must be '\G') and return it as a hash
    # :attempts:: by default, queries will be attempted up to 3 times. set this to 0 or false for non-idempotent queries.
    def mysql_root_cmd(cmd, options={})
      terminator = options[:terminator] || '\G'
      attempts = (options[:attempts].nil? ? 3 : (options[:attempts].to_i || 1))
      failures = 0
      
      begin
        raise "MySQL is not running" unless running?
        supply_root_pw = (Jetpants.mysql_root_password ? "-p#{Jetpants.mysql_root_password}" : '')
        supply_port = (@port == 3306 ? '' : "-h 127.0.0.1 -P #{@port}")
        real_cmd = %Q{mysql #{supply_root_pw} #{supply_port} -ss -e "#{cmd}#{terminator}" #{Jetpants.mysql_schema}}
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
    
    # Returns a Sequel database object
    def mysql
      return @db if @db
      @db = Sequel.connect(
        :adapter          =>  'mysql2',
        :host             =>  @ip,
        :port             =>  @port,
        :user             =>  @user || Jetpants.app_credentials[:user],
        :password         =>  Jetpants.app_credentials[:pass],
        :database         =>  Jetpants.mysql_schema,
        :max_connections  =>  Jetpants.max_concurrency)
    end
    alias init_db_connection_pool mysql
    
    # Closes existing mysql connection pool and opens a new one. Useful when changing users.
    # Supply a new user name as the param, or nothing/false to keep old user name, or
    # a literal true value to switch to the default app user in Jetpants configuration
    def reconnect(new_user=false)
      @user = (new_user == true ? Jetpants.app_credentials[:user] : new_user) if new_user
      if @db
        @db.disconnect rescue nil
        @db = nil
      end
      init_db_connection_pool
    end
    
    # Execute a write (INSERT, UPDATE, DELETE, REPLACE, etc) query.
    # If the query is an INSERT, returns the last insert ID (if an auto_increment
    # column is involved).  Otherwise returns the number of affected rows.
    def query(sql, *binds)
      ds = mysql.fetch(sql, *binds)
      mysql.execute_dui(ds.update_sql) {|c| return c.last_id > 0 ? c.last_id : c.affected_rows}
    end
    
    # Execute a read (SELECT) query. Returns an array of hashes.
    def query_return_array(sql, *binds)
      mysql.fetch(sql, *binds).all
    end
    
    # Execute a read (SELECT) query. Returns a hash of the first row only.
    def query_return_first(sql, *binds)
      mysql.fetch(sql, *binds).first
    end
    
    # Execute a read (SELECT) query. Returns the value of the first column of the first row only.
    def query_return_first_value(sql, *binds)
      mysql.fetch(sql, *binds).single_value
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