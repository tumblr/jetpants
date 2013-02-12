module Jetpants
  
  #--
  # User / Grant manipulation methods ##########################################
  #++
  
  class DB
    # Create a MySQL user. If you omit parameters, the defaults from Jetpants'
    # configuration will be used instead.  Does not automatically grant any
    # privileges; use DB#grant_privileges for that.  Intentionally cannot
    # create a passwordless user. 
    def create_user(username=false, password=false, skip_binlog=false)
      username ||= app_credentials[:user]
      password ||= app_credentials[:pass]
      commands = []
      commands << 'SET sql_log_bin = 0' if skip_binlog
      Jetpants.mysql_grant_ips.each do |ip|
        commands << "CREATE USER '#{username}'@'#{ip}' IDENTIFIED BY '#{password}'"
      end
      commands << "FLUSH PRIVILEGES"
      commands = commands.join '; '
      mysql_root_cmd commands, schema: true
      Jetpants.mysql_grant_ips.each do |ip|
        message = "Created user '#{username}'@'#{ip}'"
        message += ' (only on this node - skipping binlog!)' if skip_binlog
        output message
      end
    end
    
    # Drops a user. Can optionally make this statement skip replication, if you
    # want to drop a user on master and not on its slaves.
    def drop_user(username=false, skip_binlog=false)
      username ||= app_credentials[:user]
      commands = []
      commands << 'SET sql_log_bin = 0' if skip_binlog
      Jetpants.mysql_grant_ips.each do |ip|
        commands << "DROP USER '#{username}'@'#{ip}'"
      end
      commands << "FLUSH PRIVILEGES"
      commands = commands.join '; '
      mysql_root_cmd commands, schema: true
      Jetpants.mysql_grant_ips.each do |ip|
        message = "Dropped user '#{username}'@'#{ip}'"
        message += ' (only on this node - skipping binlog!)' if skip_binlog
        output message
      end
    end
    
    # Grants privileges to the given username for the specified database.
    # Pass in privileges as additional params, each as strings.
    # You may omit parameters to use the defaults in the Jetpants config file.
    def grant_privileges(username=false, database=false, *privileges)
      grant_or_revoke_privileges('GRANT', username, database, privileges)
    end
    
    # Revokes privileges from the given username for the specified database.
    # Pass in privileges as additional params, each as strings.
    # You may omit parameters to use the defaults in the Jetpants config file.
    def revoke_privileges(username=false, database=false, *privileges)
      grant_or_revoke_privileges('REVOKE', username, database, privileges)
    end
    
    # Helper method that can do grants or revokes.
    def grant_or_revoke_privileges(statement, username, database, privileges)
      preposition = (statement.downcase == 'revoke' ? 'FROM' : 'TO')
      username ||= app_credentials[:user]
      database ||= app_schema
      privileges = Jetpants.mysql_grant_privs if privileges.empty?
      privileges = privileges.join(',')
      commands = []
      
      Jetpants.mysql_grant_ips.each do |ip|
        commands << "#{statement} #{privileges} ON #{database}.* #{preposition} '#{username}'@'#{ip}'"
      end
      commands << "FLUSH PRIVILEGES"
      commands = commands.join '; '
      mysql_root_cmd commands, schema: true
      Jetpants.mysql_grant_ips.each do |ip|
        verb = (statement.downcase == 'revoke' ? 'Revoking' : 'Granting')
        target_db = (database == '*' ? 'globally' : "on #{database}.*")
        output "#{verb} privileges #{preposition.downcase} '#{username}'@'#{ip}' #{target_db}: #{privileges.downcase}"
      end
    end
    
    # Disables access to a DB by the application user, and sets the DB to 
    # read-only. Useful when decommissioning instances from a shard that's
    # been split, or a former slave that's been permanently removed from the pool
    def revoke_all_access!
      user_name = app_credentials[:user]
      enable_read_only!
      drop_user(user_name, true) # drop the user without replicating the drop statement to slaves
    end
    
    # Enables global read-only mode on the database.
    def enable_read_only!
      if read_only?
        output "Node already has read_only mode enabled"
        true
      else
        output "Enabling read_only mode"
        mysql_root_cmd 'SET GLOBAL read_only = 1'
        read_only?
      end
    end
    
    # Disables global read-only mode on the database.
    def disable_read_only!
      if read_only?
        output "Disabling read_only mode"
        mysql_root_cmd 'SET GLOBAL read_only = 0'
        not read_only?
      else
        output "Confirmed that read_only mode is already disabled"
        true
      end
    end
    
    # Generate and return a random string consisting of uppercase
    # letters, lowercase letters, and digits.
    def self.random_password(length=50)
      chars = [('a'..'z'), ('A'..'Z'), (0..9)].map(&:to_a).flatten
      (1..length).map{ chars[rand(chars.length)] }.join
    end

    # override Jetpants.mysql_grant_ips temporarily before executing a block
    # then set Jetpants.mysql_grant_ips back to the original values
    #   eg. master.override_mysql_grant_ips(['10.10.10.10']) do
    #         #something
    #       end
    def override_mysql_grant_ips(ips)
      ip_holder = Jetpants.mysql_grant_ips
      Jetpants.mysql_grant_ips = ips
      begin
        yield
      rescue StandardError, Interrupt, IOError
        Jetpants.mysql_grant_ips = ip_holder
        raise
      end
      Jetpants.mysql_grant_ips = ip_holder
    end
    
  end
end