module Jetpants
  
  #--
  # User / Grant manipulation methods ##########################################
  #++
  
  class DB
    # Create a MySQL user. If you omit parameters, the defaults from Jetpants'
    # configuration will be used instead.  Does not automatically grant any
    # privileges; use DB#grant_privileges for that.
    def create_user(username=false, database=false, password=false)
      username ||= Jetpants.app_credentials[:user]
      database ||= Jetpants.mysql_schema
      password ||= Jetpants.app_credentials[:pass]
      commands = []
      Jetpants.mysql_grant_ips.each do |ip|
        commands << "CREATE USER '#{username}'@'#{ip}' IDENTIFIED BY '#{password}'"
      end
      commands << "FLUSH PRIVILEGES"
      mysql_root_cmd(commands.join '; ')
    end
    
    # Drops a user. Can optionally make this statement skip replication, if you
    # want to drop a user on master and not on its slaves.
    def drop_user(username=false, skip_binlog=false)
      username ||= Jetpants.app_credentials[:user]
      commands = []
      commands << 'SET sql_log_bin = 0' if skip_binlog
      Jetpants.mysql_grant_ips.each do |ip|
        commands << "DROP USER '#{username}'@'#{ip}'"
      end
      commands << "FLUSH PRIVILEGES"
      mysql_root_cmd(commands.join '; ')
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
      username ||= Jetpants.app_credentials[:user]
      database ||= Jetpants.mysql_schema
      privileges = Jetpants.mysql_grant_privs if privileges.empty?
      privileges = privileges.join(',')
      commands = []
      
      Jetpants.mysql_grant_ips.each do |ip|
        commands << "#{statement} #{privileges} ON #{database}.* #{preposition} '#{username}'@'#{ip}'"
      end
      commands << "FLUSH PRIVILEGES"
      mysql_root_cmd(commands.join '; ')
    end
    
    # Disables access to a DB by the application user, and sets the DB to 
    # read-only. Useful when decommissioning instances from a shard that's
    # been split.
    def revoke_all_access!
      user_name = Jetpants.app_credentials[:user]
      output("Revoking access for user #{user_name} and setting global read-only.")
      read_only!
      output(drop_user(user_name, true)) # drop the user without replicating the drop statement to slaves
    end
    
    # Enables global read-only mode on the database.
    def read_only!
      mysql_root_cmd 'SET GLOBAL read_only = 1' unless read_only?
      read_only?
    end
    
    # Disables global read-only mode on the database.
    def disable_read_only!
      mysql_root_cmd 'SET GLOBAL read_only = 0' if read_only?
      not read_only?
    end
    
  end
end