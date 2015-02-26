module Jetpants
  class Host
    
    ##### NEW METHODS ##########################################################
    
    # Converts tcpdump output into slowlog format using pt-query-digest. Requires that
    # pt-query-digest is installed and in root's path. Returns the full path to the
    # slowlog. Does not delete or remove the tcpdump output file.
    #
    # This is in Host instead of DB because it may be preferable to run this on
    # the host running Jetpants, as opposed to the DB where the dumpfile came from,
    # because pt-query-digest may be taxing to run on the server.
    def dumpfile_to_slowlog(tcpdump_output_file_path, delete_tcpdumpfile=true)
      pt_query_digest_version = `pt-query-digest --version`.to_s.split(' ').last.chomp rescue '0.0.0'
      raise "pt-query-digest executable is not available on the host" unless $?.exitstatus == 1

      slowlog_file_path = tcpdump_output_file_path.sub('.dumpfile', '') + '.slowlog'
      if pt_query_digest_version.to_f >= 2.2
        ssh_cmd "pt-query-digest #{tcpdump_output_file_path} --type tcpdump --no-report --output slowlog >#{slowlog_file_path}"
      else
        ssh_cmd "pt-query-digest #{tcpdump_output_file_path} --type tcpdump --no-report --print >#{slowlog_file_path}"
      end
      ssh_cmd "rm #{tcpdump_output_file_path}" if delete_tcpdumpfile
      slowlog_file_path = filter_slowlog(slowlog_file_path)
      slowlog_file_path
    end
   
    # Perform any slowlog filtering eg. removing SELECT UNIX_TIMESTAMP() like queries
    def filter_slowlog(slowlog_file_path)
      slowlog_file_path
    end
  end
end
