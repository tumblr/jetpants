require 'net/ssh'
require 'socket'

module Jetpants
  
  # Encapsulates a UNIX server that we can SSH to as root. Maintains a pool of SSH
  # connections to the host as needed.
  class Host
    include CallbackHandler
    
    # IP address of the Host, as a string.
    attr_reader :ip

    @@all_hosts = {}
    @@all_hosts_mutex = Mutex.new
    
    def self.clear
      @@all_hosts_mutex.synchronize {@@all_hosts = {}}
    end
    
    # We override Host.new so that attempting to create a duplicate Host object
    # (that is, one with the same IP as an existing Host object) returns the
    # original object.
    def self.new(ip)
      @@all_hosts_mutex.synchronize do
        @@all_hosts[ip] = nil unless @@all_hosts[ip].is_a? self
        @@all_hosts[ip] ||= super
      end
    end
    
    def initialize(ip)
      # Only supporting ipv4 for now
      raise "Invalid IP address: #{ip}" unless ip =~ /\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}/
      @ip = ip
      @connection_pool = [] # array of idle Net::SSH::Connection::Session objects
      @lock = Mutex.new
      @available = nil
    end
    
    # Returns a Host object for the machine Jetpants is running on.
    def self.local(interface=false)
      interface ||= Jetpants.private_interface
      # This technique is adapted from Sergio Rubio Gracia's, described at
      # http://blog.frameos.org/2006/12/09/getting-network-interface-addresses-using-ioctl-pure-ruby-2/
      sock = Socket.new(Socket::AF_INET, Socket::SOCK_DGRAM,0)
      buf = [interface, ""].pack('a16h16')
      sock.ioctl(0x8915, buf) # SIOCGIFADDR
      sock.close
      ip_string = buf[20...24].unpack('C*').join '.'
      self.new(ip_string)
    end
    
    # Returns a Net::SSH::Connection::Session for the host. Verifies that the
    # connection is working before returning it.
    def get_ssh_connection
      conn = nil
      attempts = 0
      5.times do |attempt|
        @lock.synchronize do
          if @connection_pool.count > 0
            conn = @connection_pool.shift
          end
        end
        unless conn
          params = {
            :paranoid => false,
            :user_known_hosts_file => '/dev/null',
            :timeout => 5,
          }
          params[:keys] = Jetpants.ssh_keys if Jetpants.ssh_keys
          begin
            @lock.synchronize do 
              conn = Net::SSH.start(@ip, 'root', params)
            end
          rescue => ex
            output "Unable to SSH on attempt #{attempt + 1}: #{ex.to_s}"
            conn = nil
            next
          end
        end
        
        # Confirm that the connection works
        if conn
          begin
            result = conn.exec!('echo ping').strip
            raise "Unexpected result" unless result == 'ping'
            @available = true
            return conn
          rescue
            output "Discarding nonfunctional SSH connection"
            conn = nil
          end
        end
      end
      @available = false
      raise "Unable to obtain working SSH connection to #{self} after 5 attempts"
    end
    
    # Adds a Net::SSH::Connection::Session to a pool of idle persistent connections.
    def save_ssh_connection(conn)
      conn.exec! 'cd ~'
      @lock.synchronize do
        @connection_pool << conn
      end
    rescue
      output "Discarding nonfunctional SSH connection"
    end
    
    # Execute the given UNIX command string (or array of strings) as root via SSH.
    # By default, if something is wrong with the SSH connection, the command 
    # will be attempted up to 3 times before an exception is thrown. Be sure
    # to set this to 1 or false for commands that are not idempotent.
    # Returns the result of the command executed. If cmd was an array of strings,
    # returns the result of the LAST command executed.
    def ssh_cmd(cmd, attempts=3)
      attempts ||= 1
      conn = get_ssh_connection
      cmd = [cmd] unless cmd.is_a? Array
      result = nil
      cmd.each do |c|
        failures = 0
        begin
          result = conn.exec! c
        rescue
          failures += 1
          raise if failures >= attempts
          output "Command \"#{c}\" failed, re-trying after delay"
          sleep(failures)
          retry
        end
      end
      save_ssh_connection conn
      return result
    end
    
    # Shortcut for use when a command is not idempotent and therefore
    # isn't safe to retry if something goes wonky with the SSH connection.
    def ssh_cmd!(cmd)
      ssh_cmd cmd, false
    end
    
    # Confirm that something is listening on the given port. The timeout param
    # indicates how long to wait (in seconds) for a process to be listening.
    def confirm_listening_on_port(port, timeout=10)
      checker_th = Thread.new { ssh_cmd "while [[ `netstat -ln | grep :#{port} | wc -l` -lt 1 ]] ; do sleep 1; done" }
      raise "Nothing is listening on #{@ip}:#{port} after #{timeout} seconds" unless checker_th.join(timeout)
      true
    end
    
    # Returns true if the host is accessible via SSH, false otherwise
    def available?
      # If we haven't tried an ssh command yet, @available will be nil. Running
      # a first no-op command will populate it to true or false.
      if @available.nil?
        ssh_cmd 'echo ping' rescue nil
      end
      @available
    end
    
    
    ###### Directory Copying / Listing / Comparison methods ####################
    
    # Quickly and efficiently recursively copies a directory to one or more target hosts.
    # base_dir::  is base directory to copy from the source (self). Also the default destination base
    #             directory on the targets, if not supplied via next param.
    # targets::   is one of the following:
    #             * Host object, or any object that delegates method_missing to a Host (such as DB)
    #             * array of Host objects (or delegates)
    #             * hash mapping Host objects (or delegates) to destination base directory overrides (as string)
    # options::   is a hash that can contain --
    #             * :files            =>  only copy these filenames instead of entire base_dir. String, or Array of Strings.
    #             * :port             =>  port number to use for netcat. defaults to 7000 if omitted.
    #             * :overwrite        =>  if true, don't raise an exception if the base_dir is non-empty or :files exist. default false.
    def fast_copy_chain(base_dir, targets, options={})
      # Normalize the filesnames param so it is an array
      filenames = options[:files] || ['.']
      filenames = [filenames] unless filenames.respond_to?(:each)
      
      # Normalize the targets param, so that targets is an array of Hosts and
      # destinations is a hash of hosts => dirs
      destinations = {}
      targets = [targets] unless targets.respond_to?(:each)
      base_dir += '/' unless base_dir[-1] == '/'
      if targets.is_a? Hash
        destinations = targets
        destinations.each {|t, d| destinations[t] += '/' unless d[-1] == '/'}
        targets = targets.keys
      else
        destinations = targets.inject({}) {|memo, target| memo[target] = base_dir; memo}
      end
      raise "No target hosts supplied" if targets.count < 1
      
      file_list = filenames.join ' '
      port = (options[:port] || 7000).to_i
      
      if Jetpants.compress_with || Jetpants.decompress_with
        comp_bin = Jetpants.compress_with.split(' ')[0]
        confirm_installed comp_bin
        output "Using #{comp_bin} for compression"
      else
        output "Compression disabled -- no compression method specified in Jetpants config file"
      end
      
      # On each destination host, do any initial setup (and optional validation/erasing),
      # and then listen for new files.  If there are multiple destination hosts, all of them
      # except the last will use tee to "chain" the copy along to the next machine.
      workers = []
      targets.reverse.each_with_index do |t, i|
        dir = destinations[t]
        raise "Directory #{t}:#{dir} looks suspicious" if dir.include?('..') || dir.include?('./') || dir == '/' || dir == ''
        
        if Jetpants.compress_with || Jetpants.decompress_with
          decomp_bin = Jetpants.decompress_with.split(' ')[0]
          t.confirm_installed decomp_bin
        end
        t.ssh_cmd "mkdir -p #{dir}"
        
        # Check if contents already exist / non-empty.
        # Note: doesn't do recursive scan of subdirectories
        unless options[:overwrite]
          all_paths = filenames.map {|f| dir + f}.join ' '
          dirlist = t.dir_list(all_paths)
          dirlist.each {|name, size| raise "File #{name} exists on destination and has nonzero size!" if size.to_i > 0}
        end
        
        decompression_pipe = Jetpants.decompress_with ? "| #{Jetpants.decompress_with}" : ''
        if i == 0
          workers << Thread.new { t.ssh_cmd "cd #{dir} && nc -l #{port} #{decompression_pipe} | tar xv" }
          t.confirm_listening_on_port port
          t.output "Listening with netcat."
        else
          tt = targets.reverse[i-1]
          fifo = "fifo#{port}"
          workers << Thread.new { t.ssh_cmd "cd #{dir} && mkfifo #{fifo} && nc #{tt.ip} #{port} <#{fifo} && rm #{fifo}" }
          checker_th = Thread.new { t.ssh_cmd "while [ ! -p #{dir}/#{fifo} ] ; do sleep 1; done" }
          raise "FIFO not found on #{t} after 10 tries" unless checker_th.join(10)
          workers << Thread.new { t.ssh_cmd "cd #{dir} && nc -l #{port} | tee #{fifo} #{decompression_pipe} | tar xv" }
          t.confirm_listening_on_port port
          t.output "Listening with netcat, and chaining to #{tt}."
        end
      end
      
      # Start the copy chain.
      output "Sending files over to #{targets[0]}: #{file_list}"
      compression_pipe = Jetpants.compress_with ? "| #{Jetpants.compress_with}" : ''
      ssh_cmd "cd #{base_dir} && tar vc #{file_list} #{compression_pipe} | nc #{targets[0].ip} #{port}"
      workers.each {|th| th.join}
      output "File copy complete."
      
      # Verify
      output "Verifying file sizes and types on all destinations."
      compare_dir base_dir, destinations, options
      output "Verification successful."
    end
    
    # Given the name of a directory or single file, returns a hash of filename => size of each file present.
    # Subdirectories will be returned with a size of '/', so you can process these differently as needed.
    # WARNING: This is brittle. It parses output of "ls". If anyone has a gem to do better remote file
    # management via ssh, then please by all means send us a pull request!
    def dir_list(dir)
      ls_out = ssh_cmd "ls --color=never -1AgGF #{dir}"  # disable color, 1 file per line, all but . and .., hide owner+group, include type suffix
      result = {}
      ls_out.split("\n").each do |line|
        next unless matches = line.match(/^[\w-]+\s+\d+\s+(?<size>\d+).*(?:\d\d:\d\d|\d{4})\s+(?<name>.*)$/)
        file_name = matches[:name]
        file_name = file_name[0...-1] if file_name =~ %r![*/=>@|]$!
        result[file_name.split('/')[-1]] = (matches[:name][-1] == '/' ? '/' : matches[:size].to_i)
      end
      result
    end
    
    # Compares file existence and size between hosts. Param format identical to
    # the first three params of Host#fast_copy_chain, except only supported option
    # is :files.
    # Raises an exception if the files don't exactly match, otherwise returns true.
    def compare_dir(base_dir, targets, options={})
      # Normalize the filesnames param so it is an array
      filenames = options[:files] || ['.']
      filenames = [filenames] unless filenames.respond_to?(:each)
      
      # Normalize the targets param, so that targets is an array of Hosts and
      # destinations is a hash of hosts => dirs
      destinations = {}
      targets = [targets] unless targets.respond_to?(:each)
      base_dir += '/' unless base_dir[-1] == '/'
      if targets.is_a? Hash
        destinations = targets
        destinations.each {|t, d| destinations[t] += '/' unless d[-1] == '/'}
        targets = targets.keys
      else
        destinations = targets.inject({}) {|memo, target| memo[target] = base_dir; memo}
      end
      raise "No target hosts supplied" if targets.count < 1
      
      queue = filenames.map {|f| ['', f]}  # array of [subdir, filename] pairs
      while (tuple = queue.shift)
        subdir, filename = tuple
        source_dirlist = dir_list(base_dir + subdir + filename)
        destinations.each do |target, path|
          target_dirlist = target.dir_list(path + subdir + filename)
          source_dirlist.each do |name, size|
            target_size = target_dirlist[name] || 'MISSING'
            raise "Directory listing mismatch when comparing #{self}:#{base_dir}#{subdir}#{filename}/#{name} to #{target}:#{path}#{subdir}#{filename}/#{name}  (size: #{size} vs #{target_size})" unless size == target_size
          end
        end
        queue.concat(source_dirlist.map {|name, size| size == '/' ? [subdir + '/' + name, '/'] : nil}.compact)
      end
    end
    
    # Recursively computes size of files in dir
    def dir_size(dir)
      total_size = 0
      dir_list(dir).each do |name, size|
        total_size += (size == '/' ? dir_size(dir + '/' + name) : size.to_i)
      end
      total_size
    end

    def mount_stats(mount)
      mount_stats = {}

      output = ssh_cmd "df -k " + mount + "|tail -1| awk '{print $2\",\"$3\",\"$4}'" 
      if output
        output = output.split(',').map{|s| s.to_i}

        mount_stats['total'] = output[0] * 1024
        mount_stats['used'] = output[1] * 1024
        mount_stats['available'] = output[2] * 1024
        return mount_stats
      else
        false
      end
    end
    
    
    ###### Misc methods ########################################################
    
    # Performs the given operation (:start, :stop, :restart, :status) for the
    # specified service (ie "mysql"). Requires that the "service" bin is in
    # root's PATH.
    # Please be aware that the output format and exit codes for the service
    # binary vary between Linux distros! You may find that you need to override
    # methods that call Host#service with :status operation (such as 
    # DB#probe_running) in a custom plugin, to parse the output properly on 
    # your chosen Linux distro.
    def service(operation, name, options='')
      ssh_cmd "service #{name} #{operation.to_s} #{options}".rstrip
    end
    
    # Changes the I/O scheduler to name (such as 'deadline', 'noop', 'cfq')
    # for the specified device.
    def set_io_scheduler(name, device='sda')
      output "Setting I/O scheduler for #{device} to #{name}."
      ssh_cmd "echo '#{name}' >/sys/block/#{device}/queue/scheduler"
    end
    
    # Confirms that the specified binary is installed and on the shell path.
    def confirm_installed(program_name)
      out = ssh_cmd "which #{program_name}"
      raise "#{program_name} not installed, or missing from path" if out =~ /no #{program_name} in /
      true
    end
    
    # Checks if there's a process with the given process ID running on this host.
    # Optionally also checks if matching_string is contained in the process name.
    # Returns true if so, false if not.
    # Warning: this implementation assumes Linux-style "ps" command; will not work
    # on BSD hosts.
    def pid_running?(pid, matching_string=false)
      if matching_string
        ssh_cmd("ps --no-headers -o command #{pid} | grep '#{matching_string}' | wc -l").chomp.to_i > 0
      else
        ssh_cmd("ps --no-headers #{pid} | wc -l").chomp.to_i > 0
      end
    end
    
    # Returns number of cores on machine. (reflects virtual cores if hyperthreading
    # enabled, so might be 2x real value in that case.)
    # Not currently used by anything in Jetpants base, but might be useful for plugins
    # that want to tailor the concurrency level to the machine's capabilities.
    def cores
      return @cores if @cores
      count = ssh_cmd %q{cat /proc/cpuinfo|grep 'processor\s*:' | wc -l}
      @cores = (count ? count.to_i : 1)
    end
    
    # Returns the amount of memory on machine, either in bytes (default) or in GB.
    # Linux-specific.
    def memory(in_gb=false)
      line = ssh_cmd 'cat /proc/meminfo | grep MemTotal'
      matches = line.match /(?<size>\d+)\s+(?<unit>kB|mB|gB|B)/
      size = matches[:size].to_i
      multipliers = {kB: 1024, mB: 1024**2, gB: 1024**3, B: 1}
      size *= multipliers[matches[:unit].to_sym]
      in_gb ? size / 1024**3 : size
    end
    
    # Returns the machine's hostname
    def hostname
      return 'unknown' unless available?
      @hostname ||= ssh_cmd('hostname').chomp
    end
    
    # Displays the provided output, along with information about the current time,
    # and self (the IP of this Host)
    def output(str)
      str = str.to_s.strip
      str = nil if str && str.length == 0
      str ||= "Completed (no output)"
      output = Time.now.strftime("%H:%M:%S") + " [#{self}] "
      output << str
      print output + "\n"
      output
    end
    
    # Returns the host's IP address as a string.
    def to_s
      return @ip
    end
    
    # Returns self, since this object is already a Host.
    def to_host
      self
    end
    
  end
end
