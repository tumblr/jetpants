require 'net/ssh'
require 'socket'

module Jetpants

  # Encapsulates a UNIX server that we can SSH to as root. Maintains a pool of SSH
  # connections to the host as needed.
  class Host
    include CallbackHandler
    include Output

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
      @clone_multi_threaded = false	# default use fast_copy_chain
      @service_manager = nil
    end

    # Returns a Host object for the machine Jetpants is running on.
    def self.local(interface=false)
      interface ||= (Jetpants.local_private_interface || Jetpants.private_interface)
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
          params[:port] = Jetpants.ssh_port if Jetpants.ssh_port
          user          = Jetpants.ssh_user
          begin
            @lock.synchronize do
              conn = Net::SSH.start(@ip, user, params)
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
          output "Executing (attempt #{failures + 1} / #{attempts}) on #{@ip}: #{c}" if Jetpants.debug
          result = conn.exec! c do |ch, stream, data|
            if stream == :stderr
              output "SSH ERROR: #{data}"
            end

            ch[:result] ||= ''
            ch[:result] << data
          end
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

    def manage_free_mem(free_mem_limit_mb)
      return if free_mem_limit_mb == 0
      loop do
        current_free_mem = ssh_cmd("free -m | grep Mem | awk {'print $4'}").rstrip.to_i
        ssh_cmd("echo 1 > /proc/sys/vm/drop_caches") if current_free_mem < free_mem_limit_mb
        sleep(60)
      end
    end

    def watch_free_mem(free_mem_limit_mb)
      Thread.new{ manage_free_mem(free_mem_limit_mb) }
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
    #             * :exclude_files    =>  Exclude these filens while copying data from base_dir. String, or Array of Strings.
    #             * :port             =>  port number to use for netcat. defaults to 7000 if omitted.
    #             * :overwrite        =>  if true, don't raise an exception if the base_dir is non-empty or :files exist. default false.
    def fast_copy_chain(base_dir, targets, options={})
      # Normalize the filenames param so it is an array
      filenames = options[:files] || ['.']
      filenames = [filenames] unless filenames.respond_to?(:each)

      exclude_files = options[:exclude_files]
      exclude_files = [exclude_files] unless exclude_files.respond_to?(:each)
      exclude_str = ""
      tar_options = ""
      unless options[:exclude_files].nil?
        exclude_str = "Excluding: (#{exclude_files.keys.join(",")})"
        tar_options = exclude_files.map { |file, size| "--exclude='#{file}'".sub("./", "") }.join ' '
      end

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

      should_encrypt = false
      targets.each do |t|
        should_encrypt = should_encrypt || should_encrypt_with?(t)
      end

      if Jetpants.encrypt_with && Jetpants.decrypt_with && should_encrypt
        enc_bin = Jetpants.encrypt_with.split(' ')[0]
        confirm_installed enc_bin
        output "Using #{enc_bin} for encryption"
      else
        output "Not encrypting data stream, either no encryption method specified or encryption unneeded with target"
      end

      # On each destination host, do any initial setup (and optional validation/erasing),
      # and then listen for new files.  If there are multiple destination hosts, all of them
      # except the last will use tee to "chain" the copy along to the next machine.
      workers = []
      free_mem_managers = []
      targets.reverse.each_with_index do |t, i|
        dir = destinations[t]
        raise "Directory #{t}:#{dir} looks suspicious" if dir.include?('..') || dir.include?('./') || dir == '/' || dir == ''

        if Jetpants.compress_with || Jetpants.decompress_with
          decomp_bin = Jetpants.decompress_with.split(' ')[0]
          t.confirm_installed decomp_bin
        end

        if Jetpants.encrypt_with && Jetpants.decrypt_with && should_encrypt
          decrypt_bin = Jetpants.decrypt_with.split(' ')[0]
          t.confirm_installed decrypt_bin
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
        decryption_pipe = (Jetpants.decrypt_with && should_encrypt) ? "| #{Jetpants.decrypt_with}" : ''
        if i == 0
          workers << Thread.new { t.ssh_cmd "cd #{dir} && nc -l #{port} #{decryption_pipe} #{decompression_pipe} | tar x" }
          t.confirm_listening_on_port port
          t.output "Listening with netcat."
        else
          tt = targets.reverse[i-1]
          fifo = "fifo#{port}"
          workers << Thread.new { t.ssh_cmd "cd #{dir} && mkfifo #{fifo} && nc #{tt.ip} #{port} <#{fifo} && rm #{fifo}" }
          checker_th = Thread.new { t.ssh_cmd "while [ ! -p #{dir}/#{fifo} ] ; do sleep 1; done" }
          raise "FIFO not found on #{t} after 10 tries" unless checker_th.join(10)
          workers << Thread.new { t.ssh_cmd "cd #{dir} && nc -l #{port} | tee #{fifo} #{decryption_pipe} #{decompression_pipe} | tar x" }
          t.confirm_listening_on_port port
          t.output "Listening with netcat, and chaining to #{tt}."
        end
        free_mem_managers << t.watch_free_mem(Jetpants.free_mem_min_mb || 0)
      end

      # Start the copy chain.
      output "Sending files over to #{targets[0]}: #{file_list} #{exclude_str}"
      compression_pipe = Jetpants.compress_with ? "| #{Jetpants.compress_with}" : ''
      encryption_pipe = (Jetpants.encrypt_with && should_encrypt) ? "| #{Jetpants.encrypt_with}" : ''
      ssh_cmd "cd #{base_dir} && tar c #{tar_options} #{file_list} #{compression_pipe} #{encryption_pipe} | nc #{targets[0].ip} #{port}"
      workers.each {|th| th.join}
      output "File copy complete."

      free_mem_managers.each(&:exit)

      # Verify
      if options[:exclude_files].nil?
        output "Verifying file sizes and types on all destinations."
        compare_dir base_dir, destinations, options
        output "Verification successful."
      end
    end

    # This is the method used by each thread to clone a part of file to targets.
    # The work item has all the necessary information for the thread to setup the cloning chain
    #
    # The basic logic of copy chain remains the same as that of 'fast_copy_chain'.
    # The chain has been oursourced to the individual scripts 'sender' and 'receiver'.  These
    # 2 scripts are deployed on each DB node.  Jetpants orchestrate the transfers using
    # 'ncat' (not nc) through a single port and multiple connections between sender and the
    # receivers. (The 'ncat' allows us to exec the 'receiver' script on receiving a connection
    # request from 'sender', multiple connections are not supported by 'nc').
    #
    def clone_part(work_item, base_dir, targets)
      #output "Sending item: #{work_item}"
      # Both 'sender' and 'receiver' use the specific parameter YAML file to perform the
      # transfer.  These parameters are set by jetpants through following hashes based on
      # the given work item.
      sender_params = {}
      receiver_params = {}

      should_encrypt = work_item['should_encrypt']
      block_count = work_item['size'] / Jetpants.block_size
      block_count += 1 if (work_item['size'] % Jetpants.block_size != 0)
      block_offset = work_item['offset'] / Jetpants.block_size

      sender_params['base_dir']     = base_dir
      sender_params['filename']     = work_item['filename'].sub('/./', '/')
      sender_params['block_count']  = block_count
      sender_params['block_offset'] = block_offset
      sender_params['block_size']   = Jetpants.block_size

      sender_params['transfer_id']  = work_item['transfer_id']

      sender_params['compression_cmd'] = "#{Jetpants.compress_with}" if Jetpants.compress_with
      sender_params['encryption_cmd']  = "#{Jetpants.encrypt_with}"  if (Jetpants.encrypt_with && should_encrypt)

      receiver_params['block_count']  = sender_params['block_count']
      receiver_params['block_offset'] = sender_params['block_offset']
      receiver_params['block_size']   = sender_params['block_size']
      receiver_params['filename']     = sender_params['filename']
      receiver_params['transfer_id']  = work_item['transfer_id']

      receiver_params['decompression_cmd'] = "#{Jetpants.decompress_with}" if Jetpants.decompress_with
      receiver_params['decryption_cmd']  = "#{Jetpants.decrypt_with}"  if (Jetpants.decrypt_with && should_encrypt)

      port = work_item['port']

      destinations = targets
      targets = targets.keys
      workers = []

      targets.reverse.each_with_index do |target, i|
        receiver_params['base_dir'] = destinations[target]

        if i == 0
          receiver_params.delete('chain_ip')
          receiver_params.delete('chain_port')
        else
          # If chaining needs to be setup, we add those parameters to YAML
          chain_target = targets.reverse[i - 1]
          receiver_params['chain_ip']   = chain_target.ip
          receiver_params['chain_port'] = port
        end
        string = receiver_params.to_yaml.gsub("\n", "\\n")

        # For each receiver in the chain, write the transfer parameters
        # in the YAML file at specified location.
        #
        # This location must match the one used by 'receiver.rb' script in puppet
        #
        cmd = "echo -e \"#{string}\" > #{Jetpants.recv_param_path}"
        target.ssh_cmd(cmd)
      end

      sender_params['target_ip'] = targets[0].ip
      sender_params['target_port'] = port
      string = sender_params.to_yaml.gsub("\n", "\\n")	# New lines need additional escape
      # Sender also needs the transfer parameters in the YAML file at specified
      # location.  Set those too.
      cmd = "echo -e \"#{string}\" > #{Jetpants.send_param_path}"
      ssh_cmd(cmd)

      # Trigger the data transfer
      # Sender is going to read the transfer parameters from the specified
      # location and start the data transfer
      ssh_cmd!("#{Jetpants.sender_bin_path}")
    end

    def look_for_clone_marker(marker)
      sleep(1)	# It might take a bit to start the transfer
      cmd = "test -e #{marker} && echo \"Found\" || echo \"Not Found\""
      transfer_started = false
      # With 16 threads, the node becomes so io (network) and cpu intensive that, it might not
      # be able to confirm the transfer for a long time, so we will be patient and ensure the
      # transfer is running
      25.times do |sleep_time|
        result = ssh_cmd(cmd)
        if result.strip == "Found"
          transfer_started = true
          ssh_cmd("rm -f #{marker}")
          #output "Transfer chain started Marker: #{marker}"
          break
        else
          #output "Transfer chain has not yet started Marker: #{marker}"
          sleep(sleep_time)
        end
      end
      raise "Transfer issue with node #{self.ip}" unless transfer_started
    end

    # This method ensures that the transfer started across the hosts.
    # Success of this method is crucial for the whole operation, because
    # we use the same file to convey the parameters of transfer.  The
    # reason to use same parameter file is that 'ncat' is execing the
    # transfer for us when the new connection request is received on the
    # port.  We cannot exec different command per connection.  Imagine it
    # to be a 'xinetd'.
    #
    # The way we ensure that the transfer started is by checking for an
    # existance of a file at the last node in the chain.
    def ensure_transfer_started(targets, work_item)
      targets = targets.keys
      marker = "#{Jetpants.export_location}/__#{work_item['transfer_id']}.success"

      look_for_clone_marker(marker)
      targets.each do |target|
        target.look_for_clone_marker(marker)
      end
    end

    def watch_progress(workers, progress)
      sleep(1)   # Sleep now
      workers.each do |worker, work_item|
        unless worker.alive?
          filename = work_item['filename']
          progress[filename]['sent'] += work_item['size']
          pct_progress = (progress[filename]['sent'] * 100) / progress[filename]['total_size']
          output "#{filename}: #{pct_progress} % done"
          worker.join()
          workers.delete(worker)
        end
      end
    end

    #
    # Method used to divide the work of sending large files.
    #
    # Parameters to method are exactly similar as that of 'fast_copy_chain'.
    # :files option, however, include only the large files that we want
    # to clone using multiple threads
    #
    # This method does not check whether compression and encryption binaries
    # are installed or not.  This is because, most of the times, this will be
    # preceeded by a call to 'fast_copy_chain' to send out small files.
    # 'fast_copy_chain' does ensure the binaries are installed.
    #
    def faster_copy_chain(base_dir, targets, options={})
      filenames = options[:files]
      filenames = [filenames] unless filenames.respond_to?(:each)

      progress = {}
      destinations = targets
      targets = targets.keys
      targets.each do | target |
        dir = destinations[target]
        unless options[:overwrite]
          all_paths = filenames.keys.map {|f| dir + f}.join ' '
          dirlist = target.dir_list(all_paths)
          dirlist.each {|name, size| raise "File #{name} exists on destination and has nonzero size!" if size.to_i > 0}
        end
      end

      # Is encryption required?
      should_encrypt = false
      targets.each do |t|
        should_encrypt = should_encrypt || should_encrypt_with?(t)
      end

      # Divide each large file into Jetpants.split_size work items.
      # A thread will operate on each part independently.
      # We do not physically divide the file, the illusion of dividing a file is
      # created using the file offset the "dd" will operate on (man dd, skip and seek options)
      work_items = Queue.new
      filenames.each do |file, file_size|
        remaining_size = file_size
        num_parts = remaining_size / Jetpants.split_size
        num_parts += 1 if (remaining_size % Jetpants.split_size != 0)
        offset = 0
        size = [remaining_size, Jetpants.split_size].min

        for part in (1..num_parts)
          work_item = {'filename' => file, 'offset' => offset, 'size' => size, 'part' => part, 'should_encrypt' => should_encrypt}
          work_item['transfer_id'] = "#{work_item['filename'].gsub("/", "_")}_#{work_item['part']}"
          work_items << work_item
          offset += size
          remaining_size -= size
          size = [remaining_size, Jetpants.split_size].min
        end

        progress[file] = {'total_size' => file_size, 'sent' => 0 }
      end

      # Deciding upon number of threads to use for the copy
      # Based on the minimum core count of the node in the chain
      core_counts = [cores]
      targets.each do |target| core_counts << target.cores end
      num_threads = core_counts.min - 2   # Leave 2 cores for OS
      num_threads = num_threads > 12 ? 12 : num_threads   # Cap it somewhere, Flash will be HOT otherwise
      output "Using #{num_threads} threads to clone the big files with #{work_items.size} parts"

      port = options[:port]
      # We only have one port available, so we use 'ncat' utility that allows us to
      # exec per received connection.
      #
      # Now what we 'exec' is a ruby script called 'receiver.rb' that does all the
      # ugly shell handling of the chain.  So, following command starts the ncat
      # server on each receiver.  When it receives a connection it request, it
      # is going to fork-exec the 'receiver' script, that handles the data transfer
      #
      cmd = "ncat --recv-only -lk #{port} -m 100 --sh-exec \"#{Jetpants.receiver_bin_path}\""
      receiver_cmd = "nohup #{cmd} > /dev/null 2>&1 &"

      targets.each do |target|
        dir = destinations[target]
        raise "Directory #{t}:#{dir} looks suspicious" if dir.include?('..') || dir.include?('./') || dir == '/' || dir == ''

        target.ssh_cmd "mkdir -p #{dir}"

        target.ssh_cmd(receiver_cmd)
        target.confirm_listening_on_port(port, timeout = 30)
      end

      workers = {}
      until work_items.empty?
        sleep(1)
        work_item = work_items.deq
        work_item['port'] = port
        worker = Thread.new { clone_part(work_item, base_dir, destinations) }
        workers[worker] = work_item
        ensure_transfer_started(destinations, work_item)

        while workers.count >= num_threads
          watch_progress(workers, progress)
        end
      end

      output "All work items submitted for transfer"
      # All work items have been submitted for processing now,
      # Let us just wait for all of them to finish
      until workers.count == 0
        watch_progress(workers, progress)
      end

      output "All work items transferred"
      # OK !! done, lets kill the ncat servers started on all the targets
      cmd_str = cmd.gsub!('"', '').strip
      targets.each do |target|
        target.ssh_cmd("pkill -f '#{cmd_str}'")
      end

      # Because we initiate the "dd" threads using root, the permissions of the
      # files are lost (owner:group).  We fix them now by querying for the same
      # at the source.
      # RE for stats output to extract the mode, user, group of the file

      filenames.each do |file, file_size|
        source_file = (base_dir + file).sub('/./', '/')
        result = ssh_cmd("stat #{source_file}").split("\n")[3]
        mode_stats = get_file_stats(source_file)
        raise "Could not get stats for source #{source_file}.  Clone is almost done, you can fix it manually" if mode_stats.nil?
        destinations.each do |target, dir|
          target_file = (dir + file).sub('/./', '/')
          raise "Invalid target file #{target_file} on Target #{target}.  Clone is almost done, you can fix it manually" if dir == '/' || dir == ''
          target.ssh_cmd("chmod #{mode_stats['mode']} #{target_file}")
          target.ssh_cmd("chown #{mode_stats['user']}:#{mode_stats['group']} #{target_file}")
        end
      end
    end
    # Quickly and efficiently recursively copies a directory to one or more target hosts using multiple threads
    #
    # This method first identifies the files that are larger than Jetpants.split_size (typical value: 10 GB),
    # breaks them into multiple parts of size "Jetpants.split_size" and uses: "dd | compression | encryption | nc"
    # for each part.  There are corresponding receivers at the targets.
    #
    # The files which are smaller in size than "Jetpants.split_size" are still sent using "fast_copy_chain"
    #
    # base_dir::  is base directory to copy from the source (self). Also the default destination base
    #             directory on the targets, if not supplied via next param.
    # targets::   is one of the following:
    #             * Host object, or any object that delegates method_missing to a Host (such as DB)
    #             * array of Host objects (or delegates)
    #             * hash mapping Host objects (or delegates) to destination base directory overrides (as string)
    # options::   is a hash that can contain --
    #             * :files     =>  only copy these filenames instead of entire base_dir. String, or Array of Strings.
    #             * :port      =>  port number to use for netcat. defaults to 7000 if omitted.
    #             * :overwrite =>  if true, don't raise an exception if the base_dir is non-empty or :files exist. default false.
    def multi_threaded_cloning(base_dir, targets, options={})
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

      # Make sure we find all ibd and tokudb files separately, those will be sent multi-threaded
      # Let us append the base_dir to all entries without '/', actually we can append it to all,
      # because fast_copy_chain first 'cd' to the base_dir and then tar it.
      multi_threaded_files = {}

      # Check if the file we are cloning is directory or a file.
      # The problem with dir_list is that it cannot convincingly tell if
      # the name is directory or file, where "/" is not helpful. eg.
      # if you have a 'test' directory and a file named 'test' under it
      # then output if {'test':<some file size>}
      # And if you have 'test' file at the same location, it still returns
      # the same.  So, it can't tell whether the output 'test' file is under
      # the directory or not, coz it does not retain the whole directory path
      # Lets traverse the tree to find the files to send using multi-threaded approach
      queue = filenames.map {|f| ['', f]}
      while (tuple = queue.shift)
        subdir, filename = tuple

        pathname = base_dir + '/' + subdir + '/' + filename;
        dir_list = dir_list(pathname)

        perm_stats = get_file_stats(pathname)

        # If it is a big file, add it for multi-threading and continue
        if perm_stats['permissions'].split("")[0] == '-' and dir_list.first[1].to_i > Jetpants.split_size
            multi_threaded_files[subdir + filename] = dir_list.first[1].to_i
            next
        end

        dir_list.each do |name, size|
          if size == '/'
            queue.concat([[subdir + filename + '/', name]])
            next
          elsif size.to_i > Jetpants.split_size
            file_str = (subdir + filename + '/' + name)
            multi_threaded_files[file_str] = size.to_i
          end
        end
      end

      # Send the directory structure out first (small files =< Jetpants.split_size)
      fast_copy_chain(base_dir, destinations,
                     :port          => options[:port],
                     :overwrite     => options[:overwrite],
                     :files         => options[:files],
                     :exclude_files => multi_threaded_files)

      # Then the huge files that needs splitting (large files > Jetpants.split_size)
      faster_copy_chain(base_dir, destinations,
                     :port      => options[:port],
                     :overwrite => options[:overwrite],
                     :files     => multi_threaded_files)

      output "Verifying file sizes and types on all destinations."
      compare_dir base_dir, destinations, options
      output "Verification successful."
    end

    # Add a hook point to determine whether a host should encrypt a data stream between two hosts
    # This is useful to avoid encryption latency in a secure environment
    def should_encrypt_with?(host)
      Jetpants.encrypt_file_transfers
    end

    # Given the name of a directory or single file, returns a hash of filename => size of each file present.
    # Subdirectories will be returned with a size of '/', so you can process these differently as needed.
    # WARNING: This is brittle. It parses output of "ls". If anyone has a gem to do better remote file
    # management via ssh, then please by all means send us a pull request!
    def dir_list(dir)
      ls_out = ssh_cmd "ls --color=never -1AgGF #{dir}"  # disable color, 1 file per line, all but . and .., hide owner+group, include type suffix
      result = {}
      ls_out.split("\n").each do |line|
        next unless matches = line.match(/^[\.\w-]+\s+\d+\s+(?<size>\d+).*(?:\d\d:\d\d|\d{4})\s+(?<name>.*)$/)
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
      # Normalize the filenames param so it is an array
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

    ###### Service management methods ##########################################
    def service_api
      @service_manager ||= Jetpants::HostService.pick_by_preflight(self)
    end

    def service_start(name, options=[])
      output service_api.start(name, options)
    end

    def service_restart(name, options=[])
      output service_api.restart(name, options)
    end

    def service_stop(name)
      output service_api.stop(name)
    end

    def service_running?(name)
      service_api.running?(name)
    end

    def service(operation, name, options='')
      output "Warning: Calling Host.service directly is deprecated!".red
      service_manager.service_direct(operation, name, options).rstrip
    end

    ###### Misc methods ########################################################
    # `stat` call to get all the information about the given file
    def get_file_stats(filename)
      mode_re = /^Access:\s+\((?<mode>\d+)\/(?<permissions>[drwx-]+)\)\s+Uid:\s+\(\s+\d+\/\s+(?<user>\w+)\)\s+Gid:\s+\(\s+\d+\/\s+(?<group>\w+)\)$/x
      result = ssh_cmd("stat #{filename}").split("\n")
      mode_line = result[3]
      tokens = mode_line.match(mode_re)

      # Later when we need more info we will merge hashes obtained from REs
      tokens
    end

    # Changes the I/O scheduler to name (such as 'deadline', 'noop', 'cfq')
    # for the specified device.
    def set_io_scheduler(name, device='sda')
      output "Setting I/O scheduler for #{device} to #{name}."
      ssh_cmd "echo '#{name}' >/sys/block/#{device}/queue/scheduler"
    end

    def has_installed(program_name)
      # The regex will match if the program is missing,
      # thusly: no match means it is installed
      # thusly: nil means it is present
      (ssh_cmd("which #{program_name}") =~ /no #{program_name} in /).nil?
    end

    # Confirms that the specified binary is installed and on the shell path.
    def confirm_installed(program_name)
      raise "#{program_name} not installed, or missing from path" unless has_installed(program_name)
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

    # Returns the host's IP address as a string.
    def to_s
      return @ip
    end

    def inspect
      to_s
    end

    # Returns self, since this object is already a Host.
    def to_host
      self
    end

  end
end
