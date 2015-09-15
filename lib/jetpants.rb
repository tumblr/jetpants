# This is the Jetpants module entrypoint. It loads all base Jetpants files,
# configuration, and plugins. It then initializes the object model / database
# topology.

require 'sequel'
require 'net/ssh'
require 'yaml'

module Jetpants; end

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), 'jetpants'), File.join(File.dirname(__FILE__), '..', 'plugins')
%w(output callback table host db pool topology shard shardpool monkeypatch commandsuite).each {|g| require g}

# Since Jetpants is extremely multi-threaded, we need to force uncaught exceptions to
# kill all threads in order to have any kind of sane error handling.
Thread.abort_on_exception = true

# Namespace for the Jetpants toolkit.
module Jetpants

  # Establish default configuration values, and then merge in whatever we find globally
  # in /etc/jetpants.yaml and per-user in ~/.jetpants.yaml
  @config = {
    'max_concurrency'         =>  20,         # max threads/conns per database
    'standby_slaves_per_pool' =>  2,          # number of standby slaves in every pool
    'backup_slaves_per_pool'  =>  1,          # number of backup slaves in every pool
    'mysql_schema'            =>  'test',     # database name
    'mysql_app_user'          =>  'appuser',  # mysql user for application
    'mysql_app_password'      =>  '',         # mysql password for application
    'mysql_repl_user'         =>  'repluser', # mysql user for replication
    'mysql_repl_password'     =>  '',         # mysql password for replication
    'mysql_root_password'     =>  false,      # mysql root password. omit if specified in /root/.my.cnf instead.
    'mysql_grant_ips'         =>  ['192.168.%'],  # mysql user manipulations are applied to these IPs
    'mysql_grant_privs'       =>  ['ALL'],    # mysql user manipulations grant this set of privileges by default
    'mysql_clone_ignore'      =>  ['information_schema', 'performance_schema'], # these schemata will be ignored during cloning
    'export_location'         =>  '/tmp',     # directory to use for data dumping
    'verify_replication'      =>  true,       # raise exception if the 2 repl threads are in different states, or if actual repl topology differs from Jetpants' understanding of it
    'plugins'                 =>  {},         # hash of plugin name => arbitrary plugin data (usually a nested hash of settings)
    'ssh_keys'                =>  nil,        # array of SSH key file locations
    'sharded_tables'          =>  [],         # hash of {shard_pool => {name => {sharding_key=>X, chunks=>Y}} hashes
    'compress_with'           =>  false,      # command line to use for compression in large file transfers
    'decompress_with'         =>  false,      # command line to use for decompression in large file transfers
    'private_interface'       =>  'bond0',    # network interface corresponding to private IP
    'output_caller_info'      =>  false,      # includes calling file, line and method in output calls
    'debug_exceptions'        =>  false,      # open a pry session when an uncaught exception is thrown
    'repl_wait_interval'      =>  1,          # default sleep interval currently used in pause_replication_with
    'lazy_load_pools'         =>  false,      # whether to populate the topology pools when first accessed
    'log_file'                =>  '/var/log/jetpants.log', # where to log all output from the jetpants commands
    'local_private_interface' =>  nil,        # local network interface corresponding to private IP of the machine jetpants is running on
    'free_mem_min_mb'         =>  0,          # Minimum amount of free memory in MB to be maintained on the node while performing the task (eg. network copy)
    'default_shard_pool'      =>  nil,        # default pool for sharding operations
    'import_without_indices'  =>  false,
    'ssl_ca_path'             =>  '/var/lib/mysql/ca.pem',
    'ssl_client_cert_path'    =>  '/var/lib/mysql/client-cert.pem',
    'ssl_client_key_path'     =>  '/var/lib/mysql/client-key.pem',
    'encrypt_with'            =>  false,      # command line stream encryption binary
    'decrypt_with'            =>  false,      # command line stream decryption binary
    'encrypt_file_transfers'  =>  false       # flag to use stream encryption
  }

  config_paths = ["/etc/jetpants.yaml", "~/.jetpants.yml", "~/.jetpants.yaml"]
  config_loaded = false

  config_paths.each do |path|
    begin
      overrides = YAML.load_file(File.expand_path path)
      @config.deep_merge! overrides
      config_loaded = true
    rescue Errno::ENOENT => error
    rescue ArgumentError => error
      puts "YAML parsing error in configuration file #{path} : #{error.message}\n\n"
      exit
    end
  end

  unless config_loaded
    puts "Could not find any readable configuration files at either /etc/jetpants.yaml or ~/.jetpants.yaml\n\n"
    exit
  end
  
  class << self
    include Output

    # A singleton Jetpants::Topology object is accessible from the global
    # Jetpants module namespace.
    attr_reader :topology

    # Returns true if the specified plugin is enabled, false otherwise.
    def plugin_enabled?(plugin_name)
      @config['plugins'].has_key? plugin_name
    end
    
    # Returns a hash containing :user => username string, :pass => password string
    # for the MySQL application user, as found in Jetpants' configuration. Plugins
    # may freely override this if there's a better way to obtain this password --
    # for example, if you already distribute an application configuration or
    # credentials file to all of your servers.
    def app_credentials
      {user: @config['mysql_app_user'], pass: @config['mysql_app_password']}
    end
    
    # Returns a hash containing :user => username string, :pass => password string
    # for the MySQL replication user, as found in Jetpants' configuration. Plugins
    # may freely override this if there's a better way to obtain this password --
    # for example, by parsing master.info on a specific slave in your topology.
    # SEE ALSO: DB#replication_credentials, which only falls back to the global
    # version when needed.
    def replication_credentials
      {user: @config['mysql_repl_user'], pass: @config['mysql_repl_password']}
    end
    
    # Proxy missing top-level Jetpants methods to the configuration hash,
    # or failing that, to the Topology singleton.
    def method_missing(name, *args, &block)
      if @config.has_key? name.to_s
        @config[name.to_s]
      elsif name.to_s[-1] == '=' && @config.has_key?(name.to_s[0..-2])
        var = name.to_s[0..-2]
        @config[var] = args[0]
      elsif @topology.respond_to? name
        @topology.send name, *args, &block
      else
        super
      end
    end
    
    def respond_to?(name, include_private=false)
      super || @config[name] || @topology.respond_to?(name)
    end

    def with_retries(retries = nil, max_retry_backoff = nil)
      retries = 1 unless retries.is_a?(Integer) and retries >= 0
      max_retry_backoff = 16 unless max_retry_backoff.is_a?(Integer) and max_retry_backoff >= 0
      backoff ||= 0

      yield if block_given?
    rescue SystemExit, Interrupt
      raise
    rescue Exception => e
      output e
      if retries.zero?
        output "Max retries exceeded. Not retrying."
        raise e
      else
        retries -= 1
        output "Backing off for #{backoff} seconds, then retrying."
        sleep backoff
        # increase backoff, taking the path 0, 1, 2, 4, 8, ..., max_retry_backoff
        backoff = [(backoff == 0) ? 1 : backoff << 1, max_retry_backoff].min
        retry
      end
    end
  end

  # Load plugins at this point, to allow them to override any of the previously-defined methods
  # in any of the loaded classes
  (@config['plugins'] || {}).keys.each do |name|
    begin
      require "#{name}/#{name}"
    rescue LoadError
      require name
    end
  end

  # Finally, initialize topology object
  @topology = Topology.new
  @topology.load_shard_pools unless @config['lazy_load_pools']
  @topology.load_pools unless @config['lazy_load_pools']
end
