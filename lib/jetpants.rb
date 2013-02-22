# This is the Jetpants module entrypoint. It loads all base Jetpants files,
# configuration, and plugins. It then initializes the object model / database
# topology.

require 'sequel'
require 'net/ssh'
require 'yaml'

module Jetpants; end

$LOAD_PATH.unshift File.join(File.dirname(__FILE__), 'jetpants'), File.join(File.dirname(__FILE__), '..', 'plugins')
%w(callback table host db pool topology shard monkeypatch).each {|g| require g}

# Since Jetpants is extremely multithreaded, we need to force uncaught exceptions to
# kill all threads in order to have any kind of sane error handling.
Thread.abort_on_exception = true

# Namespace for the Jetpants toolkit.
module Jetpants
  
  # Establish default configuration values, and then merge in whatever we find globally
  # in /etc/jetpants.yaml and per-user in ~/.jetpants.yaml
  @config = {
    'max_concurrency'         =>  20,         # max threads/conns per database
    'standby_slaves_per_pool' =>  2,          # number of standby slaves in every pool
    'mysql_schema'            =>  'test',     # database name
    'mysql_app_user'          =>  'appuser',  # mysql user for application
    'mysql_app_password'      =>  '',         # mysql password for application
    'mysql_repl_user'         =>  'repluser', # mysql user for replication
    'mysql_repl_password'     =>  '',         # mysql password for replication
    'mysql_root_password'     =>  false,      # mysql root password. omit if specified in /root/.my.cnf instead.
    'mysql_grant_ips'         =>  ['192.168.%'],  # mysql user manipulations are applied to these IPs
    'mysql_grant_privs'       =>  ['ALL'],    # mysql user manipulations grant this set of privileges by default
    'export_location'         =>  '/tmp',     # directory to use for data dumping
    'verify_replication'      =>  true,       # raise exception if the 2 repl threads are in different states, or if actual repl topology differs from Jetpants' understanding of it
    'plugins'                 =>  {},         # hash of plugin name => arbitrary plugin data (usually a nested hash of settings)
    'ssh_keys'                =>  nil,        # array of SSH key file locations
    'sharded_tables'          =>  [],         # array of name => {sharding_key=>X, chunks=>Y} hashes
    'compress_with'           =>  false,      # command line to use for compression in large file transfers
    'decompress_with'         =>  false,      # command line to use for decompression in large file transfers
    'private_interface'       =>  'bond0',    # network interface corresponding to private IP
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
    # A singleton Jetpants::Topology object is accessible from the global 
    # Jetpants module namespace.
    attr_reader :topology
    
    # Returns true if the specified plugin is enabled, false otherwise.
    def plugin_enabled?(plugin_name)
      !!@config['plugins'][plugin_name]
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
  end

  # Load plugins at this point, to allow them to override any of the previously-defined methods
  # in any of the loaded classes
  (@config['plugins'] || {}).each do |name, attributes|
    begin
      require "#{name}/#{name}"
    rescue LoadError
      require name
    end
  end
  
  # Finally, initialize topology object
  @topology = Topology.new
  @topology.load_pools
end
