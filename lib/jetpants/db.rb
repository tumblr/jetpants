require 'sequel'
require 'json'

module Jetpants
  
  # A Jetpants::DB is a specific mysql instance running on a particular IP and port.
  # It also contains a Jetpants::Host object corresponding to the IP; any missing
  # method calls get delegated to the Host.
  #
  # This class has been split across several files due to its size.  Please see
  # lib/jetpants/db/*.rb for the bulk of its logic, which has been divided along
  # functional lines.
  class DB
    include CallbackHandler
    
    # IP address (as a string) of the MySQL instance
    attr_reader :ip
    
    # Port number of the MySQL instance. The base Jetpants implementation only supports
    # port 3306, since this is necessary to crawl a replication hierarchy using SHOW
    # PROCESSLIST, which does not include slave port numbers. However, plugins may
    # override this behavior to support nonstandard ports and multi-instance-per-host
    # topologies.
    attr_reader :port
    
    # Jetpants::Host object that this MySQL instance runs on.
    attr_reader :host
    
    # We keep track of DB instances to prevent DB.new from every returning
    # duplicates.
    @@all_dbs = {}
    @@all_dbs_mutex = Mutex.new
    
    def self.clear
      @@all_dbs_mutex.synchronize {@@all_dbs = {}}
    end
    
    # Because this class is rather large, methods have been grouped together
    # and moved to separate files in lib/jetpants/db. We load these all now.
    # They each just re-open the DB class and add some methods.
    Dir[File.join File.dirname(__FILE__), 'db', '*'].each {|f| require f}
    
    # We override DB.new so that attempting to create a duplicate DB object
    # (that is, one with the same IP and port as an existing DB object)
    # returns the original object.
    def self.new(ip, port=3306)
      ip, embedded_port = ip.split(':', 2)
      port = embedded_port.to_i if embedded_port
      addr = "#{ip}:#{port}"
      @@all_dbs_mutex.synchronize do
        @@all_dbs[addr] = nil unless @@all_dbs[addr].is_a? self
        @@all_dbs[addr] ||= super
      end
    end
    
    def initialize(ip, port=3306)
      @ip, @port = ip, port.to_i
      @host = Host.new(ip)
      
      # These get set upon DB#probe being run
      @master = nil
      @slaves = nil
      @repl_paused = nil
      @running = nil
      
      # These get set upon DB#connect being run
      @user = nil
      @schema = nil
      
      # This is ephemeral, only known to Jetpants if you previously called
      # DB#start_mysql or DB#restart_mysql in this process
      @options = []
    end
    
    ###### Host methods ########################################################
    
    # Jetpants::DB delegates missing methods to its Jetpants::Host.
    def method_missing(name, *args, &block)
      if @host.respond_to? name
        @host.send name, *args, &block
      else
        super
      end
    end
    
    # Alters respond_to? logic to account for delegation of missing methods
    # to the instance's Host.
    def respond_to?(name, include_private=false)
      super || @host.respond_to?(name)
    end
    
    # Returns true if the supplied Jetpants::DB is on the same Jetpants::Host
    # as self.
    def same_host_as?(db)
      @ip == db.ip
    end
    
    ###### Misc methods ########################################################
    
    # Displays the provided output, along with information about the current time,
    # self, and optionally a Jetpants::Table name.
    def output(str, table=nil)
      str = str.to_s.strip
      str = nil if str && str.length == 0
      str ||= "Completed (no output)"
      output = Time.now.strftime("%H:%M:%S") + " [#{self}] "
      output << table.name << ': ' if table
      output << str
      print output + "\n"
      output
    end
    
    # DB objects are sorted as strings, ie, by calling to_s
    def <=> other
      to_s <=> other.to_s
    end
    
    # Returns a string in the form "ip:port"
    def to_s
      "#{@ip}:#{@port}"
    end
    
    # Returns self, since self is already a Jetpants::DB.
    def to_db
      self
    end
    
    # Returns the instance's Jetpants::Host.
    def to_host
      @host
    end
    
  end
end
