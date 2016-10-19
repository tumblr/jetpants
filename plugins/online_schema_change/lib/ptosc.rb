
module Jetpants
  class PTOSC
    def initialize pool, database, table, alter, username, password, opts
      @pool = pool
      @database = database
      @table = table
      @alter = alter
      @username = username
      @password = password
      @opts = opts
    end

    def dry_run= flag
      @opts[:dry_run] = !!flag
    end

    def dry_run?
      !! @opts[:dry_run]
    end

    def delayed_rename?
      !! @opts[:delayed_rename]
    end

    def exec!
      IO.popen([{}, exec_command].concat(exec_options), :err => [:child, :out]) do |io|
        yield io
      end

      return $?.to_i == 0
    end

    def exec_command_line
      [exec_command].concat(exec_options).join(' ')
    end

    def exec_command
      "pt-online-schema-change"
    end

    def exec_options
      opts = [
        "--nocheck-replication-filters",
        "--max-load", "Threads_running:#{max_running_threads}",
        "--critical-load", "Threads_running:#{critical_threads_running}",
        "--nodrop-old-table",
        "--set-vars", "wait_timeout=100000",
        "--print",
        "--alter", @alter,
      ]

      if @opts[:slave_monitor_dsn]
        opts << '--recursion-method'
        opts << "dsn=#{@opts[:slave_monitor_dsn]}"
      end

      if @opts[:dry_run]
        opts << '--dry-run'
      else
        opts << '--no-check-alter'
        opts << '--execute'

        # nodrop-new-table was originally in the default opts, but it was found ptosc would
        # create the tables anyway (despite passing --dry-run) and then we'd create many
        # extra tables through the process.
        opts << "--nodrop-new-table"
      end

      if @opts[:no_check_plan]
        opts << '--nocheck-plan'
      end

      if @opts[:delayed_rename]
        opts << '--no-swap-tables'
        opts << '--no-drop-triggers'
      end

      opts << "D=#{@database},t=#{@table},h=#{@pool.master.ip},u=#{@username},p=#{@password}"
      return opts
    end

    def max_running_threads
      # Try to keep  at 50 running threads, unless the running threads on the server is over 50.
      @max_threads ||= @pool.max_threads_running(30, 1)
      @max_threads = 50 unless @max_threads > 50
    end

    def critical_threads_running
      # The running thread threshold to abort the alter. To absorb spikes, we want this number to
      # be at least 500, but up to 2x the maximum running threads.
      [500, 2 * max_running_threads].max
    end
  end
end
