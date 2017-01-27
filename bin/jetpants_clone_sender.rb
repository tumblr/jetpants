#!/opt/ruby/2.3/bin/ruby

#
# Sender:
#
# This ruby script is invoked by Jetpants as a part of cloning process to transfer a part of a file
# to the targets.  Data transfer is controlled by the parameters in 'param_file'.  Based on the
# parameters, a simple command chain "dd | compression | encryption" is used for the transfer.
#
# When sender has successfully read the parameters and built the command line, it cerates a
# marker at /db-binlog/<some_id>.success.  That marker is indication to Jetpants that the transfer
# has almost started.  Based on these markers, Jetpants orchestrate the further transfers.
#
require 'yaml'
require 'time'
require 'logger'

param_file = "/db-binlog/__ncat_clone_send_params.yaml"
transfer_parameters = YAML.load(File.read(param_file))
log_file = "/var/log/jetpants_clone_sender.log"

io_size = 524288
# Sanity checks
raise "'base_dir' parameter missing in parameter file" unless transfer_parameters.key?('base_dir')
raise "'filename' parameter missing in parameter file" unless transfer_parameters.key?('filename')
raise "'block_count' parameter missing in parameter file"  unless transfer_parameters.key?('block_count')
raise "'block_offset' parameter missing in parameter file" unless transfer_parameters.key?('block_offset')
raise "'target_ip' parameter missing in parameter file"   unless transfer_parameters.key?('target_ip')
raise "'target_port' parameter missing in parameter file" unless transfer_parameters.key?('target_port')
raise "'transfer_id' parameter missing in parameter file"  unless transfer_parameters.key?('transfer_id')
base_dir     = transfer_parameters['base_dir']
filename     = transfer_parameters['filename']
block_count  = transfer_parameters['block_count'].to_i
block_offset = transfer_parameters['block_offset'].to_i
target_ip    = transfer_parameters['target_ip']
target_port  = transfer_parameters['target_port'].to_i
transfer_id  = transfer_parameters['transfer_id']

block_size = transfer_parameters['block_size'] || io_size

logger = Logger.new(log_file)
logger.formatter = proc do |severity, datetime, progname, msg|
  "[#{datetime}][#{transfer_id}] #{severity} #{msg}\n"
end

# Build the 'dd' command
send_dd = "dd if=#{filename} bs=#{block_size} count=#{block_count} skip=#{block_offset} 2>/dev/null"
logger.info("send dd = #{send_dd}")

# How to compress and encrypt?
compression_stage = ''
compression_stage = " | #{transfer_parameters['compression_cmd']} " if transfer_parameters['compression_cmd']

encryption_stage = ''
encryption_stage = " | #{transfer_parameters['encryption_cmd']} " if transfer_parameters['encryption_cmd']

# The pipeline of sender
final_cmd = "cd #{base_dir} && #{send_dd} #{compression_stage} #{encryption_stage} | ncat --send-only #{target_ip} #{target_port}"
logger.info("send cmd = #{final_cmd}")

# Save the parameter file
cmd = "cat #{param_file} > /db-binlog/__#{transfer_id}.success"
%x[ #{cmd} ]

# Fire it
%x[ #{final_cmd} ]
logger.info("Done")
