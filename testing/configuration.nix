{ config, lib, pkgs, ... }:
{
  # ## imports
  # The imports here define additional services and packages we can
  # use in our configuration.
  # We'll get in to that in those files, but
  # [`packages.nix`](./packages/packages.html)) adds packages like
  # jetpants and collins to the overall system, and
  # [`services.nix`](./packages/services.html) allows us to set things
  # like `services.collins.enable = true`.
  imports = [
    ./packages/packages.nix
    ./packages/services.nix
  ];

  # ## Services
  # Enable the [collins
  # service](./packages/collins-container/service.html). We could have
  # additional config here like a listening port or whatever but for
  # this demo that configurability has been left out.
  services.collins.enable = true;

  # Jetpants isn't really a service per say, but `enable`ing the
  # service here [writes a global
  # configuration](./packages/jetpants/service.html) file used by the
  # client.
  services.jetpants.enable = true;


  # ## Containers
  # The mysql-containers service is defined by us in
  # [`./packages/mysql-container/service.nix`](./packages/mysql-container/service.html).
  # There is a bit of magic going on here mostly because of the
  # limited time to do the hack.

  # Basically what is going on here is we create a container on the
  # system running mysql, with the last octet of its IP being
  # represented here. The first three octets of its IP are hard-coded
  # in this project to simplify the hack. In this hack, the hard-coded
  # network is `10.50.2.1/24`.

  # The system creates a network bridge and what-not so each
  # container can talk to each other, and the system can also talk to
  # each container.

  # For example, `"10" = {};` represents a mysql server running in a
  # container whose IP is `10.50.2.10`.

  services.openssh = {
    enable = true;
    permitRootLogin = "yes";
  };
  users.extraUsers.root.initialPassword = "foobar";


  # We install the collins-client (which is defined in
  # `./packages/packages.nix` by
  # [`./packages/collins-client/default.nix`](./packages/collins-client/default.html))
  # to the system packages,
  environment.systemPackages = with pkgs; [
    collins-client
    jetpants
    percona-toolkit
    mysql
    emacs
    vim
  ];
}
