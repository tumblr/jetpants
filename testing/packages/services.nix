{
  # Okay so maybe there was a bit of misdirection here, but this one
  # really does directly import the services we define. It is common
  # to use a sort of aggregator file to include all your services in
  # one place.

  # Note that importing them doesn't necessarily `enable` them, so it
  # doesn't actually have any impact on your system until you enable
  # it.

  # I put these in order of complexity so we can understand simpler
  # ones and move on up to harder ones.
  imports = [
    # [Jetpants Service](./jetpants/service.html)
    ./jetpants/service.nix

    # [MySQL Container Service](./mysql-container/service.html)
    ./mysql-container/service.nix

    # [Collins Container Service](./collins-container/service.html)
    ./collins-container/service.nix
  ];
}
