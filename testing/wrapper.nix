{ pkgs }:
rec {
  buildScript = tests: (pkgs.writeScript "build.sh" (''
    #!/bin/bash

    statuses=()

  '' + (pkgs.lib.concatMapStringsSep "\n"
         drvBuildScript
         tests) + ''

    for status in "''${statuses[@]}"; do
      echo "$status"
    done
  ''));

  drvBuildScript = {name, drv}: ''
    if ${pkgs.nix}/bin/nix-store -r ${drv}; then
      statuses+=("${name}: Success, log: nix-store --read-log ${drv}")
    else
      statuses+=("${name}: Failed, log: nix-store --read-log ${drv}")
    fi
  '';

  scriptMap = testlist: script: (script
    (map
      (test: ({
        name = test.name;
        drv = (builtins.unsafeDiscardStringContext (test.value {}).drvPath);
      }))
      testlist
    )
  );

  build-wrapper = tests: {
    all = scriptMap tests buildScript;
    tests = builtins.listToAttrs tests;
    raw = tests;
  };
}
