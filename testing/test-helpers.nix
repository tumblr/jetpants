{ pkgs }:
let
  inherit (pkgs) lib;
in
rec {
  verify-test-case = name: { test-phases, ... } @ args: {
    inherit name;
    value = (import ./test-harness.nix ( (removeAttrs args ["test-phases"]) // {
      testname = name;
      test-script = pkgs.writeScript "phases.${name}" ''
        #!${pkgs.bash}/bin/bash

        set -eux

        ${lib.concatMapStrings (p: "${p}\n") test-phases}
      '';
    }));
  };

  phase = name: script: let
    test = pkgs.writeScript "test.${name}" ''
      #!${pkgs.bash}/bin/bash
      set -eux
      ${script}
    '';

  in pkgs.writeScript "test.${name}.wrapper" ''
    #!${pkgs.bash}/bin/bash
    systemd-cat -t "${name}" ${test}
    RET=$?
    if [ $RET -eq 0 ]; then
      echo " OK: ${name}" | systemd-cat -t "test.${name}.wrapper"
    else
      echo " FAIL: ${name} (exit code: $RET)"  | systemd-cat -t "test.${name}.wrapper"
    fi
    exit $RET
  '';

  expect-phase = name: script: let
    test = pkgs.writeScript "test.${name}.expect" ''
      #!${pkgs.expect}/bin/expect -f

      ${script}
    '';
  in phase name ''
    export PAGER=cat

    ${test}
  '';

  jetpants-phase = name: code: (phase name (pkgs.jetpants.ruby_script name code));

  assert-shard-exists = name: jetpants-phase "assert-shard-exists-${name}" ''
    abort if Jetpants.pool('${name}').nil?
  '';

  assert-shard-does-not-exist = name: jetpants-phase "assert-shard-does-not-exist-${name}" ''
    abort unless Jetpants.pool('${name}').nil?
  '';

  assert-master-has-n-slaves = shard: slaves: jetpants-phase "assert-${shard}-has-${toString slaves}-slaves" ''
    abort "Actual: #{Jetpants.pool('${shard}').slaves.length}" unless Jetpants.pool('${shard}').slaves.length == ${toString slaves}
  '';

  assert-shard-master = shard: master: jetpants-phase "assert-${shard}-master" ''
    abort "Actual master: #{Jetpants.pool('${shard}').master}" unless Jetpants.pool('${shard}').master == '${toString master}'.to_db
  '';

  assert-shard-slave = shard: slave: jetpants-phase "assert-${shard}-slave" ''
    abort "Slave not found." unless Jetpants.pool('${shard}').master == '${toString slave}'.to_db.master
  '';
}
