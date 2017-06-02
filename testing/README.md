# Jetpants Integration Testing

Note some of the code samples have erroneous syntax highlighting on
GitHub. I'm trying to fix that with GitHub.

## About NixOS's test framework
The integration tests are run using NixOS's test framework. The
framework is based around defining a machine's configuration, then
running commands inside the machine's virtual machine.

These tests can do complicated things:

 - Run more than one VM at a time, and setup network access between
   the two
 - Plug and unplug the network interfaces
 - Cause VMs to hard-crash
 - Don't require full provisioning time each build... this one
   deserves to have more said about it.

When Nix creates the test VM, it is fully provisioned. There is no
internal step of running Puppet to place configuration files, or
reinstalling software. This reduces build time and setup time for each
test dramatically, and was key for the initial success of the testing
project.

NixOS tests are also completely isolated from each other, only having
access to what is in the VM based on its configuration. There is no
external network access, and no carry-over from other tests. When
re-running tests, there is no leftover state from a previous test.
Each run is hermetically sealed.

The point about external network access is a bit double sided:

 1. NixOS tests are guaranteed to not be able to harm its external
    environment. This was important to me, as the tests create and
    destroy many resources in Collins.
 2. NixOS tests must start with everything they need to run, already
    packaged inside of them. This is actually a good thing, but may
    take a novice user by surprise the first few times.

For some examples of NixOS tests being used by NixOS, take a look at:
[nixos/tests](https://github.com/NixOS/nixpkgs/tree/master/nixos/tests). For
documentation about what NixOS tests can do, take a look at
[NixOS Manual: Writing NixOS Tests](https://nixos.org/nixos/manual/index.html#sec-writing-nixos-tests) and
[NixOS Manual: NixOS Tests](https://nixos.org/nixos/manual/index.html#sec-nixos-tests).

## Writing Tests

All of our tests start out the same way:

1. Spawn 2 or more MySQL servers
2. Add them to Collins
3. Initialize the first pool in Collins with Jetpants

After this, we want to run some jetpants commands and assertions about
what happened.

To Accomplish this easily, I've written some test helpers for this
task. These are located in `test-helpers.nix`. The tests themselves are
in`tests/`.

### Walking through a very simple test: `verify-default-environment`

The tests live in `tests/`. Let's start by looking at a very simple test,
`./tests/verify-default-environment.nix`.

     1	import ../make-test.nix  ({ helpers, ... }:
     2	{
     3	  name = "verify-default-environment";
     4
     5	  starting-spare-dbs = 0;
     6
     7	  test-phases = with helpers; [
     8	    (assert-shard-exists "POSTS-1-INFINITY")
     9	    (assert-master-has-n-slaves "POSTS-1-INFINITY" 1)
    10	  ];
    11	})


Line one is importing a file, `../make-test.nix`, and passing a function to `make-test.nix`. The
function starts at the `({ helpers, ...}` and ends at the `})` on line 11. The function returns an
"Attrset", which decribes the test. An Attrset is like a dictionary, associative array, or HashMap
in other languages.

`make-test.nix` will handle configuring the VM do the initial setup
we wanted, plus add the extra spare servers and our test phases. These
are defined in the test definition.

The test definition contains two names and values:

 - `starting-spare-dbs` (line 2) Our VM requires a minimum of 2
   starting DB servers, to create a `POSTS-1-INFINITY` pool shard with
   a master and a slave. If your test creates more pools or slaves,
   you can calculate how many you need and add them here.
 - `test-phases` (line 4) is a list of steps and assertions to make,
   starting at the `[` on line 4 and ending with the `];` on line 7.

The goal of this test is to verify that the base machinery of the test
infrastructure is working correctly. The basic promises is that before
your test starts, there is already:

 - a shard named `POSTS-1-INFINITY`, expressed with the
   `assert-shard-exists` function, which accepts a shard name as its
   only parameter (line 5)
 - the shard `POSTS-1-INFINITY` has a exactly one slave, tested with
   the `assert-master-has-n-slaves` function (line 6). This function
   accepts two arguments: a shard-pool name, and the number of slaves
   to expect. This function looks up the shard, finds the master, and
   compares the expected count to the actual count.

You can run this test via `nix-build ./tests/verify-default-environment.nix`.

### A slightly more complex test: `simple-shard-cutover`

This test is very small extension from the verify-basic-environment
test. At first look, you'll see we're starting with 2 extra spare
databases. These DBs will be loaded in to collins as
`allocated:spare`, and ready for jetpants to claim.

In the `test-phases` list we see a new phase, simply named `phase`.
This is the simplest phase of all: running a shell script. It takes
two parameters:
 - a name, to identify what phase passed or failed. In this case, the
   phase is named `shard-cutover`.
 - a shell script to run. This phase expects that if any command
   inside fails, the phase has failed and the test will be aborted.
   ie: it is run with `set -eu`.

In our `shard-cutover` phase, we see a new syntax: the `''` quotes.
`''` makes a multi-line string which ends at the next `''`. We run
`jetpants shard-cutover --cutover-id=10000 --shard-pool=posts`. After
that finishes, we make assertions similar to the
`verify-default-environment`.

You can run this test via `nix-build ./tests/simple-shard-cutover.nix`.

## Other Test Phase Helpers

#### A quick note on nix functions

These tests will be described, sometimes, using the functions used to
make them. Here are some example functions to understand their syntax.

A function named `sum` which adds two numbers:

```nix
sum = x: y: x + y;
```

`sum` takes two parameters, `x` and `y`, then returns the sum of `x`
and `y`, and it can be called like `sum 1 2`. `sum 1 2 == 3`. The
separator of the arguments is done at the colon.

```nix
sum = x: y: x + y;
sum-3 = x: y: z: sum x (sum y z);
```

Here we make a new function `sum-3` which accepts 3 numbers to sum
together:

 - `sum-3 = x: y: z: ` define `sum-3` as a 3-parameter function
 - `sum x (sum y z)` first add y and z together, then add that value
    with x.

### `jetpants-phase`

Accepts a name and a ruby script to run with jetpants. An example is
the implementation of `assert-master-has-n-slaves`:

```nix
assert-master-has-n-slaves = shard: slaves: jetpants-phase "assert-${shard}-has-${toString slaves}-slaves" ''
    abort "Actual: #{Jetpants.pool('${shard}').slaves.length}" unless Jetpants.pool('${shard}').slaves.length == ${toString slaves}
'';
```

We're defining the function to accept two parameters, `shard` and
`slaves`. It then calls the function `jetpants-phase` (remember that
one?) and passes it:

 - the name: `assert-${shard}-has-${toString slaves}-slaves` where
   `${shard}` is replaced with the name of the shard passed in, and
   `${toString slaves}` is substituted with the variable `slaves`
   after converting the variable from an integer to a string.
 - the script:

```
abort "Actual: #{Jetpants.pool('${shard}').slaves.length}" unless Jetpants.pool('${shard}').slaves.length == ${toString slaves}
 ```

where the same subtitutions happen in the name.

If we wrote:

```nix
(assert-shard-has-n-slaves "POSTS-1-INFINITY" 1)
```

it would be identical to if we had written:

```nix
(jetpants-phase "assert-POSTS-1-INFINITY-has-1-slaves" ''
    abort "Actual: #{Jetpants.pool('POSTS-1-INFINITY').slaves.length}" unless Jetpants.pool('POSTS-1-INFINITY').slaves.length == 1
'')
```

### `assert-shard-does-not-exist`

Accepts a name of a shard, and confirms it does not exist. Example:

```nix
(assert-shard-does-not-exist "BOGUS-100-200")
```

### `expect-phase`

This one is a bit wild, but basically you run an expect script from
within the test phase.

Explaining expect is out of scope for this document, but you'd use it
like this:

```nix
(expect-phase "my-expect-phase" ''
  spawn foo
  expect "hello"
  send "YES\n"
'')
```

## Running an arbitrary test

1. Run all tests: `nix-build ./all-tests.nix`
2. Run a particular test: `nix-build ./tests/verify-default-environment.nix`

## Entering a test VM

This is a bit of a WIP, but to get in to a test's VM:

```bash
rm -rf vde1.ctl/ /tmp/vm-state-machine/
nix-build /var/www/apps/jetpants/testing/tests/verify-default-environment.nix -A driver
QEMU_NET_OPTS="hostfwd=tcp::2223-:22" tests='startAll; joinAll;' ./result/bin/nixos-run-vms
```

then in another terminal session:

```
ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null root@127.0.0.1 -p 2223
```

with the password `foobar`. This will put you in the
`verify-default-environment` test's VM. You can select any test, and
enter its VM.
