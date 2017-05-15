#!/bin/sh

(
    cd packages/jetpants
    nix-shell ./update.nix
)
