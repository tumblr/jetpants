#!/bin/bash

# bash completion for jetpants
#
# Generates a cache file ${HOME}/.jetpants_completion_cache

_jetpants_complete () {
  local cache_file="${HOME}/.jetpants_completion_cache"
  local cur=${COMP_WORDS[COMP_CWORD]}
  local prev=${COMP_WORDS[COMP_CWORD-1]}

  COMPREPLY=()

  # try to expand anything but options (starting with -)
  if [[ $cur != -* ]]; then
    # refresh cache if it doesn't exist, is for the wrong path, or is too old
    if [[ ! -e $cache_file ]] || [ `find "$cache_file" -mmin +1440` ] ; then
      jetpants |grep jetpants  | awk -v ORS=' ' '{ print $2 }' >$cache_file
    fi
    COMPREPLY=($(compgen -W "$(tail -1 $cache_file)" -- $cur))
  fi
  return 0
}

complete -F _jetpants_complete jetpants