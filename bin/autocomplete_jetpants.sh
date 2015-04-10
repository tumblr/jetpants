_jetpantscomplete() {
        COMPREPLY=($(compgen -W "`jetpants |grep jetpants  | awk -v ORS=' ' '{ print $2 }'`" -- ${COMP_WORDS[COMP_CWORD]}))
        return 0
}
complete -o default -o nospace -F _jetpantscomplete jetpants
