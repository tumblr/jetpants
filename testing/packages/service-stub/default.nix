{ writeScriptBin }:
writeScriptBin "service" ''
#!/bin/sh

echo "SERVICE WAS CALLED!" | systemd-cat
echo "SERVICE WAS CALLED!" | systemd-cat
echo "SERVICE WAS CALLED!" | systemd-cat
echo "SERVICE WAS CALLED!" | systemd-cat
halt

''
