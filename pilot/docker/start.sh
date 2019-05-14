#!/bin/sh
echo "starting proxy_debug"
chmod +777 /dev/stdout
/usr/local/bin/istio-iptables.sh -p 15001 -u 1337 #-m REDIRECT -i '*' -b "$INBOUND_PORTS"
iptables -t nat -I ISTIO_OUTPUT -d $HOST/32 -p tcp -m tcp --dport 5051 -j RETURN
su istio-proxy -c "/usr/local/bin/pilot-agent proxy --discoveryAddress $DISCOVERY_ADDRESSS  --domain $SERVICES_DOMAIN --serviceregistry Mesos --serviceCluster $SERVICE_NAME --zipkinAddress $ZIPKIN_ADDRESS --configPath /var/lib/istio"