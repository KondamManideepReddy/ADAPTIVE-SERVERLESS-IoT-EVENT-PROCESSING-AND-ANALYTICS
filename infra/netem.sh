#!/bin/sh
# Network fault injection for the REAL MQTT path (Mosquitto in docker-compose). Needs NET_ADMIN.
# Usage:  netem.sh <iface> delay|loss|outage|clear
#   delay  : 80 ms +/- 30 ms
#   loss   : 5% packet loss + 40 ms delay
#   outage : 100% loss for 5 s, then restore (uplink disconnection)
IF=${1:-eth0}
case "$2" in
  delay)  tc qdisc replace dev "$IF" root netem delay 80ms 30ms distribution normal ;;
  loss)   tc qdisc replace dev "$IF" root netem delay 40ms loss 5% ;;
  outage) tc qdisc replace dev "$IF" root netem loss 100%; sleep 5; tc qdisc del dev "$IF" root ;;
  clear)  tc qdisc del dev "$IF" root 2>/dev/null || true ;;
  *) echo "usage: $0 <iface> delay|loss|outage|clear"; exit 1 ;;
esac
