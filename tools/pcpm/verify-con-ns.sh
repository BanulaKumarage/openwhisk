#!/bin/bash
# Usage: ./check_container_ns.sh <pause-container> <action-container>

PAUSE_CONTAINER=$1
ACTION_CONTAINER=$2

if [ -z "$PAUSE_CONTAINER" ] || [ -z "$ACTION_CONTAINER" ]; then
  echo "Usage: $0 <pause-container> <action-container>"
  exit 1
fi

# Get PIDs of both containers
PID_PAUSE=$(docker inspect -f '{{.State.Pid}}' "$PAUSE_CONTAINER")
PID_ACTION=$(docker inspect -f '{{.State.Pid}}' "$ACTION_CONTAINER")

if [ -z "$PID_PAUSE" ] || [ -z "$PID_ACTION" ]; then
  echo "Error: Could not get PID(s) of containers."
  exit 1
fi

# Get network namespace inodes
NET_PAUSE=$(readlink /proc/$PID_PAUSE/ns/net)
NET_ACTION=$(readlink /proc/$PID_ACTION/ns/net)

# Get IPC namespace inodes
IPC_PAUSE=$(readlink /proc/$PID_PAUSE/ns/ipc)
IPC_ACTION=$(readlink /proc/$PID_ACTION/ns/ipc)

echo "Network namespace:"
echo "  $PAUSE_CONTAINER: $NET_PAUSE"
echo "  $ACTION_CONTAINER: $NET_ACTION"

echo "IPC namespace:"
echo "  $PAUSE_CONTAINER: $IPC_PAUSE"
echo "  $ACTION_CONTAINER: $IPC_ACTION"

# Check if namespaces match
if [ "$NET_PAUSE" = "$NET_ACTION" ]; then
  echo "✅ Network namespaces are shared."
else
  echo "❌ Network namespaces are NOT shared."
fi

if [ "$IPC_PAUSE" = "$IPC_ACTION" ]; then
  echo "✅ IPC namespaces are shared."
else
  echo "❌ IPC namespaces are NOT shared."
fi
