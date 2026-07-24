#!/usr/bin/env sh
# wait-for-it.sh version 2.2.0
# ... (simplified version for this project, often people use the standard script but for brevity I will create a basic wrapper) ...
# Actually, since docker-compose `depends_on: condition: service_healthy` is used, we don't strictly need wait-for-it.sh.
# I will create a dummy script or minimal check just in case it's invoked manually.
echo "Docker compose handles health checks and dependencies natively now."
exit 0
