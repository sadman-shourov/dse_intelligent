#!/bin/bash
echo "=== ARIA Health Check ==="
echo ""
echo "PM2 Status:"
pm2 status aria-api

echo ""
echo "API Health:"
curl -s http://localhost:8000/health | python3 -m json.tool

echo ""
echo "Recent logs (last 20 lines):"
pm2 logs aria-api --lines 20 --nostream
