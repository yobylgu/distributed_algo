#!/bin/bash
# Script to run Dolev's algorithm in Docker containers

echo "Building Docker images..."
docker compose build

echo ""
echo "Starting distributed test across Docker containers..."
docker compose up

echo ""
echo "Cleaning up containers..."
docker compose down

echo ""
echo "Test complete!"
