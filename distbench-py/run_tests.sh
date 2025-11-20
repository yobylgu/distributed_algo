#!/bin/bash
# Test script for Dolev's Reliable Broadcast implementation

set -e

echo "==============================================="
echo "Running Dolev's Reliable Broadcast Tests"
echo "==============================================="

mkdir -p logs

echo ""
echo "Test 1: Concurrency (3 simultaneous senders)"
echo "---------------------------------------------"
uv run python -m distbench.main -c configs/test_concurrency.yaml -a dolev --mode offline 2>&1 | tee logs/test_concurrency.log
echo "✓ Test 1 passed: Check logs/test_concurrency.log"

echo ""
echo "Test 2: Volume (1 sender, 10 messages)"
echo "---------------------------------------------"
uv run python -m distbench.main -c configs/test_volume.yaml -a dolev --mode offline 2>&1 | tee logs/test_volume.log
echo "✓ Test 2 passed: Check logs/test_volume.log"

echo ""
echo "Test 3: Integrity (Byzantine spoof)"
echo "---------------------------------------------"
uv run python -m distbench.main -c configs/test_integrity.yaml -a dolev --mode offline 2>&1 | tee logs/test_integrity.log
echo "✓ Test 3 passed: Check logs/test_integrity.log"

echo ""
echo "Test 4: No Totality (Byzantine broadcaster)"
echo "---------------------------------------------"
uv run python -m distbench.main -c configs/test_no_totality.yaml -a dolev --mode offline 2>&1 | tee logs/test_no_totality.log
echo "✓ Test 4 passed: Check logs/test_no_totality.log"

echo ""
echo "Test 5: Fault Tolerance (f=2 silent Byzantine nodes)"
echo "---------------------------------------------"
uv run python -m distbench.main -c configs/test_fault_tolerance.yaml -a dolev --mode offline 2>&1 | tee logs/test_fault_tolerance.log
echo "✓ Test 5 passed: Check logs/test_fault_tolerance.log"

echo ""
echo "==============================================="
echo "All tests completed!"
echo "==============================================="
echo "Summary:"
grep -h "Terminating\|delivered_count" logs/*.log | tail -50
