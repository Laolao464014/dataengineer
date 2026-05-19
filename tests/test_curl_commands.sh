#!/bin/bash

# API Test Commands for IoT Sensor Platform
# Run these commands to test all endpoints

BASE_URL="http://localhost:5000/api/v1"

echo "=========================================="
echo "Testing IoT Sensor Platform API"
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test 1: Health Check
echo -e "\n${GREEN}1. Testing Health Check${NC}"
curl -s $BASE_URL/health | python3 -m json.tool
echo ""

# Test 2: List Sensors
echo -e "\n${GREEN}2. Testing List Sensors${NC}"
curl -s $BASE_URL/sensors | python3 -m json.tool
echo ""

# Test 3: Get Latest Reading (Valid)
echo -e "\n${GREEN}3. Testing Get Latest Reading (Valid sensor)${NC}"
curl -s $BASE_URL/sensors/temperature/latest | python3 -m json.tool
echo ""

# Test 4: Get Latest Reading (Invalid)
echo -e "\n${GREEN}4. Testing Get Latest Reading (Invalid sensor)${NC}"
curl -s $BASE_URL/sensors/invalid_type/latest | python3 -m json.tool
echo ""

# Test 5: Get Sensor Stats
echo -e "\n${GREEN}5. Testing Get Sensor Stats${NC}"
curl -s "$BASE_URL/sensors/temperature/stats?days=7" | python3 -m json.tool
echo ""

# Test 6: Get Sensor Stats (Invalid days)
echo -e "\n${GREEN}6. Testing Get Sensor Stats (Invalid days)${NC}"
curl -s "$BASE_URL/sensors/temperature/stats?days=100" | python3 -m json.tool
echo ""

# Test 7: Get Anomalies
echo -e "\n${GREEN}7. Testing Get Anomalies${NC}"
curl -s "$BASE_URL/anomalies?limit=10" | python3 -m json.tool
echo ""

# Test 8: Get Anomalies (Filtered by sensor)
echo -e "\n${GREEN}8. Testing Get Anomalies (Filtered by sensor)${NC}"
curl -s "$BASE_URL/anomalies?sensor=temperature&limit=5" | python3 -m json.tool
echo ""

# Test 9: Publish Reading (Valid)
echo -e "\n${GREEN}9. Testing Publish Reading (Valid)${NC}"
curl -s -X POST $BASE_URL/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor": "temperature", "value": 25.5, "unit": "C", "source": "test-script"}' \
  | python3 -m json.tool
echo ""

# Test 10: Publish Reading (Invalid - missing fields)
echo -e "\n${GREEN}10. Testing Publish Reading (Invalid - missing fields)${NC}"
curl -s -X POST $BASE_URL/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor": "temperature", "value": 25.5}' \
  | python3 -m json.tool
echo ""

# Test 11: Publish Reading (Invalid - out of range)
echo -e "\n${GREEN}11. Testing Publish Reading (Invalid - out of range)${NC}"
curl -s -X POST $BASE_URL/readings \
  -H "Content-Type: application/json" \
  -d '{"sensor": "temperature", "value": 100, "unit": "C", "source": "test-script"}' \
  | python3 -m json.tool
echo ""

# Test 12: 404 Not Found
echo -e "\n${GREEN}12. Testing 404 Not Found${NC}"
curl -s $BASE_URL/nonexistent | python3 -m json.tool
echo ""

echo -e "\n${GREEN}=========================================="
echo "API Tests Complete"
echo "=========================================="