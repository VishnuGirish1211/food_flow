"""Integration test for the payment failure compensation flow.

Assumes the docker-compose environment is running on localhost.
"""

import asyncio
import uuid
import httpx
import pytest

@pytest.mark.asyncio
async def test_payment_failure_compensation(api_base):
    async with httpx.AsyncClient(base_url=api_base) as client:
        # 1. Register and Login
        email = f"test-{uuid.uuid4()}@example.com"
        await client.post("/auth/register", json={"email": email, "password": "password123"})
        
        login_resp = await client.post("/auth/login", json={"email": email, "password": "password123"})
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        # 2. Get a restaurant and menu item (from seed data)
        rest_resp = await client.get("/restaurants")
        restaurant_id = rest_resp.json()[0]["id"]

        menu_resp = await client.get(f"/restaurants/{restaurant_id}/menu")
        menu = menu_resp.json()
        item_id = menu[0]["id"]
        price = menu[0]["price"]
        initial_stock = menu[0]["stock_quantity"]

        # 3. Create order with X-Payment-Simulate: failure
        order_req = {
            "restaurant_id": restaurant_id,
            "items": [{"item_id": item_id, "quantity": 2, "price": price}]
        }
        order_headers = {**headers, "X-Payment-Simulate": "failure"}
        
        create_resp = await client.post("/orders", json=order_req, headers=order_headers)
        assert create_resp.status_code == 202
        order_id = create_resp.json()["id"]

        # 4. Poll for PAYMENT_FAILED status
        max_attempts = 15
        for _ in range(max_attempts):
            await asyncio.sleep(1)
            get_resp = await client.get(f"/orders/{order_id}", headers=headers)
            status = get_resp.json()["status"]
            
            if status == "PAYMENT_FAILED":
                break
        else:
            pytest.fail(f"Order {order_id} did not reach PAYMENT_FAILED status.")

        assert status == "PAYMENT_FAILED"

        # 5. Verify stock was restored (compensation)
        # Give compensation a little time to propagate back to restaurant service
        await asyncio.sleep(2)
        
        menu_resp2 = await client.get(f"/restaurants/{restaurant_id}/menu")
        for item in menu_resp2.json():
            if item["id"] == item_id:
                final_stock = item["stock_quantity"]
                break
                
        # The stock should have been temporarily reserved (-2) but then released (+2)
        assert final_stock == initial_stock
