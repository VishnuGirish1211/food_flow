"""Integration test for the happy path saga.

Assumes the docker-compose environment is running on localhost.
"""

import asyncio
import uuid
import httpx
import pytest

@pytest.mark.asyncio
async def test_happy_path_saga(api_base):
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
        assert rest_resp.status_code == 200
        restaurants = rest_resp.json()
        assert len(restaurants) > 0
        restaurant_id = restaurants[0]["id"]

        menu_resp = await client.get(f"/restaurants/{restaurant_id}/menu")
        assert menu_resp.status_code == 200
        menu = menu_resp.json()
        assert len(menu) > 0
        item_id = menu[0]["id"]
        price = menu[0]["price"]

        # 3. Create order (force payment success just in case)
        order_req = {
            "restaurant_id": restaurant_id,
            "items": [{"item_id": item_id, "quantity": 1, "price": price}]
        }
        order_headers = {**headers, "X-Payment-Simulate": "success"}
        
        create_resp = await client.post("/orders", json=order_req, headers=order_headers)
        assert create_resp.status_code == 202
        order_id = create_resp.json()["id"]

        # 4. Poll for completion (Saga: PENDING -> CONFIRMED)
        max_attempts = 15
        for _ in range(max_attempts):
            await asyncio.sleep(1)
            get_resp = await client.get(f"/orders/{order_id}", headers=headers)
            status = get_resp.json()["status"]
            
            if status == "CONFIRMED":
                break
        else:
            pytest.fail(f"Order {order_id} did not reach CONFIRMED status within timeout.")

        assert status == "CONFIRMED"
