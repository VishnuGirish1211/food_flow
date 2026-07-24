-- Seed data for restaurant_db.
-- Run after the restaurant service has created its tables.
-- Uses fixed UUIDs so tests and curl examples are reproducible.

\connect restaurant_db;

INSERT INTO restaurants (id, name, description) VALUES
    ('a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Burger Palace', 'Classic American burgers and fries'),
    ('b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Pizza Planet', 'Wood-fired Neapolitan pizzas')
ON CONFLICT (id) DO NOTHING;

INSERT INTO menu_items (id, restaurant_id, name, description, price) VALUES
    ('11111111-1111-1111-1111-111111111111', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Classic Burger', 'Beef patty with lettuce, tomato, and cheese', 9.99),
    ('22222222-2222-2222-2222-222222222222', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Fries', 'Crispy golden fries', 4.99),
    ('33333333-3333-3333-3333-333333333333', 'a1b2c3d4-e5f6-7890-abcd-ef1234567890', 'Milkshake', 'Vanilla milkshake', 5.99),
    ('44444444-4444-4444-4444-444444444444', 'b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Margherita Pizza', 'Fresh mozzarella, basil, and tomato sauce', 12.99),
    ('55555555-5555-5555-5555-555555555555', 'b2c3d4e5-f6a7-8901-bcde-f12345678901', 'Pepperoni Pizza', 'Classic pepperoni with mozzarella', 14.99)
ON CONFLICT (id) DO NOTHING;

INSERT INTO menu_item_stock (item_id, quantity) VALUES
    ('11111111-1111-1111-1111-111111111111', 100),
    ('22222222-2222-2222-2222-222222222222', 200),
    ('33333333-3333-3333-3333-333333333333', 50),
    ('44444444-4444-4444-4444-444444444444', 80),
    ('55555555-5555-5555-5555-555555555555', 80)
ON CONFLICT (item_id) DO NOTHING;
