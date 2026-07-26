import React, { useState, useEffect } from 'react';
import { useAuth } from '../context/AuthContext';
import { useNavigate } from 'react-router-dom';
import { LogOut, LayoutDashboard, UtensilsCrossed, Receipt, ChevronRight, Activity, X, ShoppingCart, Plus, Minus } from 'lucide-react';

export default function Dashboard() {
  const { user, logout, authenticatedFetch } = useAuth();
  const navigate = useNavigate();

  const [restaurants, setRestaurants] = useState([]);
  const [orders, setOrders] = useState([]);
  const [loading, setLoading] = useState(true);

  // Menu & Cart Modal State
  const [selectedRestaurant, setSelectedRestaurant] = useState(null);
  const [menuItems, setMenuItems] = useState([]);
  const [loadingMenu, setLoadingMenu] = useState(false);
  const [cart, setCart] = useState([]);
  const [isCartOpen, setIsCartOpen] = useState(false);
  const [placingOrder, setPlacingOrder] = useState(false);
  const [cartIdempotencyKey, setCartIdempotencyKey] = useState(null);

  useEffect(() => {
    if (cart.length > 0 && !cartIdempotencyKey) {
      setCartIdempotencyKey(crypto.randomUUID());
    } else if (cart.length === 0 && cartIdempotencyKey) {
      setCartIdempotencyKey(null);
    }
  }, [cart.length, cartIdempotencyKey]);

  // Receipt Modal State
  const [receiptOrder, setReceiptOrder] = useState(null);

  const fetchOrders = async () => {
    try {
      const storedOrders = JSON.parse(localStorage.getItem(`orders_${user?.email}`) || '[]');
      if (storedOrders.length === 0) {
        setOrders([]);
        return;
      }

      const orderPromises = storedOrders.map(id => authenticatedFetch(`/orders/${id}`));
      const responses = await Promise.all(orderPromises);
      
      const fetchedOrders = [];
      for (const res of responses) {
        if (res.ok) {
          fetchedOrders.push(await res.json());
        }
      }
      setOrders(fetchedOrders);
    } catch (err) {
      console.error("Failed to fetch orders:", err);
    }
  };

  useEffect(() => {
    if (!user) {
      navigate('/login');
      return;
    }

    const fetchData = async () => {
      try {
        const [restRes] = await Promise.all([
          authenticatedFetch('/restaurants')
        ]);
        
        if (restRes.ok) setRestaurants(await restRes.json());
        
        // Fetch orders using the new local storage method
        await fetchOrders();
      } catch (err) {
        console.error("Failed to fetch dashboard data:", err);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
    
    // Auto-refresh orders every 3 seconds
    const interval = setInterval(fetchOrders, 3000);
    return () => clearInterval(interval);
  }, [user, navigate, authenticatedFetch]);

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  const openMenu = async (restaurant) => {
    // If switching restaurants, clear the cart
    if (cart.length > 0 && cart[0].restaurant_id !== restaurant.id) {
      if (window.confirm("Switching restaurants will clear your current cart. Continue?")) {
        setCart([]);
      } else {
        return;
      }
    }
    
    setSelectedRestaurant(restaurant);
    setLoadingMenu(true);
    setMenuItems([]);
    try {
      const res = await authenticatedFetch(`/restaurants/${restaurant.id}/menu`);
      if (res.ok) {
        setMenuItems(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch menu:", err);
    } finally {
      setLoadingMenu(false);
    }
  };

  const addToCart = (item, restaurant_id) => {
    setCart(prev => {
      const existing = prev.find(i => i.item_id === item.id);
      if (existing) {
        if (existing.quantity >= item.stock_quantity) return prev; // Prevent adding more than stock
        return prev.map(i => i.item_id === item.id ? { ...i, quantity: i.quantity + 1 } : i);
      }
      return [...prev, { item_id: item.id, name: item.name, price: item.price, quantity: 1, restaurant_id }];
    });
  };

  const removeFromCart = (itemId) => {
    setCart(prev => {
      const existing = prev.find(i => i.item_id === itemId);
      if (existing.quantity === 1) {
        return prev.filter(i => i.item_id !== itemId);
      }
      return prev.map(i => i.item_id === itemId ? { ...i, quantity: i.quantity - 1 } : i);
    });
  };

  const cartTotal = cart.reduce((sum, item) => sum + (item.price * item.quantity), 0);

  const placeOrder = async () => {
    if (cart.length === 0 || !cartIdempotencyKey) return;
    setPlacingOrder(true);
    try {
      const res = await authenticatedFetch('/orders', {
        method: 'POST',
        headers: { 
          'Content-Type': 'application/json',
          'Idempotency-Key': cartIdempotencyKey
        },
        body: JSON.stringify({
          restaurant_id: cart[0].restaurant_id,
          items: cart.map(i => ({ item_id: i.item_id, quantity: i.quantity, price: i.price }))
        })
      });
      if (res.ok) {
        const data = await res.json();
        // Save the new order ID to local storage so we can track it
        const storedOrders = JSON.parse(localStorage.getItem(`orders_${user?.email}`) || '[]');
        storedOrders.push(data.id);
        localStorage.setItem(`orders_${user?.email}`, JSON.stringify(storedOrders));

        await fetchOrders();
        setCart([]);
        setIsCartOpen(false);
        setSelectedRestaurant(null);
      } else {
        alert("Failed to place order.");
      }
    } catch (err) {
      console.error("Order failed:", err);
    } finally {
      setPlacingOrder(false);
    }
  };

  const openReceipt = async (orderId) => {
    try {
      const res = await authenticatedFetch(`/orders/${orderId}`);
      if (res.ok) {
        setReceiptOrder(await res.json());
      }
    } catch (err) {
      console.error("Failed to fetch receipt:", err);
    }
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'CONFIRMED': return 'var(--accent-green)';
      case 'PENDING': return 'var(--accent-orange)';
      case 'PAYMENT_FAILED': return 'var(--accent-red)';
      case 'STOCK_UNAVAILABLE': return 'var(--accent-red)';
      default: return 'var(--text-muted)';
    }
  };

  return (
    <div style={{ display: 'flex', minHeight: '100vh', backgroundColor: 'var(--bg-dark)' }}>
      {/* Sidebar */}
      <aside style={{ width: '250px', backgroundColor: 'var(--bg-panel)', borderRight: '1px solid var(--border-color)', padding: '1.5rem', display: 'flex', flexDirection: 'column' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '2.5rem' }}>
          <Activity color="var(--accent-purple)" size={28} />
          <h1 style={{ fontSize: '1.25rem', fontWeight: 700, letterSpacing: '0.5px' }}>FoodFlow</h1>
        </div>

        <nav style={{ flex: 1, display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.75rem 1rem', backgroundColor: 'var(--bg-hover)', borderRadius: '6px', color: 'var(--text-main)', fontWeight: 500 }}>
            <LayoutDashboard size={20} /> Dashboard
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.75rem 1rem', color: 'var(--text-muted)', cursor: 'not-allowed' }}>
            <UtensilsCrossed size={20} /> Menus (Coming Soon)
          </div>
        </nav>

        <button onClick={handleLogout} style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', padding: '0.75rem 1rem', color: 'var(--text-muted)', transition: 'color 0.2s', marginTop: 'auto' }} className="glow-hover">
          <LogOut size={20} /> Logout
        </button>
      </aside>

      {/* Main Content */}
      <main style={{ flex: 1, display: 'flex', flexDirection: 'column', position: 'relative' }}>
        {/* Top Nav */}
        <header style={{ height: '70px', borderBottom: '1px solid var(--border-color)', display: 'flex', alignItems: 'center', padding: '0 2rem', justifyContent: 'flex-end' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '2rem' }}>
            
            {/* Cart Icon in Header */}
            <button onClick={() => setIsCartOpen(true)} style={{ position: 'relative', display: 'flex', alignItems: 'center', color: 'var(--text-main)' }}>
              <ShoppingCart size={24} />
              {cart.length > 0 && (
                <div style={{ position: 'absolute', top: '-8px', right: '-8px', backgroundColor: 'var(--accent-red)', color: 'white', fontSize: '0.75rem', width: '20px', height: '20px', borderRadius: '50%', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 'bold' }}>
                  {cart.reduce((sum, item) => sum + item.quantity, 0)}
                </div>
              )}
            </button>

            <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', borderLeft: '1px solid var(--border-color)', paddingLeft: '2rem' }}>
              <span style={{ color: 'var(--text-muted)', fontSize: '0.9rem' }}>{user?.email}</span>
              <div style={{ width: '36px', height: '36px', borderRadius: '50%', backgroundColor: 'var(--accent-purple)', display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 600 }}>
                {user?.email?.charAt(0).toUpperCase()}
              </div>
            </div>
          </div>
        </header>

        {/* Dashboard Content */}
        <div style={{ padding: '2rem', maxWidth: '1200px', margin: '0 auto', width: '100%' }}>
          
          <div style={{ marginBottom: '3rem' }}>
            <h2 style={{ fontSize: '1.5rem', fontWeight: 600, marginBottom: '1.5rem' }}>Available Restaurants</h2>
            {loading ? (
              <p style={{ color: 'var(--text-muted)' }}>Loading...</p>
            ) : (
              <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '1.5rem' }}>
                {restaurants.map(rest => (
                  <div key={rest.id} className="glass-panel glow-hover" style={{ padding: '1.5rem', display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                    <h3 style={{ fontSize: '1.1rem', fontWeight: 600, color: 'var(--accent-orange)' }}>{rest.name}</h3>
                    <p style={{ color: 'var(--text-muted)', fontSize: '0.9rem', lineHeight: '1.4' }}>{rest.description}</p>
                    <div style={{ marginTop: 'auto', display: 'flex', justifyContent: 'space-between', alignItems: 'center', paddingTop: '1rem', borderTop: '1px solid var(--border-color)' }}>
                      <span style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>ID: {rest.id.substring(0,8)}...</span>
                      <button className="btn-secondary" onClick={() => openMenu(rest)} style={{ padding: '0.4rem 0.8rem', fontSize: '0.85rem' }}>View Menu</button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div>
            <h2 style={{ fontSize: '1.5rem', fontWeight: 600, marginBottom: '1.5rem' }}>Recent Orders Saga Status</h2>
            {loading ? (
              <p style={{ color: 'var(--text-muted)' }}>Loading...</p>
            ) : orders.length === 0 ? (
              <div className="glass-panel" style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-muted)' }}>
                No orders found. Click "View Menu" to place an order and see the saga flow.
              </div>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {[...orders].reverse().map(order => (
                  <div 
                    key={order.id} 
                    className="glass-panel glow-hover" 
                    onClick={() => openReceipt(order.id)}
                    style={{ padding: '1.25rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', cursor: 'pointer' }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1.5rem' }}>
                      <Receipt color="var(--text-muted)" size={24} />
                      <div>
                        <div style={{ fontWeight: 500, marginBottom: '0.25rem' }}>Order {order.id.substring(0,8)}</div>
                        <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>${(order.total_amount / 100).toFixed(2)}</div>
                      </div>
                    </div>
                    
                    <div style={{ display: 'flex', alignItems: 'center', gap: '2rem' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
                        <div style={{ width: '8px', height: '8px', borderRadius: '50%', backgroundColor: getStatusColor(order.status) }}></div>
                        <span style={{ fontSize: '0.9rem', fontWeight: 500, color: getStatusColor(order.status) }}>
                          {order.status}
                        </span>
                      </div>
                      <ChevronRight color="var(--text-muted)" size={20} />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

        </div>

        {/* Menu Modal */}
        {selectedRestaurant && (
          <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.7)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 40 }}>
            <div className="glass-panel" style={{ width: '100%', maxWidth: '600px', maxHeight: '80vh', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
              <div style={{ padding: '1.5rem', borderBottom: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h2 style={{ fontSize: '1.25rem', fontWeight: 600 }}>{selectedRestaurant.name} Menu</h2>
                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                  <button onClick={() => setIsCartOpen(true)} className="btn-primary" style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', padding: '0.5rem 1rem' }}>
                    <ShoppingCart size={16} /> View Cart ({cart.reduce((sum, item) => sum + item.quantity, 0)})
                  </button>
                  <button onClick={() => setSelectedRestaurant(null)} style={{ color: 'var(--text-muted)' }}><X size={24} /></button>
                </div>
              </div>
              
              <div style={{ padding: '1.5rem', overflowY: 'auto' }}>
                {loadingMenu ? (
                  <p style={{ textAlign: 'center', color: 'var(--text-muted)' }}>Loading menu...</p>
                ) : menuItems.length === 0 ? (
                  <p style={{ textAlign: 'center', color: 'var(--text-muted)' }}>No items available.</p>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                    {menuItems.map(item => {
                      const cartItem = cart.find(i => i.item_id === item.id);
                      return (
                        <div key={item.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '1rem', backgroundColor: 'var(--bg-dark)', borderRadius: '6px', border: '1px solid var(--border-color)' }}>
                          <div>
                            <div style={{ fontWeight: 600, color: 'var(--text-main)', marginBottom: '0.25rem' }}>{item.name}</div>
                            <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>{item.description}</div>
                            <div style={{ fontSize: '0.85rem', color: 'var(--accent-orange)', marginTop: '0.5rem', fontWeight: 500 }}>
                              ${(item.price / 100).toFixed(2)} • Stock: {item.stock_quantity}
                            </div>
                          </div>
                          
                          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                            {cartItem ? (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', backgroundColor: 'var(--bg-card)', padding: '0.25rem', borderRadius: '4px', border: '1px solid var(--border-color)' }}>
                                <button onClick={() => removeFromCart(item.id)} style={{ color: 'var(--text-muted)' }}><Minus size={16} /></button>
                                <span style={{ width: '20px', textAlign: 'center', fontSize: '0.9rem' }}>{cartItem.quantity}</span>
                                <button 
                                  onClick={() => addToCart(item, selectedRestaurant.id)} 
                                  style={{ color: cartItem.quantity >= item.stock_quantity ? 'var(--text-muted)' : 'var(--accent-purple)' }}
                                  disabled={cartItem.quantity >= item.stock_quantity}
                                >
                                  <Plus size={16} />
                                </button>
                              </div>
                            ) : (
                              <button 
                                onClick={() => addToCart(item, selectedRestaurant.id)} 
                                className="btn-secondary"
                                disabled={item.stock_quantity <= 0}
                                style={{ padding: '0.5rem 1rem', fontSize: '0.85rem', opacity: item.stock_quantity <= 0 ? 0.5 : 1 }}
                              >
                                {item.stock_quantity <= 0 ? 'Out of Stock' : 'Add to Cart'}
                              </button>
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Cart/Checkout Modal */}
        {isCartOpen && (
          <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.8)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 50 }}>
            <div className="glass-panel" style={{ width: '100%', maxWidth: '400px', display: 'flex', flexDirection: 'column' }}>
              <div style={{ padding: '1.5rem', borderBottom: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h2 style={{ fontSize: '1.25rem', fontWeight: 600 }}>Your Cart</h2>
                <button onClick={() => setIsCartOpen(false)} style={{ color: 'var(--text-muted)' }}><X size={24} /></button>
              </div>
              
              <div style={{ padding: '1.5rem', overflowY: 'auto', maxHeight: '400px' }}>
                {cart.length === 0 ? (
                  <p style={{ textAlign: 'center', color: 'var(--text-muted)' }}>Cart is empty.</p>
                ) : (
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                    {cart.map(item => (
                      <div key={item.item_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div>
                          <div style={{ fontWeight: 500 }}>{item.name}</div>
                          <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>${(item.price / 100).toFixed(2)} x {item.quantity}</div>
                        </div>
                        <div style={{ fontWeight: 600 }}>
                          ${((item.price * item.quantity) / 100).toFixed(2)}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
              
              {cart.length > 0 && (
                <div style={{ padding: '1.5rem', borderTop: '1px solid var(--border-color)', backgroundColor: 'var(--bg-dark)' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem', fontSize: '1.1rem', fontWeight: 600 }}>
                    <span>Total</span>
                    <span style={{ color: 'var(--accent-green)' }}>${(cartTotal / 100).toFixed(2)}</span>
                  </div>
                  <button 
                    onClick={placeOrder} 
                    className="btn-primary" 
                    style={{ width: '100%' }}
                    disabled={placingOrder}
                  >
                    {placingOrder ? 'Processing...' : 'Place Order'}
                  </button>
                </div>
              )}
            </div>
          </div>
        )}

        {/* Detailed Receipt Modal */}
        {receiptOrder && (
          <div style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, backgroundColor: 'rgba(0,0,0,0.8)', display: 'flex', alignItems: 'center', justifyContent: 'center', zIndex: 60 }}>
            <div className="glass-panel" style={{ width: '100%', maxWidth: '450px', display: 'flex', flexDirection: 'column' }}>
              <div style={{ padding: '1.5rem', borderBottom: '1px solid var(--border-color)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <h2 style={{ fontSize: '1.25rem', fontWeight: 600 }}>Order Receipt</h2>
                <button onClick={() => setReceiptOrder(null)} style={{ color: 'var(--text-muted)' }}><X size={24} /></button>
              </div>
              
              <div style={{ padding: '2rem', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
                
                <div style={{ textAlign: 'center' }}>
                  <div style={{ fontSize: '0.85rem', color: 'var(--text-muted)' }}>Order ID</div>
                  <div style={{ fontFamily: 'monospace', marginTop: '0.25rem' }}>{receiptOrder.id}</div>
                </div>

                <div style={{ display: 'flex', justifyContent: 'center' }}>
                  <div style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem', backgroundColor: 'var(--bg-dark)', padding: '0.5rem 1rem', borderRadius: '20px', border: '1px solid var(--border-color)' }}>
                    <div style={{ width: '10px', height: '10px', borderRadius: '50%', backgroundColor: getStatusColor(receiptOrder.status) }}></div>
                    <span style={{ fontWeight: 600, color: getStatusColor(receiptOrder.status) }}>{receiptOrder.status}</span>
                  </div>
                </div>

                <div style={{ borderTop: '1px dashed var(--border-color)', paddingTop: '1.5rem' }}>
                  <div style={{ marginBottom: '1rem', fontWeight: 600, color: 'var(--text-muted)', fontSize: '0.9rem' }}>ORDER ITEMS</div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                    {receiptOrder.items.map((item, idx) => (
                      <div key={idx} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '0.95rem' }}>
                        <span>Item {item.item_id.substring(0,6)}... x {item.quantity}</span>
                        <span>${((item.price * item.quantity) / 100).toFixed(2)}</span>
                      </div>
                    ))}
                  </div>
                </div>

                <div style={{ borderTop: '1px solid var(--border-color)', paddingTop: '1.5rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '1.2rem', fontWeight: 700 }}>
                  <span>Total Amount</span>
                  <span style={{ color: 'var(--accent-green)' }}>${(receiptOrder.total_amount / 100).toFixed(2)}</span>
                </div>

                <div style={{ textAlign: 'center', fontSize: '0.8rem', color: 'var(--text-muted)', marginTop: '1rem' }}>
                  Placed on: {new Date(receiptOrder.created_at).toLocaleString()}
                </div>
              </div>
            </div>
          </div>
        )}

      </main>
    </div>
  );
}
