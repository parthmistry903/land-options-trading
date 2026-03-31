from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
from datetime import datetime, date
from valuation import get_land_price_analytics
from db import execute_query, execute_transaction
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import uuid
import os

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "supersecretkeyforflashmessages")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login' 
login_manager.login_message_category = "warning"

class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data['user_id']
        self.username = user_data['username']
        self.full_name = user_data['full_name']
        self.balance_cash = user_data['balance_cash']
        self.role = user_data.get('role', 'user') 

@login_manager.user_loader
def load_user(user_id):
    user_data = execute_query("SELECT * FROM Users WHERE user_id = %s", (user_id,), fetch_all=False)
    if user_data:
        return User(user_data)
    return None

@app.route('/register', methods=['GET', 'POST'])
def register():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form['username']
        full_name = request.form['full_name']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form.get('confirm_password')
        if password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for('register'))
        existing_user = execute_query("SELECT * FROM Users WHERE username = %s OR email = %s", (username, email), fetch_all=False)
        if existing_user:
            flash("Username or Email already exists.", "danger")
            return redirect(url_for('register'))
        new_user_id = f"U{uuid.uuid4().hex[:6].upper()}"
        hashed_pw = generate_password_hash(password)
        sql = "INSERT INTO Users (user_id, username, full_name, email, registration_date, balance_cash, password_hash, role) VALUES (%s, %s, %s, %s, CURDATE(), 0, %s, 'user')"
        if execute_query(sql, (new_user_id, username, full_name, email, hashed_pw)):
            flash("Registration successful!", "success")
            return redirect(url_for('login'))
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user_data = execute_query("SELECT * FROM Users WHERE username = %s", (username,), fetch_all=False)
        if user_data and user_data.get('password_hash'):
            if check_password_hash(user_data['password_hash'], password):
                user_obj = User(user_data)
                login_user(user_obj)
                flash(f"Welcome back, {user_data['full_name']}!", "success")
                next_page = request.args.get('next')
                return redirect(next_page if next_page else url_for('index'))
        flash("Invalid username or password.", "danger")
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

def rows_to_geojson(rows, lat_key="latitude", lon_key="longitude"):
    features = []
    if not rows:
        return {"type": "FeatureCollection", "features": features}
    
    for r in rows:
        raw_lat = r.get(lat_key)
        raw_lon = r.get(lon_key)
        
        # SKIP if the database literally returned NULL for coordinates
        if raw_lat is None or raw_lon is None:
            continue
            
        try:
            # FORCE the coordinates to be strict numbers so Leaflet doesn't crash
            lat = float(raw_lat)
            lon = float(raw_lon)
        except (ValueError, TypeError):
            # If the database sent garbage text instead of a number, skip it
            continue
            
        props = {k: v for k, v in r.items() if k not in (lat_key, lon_key)}
        
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })
        
    return {"type": "FeatureCollection", "features": features}

@app.route("/map")
@login_required
def map_page():
    cities_data = execute_query("SELECT DISTINCT city FROM Parcels WHERE city IS NOT NULL", fetch_all=True)
    cities = [c["city"] for c in cities_data] if cities_data else []
    return render_template("map.html", cities=cities, current_user_id=current_user.id)

@app.route("/api/parcels_geojson")
@login_required
def api_parcels_geojson():
    city = request.args.get("city")
    params = []
    sql = "SELECT P.parcel_id, P.address, P.city, P.state, P.base_price_inr, P.listing_price_inr, P.is_for_sale, P.owner_user_id, U.username as owner_name, P.latitude, P.longitude FROM Parcels P LEFT JOIN Users U ON P.owner_user_id = U.user_id WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL"
    if city:
        sql += " AND P.city = %s"
        params.append(city)
    rows = execute_query(sql, tuple(params), fetch_all=True)
    return jsonify(rows_to_geojson(rows))

@app.route("/api/options_geojson")
@login_required
def api_options_geojson():
    sql = "SELECT O.option_id, O.parcel_id, O.strike_inr, O.premium_inr, O.issue_date, O.expiry_date, O.status, P.address, P.city, P.latitude, P.longitude, U_Seller.username as seller_name, U_Buyer.username as buyer_name FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id LEFT JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL"
    rows = execute_query(sql, fetch_all=True)
    return jsonify(rows_to_geojson(rows))

@app.route("/api/heat_by_city")
@login_required
def api_heat_by_city():
    sql = "SELECT city, AVG(base_price_inr) as avg_price, COUNT(*) as count, MAX(latitude) as lat, MAX(longitude) as lon FROM Parcels WHERE latitude IS NOT NULL AND longitude IS NOT NULL GROUP BY city"
    rows = execute_query(sql, fetch_all=True)
    if not rows:
        return jsonify([])
    return jsonify(rows)

def format_inr(amount):
    if amount is None or amount == "":
        return "N/A"
    try:
        return f"INR {float(amount):,.0f}"
    except ValueError:
        return "N/A"

app.jinja_env.filters["inr"] = format_inr
app.jinja_env.filters["date"] = lambda d: d.strftime("%Y-%m-%d") if isinstance(d, (datetime, date)) else d

@app.route("/")
@login_required
def index():
    users = execute_query("SELECT user_id, username, balance_cash FROM Users ORDER BY balance_cash DESC LIMIT 5", fetch_all=True)
    open_count = execute_query("SELECT COUNT(*) as count FROM Options WHERE status = 'Open'", fetch_all=False)
    stats = {
        "users_count": execute_query("SELECT COUNT(*) as count FROM Users", fetch_all=False).get("count", 0),
        "parcels_count": execute_query("SELECT COUNT(*) as count FROM Parcels", fetch_all=False).get("count", 0),
        "price_history_count": execute_query("SELECT COUNT(*) as count FROM Price_History", fetch_all=False).get("count", 0),
        "total_options_count": execute_query("SELECT COUNT(*) as count FROM Options", fetch_all=False).get("count", 0),
        "trades_count": execute_query("SELECT COUNT(*) as count FROM Trades", fetch_all=False).get("count", 0)
    }
    return render_template("dashboard.html", users=users, open_options_count=open_count.get("count", 0) if open_count else 0, stats=stats)

@app.route("/users")
@login_required
def list_users():
    if current_user.role != 'admin':
        flash("Access Denied: Only Admins can view the full user list.", "danger")
        return redirect(url_for("index"))
        
    search_query = request.args.get("search", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    per_page = 50
    
    where_clauses, params = [], []
    if search_query:
        where_clauses.append("(user_id LIKE %s OR username LIKE %s OR full_name LIKE %s OR email LIKE %s)")
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
        
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    count_sql = "SELECT COUNT(*) as total FROM Users" + where_sql
    total_records_result = execute_query(count_sql, tuple(params), fetch_all=False)
    total = total_records_result["total"] if total_records_result else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    
    sql = f"SELECT user_id, username, full_name, balance_cash FROM Users {where_sql} ORDER BY registration_date DESC LIMIT %s OFFSET %s"
    users = execute_query(sql, tuple(params) + (per_page, offset), fetch_all=True)
    
    return render_template("users.html", users=users, search_query=search_query, page=page, total_pages=total_pages)

@app.route("/users/<user_id>")
@login_required
def view_user(user_id):
    if current_user.role != 'admin' and current_user.id != user_id:
        flash("Privacy Error: You can only view your own profile.", "danger")
        return redirect(url_for("index"))
    user = execute_query("SELECT * FROM Users WHERE user_id = %s", (user_id,), fetch_all=False)
    if not user:
        flash(f"User ID {user_id} not found.", "danger")
        return redirect(url_for("index"))
    parcels = execute_query("SELECT parcel_id, address, city, base_price_inr FROM Parcels WHERE owner_user_id = %s", (user_id,), fetch_all=True)
    trades = execute_query("SELECT T.trade_id, T.trade_date, O.option_id, P.address, P.city, T.quantity, T.trade_price_inr FROM Trades T JOIN Options O ON T.option_id = O.option_id JOIN Parcels P ON O.parcel_id = P.parcel_id WHERE T.buyer_user_id = %s OR T.seller_user_id = %s ORDER BY T.trade_date DESC", (user_id, user_id), fetch_all=True)
    return render_template("user_profile.html", user=user, parcels=parcels, trades=trades)

@app.route("/users/add", methods=["GET", "POST"])
@login_required
def add_user():
    if current_user.role != 'admin':
        flash("Access Denied: You do not have permission to add users.", "danger")
        return redirect(url_for("index"))
    if request.method == "POST":
        user_id = request.form["user_id"]
        username = request.form["username"]
        full_name = request.form["full_name"]
        email = request.form["email"]
        balance = request.form["balance_cash"]
        password = request.form["password"]
        hashed_pw = generate_password_hash(password)
        sql = "INSERT INTO Users (user_id, username, full_name, email, registration_date, balance_cash, password_hash, role) VALUES (%s, %s, %s, %s, CURDATE(), %s, %s, 'user')"
        success = execute_query(sql, (user_id, username, full_name, email, balance, hashed_pw))
        if success:
            flash(f"User {username} added successfully!", "success")
            return redirect(url_for("list_users"))
        else:
            flash("Error adding user. User ID or Username/Email might already exist.", "danger")
    return render_template("add_user.html")

@app.route("/users/delete/<user_id>", methods=["POST"])
@login_required
def delete_user(user_id):
    if current_user.role != 'admin':
        flash("Access Denied: Only Admins can delete users.", "danger")
        return redirect(url_for("index"))
    parcels_count = execute_query("SELECT COUNT(*) as count FROM Parcels WHERE owner_user_id = %s", (user_id,), fetch_all=False)
    options_count = execute_query("SELECT COUNT(*) as count FROM Options WHERE seller_user_id = %s OR buyer_user_id = %s", (user_id, user_id), fetch_all=False)
    trades_count = execute_query("SELECT COUNT(*) as count FROM Trades WHERE seller_user_id = %s OR buyer_user_id = %s", (user_id, user_id), fetch_all=False)
    if (parcels_count and parcels_count.get("count", 0) > 0) or (options_count and options_count.get("count", 0) > 0) or (trades_count and trades_count.get("count", 0) > 0):
        flash("Deletion failed: This user is linked to existing Parcels, Options, or Trades.", "danger")
        return redirect(url_for("list_users"))
    success = execute_query("DELETE FROM Users WHERE user_id = %s", (user_id,))
    if success:
        flash(f"User ID {user_id} successfully deleted.", "success")
    else:
        flash(f"Error deleting User ID {user_id}.", "danger")
    return redirect(url_for("list_users"))

@app.route("/parcels")
@login_required
def list_parcels():
    status_filter = request.args.get("status", "All")
    search_query = request.args.get("search", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    per_page = 50
    
    where_clauses, params = [], []
    if status_filter == "For Sale":
        where_clauses.append("P.is_for_sale = TRUE")
    if search_query:
        where_clauses.append("(P.parcel_id LIKE %s OR P.city LIKE %s OR U.username LIKE %s)")
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern])
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    total_result = execute_query("SELECT COUNT(*) as total FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id" + where_sql, tuple(params), fetch_all=False)
    total = total_result["total"] if total_result else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    
    sql = f"SELECT P.*, U.username as owner_name FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id {where_sql} ORDER BY P.parcel_id ASC LIMIT %s OFFSET %s"
    parcels = execute_query(sql, tuple(params) + (per_page, offset), fetch_all=True)
    return render_template("parcels.html", parcels=parcels, status_filter=status_filter, search_query=search_query, page=page, total_pages=total_pages)

@app.route("/parcels/<parcel_id>")
@login_required
def view_parcel(parcel_id):
    parcel = execute_query("SELECT P.*, U.username as owner_name FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id WHERE P.parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel: return redirect(url_for("list_parcels"))
    history = execute_query("SELECT record_date, price_inr FROM Price_History WHERE parcel_id = %s ORDER BY record_date ASC", (parcel_id,), fetch_all=True)
    analytics = get_land_price_analytics(history)
    dates = [r["record_date"].strftime("%b %d, %Y") for r in history]
    if analytics["forecasted_price"] and history:
        dates.append((history[-1]["record_date"] + pd.Timedelta(days=30)).strftime("%b %d, %Y"))
    chart_data = {
        "dates": dates, "actual": [r["price_inr"] for r in history],
        "trend": [p["price"] for p in analytics["regression_line"]],
        "ma": [p["price"] for p in analytics["moving_average"]],
        "forecast": analytics["forecasted_price"]
    }
    active_price = parcel.get("listing_price_inr") if parcel.get("listing_price_inr") else (history[-1]["price_inr"] if history else parcel["base_price_inr"])
    
    return render_template("parcel_detail.html", parcel=parcel, current_price=history[-1]["price_inr"] if history else parcel["base_price_inr"], forecasted_price=analytics["forecasted_price"], chart_data=chart_data, active_price=active_price)

@app.route("/toggle_sale/<parcel_id>", methods=["POST"])
@login_required
def toggle_sale(parcel_id):
    parcel = execute_query("SELECT owner_user_id, is_for_sale FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel or parcel['owner_user_id'] != current_user.id:
        flash("Unauthorized or parcel not found.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    
    current_status = True if parcel['is_for_sale'] in (1, '1', True, 'True') else False
    new_status = not current_status
    asking_price = request.form.get('asking_price')
    
    if new_status:
        if not asking_price:
            flash("You must set an asking price to list the property.", "warning")
            return redirect(url_for("view_parcel", parcel_id=parcel_id))
        execute_query("UPDATE Parcels SET is_for_sale = %s, listing_price_inr = %s WHERE parcel_id = %s", (new_status, asking_price, parcel_id))
        flash(f"Parcel listed for sale at INR {float(asking_price):,.0f}.", "success")
    else:
        execute_query("UPDATE Parcels SET is_for_sale = %s, listing_price_inr = NULL WHERE parcel_id = %s", (new_status, parcel_id))
        flash("Parcel successfully removed from sale.", "info")
        
    return redirect(url_for("view_parcel", parcel_id=parcel_id))

@app.route("/buy_parcel/<parcel_id>", methods=["POST"])
@login_required
def buy_parcel(parcel_id):
    buyer_id = current_user.id
    parcel = execute_query("SELECT base_price_inr, listing_price_inr, owner_user_id, is_for_sale FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel:
        flash("Transaction failed: Parcel not found.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    
    is_for_sale = True if parcel['is_for_sale'] in (1, '1', True, 'True') else False
    if not is_for_sale or buyer_id == parcel["owner_user_id"]:
        flash("Transaction failed: Parcel not available or already owned.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    
    price = parcel.get("listing_price_inr") if parcel.get("listing_price_inr") else parcel["base_price_inr"]
    seller_id = parcel["owner_user_id"]
    
    queries = [
        ("UPDATE Parcels SET owner_user_id = %s, is_for_sale = FALSE, listing_price_inr = NULL WHERE parcel_id = %s AND is_for_sale = TRUE", (buyer_id, parcel_id)),
        ("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s AND balance_cash >= %s", (price, buyer_id, price)),
        ("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (price, seller_id))
    ]
    if execute_transaction(queries):
        flash(f"Parcel purchased for INR {float(price):,.0f}!", "success")
    else:
        flash("Transaction failed: Insufficient balance or race condition.", "danger")
    return redirect(url_for("view_parcel", parcel_id=parcel_id))

@app.route("/options")
@login_required
def list_options():
    status_filter = request.args.get("status", "Open")
    search_query = request.args.get("search", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    per_page = 50
    
    base_sql = "SELECT O.*, P.address, P.city, U_Seller.username AS seller_name, U_Buyer.username AS buyer_name FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id"
    params, where_clauses = [], []
    if status_filter != "All":
        where_clauses.append("O.status = %s")
        params.append(status_filter)
    if search_query:
        where_clauses.append("(O.option_id LIKE %s OR P.city LIKE %s OR U_Seller.username LIKE %s OR U_Buyer.username LIKE %s)")
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    count_sql = "SELECT COUNT(*) as total FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id" + where_sql
    total_records_result = execute_query(count_sql, tuple(params), fetch_all=False)
    total = total_records_result["total"] if total_records_result else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    
    final_sql = base_sql + where_sql + " ORDER BY O.expiry_date ASC LIMIT %s OFFSET %s"
    options = execute_query(final_sql, tuple(params) + (per_page, offset), fetch_all=True)
    return render_template("options.html", options=options, status_filter=status_filter, search_query=search_query, page=page, total_pages=total_pages)

@app.route("/buy_option/<option_id>", methods=["POST"])
@login_required
def buy_option(option_id):
    buyer_id = current_user.id
    option = execute_query("SELECT premium_inr, seller_user_id, status FROM Options WHERE option_id = %s AND expiry_date >= CURDATE()", (option_id,), fetch_all=False)
    if not option or option["status"] != "Open" or buyer_id == option["seller_user_id"]:
        flash("Trade failed: Option unavailable or expired.", "danger")
        return redirect(url_for("list_options"))
    premium = option["premium_inr"]
    seller_id = option["seller_user_id"]
    trade_id = f"T{uuid.uuid4().hex[:10].upper()}"
    queries = [
        ("UPDATE Options SET status = 'Traded', buyer_user_id = %s WHERE option_id = %s AND status = 'Open'", (buyer_id, option_id)),
        ("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s AND balance_cash >= %s", (premium, buyer_id, premium)),
        ("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (premium, seller_id)),
        ("INSERT INTO Trades (trade_id, option_id, trade_date, trade_price_inr, quantity, buyer_user_id, seller_user_id) VALUES (%s, %s, CURDATE(), %s, 1, %s, %s)", (trade_id, option_id, premium, buyer_id, seller_id))
    ]
    if execute_transaction(queries):
        flash(f"Option bought! Premium paid: {format_inr(premium)}", "success")
    else:
        flash("Trade failed: Insufficient balance or race condition.", "danger")
    return redirect(url_for("list_options"))

@app.route("/trades")
@login_required
def list_trades():
    search_query = request.args.get("search", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    per_page = 50
    
    base_sql = "SELECT T.*, O.option_id, P.address, P.city, U_Buyer.username as buyer_name, U_Seller.username as seller_name FROM Trades T JOIN Options O ON T.option_id = O.option_id JOIN Parcels P ON O.parcel_id = P.parcel_id JOIN Users U_Buyer ON T.buyer_user_id = U_Buyer.user_id JOIN Users U_Seller ON T.seller_user_id = U_Seller.user_id"
    params, where_clauses = [], []
    if search_query:
        where_clauses.append("(T.trade_id LIKE %s OR O.option_id LIKE %s OR P.city LIKE %s OR U_Buyer.username LIKE %s OR U_Seller.username LIKE %s)")
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern, search_pattern])
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    count_sql = "SELECT COUNT(*) as total FROM Trades T JOIN Options O ON T.option_id = O.option_id JOIN Parcels P ON O.parcel_id = P.parcel_id JOIN Users U_Buyer ON T.buyer_user_id = U_Buyer.user_id JOIN Users U_Seller ON T.seller_user_id = U_Seller.user_id" + where_sql
    total_records_result = execute_query(count_sql, tuple(params), fetch_all=False)
    total = total_records_result["total"] if total_records_result else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    
    final_sql = base_sql + where_sql + " ORDER BY T.trade_date DESC LIMIT %s OFFSET %s"
    trades = execute_query(final_sql, tuple(params) + (per_page, offset), fetch_all=True)
    return render_template("trades.html", trades=trades, search_query=search_query, page=page, total_pages=total_pages)

@app.route("/settle_options", methods=["GET", "POST"])
@login_required
def settle_options():
    if current_user.role != 'admin':
        flash("Access Denied: Only Admins can run settlement.", "danger")
        return redirect(url_for("index"))
    expired_options = execute_query("SELECT O.option_id, O.parcel_id, O.strike_inr, O.buyer_user_id, O.seller_user_id, P.base_price_inr FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id WHERE O.status = 'Traded' AND O.expiry_date <= CURDATE()", fetch_all=True)
    settlement_results = []
    success_count = 0
    if not expired_options:
        flash("No expired options found.", "info")
        return render_template("settlement.html", results=settlement_results)
        
    for option in expired_options:
        latest_price_record = execute_query("SELECT price_inr FROM Price_History WHERE parcel_id = %s ORDER BY record_date DESC LIMIT 1", (option["parcel_id"],), fetch_all=False)
        settlement_price = latest_price_record["price_inr"] if latest_price_record else option["base_price_inr"]
        strike = option["strike_inr"]
        payout = 0
        status_update = "Expired OTM"
        single_option_queries = []
        if settlement_price > strike:
            payout = settlement_price - strike
            status_update = "Expired ITM"
            single_option_queries.append(("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s", (payout, option["seller_user_id"])))
            single_option_queries.append(("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (payout, option["buyer_user_id"])))
        single_option_queries.append(("UPDATE Options SET status = %s WHERE option_id = %s", (status_update, option["option_id"])))
        if execute_transaction(single_option_queries):
            success_count += 1
            settlement_results.append({
                "option_id": option["option_id"], "settlement_price": settlement_price,
                "strike": strike, "payout": payout, "result": status_update
            })
            
    if success_count > 0:
        flash(f"Settlement complete. {success_count} options processed.", "success")
    else:
        flash("Settlement encountered errors on some options.", "warning")
    return render_template("settlement.html", results=settlement_results)

@app.route('/deposit', methods=['POST'])
@login_required
def deposit_funds():
    amount = request.form.get('amount', type=float)
    if amount and 0 < amount <= 1000000000:
        success = execute_query("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (amount, current_user.id))
        if success:
            flash(f"Deposited INR {amount:,.2f}.", "success")
            current_user.balance_cash += amount
        else:
            flash("Database error.", "danger")
    else:
        flash("Invalid amount.", "danger")
    return redirect(url_for('view_user', user_id=current_user.id))

@app.route('/change_password', methods=['POST'])
@login_required
def change_password():
    current_password = request.form.get('current_password')
    new_password = request.form.get('new_password')
    confirm_new_password = request.form.get('confirm_new_password')
    if new_password != confirm_new_password:
        flash("Passwords do not match.", "danger")
        return redirect(url_for('view_user', user_id=current_user.id))
    user_data = execute_query("SELECT password_hash FROM Users WHERE user_id = %s", (current_user.id,), fetch_all=False)
    if not user_data or not check_password_hash(user_data['password_hash'], current_password):
        flash("Incorrect password.", "danger")
        return redirect(url_for('view_user', user_id=current_user.id))
    hashed_pw = generate_password_hash(new_password)
    success = execute_query("UPDATE Users SET password_hash = %s WHERE user_id = %s", (hashed_pw, current_user.id))
    if success:
        flash("Password updated.", "success")
    else:
        flash("Database error.", "danger")
    return redirect(url_for('view_user', user_id=current_user.id))

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000, debug=os.environ.get('FLASK_DEBUG', 'False').lower() == 'true')
