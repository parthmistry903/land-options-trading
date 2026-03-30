from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import pandas as pd
import numpy as np
import base64
from io import BytesIO
from datetime import datetime, date
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from valuation import get_land_price_analytics, calculate_fair_option_premium
from db import execute_query
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import random
import string

matplotlib.use("Agg")

app = Flask(__name__)
app.secret_key = "supersecretkeyforflashmessages"

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
            flash("Registration failed: Passwords do not match.", "danger")
            return redirect(url_for('register'))
            
        existing_user = execute_query("SELECT * FROM Users WHERE username = %s OR email = %s", (username, email), fetch_all=False)
        if existing_user:
            flash("Username or Email already exists.", "danger")
            return redirect(url_for('register'))
            
        new_user_id = f"U{random.randint(1000, 9999)}"
        hashed_pw = generate_password_hash(password)
        starting_balance = 0 
        
        sql = """
        INSERT INTO Users (user_id, username, full_name, email, registration_date, balance_cash, password_hash, role)
        VALUES (%s, %s, %s, %s, CURDATE(), %s, %s, 'user')
        """
        success = execute_query(sql, (new_user_id, username, full_name, email, starting_balance, hashed_pw))
        
        if success:
            flash("Registration successful! Please log in.", "success")
            return redirect(url_for('login'))
        else:
            flash("Database error occurred.", "danger")
            
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        user_data = execute_query("SELECT * FROM Users WHERE username = %s", (username,), fetch_all=False)
        
        if user_data:
            db_password = user_data.get('password_hash') or user_data.get('password')
            
            if db_password:
                is_valid = False
                
                try:
                    is_valid = check_password_hash(db_password, password)
                except ValueError:
                    pass
                
                if not is_valid and db_password == password:
                    is_valid = True
                    
                if is_valid:
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
    flash("You have been logged out.", "info")
    return redirect(url_for('login'))

def rows_to_geojson(rows, lat_key="latitude", lon_key="longitude"):
    features = []
    for r in rows:
        lat = r.get(lat_key)
        lon = r.get(lon_key)
        if lat is None or lon is None:
            continue
        props = {k: v for k, v in r.items() if k not in (lat_key, lon_key)}
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )
    return {"type": "FeatureCollection", "features": features}

@app.route("/map")
@login_required
def map_page():
    cities = execute_query("SELECT DISTINCT city FROM Parcels WHERE city IS NOT NULL", fetch_all=True)
    cities = [c["city"] for c in cities] if cities else []
    return render_template("map.html", cities=cities)

@app.route("/api/parcels_geojson")
@login_required
def api_parcels_geojson():
    city = request.args.get("city")
    params = []
    sql = """
      SELECT P.parcel_id, P.address, P.city, P.state, P.base_price_inr, P.owner_user_id,
             U.username as owner_name, P.latitude, P.longitude
      FROM Parcels P
      LEFT JOIN Users U ON P.owner_user_id = U.user_id
      WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL
    """
    if city:
        sql += " AND P.city = %s"
        params.append(city)
    rows = execute_query(sql, tuple(params), fetch_all=True)
    return jsonify(rows_to_geojson(rows))

@app.route("/api/options_geojson")
@login_required
def api_options_geojson():
    sql = """
      SELECT O.option_id, O.parcel_id, O.strike_inr, O.premium_inr, O.issue_date, O.expiry_date, O.status,
             P.address, P.city, P.latitude, P.longitude, U_Seller.username as seller_name, U_Buyer.username as buyer_name
      FROM Options O
      JOIN Parcels P ON O.parcel_id = P.parcel_id
      LEFT JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id
      LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id
      WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL
    """
    rows = execute_query(sql, fetch_all=True)
    return jsonify(rows_to_geojson(rows))

@app.route("/api/heat_by_city")
@login_required
def api_heat_by_city():
    sql = """
      SELECT city, AVG(base_price_inr) as avg_price, COUNT(*) as n_parcels
      FROM Parcels
      WHERE latitude IS NOT NULL AND longitude IS NOT NULL
      GROUP BY city
    """
    rows = execute_query(sql, fetch_all=True)
    results = []
    for r in rows:
        city = r["city"]
        coord = execute_query(
            "SELECT latitude, longitude FROM Parcels WHERE city = %s AND latitude IS NOT NULL LIMIT 1",
            (city,), fetch_all=False,
        )
        if not coord:
            continue
        results.append({
            "city": city,
            "lat": coord["latitude"],
            "lon": coord["longitude"],
            "avg_price": r["avg_price"],
            "count": r["n_parcels"],
        })
    return jsonify(results)

def format_inr(amount):
    if amount is None:
        return "N/A"
    return f"INR {amount:,.0f}"

app.jinja_env.filters["inr"] = format_inr
app.jinja_env.filters["date"] = lambda d: d.strftime("%Y-%m-%d") if isinstance(d, (datetime, date)) else d

@app.route("/")
@login_required
def index():
    users = execute_query("SELECT user_id, username, balance_cash FROM Users ORDER BY balance_cash DESC LIMIT 5", fetch_all=True)
    open_options_count = execute_query("SELECT COUNT(*) as count FROM Options WHERE status = 'Open'", fetch_all=False)
    open_options_count = open_options_count["count"] if open_options_count else 0

    stats = {
        "users_count": execute_query("SELECT COUNT(*) as count FROM Users", fetch_all=False)["count"],
        "parcels_count": execute_query("SELECT COUNT(*) as count FROM Parcels", fetch_all=False)["count"],
        "price_history_count": execute_query("SELECT COUNT(*) as count FROM Price_History", fetch_all=False)["count"],
        "total_options_count": execute_query("SELECT COUNT(*) as count FROM Options", fetch_all=False)["count"],
        "trades_count": execute_query("SELECT COUNT(*) as count FROM Trades", fetch_all=False)["count"]
    }

    return render_template("dashboard.html", users=users, open_options_count=open_options_count, stats=stats)

@app.route("/users")
@login_required
def list_users():
    if current_user.role != 'admin':
        flash("Access Denied: Only Admins can view the full user list.", "danger")
        return redirect(url_for("index"))
        
    users = execute_query("SELECT user_id, username, full_name, balance_cash FROM Users", fetch_all=True)
    return render_template("users.html", users=users)

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
    trades = execute_query("""
        SELECT T.trade_id, T.trade_date, O.option_id, P.address, P.city, T.quantity, T.trade_price_inr
        FROM Trades T
        JOIN Options O ON T.option_id = O.option_id
        JOIN Parcels P ON O.parcel_id = P.parcel_id
        WHERE T.buyer_user_id = %s OR T.seller_user_id = %s
        ORDER BY T.trade_date DESC
        """, (user_id, user_id), fetch_all=True)

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

        sql = """
        INSERT INTO Users (user_id, username, full_name, email, registration_date, balance_cash, password_hash, role)
        VALUES (%s, %s, %s, %s, CURDATE(), %s, %s, 'user')
        """
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

    if (parcels_count and parcels_count["count"] > 0) or (options_count and options_count["count"] > 0) or (trades_count and trades_count["count"] > 0):
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
    
    base_sql = """
        SELECT P.parcel_id, P.address, P.city, P.state, P.area_sqm, P.is_for_sale, 
               U.username as owner_name, P.owner_user_id 
        FROM Parcels P 
        JOIN Users U ON P.owner_user_id = U.user_id
    """
    
    if status_filter == "For Sale":
        base_sql += " WHERE P.is_for_sale = TRUE"
        
    parcels = execute_query(base_sql, fetch_all=True)
    return render_template("parcels.html", parcels=parcels, status_filter=status_filter)

@app.route("/parcels/<parcel_id>")
@login_required
def view_parcel(parcel_id):
    parcel = execute_query(
        "SELECT P.*, U.username as owner_name, U.user_id as owner_id FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id WHERE P.parcel_id = %s",
        (parcel_id,), fetch_all=False
    )
    if not parcel:
        flash(f"Parcel ID {parcel_id} not found.", "danger")
        return redirect(url_for("list_parcels"))

    history = execute_query("SELECT record_date, price_inr FROM Price_History WHERE parcel_id = %s ORDER BY record_date ASC", (parcel_id,), fetch_all=True)
    analytics = get_land_price_analytics(history)
    chart_base64 = generate_price_chart(history, analytics)
    current_price = history[-1]["price_inr"] if history else parcel["base_price_inr"]

    return render_template(
        "parcel_detail.html",
        parcel=parcel,
        current_price=current_price,
        forecasted_price=analytics["forecasted_price"],
        chart_base64=chart_base64
    )

def generate_price_chart(history_data, analytics):
    df = pd.DataFrame(history_data)
    if df.empty: return None

    df["record_date"] = pd.to_datetime(df["record_date"])
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df["record_date"], df["price_inr"], marker="o", linestyle="-", color="tab:blue", label="Actual Price")

    if analytics["data_sufficient"]:
        reg_dates = pd.to_datetime([d["date"] for d in analytics["regression_line"]])
        ax.plot(reg_dates, [d["price"] for d in analytics["regression_line"]], linestyle="--", color="green", label="Linear Regression Trend")
        
        ma_dates = pd.to_datetime([d["date"] for d in analytics["moving_average"]])
        ax.plot(ma_dates, [d["price"] for d in analytics["moving_average"]], linestyle=":", color="orange", label="6-Period Moving Average")

    if analytics["forecasted_price"] is not None:
        last_date = df["record_date"].max()
        forecast_date = last_date + pd.Timedelta(days=30)
        ax.plot([last_date, forecast_date], [df["price_inr"].iloc[-1], analytics["forecasted_price"]], marker="x", linestyle="-.", color="tab:red", label="LR Forecast (30 Days)")
        ax.annotate(f"Forecast: {format_inr(analytics['forecasted_price'])}", (forecast_date, analytics["forecasted_price"]), textcoords="offset points", xytext=(-10, 10), ha="right", color="tab:red", fontweight="bold")

    ax.set_title("Land Price History with Regression and MA")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price (INR)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()
    ax.grid(True)
    ax.legend(loc="best")

    buf = BytesIO()
    plt.savefig(buf, format="png")
    plt.close(fig)
    return f"data:image/png;base64,{base64.b64encode(buf.getbuffer()).decode('ascii')}"

@app.route("/toggle_sale/<parcel_id>", methods=["POST"])
@login_required
def toggle_sale(parcel_id):
    parcel = execute_query("SELECT owner_user_id, is_for_sale FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    
    if not parcel or parcel['owner_user_id'] != current_user.id:
        flash("Unauthorized or parcel not found.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    
    new_status = not parcel['is_for_sale']
    execute_query("UPDATE Parcels SET is_for_sale = %s WHERE parcel_id = %s", (new_status, parcel_id))
    
    status_text = "listed for sale" if new_status else "removed from sale"
    flash(f"Parcel {parcel_id} successfully {status_text}.", "success")
    return redirect(url_for("view_parcel", parcel_id=parcel_id))

@app.route("/buy_parcel/<parcel_id>", methods=["POST"])
@login_required
def buy_parcel(parcel_id):
    buyer_id = current_user.id 

    parcel = execute_query("SELECT base_price_inr, owner_user_id, is_for_sale FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    buyer = execute_query("SELECT balance_cash FROM Users WHERE user_id = %s", (buyer_id,), fetch_all=False)

    if not parcel or not buyer:
        flash("Transaction failed: Parcel or Buyer not found.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))

    if not parcel.get('is_for_sale'):
        flash("Transaction failed: The owner has not listed this parcel for sale.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))

    price = parcel["base_price_inr"]
    seller_id = parcel["owner_user_id"]

    if buyer_id == seller_id:
        flash("Transaction failed: You already own this parcel.", "warning")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))

    if buyer["balance_cash"] < price:
        flash(f"Transaction failed: Insufficient balance. Required: INR {price:,.0f}", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))

    execute_query("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s", (price, buyer_id))
    execute_query("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (price, seller_id))
    execute_query("UPDATE Parcels SET owner_user_id = %s, is_for_sale = FALSE WHERE parcel_id = %s", (buyer_id, parcel_id))

    flash(f"Parcel {parcel_id} successfully purchased for INR {price:,.0f}!", "success")
    return redirect(url_for("view_parcel", parcel_id=parcel_id))

@app.route("/options")
@login_required
def list_options():
    base_sql = """
        SELECT O.*, P.address, P.city, U_Seller.username AS seller_name, U_Buyer.username AS buyer_name
        FROM Options O
        JOIN Parcels P ON O.parcel_id = P.parcel_id
        JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id
        LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id
    """
    status_filter = request.args.get("status", "Open")
    params, where_clauses = [], []

    if status_filter != "All":
        where_clauses.append("O.status = %s")
        params.append(status_filter)

    if where_clauses:
        base_sql += " WHERE " + " AND ".join(where_clauses)
    base_sql += " ORDER BY O.expiry_date ASC"

    options = execute_query(base_sql, tuple(params), fetch_all=True)
    return render_template("options.html", options=options, status_filter=status_filter)

@app.route("/buy_option/<option_id>", methods=["POST"])
@login_required
def buy_option(option_id):
    buyer_id = current_user.id
    option = execute_query("SELECT premium_inr, seller_user_id, status FROM Options WHERE option_id = %s", (option_id,), fetch_all=False)
    buyer = execute_query("SELECT balance_cash FROM Users WHERE user_id = %s", (buyer_id,), fetch_all=False)

    if not option or not buyer:
        flash("Trade failed: Option or Buyer not found.", "danger")
        return redirect(url_for("list_options"))

    premium = option["premium_inr"]
    seller_id = option["seller_user_id"]

    if option["status"] != "Open":
        flash("Trade failed: Option is not 'Open'.", "danger")
        return redirect(url_for("list_options"))
    if buyer_id == seller_id:
        flash("Trade failed: Cannot buy an option from yourself.", "danger")
        return redirect(url_for("list_options"))
    if buyer["balance_cash"] < premium:
        flash(f"Trade failed: Insufficient balance. Required: {format_inr(premium)}", "danger")
        return redirect(url_for("list_options"))

    execute_query("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s", (premium, buyer_id))
    execute_query("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (premium, seller_id))
    execute_query("UPDATE Options SET status = 'Traded', buyer_user_id = %s WHERE option_id = %s", (buyer_id, option_id))

    trade_id = "T" + datetime.now().strftime("%y%m%d%H%M%S")
    execute_query(
        "INSERT INTO Trades (trade_id, option_id, trade_date, trade_price_inr, quantity, buyer_user_id, seller_user_id) VALUES (%s, %s, CURDATE(), %s, 1, %s, %s)",
        (trade_id, option_id, premium, buyer_id, seller_id)
    )

    flash(f"Option {option_id} successfully bought! Premium paid: {format_inr(premium)}", "success")
    return redirect(url_for("list_options"))

@app.route("/trades")
@login_required
def list_trades():
    trades = execute_query("""
        SELECT T.*, O.option_id, P.address, P.city, U_Buyer.username as buyer_name, U_Seller.username as seller_name
        FROM Trades T
        JOIN Options O ON T.option_id = O.option_id
        JOIN Parcels P ON O.parcel_id = P.parcel_id
        JOIN Users U_Buyer ON T.buyer_user_id = U_Buyer.user_id
        JOIN Users U_Seller ON T.seller_user_id = U_Seller.user_id
        ORDER BY T.trade_date DESC
        """, fetch_all=True)
    return render_template("trades.html", trades=trades)

@app.route("/settle_options", methods=["GET", "POST"])
@login_required
def settle_options():
    if current_user.role != 'admin':
        flash("Access Denied: Only Admins can run the settlement engine.", "danger")
        return redirect(url_for("index"))

    expired_options = execute_query("""
        SELECT O.option_id, O.parcel_id, O.strike_inr, O.buyer_user_id, O.seller_user_id, P.base_price_inr
        FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id
        WHERE O.status = 'Traded' AND O.expiry_date <= CURDATE()
        """, fetch_all=True)

    settlement_results = []
    for option in expired_options:
        latest_price_record = execute_query("SELECT price_inr FROM Price_History WHERE parcel_id = %s ORDER BY record_date DESC LIMIT 1", (option["parcel_id"],), fetch_all=False)
        settlement_price = latest_price_record["price_inr"] if latest_price_record else option["base_price_inr"]
        strike = option["strike_inr"]
        payout = 0
        status_update = "Expired OTM"

        if settlement_price > strike:
            payout = settlement_price - strike
            status_update = "Expired ITM"
            execute_query("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s", (payout, option["seller_user_id"]))
            execute_query("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (payout, option["buyer_user_id"]))

        execute_query("UPDATE Options SET status = %s WHERE option_id = %s", (status_update, option["option_id"]))
        settlement_results.append({
            "option_id": option["option_id"], "settlement_price": settlement_price,
            "strike": strike, "payout": payout, "result": status_update
        })

    flash(f"Settlement run complete. {len(settlement_results)} options processed.", "info")
    return render_template("settlement.html", results=settlement_results)

@app.route('/deposit', methods=['POST'])
@login_required
def deposit_funds():
    amount = request.form.get('amount', type=float)
    if amount and amount > 0:
        success = execute_query(
            "UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s",
            (amount, current_user.id)
        )
        if success:
            flash(f"Successfully deposited INR {amount:,.2f} into your account.", "success")
            current_user.balance_cash += amount 
        else:
            flash("Database error during deposit.", "danger")
    else:
        flash("Invalid deposit amount.", "danger")
    
    return redirect(url_for('view_user', user_id=current_user.id))

if __name__ == "__main__":
    app.run(debug=True)
