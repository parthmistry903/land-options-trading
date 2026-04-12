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
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "supersecretkeyforlandoptions")

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message_category = "warning"


class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data["user_id"]
        self.username = user_data["username"]
        self.full_name = user_data["full_name"]
        self.balance_cash = user_data["balance_cash"]
        self.role = user_data.get("role", "user")


@login_manager.user_loader
def load_user(user_id):
    user_data = execute_query("SELECT * FROM Users WHERE user_id = %s", (user_id,), fetch_all=False)
    return User(user_data) if user_data else None


def generate_unique_id(prefix, table, column_name):
    while True:
        new_id = f"{prefix}{uuid.uuid4().hex[:8].upper()}"
        check = execute_query(f"SELECT 1 FROM {table} WHERE {column_name} = %s", (new_id,), fetch_all=False)
        if not check:
            return new_id


def format_inr(amount):
    if amount is None or amount == "":
        return "₹0"
    try:
        return f"INR {float(amount):,.0f}"
    except (ValueError, TypeError):
        return "₹0"


def format_date(d):
    if not d:
        return "N/A"
    if isinstance(d, (datetime, date)):
        return d.strftime("%Y-%m-%d")
    return str(d)


app.jinja_env.filters["inr"] = format_inr
app.jinja_env.filters["date"] = format_date


@app.route("/")
@login_required
def index():
    users = execute_query("SELECT user_id, username, balance_cash FROM Users ORDER BY balance_cash DESC LIMIT 5", fetch_all=True)
    open_count_res = execute_query("SELECT COUNT(*) as count FROM Options WHERE status = 'Open'", fetch_all=False)
    stats_u = execute_query("SELECT COUNT(*) as count FROM Users", fetch_all=False)
    stats_p = execute_query("SELECT COUNT(*) as count FROM Parcels", fetch_all=False)
    stats_h = execute_query("SELECT COUNT(*) as count FROM Price_History", fetch_all=False)
    stats_o = execute_query("SELECT COUNT(*) as count FROM Options", fetch_all=False)
    stats_t = execute_query("SELECT (SELECT COUNT(*) FROM Trades) + (SELECT COUNT(*) FROM Price_History) as count", fetch_all=False)
    stats = {
        "users_count": stats_u.get("count", 0) if stats_u else 0,
        "parcels_count": stats_p.get("count", 0) if stats_p else 0,
        "price_history_count": stats_h.get("count", 0) if stats_h else 0,
        "total_options_count": stats_o.get("count", 0) if stats_o else 0,
        "trades_count": stats_t.get("count", 0) if stats_t else 0,
    }
    return render_template("dashboard.html", users=users or [], open_options_count=open_count_res.get("count", 0) if open_count_res else 0, stats=stats)


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        username = request.form["username"]
        full_name = request.form["full_name"]
        email = request.form["email"]
        password = request.form["password"]
        confirm_password = request.form.get("confirm_password")

        if password != confirm_password:
            flash("Passwords do not match.", "danger")
            return redirect(url_for("register"))

        existing_user = execute_query("SELECT * FROM Users WHERE username = %s OR email = %s", (username, email), fetch_all=False)
        if existing_user:
            flash("Username or Email already exists.", "danger")
            return redirect(url_for("register"))

        new_user_id = generate_unique_id("U", "Users", "user_id")
        hashed_pw = generate_password_hash(password)
        sql = "INSERT INTO Users (user_id, username, full_name, email, registration_date, balance_cash, password_hash, role) VALUES (%s, %s, %s, %s, CURDATE(), 0, %s, 'user')"
        if execute_query(sql, (new_user_id, username, full_name, email, hashed_pw)):
            flash("Registration successful!", "success")
            return redirect(url_for("login"))
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        user_data = execute_query("SELECT * FROM Users WHERE username = %s", (request.form["username"],), fetch_all=False)
        if user_data and check_password_hash(user_data["password_hash"], request.form["password"]):
            login_user(User(user_data))
            flash(f"Session Initialized: Welcome back, {user_data['full_name']}!", "success")
            next_p = request.args.get("next")
            return redirect(next_p if next_p else url_for("index"))
        flash("Authentication Failure.", "danger")
    return render_template("login.html")


@app.route("/forgot_password", methods=["GET", "POST"])
def forgot_password():
    if current_user.is_authenticated:
        return redirect(url_for("index"))
    if request.method == "POST":
        user_data = execute_query(
            "SELECT user_id FROM Users WHERE username = %s AND email = %s",
            (request.form.get("username"), request.form.get("email")),
            fetch_all=False,
        )
        if user_data:
            execute_query(
                "UPDATE Users SET password_hash = %s WHERE user_id = %s",
                (generate_password_hash(request.form.get("new_password")), user_data["user_id"]),
            )
            flash("Cipher Reset Complete.", "success")
            return redirect(url_for("login"))
        flash("Entity Identification Failed.", "danger")
    return render_template("forgot_password.html")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


def rows_to_geojson(rows, lat_key="latitude", lon_key="longitude"):
    features = []
    if not rows:
        return {"type": "FeatureCollection", "features": features}
    for r in rows:
        raw_lat = r.get(lat_key)
        raw_lon = r.get(lon_key)
        if raw_lat is None or raw_lon is None:
            continue
        try:
            lat = float(raw_lat)
            lon = float(raw_lon)
        except (ValueError, TypeError):
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
    cities_data = execute_query("SELECT DISTINCT city FROM Parcels WHERE city IS NOT NULL", fetch_all=True)
    cities = [c["city"] for c in cities_data] if cities_data else []
    return render_template("map.html", cities=cities, current_user_id=current_user.id)


@app.route("/api/parcels_geojson")
@login_required
def api_parcels_geojson():
    city = request.args.get("city")
    params = []
    sql = "SELECT P.*, U.username as owner_name, (SELECT price_inr FROM Price_History PH WHERE PH.parcel_id = P.parcel_id ORDER BY record_date DESC LIMIT 1) as current_price FROM Parcels P LEFT JOIN Users U ON P.owner_user_id = U.user_id WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL"
    if city:
        sql += " AND P.city = %s"
        params.append(city)
    rows = execute_query(sql, tuple(params), fetch_all=True)
    return jsonify(rows_to_geojson(rows))


@app.route("/api/options_geojson")
@login_required
def api_options_geojson():
    sql = "SELECT O.*, P.address, P.city, P.latitude, P.longitude, U_Seller.username as seller_name, U_Buyer.username as buyer_name FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id LEFT JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL AND O.status = 'Open' AND O.expiry_date >= CURDATE()"
    rows = execute_query(sql, fetch_all=True)
    return jsonify(rows_to_geojson(rows))


@app.route("/api/heat_by_city")
@login_required
def api_heat_by_city():
    sql = "SELECT P.city, AVG(COALESCE((SELECT price_inr FROM Price_History PH WHERE PH.parcel_id = P.parcel_id ORDER BY record_date DESC LIMIT 1), P.base_price_inr)) as avg_price, COUNT(*) as count, MAX(P.latitude) as lat, MAX(P.longitude) as lon FROM Parcels P WHERE P.latitude IS NOT NULL AND P.longitude IS NOT NULL GROUP BY P.city"
    rows = execute_query(sql, fetch_all=True)
    if not rows:
        return jsonify([])
    return jsonify(rows)


@app.route("/users")
@login_required
def list_users():
    if current_user.role != "admin":
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
    return render_template("users.html", users=users or [], search_query=search_query, page=page, total_pages=total_pages)


@app.route("/users/<user_id>")
@login_required
def view_user(user_id):
    if current_user.role != "admin" and current_user.id != user_id:
        flash("Privacy Error: You can only view your own profile.", "danger")
        return redirect(url_for("index"))
    user = execute_query("SELECT * FROM Users WHERE user_id = %s", (user_id,), fetch_all=False)
    if not user:
        flash(f"User ID {user_id} not found.", "danger")
        return redirect(url_for("index"))
    parcels = execute_query("SELECT parcel_id, address, city, base_price_inr FROM Parcels WHERE owner_user_id = %s", (user_id,), fetch_all=True)
    trades = execute_query("SELECT T.trade_id, T.trade_date, O.option_id, P.address, P.city, T.quantity, T.trade_price_inr FROM Trades T JOIN Options O ON T.option_id = O.option_id JOIN Parcels P ON O.parcel_id = P.parcel_id WHERE T.buyer_user_id = %s OR T.seller_user_id = %s ORDER BY T.trade_date DESC", (user_id, user_id), fetch_all=True)
    return render_template("user_profile.html", user=user, parcels=parcels or [], trades=trades or [])


@app.route("/users/add", methods=["GET", "POST"])
@login_required
def add_user():
    if current_user.role != "admin":
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
        if execute_query(sql, (user_id, username, full_name, email, balance, hashed_pw)):
            flash(f"User {username} added successfully!", "success")
            return redirect(url_for("list_users"))
        else:
            flash("Error adding user. User ID or Username/Email might already exist.", "danger")
    return render_template("add_user.html")


@app.route("/users/delete/<user_id>", methods=["POST"])
@login_required
def delete_user(user_id):
    if current_user.role != "admin":
        flash("Access Denied: Only Admins can delete users.", "danger")
        return redirect(url_for("index"))
    parcels_count = execute_query("SELECT COUNT(*) as count FROM Parcels WHERE owner_user_id = %s", (user_id,), fetch_all=False)
    options_count = execute_query("SELECT COUNT(*) as count FROM Options WHERE seller_user_id = %s OR buyer_user_id = %s", (user_id, user_id), fetch_all=False)
    trades_count = execute_query("SELECT COUNT(*) as count FROM Trades WHERE seller_user_id = %s OR buyer_user_id = %s", (user_id, user_id), fetch_all=False)
    if (parcels_count and parcels_count.get("count", 0) > 0) or (options_count and options_count.get("count", 0) > 0) or (trades_count and trades_count.get("count", 0) > 0):
        flash("Deletion failed: This user is linked to existing Parcels, Options, or Trades.", "danger")
        return redirect(url_for("list_users"))
    if execute_query("DELETE FROM Users WHERE user_id = %s", (user_id,)):
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
    total_res = execute_query("SELECT COUNT(*) as total FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id" + where_sql, tuple(params), fetch_all=False)
    total = total_res["total"] if total_res else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = (min(page, total_pages) - 1) * per_page
    sql = f"SELECT P.*, U.username as owner_name FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id {where_sql} ORDER BY P.parcel_id ASC LIMIT %s OFFSET %s"
    parcels = execute_query(sql, tuple(params) + (per_page, offset), fetch_all=True)
    return render_template("parcels.html", parcels=parcels or [], status_filter=status_filter, search_query=search_query, page=page, total_pages=total_pages)


@app.route("/parcels/<parcel_id>")
@login_required
def view_parcel(parcel_id):
    parcel = execute_query("SELECT P.*, U.username as owner_name FROM Parcels P JOIN Users U ON P.owner_user_id = U.user_id WHERE P.parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel:
        return redirect(url_for("list_parcels"))
    history = execute_query("SELECT record_date, price_inr FROM Price_History WHERE parcel_id = %s ORDER BY record_date ASC", (parcel_id,), fetch_all=True)
    analytics = get_land_price_analytics(history)
    dates = [r["record_date"].strftime("%b %d, %Y") for r in history] if history else []
    if analytics["forecasted_price"] and history:
        dates.append((history[-1]["record_date"] + pd.Timedelta(days=30)).strftime("%b %d, %Y"))
    chart_data = {
        "dates": dates,
        "actual": [r["price_inr"] for r in history] if history else [],
        "trend": [p["price"] for p in analytics["regression_line"]] if "regression_line" in analytics else [],
        "ma": [p["price"] for p in analytics["moving_average"]] if "moving_average" in analytics else [],
        "forecast": analytics.get("forecasted_price"),
    }
    active_price = parcel.get("listing_price_inr") if parcel.get("listing_price_inr") else (history[-1]["price_inr"] if history else parcel["base_price_inr"])
    return render_template(
        "parcel_detail.html",
        parcel=parcel,
        current_price=history[-1]["price_inr"] if history else parcel["base_price_inr"],
        forecasted_price=analytics.get("forecasted_price"),
        chart_data=chart_data,
        active_price=active_price,
    )


@app.route("/toggle_sale/<parcel_id>", methods=["POST"])
@login_required
def toggle_sale(parcel_id):
    parcel = execute_query("SELECT owner_user_id, is_for_sale FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel or parcel["owner_user_id"] != current_user.id:
        flash("Unauthorized or parcel not found.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    current_status = True if parcel["is_for_sale"] in (1, "1", True, "True") else False
    new_status = not current_status
    if new_status:
        try:
            asking_price = float(request.form.get("asking_price"))
            if asking_price <= 0:
                raise ValueError
        except (ValueError, TypeError):
            flash("Asking price must be a valid positive number.", "danger")
            return redirect(url_for("view_parcel", parcel_id=parcel_id))
        execute_query("UPDATE Parcels SET is_for_sale = %s, listing_price_inr = %s WHERE parcel_id = %s", (new_status, asking_price, parcel_id))
        flash(f"Parcel listed for sale at INR {float(asking_price):,.0f}.", "success")
    else:
        execute_query("UPDATE Parcels SET is_for_sale = %s, listing_price_inr = NULL WHERE parcel_id = %s", (new_status, parcel_id))
        flash("Parcel successfully removed from sale.", "info")
    return redirect(url_for("view_parcel", parcel_id=parcel_id))


@app.route("/create_option/<parcel_id>", methods=["POST"])
@login_required
def create_option(parcel_id):
    parcel = execute_query("SELECT owner_user_id FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel or parcel["owner_user_id"] != current_user.id:
        flash("Security Error: You can only create contracts for land you own.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    try:
        strike_price = float(request.form.get("strike_price"))
        premium = float(request.form.get("premium"))
        expiry_date_str = request.form.get("expiry_date")
        if strike_price <= 0 or premium <= 0:
            raise ValueError
        if datetime.strptime(expiry_date_str, "%Y-%m-%d").date() <= date.today():
            flash("Expiration date must be set to a future date.", "danger")
            return redirect(url_for("view_parcel", parcel_id=parcel_id))
    except (ValueError, TypeError, Exception):
        flash("Strike, Premium, and Date must be valid values.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    spam_check = execute_query("SELECT COUNT(*) as c FROM Options WHERE parcel_id = %s AND status = 'Open'", (parcel_id,), fetch_all=False)
    if spam_check and spam_check["c"] > 0:
        flash("You already have an active contract for this property. Cancel it to create a new one.", "warning")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    new_option_id = generate_unique_id("O", "Options", "option_id")
    if execute_query(
        "INSERT INTO Options (option_id, parcel_id, seller_user_id, strike_inr, premium_inr, expiry_date, status) VALUES (%s, %s, %s, %s, %s, %s, 'Open')",
        (new_option_id, parcel_id, current_user.id, strike_price, premium, expiry_date_str),
    ):
        flash("Success! Your Options Contract is now live on the market.", "success")
    else:
        flash("Database Error: Could not create the contract.", "danger")
    return redirect(url_for("view_parcel", parcel_id=parcel_id))


@app.route("/cancel_parcel_options/<parcel_id>", methods=["POST"])
@login_required
def cancel_parcel_options(parcel_id):
    if execute_query("UPDATE Options SET status = 'Cancelled by Owner' WHERE parcel_id = %s AND seller_user_id = %s AND status = 'Open'", (parcel_id, current_user.id)):
        flash("Your open contracts for this parcel have been successfully cancelled.", "info")
    return redirect(url_for("view_parcel", parcel_id=parcel_id))


@app.route("/buy_parcel/<parcel_id>", methods=["POST"])
@login_required
def buy_parcel(parcel_id):
    buyer_id = current_user.id
    parcel = execute_query("SELECT base_price_inr, listing_price_inr, owner_user_id, is_for_sale FROM Parcels WHERE parcel_id = %s", (parcel_id,), fetch_all=False)
    if not parcel:
        flash("Transaction failed: Asset not found.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    is_for_sale = True if parcel["is_for_sale"] in (1, "1", True, "True") else False
    if not is_for_sale or buyer_id == parcel["owner_user_id"]:
        flash("Transaction failed: Asset not available or already owned.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    price = parcel.get("listing_price_inr") if parcel.get("listing_price_inr") else parcel["base_price_inr"]
    if current_user.balance_cash < price:
        flash(f"Transaction failed: Insufficient liquid capital. You require {format_inr(price)}.", "danger")
        return redirect(url_for("view_parcel", parcel_id=parcel_id))
    queries = [
        ("UPDATE Parcels SET owner_user_id = %s, is_for_sale = FALSE, listing_price_inr = NULL WHERE parcel_id = %s AND is_for_sale = TRUE", (buyer_id, parcel_id)),
        ("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s", (price, buyer_id)),
        ("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (price, parcel["owner_user_id"])),
        ("INSERT INTO Price_History (parcel_id, record_date, price_inr) VALUES (%s, CURDATE(), %s) ON DUPLICATE KEY UPDATE price_inr = VALUES(price_inr)", (parcel_id, price)),
    ]
    if execute_query("SELECT 1 FROM Options WHERE parcel_id = %s AND status = 'Open'", (parcel_id,), fetch_all=False):
        queries.append(("UPDATE Options SET status = 'Cancelled (Asset Sold)' WHERE parcel_id = %s AND status = 'Open'", (parcel_id,)))
    if execute_transaction(queries):
        current_user.balance_cash -= price
        flash(f"Asset Acquired! {format_inr(price)} transferred to @{parcel.get('owner_name', parcel['owner_user_id'])}.", "success")
    else:
        flash("Transaction failed: Ledger desync or race condition detected.", "danger")
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
        where_clauses.append("(O.option_id LIKE %s OR P.parcel_id LIKE %s OR P.city LIKE %s OR U_Seller.username LIKE %s OR U_Buyer.username LIKE %s)")
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern, search_pattern])
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    count_res = execute_query(
        "SELECT COUNT(*) as total FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id JOIN Users U_Seller ON O.seller_user_id = U_Seller.user_id LEFT JOIN Users U_Buyer ON O.buyer_user_id = U_Buyer.user_id"
        + where_sql,
        tuple(params),
        fetch_all=False,
    )
    total = count_res["total"] if count_res else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = (min(page, total_pages) - 1) * per_page
    options = execute_query(base_sql + where_sql + " ORDER BY O.expiry_date ASC LIMIT %s OFFSET %s", tuple(params) + (per_page, offset), fetch_all=True)
    return render_template("options.html", options=options or [], status_filter=status_filter, search_query=search_query, page=page, total_pages=total_pages)


@app.route("/buy_option/<option_id>", methods=["POST"])
@login_required
def buy_option(option_id):
    buyer_id = current_user.id
    option = execute_query("SELECT premium_inr, seller_user_id, status FROM Options WHERE option_id = %s AND expiry_date >= CURDATE()", (option_id,), fetch_all=False)
    if not option or option["status"] != "Open" or buyer_id == option["seller_user_id"]:
        flash("Trade failed: Contract unavailable or expired.", "danger")
        return redirect(url_for("list_options"))
    premium = option["premium_inr"]
    if current_user.balance_cash < premium:
        flash(f"Trade failed: Insufficient liquid capital. You require {format_inr(premium)}.", "danger")
        return redirect(url_for("list_options"))
    queries = [
        ("UPDATE Options SET status = 'Traded', buyer_user_id = %s WHERE option_id = %s AND status = 'Open'", (buyer_id, option_id)),
        ("UPDATE Users SET balance_cash = balance_cash - %s WHERE user_id = %s", (premium, buyer_id)),
        ("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (premium, option["seller_user_id"])),
        ("INSERT INTO Trades (trade_id, option_id, trade_date, trade_price_inr, quantity, buyer_user_id, seller_user_id) VALUES (%s, %s, CURDATE(), %s, 1, %s, %s)", (generate_unique_id("T", "Trades", "trade_id"), option_id, premium, buyer_id, option["seller_user_id"])),
    ]
    if execute_transaction(queries):
        current_user.balance_cash -= premium
        flash(f"Derivative Contract Executed! Premium paid: {format_inr(premium)}", "success")
    else:
        flash("Trade failed: Ledger desync or race condition detected.", "danger")
    return redirect(url_for("list_options"))


@app.route("/trades")
@login_required
def list_trades():
    search_query = request.args.get("search", "").strip()
    page = max(1, request.args.get("page", 1, type=int))
    per_page = 50
    base_sql = "SELECT * FROM (SELECT T.trade_id, T.trade_date, O.option_id, P.address, P.city, T.trade_price_inr, U_Buyer.username as buyer_name, U_Seller.username as seller_name FROM Trades T JOIN Options O ON T.option_id = O.option_id JOIN Parcels P ON O.parcel_id = P.parcel_id JOIN Users U_Buyer ON T.buyer_user_id = U_Buyer.user_id JOIN Users U_Seller ON T.seller_user_id = U_Seller.user_id UNION ALL SELECT CONCAT('LND-', PH.parcel_id, '-', DATE_FORMAT(PH.record_date, '%Y%m%d')), PH.record_date, PH.parcel_id, P.address, P.city, PH.price_inr, 'Market Purchase', 'Market Sale' FROM Price_History PH JOIN Parcels P ON PH.parcel_id = P.parcel_id) AS MasterTrades"
    params, where_clauses = [], []
    if search_query:
        where_clauses.append("(trade_id LIKE %s OR option_id LIKE %s OR city LIKE %s OR buyer_name LIKE %s OR seller_name LIKE %s)")
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern, search_pattern, search_pattern])
    where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    count_sql = "SELECT COUNT(*) as total FROM (" + base_sql + where_sql + ") AS CountQuery"
    total_res = execute_query(count_sql, tuple(params), fetch_all=False)
    total = total_res["total"] if total_res else 0
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = (min(page, total_pages) - 1) * per_page
    trades = execute_query(base_sql + where_sql + " ORDER BY trade_date DESC LIMIT %s OFFSET %s", tuple(params) + (per_page, offset), fetch_all=True)
    return render_template("trades.html", trades=trades or [], search_query=search_query, page=page, total_pages=total_pages)


@app.route("/settle_options", methods=["GET", "POST"])
@login_required
def settle_options():
    if current_user.role != "admin":
        flash("Access Denied: Only Admins can run settlement.", "danger")
        return redirect(url_for("index"))
    execute_query("UPDATE Options SET status = 'Expired (Unsold)' WHERE status = 'Open' AND expiry_date < CURDATE()")
    expired_options = execute_query("SELECT O.option_id, O.parcel_id, O.strike_inr, O.buyer_user_id, O.seller_user_id, P.base_price_inr FROM Options O JOIN Parcels P ON O.parcel_id = P.parcel_id WHERE O.status = 'Traded' AND O.expiry_date <= CURDATE()", fetch_all=True)
    settlement_results = []
    success_count = 0
    if not expired_options:
        flash("No expired options found.", "info")
        return render_template("settlement.html", results=settlement_results)
    for option in expired_options:
        history_data = execute_query("SELECT price_inr FROM Price_History WHERE parcel_id = %s ORDER BY record_date DESC LIMIT 3", (option["parcel_id"],), fetch_all=True)
        settlement_price = sum(h["price_inr"] for h in history_data) / len(history_data) if history_data else option["base_price_inr"]
        strike = option["strike_inr"]
        payout = settlement_price - strike if settlement_price > strike else 0
        status_update = "Expired ITM" if settlement_price > strike else "Expired OTM"
        queries = []
        if payout > 0:
            queries.extend(
                [
                    ("UPDATE Users SET balance_cash = GREATEST(0, balance_cash - %s) WHERE user_id = %s", (payout, option["seller_user_id"])),
                    ("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (payout, option["buyer_user_id"])),
                ]
            )
        queries.append(("UPDATE Options SET status = %s WHERE option_id = %s", (status_update, option["option_id"])))
        if execute_transaction(queries):
            success_count += 1
            settlement_results.append(
                {"option_id": option["option_id"], "settlement_price": settlement_price, "strike": strike, "payout": payout, "result": status_update}
            )
    if success_count > 0:
        flash(f"Settlement complete. {success_count} options processed.", "success")
    else:
        flash("Settlement encountered errors on some options.", "warning")
    return render_template("settlement.html", results=settlement_results)


@app.route("/deposit", methods=["POST"])
@login_required
def deposit_funds():
    try:
        amount = float(request.form.get("amount", 0))
    except (ValueError, TypeError):
        amount = 0
    if 0 < amount <= 10000000:
        if current_user.balance_cash + amount > 10000000000:
            flash("Deposit failed: Your wallet has reached the maximum permitted limit of 10 Billion INR.", "danger")
        else:
            if execute_query("UPDATE Users SET balance_cash = balance_cash + %s WHERE user_id = %s", (amount, current_user.id)):
                flash(f"Deposited INR {amount:,.2f}.", "success")
                current_user.balance_cash += amount
            else:
                flash("Database error.", "danger")
    else:
        flash("Invalid amount. Deposits must be between ₹1 and ₹10,000,000.", "danger")
    return redirect(url_for("view_user", user_id=current_user.id))


@app.route("/change_password", methods=["POST"])
@login_required
def change_password():
    if request.form.get("new_password") != request.form.get("confirm_new_password"):
        flash("Passwords do not match.", "danger")
        return redirect(url_for("view_user", user_id=current_user.id))
    user_data = execute_query("SELECT password_hash FROM Users WHERE user_id = %s", (current_user.id,), fetch_all=False)
    if not user_data or not check_password_hash(user_data["password_hash"], request.form.get("current_password")):
        flash("Incorrect password.", "danger")
        return redirect(url_for("view_user", user_id=current_user.id))
    if execute_query("UPDATE Users SET password_hash = %s WHERE user_id = %s", (generate_password_hash(request.form.get("new_password")), current_user.id)):
        flash("Password updated.", "success")
    else:
        flash("Database error.", "danger")
    return redirect(url_for("view_user", user_id=current_user.id))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=os.environ.get("FLASK_DEBUG", "False").lower() == "true")
