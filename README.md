# 🗺️ Land Options Trading & Valuation System

**A Full-Stack Real Estate FinTech Platform** *Developed as a Software Engineering Semester Project | PDEU CSE*

## 🚀 Live Production Environment
**Deployment URL:** [https://landoptionstrading.onrender.com](https://landoptionstrading.onrender.com)  
**System Status:** 🟢 Operational (Monitored via UptimeRobot)

---

## 🏗️ Software Architecture & Design
This project follows the **MVC (Model-View-Controller)** architectural pattern to ensure high maintainability, scalability, and separation of concerns:

* **Model:** Relational database schema hosted on MySQL, managed via a robust abstraction layer in `db.py`.
* **View:** Responsive, accessible front-end built with HTML5, Bootstrap 5, and JavaScript (Leaflet GIS / Chart.js).
* **Controller:** Flask-based RESTful API in `app.py` managing authentication, core business logic, and server-side routing.

### 🧩 Modular Design
To adhere to the SE principle of **High Cohesion and Low Coupling**, the system is divided into specialized modules:
- `valuation.py`: Dedicated analytics engine using `scikit-learn` for predictive price forecasting.
- `db.py`: Standalone database connector handling connection security and transaction integrity.
- `app.py`: The core orchestrator managing system state and API endpoints.

---

## 🛠️ Tech Stack & Engineering Tools
- **Backend:** Python 3.x (Flask Framework)
- **Database:** MySQL (Cloud-hosted on Aiven)
- **Frontend Engine:** Jinja2 Templates, Bootstrap 5, Leaflet GIS, Chart.js
- **Predictive Modeling:** Scikit-learn (Linear Regression)
- **Infrastructure:** Render (PaaS), GitHub (VCS), UptimeRobot (Monitoring)

---

## 💎 Key Software Engineering Features

### 1. Robust Data Integrity (ACID Compliance)
Implemented **Atomic Transactions** for all financial exchanges. By bundling land transfers and cash deductions into a single unit of work, the system prevents data corruption or "partial states" during server interruptions.

### 2. Defensive Programming & Fault Tolerance
The system implements strict error-handling protocols:
- **API Resilience:** The GIS engine uses defensive casting (`float()`) and try-catch blocks to prevent UI crashes if the database contains malformed or null values.
- **Concurrency Control:** Utilizes row-level locking logic in SQL to mitigate **Race Conditions** during high-concurrency trade scenarios (preventing double-buying).

### 3. Responsive UI/UX Design
Adheres to modern web accessibility and usability standards:
- **Mobile-First Design:** Charts and maps include touch-aware wrappers and scroll-lock logic for seamless mobile navigation.
- **Localization:** Real-time formatting of financial data using the **Indian Numbering System (en-IN)** (e.g., ₹1,00,00,000).
- **Persistent Theming:** Client-side Dark/Light mode toggle utilizing `LocalStorage` for session persistence.

### 4. GIS-Driven Discovery Engine
A custom-built map engine providing high transparency into the land market:
- **Spatial Filtering:** Dynamic city-based parcel filtering.
- **Visual Categorization:** Color-coded markers distinguish between Owned Assets (Blue), For Sale (Green), Active Options (Purple), and General Land (Grey).

---

## 📈 Analytical Capabilities
The system leverages a **Predictive Analytics Module** that:
1.  Aggregates historical transaction data from the `Price_History` table.
2.  Applies a **Linear Regression** model to calculate a 30-day price forecast.
3.  Computes **Moving Averages** to smoothen market volatility for end-users.

---

## ⚙️ Local Development Setup
1.  **Clone the Project:**
    ```bash
    git clone [https://github.com/parthmistry903/land-options-trading.git](https://github.com/parthmistry903/land-options-trading.git)
    ```
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Environment Configuration:**
    Set `FLASK_SECRET_KEY` and database credentials in your environment variables.
4.  **Execution:**
    ```bash
    python app.py
    ```

---

## 👤 Lead Engineer
- **Parth Mistry** (Computer Science & Engineering, PDEU)
