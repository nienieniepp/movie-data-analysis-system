from flask import Flask, render_template, request, redirect, url_for, flash, session
import sqlite3
import pandas as pd
import os
import hashlib
from functools import wraps
from datetime import datetime
# ================== Flask 设置 ==================
app = Flask(__name__, template_folder="templates")
app.secret_key = "change-this-secret-key"  # 只设一次密钥

DB_PATH = "movies_acms.db"
MOVIES_CSV_PATH = "Movies_dataset.csv"


# ================== 数据库工具 ==================

def get_connection():
    """获取 SQLite 连接，并返回 Row 风格结果，模板中可用 row['title']。"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """创建基本表结构。"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    # 添加 users 表（用于用户信息）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            email TEXT UNIQUE,
            is_admin INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # 已有的其他表（movies、templates 等）
    cur.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            original_language TEXT,
            release_date TEXT,
            release_year INTEGER,
            popularity REAL,
            vote_average REAL,
            vote_count INTEGER,
            overview TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            topic TEXT,
            description TEXT,
            content_text TEXT NOT NULL,
            content_markdown TEXT,
            content_html TEXT,
            active INTEGER DEFAULT 1
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS saved_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            sql_text TEXT NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS generated_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER,
            generated_at TEXT,
            format TEXT,
            parameters TEXT,
            content TEXT NOT NULL,
            FOREIGN KEY (template_id) REFERENCES templates(id)
        );
    """)

    # 所有表创建完成后再提交并关闭连接
    conn.commit()
    conn.close()

def import_movies_from_csv_pandas():
    """从 CSV 导入电影数据（会清空原 movies 表）。"""
    if not os.path.exists(MOVIES_CSV_PATH):
        return False, f"CSV file not found: {MOVIES_CSV_PATH}"

    try:
        df = pd.read_csv(MOVIES_CSV_PATH)
    except Exception as e:
        return False, f"Error reading CSV: {e}"

    def parse_date(x):
        if pd.isna(x):
            return pd.NaT
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                return pd.to_datetime(x, format=fmt)
            except Exception:
                continue
        return pd.NaT

    if "release_date" not in df.columns:
        return False, "Column 'release_date' not found in CSV."

    df["release_date"] = df["release_date"].apply(parse_date)
    df["release_year"] = df["release_date"].dt.year

    numeric_cols = [c for c in ["popularity", "vote_average", "vote_count"] if c in df.columns]
    if numeric_cols:
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    df["release_date"] = df["release_date"].dt.strftime("%Y-%m-%d")

    required_cols = ["title", "original_language", "release_date",
                     "release_year", "popularity", "vote_average",
                     "vote_count", "overview"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[required_cols]

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM movies;")
    conn.commit()

    df.to_sql("movies", conn, if_exists="append", index=False)
    conn.close()
    return True, f"Imported {len(df)} movies from CSV."


# ================== 默认模板 & 查询 ==================

DEFAULT_TEMPLATES = [
    {
        "name": "Hot Top N Movies",
        "topic": "top_n_popular",
        "description": "Hot movies in a selected time range.",
        "content_text": "",
        "content_markdown": "",
        "content_html": (
            "<h2>Top {n} Most Popular Movies</h2>"
            "<p>Time range: {time_desc}</p>"
            "<p>Total movies in range: <strong>{total_movies}</strong></p>"
            "<p>Average rating: <strong>{avg_rating:.2f}</strong> / 10, "
            "Average popularity: <strong>{avg_popularity:.2f}</strong></p>"
            "<h3>Top Titles</h3>"
            "<pre>{movie_list}</pre>"
        ),
    },
    {
        "name": "Yearly Hot Summary",
        "topic": "year_top_popularity",
        "description": "Yearly hot movie summary.",
        "content_text": "",
        "content_markdown": "",
        "content_html": (
            "<h2>Yearly Hot Movies Summary – {year}</h2>"
            "<p>Movies recorded: <strong>{movie_count}</strong></p>"
            "<p>Average rating: <strong>{avg_rating:.2f}</strong> / 10, "
            "Average popularity: <strong>{avg_popularity:.2f}</strong></p>"
            "<h3>Top {n} Titles</h3>"
            "<pre>{top_n_list}</pre>"
        ),
    },
    {
        "name": "High Score Recommendation",
        "topic": "high_score_recommendation",
        "description": "High rating movies recommendation list.",
        "content_text": "",
        "content_markdown": "",
        "content_html": (
            "<h2>High-Score Recommendation List</h2>"
            "<p>Filters: min rating {min_rating:.1f}, min votes {min_votes}, "
            "language: {language_desc}</p>"
            "<p>Matched movies: <strong>{movie_count}</strong>, "
            "average rating <strong>{avg_rating:.2f}</strong> / 10</p>"
            "<h3>Recommended Titles</h3>"
            "<pre>{movie_list}</pre>"
        ),
    },
    {
        "name": "Hidden Gems",
        "topic": "hidden_gems",
        "description": "High rating but low popularity movies.",
        "content_text": "",
        "content_markdown": "",
        "content_html": (
            "<h2>Potential Hidden Gems</h2>"
            "<p>Filters: min rating {min_rating:.1f}, max popularity {max_popularity:.1f}</p>"
            "<p>Matched movies: <strong>{movie_count}</strong></p>"
            "<h3>Representative Titles</h3>"
            "<pre>{movie_list}</pre>"
        ),
    },
    {
        "name": "Language Structure",
        "topic": "language_structure",
        "description": "Movies count and averages by language.",
        "content_text": "",
        "content_markdown": "",
        "content_html": (
            "<h2>Language-Level Content Structure</h2>"
            "<p>Number of languages: <strong>{language_count}</strong></p>"
            "<h3>Breakdown</h3>"
            "<pre>{language_stats}</pre>"
        ),
    },
    {
        "name": "Time Window Performance",
        "topic": "time_window_performance",
        "description": "Performance of movies in a date range.",
        "content_text": "",
        "content_markdown": "",
        "content_html": (
            "<h2>Time Window Performance</h2>"
            "<p>Range: {start_date} ~ {end_date}</p>"
            "<p>Movies in range: <strong>{movie_count}</strong></p>"
            "<p>Average rating: <strong>{avg_rating:.2f}</strong> / 10, "
            "Average popularity: <strong>{avg_popularity:.2f}</strong></p>"
            "<h3>Top {n} Titles</h3>"
            "<pre>{top_n_list}</pre>"
        ),
    },
]


DEFAULT_QUERIES = [
    {
        "name": "Top 10 popular movies",
        "description": "Top 10 movies ordered by popularity.",
        "sql": """
            SELECT title, popularity, vote_average, vote_count
            FROM movies
            WHERE popularity IS NOT NULL
            ORDER BY popularity DESC
            LIMIT 10;
        """
    },
    {
        "name": "Average rating by language",
        "description": "Average rating and total votes per original language.",
        "sql": """
            SELECT original_language,
                   COUNT(*) AS movie_count,
                   AVG(vote_average) AS avg_rating,
                   AVG(popularity) AS avg_popularity,
                   SUM(vote_count) AS total_votes
            FROM movies
            GROUP BY original_language
            ORDER BY avg_rating DESC;
        """
    },
    {
        "name": "Top movie per year by popularity",
        "description": "Get the movie with the highest popularity for a given year.",
        "sql": """
            SELECT m.*
            FROM movies m
            WHERE m.release_year = ?
              AND m.popularity = (
                  SELECT MAX(popularity)
                  FROM movies
                  WHERE release_year = ?
              );
        """
    }
]



def insert_default_templates_and_queries():
    conn = get_connection()
    cur = conn.cursor()

    # templates
    cur.execute("SELECT COUNT(*) FROM templates;")
    count_t = cur.fetchone()[0]
    if count_t == 0:
        for t in DEFAULT_TEMPLATES:
            cur.execute("""
                INSERT INTO templates
                (name, topic, description, content_text, content_markdown, content_html, active)
                VALUES (?, ?, ?, ?, ?, ?, 1);
            """, (
                t["name"], t["topic"], t["description"],
                t["content_text"], t["content_markdown"], t["content_html"]
            ))

    # saved_queries
    cur.execute("SELECT COUNT(*) FROM saved_queries;")
    count_q = cur.fetchone()[0]
    if count_q == 0:
        for q in DEFAULT_QUERIES:
            cur.execute("""
                INSERT INTO saved_queries (name, description, sql_text)
                VALUES (?, ?, ?);
            """, (q["name"], q["description"], q["sql"]))

    conn.commit()
    conn.close()


def render_template_html(topic, **kwargs):
    """从 templates 表中取 HTML 模板并 format。"""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, content_html
        FROM templates
        WHERE topic = ? AND active = 1
        LIMIT 1;
    """, (topic,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None, "[ERROR] No active HTML template found.", None
    tpl_id = row["id"]
    html = row["content_html"]

    safe_kwargs = {}
    for k, v in kwargs.items():
        safe_kwargs[k] = "N/A" if v is None else v

    try:
        rendered = html.format(** safe_kwargs)
    except KeyError as e:
        missing = e.args[0]
        return None, f"[TEMPLATE ERROR] missing placeholder {missing}", None
    return tpl_id, rendered, None


def save_report(template_id, parameters, html_content):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO generated_reports (template_id, generated_at, format, parameters, content)
        VALUES (?, datetime('now','localtime'), 'html', ?, ?);
    """, (template_id, parameters, html_content))
    conn.commit()
    conn.close()


# ================== 登录验证装饰器 ==================
@app.route('/forgot-password')
def forgot_password():
    # 忘记密码功能逻辑
    return render_template('forgot_password.html')


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            flash('Please log in first to access this page', 'warning')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


# ================== 登录注册路由 ==================

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            'SELECT id, password_hash FROM users WHERE username = ?',
            (username,)
        )
        user = cur.fetchone()
        conn.close()

        if user and hashlib.sha256(password.encode()).hexdigest() == user['password_hash']:
            session['user_id'] = user['id']
            session['username'] = username
            next_page = request.args.get('next', url_for('index'))
            return redirect(next_page)
        else:
            flash('The username or password is incorrect', 'danger')
            return redirect(url_for('login'))

    return render_template('login.html')


@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        confirm = request.form.get('confirm')

        if not username or not password:
            flash('The username and password cannot be empty', 'danger')
            return redirect(url_for('register'))

        if password != confirm:
            flash('The two password entries are inconsistent', 'danger')
            return redirect(url_for('register'))

        # 密码加密存储
        password_hash = hashlib.sha256(password.encode()).hexdigest()

        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute(
                'INSERT INTO users (username, password_hash) VALUES (?, ?)',
                (username, password_hash)
            )
            conn.commit()
            flash('Registration successful. Please log in', 'success')
            return redirect(url_for('login'))
        except sqlite3.IntegrityError:
            flash('The username already exists', 'danger')
            return redirect(url_for('register'))
        finally:
            conn.close()

    return render_template('register.html')


@app.route('/logout')
def logout():
    session.pop('user_id', None)
    session.pop('username', None)
    flash('Logged out successfully', 'success')
    return redirect(url_for('login'))


# ================== 路由：主页 & 大类菜单 ==================

# 1）根路径：根据是否登录做跳转
@app.route("/")
def root():
    if 'user_id' in session:
        # 已登录 -> 去主菜单
        return redirect(url_for('index'))
    else:
        # 未登录 -> 去登录页
        return redirect(url_for('login'))

# 2）主菜单页面：需要登录
@app.route("/index")
@login_required
def index():
    return render_template("index.html")



# ================== A. 整体数据概况 ==================

@app.route("/overview")
@login_required
def overview():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM movies;")
    total_movies = cur.fetchone()["cnt"] or 0

    cur.execute("SELECT COUNT(DISTINCT original_language) AS lc FROM movies;")
    language_count = cur.fetchone()["lc"] or 0

    cur.execute("""
        SELECT AVG(vote_average) AS ar, AVG(popularity) AS ap
        FROM movies
        WHERE vote_average IS NOT NULL AND popularity IS NOT NULL;
    """)
    row = cur.fetchone()
    avg_rating = row["ar"] or 0
    avg_popularity = row["ap"] or 0

    cur.execute("""
        SELECT title, vote_average, popularity
        FROM movies
        ORDER BY popularity DESC
        LIMIT 5;
    """)
    top5 = cur.fetchall()
    conn.close()

    return render_template(
        "overview.html",
        total_movies=total_movies,
        language_count=language_count,
        avg_rating=avg_rating,
        avg_popularity=avg_popularity,
        top5=top5,
        report_text=None
    )


# ================== B. 热门影片统计与分析 ==================
@app.route("/hot")
@login_required
def hot_menu():
    conn = get_connection()
    cur = conn.cursor()

    # 1. 获取今年最热门影片（按 popularity 降序取第一）
    current_year = datetime.now().year  # 需要导入 datetime 模块
    cur.execute("""
                SELECT title
                FROM movies
                WHERE release_year = ?
                  AND popularity IS NOT NULL
                ORDER BY popularity DESC LIMIT 1;
                """, (current_year,))
    top_movie = cur.fetchone()
    top_movie_title = top_movie['title'] if top_movie else "暂无数据"

    # 2. 获取历史最高人气值
    cur.execute("SELECT MAX(popularity) AS max_pop FROM movies WHERE popularity IS NOT NULL;")
    max_pop_row = cur.fetchone()
    max_popularity = round(max_pop_row['max_pop'], 1) if max_pop_row['max_pop'] else 0

    # 3. 获取热门影片平均评分（可按 popularity 前30%筛选）
    cur.execute("""
                -- 步骤1：筛选有 popularity 值的影片并排序
                WITH ranked_movies AS (SELECT popularity
                                       FROM movies
                                       WHERE popularity IS NOT NULL
                                       ORDER BY popularity ASC -- 升序排序，方便取偏移量
                ),
                     -- 步骤2：计算总记录数
                     total_count AS (SELECT COUNT(*) AS cnt
                                     FROM ranked_movies)
                -- 步骤3：取 70% 位置的 popularity 作为阈值（OFFSET 从0开始）
                SELECT popularity
                FROM ranked_movies,
                     total_count LIMIT 1
                OFFSET (SELECT CASE WHEN cnt = 0 THEN 0 ELSE ROUND(cnt * 0.7) END FROM total_count);
                """)
    threshold_row = cur.fetchone()
    # 处理无数据的情况
    threshold = threshold_row['popularity'] if (threshold_row and threshold_row['popularity'] is not None) else 0

    # 3.2 用阈值筛选前 30% 热门影片，计算平均评分
    cur.execute("""
                SELECT AVG(vote_average) AS avg_rating
                FROM movies
                WHERE vote_average IS NOT NULL
                  AND popularity IS NOT NULL
                  AND popularity > ?; -- 使用上面计算的阈值
                """, (threshold,))  # 传递阈值作为参数
    avg_rating_row = cur.fetchone()
    avg_rating = round(avg_rating_row['avg_rating'], 1) if (
                avg_rating_row and avg_rating_row['avg_rating'] is not None) else 0

    # 4. 获取每月平均人气值（用于趋势图）
    monthly_avg = []
    for month in range(1, 13):
        cur.execute("""
                    SELECT AVG(popularity) AS avg_pop
                    FROM movies
                    WHERE popularity IS NOT NULL
                      AND strftime('%m', release_date) = ?
                    """, (f"{month:02d}",))
        row = cur.fetchone()
        monthly_avg.append(round(row['avg_pop'], 1) if row['avg_pop'] else 0)

    conn.close()

    # 将数据传递给模板
    return render_template(
        "hot_menu.html",
        top_movie_title=top_movie_title,
        max_popularity=max_popularity,
        avg_rating=avg_rating,
        monthly_avg=monthly_avg  # 用于趋势图
    )



@app.route("/hot/topn", methods=["GET", "POST"])
@login_required
def hot_topn():
    if request.method == "GET":
        return render_template("form_hot_topn.html")

    time_type = request.form.get("time_type", "all")
    year = request.form.get("year") or ""
    start_date = request.form.get("start_date") or ""
    end_date = request.form.get("end_date") or ""
    n_str = request.form.get("n") or "10"

    try:
        n = int(n_str)
    except ValueError:
        flash("Invalid N.", "danger")
        return redirect(url_for("hot_topn"))
    if n <= 0:
        flash("N must be > 0.", "danger")
        return redirect(url_for("hot_topn"))

    where = []
    params = []
    time_desc = "all movies"

    if time_type == "year" and year.isdigit():
        where.append("release_year = ?")
        params.append(int(year))
        time_desc = f"movies released in {year}"
    elif time_type == "range" and len(start_date) == 10 and len(end_date) == 10:
        where.append("release_date >= ?")
        where.append("release_date <= ?")
        params.extend([start_date, end_date])
        time_desc = f"movies released between {start_date} and {end_date}"

    where_sql = " AND ".join(where) if where else "1=1"

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) AS cnt, AVG(vote_average) AS ar, AVG(popularity) AS ap
        FROM movies
        WHERE {where_sql}
          AND popularity IS NOT NULL
          AND vote_average IS NOT NULL;
    """, params)
    row = cur.fetchone()
    total_movies = row["cnt"]
    if not total_movies:
        conn.close()
        flash("No movies found for selected range.", "warning")
        return redirect(url_for("hot_topn"))
    avg_rating = row["ar"] or 0
    avg_pop = row["ap"] or 0

    cur.execute(f"""
        SELECT title, popularity, vote_average
        FROM movies
        WHERE {where_sql}
          AND popularity IS NOT NULL
        ORDER BY popularity DESC
        LIMIT ?;
    """, params + [n])
    rows = cur.fetchall()
    conn.close()

    lines = []
    for i, r in enumerate(rows, start=1):
        title = r["title"]
        pop = r["popularity"]
        rating = r["vote_average"]
        pop_str = f"{pop:.1f}" if pop is not None else "N/A"
        rating_str = f"{rating:.1f}" if rating is not None else "N/A"
        lines.append(f"{i}. {title} | popularity {pop_str} | rating {rating_str}")
    movie_list = "\n".join(lines)

    tpl_id, html_report, err = render_template_html(
        "top_n_popular",
        n=len(rows),
        time_desc=time_desc,
        total_movies=total_movies,
        avg_rating=avg_rating,
        avg_popularity=avg_pop,
        movie_list=movie_list
    )
    if not tpl_id:
        flash(html_report, "danger")
        return redirect(url_for("hot_topn"))

    save_report(tpl_id, f"hot_topn | {time_desc} | N={n}", html_report)
    return render_template("report.html", title="Hot Movies Top N", report_html=html_report)


@app.route("/hot/year", methods=["GET", "POST"])
@login_required
def year_hot():
    if request.method == "GET":
        return render_template("form_year_hot.html")

    year_str = request.form.get("year") or ""
    n_str = request.form.get("n") or "10"
    if not year_str.isdigit():
        flash("Invalid year.", "danger")
        return redirect(url_for("year_hot"))
    try:
        n = int(n_str)
    except ValueError:
        flash("Invalid N.", "danger")
        return redirect(url_for("year_hot"))
    if n <= 0:
        flash("N must be > 0.", "danger")
        return redirect(url_for("year_hot"))

    year = int(year_str)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) AS cnt, AVG(vote_average) AS ar, AVG(popularity) AS ap
        FROM movies
        WHERE release_year = ?
          AND vote_average IS NOT NULL
          AND popularity IS NOT NULL;
    """, (year,))
    row = cur.fetchone()
    movie_count = row["cnt"]
    if not movie_count:
        conn.close()
        flash("No movies found for this year.", "warning")
        return redirect(url_for("year_hot"))
    avg_rating = row["ar"] or 0
    avg_pop = row["ap"] or 0

    cur.execute("""
        SELECT title, popularity, vote_average
        FROM movies
        WHERE release_year = ?
        ORDER BY popularity DESC
        LIMIT ?;
    """, (year, n))
    rows = cur.fetchall()
    conn.close()

    lines = []
    for i, r in enumerate(rows, start=1):
        title = r["title"]
        pop = r["popularity"]
        rating = r["vote_average"]
        pop_str = f"{pop:.1f}" if pop is not None else "N/A"
        rating_str = f"{rating:.1f}" if rating is not None else "N/A"
        lines.append(f"{i}. {title} | popularity {pop_str} | rating {rating_str}")
    top_n_list = "\n".join(lines)

    tpl_id, html_report, err = render_template_html(
        "year_top_popularity",
        year=year,
        movie_count=movie_count,
        avg_rating=avg_rating,
        avg_popularity=avg_pop,
        n=len(rows),
        top_n_list=top_n_list
    )
    if not tpl_id:
        flash(html_report, "danger")
        return redirect(url_for("year_hot"))

    save_report(tpl_id, f"year_hot | year={year} | N={n}", html_report)
    return render_template("report.html", title="Yearly Hot Movies Summary", report_html=html_report)


# ================== C. 推荐与选片 ==================
@app.route("/recommend")
@login_required
def rec_menu():
    conn = get_connection()
    cur = conn.cursor()
    return render_template(
        "rec_menu.html",
    )


@app.route("/potential", methods=["GET", "POST"])
@login_required
def potential():
    # GET：显示筛选表单
    if request.method == "GET":
        # 这里用的是你之前 C2 用的表单模板名字，
        # 如果你叫的是别的名字（比如 potential_form.html），改成对应的即可。
        return render_template("form_hidden_gems.html")

    # POST：根据表单参数生成“潜力影片”英文报告
    min_rating_str = request.form.get("min_rating") or "8.0"
    max_pop_str = request.form.get("max_popularity") or "200"

    try:
        min_rating = float(min_rating_str)
    except ValueError:
        min_rating = 8.0

    try:
        max_popularity = float(max_pop_str)
    except ValueError:
        max_popularity = 200.0

    conn = get_connection()
    cur = conn.cursor()

    # 1. 汇总统计：符合条件的影片数量 & 平均评分
    cur.execute("""
        SELECT COUNT(*) AS cnt, AVG(vote_average) AS ar
        FROM movies
        WHERE vote_average >= ?
          AND popularity IS NOT NULL
          AND popularity <= ?;
    """, (min_rating, max_popularity))
    row = cur.fetchone()
    movie_count = row["cnt"]
    if not movie_count:
        conn.close()
        flash("Under the current conditions, no potential films have been found (high scores but low popularity)", "warning")
        return redirect(url_for("potential"))

    avg_rating = row["ar"] or 0

    # 2. 取代表影片列表（Top 50）
    cur.execute("""
        SELECT title, vote_average, vote_count, popularity
        FROM movies
        WHERE vote_average >= ?
          AND popularity IS NOT NULL
          AND popularity <= ?
        ORDER BY vote_average DESC, vote_count DESC
        LIMIT 50;
    """, (min_rating, max_popularity))
    rows = cur.fetchall()
    conn.close()

    # 3. 组装英文列表文本 movie_list，给模板用
    lines = []
    for i, r in enumerate(rows, start=1):
        title = r["title"]
        rating = r["vote_average"]
        votes = r["vote_count"]
        pop = r["popularity"]

        rating_str = f"{rating:.1f}" if rating is not None else "N/A"
        pop_str = f"{pop:.1f}" if pop is not None else "N/A"
        lines.append(
            f"{i}. {title} | rating {rating_str} | votes {votes} | popularity {pop_str}"
        )
    movie_list = "\n".join(lines)

    # 4. 用 topic='hidden_gems' 的英文 HTML 模板生成报告
    tpl_id, html_report, err = render_template_html(
        "hidden_gems",
        min_rating=min_rating,
        max_popularity=max_popularity,
        movie_count=movie_count,
        movie_list=movie_list
    )

    # render_template_html 约定：tpl_id 为 None 时，html_report 里是错误信息
    if not tpl_id:
        flash(html_report, "danger")
        return redirect(url_for("potential"))

    # 5. 保存报告到 generated_reports 表
    param_desc = f"hidden_gems | rating>={min_rating} | pop<={max_popularity}"
    save_report(tpl_id, param_desc, html_report)

    # 6. 在网页上展示英文报告
    return render_template(
        "report.html",
        title="Potential Hidden Gems",
        report_html=html_report
    )




@app.route('/movie-search', methods=['GET', 'POST'])
def movie_search():
    if request.method == 'GET':
        return render_template('form_movie_search.html', params={})

    # 处理查询参数
    params = {
        'title_keyword': request.form.get('title_keyword', '').strip(),
        'language': request.form.get('language', '').strip(),
        'release_year': request.form.get('release_year', '').strip(),
        'min_rating': float(request.form.get('min_rating', 0)),
        'max_rating': float(request.form.get('max_rating', 10)),
        'min_popularity': float(request.form.get('min_popularity', 0)),
        'limit': int(request.form.get('limit', 20))
    }

    # 构建查询条件
    query_conditions = []
    query_params = []

    if params['title_keyword']:
        query_conditions.append("title LIKE ?")
        query_params.append(f"%{params['title_keyword']}%")

    if params['language']:
        query_conditions.append("original_language = ?")
        query_params.append(params['language'])

    if params['release_year']:
        query_conditions.append("release_year = ?")
        query_params.append(params['release_year'])

    query_conditions.append("vote_average BETWEEN ? AND ?")
    query_params.extend([params['min_rating'], params['max_rating']])

    query_conditions.append("popularity >= ?")
    query_params.append(params['min_popularity'])

    # 执行查询
    conn = get_connection()
    cur = conn.cursor()

    # 基础查询
    where_clause = "WHERE " + " AND ".join(query_conditions) if query_conditions else ""
    sql = f"""
        SELECT title, original_language, release_year, vote_average, popularity, vote_count
        FROM movies
        {where_clause}
        ORDER BY popularity DESC
        LIMIT ?
    """
    query_params.append(params['limit'])
    cur.execute(sql, query_params)
    movies = [dict(row) for row in cur.fetchall()]

    # 统计信息
    stats = None
    if query_conditions:
        stats_sql = f"""
            SELECT COUNT(*) as movie_count,
                   AVG(vote_average) as avg_rating,
                   AVG(popularity) as avg_popularity
            FROM movies
            {where_clause}
        """
        cur.execute(stats_sql, query_params[:-1])  # 排除limit参数
        result= cur.fetchone()
        stats= dict(result) if result else None

    conn.close()

    return render_template(
        'movie_search_results.html',
        params=params,
        movies=movies,
        stats=stats
    )

@app.route("/recommend/highscore", methods=["GET", "POST"])
@login_required
def high_rated():
    if request.method == "GET":
        return render_template("form_highscore.html")

    min_rating_str = request.form.get("min_rating") or "8.0"
    min_votes_str = request.form.get("min_votes") or "50"
    lang = request.form.get("lang") or ""

    try:
        min_rating = float(min_rating_str)
    except ValueError:
        min_rating = 8.0
    try:
        min_votes = int(min_votes_str)
    except ValueError:
        min_votes = 50

    where = ["vote_average >= ?", "vote_count >= ?"]
    params = [min_rating, min_votes]
    language_desc = "All languages"
    if lang:
        where.append("original_language = ?")
        params.append(lang)
        language_desc = f"Only '{lang}'"
    where_sql = " AND ".join(where)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) AS cnt, AVG(vote_average) AS ar
        FROM movies
        WHERE {where_sql};
    """, params)
    row = cur.fetchone()
    movie_count = row["cnt"]
    if not movie_count:
        conn.close()
        flash("No movies match the filters.", "warning")
        return redirect(url_for("high_rated"))
    avg_rating = row["ar"] or 0

    cur.execute(f"""
        SELECT title, vote_average, vote_count, popularity
        FROM movies
        WHERE {where_sql}
        ORDER BY vote_average DESC, vote_count DESC
        LIMIT 50;
    """, params)
    rows = cur.fetchall()
    conn.close()

    lines = []
    for i, r in enumerate(rows, start=1):
        title = r["title"]
        rating = r["vote_average"]
        votes = r["vote_count"]
        pop = r["popularity"]
        rating_str = f"{rating:.1f}" if rating is not None else "N/A"
        pop_str = f"{pop:.1f}" if pop is not None else "N/A"
        lines.append(f"{i}. {title} | rating {rating_str} | votes {votes} | popularity {pop_str}")
    movie_list = "\n".join(lines)

    tpl_id, html_report, err = render_template_html(
        "high_score_recommendation",
        min_rating=min_rating,
        min_votes=min_votes,
        language_desc=language_desc,
        movie_count=movie_count,
        avg_rating=avg_rating,
        movie_list=movie_list
    )
    if not tpl_id:
        flash(html_report, "danger")
        return redirect(url_for("high_rated"))

    save_report(tpl_id, f"high_rated | min_rating={min_rating} | min_votes={min_votes} | lang={lang}", html_report)
    return render_template("report.html", title="High-Score Recommendation", report_html=html_report)


# ================== D. 内容结构与时段表现 ==================

@app.route("/structure")
@login_required
def structure_menu():
    return render_template("structure_menu.html")


@app.route("/structure/language")
@login_required
def language_stats():
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            original_language,
            COUNT(*) AS movie_count,
            AVG(vote_average) AS avg_rating,
            AVG(popularity) AS avg_popularity,
            SUM(vote_count) AS total_votes
        FROM movies
        GROUP BY original_language
        ORDER BY movie_count DESC;
    """)
    stats = cur.fetchall()
    
    language_count = len(stats)
    
    lines = []
    for row in stats:
        lang = row['original_language'] or 'N/A'
        count = row['movie_count']
        rating = row['avg_rating']
        pop = row['avg_popularity']
        votes = row['total_votes'] or 0
        
        rating_str = f"{rating:.2f}" if rating is not None else "N/A"
        pop_str = f"{pop:.2f}" if pop is not None else "N/A"
        lines.append(f"{lang}: {count} movies | avg rating {rating_str} | avg popularity {pop_str} | total votes {votes}")
    language_stats_text = "\n".join(lines)
    
    tpl_id, html_report, err = render_template_html(
        "language_structure",
        language_count=language_count,
        language_stats=language_stats_text
    )
    conn.close()
    
    if err:
        flash(err, "danger")
        return redirect(url_for("structure_menu"))
    
    return render_template("language_stats.html", stats=stats, report_html=html_report)


@app.route("/structure/period", methods=["GET", "POST"])
@login_required
def period_stats():
    if request.method == "GET":
        return render_template("form_time_window.html")
    
    start_date = request.form.get("start_date")
    end_date = request.form.get("end_date")
    n_str = request.form.get("n", "10")
    
    if not start_date or not end_date:
        flash("Please enter the complete date range", "danger")
        return redirect(url_for("period_stats"))
    
    try:
        n = int(n_str)
        if n <= 0:
            raise ValueError
    except ValueError:
        flash("The number of invalid Top N", "danger")
        return redirect(url_for("period_stats"))
    
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            COUNT(*) AS movie_count,
            AVG(vote_average) AS avg_rating,
            AVG(popularity) AS avg_popularity
        FROM movies
        WHERE release_date BETWEEN ? AND ?;
    """, (start_date, end_date))
    summary = cur.fetchone()
    
    if not summary['movie_count']:
        conn.close()
        flash("There is no movie data during this period", "warning")
        return redirect(url_for("period_stats"))
    
    cur.execute("""
        SELECT title, vote_average, popularity, release_date
        FROM movies
        WHERE release_date BETWEEN ? AND ?
        ORDER BY popularity DESC
        LIMIT ?;
    """, (start_date, end_date, n))
    movies = cur.fetchall()
    conn.close()
    
    lines = []
    for i, m in enumerate(movies, 1):
        title = m['title']
        rating = m['vote_average']
        pop = m['popularity']
        date = m['release_date']
        
        rating_str = f"{rating:.1f}" if rating else "N/A"
        pop_str = f"{pop:.1f}" if pop else "N/A"
        lines.append(f"{i}. {title} | rating {rating_str} | popularity {pop_str} | released {date}")
    top_n_list = "\n".join(lines)
    
    tpl_id, html_report, err = render_template_html(
        "time_window_performance",
        start_date=start_date,
        end_date=end_date,
        movie_count=summary['movie_count'],
        avg_rating=summary['avg_rating'] or 0,
        avg_popularity=summary['avg_popularity'] or 0,
        n=len(movies),
        top_n_list=top_n_list
    )
    
    if err:
        flash(err, "danger")
        return redirect(url_for("period_stats"))
    
    save_report(tpl_id, f"period_stats | {start_date} to {end_date} | N={n}", html_report)
    return render_template("report.html", title="Time Window Performance", report_html=html_report)


# ================== E. 报告与SQL管理 ==================

@app.route("/reports")
@login_required
def reports_menu():
    return render_template("reports_menu.html")


@app.route("/reports/list")
@login_required
def reports_list():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, t.name as template_name
        FROM generated_reports r
        LEFT JOIN templates t ON r.template_id = t.id
        ORDER BY r.generated_at DESC;
    """)
    reports = cur.fetchall()
    conn.close()
    return render_template("reports_list.html", reports=reports)


@app.route("/reports/detail/<int:report_id>")
@login_required
def report_detail(report_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT r.*, t.name as template_name
        FROM generated_reports r
        LEFT JOIN templates t ON r.template_id = t.id
        WHERE r.id = ?;
    """, (report_id,))
    report = cur.fetchone()
    conn.close()
    
    if not report:
        flash("The report does not exist.", "danger")
        return redirect(url_for("reports_list"))
    
    return render_template("report_detail.html", report=report)


@app.route("/reports/templates")
@login_required
def templates_list():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM templates ORDER BY id;")
    templates = cur.fetchall()
    conn.close()
    return render_template("templates_list.html", templates=templates)


@app.route("/sql")
@login_required
def sql_list():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM saved_queries ORDER BY id;")
    queries = cur.fetchall()
    conn.close()
    return render_template("sql_list.html", queries=queries)


@app.route("/sql/run/<int:query_id>", methods=["GET", "POST"])
@login_required
def sql_run(query_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM saved_queries WHERE id = ?;", (query_id,))
    query = cur.fetchone()
    
    if not query:
        conn.close()
        flash("The query does not exist.", "danger")
        return redirect(url_for("sql_list"))
    
    error = None
    rows = None
    col_names = None
    
    if request.method == "POST":
        param = request.form.get("param", "")
        try:
            sql_text = query['sql_text'].strip()
            if "?" in sql_text:
                cur.execute(sql_text, (param,))
            else:
                cur.execute(sql_text)
            
            col_names = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        except Exception as e:
            error = str(e)
    
    conn.close()
    return render_template(
        "sql_result.html",
        name=query['name'],
        sql_text=query['sql_text'],
        error=error,
        rows=rows,
        col_names=col_names
    )


# ================== F. 系统维护 ==================

@app.route("/admin")
@login_required
def admin_menu():
    return render_template("admin_menu.html")

#新增修改模板处
@app.route("/admin/templates/edit/<int:template_id>", methods=["GET", "POST"])
@login_required
def edit_template(template_id):
    conn = get_connection()
    cur = conn.cursor()

    # 获取模板信息
    cur.execute("SELECT * FROM templates WHERE id = ?;", (template_id,))
    template = cur.fetchone()
    if not template:
        conn.close()
        flash("The template does not exist.", "danger")
        return redirect(url_for("templates_list"))

    if request.method == "POST":
        # 获取表单提交的修改内容
        name = request.form.get("name")
        topic = request.form.get("topic")
        description = request.form.get("description")
        content_html = request.form.get("content_html")
        active = 1 if request.form.get("active") else 0

        # 验证必填字段
        if not name or not topic or not content_html:
            flash("Names, identifiers, and HTML content cannot be empty", "danger")
            conn.close()
            return render_template("form_edit_template.html", template=template)

        # 更新数据库
        cur.execute("""
                    UPDATE templates
                    SET name=?,
                        topic=?,
                        description=?,
                        content_html=?,
                        active=?
                    WHERE id = ?;
                    """, (name, topic, description, content_html, active, template_id))
        conn.commit()
        conn.close()

        flash("The template has been modified successfully.", "success")
        return redirect(url_for("templates_list"))

    conn.close()
    return render_template("form_edit_template.html", template=template)

@app.route("/admin/init")
@login_required
def admin_init():
    try:
        init_db()
        flash("The database structure has been initialized successfully", "success")
    except Exception as e:
        flash(f"Initialization failed: {str(e)}", "danger")
    return render_template("admin_result.html", message="The database structure initialization has been completed")


@app.route("/admin/import")
@login_required
def admin_import():
    success, msg = import_movies_from_csv_pandas()
    if success:
        flash(msg, "success")
    else:
        flash(msg, "danger")
    return render_template("admin_result.html", message=msg)


@app.route("/admin/insert_defaults")
@login_required
def admin_insert_defaults():
    try:
        insert_default_templates_and_queries()
        msg = "The default template and query have been inserted"
        flash(msg, "success")
    except Exception as e:
        msg = f"Insertion failed: {str(e)}"
        flash(msg, "danger")
    return render_template("admin_result.html", message=msg)

@app.route('/admin/bulk-import', methods=['GET', 'POST'])
def admin_bulk_import():
    """批量导入电影数据（追加模式）"""
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            return render_template('form_bulk_import.html',
                                   message='The uploaded file was not found',
                                   success=False)

        file = request.files['csv_file']
        if file.filename == '':
            return render_template('form_bulk_import.html',
                                   message='Unselected file',
                                   success=False)

        if not file.filename.endswith('.csv'):
            return render_template('form_bulk_import.html',
                                   message='Please upload the file in CSV format',
                                   success=False)

        try:
            # 读取CSV文件
            df = pd.read_csv(file.stream)

            # 数据处理（复用现有逻辑但不清除原有数据）
            def parse_date(x):
                if pd.isna(x):
                    return pd.NaT
                for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
                    try:
                        return pd.to_datetime(x, format=fmt)
                    except Exception:
                        continue
                return pd.NaT

            if "release_date" in df.columns:
                df["release_date"] = df["release_date"].apply(parse_date)
                df["release_year"] = df["release_date"].dt.year
            else:
                df["release_date"] = pd.NaT
                df["release_year"] = pd.NA

            # 处理数值列
            numeric_cols = [c for c in ["popularity", "vote_average", "vote_count"] if c in df.columns]
            if numeric_cols:
                df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

            # 格式化日期
            df["release_date"] = df["release_date"].dt.strftime("%Y-%m-%d")

            # 确保必要列存在
            required_cols = ["title", "original_language", "release_date",
                             "release_year", "popularity", "vote_average",
                             "vote_count", "overview"]
            for col in required_cols:
                if col not in df.columns:
                    df[col] = pd.NA

            df = df[required_cols]

            # 追加到数据库（不清除原有数据）
            conn = get_connection()
            df.to_sql("movies", conn, if_exists="append", index=False)
            conn.close()

            return render_template('form_bulk_import.html',
                                   message=f'Successful import {len(df)} film data',
                                   success=True)
        except Exception as e:
            return render_template('form_bulk_import.html',
                                   message=f'Import failed: {str(e)}',
                                   success=False)

    return render_template('form_bulk_import.html')

@app.route("/admin/add_movie", methods=["GET", "POST"])
@login_required
def admin_add_movie():
    if request.method == "POST":
        title = request.form.get("title")
        lang = request.form.get("lang")
        release_date = request.form.get("release_date")
        popularity = request.form.get("popularity")
        vote_average = request.form.get("vote_average")
        vote_count = request.form.get("vote_count")
        overview = request.form.get("overview")
        
        if not title:
            flash("The title of the film cannot be empty", "danger")
            return render_template("form_add_movie.html")
        
        # 处理数字字段
        def to_float(s):
            if s and s.strip():
                return float(s)
            return 0.0

        def to_int(s):
            if s and s.strip():
                return int(s)
            return 0
        
        def to_int(s):
            return int(s) if s and s.strip() else None
        
        popularity = to_float(popularity)
        vote_average = to_float(vote_average)
        vote_count = to_int(vote_count)
        
        # 提取年份
        release_year = None
        if release_date and len(release_date) >= 4:
            try:
                release_year = int(release_date[:4])
            except ValueError:
                pass
        
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO movies 
            (title, original_language, release_date, release_year, 
             popularity, vote_average, vote_count, overview)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """, (title, lang, release_date, release_year,
              popularity, vote_average, vote_count, overview))
        conn.commit()
        conn.close()
        
        flash("The movie record has been added successfully", "success")
        return redirect(url_for("admin_menu"))
    
    return render_template("form_add_movie.html")


# ================== 启动入口 ==================

if __name__ == "__main__":
    # 强制初始化数据库 + 插入默认模板（确保模板数据存在）
    init_db()
    insert_default_templates_and_queries()

    app.run(debug=True, host="127.0.0.1", port=5000)