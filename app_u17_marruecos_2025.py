# --------------------------
# app_u17_marruecos_2025.py
# --------------------------
# Scouting U17 — Marruecos 2025 (Streamlit)

import io, os, sqlite3, base64, hashlib, mimetypes, re, colorsys
from datetime import datetime
from typing import Tuple, Optional, Dict, List

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
import requests
import plotly.io as pio
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
import matplotlib
matplotlib.use("Agg")  # backend seguro para servidores/headless

st.set_page_config(page_title="Scouting U17 — Marruecos 2025", page_icon="⚽", layout="wide")

# === Rutas base con soporte de disco persistente ===
# Prioridad: envvar DATA_DIR -> /data (si existe) -> ~/.scouting_u17 -> .
def _resolve_data_dir() -> str:
    if os.getenv("DATA_DIR"):
        return os.getenv("DATA_DIR")
    if os.path.isdir("/data"):
        return "/data"
    home = os.path.expanduser("~")
    cand = os.path.join(home, ".scouting_u17")
    os.makedirs(cand, exist_ok=True)
    return cand if os.path.isdir(cand) else "."

DATA_DIR = _resolve_data_dir()
DB_PATH     = os.path.join(DATA_DIR, "scouting_u17.db")
FLAGS_DIR   = os.path.join(DATA_DIR, "flags")
PHOTOS_DIR  = os.path.join(DATA_DIR, "photos")
EXPORTS_DIR = os.path.join(DATA_DIR, "exports")
for d in (FLAGS_DIR, PHOTOS_DIR, EXPORTS_DIR):
    os.makedirs(d, exist_ok=True)

# Permite auto-importar jugadoras del Excel al arrancar
AUTO_IMPORT_PLAYERS = (os.getenv("AUTO_IMPORT_PLAYERS", "1") == "1")  # por defecto ACTIVADO

# Excel del repo (misma carpeta que el .py)
EXCEL_DEFAULT   = "Players U17 World Cup Marruecos 2025.xlsx"
APP_DIR         = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH_REPO = os.path.join(APP_DIR, EXCEL_DEFAULT)


def st_plot(fig, key: str):
    st.plotly_chart(fig, use_container_width=True, key=key)

# ---------------- Banderas -----------------
def _flag_from_alpha2(code: Optional[str]) -> str:
    try:
        if not code: return ""
        code = code.upper()
        return chr(0x1F1E6 + ord(code[0]) - 65) + chr(0x1F1E6 + ord(code[1]) - 65)
    except Exception:
        return ""

FLAG_MAP = {
    "Morocco":"MA","Brazil":"BR","Italy":"IT","Costa Rica":"CR",
    "Korea DPR":"KP","Mexico":"MX","Cameroon":"CM","Netherlands":"NL",
    "USA":"US","Ecuador":"EC","China PR":"CN","Norway":"NO",
    "France":"FR","Nigeria":"NG","Canada":"CA","Samoa":"WS",
    "Spain":"ES","Korea Republic":"KR","Colombia":"CO","Côte d'Ivoire":"CI",
    "Japan":"JP","New Zealand":"NZ","Zambia":"ZM","Paraguay":"PY",
    "Cote d'Ivoire":"CI","Ivory Coast":"CI","Korea, Republic of":"KR","Korea, DPR":"KP",
}
FIFA3_TO_ISO2 = {
    "BRA":"BR","MAR":"MA","ITA":"IT","CRC":"CR","PRK":"KP","MEX":"MX","CMR":"CM","NED":"NL",
    "USA":"US","ECU":"EC","CHN":"CN","NOR":"NO","FRA":"FR","NGA":"NG","CAN":"CA","SAM":"WS",
    "ESP":"ES","KOR":"KR","CIV":"CI","COL":"CO","JPN":"JP","NZL":"NZ","ZMB":"ZM","PAR":"PY",
}

def _iso2_from_any(name_or_code: str) -> Optional[str]:
    if not name_or_code: return None
    s = str(name_or_code).strip()
    if len(s) == 3 and s.isupper(): return FIFA3_TO_ISO2.get(s)
    iso2 = FLAG_MAP.get(s) or FLAG_MAP.get(s.replace("’","'"))
    if iso2: return iso2
    if len(s) == 2: return s.upper()
    return None

def flag_emoji(name: str) -> str:
    iso2 = _iso2_from_any(name)
    return _flag_from_alpha2(iso2) if iso2 else ""

def flag_url_cdn(name_or_code: str, size: int = 160) -> Optional[str]:
    iso2 = _iso2_from_any(name_or_code)
    if not iso2: return None
    return f"https://flagcdn.com/w{size}/{iso2.lower()}.png"

def ensure_flag_png(name_or_code: str, size: int = 200) -> Optional[str]:
    iso2 = _iso2_from_any(name_or_code)
    if not iso2: return None
    local = os.path.join(FLAGS_DIR, f"{iso2.lower()}_{size}.png")
    if os.path.exists(local): return local
    url = flag_url_cdn(iso2, size=size)
    if not url: return None
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200 and r.content:
            with open(local, "wb") as f: f.write(r.content)
            return local
    except Exception:
        return None
    return None

def _file_to_data_uri(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except Exception:
        return None

def flag_image_uri(name_or_code: str, size: int = 40) -> Optional[str]:
    lp = ensure_flag_png(name_or_code, size=max(80, size))
    if lp:
        data_uri = _file_to_data_uri(lp)
        if data_uri: return data_uri
    return flag_url_cdn(name_or_code, size=max(40, size))

def load_flag_image_array(name_or_code: str, size: int = 320):
    """
    Devuelve la imagen de la bandera como array listo para imshow o None.
    Intenta: cache local -> descarga directa del CDN -> None.
    """
    try:
        # 1) cache local (guarda en ./flags)
        p = ensure_flag_png(name_or_code, size=size)
        if p and os.path.exists(p):
            return plt.imread(p)

        # 2) descarga directa (por si el tamaño exacto no está en cache)
        for sz in (size, 240, 160):
            url = flag_url_cdn(name_or_code, size=sz)
            if not url:
                continue
            r = requests.get(url, timeout=10)
            if r.status_code == 200 and r.content:
                return plt.imread(io.BytesIO(r.content))
    except Exception:
        pass
    return None

def flag_img_md(name_or_code: str, size: int = 20) -> str:
    src = flag_image_uri(name_or_code, size=max(20, size))
    return f'<img src="{src}" width="{size}" style="vertical-align:middle;margin-right:6px;">' if src else f"{flag_emoji(name_or_code)} "


# ---------- Fotos cache ----------
def ensure_http_image_cached(url: Optional[str], folder: str = PHOTOS_DIR, name_hint: Optional[str] = None) -> Optional[str]:
    if not url or not isinstance(url, str): return None
    try:
        h = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
        ext = os.path.splitext(url)[1].lower()
        if not ext or len(ext) > 5:
            head = requests.get(url, timeout=8, stream=True)
            ctype = head.headers.get("Content-Type","")
            ext = (mimetypes.guess_extension((ctype or "").split(";")[0]) or ".jpg")
        fname = f"{(name_hint or 'img').lower().replace(' ','_')}_{h}{ext}"
        fpath = os.path.join(folder, fname)
        if os.path.exists(fpath): return fpath
        r = requests.get(url, timeout=12)
        if r.status_code == 200 and r.content:
            with open(fpath, "wb") as f: f.write(r.content)
            return fpath
    except Exception:
        return None
    return None

def get_player_photo_local(player_row: pd.Series) -> Optional[str]:
    purl = player_row.get("photo_url")
    if purl and isinstance(purl, str) and purl.startswith(("http://","https://")):
        return ensure_http_image_cached(purl, name_hint=str(player_row.get("name")))
    return None

# ---------- Normalización selecciones ----------
FIFA3_TO_NAME = {
    "BRA":"Brazil","COL":"Colombia","ESP":"Spain","MAR":"Morocco","MEX":"Mexico",
    "USA":"USA","CAN":"Canada","FRA":"France","ITA":"Italy","NED":"Netherlands","NOR":"Norway",
    "CHN":"China PR","JPN":"Japan","KOR":"Korea Republic","PRK":"Korea DPR","CMR":"Cameroon",
    "NZL":"New Zealand","ZMB":"Zambia","PAR":"Paraguay","ECU":"Ecuador","CRC":"Costa Rica",
    "SAM":"Samoa","CIV":"Côte d'Ivoire"
}
VARIANT_TO_NAME = {
    "england":"England","ivory coast":"Côte d'Ivoire","cote d'ivoire":"Côte d'Ivoire",
    "côte d’ivoire":"Côte d'Ivoire","korea, republic of":"Korea Republic","korea, dpr":"Korea DPR",
    "korea dpr":"Korea DPR","korea prk":"Korea DPR","china":"China PR","u.s.a.":"USA","u s a":"USA",
    "brasil":"Brazil","estados unidos":"USA","corea del sur":"Korea Republic","corea del norte":"Korea DPR",
}

def _clean_weird(s: str) -> str:
    s = (s or "")
    s = s.replace("CÃ´te", "Côte")
    s = re.sub(r"[\u200b-\u200f\u202a-\u202e]", "", s)
    s = re.sub(r"^[^A-Za-zÀ-ÖØ-öø-ÿ' ]+", "", s)
    s = re.sub(r"[^A-Za-zÀ-ÖØ-öø-ÿ' ]+$", "", s)
    return s.strip()

def normalize_team_name(s: Optional[str]) -> Optional[str]:
    if s is None: return None
    raw = _clean_weird(str(s).strip())
    if not raw: return None
    if len(raw) == 3 and raw.isalpha():
        code = raw.upper()
        if code in FIFA3_TO_NAME: return FIFA3_TO_NAME[code]
    low = raw.casefold()
    if low in VARIANT_TO_NAME: return VARIANT_TO_NAME[low]
    if raw in FLAG_MAP or raw.replace("’","'") in FLAG_MAP: return raw.replace("’","'")
    iso2 = _iso2_from_any(raw)
    if iso2:
        for k, v in FLAG_MAP.items():
            if v.upper() == iso2.upper(): return k
    if len(raw) <= 3: return raw
    return " ".join(raw.split())

# ---------- BD ----------
@st.cache_resource(show_spinner=False)
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = 1;")
    return conn

@st.cache_resource(show_spinner=False)
def init_db():
    conn = get_conn(); cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            confederation TEXT,
            group_name TEXT,
            manager TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            team_id INTEGER,
            position TEXT,
            shirt_number TEXT,
            birth_year INTEGER,
            birthdate TEXT,
            height_cm INTEGER,
            club TEXT,
            agency TEXT,
            photo_url TEXT,
            contract_end TEXT,
            FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE SET NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_date TEXT NOT NULL,
            opponent TEXT,
            tournament TEXT,
            stage TEXT,
            venue TEXT,
            kickoff_local TEXT,
            home_team TEXT,
            away_team TEXT,
            group_name TEXT,
            city TEXT,
            match_notes TEXT,
            home_system TEXT,
            away_system TEXT,
            home_manager TEXT,
            away_manager TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            match_id INTEGER,
            rating INTEGER NOT NULL CHECK (rating IN (5,7,9)),
            final_score REAL,
            comment TEXT,
            scout TEXT,
            is_featured INTEGER DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE SET NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS evaluation_factors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evaluation_id INTEGER NOT NULL,
            factor TEXT NOT NULL,
            score INTEGER NOT NULL CHECK (score IN (5,7,9)),
            FOREIGN KEY(evaluation_id) REFERENCES evaluations(id) ON DELETE CASCADE
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS player_tournaments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            tournament_year INTEGER NOT NULL,
            team_id INTEGER,
            UNIQUE(player_id, tournament_year),
            FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
            FOREIGN KEY(team_id) REFERENCES teams(id) ON DELETE SET NULL
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tournament_teams (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_year INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            UNIQUE(tournament_year, team_name)
        );
    """)

    # índice único de partidos (evita duplicados)
    def _ensure_matches_unique_index(_conn):
        _cur = _conn.cursor()
        try:
            _cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_matches_uni
                ON matches(
                    date(match_date),
                    IFNULL(tournament,''),
                    IFNULL(stage,''),
                    IFNULL(home_team,''),
                    IFNULL(away_team,'')
                );
            """)
        except sqlite3.IntegrityError:
            _cur.execute("""
                DELETE FROM matches
                WHERE id NOT IN (
                    SELECT MIN(id)
                    FROM matches
                    GROUP BY
                        date(match_date),
                        IFNULL(tournament,''),
                        IFNULL(stage,''),
                        IFNULL(home_team,''),
                        IFNULL(away_team,'')
                );
            """)
            _conn.commit()
            _cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_matches_uni
                ON matches(
                    date(match_date),
                    IFNULL(tournament,''),
                    IFNULL(stage,''),
                    IFNULL(home_team,''),
                    IFNULL(away_team,'')
                );
            """)
    _ensure_matches_unique_index(conn)

    # índice único equipos por nombre y limpieza de duplicados previos
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_teams_name ON teams(name);")
    except sqlite3.IntegrityError:
        cur.execute("DELETE FROM teams WHERE id NOT IN (SELECT MIN(id) FROM teams GROUP BY name)")
        conn.commit()
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_teams_name ON teams(name);")
    conn.commit()
    return conn

conn = init_db()

# --- Migración defensiva managers en tablas (por si la BD es antigua) ---
def _migrate_add_managers():
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(teams);")
    cols = [r[1] for r in cur.fetchall()]
    if "manager" not in cols:
        try: cur.execute("ALTER TABLE teams ADD COLUMN manager TEXT;")
        except Exception: pass

    cur.execute("PRAGMA table_info(matches);")
    cols = [r[1] for r in cur.fetchall()]
    if "home_manager" not in cols:
        try: cur.execute("ALTER TABLE matches ADD COLUMN home_manager TEXT;")
        except Exception: pass
    if "away_manager" not in cols:
        try: cur.execute("ALTER TABLE matches ADD COLUMN away_manager TEXT;")
        except Exception: pass
    conn.commit()
_migrate_add_managers()

def fetch_df(query: str, params: Tuple = ()):
    return pd.read_sql_query(query, conn, params=params)

def _bootstrap_if_needed():
    try:
        # 1) Managers y calendario si faltan (siempre seguros)
        ct_teams = int(fetch_df("SELECT COUNT(*) AS c FROM teams").iloc[0]["c"])
        if ct_teams == 0:
            seed_managers_fixed()

        ct_matches = int(fetch_df("SELECT COUNT(*) AS c FROM matches").iloc[0]["c"])
        if ct_matches == 0:
            seed_official_matches(replace_all=True)

        # 2) Jugadoras 2025 desde el Excel del repo
        ct_players_2025 = int(fetch_df(
            "SELECT COUNT(*) AS c FROM player_tournaments WHERE tournament_year=2025"
        ).iloc[0]["c"])

        # Importa SIEMPRE si AUTO_IMPORT_PLAYERS=1,
        # o si todavía no hay jugadoras 2025
        if os.path.exists(EXCEL_PATH_REPO) and (AUTO_IMPORT_PLAYERS or ct_players_2025 == 0):
            import_players_from_excel(EXCEL_PATH_REPO)
            clear_caches()
    except Exception:
        # no bloqueamos la app si algo falla al bootstrap
        pass


_bootstrap_if_needed()

@st.cache_data(show_spinner=False)
def list_teams() -> pd.DataFrame:
    return fetch_df("SELECT * FROM teams ORDER BY name")

@st.cache_data(show_spinner=False)
def list_matches() -> pd.DataFrame:
    return fetch_df("SELECT * FROM matches ORDER BY date(match_date) ASC, id ASC")

@st.cache_data(show_spinner=False)
def list_evaluations() -> pd.DataFrame:
    q = """
    SELECT e.id, e.player_id, p.name as player_name, p.position, p.shirt_number,
           p.birthdate, p.birth_year, p.height_cm, p.club, p.agency, t.name as team_name,
           e.rating, e.final_score, e.comment, e.scout, e.created_at, e.is_featured,
           m.id as match_id, m.match_date, m.opponent, m.stage, m.tournament,
           m.home_team, m.away_team
    FROM evaluations e
    LEFT JOIN players p ON p.id = e.player_id
    LEFT JOIN teams t ON t.id = p.team_id
    LEFT JOIN matches m ON m.id = e.match_id
    ORDER BY datetime(e.created_at) DESC
    """
    return fetch_df(q)

def compact_team_duplicates() -> int:
    # TODO: implementar si lo necesitas; por ahora evita que el botón rompa
    return 0

def clear_caches():
    try: list_teams.clear()
    except Exception: pass
    try: list_matches.clear()
    except Exception: pass
    try: list_evaluations.clear()
    except Exception: pass
    # Puede que aún no exista si se llama en el bootstrap muy pronto
    try: list_players_2025.clear()
    except Exception: pass


# ========= HELPERS 2025 =========
@st.cache_data(show_spinner=False)
def list_players_2025(team_id: Optional[int] = None) -> pd.DataFrame:
    if team_id:
        q = """
        SELECT p.*
        FROM players p
        JOIN player_tournaments pt ON pt.player_id = p.id AND pt.tournament_year = 2025
        WHERE COALESCE(pt.team_id, p.team_id) = ?
        ORDER BY p.name
        """
        return fetch_df(q, (team_id,))
    else:
        q = """
        SELECT DISTINCT p.*
        FROM players p
        JOIN player_tournaments pt ON pt.player_id = p.id AND pt.tournament_year = 2025
        ORDER BY p.name
        """
        return fetch_df(q)

# ---------- Posiciones / factores ----------
def map_pos_group(p: Optional[str]) -> Optional[str]:
    if not p: return None
    p = str(p).lower()
    if any(k in p for k in ["gk","port","arquera","po","goalkeeper"]): return "portera"
    if any(k in p for k in ["rb","lb","lateral","back","fullback"]): return "laterales"
    if any(k in p for k in ["cb","central","defensa central","centre back","center back","defender"]): return "centrales"
    if any(k in p for k in ["mcd","mdf","pivot","pivote","defensive mid"," 6","6 "]): return "medios defensivos"
    if any(k in p for k in ["mco","mediapunta","am","enganche","interior"," 8","8 "]): return "medios ofensivos"
    if any(k in p for k in ["ext","wing","w","lw","rw","winger"]): return "extremos"
    if any(k in p for k in ["dl","dc","st","fw","delantera"," 9","9 "]): return "delanteras"
    if "midfielder" in p and "defens" in p: return "medios defensivos"
    if "midfielder" in p: return "medios ofensivos"
    return None

POS_FACTORS = {
    "portera": ["Juego de pies","Juego aéreo","Desplazamiento corto","Desplazamiento largo","Comunicación"],
    "laterales": ["Velocidad/ida y vuelta","Centros","1v1 defensivo","Progresión con balón","Temporización/altura línea"],
    "centrales": ["Duelos aéreos","Timing/anticipación","Salida de balón","Perfilado/ubicación","Coberturas/correcciones"],
    "medios defensivos": ["Coberturas","Posicionamiento sin balón","Progresión de pase","Duelos/robos","Orientación del juego"],
    "medios ofensivos": ["Recepción entre líneas","Último pase","Movilidad/espacios","Gol/lejana","Presión tras pérdida"],
    "extremos": ["1v1 desborde","Centros/decisión","Rupturas al espacio","Presión/retorno","Finalización 2º palo"],
    "delanteras": ["Desmarques de ruptura","Definición","Descargas/apoyos","Presión alta","Ataque primer palo"],
}

def infer_group_from_factors(labels: List[str]) -> Optional[str]:
    """
    Devuelve el grupo posicional cuyo conjunto de ítems más coincide con 'labels'.
    Requiere al menos 2 coincidencias para aceptar la inferencia.
    """
    if not labels:
        return None
    best_group, best_overlap = None, -1
    label_set = set(labels)
    for g, items in POS_FACTORS.items():
        overlap = len(label_set.intersection(items))
        if overlap > best_overlap:
            best_group, best_overlap = g, overlap
    return best_group if best_overlap >= 2 else None

# ---------- Pitch / campograma ----------
POS_PITCH = {
    "portera": (5, 34), "centrales": (20, 34), "laterales": (25, 12),
    "medios defensivos": (40, 34), "medios ofensivos": (60, 34),
    "extremos": (65, 12), "delanteras": (85, 34),
}

def add_pitch_background(fig: go.Figure):
    fig.add_shape(type="rect", x0=0, y0=0, x1=105, y1=68, line=dict(width=2))
    fig.add_shape(type="line", x0=52.5, y0=0, x1=52.5, y1=68)
    fig.add_shape(type="circle", x0=52.5-9.15, y0=34-9.15, x1=52.5+9.15, y1=34+9.15)

def draw_pitch_and_point(pos_group: str, title: str = "Campograma"):
    x, y = POS_PITCH.get(pos_group, (52.5, 34))
    fig = go.Figure()
    add_pitch_background(fig)
    fig.add_trace(go.Scatter(x=[x], y=[y], mode="markers", marker=dict(size=14)))
    fig.add_annotation(x=x+1.6, y=y, text=pos_group.title(), showarrow=False,
                       xanchor="left", yanchor="middle",
                       bgcolor="rgba(255,255,255,0.7)", font=dict(size=12, color="black"))
    fig.update_xaxes(range=[-2, 107], visible=False)
    fig.update_yaxes(range=[-2, 70], visible=False)
    fig.update_layout(title=title, height=400, margin=dict(l=10,r=10,t=40,b=10))
    return fig

# ---------- Matplotlib helpers (para PDF) ----------
def _matplotlib_radar_png(labels: List[str], values: List[float], label_name: Optional[str] = None) -> Optional[bytes]:
    try:
        N = len(labels)
        if N == 0: return None
        angles = np.linspace(0, 2*np.pi, N, endpoint=False).tolist()
        vals = list(values) + [values[0]]
        angs = angles + [angles[0]]
        fig: Figure = plt.figure(figsize=(4,4))
        ax = fig.add_subplot(111, polar=True)
        ax.set_theta_offset(np.pi / 2); ax.set_theta_direction(-1)
        ax.set_thetagrids(np.degrees(angles), labels, fontsize=8)
        ax.set_rlabel_position(0); ax.set_ylim(0, 10)
        ax.plot(angs, vals, linewidth=2)
        ax.fill(angs, vals, alpha=0.25)
        if label_name: ax.set_title(label_name, fontsize=10)
        buff = io.BytesIO(); fig.tight_layout(); fig.savefig(buff, format="png", dpi=200); plt.close(fig)
        return buff.getvalue()
    except Exception:
        return None

def _matplotlib_pitch_point_png(pos_group: str) -> Optional[bytes]:
    try:
        x, y = POS_PITCH.get(pos_group, (52.5,34))
        fig = plt.figure(figsize=(4,4)); ax = fig.add_subplot(111)
        ax.add_patch(plt.Rectangle((0,0),105,68, fill=False, linewidth=2))
        ax.plot([52.5,52.5],[0,68], linewidth=1, color="black")
        circ = plt.Circle((52.5,34), 9.15, fill=False); ax.add_patch(circ)
        ax.scatter([x],[y], s=40)
        ax.set_xlim(-2,107); ax.set_ylim(-2,70); ax.axis("off")
        buff = io.BytesIO(); fig.tight_layout(); fig.savefig(buff, format="png", dpi=200); plt.close(fig)
        return buff.getvalue()
    except Exception:
        return None

# ========= Managers fijos (24, sin Excel) =========
MANAGERS_FIXED = {
    "Morocco":"Anwar MGHINIA","Brazil":"RILANY","Cameroon":"Josephine NDOUMOU","Canada":"Jen HERST",
    "China PR":"WANG Hongliang","Colombia":"Carlos PANIAGUA","Costa Rica":"Edgar RODRIGUEZ",
    "Côte d'Ivoire":"Ozoua KOUDOUGNON","France":"Mickael FERREIRA","Ecuador":"Victor IDROBO",
    "Italy":"Viviana SCHIAVI","Japan":"Sadayoshi SHIRAI","Korea DPR":"PAK Song Jin",
    "Korea Republic":"GO Hyunbok","Mexico":"Miguel GAMERO","Netherlands":"Olivier AMELINK",
    "New Zealand":"Alana GUNN","Nigeria":"Bankole OLOWOOKERE","Norway":"Eline KULSTAD-TORNEUS",
    "Paraguay":"LUIZ ALMEIDA","Samoa":"Juan CHANG","Spain":"Milagros MARTINEZ",
    "USA":"Katie SCHOEPFER","Zambia":"Carol KANYEMBA",
}
OFFICIAL_TEAMS = list(MANAGERS_FIXED.keys())

def seed_managers_fixed():
    cur = conn.cursor()
    for team, mgr in MANAGERS_FIXED.items():
        t = normalize_team_name(team)
        cur.execute("INSERT OR IGNORE INTO teams(name, manager) VALUES (?,?)", (t, mgr))
        cur.execute("UPDATE teams SET manager=? WHERE name=?", (mgr, t))
    conn.commit(); clear_caches()

# ========= Calendario oficial =========
OFFICIAL_MATCHES = [
    ("2025-10-17","21:00","Morocco","Brazil","First stage","Group A","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-10-18","15:00","Italy","Costa Rica","First stage","Group A","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-18","15:00","China PR","Norway","First stage","Group C","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-18","21:00","Korea DPR","Mexico","First stage","Group B","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-18","21:00","Cameroon","Netherlands","First stage","Group B","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-18","21:00","USA","Ecuador","First stage","Group C","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-19","15:00","Korea Republic","Côte d'Ivoire","First stage","Group E","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-19","15:00","Japan","New Zealand","First stage","Group F","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-19","18:00","France","Samoa","First stage","Group D","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-19","21:00","Nigeria","Canada","First stage","Group D","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-19","21:00","Spain","Colombia","First stage","Group E","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-19","21:00","Zambia","Paraguay","First stage","Group F","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-21","15:00","Korea DPR","Cameroon","First stage","Group B","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-21","18:00","Costa Rica","Brazil","First stage","Group A","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-21","18:00","USA","China PR","First stage","Group C","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-21","21:00","Morocco","Italy","First stage","Group A","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-10-21","21:00","Netherlands","Mexico","First stage","Group B","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-21","21:00","Norway","Ecuador","First stage","Group C","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-22","15:00","Spain","Korea Republic","First stage","Group E","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-22","18:00","Samoa","Canada","First stage","Group D","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-22","18:00","Japan","Zambia","First stage","Group F","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-22","21:00","Nigeria","France","First stage","Group D","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-22","21:00","Côte d'Ivoire","Colombia","First stage","Group E","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-22","21:00","Paraguay","New Zealand","First stage","Group F","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-24","15:00","Norway","USA","First stage","Group C","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-24","15:00","Ecuador","China PR","First stage","Group C","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-24","21:00","Costa Rica","Morocco","First stage","Group A","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-10-24","21:00","Brazil","Italy","First stage","Group A","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-24","21:00","Netherlands","Korea DPR","First stage","Group B","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-24","21:00","Mexico","Cameroon","First stage","Group B","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-25","15:00","Côte d'Ivoire","Spain","First stage","Group E","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-25","15:00","Colombia","Korea Republic","First stage","Group E","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-25","18:00","Samoa","Nigeria","First stage","Group D","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-25","18:00","Canada","France","First stage","Group D","Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-25","21:00","Paraguay","Japan","First stage","Group F","Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-25","21:00","New Zealand","Zambia","First stage","Group F","Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-28","16:30","2A","2C","Round of 16","", "Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-28","16:30","1C","3ABF","Round of 16","", "Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-28","20:00","1B","3ACD","Round of 16","", "Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-10-28","20:00","1A","3CDE","Round of 16","", "Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-10-29","16:30","1E","2D","Round of 16","", "Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-29","16:30","2B","2F","Round of 16","", "Football Academy Mohammed VI (Pitch 3)","SALE","U17 WWC 2025"),
    ("2025-10-29","20:00","1D","3BEF","Round of 16","", "Football Academy Mohammed VI (Pitch 1)","SALE","U17 WWC 2025"),
    ("2025-10-29","20:00","1F","2E","Round of 16","", "Football Academy Mohammed VI (Pitch 2)","SALE","U17 WWC 2025"),
    ("2025-11-01","16:30","W37","W38","Quarter-final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-01","20:00","W39","W40","Quarter-final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-02","16:30","W41","W42","Quarter-final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-02","20:00","W43","W44","Quarter-final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-05","16:30","W45","W46","Semi-final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-05","20:00","W47","W48","Semi-final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-08","16:30","RU49","RU50","Match for third place","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
    ("2025-11-08","20:00","W49","W50","Final","","Olympic Stadium Annex Sports Complex Prince Moulay Abdellah","Rabat","U17 WWC 2025"),
]

def seed_official_matches(replace_all: bool = True):
    cur = conn.cursor()
    if replace_all:
        cur.execute("DELETE FROM matches;")
    for mdate, ko, h, a, stg, grp, ven, city, tourn in OFFICIAL_MATCHES:
        ht = normalize_team_name(h); at = normalize_team_name(a)
        cur.execute("""
            INSERT OR IGNORE INTO matches(match_date, kickoff_local, home_team, away_team,
                stage, group_name, venue, city, tournament)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (mdate, ko, ht, at, stg, grp or None, ven, city, tourn))
    conn.commit(); clear_caches()

# ========= Importar jugadoras desde Excel =========
def _coerce_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.lower().strip(): c for c in df.columns}
    # intentos típicos de nombres
    name_c = next((cols[k] for k in cols if k in ("name","player","jugadora")), None)
    team_c = next((cols[k] for k in cols if k in ("team","team_name","seleccion","country","nation")), None)
    pos_c  = next((cols[k] for k in cols if k in ("position","posicion","role")), None)
    if not all([name_c, team_c, pos_c]):
        raise ValueError("El Excel debe tener columnas: name / team_name / position (o equivalentes).")
    df2 = df[[name_c, team_c, pos_c]].copy()
    df2.columns = ["name","team_name","position"]
    df2["team_name"] = df2["team_name"].apply(lambda x: normalize_team_name(x))
    df2["name"] = df2["name"].astype(str).str.strip()
    df2["position"] = df2["position"].astype(str).str.strip()
    df2 = df2.dropna(subset=["name","team_name","position"])
    return df2

def import_players_from_excel(path: str) -> int:
    df = pd.read_excel(path)
    df = _coerce_cols(df)
    cur = conn.cursor()
    # crea equipos oficiales
    for team, mgr in MANAGERS_FIXED.items():
        t = normalize_team_name(team)
        cur.execute("INSERT OR IGNORE INTO teams(name, manager) VALUES (?,?)", (t, mgr))
        cur.execute("UPDATE teams SET manager=? WHERE name=?", (mgr, t))
    # inserta jugadoras (solo en equipos oficiales)
    teams_map = fetch_df("SELECT id, name FROM teams").set_index("name")["id"].to_dict()
    inserted = 0
    for _, r in df.iterrows():
        tname = normalize_team_name(r["team_name"])
        if tname not in teams_map:  # ignora selecciones no oficiales
            continue
        tid = int(teams_map[tname])
        name = str(r["name"]).strip()
        pos  = str(r["position"]).strip()
        # upsert
        row = fetch_df("SELECT id FROM players WHERE name=? COLLATE NOCASE", (name,))
        if row.empty:
            conn.execute("INSERT INTO players(name, team_id, position) VALUES (?,?,?)", (name, tid, pos))
            pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            inserted += 1
        else:
            pid = int(row.iloc[0]["id"])
            conn.execute("UPDATE players SET team_id=?, position=? WHERE id=?", (tid, pos, pid))
        # vínculo torneo 2025
        conn.execute("""
            INSERT OR IGNORE INTO player_tournaments(player_id, tournament_year, team_id)
            VALUES (?,?,?)
        """, (pid, 2025, tid))
    conn.commit(); clear_caches()
    return inserted

# ---------- Factores: media con fallback inteligente ----------
def _factors_last_for_player(player_id: int, wanted: Optional[List[str]] = None) -> Tuple[List[str], List[float]]:
    # Devuelve los factores EXACTOS de la última valoración (sirve para fallback)
    e = fetch_df("SELECT id FROM evaluations WHERE player_id=? ORDER BY datetime(created_at) DESC LIMIT 1", (int(player_id),))
    if e.empty: 
        if wanted: return (wanted, [0.0]*len(wanted))
        return ([], [])
    last_id = int(e.iloc[0]["id"])
    dfl = fetch_df("SELECT factor, score FROM evaluation_factors WHERE evaluation_id=?", (last_id,))
    if dfl.empty:
        if wanted: return (wanted, [0.0]*len(wanted))
        return ([], [])
    labs = [str(r["factor"]).strip() for _, r in dfl.iterrows()]
    vals = [float(r["score"]) for _, r in dfl.iterrows()]
    return (labs, vals)

def _factors_avg_for_player(player_id: int) -> Tuple[List[str], List[float]]:
    # Calcula media por grupo posicional; si queda todo 0 (mismatch etiquetas), cae a la última valoración REAL
    player = fetch_df("SELECT id, position FROM players WHERE id=?", (player_id,))
    if player.empty: return ([], [])
    posg = map_pos_group(player.iloc[0]["position"]) or "medios ofensivos"
    wanted = POS_FACTORS.get(posg, [])
    if not wanted: return _factors_last_for_player(player_id)  # sin mapeo, usa última

    df = fetch_df("""
      SELECT ef.factor, AVG(ef.score) as avg_score
      FROM evaluation_factors ef
      JOIN evaluations e ON e.id = ef.evaluation_id
      WHERE e.player_id=?
      GROUP BY ef.factor
    """, (int(player_id),))
    if df.empty:
        return _factors_last_for_player(player_id, wanted)

    m = {str(r["factor"]).strip(): float(r["avg_score"]) for _, r in df.iterrows()}
    vals = [m.get(f, 0.0) for f in wanted]
    # si todo 0 (etiquetas distintas a las esperadas para el grupo), usa última valoración real
    if not any(v > 0 for v in vals):
        return _factors_last_for_player(player_id, wanted=None)
    return (wanted, vals)

# ---------- Guardado evaluación ----------
def save_evaluation(player_id: int, rating: int, comment: Optional[str],
                    factors: Dict[str, int], is_featured: bool,
                    match_selected_id: Optional[int]) -> int:
    cur = conn.cursor()
    cur.execute("""INSERT INTO evaluations(player_id, match_id, rating, final_score, comment, scout, is_featured)
                   VALUES (?,?,?,?,?,?,?)""",
                (int(player_id), match_selected_id, int(rating), float(rating), (comment or ""), None, int(is_featured)))
    eid = cur.lastrowid
    for k, v in (factors or {}).items():
        cur.execute("""INSERT INTO evaluation_factors(evaluation_id, factor, score) VALUES (?,?,?)""",
                    (eid, str(k), int(v)))
    conn.commit(); clear_caches()
    return eid

# ---------- Export PDF ----------
def export_player_pdf(player_id: int, save_path: str) -> Optional[Tuple[str, bytes]]:
    # Datos de la jugadora
    p = fetch_df("""
        SELECT p.*, t.name AS team_name
        FROM players p LEFT JOIN teams t ON t.id = p.team_id
        WHERE p.id=?""", (player_id,))
    if p.empty:
        return None
    row = p.iloc[0]
    name = str(row["name"])
    team = normalize_team_name(str(row.get("team_name") or ""))
    posg = map_pos_group(row.get("position")) or ""

    # Factores (media con fallback a última)
    labels, values = _factors_avg_for_player(int(player_id))

    # Gráficos como PNG en memoria
    radar_png = None
    if labels:
        try:
            radar_png = _matplotlib_radar_png(labels, values, label_name=name)
        except Exception:
            radar_png = None

    camp_png = None
    if posg:
        try:
            camp_png = _matplotlib_pitch_point_png(posg)
        except Exception:
            camp_png = None

    # Manager
    mgr = ""
    try:
        tt = fetch_df("SELECT manager FROM teams WHERE name=?", (team,))
        if not tt.empty and isinstance(tt.iloc[0]["manager"], str):
            mgr = tt.iloc[0]["manager"]
    except Exception:
        pass


    # Última valoración (texto)
    last = fetch_df("""
        SELECT e.id, e.rating, e.comment, e.created_at
        FROM evaluations e WHERE e.player_id=?
        ORDER BY datetime(e.created_at) DESC LIMIT 1
    """, (player_id,))
    last_txt = ""
    if not last.empty:
        last_txt = f"Rating: {last.iloc[0]['rating']}  —  {last.iloc[0]['comment'] or ''}"

    factors_txt = ""
    if labels:
        try:
            pairs = [f"{lab}: {round(values[i],1)}" for i, lab in enumerate(labels)]
            factors_txt = " | ".join(pairs)
        except Exception:
            factors_txt = ""

    # === DIBUJO DEL PDF ===
    fig = plt.figure(figsize=(8.27, 11.69))  # A4
    ax = fig.add_axes([0, 0, 1, 1]); ax.axis("off")

    y = 0.96
    ax.text(0.06, y, f"{name}", fontsize=18, weight="bold", transform=ax.transAxes); y -= 0.04
    ax.text(0.06, y, f"Selección: {team}    Entrenador/a: {mgr or '-'}", fontsize=11, transform=ax.transAxes); y -= 0.03
    ax.text(0.06, y, f"Posición: {row.get('position') or ''}    Grupo: {posg.title() if posg else ''}", fontsize=11, transform=ax.transAxes); y -= 0.03
    ax.text(0.06, y, f"Club: {row.get('club') or '-'}    Altura: {row.get('height_cm') or '-'} cm    Año Nac.: {row.get('birth_year') or '-'}",
            fontsize=10, transform=ax.transAxes); y -= 0.03
    ax.text(0.06, y, f"Última valoración — {last_txt}", fontsize=10, transform=ax.transAxes); y -= 0.04

    # --- Bandera arriba-derecha (alta calidad, si se pudo cargar) ---
    try:
        flag_img = load_flag_image_array(team, size=320)  # usa 320 para más nitidez
        if flag_img is not None:
        # Extent en coordenadas del eje (x0, x1, y0, y1).
        # x1=0.975 la hace un poco más ancha que 0.95, se ve mejor en A4.
            ax.imshow(
                flag_img,
                extent=(0.78, 0.975, 0.885, 0.985),
                transform=ax.transAxes,
                aspect="auto"
            )
    except Exception:
        pass

    # Gráficos (si existen)
    try:
        if radar_png:
            ax.imshow(plt.imread(io.BytesIO(radar_png)), extent=(0.08, 0.52, 0.48, 0.80),
                      transform=ax.transAxes, aspect='auto')
    except Exception:
        pass
    try:
        if camp_png:
            ax.imshow(plt.imread(io.BytesIO(camp_png)), extent=(0.56, 0.96, 0.48, 0.80),
                      transform=ax.transAxes, aspect='auto')
    except Exception:
        pass

    if factors_txt:
        ax.text(0.06, 0.44, "Ítems (media):", fontsize=12, weight="bold", transform=ax.transAxes)
        ax.text(0.06, 0.40, factors_txt, fontsize=10, transform=ax.transAxes, wrap=True)

    if not last.empty and not pd.isna(last.iloc[0]['comment']):
        ax.text(0.06, 0.35, "Notas:", fontsize=12, weight="bold", transform=ax.transAxes)
        ax.text(0.06, 0.31, str(last.iloc[0]['comment']), fontsize=10, transform=ax.transAxes, wrap=True)

    # Guardar a memoria y a disco
    pdf_buffer = io.BytesIO()
    fig.savefig(pdf_buffer, format="pdf", dpi=200, bbox_inches="tight")
    plt.close(fig)
    pdf_bytes = pdf_buffer.getvalue()

    try:
        os.makedirs(os.path.dirname(save_path) or ".", exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(pdf_bytes)
    except Exception:
        pass

    return (save_path, pdf_bytes)

# ===================== UI =====================
st.title("Scouting U17 — Marruecos 2025")
tabs = st.tabs(["🛠️ Admin", "📅 Partidos", "⚡ Valoración", "⭐ Destacadas", "🏅 Ranking", "📊 Radar & Campo", "🌍 Campograma global"])

# ========== Admin ==========
with tabs[0]:
    # --- Admin: edición de manager segura ---
    st.subheader("Datos base")
    c1, c2, c3 = st.columns(3)

    with c1:
        if st.button("Cargar/actualizar MANAGERS fijos (24)", use_container_width=True):
            seed_managers_fixed()
            st.session_state.pop("mgr_team_sel", None)
            clear_caches()
            st.success("Managers guardados.")

    with c2:
        if st.button("Reemplazar calendario oficial (sólo estos)", use_container_width=True):
            seed_official_matches(replace_all=True)
            st.session_state.pop("mgr_team_sel", None)
            clear_caches()
            st.success("Calendario oficial cargado (y anteriores eliminados).")

    with c3:
        if st.button("Compactar equipos duplicados", use_container_width=True):
            n = compact_team_duplicates()
            st.success(f"Compactados {n} duplicados.") if n else st.info("No había duplicados.")
            st.session_state.pop("mgr_team_sel", None)
            clear_caches()

    st.markdown("— Edita **manager** manualmente (móvil friendly):")
    tdf_raw = list_teams().copy()

    # sólo las 24 selecciones oficiales + dedupe por nombre
    tdf = tdf_raw[tdf_raw["name"].isin(OFFICIAL_TEAMS)].copy()
    if not tdf.empty:
        tdf["_k"] = tdf["name"].astype(str).str.strip().str.casefold()
        tdf = (
            tdf.sort_values(["_k", "id"])
               .drop_duplicates(subset=["_k"], keep="last")
               .drop(columns=["_k"])
               .sort_values("name")
        )

    options = tdf["name"].tolist() if not tdf.empty else []
    tname = st.selectbox("Selección", options=options, key="mgr_team_sel")

    # Manager actual (SAFE)
    def _safe_manager(df: pd.DataFrame, name: Optional[str]) -> str:
        if df.empty or not name:
            return ""
        col = df.loc[df["name"] == name, "manager"]
        return str(col.iloc[0]) if not col.empty else ""

    cur_mgr = _safe_manager(tdf, tname)
    new_mgr = st.text_input("Entrenador/a", value=cur_mgr, key="mgr_name_edit")

    # Botón guardar protegido (deshabilitado si no hay selección)
    if st.button("💾 Guardar entrenador/a", use_container_width=True, disabled=not bool(tname)):
        conn.execute("UPDATE teams SET manager=? WHERE name=?", (new_mgr.strip(), tname))
        conn.commit()
        clear_caches()
        st.success("Actualizado.")

    # ---------- Importar jugadoras dentro de Admin ----------
    st.divider()
    st.subheader("Importar jugadoras desde Excel")

    colA, colB = st.columns([1, 1])

    with colA:
        st.caption(f"Excel del repo: **{os.path.basename(EXCEL_PATH_REPO)}**")
        if os.path.exists(EXCEL_PATH_REPO):
            if st.button("📥 Importar del Excel del repo", use_container_width=True):
                n = import_players_from_excel(EXCEL_PATH_REPO)
                clear_caches()
                st.success(f"Importadas/actualizadas {n} jugadoras desde el Excel del repo.")
        else:
            st.warning("No se encontró el Excel en el repositorio (sube el .xlsx al repo).")

    with colB:
        up = st.file_uploader("…o súbelo manualmente (.xlsx)", type=["xlsx"])
        if up is not None:
            tmp_dir = os.path.join(DATA_DIR, "uploads")
            os.makedirs(tmp_dir, exist_ok=True)
            tmp_path = os.path.join(tmp_dir, "players_import.xlsx")
            with open(tmp_path, "wb") as f:
                f.write(up.getbuffer())
            n = import_players_from_excel(tmp_path)
            clear_caches()
            st.success(f"Importadas/actualizadas {n} jugadoras desde el archivo subido.")

    # este divider VA al nivel de Admin, no dentro de colB
    st.divider()
    st.write("**Selecciones (Mundial 2025)**")
    view = tdf[["name","manager"]].copy().rename(columns={"name":"Selección","manager":"Entrenador/a"})
    view["Bandera"] = view["Selección"].apply(lambda n: flag_img_md(n, 20))
    html = view[["Bandera","Selección","Entrenador/a"]].to_html(escape=False, index=False).replace("NaN","")
    st.markdown(html, unsafe_allow_html=True)

# ========== Partidos ==========
with tabs[1]:
    st.subheader("Partidos — U17 WWC 2025")
    dfm = list_matches()
    if dfm.empty:
        st.info("Pulsa en Admin → “Reemplazar calendario oficial (sólo estos)”.")
    else:
        show = dfm.copy()
        show["Fecha"] = pd.to_datetime(show["match_date"], errors="coerce")
        # tabla con banderas home/away
        def flag_html(name): return flag_img_md(name, 18) + (name or "")
        show["Home"] = show["home_team"].apply(flag_html)
        show["Away"] = show["away_team"].apply(flag_html)
        show = show.sort_values(["Fecha","tournament","stage"]).reset_index(drop=True)
        cols = ["Fecha","kickoff_local","Home","Away","stage","group_name","venue","city","tournament"]
        html = show[cols].rename(columns={
            "kickoff_local":"KO","stage":"Fase","group_name":"Grupo","venue":"Estadio","city":"Ciudad","tournament":"Torneo"
        }).to_html(escape=False, index=False)
        st.markdown(html, unsafe_allow_html=True)

# ========== ⚡ Valoración ==========
with tabs[2]:
    st.subheader("Nueva valoración")
    mdf = list_matches()
    if mdf.empty:
        st.info("Carga el calendario para poder filtrar por partido.")
    else:
        mdf["_label"] = mdf.apply(lambda r: f"{r['match_date']} — {r['home_team']} vs {r['away_team']}", axis=1)
        sel = st.selectbox("Partido", mdf["_label"].tolist())
        mrow = mdf.iloc[mdf["_label"].tolist().index(sel)]
        side = st.radio("Equipo", ["Home","Away"], horizontal=True)
        team_name = mrow["home_team"] if side=="Home" else mrow["away_team"]

        # jugadoras de esa selección (torneo 2025)
        tmap = fetch_df("SELECT id,name FROM teams").set_index("name")["id"].to_dict()
        tid = tmap.get(team_name)
        plist = list_players_2025(team_id=tid) if tid else pd.DataFrame()
        if plist.empty:
            st.info("Importa jugadoras de esa selección (Admin > Excel).")
        else:
            plist = plist.sort_values("name")
            pnames = [f"{r['name']} — {r.get('position','') or ''}" for _, r in plist.iterrows()]
            psel = st.selectbox("Jugadora", pnames)
            prow = plist.iloc[pnames.index(psel)]
            pid = int(prow["id"])
            posg = map_pos_group(prow.get("position")) or "medios ofensivos"

            # factores del grupo
            ff = POS_FACTORS.get(posg, [])
            cols = st.columns(len(ff) if ff else 1)
            values: Dict[str,int] = {}
            for i, f in enumerate(ff):
                with cols[i]:
                    values[f] = st.select_slider(f, options=[5,7,9], value=7, key=f"new_{pid}_{i}")

            rating = st.select_slider("Rating global", options=[5,7,9], value=7)
            featured = st.toggle("Marcar como ⭐ Destacada", value=False)
            comment = st.text_area("Comentario", placeholder="Observaciones…")

            if st.button("💾 Guardar valoración", use_container_width=True):
                eid = save_evaluation(
                    player_id=pid, rating=int(rating), comment=comment,
                    factors=values, is_featured=bool(featured),
                    match_selected_id=int(mrow["id"])
                )
                st.success(f"Guardado (evaluation_id={eid}).")

# ========== ⭐ Destacadas ==========
with tabs[3]:
    st.subheader("Jugadoras destacadas")

    ev = list_evaluations()
    if ev.empty:
        st.info("Aún no hay evaluaciones.")
    else:
        # Última evaluación por jugadora, manteniendo solo las que están marcadas como destacadas
        last_by_player = ev.sort_values("created_at").drop_duplicates(subset=["player_id"], keep="last")
        players_df = fetch_df("SELECT * FROM players")
        merged = last_by_player.merge(players_df, left_on="player_id", right_on="id", how="left")
        merged = merged[merged["is_featured"] == 1].copy()

        if merged.empty:
            st.info("No hay destacadas todavía.")
        else:
            # --------- BOTÓN: Exportar CSV con destacadas + datos + ítems ----------
            def _featured_csv_bytes() -> bytes:
                # Base: columnas fijas
                base_cols = [
                    ("player_name", "jugadora"),
                    ("team_name", "selección"),
                    ("position", "posición"),
                    ("birth_year", "año_nac"),
                    ("height_cm", "altura_cm"),
                    ("club", "club"),
                    ("rating", "rating"),
                    ("final_score", "final_score"),
                    ("created_at", "fecha_valoración"),
                    ("comment", "comentario"),
                ]

                # Recolecta TODOS los ítems usados en las últimas valoraciones destacadas
                eids = merged["id_x"].tolist() if "id_x" in merged.columns else merged["id"].tolist()
                all_items = set()
                per_eval_items = {}

                for eid in eids:
                    fdf = fetch_df("SELECT factor, score FROM evaluation_factors WHERE evaluation_id=?", (int(eid),))
                    d = {str(r["factor"]).strip(): int(r["score"]) for _, r in fdf.iterrows()}
                    per_eval_items[int(eid)] = d
                    all_items.update(d.keys())

                # Ordena los ítems alfabéticamente para columnas
                item_cols = sorted(list(all_items))

                # Construye filas
                rows = []
                for _, r in merged.iterrows():
                    # id de la última evaluación (columna puede ser id_x si viene del merge)
                    eid = int(r["id_x"] if "id_x" in r else r["id"])
                    row = {}
                    for src, dst in base_cols:
                        row[dst] = r.get(src)
                    # ítems → columnas
                    fmap = per_eval_items.get(eid, {})
                    for it in item_cols:
                        row[it] = fmap.get(it, None)
                    rows.append(row)

                out = pd.DataFrame(rows, columns=[dst for _, dst in base_cols] + item_cols)
                # CSV "Excel-friendly"
                return out.to_csv(index=False).encode("utf-8-sig")

            col_left, col_right = st.columns([1, 1], vertical_alignment="center")
            with col_right:
                try:
                    csv_bytes = _featured_csv_bytes()
                    fname = f"destacadas_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
                    st.download_button(
                        "⬇️ Exportar destacadas (CSV)",
                        data=csv_bytes,
                        file_name=fname,
                        mime="text/csv",
                        use_container_width=True,
                        key="dl_csv_featured",
                    )
                except Exception:
                    st.warning("No se pudo generar el CSV de destacadas.")

            # --------- UI de una destacada (igual que tenías) ----------
            merged = merged.sort_values("name")
            names = [f"{r['name']} — {r.get('position','') or ''}" for _, r in merged.iterrows()]
            pick = col_left.selectbox("Selecciona destacada", options=names, key="feat_pick")
            prow = merged.iloc[names.index(pick)]
            pid = int(prow["player_id"])

            # Factores de la última valoración (para coherencia con destacadas)
            labs, vals = _factors_last_for_player(pid)

            if labs:
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(
                    r=vals + [vals[0]],
                    theta=labs + [labs[0]],
                    fill="toself",
                    name="Última"
                ))
                fig.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
                    height=420, margin=dict(l=10, r=10, t=10, b=10)
                )
                st_plot(fig, key=f"radar_feat_{pid}")
            else:
                st.info("Sin ítems para el radar todavía.")

            # Campograma: usa la posición declarada; si no hay, infiere por ítems
            posg = map_pos_group(prow.get("position")) or infer_group_from_factors(labs) or "medios ofensivos"
            st_plot(draw_pitch_and_point(posg, title=f"Campograma — {posg.title()}"), key=f"pitch_feat_{pid}")

            # --------- Acciones: Exportar PDF / Editar última / Quitar destacada ----------
            c1, c2, c3 = st.columns([1, 1, 1])

            def _safe_fname(s: str) -> str:
                base = re.sub(r"[^A-Za-z0-9_-]+", "_", str(s).strip())
                return base.strip("_") or "player"

            # Exportar PDF
            with c1:
                if st.button("📝 Exportar PDF", use_container_width=True, key=f"expdf_{pid}"):
                    os.makedirs(EXPORTS_DIR, exist_ok=True)
                    fname = f"{_safe_fname(prow['name'])}_U17_2025.pdf"
                    out_path = os.path.join(EXPORTS_DIR, fname)
                    res = export_player_pdf(pid, out_path)
                    if not res:
                        st.warning("No se pudo exportar el PDF.")
                    else:
                        saved_path, pdf_bytes = res if isinstance(res, tuple) else (res, None)
                        if pdf_bytes is None:
                            try:
                                with open(saved_path, "rb") as f:
                                    pdf_bytes = f.read()
                            except Exception:
                                pdf_bytes = None
                        st.toast(f"PDF listo: {fname}")
                        if pdf_bytes:
                            st.download_button(
                                "⬇️ Descargar PDF",
                                data=pdf_bytes,
                                file_name=fname,
                                mime="application/pdf",
                                use_container_width=True,
                                key=f"dl_{pid}"
                            )
                        else:
                            st.warning("No se pudieron preparar los bytes del PDF para la descarga.")

            # Editar última valoración
            with c2:
                with st.expander("✏️ Editar última valoración"):
                    last = fetch_df("""
                        SELECT * FROM evaluations
                        WHERE player_id=? ORDER BY datetime(created_at) DESC LIMIT 1
                    """, (pid,))
                    if last.empty:
                        st.info("No hay valoración que editar.")
                    else:
                        le = last.iloc[0]
                        rating_new = st.select_slider(
                            "Rating global", [5, 7, 9], value=int(le["rating"]),
                            key=f"edit_rating_{pid}"
                        )
                        comment_new = st.text_area(
                            "Comentario", value=str(le.get("comment") or ""),
                            key=f"edit_comment_{pid}"
                        )
                        fdf = fetch_df(
                            "SELECT factor, score FROM evaluation_factors WHERE evaluation_id=?",
                            (int(le["id"]),)
                        )
                        current = {str(r["factor"]): int(r["score"]) for _, r in fdf.iterrows()}
                        labs_now = list(current.keys())
                        cols = st.columns(len(labs_now) if labs_now else 1)
                        newvals = {}
                        for i, f in enumerate(labs_now):
                            with cols[i]:
                                newvals[f] = st.select_slider(
                                    f, [5, 7, 9], value=int(current.get(f, 7)),
                                    key=f"edit_{pid}_{i}"
                                )
                        if st.button("💾 Guardar cambios", use_container_width=True, key=f"save_edit_{pid}"):
                            cur = conn.cursor()
                            cur.execute(
                                "UPDATE evaluations SET rating=?, final_score=?, comment=? WHERE id=?",
                                (int(rating_new), float(rating_new), comment_new, int(le["id"]))
                            )
                            cur.execute("DELETE FROM evaluation_factors WHERE evaluation_id=?", (int(le["id"]),))
                            for k, v in newvals.items():
                                cur.execute(
                                    "INSERT INTO evaluation_factors(evaluation_id, factor, score) VALUES (?,?,?)",
                                    (int(le["id"]), k, int(v))
                                )
                            conn.commit(); clear_caches()
                            st.success("Actualizado.")

            # Quitar destacada
            with c3:
                st.caption("Quitar de destacadas")
                if st.button("🗑️ Quitar ⭐", type="secondary", use_container_width=True, key=f"rm_feat_{pid}"):
                    cur = conn.cursor()
                    cur.execute("""
                        UPDATE evaluations
                        SET is_featured = 0
                        WHERE player_id = ?
                          AND datetime(created_at) = (
                            SELECT MAX(datetime(created_at)) FROM evaluations WHERE player_id = ?
                          )
                    """, (pid, pid))
                    conn.commit(); clear_caches()
                    st.success("Se ha quitado de destacadas.")



# ========== 🏅 Ranking ==========
with tabs[4]:
    st.subheader("Ranking 2025 (media de destacadas)")
    q = """
    SELECT p.id as player_id, p.name as player_name, p.position, t.name as team_name,
           AVG(COALESCE(e.final_score, e.rating)) AS media, COUNT(*) as n
    FROM evaluations e
    JOIN players p ON p.id = e.player_id
    LEFT JOIN teams t ON t.id = p.team_id
    WHERE e.is_featured=1
    GROUP BY p.id, p.name, p.position, t.name
    ORDER BY media DESC
    """
    rank = fetch_df(q)
    if rank.empty:
        st.info("Sin destacadas todavía.")
    else:
        rank["flag"] = rank["team_name"].apply(lambda n: flag_image_uri(n or "", size=40))
        st.dataframe(
            rank[["flag", "player_name", "team_name", "position", "media", "n"]],
            use_container_width=True, hide_index=True,
            column_config={
                "flag": st.column_config.ImageColumn(" ", width="small"),
                "player_name": "jugadora",
                "team_name": "selección",
                "position": "posición",
                "media": st.column_config.NumberColumn("media", format="%.1f"),
            }
        )

# ========== 📊 Radar & Campo ==========
with tabs[5]:
    st.subheader("Radar & Campo (cualquier jugadora 2025)")
    allp = list_players_2025()
    if allp.empty:
        st.info("Importa primero jugadoras 2025.")
    else:
        allp["_k"] = allp["name"].str.strip().str.casefold()
        allp = allp.sort_values("name").drop_duplicates(subset=["_k"]).drop(columns=["_k"])
        pnames = [f"{r['name']} — {r['position'] or ''}" for _, r in allp.iterrows()]
        sel = st.selectbox("Jugadora", pnames, key="any_player_sel")
        selrow = allp.iloc[pnames.index(sel)]
        pid = int(selrow["id"])

        # Usa la media por grupo y si viene vacía (labels distintos) cae a la última real
        labels, vals = _factors_avg_for_player(pid)
        if labels and any(v > 0 for v in vals):
            fig = go.Figure()
            fig.add_trace(go.Scatterpolar(r=vals + [vals[0]], theta=labels + [labels[0]], fill='toself'))
            fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
                              height=420, margin=dict(l=10, r=10, t=10, b=10))
            st_plot(fig, key=f"radar_any_{pid}")
        else:
            # última real
            labs2, vals2 = _factors_last_for_player(pid)
            if labs2:
                fig = go.Figure()
                fig.add_trace(go.Scatterpolar(r=vals2 + [vals2[0]], theta=labs2 + [labs2[0]], fill='toself'))
                fig.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
                                  height=420, margin=dict(l=10, r=10, t=10, b=10))
                st_plot(fig, key=f"radar_any_last_{pid}")
            else:
                st.info("Esta jugadora aún no tiene valoración para construir el radar.")

        posg = map_pos_group(selrow.get("position")) or "medios ofensivos"
        st_plot(draw_pitch_and_point(posg, title=f"Campograma — {posg.title()}"), key=f"pitch_any_{pid}")

# ========== 🌍 Campograma global ==========
with tabs[6]:
    st.subheader("Campograma global — Destacadas 2025 (banderas)")

    # --- tamaños/espaciados dentro del campo ---
    FLAG_W_FIELD = 6.0    # ancho de la bandera en unidades del campo
    FLAG_H_FIELD = 4.0    # alto de la bandera
    NAME_DX      = 1.0    # desplazamiento del nombre a la derecha de la bandera

    # banderas como DATA-URI (evita problemas en iOS/Safari)
    def team_flag_data_uri(team: str, px: int = 80) -> Optional[str]:
        # intenta cache local -> base64; si no, cae a CDN (último recurso)
        p = ensure_flag_png(team, size=px)
        if p:
            uri = _file_to_data_uri(p)
            if uri:
                return uri
        return flag_url_cdn(team, size=px)

    # solo jugadoras DESTACADAS
    df_all = fetch_df("""
        SELECT p.id, p.name, p.position, t.name as team_name,
               AVG(COALESCE(e.final_score, e.rating)) as media
        FROM evaluations e
        JOIN players p ON p.id=e.player_id
        LEFT JOIN teams t ON t.id=p.team_id
        WHERE e.is_featured=1
        GROUP BY p.id, p.name, p.position, t.name
    """)

    if df_all.empty:
        st.info("Sin destacadas todavía.")
    else:
        rows = []
        for _, r in df_all.iterrows():
            posg = map_pos_group(r.get("position") or "")
            x, y = POS_PITCH.get(posg or "", (52.5, 34))
            rows.append({
                "player_id": r["id"],
                "name": r["name"],
                "team_name": r["team_name"],
                "pos_group": posg or "—",
                "x": x, "y": y,
                "media": float(r.get("media") or 0.0)
            })
        gdf = pd.DataFrame(rows)

        fig = go.Figure()
        add_pitch_background(fig)

        # puntos invisibles para hover (no afectan al dibujo)
        fig.add_trace(go.Scatter(
            x=gdf["x"], y=gdf["y"], mode="markers",
            marker=dict(size=1, opacity=0),
            hovertext=[
                f"{nm} — {tm} — {pg} · {md:.2f}"
                for nm, tm, pg, md in zip(
                    gdf["name"], gdf["team_name"], gdf["pos_group"], gdf["media"]
                )
            ],
            hoverinfo="text",
            showlegend=False
        ))

        # bandera + apellido a la derecha (dentro del campo)
        for _, r in gdf.iterrows():
            # bandera (como imagen base64 para iOS)
            flag_src = team_flag_data_uri(str(r["team_name"]), px=80)
            if flag_src:
                fig.add_layout_image(
                    dict(
                        source=flag_src,
                        xref="x", yref="y",
                        x=r["x"] - FLAG_W_FIELD/2,
                        y=r["y"] + FLAG_H_FIELD/2,
                        sizex=FLAG_W_FIELD, sizey=FLAG_H_FIELD,
                        xanchor="left", yanchor="top",
                        layer="above"
                    )
                )

            # apellido desplazado a la derecha para no pisar la bandera
            last = str(r["name"]).split(" ")[-1].upper()
            fig.add_annotation(
                x=r["x"] + FLAG_W_FIELD/2 + NAME_DX,
                y=r["y"],
                text=last,
                showarrow=False,
                xanchor="left", yanchor="middle",
                font=dict(size=12, color="black"),
                bgcolor="rgba(255,255,255,0)"
            )

        # límites del campo y márgenes
        fig.update_xaxes(range=[-2, 107], visible=False)
        fig.update_yaxes(range=[-2, 70], visible=False)
        fig.update_layout(
            title="Campograma global — Destacadas 2025 (banderas)",
            height=560,
            margin=dict(l=10, r=10, t=50, b=10)
        )
        st_plot(fig, key="campoglobal_flags_iossafe")
