# bot_fotos.py
# Requisitos:
#   pip install -U python-telegram-bot==21.6 gspread google-auth
#
# PowerShell (PC):
#   cd "C:\Users\Diego_Siancas\Desktop\BOT TuFibra"
#   $env:BOT_TOKEN="TU_TOKEN"
#   $env:ROUTING_JSON='{"-5252607752":{"evidence":"-5143236367","summary":"-5143236367"}}'
#   $env:SHEET_ID="TU_SHEET_ID"
#   $env:GOOGLE_CREDS_JSON="google_creds.json"
#   $env:BOT_VERSION="1.0.0"
#   python bot_fotos.py

import os
import json
import sqlite3
import logging
import time
import uuid
import re
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message, InputMediaPhoto, InputMediaVideo
from telegram.error import BadRequest
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "bot_fotos.sqlite3")
ROUTING_JSON = os.getenv("ROUTING_JSON", "").strip()

MAX_MEDIA_PER_STEP = int(os.getenv("MAX_MEDIA_PER_STEP", "8"))
STEP_LOCK_TIMEOUT_MINUTES = int(os.getenv("STEP_LOCK_TIMEOUT_MINUTES", "10"))
MEDIA_ACK_WINDOW_SECONDS = float(os.getenv("MEDIA_ACK_WINDOW_SECONDS", "1.8"))

# Perú (UTC-5)
PERU_TZ = timezone(timedelta(hours=-5))

# (Deprecated as hardcode) - mantenido solo como fallback si la hoja TECNICOS está vacía o no disponible
TECHNICIANS_FALLBACK = [
    "FLORO FERNANDEZ VASQUEZ",
    "ANTONY SALVADOR CORONADO",
    "DANIEL EDUARDO LUCENA PIÑANGO",
    "JOSE RODAS BERECHE",
    "LUIS OMAR EPEQUIN ZAPATA",
    "CESAR ABRAHAM VASQUEZ MEZA",
]

SERVICE_TYPES = ["ALTA NUEVA", "POSTVENTA", "AVERIAS"]

CASE_STATUS_OPEN = "OPEN"
CASE_STATUS_CLOSED = "CLOSED"
CASE_STATUS_CANCELLED = "CANCELLED"

PHASE_WAIT_TECHNICIAN = "WAIT_TECHNICIAN"
PHASE_WAIT_SERVICE = "WAIT_SERVICE"
PHASE_WAIT_ABONADO = "WAIT_ABONADO"
PHASE_WAIT_LOCATION = "WAIT_LOCATION"
PHASE_MENU_INST = "MENU_INST"
PHASE_MENU_EVID = "MENU_EVID"
PHASE_EVID_ACTION = "EVID_ACTION"
PHASE_AUTH_MODE = "AUTH_MODE"
PHASE_AUTH_TEXT_WAIT = "AUTH_TEXT_WAIT"
PHASE_AUTH_MEDIA = "AUTH_MEDIA"
PHASE_AUTH_REVIEW = "AUTH_REVIEW"
PHASE_STEP_MEDIA = "STEP_MEDIA"
PHASE_STEP_REVIEW = "STEP_REVIEW"
PHASE_CLOSED = "CLOSED"
PHASE_CANCELLED = "CANCELLED"

STEP_STATE_PENDIENTE = "PENDIENTE"
STEP_STATE_EN_CARGA = "EN_CARGA"
STEP_STATE_EN_REVISION = "EN_REVISION"
STEP_STATE_APROBADO = "APROBADO"
STEP_STATE_RECHAZADO = "RECHAZADO"
STEP_STATE_REABIERTO = "REABIERTO"
STEP_STATE_BLOQUEADO = "BLOQUEADO"

# EXTERNA (1..11) -> step_no interno 5..15
EXTERNA_MENU: List[Tuple[int, str, int]] = [
    (1, "FACHADA", 5),
    (2, "CTO", 6),
    (3, "POTENCIA EN CTO", 7),
    (4, "PRECINTO ROTULADOR", 8),
    (5, "DROP QUE INGRESA AL DOMICILIO", 9),
    (6, "ANCLAJE", 10),
    (7, "ROSETA + MEDICION POTENCIA", 11),
    (8, "MAC ONT", 12),
    (9, "ONT", 13),
    (10, "TEST DE VELOCIDAD", 14),
    (11, "ACTA DE INSTALACION", 15),
]

# INTERNA (1..9)
INTERNA_MENU: List[Tuple[int, str, int]] = [
    (1, "FACHADA", 5),
    (2, "CTO", 6),
    (3, "POTENCIA EN CTO", 7),
    (4, "PRECINTO ROTULADOR", 8),
    (5, "ROSETA + MEDICION POTENCIA", 11),
    (6, "MAC ONT", 12),
    (7, "ONT", 13),
    (8, "TEST DE VELOCIDAD", 14),
    (9, "ACTA DE INSTALACION", 15),
]

STEP_MEDIA_DEFS = {
    5:  ("FACHADA", "Envía foto de Fachada con placa de dirección y/o suministro eléctrico"),
    6:  ("CTO", "Envía foto panorámica de la CTO o FAT rotulada"),
    7:  ("POTENCIA EN CTO", "Envía la foto de la medida de potencia del puerto a utilizar"),
    8:  ("PRECINTO ROTULADOR", "Envía la foto del cintillo rotulado identificando al cliente (DNI o CE y nro de puerto)"),
    9:  ("FALSO TRAMO", "Envía foto del tramo de ingreso al domicilio"),
    10: ("ANCLAJE", "Envía foto del punto de anclaje de la fibra drop en el domicilio"),
    11: ("ROSETA + MEDICION POTENCIA", "Envía foto de la roseta abierta y medición de potencia"),
    12: ("MAC ONT", "Envía foto de la MAC (Etiqueta) de la ONT y/o equipos usados"),
    13: ("ONT", "Envía foto panorámica de la ONT operativa"),
    14: ("TEST DE VELOCIDAD", "Envía foto del test de velocidad App Speedtest mostrar ID y fecha claramente"),
    15: ("ACTA DE INSTALACION", "Envía foto del acta de instalación completa con la firma de cliente y datos llenos"),
}

# =========================
# Google Sheets CONFIG
# =========================
SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()
BOT_VERSION = os.getenv("BOT_VERSION", "1.0.0").strip()

# Tabs existentes (historial)
CASOS_COLUMNS = [
    "case_id",
    "estado",
    "chat_id_origen",
    "fecha_inicio",
    "hora_inicio",
    "fecha_cierre",
    "hora_cierre",
    "duracion_min",
    "tecnico_nombre",
    "tecnico_user_id",
    "tipo_servicio",
    "codigo_abonado",
    "modo_instalacion",
    "latitud",
    "longitud",
    "link_maps",
    "total_pasos",
    "pasos_aprobados",
    "pasos_rechazados",
    "total_evidencias",
    "requiere_aprobacion",
    "registrado_en",
    "version_bot",
    "paso_actual",
    "bloqueado_por_user_id",
    "bloqueado_por_nombre",
    "bloqueado_desde",
    "bloqueo_expira",
    "admin_pendiente",
]

DETALLE_PASOS_COLUMNS = [
    "case_id",
    "paso_numero",
    "paso_nombre",
    "attempt",
    "estado_paso",
    "revisado_por",
    "fecha_revision",
    "hora_revision",
    "motivo_rechazo",
    "cantidad_fotos",
    "ids_mensajes",
    "tomado_por_user_id",
    "tomado_por_nombre",
    "tomado_desde",
    "reabierto_por",
    "fecha_reapertura",
    "hora_reapertura",
    "motivo_reapertura",
    "bloqueado",
]

EVIDENCIAS_COLUMNS = [
    "case_id",
    "paso_numero",
    "attempt",
    "file_id",
    "file_unique_id",
    "mensaje_telegram_id",
    "fecha_carga",
    "hora_carga",
    "grupo_evidencias",
]

CONFIG_COLUMNS = ["parametro", "valor"]

# Tabs nuevas (config pro)
TECNICOS_TAB = "TECNICOS"
ROUTING_TAB = "ROUTING"
PAIRING_TAB = "PAIRING"

TECNICOS_COLUMNS = ["nombre", "activo", "orden", "alias", "updated_at"]
ROUTING_COLUMNS = ["origin_chat_id", "evidence_chat_id", "summary_chat_id", "alias", "activo", "updated_by", "updated_at"]
PAIRING_COLUMNS = ["code", "origin_chat_id", "purpose", "expires_at", "used", "created_by", "created_at", "used_by", "used_at"]

# Cache/refresh
TECH_CACHE_TTL_SEC = int(os.getenv("TECH_CACHE_TTL_SEC", "180"))
ROUTING_CACHE_TTL_SEC = int(os.getenv("ROUTING_CACHE_TTL_SEC", "180"))
PAIRING_TTL_MINUTES = int(os.getenv("PAIRING_TTL_MINUTES", "10"))

# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("tufibra_bot")

# =========================
# Safe Telegram helpers
# =========================
async def safe_q_answer(q, text: Optional[str] = None, show_alert: bool = False) -> None:
    if q is None:
        return
    try:
        await q.answer(text=text, show_alert=show_alert, cache_time=0)
    except BadRequest as e:
        msg = str(e).lower()
        if "query is too old" in msg or "response timeout expired" in msg or "query id is invalid" in msg:
            return
        if "invalid callback query" in msg:
            return
        log.warning(f"safe_q_answer BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_q_answer error: {e}")


async def safe_edit_message_text(q, text: str, **kwargs) -> None:
    if q is None:
        return
    try:
        await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if "message to edit not found" in msg:
            return
        if "query is too old" in msg or "response timeout expired" in msg or "query id is invalid" in msg:
            return
        log.warning(f"safe_edit_message_text BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_edit_message_text error: {e}")

# =========================
# DB helpers
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(dt_s: str) -> Optional[datetime]:
    if not dt_s:
        return None
    try:
        d = datetime.fromisoformat(dt_s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def fmt_time_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%H:%M")


def fmt_date_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%Y-%m-%d")


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)


def lock_expires_at_iso(minutes: int = STEP_LOCK_TIMEOUT_MINUTES) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def init_db():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS cases (
                case_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER,
                username TEXT,
                created_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                step_index INTEGER NOT NULL,
                phase TEXT,
                pending_step_no INTEGER,
                technician_name TEXT,
                technician_user_id INTEGER,
                service_type TEXT,
                abonado_code TEXT,
                location_lat REAL,
                location_lon REAL,
                location_at TEXT,
                install_mode TEXT,
                current_step_no INTEGER,
                locked_by_user_id INTEGER,
                locked_by_name TEXT,
                locked_at TEXT,
                lock_expires_at TEXT,
                admin_pending INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cases_open_chat ON cases(chat_id, status);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_config (
                chat_id INTEGER PRIMARY KEY,
                approval_required INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS step_state (
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                submitted INTEGER NOT NULL DEFAULT 0,
                approved INTEGER,
                reviewed_by INTEGER,
                reviewed_at TEXT,
                created_at TEXT NOT NULL,
                reject_reason TEXT,
                reject_reason_by INTEGER,
                reject_reason_at TEXT,
                state_name TEXT NOT NULL DEFAULT 'PENDIENTE',
                taken_by_user_id INTEGER,
                taken_by_name TEXT,
                taken_at TEXT,
                reopened_by TEXT,
                reopened_at TEXT,
                reopen_reason TEXT,
                blocked INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(case_id, step_no, attempt),
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                media_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                file_type TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_unique_id TEXT,
                tg_message_id INTEGER NOT NULL,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_case_step ON media(case_id, step_no, attempt);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_case_step_msg ON media(case_id, step_no, attempt, tg_message_id);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_text (
                auth_id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                text TEXT NOT NULL,
                tg_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(case_id) REFERENCES cases(case_id)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_text_case_step ON auth_text(case_id, step_no, attempt);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_inputs (
                pending_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                reply_to_message_id INTEGER,
                tech_user_id INTEGER
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_inputs ON pending_inputs(chat_id, user_id, kind);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sheet_outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT NOT NULL,
                op_type TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                row_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON sheet_outbox(status, next_retry_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_key ON sheet_outbox(sheet_name, dedupe_key);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media_ack_buffer (
                ack_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                case_id INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                phase TEXT NOT NULL,
                created_by_user_id INTEGER NOT NULL,
                created_by_name TEXT,
                count_media INTEGER NOT NULL DEFAULT 0,
                last_media_at TEXT,
                ack_status TEXT NOT NULL DEFAULT 'PENDING'
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_ack_buffer ON media_ack_buffer(chat_id, case_id, step_no, attempt, phase, ack_status);")

        # Soft migrations cases
        for col, ddl in [
            ("finished_at", "TEXT"),
            ("phase", "TEXT"),
            ("pending_step_no", "INTEGER"),
            ("technician_name", "TEXT"),
            ("technician_user_id", "INTEGER"),
            ("service_type", "TEXT"),
            ("abonado_code", "TEXT"),
            ("location_lat", "REAL"),
            ("location_lon", "REAL"),
            ("location_at", "TEXT"),
            ("install_mode", "TEXT"),
            ("current_step_no", "INTEGER"),
            ("locked_by_user_id", "INTEGER"),
            ("locked_by_name", "TEXT"),
            ("locked_at", "TEXT"),
            ("lock_expires_at", "TEXT"),
            ("admin_pending", "INTEGER NOT NULL DEFAULT 0"),
        ]:
            if not _col_exists(conn, "cases", col):
                conn.execute(f"ALTER TABLE cases ADD COLUMN {col} {ddl};")

        # Soft migrations step_state
        for col, ddl in [
            ("reject_reason", "TEXT"),
            ("reject_reason_by", "INTEGER"),
            ("reject_reason_at", "TEXT"),
            ("state_name", "TEXT NOT NULL DEFAULT 'PENDIENTE'"),
            ("taken_by_user_id", "INTEGER"),
            ("taken_by_name", "TEXT"),
            ("taken_at", "TEXT"),
            ("reopened_by", "TEXT"),
            ("reopened_at", "TEXT"),
            ("reopen_reason", "TEXT"),
            ("blocked", "INTEGER NOT NULL DEFAULT 0"),
        ]:
            if not _col_exists(conn, "step_state", col):
                conn.execute(f"ALTER TABLE step_state ADD COLUMN {col} {ddl};")

        # Soft migrations pending_inputs
        for col, ddl in [
            ("reply_to_message_id", "INTEGER"),
            ("tech_user_id", "INTEGER"),
        ]:
            if not _col_exists(conn, "pending_inputs", col):
                conn.execute(f"ALTER TABLE pending_inputs ADD COLUMN {col} {ddl};")

        conn.commit()


def set_approval_required(chat_id: int, required: bool):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO chat_config(chat_id, approval_required, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(chat_id) DO UPDATE
              SET approval_required=excluded.approval_required, updated_at=excluded.updated_at
            """,
            (chat_id, 1 if required else 0, now_utc()),
        )
        conn.commit()


def get_approval_required(chat_id: int) -> bool:
    with db() as conn:
        row = conn.execute("SELECT approval_required FROM chat_config WHERE chat_id=?", (chat_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT OR IGNORE INTO chat_config(chat_id, approval_required, updated_at) VALUES(?,?,?)",
                (chat_id, 1, now_utc()),
            )
            conn.commit()
            return True
        return bool(row["approval_required"])


def get_open_case(chat_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM cases WHERE chat_id=? AND status='OPEN' ORDER BY case_id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()


def get_case(case_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM cases WHERE case_id=?", (case_id,)).fetchone()


def update_case(case_id: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE cases SET {sets} WHERE case_id=?", (*vals, case_id))
        conn.commit()


def clear_case_lock(case_id: int):
    update_case(
        case_id,
        locked_by_user_id=None,
        locked_by_name=None,
        locked_at=None,
        lock_expires_at=None,
    )


def lock_case_step(case_id: int, user_id: int, user_name: str):
    update_case(
        case_id,
        locked_by_user_id=user_id,
        locked_by_name=user_name,
        locked_at=now_utc(),
        lock_expires_at=lock_expires_at_iso(),
    )


def is_case_lock_expired(case_row: sqlite3.Row) -> bool:
    exp = parse_iso(case_row["lock_expires_at"] or "")
    if not exp:
        return True
    return datetime.now(timezone.utc) > exp


def maybe_release_expired_case_lock(case_row: Optional[sqlite3.Row]) -> Optional[sqlite3.Row]:
    if not case_row:
        return None
    if case_row["locked_by_user_id"] and is_case_lock_expired(case_row):
        clear_case_lock(int(case_row["case_id"]))
        return get_case(int(case_row["case_id"]))
    return case_row


def create_or_reset_case(chat_id: int, user_id: int, username: str) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM cases WHERE chat_id=? AND status='OPEN' ORDER BY case_id DESC LIMIT 1",
            (chat_id,),
        ).fetchone()

        if row:
            conn.execute(
                """
                UPDATE cases
                SET user_id=?,
                    username=?,
                    created_at=?,
                    finished_at=NULL,
                    status='OPEN',
                    step_index=0,
                    phase=?,
                    pending_step_no=NULL,
                    technician_name=NULL,
                    technician_user_id=NULL,
                    service_type=NULL,
                    abonado_code=NULL,
                    location_lat=NULL,
                    location_lon=NULL,
                    location_at=NULL,
                    install_mode=NULL,
                    current_step_no=NULL,
                    locked_by_user_id=NULL,
                    locked_by_name=NULL,
                    locked_at=NULL,
                    lock_expires_at=NULL,
                    admin_pending=0
                WHERE case_id=?
                """,
                (user_id, username, now_utc(), PHASE_WAIT_TECHNICIAN, row["case_id"]),
            )
            conn.execute("DELETE FROM step_state WHERE case_id=?", (row["case_id"],))
            conn.execute("DELETE FROM media WHERE case_id=?", (row["case_id"],))
            conn.execute("DELETE FROM auth_text WHERE case_id=?", (row["case_id"],))
            conn.execute("DELETE FROM media_ack_buffer WHERE case_id=?", (row["case_id"],))
            conn.commit()
            return get_case(int(row["case_id"]))

        conn.execute(
            """
            INSERT INTO cases(
                chat_id, user_id, username, created_at, finished_at, status, step_index, phase, pending_step_no,
                technician_name, technician_user_id, service_type, abonado_code, location_lat, location_lon, location_at,
                install_mode, current_step_no, locked_by_user_id, locked_by_name, locked_at, lock_expires_at, admin_pending
            )
            VALUES(
                ?,?,?,?,NULL,'OPEN',0,?,NULL,
                NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                NULL,NULL,NULL,NULL,NULL,NULL,0
            )
            """,
            (chat_id, user_id, username, now_utc(), PHASE_WAIT_TECHNICIAN),
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        return get_case(int(new_id))


# =========================
# Routing (Sheets cache + fallback)
# =========================
def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def get_route_for_chat_cached(application: Application, origin_chat_id: int) -> Dict[str, Optional[int]]:
    try:
        rc = application.bot_data.get("routing_cache") or {}
        row = rc.get(int(origin_chat_id))
        if row and int(row.get("activo", 1)) == 1:
            return {
                "evidence": _safe_int(row.get("evidence_chat_id")),
                "summary": _safe_int(row.get("summary_chat_id")),
            }
    except Exception:
        pass

    if ROUTING_JSON:
        try:
            mapping = json.loads(ROUTING_JSON)
            cfg = mapping.get(str(origin_chat_id)) or {}
            ev = cfg.get("evidence")
            sm = cfg.get("summary")
            return {"evidence": int(ev) if ev else None, "summary": int(sm) if sm else None}
        except Exception as e:
            log.warning(f"ROUTING_JSON inválido: {e}")

    return {"evidence": None, "summary": None}


async def maybe_copy_to_group(
    context: ContextTypes.DEFAULT_TYPE,
    dest_chat_id: Optional[int],
    file_type: str,
    file_id: str,
    caption: str,
):
    if not dest_chat_id:
        return
    try:
        if file_type == "video":
            await context.bot.send_video(chat_id=dest_chat_id, video=file_id, caption=caption[:1024])
        else:
            await context.bot.send_photo(chat_id=dest_chat_id, photo=file_id, caption=caption[:1024])
    except Exception as e:
        log.warning(f"No pude copiar evidencia a destino {dest_chat_id}: {e}")


# =========================
# Step state helpers
# =========================
def get_mode_items(mode: str) -> List[Tuple[int, str, int]]:
    return EXTERNA_MENU if mode == "EXTERNA" else INTERNA_MENU


def step_name(step_no: int) -> str:
    return STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}", ""))[0]


def is_last_step(mode: str, step_no: int) -> bool:
    items = get_mode_items(mode)
    return step_no == items[-1][2]


def _max_attempt(case_id: int, step_no: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT MAX(attempt) AS mx FROM step_state WHERE case_id=? AND step_no=?",
            (case_id, step_no),
        ).fetchone()
        mx = row["mx"] if row and row["mx"] is not None else 0
        return int(mx) if mx else 0


def get_latest_step_state(case_id: int, step_no: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=?
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()


def get_active_unsubmitted_step_state(case_id: int, step_no: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=? AND submitted=0
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()


def ensure_step_state(case_id: int, step_no: int, *, owner_user_id: Optional[int] = None, owner_name: Optional[str] = None) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=? AND submitted=0
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()
        if row:
            return row

        prev = conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=?
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()

        attempt = (_max_attempt(case_id, step_no) + 1)
        initial_state = STEP_STATE_REABIERTO if (prev and prev["approved"] is not None and int(prev["approved"]) == 1) else STEP_STATE_EN_CARGA

        conn.execute(
            """
            INSERT INTO step_state(
                case_id, step_no, attempt, submitted, approved, reviewed_by, reviewed_at, created_at,
                reject_reason, reject_reason_by, reject_reason_at, state_name,
                taken_by_user_id, taken_by_name, taken_at, reopened_by, reopened_at, reopen_reason, blocked
            )
            VALUES(?,?,?,0,NULL,NULL,NULL,?,NULL,NULL,NULL,?,?,?,?,NULL,NULL,NULL,0)
            """,
            (
                case_id,
                step_no,
                attempt,
                now_utc(),
                initial_state,
                owner_user_id,
                owner_name,
                now_utc() if owner_user_id else None,
            ),
        )
        conn.commit()
        return conn.execute(
            "SELECT * FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
            (case_id, step_no, attempt),
        ).fetchone()


def set_step_owner(case_id: int, step_no: int, attempt: int, user_id: int, user_name: str):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET taken_by_user_id=?, taken_by_name=?, taken_at=?, state_name=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (user_id, user_name, now_utc(), STEP_STATE_EN_CARGA, case_id, step_no, attempt),
        )
        conn.commit()


def set_step_state_name(case_id: int, step_no: int, attempt: int, state_name: str):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET state_name=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (state_name, case_id, step_no, attempt),
        )
        conn.commit()


def mark_step_blocked_from(case_id: int, from_step_no: int, mode: str, blocked: int):
    items = get_mode_items(mode)
    affected = [step_no for _, _, step_no in items if step_no > from_step_no]
    if not affected:
        return
    with db() as conn:
        for step_no in affected:
            row = conn.execute(
                """
                SELECT * FROM step_state
                WHERE case_id=? AND step_no=?
                ORDER BY attempt DESC LIMIT 1
                """,
                (case_id, step_no),
            ).fetchone()
            if not row:
                continue
            conn.execute(
                "UPDATE step_state SET blocked=? WHERE case_id=? AND step_no=? AND attempt=?",
                (1 if blocked else 0, case_id, step_no, row["attempt"]),
            )
        conn.commit()


def get_latest_submitted_state(case_id: int, step_no: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM step_state
            WHERE case_id=? AND step_no=? AND submitted=1
            ORDER BY attempt DESC LIMIT 1
            """,
            (case_id, step_no),
        ).fetchone()


def get_effective_step_status(case_id: int, step_no: int) -> str:
    row = get_latest_step_state(case_id, step_no)
    if not row:
        return STEP_STATE_PENDIENTE
    state_name = (row["state_name"] or "").strip().upper() or STEP_STATE_PENDIENTE
    if int(row["blocked"] or 0) == 1:
        return STEP_STATE_BLOQUEADO
    return state_name


def compute_next_required_step(case_id: int, mode: str) -> Tuple[int, str, int, str]:
    items = get_mode_items(mode)
    for num, label, step_no in items:
        st = get_effective_step_status(case_id, step_no)
        if st != STEP_STATE_APROBADO:
            return (num, label, step_no, st)
    last_num, last_label, last_step = items[-1]
    return (last_num, last_label, last_step, STEP_STATE_APROBADO)


def media_count(case_id: int, step_no: int, attempt: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM media WHERE case_id=? AND step_no=? AND attempt=?",
            (case_id, step_no, attempt),
        ).fetchone()
        return int(row["c"]) if row else 0


def media_message_ids(case_id: int, step_no: int, attempt: int) -> List[int]:
    with db() as conn:
        rows = conn.execute(
            "SELECT tg_message_id FROM media WHERE case_id=? AND step_no=? AND attempt=? ORDER BY media_id ASC",
            (case_id, step_no, attempt),
        ).fetchall()
        return [int(r["tg_message_id"]) for r in rows] if rows else []


def total_media_for_case(case_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM media WHERE case_id=? AND step_no > 0",
            (case_id,),
        ).fetchone()
        return int(row["c"] or 0)


def total_rejects_for_case(case_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM step_state WHERE case_id=? AND step_no > 0 AND approved=0",
            (case_id,),
        ).fetchone()
        return int(row["c"] or 0)


def total_approved_steps_for_case(case_id: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM step_state WHERE case_id=? AND step_no > 0 AND approved=1",
            (case_id,),
        ).fetchone()
        return int(row["c"] or 0)


def add_media(
    case_id: int,
    step_no: int,
    attempt: int,
    file_type: str,
    file_id: str,
    file_unique_id: Optional[str],
    tg_message_id: int,
    meta: Dict[str, Any],
):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO media(case_id, step_no, attempt, file_type, file_id, file_unique_id, tg_message_id, meta_json, created_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                case_id,
                step_no,
                attempt,
                file_type,
                file_id,
                file_unique_id or "",
                tg_message_id,
                json.dumps(meta, ensure_ascii=False),
                now_utc(),
            ),
        )
        conn.commit()


def mark_submitted(case_id: int, step_no: int, attempt: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET submitted=1, state_name=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (STEP_STATE_EN_REVISION, case_id, step_no, attempt),
        )
        conn.commit()


def set_review(case_id: int, step_no: int, attempt: int, approved: int, reviewer_id: int):
    state_name = STEP_STATE_APROBADO if int(approved) == 1 else STEP_STATE_RECHAZADO
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET approved=?, reviewed_by=?, reviewed_at=?, state_name=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (approved, reviewer_id, now_utc(), state_name, case_id, step_no, attempt),
        )
        conn.commit()


def set_reject_reason(case_id: int, step_no: int, attempt: int, reason: str, reviewer_id: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET reject_reason=?, reject_reason_by=?, reject_reason_at=?, state_name=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (reason, reviewer_id, now_utc(), STEP_STATE_RECHAZADO, case_id, step_no, attempt),
        )
        conn.commit()


def reopen_step(case_id: int, step_no: int, admin_name: str, reason: str, mode: str) -> sqlite3.Row:
    prev = get_latest_step_state(case_id, step_no)
    if not prev:
        raise RuntimeError("No existe un intento previo para reabrir.")
    if prev["approved"] is None or int(prev["approved"]) != 1:
        raise RuntimeError("Solo se puede reabrir un paso aprobado.")

    with db() as conn:
        attempt = _max_attempt(case_id, step_no) + 1
        conn.execute(
            """
            INSERT INTO step_state(
                case_id,
                step_no,
                attempt,
                submitted,
                approved,
                reviewed_by,
                reviewed_at,
                created_at,
                reject_reason,
                reject_reason_by,
                reject_reason_at,
                state_name,
                taken_by_user_id,
                taken_by_name,
                taken_at,
                reopened_by,
                reopened_at,
                reopen_reason,
                blocked
            )
            VALUES(
                ?, ?, ?, 
                0, NULL, NULL, NULL,
                ?, 
                NULL, NULL, NULL,
                ?, 
                NULL, NULL, NULL,
                ?, ?, ?,
                0
            )
            """,
            (
                case_id,
                step_no,
                attempt,
                now_utc(),
                STEP_STATE_REABIERTO,
                admin_name,
                now_utc(),
                reason,
            ),
        )
        conn.commit()

    mark_step_blocked_from(case_id, step_no, mode, True)
    return get_latest_step_state(case_id, step_no)

def save_auth_text(case_id: int, auth_step_no: int, attempt: int, text: str, tg_message_id: int):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO auth_text(case_id, step_no, attempt, text, tg_message_id, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (case_id, auth_step_no, attempt, text, tg_message_id, now_utc()),
        )
        conn.commit()


def set_pending_input(
    chat_id: int,
    user_id: int,
    kind: str,
    case_id: int,
    step_no: int,
    attempt: int,
    reply_to_message_id: Optional[int] = None,
    tech_user_id: Optional[int] = None,
):
    with db() as conn:
        conn.execute("DELETE FROM pending_inputs WHERE chat_id=? AND user_id=? AND kind=?", (chat_id, user_id, kind))
        conn.execute(
            """
            INSERT INTO pending_inputs(chat_id, user_id, kind, case_id, step_no, attempt, created_at, reply_to_message_id, tech_user_id)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (chat_id, user_id, kind, case_id, step_no, attempt, now_utc(), reply_to_message_id, tech_user_id),
        )
        conn.commit()


def pop_pending_input(chat_id: int, user_id: int, kind: str) -> Optional[sqlite3.Row]:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM pending_inputs WHERE chat_id=? AND user_id=? AND kind=? ORDER BY pending_id DESC LIMIT 1",
            (chat_id, user_id, kind),
        ).fetchone()
        if row:
            conn.execute("DELETE FROM pending_inputs WHERE pending_id=?", (row["pending_id"],))
            conn.commit()
        return row


def upsert_media_ack_buffer(chat_id: int, case_id: int, step_no: int, attempt: int, phase: str, user_id: int, user_name: str):
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM media_ack_buffer
            WHERE chat_id=? AND case_id=? AND step_no=? AND attempt=? AND phase=? AND ack_status='PENDING'
            ORDER BY ack_id DESC LIMIT 1
            """,
            (chat_id, case_id, step_no, attempt, phase),
        ).fetchone()

        if row:
            count_media = int(row["count_media"] or 0) + 1
            conn.execute(
                """
                UPDATE media_ack_buffer
                SET count_media=?, last_media_at=?, created_by_user_id=?, created_by_name=?
                WHERE ack_id=?
                """,
                (count_media, now_utc(), user_id, user_name, row["ack_id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO media_ack_buffer(chat_id, case_id, step_no, attempt, phase, created_by_user_id, created_by_name, count_media, last_media_at, ack_status)
                VALUES(?,?,?,?,?,?,?,?,?, 'PENDING')
                """,
                (chat_id, case_id, step_no, attempt, phase, user_id, user_name, 1, now_utc()),
            )
        conn.commit()


def get_pending_media_ack_buffers() -> List[sqlite3.Row]:
    limit_dt = datetime.now(timezone.utc) - timedelta(seconds=MEDIA_ACK_WINDOW_SECONDS)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM media_ack_buffer
            WHERE ack_status='PENDING' AND last_media_at <= ?
            ORDER BY ack_id ASC
            """,
            (limit_dt.isoformat(),),
        ).fetchall()
        return rows


def mark_media_ack_sent(ack_id: int):
    with db() as conn:
        conn.execute("UPDATE media_ack_buffer SET ack_status='SENT' WHERE ack_id=?", (ack_id,))
        conn.commit()


def duration_minutes(created_at: str, finished_at: str) -> Optional[int]:
    a = parse_iso(created_at)
    b = parse_iso(finished_at)
    if not a or not b:
        return None
    seconds = int((b - a).total_seconds())
    if seconds < 0:
        return None
    return max(0, seconds // 60)


def can_user_operate_current_step(case_row: sqlite3.Row, user_id: int) -> Tuple[bool, str]:
    case_row = maybe_release_expired_case_lock(case_row)
    if not case_row:
        return False, "No hay un caso activo."
    if int(case_row["admin_pending"] or 0) == 1:
        return False, "⏳ El paso actual está en revisión del admin."
    lock_user = case_row["locked_by_user_id"]
    if lock_user and int(lock_user) != int(user_id):
        name = case_row["locked_by_name"] or "otro técnico"
        return False, f"🔒 Este paso está siendo trabajado por {name}."
    return True, ""


def sync_case_progress(case_id: int):
    case_row = get_case(case_id)
    if not case_row:
        return
    mode = (case_row["install_mode"] or "").strip()
    if mode not in ("EXTERNA", "INTERNA"):
        return
    _, _, next_step_no, next_status = compute_next_required_step(case_id, mode)
    update_fields = {
        "current_step_no": None if next_status == STEP_STATE_APROBADO else next_step_no,
        "pending_step_no": None if next_status == STEP_STATE_APROBADO else next_step_no,
        "admin_pending": 1 if next_status == STEP_STATE_EN_REVISION else 0,
    }
    update_case(case_id, **update_fields)

# =========================
# Outbox helpers (Google Sheets - historial)
# =========================
def outbox_enqueue(sheet_name: str, op_type: str, dedupe_key: str, row: Dict[str, Any]):
    now = now_utc()
    row_json = json.dumps(row, ensure_ascii=False)
    with db() as conn:
        existing = conn.execute(
            """
            SELECT outbox_id, status FROM sheet_outbox
            WHERE sheet_name=? AND dedupe_key=? AND status IN ('PENDING','FAILED')
            ORDER BY outbox_id DESC LIMIT 1
            """,
            (sheet_name, dedupe_key),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE sheet_outbox
                SET row_json=?, op_type=?, status='PENDING', last_error=NULL, next_retry_at=NULL, updated_at=?
                WHERE outbox_id=?
                """,
                (row_json, op_type, now, int(existing["outbox_id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO sheet_outbox(sheet_name, op_type, dedupe_key, row_json, status, attempts, last_error, next_retry_at, created_at, updated_at)
                VALUES(?,?,?,?, 'PENDING', 0, NULL, NULL, ?, NULL)
                """,
                (sheet_name, op_type, dedupe_key, row_json, now),
            )
        conn.commit()


def outbox_fetch_batch(limit: int = 20) -> List[sqlite3.Row]:
    now = now_utc()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM sheet_outbox
            WHERE status IN ('PENDING','FAILED')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
        return rows


def outbox_mark_sent(outbox_id: int):
    with db() as conn:
        conn.execute(
            "UPDATE sheet_outbox SET status='SENT', updated_at=? WHERE outbox_id=?",
            (now_utc(), outbox_id),
        )
        conn.commit()


def _next_retry_time(attempts: int) -> str:
    minutes = [1, 2, 4, 8, 15, 30, 60, 120]
    idx = min(attempts, len(minutes) - 1)
    dt = datetime.now(timezone.utc) + timedelta(minutes=minutes[idx])
    return dt.isoformat()


def outbox_mark_failed(outbox_id: int, attempts: int, err: str, dead: bool = False):
    status = "DEAD" if dead else "FAILED"
    next_retry_at = None if dead else _next_retry_time(attempts)
    with db() as conn:
        conn.execute(
            """
            UPDATE sheet_outbox
            SET status=?, attempts=?, last_error=?, next_retry_at=?, updated_at=?
            WHERE outbox_id=?
            """,
            (status, attempts, err[:500], next_retry_at, now_utc(), outbox_id),
        )
        conn.commit()


# =========================
# Google Sheets helpers
# =========================
def sheets_client():
    if not SHEET_ID:
        raise RuntimeError("Falta SHEET_ID. Configura la variable SHEET_ID.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    if GOOGLE_CREDS_JSON_TEXT:
        creds_info = json.loads(GOOGLE_CREDS_JSON_TEXT)
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    else:
        if not GOOGLE_CREDS_JSON:
            raise RuntimeError("Falta GOOGLE_CREDS_JSON o GOOGLE_CREDS_JSON_TEXT.")
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh


def _ensure_headers(ws, expected_headers: List[str]):
    values = ws.get_all_values()
    if not values:
        ws.append_row(expected_headers, value_input_option="RAW")
        return
    headers = values[0]
    for h in expected_headers:
        if h not in headers:
            raise RuntimeError(f"Falta columna '{h}' en hoja '{ws.title}'. No modifiques headers.")


def build_index(ws, key_cols: List[str]) -> Dict[str, int]:
    values = ws.get_all_values()
    if not values:
        return {}
    headers = values[0]
    col_idx = {h: i for i, h in enumerate(headers)}
    for c in key_cols:
        if c not in col_idx:
            raise RuntimeError(f"Falta columna '{c}' en hoja '{ws.title}'")

    idx: Dict[str, int] = {}
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        parts: List[str] = []
        for c in key_cols:
            i = col_idx[c]
            parts.append(row[i] if i < len(row) else "")
        k = "|".join(parts).strip()
        if k:
            idx[k] = r
    return idx


def row_to_values(row: Dict[str, Any], columns: List[str]) -> List[Any]:
    return [row.get(c, "") for c in columns]


def _col_index_map(ws) -> Dict[str, int]:
    values = ws.get_all_values()
    if not values:
        return {}
    headers = values[0]
    return {h: i + 1 for i, h in enumerate(headers)}


def _a1(col: int, row: int) -> str:
    letters = ""
    n = col
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row}"


def sheet_upsert(ws, index: Dict[str, int], key: str, row: Dict[str, Any], columns: List[str], key_cols: List[str]):
    _ensure_headers(ws, columns)
    col_map = _col_index_map(ws)

    for kc in key_cols:
        if kc not in col_map:
            raise RuntimeError(f"Falta columna clave '{kc}' en hoja '{ws.title}'")

    values = row_to_values(row, columns)

    if key in index:
        r = index[key]
        start = _a1(1, r)
        end = _a1(len(columns), r)
        ws.update(f"{start}:{end}", [values], value_input_option="RAW")
    else:
        ws.append_row(values, value_input_option="RAW")
        last_row = len(ws.get_all_values())
        index[key] = last_row


def _is_permanent_sheet_error(err: str) -> bool:
    low = err.lower()
    if "not found" in low and "worksheet" in low:
        return True
    if "invalid" in low and "credentials" in low:
        return True
    if "permission" in low or "insufficient" in low:
        return True
    return False


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _parse_bool01(v: Any) -> int:
    s = str(v).strip().lower()
    if s in ("1", "true", "si", "sí", "on", "activo", "yes"):
        return 1
    return 0


def _parse_int_or_default(v: Any, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_all_records(ws) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception:
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return []
        headers = values[0]
        out: List[Dict[str, Any]] = []
        for r in values[1:]:
            d = {}
            for i, h in enumerate(headers):
                d[h] = r[i] if i < len(r) else ""
            out.append(d)
        return out


def _find_row_index_by_column(ws, col_name: str, target: str) -> Optional[int]:
    values = ws.get_all_values()
    if not values:
        return None
    headers = values[0]
    try:
        ci = headers.index(col_name)
    except ValueError:
        return None
    for idx in range(2, len(values) + 1):
        row = values[idx - 1]
        val = row[ci] if ci < len(row) else ""
        if str(val).strip() == str(target).strip():
            return idx
    return None


def _update_cells_by_headers(ws, row_index: int, updates: Dict[str, Any]) -> None:
    values = ws.get_all_values()
    if not values:
        raise RuntimeError("Hoja vacía, no puedo actualizar.")
    headers = values[0]
    col_map = {h: i + 1 for i, h in enumerate(headers)}
    for k, v in updates.items():
        if k not in col_map:
            raise RuntimeError(f"Falta columna '{k}' en hoja '{ws.title}'")
        ws.update_cell(row_index, col_map[k], v)

def get_config_value(app: Application, param: str) -> Optional[str]:
    ws = app.bot_data.get("ws_config")
    if not ws:
        return None

    rows = _read_all_records(ws)
    for r in rows:
        if str(r.get("parametro", "")).strip() == str(param).strip():
            val = str(r.get("valor", "")).strip()
            return val if val else None

    return None

async def send_step_guide(context: ContextTypes.DEFAULT_TYPE, chat_id: int, step_no: int) -> None:
    guide_map = {
        5: "GUIA_FACHADA",
        6: "GUIA_CTO",
        7: "GUIA_POTENCIA_CTO",
        8: "GUIA_PRECINTO_ROTULADOR",
        9: "GUIA_FALSO_TRAMO",
        10: "GUIA_ANCLAJE",
        11: "GUIA_ROSETA_POTENCIA",
        12: "GUIA_MAC_ONT",
        13: "GUIA_ONT",
        14: "GUIA_TEST_VELOCIDAD",
        15: "GUIA_ACTA_INSTALACION",
    }

    guide_notes = {
        5: "Foto panoramica de la casa, debe verse toda la fachada y si hay placa tomar una segunda foto.",
        6: "Debe verse claramente el rotulado de la CTO",
        7: "La pantalla del power meter debe ser legible y verse en patchord conectado en el puerto. Pueden ser mas de 1 foto",
        8: "Debe verse el precinto y el rotulado correctamente.",
        9: "Debe verse el drop que ingresa al domicilio.",
        10: "Debe verse el anclaje y los templadores correctamente colocados",
        11: "Debe verse la roseta (con 4 vueltas) y la medición de potencia.",
        12: "La etiqueta MAC de la ONT debe ser legible.",
        13: "Debe verse la ONT instalada y conectada. Foto debe ser panoramica",
        14: "Debe verse la la fecha y el id del test de velocidad",
        15: "Debe verse el acta completa con firma. No debe taparse ningun dato. Tomar foto con camara directa sin Timestmap.",
    }

    param = guide_map.get(step_no)
    if not param:
        return

    file_id = get_config_value(context.application, param)
    if not file_id:
        return

    note = guide_notes.get(step_no, "")

    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=file_id,
            caption=f"📷 Ejemplo de foto correcta\n\n⚠️ {note}",
        )
    except Exception as e:
        log.warning(f"No pude enviar foto guía del paso {step_no}: {e}")

# =========================
# Sheets config cache loaders
# =========================
def load_tecnicos_cache(app: Application) -> None:
    if not app.bot_data.get("sheets_ready"):
        return
    ws = app.bot_data.get("ws_tecnicos")
    if not ws:
        return
    try:
        _ensure_headers(ws, TECNICOS_COLUMNS)
        rows = _read_all_records(ws)
        techs: List[Dict[str, Any]] = []
        for r in rows:
            nombre = _safe_str(r.get("nombre"))
            if not nombre:
                continue
            activo = _parse_bool01(r.get("activo"))
            if activo != 1:
                continue
            alias = _safe_str(r.get("alias"))
            orden = _parse_int_or_default(r.get("orden"), 9999)
            techs.append({"nombre": nombre, "alias": alias, "orden": orden})
        techs.sort(key=lambda x: (x.get("orden", 9999), x.get("nombre", "")))
        app.bot_data["tech_cache"] = techs
        app.bot_data["tech_cache_at"] = time.time()
        log.info(f"TECNICOS cache actualizado: {len(techs)} activos.")
    except Exception as e:
        log.warning(f"TECNICOS cache error: {e}")


def load_routing_cache(app: Application) -> None:
    if not app.bot_data.get("sheets_ready"):
        return
    ws = app.bot_data.get("ws_routing")
    if not ws:
        return
    try:
        _ensure_headers(ws, ROUTING_COLUMNS)
        rows = _read_all_records(ws)
        m: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            origin = _safe_int(r.get("origin_chat_id"))
            if origin is None:
                continue
            activo = _parse_bool01(r.get("activo"))
            m[int(origin)] = {
                "origin_chat_id": int(origin),
                "evidence_chat_id": _safe_str(r.get("evidence_chat_id")),
                "summary_chat_id": _safe_str(r.get("summary_chat_id")),
                "alias": _safe_str(r.get("alias")),
                "activo": 1 if activo == 1 else 0,
                "updated_by": _safe_str(r.get("updated_by")),
                "updated_at": _safe_str(r.get("updated_at")),
            }
        app.bot_data["routing_cache"] = m
        app.bot_data["routing_cache_at"] = time.time()
        log.info(f"ROUTING cache actualizado: {len(m)} rutas.")
    except Exception as e:
        log.warning(f"ROUTING cache error: {e}")


async def refresh_config_jobs(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    if not app.bot_data.get("sheets_ready"):
        return

    now_ts = time.time()
    tech_at = app.bot_data.get("tech_cache_at", 0)
    routing_at = app.bot_data.get("routing_cache_at", 0)

    if now_ts - tech_at >= TECH_CACHE_TTL_SEC:
        load_tecnicos_cache(app)
    if now_ts - routing_at >= ROUTING_CACHE_TTL_SEC:
        load_routing_cache(app)


# =========================
# Sheets pairing
# =========================
def _gen_pair_code() -> str:
    raw = uuid.uuid4().hex.upper()
    return f"PAIR-{raw[:6]}"


def pairing_create(app: Application, origin_chat_id: int, purpose: str, created_by: str) -> str:
    if not app.bot_data.get("sheets_ready"):
        raise RuntimeError("Sheets no disponible.")
    ws = app.bot_data.get("ws_pairing")
    if not ws:
        raise RuntimeError("Hoja PAIRING no disponible.")

    _ensure_headers(ws, PAIRING_COLUMNS)

    code = _gen_pair_code()
    for _ in range(3):
        ri = _find_row_index_by_column(ws, "code", code)
        if ri is None:
            break
        code = _gen_pair_code()

    expires = (datetime.now(timezone.utc) + timedelta(minutes=PAIRING_TTL_MINUTES)).isoformat()
    created_at = _utc_iso_now()

    row = {
        "code": code,
        "origin_chat_id": str(origin_chat_id),
        "purpose": purpose,
        "expires_at": expires,
        "used": "0",
        "created_by": created_by,
        "created_at": created_at,
        "used_by": "",
        "used_at": "",
    }
    ws.append_row([row.get(c, "") for c in PAIRING_COLUMNS], value_input_option="RAW")
    return code


def pairing_consume_and_upsert_routing(
    app: Application,
    code: str,
    dest_chat_id: int,
    used_by: str,
    purpose_expected: str,
    dest_kind: str,
) -> Dict[str, Any]:
    if not app.bot_data.get("sheets_ready"):
        raise RuntimeError("Sheets no disponible.")
    ws_p = app.bot_data.get("ws_pairing")
    ws_r = app.bot_data.get("ws_routing")
    if not ws_p or not ws_r:
        raise RuntimeError("Hojas de configuración no disponibles.")

    _ensure_headers(ws_p, PAIRING_COLUMNS)
    _ensure_headers(ws_r, ROUTING_COLUMNS)

    code = str(code).strip().upper()
    row_idx = _find_row_index_by_column(ws_p, "code", code)
    if row_idx is None:
        raise RuntimeError("Código no encontrado.")

    values = ws_p.get_all_values()
    headers = values[0]
    row = values[row_idx - 1] if row_idx - 1 < len(values) else []
    col = {h: i for i, h in enumerate(headers)}

    def get_cell(name: str) -> str:
        i = col.get(name)
        if i is None:
            return ""
        return row[i] if i < len(row) else ""

    used = _parse_bool01(get_cell("used"))
    if used == 1:
        raise RuntimeError("Este código ya fue usado.")

    purpose = _safe_str(get_cell("purpose")).upper()
    if purpose not in ("EVIDENCE", "SUMMARY"):
        raise RuntimeError("Código inválido (purpose).")
    if purpose_expected and purpose != purpose_expected:
        raise RuntimeError(f"Este código es para {purpose}, no para {purpose_expected}.")

    expires_at = _safe_str(get_cell("expires_at"))
    dt_exp = parse_iso(expires_at)
    if not dt_exp:
        raise RuntimeError("Código inválido (expires_at).")
    if datetime.now(timezone.utc) > dt_exp:
        raise RuntimeError("Este código está vencido. Genera uno nuevo en el grupo ORIGEN.")

    origin_chat_id = _safe_int(get_cell("origin_chat_id"))
    if origin_chat_id is None:
        raise RuntimeError("Código inválido (origin_chat_id).")

    used_at = _utc_iso_now()
    _update_cells_by_headers(ws_p, row_idx, {"used": "1", "used_by": used_by, "used_at": used_at})

    origin_str = str(origin_chat_id)
    r_idx = _find_row_index_by_column(ws_r, "origin_chat_id", origin_str)

    alias = ""
    try:
        rc = app.bot_data.get("routing_cache") or {}
        if rc.get(int(origin_chat_id)):
            alias = _safe_str(rc[int(origin_chat_id)].get("alias"))
    except Exception:
        pass

    if not alias:
        alias = f"ORIGEN {origin_chat_id}"

    upd_by = used_by
    upd_at = _utc_iso_now()

    if r_idx is None:
        new_row = {
            "origin_chat_id": origin_str,
            "evidence_chat_id": str(dest_chat_id) if dest_kind == "EVIDENCE" else "",
            "summary_chat_id": str(dest_chat_id) if dest_kind == "SUMMARY" else "",
            "alias": alias,
            "activo": "1",
            "updated_by": upd_by,
            "updated_at": upd_at,
        }
        ws_r.append_row([new_row.get(c, "") for c in ROUTING_COLUMNS], value_input_option="RAW")
    else:
        updates = {
            "activo": "1",
            "updated_by": upd_by,
            "updated_at": upd_at,
        }
        if dest_kind == "EVIDENCE":
            updates["evidence_chat_id"] = str(dest_chat_id)
        else:
            updates["summary_chat_id"] = str(dest_chat_id)

        vals_r = ws_r.get_all_values()
        hdr_r = vals_r[0]
        try:
            ci_alias = hdr_r.index("alias")
        except ValueError:
            ci_alias = None
        current_alias = ""
        if ci_alias is not None and (r_idx - 1) < len(vals_r):
            rr = vals_r[r_idx - 1]
            current_alias = rr[ci_alias] if ci_alias < len(rr) else ""
        if not str(current_alias).strip():
            updates["alias"] = alias

        _update_cells_by_headers(ws_r, r_idx, updates)

    load_routing_cache(app)

    return {"origin_chat_id": int(origin_chat_id), "purpose": purpose, "alias": alias}


# =========================
# Admin helper
# =========================
async def is_admin_of_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user and a.user.id == user_id for a in admins)
    except Exception:
        return False


def mention_user_html(user_id: int, label: str = "Técnico") -> str:
    return f'<a href="tg://user?id={user_id}">{label}</a>'


# =========================
# Keyboards
# =========================
def kb_technicians_dynamic(app: Application) -> InlineKeyboardMarkup:
    techs = app.bot_data.get("tech_cache") or []
    rows: List[List[InlineKeyboardButton]] = []

    if not techs:
        for name in TECHNICIANS_FALLBACK:
            rows.append([InlineKeyboardButton(name, callback_data=f"TECH|{name}")])
        return InlineKeyboardMarkup(rows)

    for t in techs:
        nombre = _safe_str(t.get("nombre"))
        alias = _safe_str(t.get("alias"))
        label = alias if alias else nombre
        rows.append([InlineKeyboardButton(label, callback_data=f"TECH|{nombre}")])

    return InlineKeyboardMarkup(rows)


def kb_services() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(s, callback_data=f"SERV|{s}")] for s in SERVICE_TYPES]
    return InlineKeyboardMarkup(rows)


def kb_install_mode() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("INST EXTERNA", callback_data="MODE|EXTERNA"),
            InlineKeyboardButton("INST INTERNA", callback_data="MODE|INTERNA"),
        ]]
    )


def kb_evidence_menu(case_id: int, mode: str) -> InlineKeyboardMarkup:
    items = get_mode_items(mode)
    req_num, req_label, req_step_no, _req_status = compute_next_required_step(case_id, mode)

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|MODE")])

    for num, label, step_no in items:
        st = get_effective_step_status(case_id, step_no)

        if st == STEP_STATE_APROBADO:
            prefix = "🟢"
        elif st == STEP_STATE_EN_REVISION:
            prefix = "🟡"
        elif st == STEP_STATE_RECHAZADO:
            prefix = "🔴"
        elif st == STEP_STATE_BLOQUEADO:
            prefix = "⛔"
        elif st == STEP_STATE_REABIERTO:
            prefix = "🟠"
        elif step_no == req_step_no:
            prefix = "➡️"
        else:
            prefix = "🔒"

        rows.append([InlineKeyboardButton(f"{prefix} {num}. {label}", callback_data=f"EVID|{mode}|{num}|{step_no}")])

    return InlineKeyboardMarkup(rows)


def kb_action_menu(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("SOLICITUD DE PERMISO", callback_data=f"ACT|{case_id}|{step_no}|PERMISO"),
            InlineKeyboardButton("CARGAR FOTO", callback_data=f"ACT|{case_id}|{step_no}|FOTO"),
        ]]
    )


def kb_auth_mode(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Solo texto", callback_data=f"AUTH_MODE|{case_id}|{step_no}|TEXT"),
            InlineKeyboardButton("Multimedia", callback_data=f"AUTH_MODE|{case_id}|{step_no}|MEDIA"),
        ]]
    )


def kb_auth_media_controls(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("➕ CARGAR MAS", callback_data=f"AUTH_MORE|{case_id}|{step_no}"),
            InlineKeyboardButton("✅ EVIDENCIAS COMPLETAS", callback_data=f"AUTH_DONE|{case_id}|{step_no}"),
        ]]
    )


def kb_auth_review(case_id: int, step_no: int, attempt: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ AUTORIZADO", callback_data=f"AUT_OK|{case_id}|{step_no}|{attempt}"),
            InlineKeyboardButton("❌ RECHAZO", callback_data=f"AUT_BAD|{case_id}|{step_no}|{attempt}"),
        ]]
    )


def kb_media_controls(case_id: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("➕ CARGAR MAS", callback_data=f"MEDIA_MORE|{case_id}|{step_no}"),
            InlineKeyboardButton("✅ EVIDENCIAS COMPLETAS", callback_data=f"MEDIA_DONE|{case_id}|{step_no}"),
        ]]
    )


def kb_review_step(case_id: int, step_no: int, attempt: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ CONFORME", callback_data=f"REV_OK|{case_id}|{step_no}|{attempt}"),
            InlineKeyboardButton("❌ RECHAZO", callback_data=f"REV_BAD|{case_id}|{step_no}|{attempt}"),
        ]]
    )


def kb_reopen_menu(case_id: int, mode: str) -> InlineKeyboardMarkup:
    items = get_mode_items(mode)
    rows: List[List[InlineKeyboardButton]] = []
    for num, label, step_no in items:
        st = get_effective_step_status(case_id, step_no)
        if st == STEP_STATE_APROBADO:
            rows.append([InlineKeyboardButton(f"🔄 {num}. {label}", callback_data=f"REOPEN|{case_id}|{step_no}")])
    rows.append([InlineKeyboardButton("❌ Cerrar", callback_data="REOPEN|CLOSE")])
    return InlineKeyboardMarkup(rows)


# =========================
# /config menu
# =========================
def kb_config_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🔗 Vincular Evidencias", callback_data="CFG|PAIR|EVIDENCE")],
            [InlineKeyboardButton("🧾 Vincular Resumen", callback_data="CFG|PAIR|SUMMARY")],
            [InlineKeyboardButton("📌 Ver rutas de este grupo", callback_data="CFG|ROUTE|STATUS")],
            [InlineKeyboardButton("❌ Cerrar", callback_data="CFG|CLOSE")],
        ]
    )


def kb_back_to_config() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("↩️ Volver a /config", callback_data="CFG|HOME")],
            [InlineKeyboardButton("❌ Cerrar", callback_data="CFG|CLOSE")],
        ]
    )


# =========================
# Prompts
# =========================
def prompt_step3() -> str:
    return (
        "PASO 3 - INGRESA CÓDIGO DE ABONADO\n"
        "✅ Envía el código como texto (puede incluir letras, números o caracteres)."
    )


def prompt_step4() -> str:
    return (
        "PASO 4 - REPORTA TU UBICACIÓN\n"
        "📌 En grupos, Telegram no permite solicitar ubicación con botón.\n"
        "✅ Envía tu ubicación así:\n"
        "1) Pulsa el clip 📎\n"
        "2) Ubicación\n"
        "3) Enviar ubicación actual"
    )


def prompt_media_step(step_no: int) -> str:
    title, desc = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}", "Envía evidencias"))
    return (
        f"{title}\n"
        f"{desc}\n"
        f"📸 Carga entre 1 a {MAX_MEDIA_PER_STEP} fotos (solo se acepta fotos)."
    )


def prompt_auth_media_step(step_no: int) -> str:
    title = STEP_MEDIA_DEFS.get(step_no, (f"PASO {step_no}",))[0]
    return (
        f"Autorización multimedia para {title}\n"
        f"📎 Carga entre 1 a {MAX_MEDIA_PER_STEP} archivos.\n"
        f"✅ En este paso (PERMISO) se acepta FOTO o VIDEO."
    )


async def send_case_status_summary(chat_id: int, context: ContextTypes.DEFAULT_TYPE, case_row: sqlite3.Row):
    case_row = maybe_release_expired_case_lock(case_row)
    approval_required = get_approval_required(chat_id)
    mode = (case_row["install_mode"] or "").strip()
    step_actual_txt = "-"
    if mode in ("EXTERNA", "INTERNA"):
        _, label, step_no, state = compute_next_required_step(int(case_row["case_id"]), mode)
        step_actual_txt = f"{label} ({step_no}) - {state}"

    locked_by = case_row["locked_by_name"] or "-"
    admin_pending = "SI" if int(case_row["admin_pending"] or 0) == 1 else "NO"
    approval_txt = "ON ✅" if approval_required else "OFF ⚠️ (auto)"

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "📌 ESTADO DEL CASO\n"
            f"• Aprobación: {approval_txt}\n"
            f"• Phase: {case_row['phase']}\n"
            f"• Paso actual: {step_actual_txt}\n"
            f"• Técnico: {case_row['technician_name'] or '(pendiente)'}\n"
            f"• Servicio: {case_row['service_type'] or '(pendiente)'}\n"
            f"• Abonado: {case_row['abonado_code'] or '(pendiente)'}\n"
            f"• Bloqueado por: {locked_by}\n"
            f"• En revisión admin: {admin_pending}\n"
        ),
    )


async def show_evidence_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, case_row: sqlite3.Row):
    case_row = maybe_release_expired_case_lock(case_row)
    mode = (case_row["install_mode"] or "").strip()
    if mode not in ("EXTERNA", "INTERNA"):
        await context.bot.send_message(chat_id=chat_id, text="Selecciona el tipo de instalación:", reply_markup=kb_install_mode())
        return

    sync_case_progress(int(case_row["case_id"]))
    case_row = get_case(int(case_row["case_id"]))

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"📌 Selecciona la evidencia a cargar ({mode}):",
        reply_markup=kb_evidence_menu(int(case_row["case_id"]), mode),
    )


# =========================
# Commands
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await context.bot.send_message(
        chat_id=msg.chat_id,
        text=(
            "Comandos:\n"
            "• /inicio  → iniciar caso\n"
            "• /estado  → ver estado\n"
            "• /cancelar → cancelar caso\n"
            "• /id → ver chat_id del grupo\n"
            "• /aprobacion on|off → activar/desactivar validaciones (solo admins)\n"
            "• /reabrir → menú de reapertura (solo admins)\n"
            "• /config → menú de configuración (solo admins)\n"
        ),
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    title = msg.chat.title if msg.chat else "-"
    await context.bot.send_message(chat_id=msg.chat_id, text=f"Chat ID: {msg.chat_id}\nTitle: {title}")


async def config_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return
    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /config.")
        return

    app = context.application
    if app.bot_data.get("sheets_ready"):
        if not app.bot_data.get("routing_cache"):
            load_routing_cache(app)

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text="⚙️ CONFIGURACIÓN (Admins)\nSelecciona una opción:",
        reply_markup=kb_config_menu(),
    )


async def inicio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    chat_id = msg.chat_id
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.full_name

    existing = maybe_release_expired_case_lock(get_open_case(chat_id))
    if existing and existing["status"] == CASE_STATUS_OPEN and int(existing["step_index"] or 0) > 0:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Ya existe un caso activo en este grupo. Termínalo o cancélalo antes de iniciar otro.",
        )
        return

    create_or_reset_case(chat_id, user_id, username)

    approval_required = get_approval_required(chat_id)
    extra = "✅ Aprobación: ON (requiere admin)" if approval_required else "⚠️ Aprobación: OFF (auto-aprobación)"

    app = context.application
    if app.bot_data.get("sheets_ready") and not app.bot_data.get("tech_cache"):
        load_tecnicos_cache(app)

    tech_cache = app.bot_data.get("tech_cache") or []
    if not tech_cache and not TECHNICIANS_FALLBACK:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ No hay técnicos activos configurados en la hoja TECNICOS.\n"
                "Admin: agrega técnicos en Google Sheets (TECNICOS) y vuelve a intentar."
            ),
        )
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=f"✅ Caso iniciado.\n{extra}\n\nPASO 1 - NOMBRE DEL TECNICO",
        reply_markup=kb_technicians_dynamic(app),
    )


async def cancelar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = maybe_release_expired_case_lock(get_open_case(msg.chat_id))
    if not case_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No hay un caso abierto en este grupo.")
        return

    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        ok, why = can_user_operate_current_step(case_row, msg.from_user.id)
        if not ok and "otro técnico" not in why.lower() and "trabajado por" not in why.lower():
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo el técnico activo o un admin puede cancelar el caso.")
            return

    update_case(
        int(case_row["case_id"]),
        status=CASE_STATUS_CANCELLED,
        phase=PHASE_CANCELLED,
        finished_at=now_utc(),
        current_step_no=None,
        pending_step_no=None,
        admin_pending=0,
    )
    clear_case_lock(int(case_row["case_id"]))
    await context.bot.send_message(chat_id=msg.chat_id, text="🧾 Caso cancelado. Puedes iniciar otro con /inicio.")


async def estado_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None:
        return

    case_row = maybe_release_expired_case_lock(get_open_case(msg.chat_id))
    if not case_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No hay un caso abierto. Usa /inicio.")
        return

    await send_case_status_summary(msg.chat_id, context, case_row)


async def aprobacion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /aprobacion on|off.")
        return

    args = context.args or []
    if not args:
        state = "ON ✅" if get_approval_required(msg.chat_id) else "OFF ⚠️ (auto)"
        await context.bot.send_message(chat_id=msg.chat_id, text=f"Estado de aprobación: {state}")
        return

    val = args[0].strip().lower()
    if val in ("on", "1", "true", "si", "sí", "activar"):
        set_approval_required(msg.chat_id, True)
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Aprobación ENCENDIDA. Se requiere validación de admins.")
    elif val in ("off", "0", "false", "no", "desactivar"):
        set_approval_required(msg.chat_id, False)
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Aprobación APAGADA. Los pasos se auto-aprobarán (APROBACION OFF).")
    else:
        await context.bot.send_message(chat_id=msg.chat_id, text="Uso: /aprobacion on  o  /aprobacion off")


async def reabrir_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /reabrir.")
        return

    case_row = maybe_release_expired_case_lock(get_open_case(msg.chat_id))
    if not case_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No hay un caso abierto en este grupo.")
        return

    mode = (case_row["install_mode"] or "").strip()
    if mode not in ("EXTERNA", "INTERNA"):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ El caso aún no tiene modo de instalación definido.")
        return

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text="🔄 Selecciona el paso aprobado que deseas reabrir:",
        reply_markup=kb_reopen_menu(int(case_row["case_id"]), mode),
    )


# =========================
# Sheets writers (enqueue)
# =========================
def enqueue_evidencia_row(case_row: sqlite3.Row, step_no: int, attempt: int, file_id: str, file_unique_id: str, tg_message_id: int, grupo_evidencias: Optional[int]):
    created_at = now_utc()
    dt = parse_iso(created_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""
    row = {
        "case_id": str(case_row["case_id"]),
        "paso_numero": str(step_no),
        "attempt": str(attempt),
        "file_id": file_id,
        "file_unique_id": file_unique_id or "",
        "mensaje_telegram_id": str(tg_message_id),
        "fecha_carga": fecha,
        "hora_carga": hora,
        "grupo_evidencias": str(grupo_evidencias or ""),
    }
    dedupe_key = f"{case_row['case_id']}|{step_no}|{attempt}|{tg_message_id}"
    outbox_enqueue("EVIDENCIAS", "UPSERT", dedupe_key, row)


def enqueue_detalle_paso_row(
    case_id: int,
    sheet_step_no: int,
    attempt: int,
    estado_paso: str,
    reviewer_name: str,
    motivo: str,
    kind: str = "EVID",
    tomado_por_user_id: Optional[int] = None,
    tomado_por_nombre: str = "",
    tomado_desde: str = "",
    reabierto_por: str = "",
    fecha_reapertura: str = "",
    hora_reapertura: str = "",
    motivo_reapertura: str = "",
    bloqueado: int = 0,
):
    case_row = get_case(case_id)
    if not case_row:
        return

    reviewed_at = now_utc()
    dt = parse_iso(reviewed_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""

    base_name = STEP_MEDIA_DEFS.get(sheet_step_no, (f"PASO {sheet_step_no}",))[0]
    if kind == "PERM":
        paso_nombre = f"PERMISO - {base_name}"
    else:
        paso_nombre = base_name

    db_step_no = -sheet_step_no if kind == "PERM" else sheet_step_no
    fotos = media_count(case_id, db_step_no, attempt)
    ids = ",".join([str(x) for x in media_message_ids(case_id, db_step_no, attempt)])

    row = {
        "case_id": str(case_id),
        "paso_numero": str(sheet_step_no),
        "paso_nombre": paso_nombre,
        "attempt": str(attempt),
        "estado_paso": estado_paso,
        "revisado_por": reviewer_name,
        "fecha_revision": fecha,
        "hora_revision": hora,
        "motivo_rechazo": motivo or "",
        "cantidad_fotos": str(fotos),
        "ids_mensajes": ids,
        "tomado_por_user_id": str(tomado_por_user_id or ""),
        "tomado_por_nombre": tomado_por_nombre or "",
        "tomado_desde": tomado_desde or "",
        "reabierto_por": reabierto_por or "",
        "fecha_reapertura": fecha_reapertura or "",
        "hora_reapertura": hora_reapertura or "",
        "motivo_reapertura": motivo_reapertura or "",
        "bloqueado": "1" if int(bloqueado or 0) == 1 else "0",
    }
    dedupe_key = f"{case_id}|{sheet_step_no}|{attempt}|{kind}"
    outbox_enqueue("DETALLE_PASOS", "UPSERT", dedupe_key, row)


def enqueue_caso_row(case_id: int):
    case_row = get_case(case_id)
    if not case_row:
        return

    created_at = case_row["created_at"] or ""
    finished_at = case_row["finished_at"] or ""
    dur = duration_minutes(created_at, finished_at) if finished_at else None
    dur_txt = str(dur) if dur is not None else ""

    lat = case_row["location_lat"]
    lon = case_row["location_lon"]
    link_maps = ""
    if lat is not None and lon is not None:
        link_maps = f"https://maps.google.com/?q={lat},{lon}"

    mode = (case_row["install_mode"] or "").strip()
    total_pasos = len(get_mode_items(mode)) if mode in ("EXTERNA", "INTERNA") else ""

    aprob = total_approved_steps_for_case(case_id)
    rech = total_rejects_for_case(case_id)
    total_evid = total_media_for_case(case_id)
    approval_required = get_approval_required(int(case_row["chat_id"]))

    row = {
        "case_id": str(case_id),
        "estado": case_row["status"],
        "chat_id_origen": str(case_row["chat_id"]),
        "fecha_inicio": fmt_date_pe(created_at) if created_at else "",
        "hora_inicio": fmt_time_pe(created_at) if created_at else "",
        "fecha_cierre": fmt_date_pe(finished_at) if finished_at else "",
        "hora_cierre": fmt_time_pe(finished_at) if finished_at else "",
        "duracion_min": dur_txt,
        "tecnico_nombre": case_row["technician_name"] or "",
        "tecnico_user_id": str(case_row["technician_user_id"] or ""),
        "tipo_servicio": case_row["service_type"] or "",
        "codigo_abonado": case_row["abonado_code"] or "",
        "modo_instalacion": mode or "",
        "latitud": str(lat) if lat is not None else "",
        "longitud": str(lon) if lon is not None else "",
        "link_maps": link_maps,
        "total_pasos": str(total_pasos) if total_pasos != "" else "",
        "pasos_aprobados": str(aprob),
        "pasos_rechazados": str(rech),
        "total_evidencias": str(total_evid),
        "requiere_aprobacion": "1" if approval_required else "0",
        "registrado_en": now_utc(),
        "version_bot": BOT_VERSION,
        "paso_actual": str(case_row["current_step_no"] or ""),
        "bloqueado_por_user_id": str(case_row["locked_by_user_id"] or ""),
        "bloqueado_por_nombre": case_row["locked_by_name"] or "",
        "bloqueado_desde": case_row["locked_at"] or "",
        "bloqueo_expira": case_row["lock_expires_at"] or "",
        "admin_pendiente": "1" if int(case_row["admin_pending"] or 0) == 1 else "0",
    }
    dedupe_key = str(case_id)
    outbox_enqueue("CASOS", "UPSERT", dedupe_key, row)


def auto_approve_db_step(case_id: int, db_step_no: int, attempt: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET submitted=1, approved=1, reviewed_by=?, reviewed_at=?, state_name=?
            WHERE case_id=? AND step_no=? AND attempt=?
            """,
            (0, now_utc(), STEP_STATE_APROBADO, case_id, db_step_no, attempt),
        )
        conn.commit()


# =========================
# Workers
# =========================
async def sheets_worker(context: ContextTypes.DEFAULT_TYPE):
    if "sheets_ready" not in context.application.bot_data:
        return
    if not context.application.bot_data.get("sheets_ready"):
        return

    ws_casos = context.application.bot_data["ws_casos"]
    ws_det = context.application.bot_data["ws_det"]
    ws_evid = context.application.bot_data["ws_evid"]
    idx_casos = context.application.bot_data["idx_casos"]
    idx_det = context.application.bot_data["idx_det"]
    idx_evid = context.application.bot_data["idx_evid"]

    batch = outbox_fetch_batch(limit=20)
    if not batch:
        return

    for item in batch:
        outbox_id = int(item["outbox_id"])
        sheet_name = item["sheet_name"]
        dedupe_key = item["dedupe_key"]
        attempts = int(item["attempts"]) + 1
        row_json = item["row_json"]

        try:
            row = json.loads(row_json)
            if sheet_name == "CASOS":
                sheet_upsert(ws_casos, idx_casos, dedupe_key, row, CASOS_COLUMNS, ["case_id"])
            elif sheet_name == "DETALLE_PASOS":
                sheet_upsert(ws_det, idx_det, dedupe_key, row, DETALLE_PASOS_COLUMNS, ["case_id", "paso_numero", "attempt"])
            elif sheet_name == "EVIDENCIAS":
                sheet_upsert(ws_evid, idx_evid, dedupe_key, row, EVIDENCIAS_COLUMNS, ["case_id", "paso_numero", "attempt", "mensaje_telegram_id"])
            else:
                raise RuntimeError(f"Hoja desconocida: {sheet_name}")

            outbox_mark_sent(outbox_id)

        except Exception as e:
            err = str(e)
            dead = _is_permanent_sheet_error(err) or attempts >= 8
            outbox_mark_failed(outbox_id, attempts, err, dead=dead)
            log.warning(f"Sheets worker error outbox_id={outbox_id} sheet={sheet_name} attempts={attempts}: {err}")
            await context.application.bot.loop.run_in_executor(None, time.sleep, 0.2)


async def media_ack_worker(context: ContextTypes.DEFAULT_TYPE):
    rows = get_pending_media_ack_buffers()
    for row in rows:
        try:
            case_row = get_case(int(row["case_id"]))
            if not case_row or case_row["status"] != CASE_STATUS_OPEN:
                mark_media_ack_sent(int(row["ack_id"]))
                continue

            count_media = int(row["count_media"] or 0)
            remaining = max(0, MAX_MEDIA_PER_STEP - media_count(int(row["case_id"]), int(row["step_no"]), int(row["attempt"])))
            controls_kb = kb_auth_media_controls(int(row["case_id"]), abs(int(row["step_no"]))) if row["phase"] == PHASE_AUTH_MEDIA else kb_media_controls(int(row["case_id"]), int(row["step_no"]))

            if remaining <= 0:
                text = f"✅ Guardado ({media_count(int(row['case_id']), int(row['step_no']), int(row['attempt']))}/{MAX_MEDIA_PER_STEP}). Ya alcanzaste el máximo. Presiona ✅ EVIDENCIAS COMPLETAS."
            else:
                total = media_count(int(row["case_id"]), int(row["step_no"]), int(row["attempt"]))
                text = f"✅ Guardado ({total}/{MAX_MEDIA_PER_STEP}). Te quedan {remaining}."

            await context.bot.send_message(
                chat_id=int(row["chat_id"]),
                text=text,
                reply_markup=controls_kb,
            )
            mark_media_ack_sent(int(row["ack_id"]))
        except Exception as e:
            log.warning(f"media_ack_worker error ack_id={row['ack_id']}: {e}")

# =========================
# Callbacks
# =========================
async def on_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q is None or q.message is None or q.from_user is None:
        return

    chat_id = q.message.chat_id
    user_id = q.from_user.id
    user_name = q.from_user.full_name
    data = (q.data or "").strip()

    log.info(f"CALLBACK data={data} chat_id={chat_id} user_id={user_id}")

    # -------------------------
    # CONFIG MENU (Admins)
    # -------------------------
    if data.startswith("CFG|"):
        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "⚠️ Solo administradores.", show_alert=True)
            return

        parts = data.split("|")

        if data == "CFG|HOME":
            await safe_q_answer(q, "Config", show_alert=False)
            await safe_edit_message_text(q, "⚙️ CONFIGURACIÓN (Admins)\nSelecciona una opción:", reply_markup=kb_config_menu())
            return

        if data == "CFG|CLOSE":
            await safe_q_answer(q, "Cerrado", show_alert=False)
            await safe_edit_message_text(q, "✅ Configuración cerrada.")
            return

        if len(parts) >= 3 and parts[1] == "ROUTE" and parts[2] == "STATUS":
            app = context.application
            if app.bot_data.get("sheets_ready") and not app.bot_data.get("routing_cache"):
                load_routing_cache(app)

            rc = app.bot_data.get("routing_cache") or {}
            row = rc.get(int(chat_id))
            if row:
                alias = row.get("alias") or f"ORIGEN {chat_id}"
                ev = row.get("evidence_chat_id") or ""
                sm = row.get("summary_chat_id") or ""
                activo = "✅ Activo" if int(row.get("activo", 1)) == 1 else "⛔ Inactivo"
                txt = (
                    f"📌 RUTAS (ORIGEN)\n"
                    f"Alias: {alias}\n"
                    f"Origin chat_id: {chat_id}\n"
                    f"Evidencias chat_id: {ev or '(no vinculado)'}\n"
                    f"Resumen chat_id: {sm or '(no vinculado)'}\n"
                    f"Estado: {activo}\n"
                )
            else:
                found_as = ""
                try:
                    for origin_id, r in rc.items():
                        if str(r.get("evidence_chat_id", "")).strip() == str(chat_id):
                            found_as = f"EVIDENCIAS de ORIGEN {origin_id} ({r.get('alias') or '-'})"
                            break
                        if str(r.get("summary_chat_id", "")).strip() == str(chat_id):
                            found_as = f"RESUMEN de ORIGEN {origin_id} ({r.get('alias') or '-'})"
                            break
                except Exception:
                    found_as = ""
                if found_as:
                    txt = f"ℹ️ Este grupo no es ORIGEN.\nEstá vinculado como: {found_as}"
                else:
                    txt = "ℹ️ Este grupo no es ORIGEN y no aparece como destino en ROUTING."
            await safe_q_answer(q, "Rutas", show_alert=False)
            await safe_edit_message_text(q, txt, reply_markup=kb_back_to_config())
            return

        if len(parts) >= 3 and parts[1] == "PAIR":
            purpose = parts[2].strip().upper()
            if purpose not in ("EVIDENCE", "SUMMARY"):
                await safe_q_answer(q, "Opción inválida.", show_alert=True)
                return

            app = context.application
            if not app.bot_data.get("sheets_ready"):
                await safe_q_answer(q, "Sheets no disponible.", show_alert=True)
                await safe_edit_message_text(q, "⚠️ Sheets no está disponible. Revisa credenciales / conexión.", reply_markup=kb_back_to_config())
                return

            if not app.bot_data.get("routing_cache"):
                load_routing_cache(app)

            rc = app.bot_data.get("routing_cache") or {}
            is_origin = int(chat_id) in rc
            if is_origin:
                try:
                    code = pairing_create(app, origin_chat_id=int(chat_id), purpose=purpose, created_by=q.from_user.full_name)
                    expires_dt = datetime.now(PERU_TZ) + timedelta(minutes=PAIRING_TTL_MINUTES)
                    expires_txt = expires_dt.strftime("%H:%M")
                    label = "EVIDENCIAS" if purpose == "EVIDENCE" else "RESUMEN"
                    txt = (
                        f"🔐 Código de vinculación ({label})\n\n"
                        f"Código: {code}\n"
                        f"Vence aprox.: {expires_txt} (Perú)\n\n"
                        f"👉 Ve al grupo DESTINO ({label})\n"
                        f"y usa /config → {'🔗 Vincular Evidencias' if purpose=='EVIDENCE' else '🧾 Vincular Resumen'}\n"
                        f"para pegar el código."
                    )
                    await safe_q_answer(q, "Código generado", show_alert=False)
                    await safe_edit_message_text(q, txt, reply_markup=kb_back_to_config())
                except Exception as e:
                    await safe_q_answer(q, "Error", show_alert=True)
                    await safe_edit_message_text(q, f"⚠️ No pude generar el código: {e}", reply_markup=kb_back_to_config())
                return
            else:
                kind = "PAIR_CODE_EVID" if purpose == "EVIDENCE" else "PAIR_CODE_SUM"
                set_pending_input(chat_id=chat_id, user_id=user_id, kind=kind, case_id=0, step_no=0, attempt=0, reply_to_message_id=q.message.message_id)
                label = "EVIDENCIAS" if purpose == "EVIDENCE" else "RESUMEN"
                txt = (
                    f"🔗 Vincular {label}\n"
                    f"✅ Pega aquí el código (ej: PAIR-ABC123)\n\n"
                    f"Este grupo será el DESTINO de {label}."
                )
                await safe_q_answer(q, "Pega el código", show_alert=False)
                await safe_edit_message_text(q, txt, reply_markup=kb_back_to_config())
                return

        await safe_q_answer(q, "Opción no válida.", show_alert=True)
        return

    # -------------------------
    # Reapertura admin
    # -------------------------
    if data == "REOPEN|CLOSE":
        await safe_q_answer(q, "Cerrado", show_alert=False)
        await safe_edit_message_text(q, "✅ Menú de reapertura cerrado.")
        return

    if data.startswith("REOPEN|"):
        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "⚠️ Solo administradores.", show_alert=True)
            return
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido.", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="REOPEN_REASON",
            case_id=case_id,
            step_no=step_no,
            attempt=0,
            reply_to_message_id=q.message.message_id,
            tech_user_id=None,
        )
        await safe_q_answer(q, "Escribe el motivo", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔄 Reapertura de paso - {step_name(step_no)}\n"
                "✍️ Admin: escribe el motivo de reapertura (un solo mensaje)."
            ),
        )
        return

    # -------------------------
    # FLUJO CASOS / EVIDENCIAS
    # -------------------------
    if data == "BACK|MODE":
        case_row = maybe_release_expired_case_lock(get_open_case(chat_id))
        if not case_row:
            await safe_q_answer(q, "No hay un caso abierto.", show_alert=True)
            return
        update_case(int(case_row["case_id"]), phase=PHASE_MENU_INST, pending_step_no=None, current_step_no=None)
        clear_case_lock(int(case_row["case_id"]))
        await safe_q_answer(q, "Volviendo…", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text="PASO 5 - TIPO DE INSTALACIÓN\nSelecciona una opción:",
            reply_markup=kb_install_mode(),
        )
        return

    if data.startswith("TECH|"):
        case_row = maybe_release_expired_case_lock(get_open_case(chat_id))
        if not case_row:
            await safe_q_answer(q, "No hay un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 0:
            await safe_q_answer(q, "Este paso ya fue atendido.", show_alert=False)
            return

        name = data.split("|", 1)[1]
        update_case(
            int(case_row["case_id"]),
            technician_name=name,
            technician_user_id=user_id,
            user_id=user_id,
            username=user_name,
            step_index=1,
            phase=PHASE_WAIT_SERVICE,
        )
        await safe_q_answer(q, "✅ Técnico registrado", show_alert=False)
        await context.bot.send_message(chat_id=chat_id, text="PASO 2 - TIPO DE SERVICIO", reply_markup=kb_services())
        return

    if data.startswith("SERV|"):
        case_row = maybe_release_expired_case_lock(get_open_case(chat_id))
        if not case_row:
            await safe_q_answer(q, "No hay un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 1:
            await safe_q_answer(q, "Este paso ya fue atendido.", show_alert=False)
            return

        service = data.split("|", 1)[1]
        if service != "ALTA NUEVA":
            await safe_q_answer(q, "PROCESO AUN NO GENERADO", show_alert=True)
            return

        update_case(int(case_row["case_id"]), service_type=service, step_index=2, phase=PHASE_WAIT_ABONADO)
        await safe_q_answer(q, "✅ Servicio registrado", show_alert=False)
        await context.bot.send_message(chat_id=chat_id, text=prompt_step3())
        return

    if data.startswith("MODE|"):
        case_row = maybe_release_expired_case_lock(get_open_case(chat_id))
        if not case_row:
            await safe_q_answer(q, "No hay un caso abierto. Usa /inicio.", show_alert=True)
            return
        if int(case_row["step_index"]) != 4:
            await safe_q_answer(q, "Aún no llegas a este paso. Completa pasos previos.", show_alert=True)
            return

        mode = data.split("|", 1)[1]
        if mode not in ("EXTERNA", "INTERNA"):
            await safe_q_answer(q, "Modo inválido.", show_alert=True)
            return

        req_num, req_label, req_step_no = 1, get_mode_items(mode)[0][1], get_mode_items(mode)[0][2]
        update_case(
            int(case_row["case_id"]),
            install_mode=mode,
            phase=PHASE_MENU_EVID,
            pending_step_no=None,
            current_step_no=req_step_no,
        )
        await safe_q_answer(q, f"✅ {mode}", show_alert=False)
        case_row2 = get_case(int(case_row["case_id"]))
        await show_evidence_menu(chat_id, context, case_row2)
        return

    if data.startswith("EVID|"):
        try:
            _, mode, num_s, step_no_s = data.split("|", 3)
            num = int(num_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = maybe_release_expired_case_lock(get_open_case(chat_id))
        if not case_row:
            await safe_q_answer(q, "No hay un caso abierto. Usa /inicio.", show_alert=True)
            return
        if (case_row["install_mode"] or "") != mode:
            await safe_q_answer(q, "Modo no coincide con el caso.", show_alert=True)
            return

        ok, why = can_user_operate_current_step(case_row, user_id)
        if not ok:
            await safe_q_answer(q, why, show_alert=True)
            return

        case_id = int(case_row["case_id"])
        req_num, req_label, req_step_no, req_status = compute_next_required_step(case_id, mode)

        if req_status == STEP_STATE_APROBADO:
            await safe_q_answer(q, "✅ Caso ya completado.", show_alert=True)
            return

        if step_no != req_step_no:
            st = get_effective_step_status(case_id, step_no)
            if st == STEP_STATE_APROBADO:
                await safe_q_answer(q, "✅ Este paso ya está conforme.", show_alert=True)
                return
            if st == STEP_STATE_EN_REVISION:
                await safe_q_answer(q, "⏳ Este paso está en revisión de admin.", show_alert=True)
                return
            if st == STEP_STATE_BLOQUEADO:
                await safe_q_answer(q, "⛔ Este paso está bloqueado por corrección de un paso anterior.", show_alert=True)
                return
            await safe_q_answer(q, f"⚠️ Debes completar primero: {req_num}. {req_label}", show_alert=True)
            return

        if req_status == STEP_STATE_EN_REVISION:
            await safe_q_answer(q, "⏳ Este paso está en revisión de admin. Espera validación.", show_alert=True)
            return

        update_case(case_id, phase=PHASE_EVID_ACTION, pending_step_no=step_no, current_step_no=step_no)
        clear_case_lock(case_id)
        await safe_q_answer(q, "Continuar…", show_alert=False)
        label = step_name(step_no)

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📌 {num}. {label}\nElige una opción:",
            reply_markup=kb_action_menu(case_id, step_no),
        )
        return

    if data.startswith("ACT|"):
        try:
            _, case_id_s, step_no_s, action = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = maybe_release_expired_case_lock(get_case(case_id))
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return
        if int(case_row["chat_id"]) != int(chat_id):
            await safe_q_answer(q, "Este caso no pertenece a este grupo.", show_alert=True)
            return

        mode = (case_row["install_mode"] or "").strip()
        if mode not in ("EXTERNA", "INTERNA"):
            await safe_q_answer(q, "El caso aún no tiene modo definido.", show_alert=True)
            return

        req_num, req_label, req_step_no, _ = compute_next_required_step(case_id, mode)
        if int(step_no) != int(req_step_no):
            await safe_q_answer(q, f"⚠️ Paso no vigente. Debes trabajar: {req_num}. {req_label}", show_alert=True)
            return

        ok, why = can_user_operate_current_step(case_row, user_id)
        if not ok:
            await safe_q_answer(q, why, show_alert=True)
            return

        latest = get_latest_step_state(case_id, step_no)
        if latest and (latest["state_name"] or "") == STEP_STATE_APROBADO:
            await safe_q_answer(q, "✅ Este paso ya fue aprobado y está cerrado.", show_alert=True)
            return
        if latest and int(latest["blocked"] or 0) == 1:
            await safe_q_answer(q, "⛔ Este paso está bloqueado.", show_alert=True)
            return

        lock_case_step(case_id, user_id, user_name)

        if action == "PERMISO":
            update_case(case_id, phase=PHASE_AUTH_MODE, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Permiso…", show_alert=False)
            await context.bot.send_message(
                chat_id=chat_id,
                text="Autorización: elige el tipo",
                reply_markup=kb_auth_mode(case_id, step_no),
            )
            return

        if action == "FOTO":
            st = ensure_step_state(case_id, step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(case_id, step_no, int(st["attempt"]), user_id, user_name)
            update_case(case_id, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Cargar foto…", show_alert=False)
            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await safe_q_answer(q, "Acción inválida.", show_alert=True)
        return

    if data.startswith("AUTH_MODE|"):
        try:
            _, case_id_s, step_no_s, mode = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = maybe_release_expired_case_lock(get_case(case_id))
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        ok, why = can_user_operate_current_step(case_row, user_id)
        if not ok:
            await safe_q_answer(q, why, show_alert=True)
            return

        if mode == "TEXT":
            st = ensure_step_state(case_id, -step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(case_id, -step_no, int(st["attempt"]), user_id, user_name)
            update_case(case_id, phase=PHASE_AUTH_TEXT_WAIT, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Envía el texto…", show_alert=False)
            await context.bot.send_message(chat_id=chat_id, text="Envía el texto de la autorización (en un solo mensaje).")
            return

        if mode == "MEDIA":
            st = ensure_step_state(case_id, -step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(case_id, -step_no, int(st["attempt"]), user_id, user_name)
            update_case(case_id, phase=PHASE_AUTH_MEDIA, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Carga evidencias…", show_alert=False)
            await context.bot.send_message(
                chat_id=chat_id,
                text=prompt_auth_media_step(step_no),
                reply_markup=kb_auth_media_controls(case_id, step_no),
            )
            return

        await safe_q_answer(q, "Modo inválido", show_alert=True)
        return

    if data.startswith("AUTH_MORE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return
        case_row = maybe_release_expired_case_lock(get_case(case_id))
        if not case_row:
            await safe_q_answer(q, "Caso no válido.", show_alert=True)
            return
        if int(case_row["admin_pending"] or 0) == 1:
            await safe_q_answer(q, "⏳ Ya fue enviado a revisión. No puedes cargar más.", show_alert=True)
            return
        await safe_q_answer(q, "Puedes seguir cargando.", show_alert=False)
        return

    if data.startswith("AUTH_DONE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = maybe_release_expired_case_lock(get_case(case_id))
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        auth_step_no = -step_no
        st = ensure_step_state(case_id, auth_step_no, owner_user_id=user_id, owner_name=user_name)
        attempt = int(st["attempt"])

        if int(st["submitted"]) == 1 and st["approved"] is None:
            await safe_q_answer(q, "Esta autorización ya fue enviada a revisión.", show_alert=True)
            return
        if st["approved"] is not None and int(st["approved"]) == 1:
            await safe_q_answer(q, "✅ Esta autorización ya está aprobada.", show_alert=True)
            return

        count = media_count(case_id, auth_step_no, attempt)
        if count <= 0:
            await safe_q_answer(q, "⚠️ Debes cargar al menos 1 archivo.", show_alert=True)
            return

        approval_required = get_approval_required(int(case_row["chat_id"]))

        if not approval_required:
            auto_approve_db_step(case_id, auth_step_no, attempt)
            enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_APROBADO, "APROBACION OFF", "", kind="PERM")

            update_case(case_id, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
            clear_case_lock(case_id)

            await safe_q_answer(q, "✅ Autorización aprobada (OFF)", show_alert=False)
            await safe_edit_message_text(q, "✅ Autorización aprobada automáticamente (APROBACION OFF). Continuando a CARGAR FOTO…")

            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        mark_submitted(case_id, auth_step_no, attempt)
        update_case(case_id, phase=PHASE_AUTH_REVIEW, pending_step_no=step_no, current_step_no=step_no, admin_pending=1)
        clear_case_lock(case_id)
        await safe_q_answer(q, "📨 Enviado a revisión", show_alert=False)

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔐 **Revisión de AUTORIZACIÓN (multimedia)**\n"
                f"Para: {step_name(step_no)}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}\n"
                f"Servicio: {case_row['service_type'] or '-'}\n"
                f"Abonado: {case_row['abonado_code'] or '-'}\n"
                f"Archivos: {count}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_auth_review(case_id, step_no, attempt),
        )
        return

    if data.startswith("AUT_OK|") or data.startswith("AUT_BAD|"):
        try:
            action, case_id_s, step_no_s, attempt_s = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
            attempt = int(attempt_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        auth_step_no = -step_no
        with db() as conn:
            row = conn.execute(
                "SELECT approved FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
                (case_id, auth_step_no, attempt),
            ).fetchone()
        if not row:
            await safe_q_answer(q, "No encontré la autorización para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await safe_q_answer(q, "Esta autorización ya fue revisada.", show_alert=True)
            return

        tech_id = int(case_row["technician_user_id"] or 0)
        admin_name = q.from_user.full_name

        if action == "AUT_OK":
            set_review(case_id, auth_step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_APROBADO, admin_name, "", kind="PERM")

            await safe_q_answer(q, "✅ Autorizado", show_alert=False)
            await safe_edit_message_text(q, "✅ Autorizado. Continuando a CARGAR FOTO…")

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔐 {mention_user_html(tech_id)}: ✅ Autorización aprobada para <b>{step_name(step_no)}</b> (Intento {attempt}) por <b>{admin_name}</b>.",
                parse_mode="HTML",
            )

            update_case(case_id, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await safe_q_answer(q, "Escribe el motivo del rechazo.", show_alert=False)

        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="AUTH_REJECT_REASON",
            case_id=case_id,
            step_no=step_no,
            attempt=attempt,
            reply_to_message_id=q.message.message_id,
            tech_user_id=tech_id,
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ Rechazo de autorización.\n"
                "✍️ Admin: escribe el *motivo del rechazo* (un solo mensaje).\n\n"
                f"Paso: {step_name(step_no)}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}"
            ),
            parse_mode="Markdown",
        )
        return

    if data.startswith("MEDIA_MORE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return
        case_row = maybe_release_expired_case_lock(get_case(case_id))
        if not case_row:
            await safe_q_answer(q, "Caso no válido.", show_alert=True)
            return
        latest = get_latest_step_state(case_id, step_no)
        if latest and int(latest["submitted"] or 0) == 1 and latest["approved"] is None:
            await safe_q_answer(q, "⏳ Este paso ya fue enviado a revisión. No puedes cargar más.", show_alert=True)
            return
        await safe_q_answer(q, "Puedes seguir cargando evidencias.", show_alert=False)
        return

    if data.startswith("MEDIA_DONE|"):
        try:
            _, case_id_s, step_no_s = data.split("|", 2)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        case_row = maybe_release_expired_case_lock(get_case(case_id))
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        # =========================
        # BLINDAJE ANTI-DOBLE-CLICK
        # =========================
        latest = get_latest_step_state(case_id, step_no)
        if latest:
            latest_state = (latest["state_name"] or "").strip().upper()

            # Si ya fue enviado a revisión, no volver a procesar nada
            if latest_state == STEP_STATE_EN_REVISION or (
                int(latest["submitted"] or 0) == 1 and latest["approved"] is None
            ):
                await safe_q_answer(q, "⏳ Este paso ya fue enviado a revisión.", show_alert=True)
                return

            # Si ya está aprobado, tampoco reprocesar
            if latest_state == STEP_STATE_APROBADO or (
                latest["approved"] is not None and int(latest["approved"]) == 1
            ):
                await safe_q_answer(q, "✅ Este paso ya está aprobado.", show_alert=True)
                return

        # Solo si realmente sigue en carga, recién continuamos
        st = get_active_unsubmitted_step_state(case_id, step_no)
        if not st:
            st = ensure_step_state(case_id, step_no, owner_user_id=user_id, owner_name=user_name)

        attempt = int(st["attempt"])

        # Revalidación por seguridad
        if int(st["submitted"] or 0) == 1 and st["approved"] is None:
            await safe_q_answer(q, "⏳ Este paso ya fue enviado a revisión.", show_alert=True)
            return

        if st["approved"] is not None and int(st["approved"]) == 1:
            await safe_q_answer(q, "✅ Este paso ya está aprobado.", show_alert=True)
            return

        count = media_count(case_id, step_no, attempt)
        if count <= 0:
            await safe_q_answer(q, "⚠️ Debes cargar al menos 1 foto.", show_alert=True)
            return

        title = step_name(step_no)
        approval_required = get_approval_required(int(case_row["chat_id"]))
        mode = (case_row["install_mode"] or "EXTERNA").strip()
        tech_id = int(case_row["technician_user_id"] or 0)

        if not approval_required:
            auto_approve_db_step(case_id, step_no, attempt)
            enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_APROBADO, "APROBACION OFF", "", kind="EVID")

            await safe_q_answer(q, "✅ Aprobado (OFF)", show_alert=False)
            await safe_edit_message_text(q, "✅ Aprobado automáticamente (APROBACION OFF).")

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>PASO COMPLETADO</b>\n"
                    f"• Evidencia: <b>{title}</b>\n"
                    f"• Intento: <b>{attempt}</b>\n"
                    f"• Evidencias: <b>{count}</b>\n"
                    f"• Revisado por: <b>APROBACION OFF</b>\n"
                    f"• Técnico: {mention_user_html(tech_id)}"
                ),
                parse_mode="HTML",
            )

            clear_case_lock(case_id)
            update_case(case_id, admin_pending=0)

            if is_last_step(mode, step_no):
                finished_at = now_utc()
                update_case(case_id, status=CASE_STATUS_CLOSED, phase=PHASE_CLOSED, finished_at=finished_at, pending_step_no=None, current_step_no=None)

                enqueue_caso_row(case_id)

                route = get_route_for_chat_cached(context.application, int(case_row["chat_id"]))
                dest_summary = route.get("summary")
                if dest_summary:
                    created_at = case_row["created_at"] or "-"
                    total_evid = total_media_for_case(case_id)
                    total_rej = total_rejects_for_case(case_id)
                    dur = duration_minutes(created_at, finished_at)
                    dur_txt = f"{dur} min" if dur is not None else "-"

                    await context.bot.send_message(
                        chat_id=dest_summary,
                        text=(
                            "🧾 **RESUMEN DE CASO (CERRADO)**\n"
                            f"Fecha: {fmt_date_pe(created_at)}\n"
                            f"Hora de Inicio: {fmt_time_pe(created_at)}\n"
                            f"Hora de Final: {fmt_time_pe(finished_at)}\n"
                            f"Duración: {dur_txt}\n"
                            f"Técnico: {case_row['technician_name'] or '-'}\n"
                            f"Tipo servicio: {case_row['service_type'] or '-'}\n"
                            f"Código abonado: {case_row['abonado_code'] or '-'}\n"
                            f"Evidencias totales: {total_evid}\n"
                            f"Rechazos: {total_rej}\n"
                            f"Grupo origen: {case_row['chat_id']}\n"
                        ),
                        parse_mode="Markdown",
                    )

                await context.bot.send_message(chat_id=chat_id, text="🧾 Caso COMPLETADO y cerrado.")
                return

            sync_case_progress(case_id)
            update_case(case_id, phase=PHASE_MENU_EVID, pending_step_no=None)
            case_row2 = get_case(case_id)
            await context.bot.send_message(chat_id=chat_id, text="➡️ Continúa con el siguiente paso.")
            await show_evidence_menu(chat_id, context, case_row2)
            return

        # Desde aquí pasa a revisión UNA sola vez
        mark_submitted(case_id, step_no, attempt)
        update_case(case_id, phase=PHASE_STEP_REVIEW, pending_step_no=step_no, current_step_no=step_no, admin_pending=1)
        clear_case_lock(case_id)
        await safe_q_answer(q, "📨 Enviado a revisión", show_alert=False)

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔎 **Revisión requerida - {title}**\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}\n"
                f"Servicio: {case_row['service_type'] or '-'}\n"
                f"Abonado: {case_row['abonado_code'] or '-'}\n"
                f"Evidencias: {count}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_review_step(case_id, step_no, attempt),
        )
        return

    if data.startswith("REV_OK|") or data.startswith("REV_BAD|"):
        try:
            action, case_id_s, step_no_s, attempt_s = data.split("|", 3)
            case_id = int(case_id_s)
            step_no = int(step_no_s)
            attempt = int(attempt_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        case_row = get_case(case_id)
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await safe_q_answer(q, "Caso no válido o cerrado.", show_alert=True)
            return

        with db() as conn:
            row = conn.execute(
                "SELECT approved FROM step_state WHERE case_id=? AND step_no=? AND attempt=?",
                (case_id, step_no, attempt),
            ).fetchone()
        if not row:
            await safe_q_answer(q, "No encontré el paso para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await safe_q_answer(q, "Este paso ya fue revisado.", show_alert=True)
            return

        mode = (case_row["install_mode"] or "EXTERNA").strip()
        tech_id = int(case_row["technician_user_id"] or 0)
        admin_name = q.from_user.full_name
        title = step_name(step_no)

        if action == "REV_OK":
            set_review(case_id, step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_APROBADO, admin_name, "", kind="EVID")

            await safe_q_answer(q, "✅ Conforme", show_alert=False)
            await safe_edit_message_text(q, "✅ Conforme.")

            evids = media_count(case_id, step_no, attempt)
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>PASO COMPLETADO</b>\n"
                    f"• Evidencia: <b>{title}</b>\n"
                    f"• Intento: <b>{attempt}</b>\n"
                    f"• Evidencias: <b>{evids}</b>\n"
                    f"• Aprobado por: <b>{admin_name}</b>\n"
                    f"• Técnico: {mention_user_html(tech_id)}"
                ),
                parse_mode="HTML",
            )

            update_case(case_id, admin_pending=0)
            mark_step_blocked_from(case_id, step_no, mode, False)

            if is_last_step(mode, step_no):
                finished_at = now_utc()
                update_case(case_id, status=CASE_STATUS_CLOSED, phase=PHASE_CLOSED, finished_at=finished_at, pending_step_no=None, current_step_no=None)

                enqueue_caso_row(case_id)

                route = get_route_for_chat_cached(context.application, int(case_row["chat_id"]))
                dest_summary = route.get("summary")
                if dest_summary:
                    created_at = case_row["created_at"] or "-"
                    total_evid = total_media_for_case(case_id)
                    total_rej = total_rejects_for_case(case_id)
                    dur = duration_minutes(created_at, finished_at)
                    dur_txt = f"{dur} min" if dur is not None else "-"

                    await context.bot.send_message(
                        chat_id=dest_summary,
                        text=(
                            "🧾 **RESUMEN DE CASO (CERRADO)**\n"
                            f"Fecha: {fmt_date_pe(created_at)}\n"
                            f"Hora de Inicio: {fmt_time_pe(created_at)}\n"
                            f"Hora de Final: {fmt_time_pe(finished_at)}\n"
                            f"Duración: {dur_txt}\n"
                            f"Técnico: {case_row['technician_name'] or '-'}\n"
                            f"Tipo servicio: {case_row['service_type'] or '-'}\n"
                            f"Código abonado: {case_row['abonado_code'] or '-'}\n"
                            f"Evidencias totales: {total_evid}\n"
                            f"Rechazos: {total_rej}\n"
                            f"Grupo origen: {case_row['chat_id']}\n"
                        ),
                        parse_mode="Markdown",
                    )

                await context.bot.send_message(chat_id=chat_id, text="🧾 Caso COMPLETADO y cerrado.")
                return

            sync_case_progress(case_id)
            update_case(case_id, phase=PHASE_MENU_EVID, pending_step_no=None)
            case_row2 = get_case(case_id)
            await context.bot.send_message(chat_id=chat_id, text="➡️ Continúa con el siguiente paso.")
            await show_evidence_menu(chat_id, context, case_row2)
            return

        await safe_q_answer(q, "Escribe el motivo del rechazo.", show_alert=False)

        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="EVID_REJECT_REASON",
            case_id=case_id,
            step_no=step_no,
            attempt=attempt,
            reply_to_message_id=q.message.message_id,
            tech_user_id=tech_id,
        )

        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"❌ Rechazo de evidencia - {title}\n"
                f"Intento: {attempt}\n"
                "✍️ Admin: escribe el *motivo del rechazo* (un solo mensaje)."
            ),
            parse_mode="Markdown",
        )
        return

    await safe_q_answer(q, "Acción no válida.", show_alert=True)


# =========================
# Text handler
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    # Pairing codes
    pending_pair_e = pop_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_EVID")
    if pending_pair_e:
        if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo administradores pueden vincular.")
            return
        code = (msg.text or "").strip().upper()
        if not re.match(r"^PAIR-[A-Z0-9]{6}$", code):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Código inválido. Ejemplo válido: PAIR-ABC123")
            set_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_EVID", 0, 0, 0)
            return
        try:
            info = pairing_consume_and_upsert_routing(
                context.application,
                code=code,
                dest_chat_id=msg.chat_id,
                used_by=msg.from_user.full_name,
                purpose_expected="EVIDENCE",
                dest_kind="EVIDENCE",
            )
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    "✅ Vinculación completada (EVIDENCIAS)\n"
                    f"ORIGEN chat_id: {info.get('origin_chat_id')}\n"
                    f"Alias: {info.get('alias')}\n"
                    f"DESTINO (este grupo): {msg.chat_id}"
                ),
                reply_markup=kb_back_to_config(),
            )
        except Exception as e:
            await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No pude vincular: {e}", reply_markup=kb_back_to_config())
        return

    pending_pair_s = pop_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_SUM")
    if pending_pair_s:
        if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo administradores pueden vincular.")
            return
        code = (msg.text or "").strip().upper()
        if not re.match(r"^PAIR-[A-Z0-9]{6}$", code):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Código inválido. Ejemplo válido: PAIR-ABC123")
            set_pending_input(msg.chat_id, msg.from_user.id, "PAIR_CODE_SUM", 0, 0, 0)
            return
        try:
            info = pairing_consume_and_upsert_routing(
                context.application,
                code=code,
                dest_chat_id=msg.chat_id,
                used_by=msg.from_user.full_name,
                purpose_expected="SUMMARY",
                dest_kind="SUMMARY",
            )
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    "✅ Vinculación completada (RESUMEN)\n"
                    f"ORIGEN chat_id: {info.get('origin_chat_id')}\n"
                    f"Alias: {info.get('alias')}\n"
                    f"DESTINO (este grupo): {msg.chat_id}"
                ),
                reply_markup=kb_back_to_config(),
            )
        except Exception as e:
            await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No pude vincular: {e}", reply_markup=kb_back_to_config())
        return

    # Reapertura admin
    pending_reopen = pop_pending_input(msg.chat_id, msg.from_user.id, "REOPEN_REASON")
    if pending_reopen:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="REOPEN_REASON",
                case_id=int(pending_reopen["case_id"]),
                step_no=int(pending_reopen["step_no"]),
                attempt=0,
                reply_to_message_id=int(pending_reopen["reply_to_message_id"]) if pending_reopen["reply_to_message_id"] is not None else None,
                tech_user_id=None,
            )
            return

        case_id = int(pending_reopen["case_id"])
        step_no = int(pending_reopen["step_no"])
        case_row = get_case(case_id)
        if not case_row or case_row["status"] != CASE_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Caso no válido o ya cerrado.")
            return

        mode = (case_row["install_mode"] or "").strip()
        if mode not in ("EXTERNA", "INTERNA"):
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ El caso no tiene modo de instalación válido.")
            return

        try:
            reopened = reopen_step(case_id, step_no, msg.from_user.full_name, reason, mode)
            dt = parse_iso(reopened["reopened_at"] or "")
            fecha_re = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
            hora_re = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""
            enqueue_detalle_paso_row(
                case_id,
                step_no,
                int(reopened["attempt"]),
                STEP_STATE_REABIERTO,
                msg.from_user.full_name,
                "",
                kind="EVID",
                tomado_por_user_id=None,
                tomado_por_nombre="",
                tomado_desde="",
                reabierto_por=msg.from_user.full_name,
                fecha_reapertura=fecha_re,
                hora_reapertura=hora_re,
                motivo_reapertura=reason,
                bloqueado=0,
            )
            update_case(
                case_id,
                phase=PHASE_MENU_EVID,
                current_step_no=step_no,
                pending_step_no=None,
                admin_pending=0,
            )
            clear_case_lock(case_id)
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    f"🔄 Paso reabierto por administrador\n"
                    f"Paso: {step_name(step_no)}\n"
                    f"Motivo: {reason}\n\n"
                    "Los pasos posteriores quedan bloqueados hasta corregir este paso."
                ),
            )
            await show_evidence_menu(msg.chat_id, context, get_case(case_id))
        except Exception as e:
            await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No pude reabrir el paso: {e}")
        return

    # Rechazos autorización/evidencia
    pending_auth = pop_pending_input(msg.chat_id, msg.from_user.id, "AUTH_REJECT_REASON")
    if pending_auth:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="AUTH_REJECT_REASON",
                case_id=int(pending_auth["case_id"]),
                step_no=int(pending_auth["step_no"]),
                attempt=int(pending_auth["attempt"]),
                reply_to_message_id=int(pending_auth["reply_to_message_id"]) if pending_auth["reply_to_message_id"] is not None else None,
                tech_user_id=int(pending_auth["tech_user_id"]) if pending_auth["tech_user_id"] is not None else None,
            )
            return

        case_id = int(pending_auth["case_id"])
        step_no = int(pending_auth["step_no"])
        attempt = int(pending_auth["attempt"])
        auth_step_no = -step_no

        case_db = get_case(case_id)
        if not case_db or case_db["status"] != CASE_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Caso no válido o ya cerrado.")
            return

        set_review(case_id, auth_step_no, attempt, approved=0, reviewer_id=msg.from_user.id)
        set_reject_reason(case_id, auth_step_no, attempt, reason, msg.from_user.id)
        enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_RECHAZADO, msg.from_user.full_name, reason, kind="PERM")

        tech_id = int(pending_auth["tech_user_id"]) if pending_auth["tech_user_id"] is not None else None
        reply_to = int(pending_auth["reply_to_message_id"]) if pending_auth["reply_to_message_id"] is not None else None
        title = step_name(step_no)

        mention = mention_user_html(tech_id) if tech_id else "Técnico"

        update_case(case_id, phase=PHASE_EVID_ACTION, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
        clear_case_lock(case_id)

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"❌ Autorización rechazada ({mention}).\n"
                f"📌 Paso: <b>{title}</b> (Intento {attempt})\n"
                f"📝 Motivo: {reason}\n\n"
                "El técnico puede volver a solicitar permiso o cargar foto."
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to if reply_to else None,
        )

        await context.bot.send_message(chat_id=msg.chat_id, text="Elige una opción:", reply_markup=kb_action_menu(case_id, step_no))
        return

    pending_evid = pop_pending_input(msg.chat_id, msg.from_user.id, "EVID_REJECT_REASON")
    if pending_evid:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="EVID_REJECT_REASON",
                case_id=int(pending_evid["case_id"]),
                step_no=int(pending_evid["step_no"]),
                attempt=int(pending_evid["attempt"]),
                reply_to_message_id=int(pending_evid["reply_to_message_id"]) if pending_evid["reply_to_message_id"] is not None else None,
                tech_user_id=int(pending_evid["tech_user_id"]) if pending_evid["tech_user_id"] is not None else None,
            )
            return

        case_id = int(pending_evid["case_id"])
        step_no = int(pending_evid["step_no"])
        attempt = int(pending_evid["attempt"])

        case_db = get_case(case_id)
        if not case_db or case_db["status"] != CASE_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Caso no válido o ya cerrado.")
            return

        set_review(case_id, step_no, attempt, approved=0, reviewer_id=msg.from_user.id)
        set_reject_reason(case_id, step_no, attempt, reason, msg.from_user.id)

        tech_id = int(pending_evid["tech_user_id"]) if pending_evid["tech_user_id"] is not None else None
        reply_to = int(pending_evid["reply_to_message_id"]) if pending_evid["reply_to_message_id"] is not None else None
        title = step_name(step_no)
        mention = mention_user_html(tech_id) if tech_id else "Técnico"

        enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_RECHAZADO, msg.from_user.full_name, reason, kind="EVID")

        update_case(case_id, phase=PHASE_EVID_ACTION, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
        clear_case_lock(case_id)

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"❌ Evidencia rechazada - <b>{title}</b> ({mention}).\n"
                f"Intento: <b>{attempt}</b>\n"
                f"📝 Motivo: {reason}\n\n"
                "El técnico debe reenviar este paso."
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to if reply_to else None,
        )

        await context.bot.send_message(chat_id=msg.chat_id, text="Elige una opción:", reply_markup=kb_action_menu(case_id, step_no))
        return

    # Flujo técnico normal
    case_row = maybe_release_expired_case_lock(get_open_case(msg.chat_id))
    if not case_row:
        return

    if (case_row["phase"] or "") in (PHASE_STEP_MEDIA, PHASE_AUTH_MEDIA):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso no se acepta texto. Envía el archivo según corresponda.")
        return

    if (case_row["phase"] or "") == PHASE_AUTH_TEXT_WAIT:
        step_no = int(case_row["pending_step_no"] or 0)
        if step_no < 5 or step_no > 15:
            return

        ok, why = can_user_operate_current_step(case_row, msg.from_user.id)
        if not ok:
            await context.bot.send_message(chat_id=msg.chat_id, text=why)
            return

        text = (msg.text or "").strip()
        if not text:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía el texto de autorización.")
            return

        case_id = int(case_row["case_id"])
        auth_step_no = -step_no
        st = ensure_step_state(case_id, auth_step_no, owner_user_id=msg.from_user.id, owner_name=msg.from_user.full_name)
        attempt = int(st["attempt"])
        set_step_owner(case_id, auth_step_no, attempt, msg.from_user.id, msg.from_user.full_name)

        save_auth_text(case_id, auth_step_no, attempt, text, msg.message_id)

        approval_required = get_approval_required(int(case_row["chat_id"]))

        if not approval_required:
            auto_approve_db_step(case_id, auth_step_no, attempt)
            enqueue_detalle_paso_row(case_id, step_no, attempt, STEP_STATE_APROBADO, "APROBACION OFF", "", kind="PERM")

            update_case(case_id, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
            clear_case_lock(case_id)

            await context.bot.send_message(
                chat_id=msg.chat_id,
                text=(
                    "✅ Autorización aprobada automáticamente (APROBACION OFF).\n"
                    "➡️ Continúa con la carga de foto del paso."
                ),
            )
            await context.bot.send_message(chat_id=msg.chat_id, text=prompt_media_step(step_no))
            return

        mark_submitted(case_id, auth_step_no, attempt)
        update_case(case_id, phase=PHASE_AUTH_REVIEW, pending_step_no=step_no, current_step_no=step_no, admin_pending=1)
        clear_case_lock(case_id)

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"🔐 **Revisión de AUTORIZACIÓN (solo texto)**\n"
                f"Para: {step_name(step_no)}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {case_row['technician_name'] or '-'}\n"
                f"Servicio: {case_row['service_type'] or '-'}\n"
                f"Abonado: {case_row['abonado_code'] or '-'}\n\n"
                f"Texto:\n{text}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_auth_review(case_id, step_no, attempt),
        )
        return

    if int(case_row["step_index"]) != 2:
        return

    text = (msg.text or "").strip()
    if not text:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía el código de abonado como texto.")
        return

    update_case(int(case_row["case_id"]), abonado_code=text, step_index=3, phase=PHASE_WAIT_LOCATION)
    await context.bot.send_message(chat_id=msg.chat_id, text=f"✅ Código de abonado registrado: {text}\n\n{prompt_step4()}")


# =========================
# PASO 4: Ubicación
# =========================
async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = maybe_release_expired_case_lock(get_open_case(msg.chat_id))
    if not case_row:
        return

    if int(case_row["step_index"]) != 3:
        return

    if not msg.location:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía tu ubicación usando 📎 → Ubicación → ubicación actual.")
        return

    update_case(
        int(case_row["case_id"]),
        location_lat=msg.location.latitude,
        location_lon=msg.location.longitude,
        location_at=now_utc(),
        step_index=4,
        phase=PHASE_MENU_INST,
        pending_step_no=None,
    )

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text="PASO 5 - TIPO DE INSTALACIÓN\nSelecciona una opción:",
        reply_markup=kb_install_mode(),
    )


# =========================
# Carga de media
# =========================
async def on_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    case_row = maybe_release_expired_case_lock(get_open_case(msg.chat_id))
    if not case_row:
        return

    case_id = int(case_row["case_id"])
    pending_step_no = int(case_row["pending_step_no"] or 0)
    phase = (case_row["phase"] or "")

    if phase not in (PHASE_AUTH_MEDIA, PHASE_STEP_MEDIA):
        if int(case_row["step_index"]) >= 4:
            await context.bot.send_message(chat_id=msg.chat_id, text="ℹ️ Usa el menú para elegir el paso antes de enviar archivos.")
        return

    ok, why = can_user_operate_current_step(case_row, msg.from_user.id)
    if not ok and "revisión" not in why.lower():
        await context.bot.send_message(chat_id=msg.chat_id, text=why)
        return

    if pending_step_no < 5 or pending_step_no > 15:
        return

    if phase == PHASE_STEP_MEDIA:
        if not msg.photo:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso solo se aceptan FOTOS.")
            return
        file_type = "photo"
    else:
        if msg.photo:
            file_type = "photo"
        elif msg.video:
            file_type = "video"
        else:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En PERMISO multimedia se aceptan FOTO o VIDEO.")
            return

    if phase == PHASE_AUTH_MEDIA:
        step_no_to_store = -pending_step_no
        label = "AUTORIZACIÓN"
    else:
        step_no_to_store = pending_step_no
        label = "EVIDENCIA"

    st = ensure_step_state(case_id, step_no_to_store, owner_user_id=msg.from_user.id, owner_name=msg.from_user.full_name)
    attempt = int(st["attempt"])
    set_step_owner(case_id, step_no_to_store, attempt, msg.from_user.id, msg.from_user.full_name)
    lock_case_step(case_id, msg.from_user.id, msg.from_user.full_name)

    if int(st["submitted"]) == 1 and st["approved"] is None:
        await context.bot.send_message(chat_id=msg.chat_id, text="⏳ Ya está en revisión. Espera validación del administrador.")
        return
    if st["approved"] is not None and int(st["approved"]) == 1:
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Ya está aprobado. Continúa con el menú.")
        return

    current = media_count(case_id, step_no_to_store, attempt)
    if current >= MAX_MEDIA_PER_STEP:
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=f"⚠️ Ya llegaste al máximo de {MAX_MEDIA_PER_STEP}. Presiona ✅ EVIDENCIAS COMPLETAS.",
        )
        return

    if file_type == "photo":
        ph = msg.photo[-1]
        file_id = ph.file_id
        file_unique_id = ph.file_unique_id
    else:
        vd = msg.video
        file_id = vd.file_id if vd else ""
        file_unique_id = vd.file_unique_id if vd else ""

    meta = {
        "from_user_id": msg.from_user.id,
        "from_username": msg.from_user.username,
        "from_name": msg.from_user.full_name,
        "date": msg.date.isoformat() if msg.date else None,
        "caption": msg.caption,
        "phase": phase,
        "step_pending": pending_step_no,
        "attempt": attempt,
        "file_type": file_type,
        "media_group_id": msg.media_group_id,
    }

    add_media(
        case_id=case_id,
        step_no=step_no_to_store,
        attempt=attempt,
        file_type=file_type,
        file_id=file_id,
        file_unique_id=file_unique_id,
        tg_message_id=msg.message_id,
        meta=meta,
    )

    route = get_route_for_chat_cached(context.application, msg.chat_id)
    caption = (
        f"📌 {label} ({step_name(pending_step_no)})\n"
        f"Técnico: {case_row['technician_name'] or '-'}\n"
        f"Servicio: {case_row['service_type'] or '-'}\n"
        f"Abonado: {case_row['abonado_code'] or '-'}\n"
        f"Intento: {attempt}\n"
        f"Tipo: {file_type.upper()}"
    )
    await maybe_copy_to_group(context, route.get("evidence"), file_type, file_id, caption)

    if phase != PHASE_AUTH_MEDIA and file_type == "photo":
        enqueue_evidencia_row(case_row, pending_step_no, attempt, file_id, file_unique_id, msg.message_id, route.get("evidence"))

    upsert_media_ack_buffer(
        chat_id=msg.chat_id,
        case_id=case_id,
        step_no=step_no_to_store,
        attempt=attempt,
        phase=phase,
        user_id=msg.from_user.id,
        user_name=msg.from_user.full_name,
    )

# =========================
# Error handler
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Error no manejado:", exc_info=context.error)


# =========================
# Main
# =========================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN. Configura la variable BOT_TOKEN con el token de BotFather.")

    init_db()

    request = HTTPXRequest(connect_timeout=10, read_timeout=25, write_timeout=25, pool_timeout=10)
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("inicio", inicio_cmd))
    app.add_handler(CommandHandler("cancelar", cancelar_cmd))
    app.add_handler(CommandHandler("estado", estado_cmd))
    app.add_handler(CommandHandler("aprobacion", aprobacion_cmd))
    app.add_handler(CommandHandler("reabrir", reabrir_cmd))
    app.add_handler(CommandHandler("config", config_cmd))

    app.add_handler(CallbackQueryHandler(on_callbacks))

    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, on_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    app.add_error_handler(error_handler)

    try:
        sh = sheets_client()

        ws_casos = sh.worksheet("CASOS")
        ws_det = sh.worksheet("DETALLE_PASOS")
        ws_evid = sh.worksheet("EVIDENCIAS")
        ws_config = sh.worksheet("CONFIG")

        _ensure_headers(ws_casos, CASOS_COLUMNS)
        _ensure_headers(ws_det, DETALLE_PASOS_COLUMNS)
        _ensure_headers(ws_evid, EVIDENCIAS_COLUMNS)
        _ensure_headers(ws_config, CONFIG_COLUMNS)

        idx_casos = build_index(ws_casos, ["case_id"])
        idx_det = build_index(ws_det, ["case_id", "paso_numero", "attempt"])
        idx_evid = build_index(ws_evid, ["case_id", "paso_numero", "attempt", "mensaje_telegram_id"])

        ws_tecnicos = sh.worksheet(TECNICOS_TAB)
        ws_routing = sh.worksheet(ROUTING_TAB)
        ws_pairing = sh.worksheet(PAIRING_TAB)

        _ensure_headers(ws_tecnicos, TECNICOS_COLUMNS)
        _ensure_headers(ws_routing, ROUTING_COLUMNS)
        _ensure_headers(ws_pairing, PAIRING_COLUMNS)

        app.bot_data["sheets_ready"] = True
        app.bot_data["sh"] = sh

        app.bot_data["ws_casos"] = ws_casos
        app.bot_data["ws_det"] = ws_det
        app.bot_data["ws_evid"] = ws_evid
        app.bot_data["ws_config"] = ws_config
        app.bot_data["idx_casos"] = idx_casos
        app.bot_data["idx_det"] = idx_det
        app.bot_data["idx_evid"] = idx_evid

        app.bot_data["ws_tecnicos"] = ws_tecnicos
        app.bot_data["ws_routing"] = ws_routing
        app.bot_data["ws_pairing"] = ws_pairing

        load_tecnicos_cache(app)
        load_routing_cache(app)

        if app.job_queue:
            app.job_queue.run_repeating(sheets_worker, interval=20, first=5)
            app.job_queue.run_repeating(refresh_config_jobs, interval=30, first=10)
            app.job_queue.run_repeating(media_ack_worker, interval=2, first=2)

        log.info("Sheets: conectado. Worker iniciado. Config cache (TECNICOS/ROUTING) habilitado.")
    except Exception as e:
        app.bot_data["sheets_ready"] = False
        log.warning(f"Sheets deshabilitado: {e}")

    log.info("Bot corriendo...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
