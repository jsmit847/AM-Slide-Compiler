from __future__ import annotations

import csv
import io
import json
import re
import base64
import calendar
import hashlib
import secrets
import unicodedata
import zipfile
from copy import copy, deepcopy
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import requests
import streamlit as st
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from simple_salesforce import Salesforce
from midland_folder_picker.servicer_uploads import (
    clear_servicer_uploads,
    upload_berkadia_workbook,
    upload_caf_folder,
)

try:
    from pptx import Presentation
    from pptx.opc.constants import RELATIONSHIP_TYPE as PPTX_RT
    from pptx.oxml.ns import qn
except ImportError:  # the Excel workbook still builds without python-pptx
    Presentation = None

TERM_SHEET = "Term"
BRIDGE_SHEET = "Bridge"
DEFAULT_TEMPLATE_NAME = "Reference AM Templates.xlsx"
VALID_STAGES = ["Closed Won", "Expired", "Matured", "Paid Off", "Sold"]

TERM_RT_EXACT = {"term loan", "dscr"}
TERM_RT_CONTAINS = {"dscr"}
BRIDGE_RT_EXACT = {
    "acquired bridge loan",
    "bridge loan",
    "sab loan",
    "single asset bridge loan",
}
BRIDGE_RT_CONTAINS = {"sab", "single asset bridge"}
BRIDGE_DEV_EXACT = {"single_asset_bridge_loan"}
BRIDGE_DEV_CONTAINS = {"single_asset_bridge", "sab"}

DC_DEAL_FIELD = "Deal__c"

# Occupancy selection tuning (matches the notebook).
EXPECTED_FREQ_BY_QTR = {1: "Q1", 2: "Q2", 3: "Q3", 4: "AN"}
EXPECTED_MONTHS_BY_QTR = {1: 3, 2: 6, 3: 9, 4: 12}
CANONICAL_FREQS = {"Q1", "Q2", "Q3", "AN"}

# FCI Bridge late-payment / late-charge check settings.
FCI_URL = "https://fapi.myfci.com/graphql"
FCI_TIMEOUT_SECONDS = 45
FCI_ALERT_SHEET = "FCI Bridge Alerts"
# Used only for FCI matching. Stripped from the public bridge_rows before display/write.
FCI_INTERNAL_BRIDGE_COLUMNS = [
    "Opportunity Id",
    "Deal Loan Number Raw",
    "Servicer Commitment ID",
    "Property Servicer IDs",
]
FCI_LOAN_FIELDS = [
    "loanAccount",
    "nextDueDate",
    "paidToDate",
    "paidOffDate",
    "principalBalance",
    "status",
    "statusLender",
    "lateChargesDays",
    "lateChargesPct",
    "unpaidLateCharges",
    "unpaidLateChargesWaived",
    "deferredLateCharges",
    "poffAcurredLateCharges",
    "poffUnpaidLateCharges",
    "poffPaidLateCharges",
]

# -------------------------
# Midland CAF extract settings (McCracken "Enterprise!" bundle)
# -------------------------
# The CAF bundle is a .zip of pipe-delimited files. Each file starts with an
# "A" audit record, then a "C" record holding the column names, then one "D"
# record per row. Everything below is a constant so the mapping can be retuned
# without touching the parsing code.
CAF_DELIMITER = "|"
CAF_QUOTECHAR = '"'
CAF_COLUMN_RECORD = "C"
CAF_DATA_RECORD = "D"
CAF_DATE_FORMAT = "%m/%d/%Y %H:%M:%S"

# Lower-cased filename fragments used to find each extract inside the .zip.
# These are matched as substrings of the member's base name, which keeps them
# unambiguous: "loanextract" does not appear in "LoanToPropertyExtract",
# "LoanBalancesExtract", "LoanEntityExtract" or any other sibling.
CAF_RENTROLL_FILE_TOKEN = "rentrollextract"
CAF_LOAN_TO_PROPERTY_FILE_TOKEN = "loantopropertyextract"
CAF_LOAN_FILE_TOKEN = "loanextract"
CAF_ALT_ASSET_ID_FILE_TOKEN = "altassetidextract"

# RentRoll extract: occupancy lives here, keyed by collateral.
CAF_RR_COLLATERAL_COL = "ColID"
CAF_RR_EXTRACT_DATE_COL = "ExtractDate"
CAF_RR_PERIOD_DATE_COL = "PRRDt"
CAF_RR_MASTER_ID_COL = "PRRMstrID"
CAF_RR_OCC_PCT_COL = "PRRMOccPct"
CAF_RR_OCC_AMOUNT_COL = "PRRMSqFtUntOcc"
CAF_RR_TOTAL_AMOUNT_COL = "PRRMTotSqFtUnt"
CAF_RR_UOM_COL = "PRRMUOMTyp"

# LoanToProperty extract: ties collateral to loans.
CAF_L2P_COLLATERAL_COL = "ColID"
CAF_L2P_LOAN_COL = "LnID"

# Loan extract: the roster of real loans, and the fallback loan number for any
# loan that carries no Investor Loan Number. LnID is the servicer's own key,
# a 030xxxxxx number that does NOT line up with Salesforce on its own. The
# first column in the candidate list that exists in the file is used.
CAF_LOAN_ID_COL = "LnID"
CAF_LOAN_NUMBER_CANDIDATE_COLUMNS = ["LnID"]

# AltAssetId extract: the Investor Loan Number, which is what Salesforce holds
# in Deal_Loan_Number__c. Verified against a CAF bundle: 1,175 of 1,198 loans
# carry one, and those values are either bare 5-digit numbers or 9-digit
# numbers behind the "6000" prefix that last5_strip_prefix() already removes.
# The servicer's own LnID matches Salesforce for only the 23 loans that have no
# Investor Loan Number at all, which is why it is the fallback and not the key.
CAF_ALT_LOAN_COL = "LnID"
CAF_ALT_TYPE_COL = "AATyp"
CAF_ALT_VALUE_COL = "AltLnID"
CAF_ALT_INVESTOR_LOAN_TYPE = "INVLN"
# Prefer the Investor Loan Number, falling back to the Loan extract's own
# number for loans that have none. Set False to key off the Loan extract only.
CAF_PREFER_INVESTOR_LOAN_NUMBER = True
# Additionally accept the Loan extract's own number for every loan. Off by
# default: it adds keys Salesforce does not use and invites false matches.
CAF_ALSO_MATCH_ON_SERVICER_LOAN_ID = False
# A loan number needs at least this many digits to be believable. The Loan
# extract carries a few non-loan rows such as "2043Funds", and without this
# they would turn into invented 5-digit keys that no real loan owns.
CAF_MIN_LOAN_NUMBER_DIGITS = 5

# Picking bundles out of a folder. Bundles are named CAF_Extracts_MMDDYY_HHMM.zip,
# and a bundle's rent rolls run about a quarter behind: the 12/31/25 bundle
# carries the 09/30/25 rent rolls, the 04/01/26 bundle the 12/31/25 ones, and so
# on. So the bundle that fills a quarter is the one dated roughly this many days
# after that quarter ended.
CAF_BUNDLE_NAME_RX = re.compile(r"^CAF_Extracts_(\d{2})(\d{2})(\d{2})_\d{4}\.zip$", re.IGNORECASE)
CAF_RENT_ROLL_LAG_DAYS = 90

# Berkadia data tapes are named like CoreVest_Data_Tape_09_19_2026.xlsx. Unlike
# a CAF bundle, one tape's Financial Analysis sheet is already a history: a
# recent tape carries roughly three years of quarters. So the newest tape is
# normally all that is needed, and older ones are only pulled in when the
# template reaches back further than the newest tape goes.
BERKADIA_TAPE_NAME_RX = re.compile(r"_(\d{2})_(\d{2})_(\d{4})\.xls[xm]$", re.IGNORECASE)
BERKADIA_TAPE_SUFFIXES = (".xlsx", ".xlsm")
# The folder also holds same-shaped files that are not data tapes, such as
# CoreVest_Transactions_MTD_09_18_2026.xlsx. Prefer names carrying this hint,
# and fall back to every dated workbook only when nothing matches.
BERKADIA_TAPE_NAME_HINT = "data_tape"
# Archived files sit in year folders and then a per-report folder, for example
# R:\Berkadia\2026\Data Tape, so two levels down are searched as well.
SERVICER_FOLDER_SEARCH_DEPTH = 2
BERKADIA_MAX_TAPES_TO_POOL = 4

# Guarantor scrubbing: a value is treated as an ID rather than a name when it
# has no letters, when it contains a digit run longer than this, or when it is
# more digits than letters. Years and small numbers in real entity names pass.
GUARANTOR_MAX_DIGIT_RUN = 5

# Placeholder written into every otherwise-empty cell of the finished slide.
BLANK_CELL_PLACEHOLDER = "-"

# -------------------------
# PowerPoint slide decks
# -------------------------
# The repo copies carry neutral sample text; the second name in each pair is
# how the templates are named in the AM team's own folder, so a fresh copy
# dropped in from there works too.
TERM_PPTX_TEMPLATE_NAMES = ("AM_TERM_SLIDE_TEMPLATE.pptx", "AM TERM SLIDE TEMPLATE.pptx")
BRIDGE_PPTX_TEMPLATE_NAMES = ("AM_BRIDGE_SLIDE_TEMPLATE.pptx", "AM BRIDGE SLIDE TEMPLATE.pptx")
TERM_DECK_TITLE = "CAF Asset Management Feedback"
BRIDGE_DECK_TITLE = "Asset Management Bridge Overview"
PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
# Empty Total-row cells hold a non-breaking space, as in every finished deck.
SLIDE_TOTAL_BLANK = "\u00a0"

# Slide geometry, in EMU (12700 per point), measured on the AM team's decks.
EMU_PER_PT = 12700
SLIDE_TABLE_TOP_EMU = 1299182            # 102.3pt, just under the intro line
SLIDE_TABLE_LEFT_EMU = 342900            # 27pt; the tables are 666pt wide
CONT_SLIDE_TABLE_TOP_EMU = 376224        # 29.6pt on table-only continuation slides
CONT_SLIDE_TABLE_FLOOR_EMU = 460 * EMU_PER_PT
SLIDE_TEXT_FLOOR_EMU = 510 * EMU_PER_PT  # page number and footer band sit below this
COMMENTARY_GAP_EMU = 15 * EMU_PER_PT     # table bottom to the commentary box
# The commentary renders 12pt, 110% spacing, 5pt before each bullet: text
# starts 3.6pt into the box, the first line takes 14.62pt and each further
# line 20.84pt. Room is kept for one more bullet than the template has, which
# covers most decks (2 bullets in 36 of 44 recent ones, 3 in 6, 4 in 2).
INTRO_FONT_PT = 12.0                     # the intro line's size in the One Column layout
COMMENTARY_INSET_EMU = int(3.6 * EMU_PER_PT)
COMMENTARY_FIRST_LINE_EMU = int(14.62 * EMU_PER_PT)
COMMENTARY_NEXT_LINE_EMU = int(20.84 * EMU_PER_PT)
COMMENTARY_SPARE_LINES = 1
# The Term chart legend's five lines are centered on a box shorter than they
# are, so they spill 14pt above it; keep the commentary this far clear of that.
LEGEND_TEXT_HEIGHT_EMU = 48 * EMU_PER_PT
LEGEND_CLEARANCE_EMU = 8 * EMU_PER_PT
# Column widths: no column may be narrower than its widest unbreakable value
# plus this, so a figure like $11,127,500 never splits across lines.
COLUMN_FIT_SAFETY_EMU = 6350
# The templates' cells have no side margins, so once columns are fitted to
# their text, neighbouring values would touch ("5A-11521SUINUJ"). Each cell
# keeps at least this much on either side, so values stay 3pt apart.
CELL_SIDE_MARGIN_EMU = 19050
COLUMN_WIDTH_STEP_EMU = 6350             # widths move 0.5pt at a time
COLUMN_WIDTH_MAX_ROUNDS = 80
# A continuation slide carries at least this many loans along with the Total.
SLIDE_MIN_LAST_PAGE_ROWS = 3


# -------------------------
# Basic helpers
# -------------------------
def install_truststore() -> None:
    try:
        import truststore

        truststore.inject_into_ssl()
    except Exception:
        pass



def soql_quote(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"



def digits_only(value: Any) -> str:
    return re.sub(r"\D", "", "" if value is None or pd.isna(value) else str(value))



def last5_strip_prefix(value: Any) -> str:
    digits = digits_only(value)
    if digits.startswith("4030") or digits.startswith("6000"):
        digits = digits[4:]
    return digits[-5:] if len(digits) >= 5 else digits



def loan_id_5(value: Any) -> str:
    digits = last5_strip_prefix(value)
    return str(digits).zfill(5) if str(digits).strip() else ""



def pct_to_dec(value: Any) -> float | None:
    if value in ("", None) or pd.isna(value):
        return None
    text = str(value).strip().replace("%", "")
    try:
        number = float(text)
        return number / 100.0 if number > 1.5 else number
    except Exception:
        return None



def parse_date_any(value: Any):
    if value in ("", None) or pd.isna(value):
        return None
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()



def extract_states_only(state_percentages: Any) -> str:
    text = "" if state_percentages is None or pd.isna(state_percentages) else str(state_percentages)
    codes = re.findall(r"\b[A-Z]{2}\b", text.upper())
    if not codes:
        return text.strip()
    seen: set[str] = set()
    out: list[str] = []
    for code in codes:
        if code not in seen:
            seen.add(code)
            out.append(code)
    return ", ".join(out)



def norm_rt(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).strip().lower())



def safe_flatten_recordtype(df: pd.DataFrame) -> pd.DataFrame:
    if "RecordType" in df.columns:
        df["RecordType.Name"] = df["RecordType"].apply(lambda x: (x or {}).get("Name"))
        df["RecordType.DeveloperName"] = df["RecordType"].apply(
            lambda x: (x or {}).get("DeveloperName")
        )
        df = df.drop(columns=["RecordType"], errors="ignore")
    return df



def clean_text_or_blank(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return text



def deal_record_type_display(row, fallback: str = "") -> str:
    """
    Returns the human-readable Opportunity record type for the Portfolio column.
    Prefers RecordType.Name. Falls back to RecordType.DeveloperName, then provided fallback.
    """
    name = clean_text_or_blank(row.get("RecordType.Name", ""))
    if name:
        return name

    dev_name = clean_text_or_blank(row.get("RecordType.DeveloperName", ""))
    if dev_name:
        return dev_name

    return fallback



def nonblank_or_fallback(value, fallback: str):
    value = clean_text_or_blank(value)
    return value if value else fallback



_GUARANTOR_DIGIT_RUN_RX = re.compile(r"\d{%d,}" % (GUARANTOR_MAX_DIGIT_RUN + 1))


def is_junk_guarantor_token(value: Any) -> bool:
    """
    True when a guarantor value looks like an identifier rather than a name.

    Catches record ids and bare number strings while keeping real entity names
    that happen to carry a year or a small number, such as
    "Colony American Finance 2015-1" or "2020 Main Street Holdings LLC".
    """
    text = clean_text_or_blank(value)
    if text == "":
        return True

    letters = sum(1 for ch in text if ch.isalpha())
    digits = sum(1 for ch in text if ch.isdigit())

    if letters == 0:
        return True
    if _GUARANTOR_DIGIT_RUN_RX.search(text):
        return True
    if digits > letters:
        return True
    return False


def clean_guarantor_name(value: Any) -> str:
    """Returns the guarantor name, or blank when the value is ID-like junk."""
    text = clean_text_or_blank(value)
    if text == "" or is_junk_guarantor_token(text):
        return ""
    return text



def chunked(values, size: int = 200):
    values = list(values)
    for i in range(0, len(values), size):
        yield values[i : i + size]



def build_where_for_search(mode: str, query_text: str) -> str:
    query_text = (query_text or "").strip()
    if mode == "Account Name":
        return "Account_Name__c LIKE " + soql_quote("%" + query_text + "%")
    if mode == "Deal Name":
        return "Name LIKE " + soql_quote("%" + query_text + "%")

    digits = re.sub(r"\D", "", query_text)
    if digits:
        return "(" + " OR ".join(
            [
                "Deal_Loan_Number__c = " + soql_quote(digits),
                "Deal_Loan_Number__c LIKE " + soql_quote("%" + digits + "%"),
                "Deal_Loan_Number__c LIKE " + soql_quote("%" + query_text + "%"),
            ]
        ) + ")"
    return "Deal_Loan_Number__c LIKE " + soql_quote("%" + query_text + "%")



def try_query_drop_missing(
    sf: Salesforce,
    object_name: str,
    fields: list[str],
    where_clause: str,
    limit: int = 2000,
    order_by: str | None = None,
):
    fields = list(fields)
    while True:
        soql = f"SELECT {', '.join(fields)} FROM {object_name} WHERE {where_clause}"
        if order_by:
            soql += f" ORDER BY {order_by}"
        soql += f" LIMIT {int(limit)}"
        try:
            rows = sf.query_all(soql).get("records", [])
            return rows, fields, soql
        except Exception as exc:
            message = str(exc)
            missing_column = re.search(r"No such column '([^']+)'", message)
            bad_relationship = re.search(r"Didn't understand relationship '([^']+)'", message)

            if missing_column:
                bad = missing_column.group(1)
                if bad in fields:
                    fields.remove(bad)
                    continue

            if bad_relationship:
                relbad = bad_relationship.group(1)
                to_drop = [
                    field
                    for field in fields
                    if field.startswith(relbad + ".") or (("." + relbad + ".") in field)
                ]
                if to_drop:
                    for field in to_drop:
                        fields.remove(field)
                    continue
            raise



def classify_term_bridge(df_all: pd.DataFrame):
    if "RecordType.Name" not in df_all.columns:
        return df_all.iloc[0:0].copy(), df_all.iloc[0:0].copy()

    rt_name = df_all["RecordType.Name"].apply(norm_rt)
    rt_dev = df_all.get(
        "RecordType.DeveloperName",
        pd.Series([""] * len(df_all), index=df_all.index),
    )
    rt_dev = rt_dev.fillna("").astype(str).str.strip().str.lower()

    term_exact = rt_name.isin(TERM_RT_EXACT)
    term_contains = pd.Series(False, index=df_all.index)
    for token in TERM_RT_CONTAINS:
        term_contains = term_contains | rt_name.str.contains(re.escape(token), na=False)
    is_term = term_exact | term_contains

    bridge_exact = rt_name.isin(BRIDGE_RT_EXACT)
    bridge_contains = pd.Series(False, index=df_all.index)
    for token in BRIDGE_RT_CONTAINS:
        bridge_contains = bridge_contains | rt_name.str.contains(re.escape(token), na=False)

    bridge_dev_exact = rt_dev.isin(BRIDGE_DEV_EXACT)
    bridge_dev_contains = pd.Series(False, index=df_all.index)
    for token in BRIDGE_DEV_CONTAINS:
        bridge_dev_contains = bridge_dev_contains | rt_dev.str.contains(re.escape(token), na=False)

    is_bridge = bridge_exact | bridge_contains | bridge_dev_exact | bridge_dev_contains
    is_bridge = is_bridge & (~is_term)
    return df_all[is_term].copy(), df_all[is_bridge].copy()


# -------------------------
# Salesforce connection
# -------------------------
def load_salesforce_oauth_config() -> dict[str, str]:
    try:
        secrets_section = dict(st.secrets.get("salesforce", {}))
    except Exception:
        secrets_section = {}

    required_keys = ["client_id", "client_secret", "auth_host", "redirect_uri"]
    missing_keys = [key for key in required_keys if not secrets_section.get(key)]
    if missing_keys:
        raise RuntimeError(
            "Missing Salesforce OAuth secrets: " + ", ".join(missing_keys)
            + ". Add them under [salesforce] in Streamlit secrets."
        )
    return secrets_section



@st.cache_resource
def _pkce_store() -> dict:
    # Process-level store that survives the redirect to Salesforce and back
    # (st.session_state does not persist across the full page reload).
    return {}



def generate_pkce_pair() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode("ascii")
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
    return verifier, challenge



def build_salesforce_login_url(oauth_config: dict[str, str]) -> str:
    auth_host = str(oauth_config["auth_host"]).rstrip("/")

    state = secrets.token_urlsafe(24)
    verifier, challenge = generate_pkce_pair()

    store = _pkce_store()
    store[state] = verifier
    # Keep the store from growing without bound across reruns.
    if len(store) > 50:
        for old_key in list(store.keys())[:-50]:
            store.pop(old_key, None)

    query = urlencode(
        {
            "response_type": "code",
            "client_id": oauth_config["client_id"],
            "redirect_uri": oauth_config["redirect_uri"],
            "scope": oauth_config.get("scope", "api refresh_token"),
            "prompt": oauth_config.get("prompt", "login"),
            "state": state,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{auth_host}/services/oauth2/authorize?{query}"



def exchange_salesforce_code_for_token(
    oauth_config: dict[str, str],
    code: str,
    code_verifier: str | None = None,
) -> dict[str, Any]:
    install_truststore()
    auth_host = str(oauth_config["auth_host"]).rstrip("/")
    token_url = f"{auth_host}/services/oauth2/token"
    form_fields = {
        "grant_type": "authorization_code",
        "client_id": oauth_config["client_id"],
        "client_secret": oauth_config["client_secret"],
        "redirect_uri": oauth_config["redirect_uri"],
        "code": code,
    }
    if code_verifier:
        form_fields["code_verifier"] = code_verifier
    payload = urlencode(form_fields).encode("utf-8")
    request = Request(
        token_url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        detail = body
        try:
            parsed = json.loads(body)
            detail = parsed.get("error_description") or parsed.get("error") or body
        except Exception:
            pass
        raise RuntimeError(f"Salesforce login failed: {detail}") from exc



def read_query_param(name: str) -> str | None:
    value = st.query_params.get(name)
    if isinstance(value, list):
        return value[0] if value else None
    return value



def clear_query_params() -> None:
    try:
        st.query_params.clear()
    except Exception:
        pass



def clear_salesforce_session() -> None:
    clear_servicer_uploads()
    for key in [
        "salesforce_auth",
        "account_candidates",
        "party_candidates",
        "party_deals",
        "term_preview",
        "bridge_preview",
        "occupancy_selected",
        "occupancy_issues",
        "occ_headers",
        "workbook_bytes",
        "workbook_name",
        "match_count",
        "term_count",
        "fci_alerts",
        "fci_matches",
        "fci_ran",
        "deck_kinds",
        "deck_default_name",
        "deck_title_name",
        "deck_term_intro",
        "deck_bridge_intro",
        "late_term_loans",
        "_midland_folder_upload",
    ]:
        st.session_state.pop(key, None)



def maybe_finish_salesforce_oauth(oauth_config: dict[str, str]) -> None:
    oauth_error = read_query_param("error")
    if oauth_error:
        oauth_description = read_query_param("error_description") or oauth_error
        clear_query_params()
        raise RuntimeError(f"Salesforce login was not completed: {oauth_description}")

    code = read_query_param("code")
    if not code:
        return

    if st.session_state.get("_last_salesforce_code") == code and st.session_state.get("salesforce_auth"):
        clear_query_params()
        return

    state = read_query_param("state")
    code_verifier = _pkce_store().pop(state, None) if state else None
    if state and not code_verifier:
        clear_query_params()
        raise RuntimeError(
            "Login could not be completed (the PKCE verifier was not found, "
            "usually because the app restarted between login steps). "
            "Click 'Log in to Salesforce' and try again."
        )

    token_payload = exchange_salesforce_code_for_token(oauth_config, code, code_verifier)
    access_token = token_payload.get("access_token")
    instance_url = token_payload.get("instance_url")
    if not access_token or not instance_url:
        raise RuntimeError("Salesforce login succeeded, but no access token or instance URL was returned.")

    st.session_state["salesforce_auth"] = {
        "access_token": access_token,
        "instance_url": instance_url,
        "issued_at": token_payload.get("issued_at"),
        "id_url": token_payload.get("id"),
        "signature": token_payload.get("signature"),
    }
    st.session_state["_last_salesforce_code"] = code
    clear_query_params()
    st.rerun()



def get_salesforce_client_from_session() -> Salesforce | None:
    install_truststore()
    auth_data = st.session_state.get("salesforce_auth", {})
    instance_url = auth_data.get("instance_url")
    access_token = auth_data.get("access_token")
    if not instance_url or not access_token:
        return None
    return Salesforce(instance_url=instance_url, session_id=access_token)



def normalize_salesforce_error(exc: Exception) -> RuntimeError:
    message = str(exc)
    if "INVALID_SESSION_ID" in message or "Session expired" in message:
        clear_salesforce_session()
        return RuntimeError("Your Salesforce session expired. Click 'Log in to Salesforce' and try again.")
    return RuntimeError(message)


# -------------------------
# Query helpers
# -------------------------
def search_matching_accounts(sf: Salesforce, search_mode: str, query_text: str) -> pd.DataFrame:
    preview_fields = [
        "Id",
        "Name",
        "Deal_Loan_Number__c",
        "Account_Name__c",
        "RecordType.Name",
        "RecordType.DeveloperName",
        "StageName",
        "CloseDate",
    ]
    where_search = (
        build_where_for_search(search_mode, query_text)
        + " AND StageName IN ("
        + ", ".join(soql_quote(stage) for stage in VALID_STAGES)
        + ")"
    )
    preview_rows, _, _ = try_query_drop_missing(
        sf,
        "Opportunity",
        preview_fields,
        where_search,
        limit=2000,
        order_by="CloseDate DESC NULLS LAST",
    )
    if not preview_rows:
        return pd.DataFrame(columns=["Account_Name__c", "loans"])

    df_preview = pd.DataFrame(preview_rows).drop(columns=["attributes"], errors="ignore")
    df_preview = safe_flatten_recordtype(df_preview)
    df_preview = df_preview[df_preview["Account_Name__c"].notna()].copy()
    account_counts = (
        df_preview.groupby("Account_Name__c", dropna=False)
        .size()
        .reset_index(name="loans")
        .sort_values(["loans", "Account_Name__c"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return account_counts



def find_contact_ref_field_on_deal_contact(sf: Salesforce):
    description = sf.Deal_Contact__c.describe()
    for field in description.get("fields", []):
        if field.get("type") == "reference":
            reference_to = field.get("referenceTo") or []
            if any(str(item).lower() == "contact" for item in reference_to):
                return field.get("name"), field.get("relationshipName")
    return None, None



def query_deal_contacts_for_guarantors(sf: Salesforce, opportunity_ids: list[str]) -> pd.DataFrame:
    if not opportunity_ids:
        return pd.DataFrame(columns=[DC_DEAL_FIELD, "GuarantorName"])

    _, contact_relationship_name = find_contact_ref_field_on_deal_contact(sf)
    contact_name_path = (
        f"{contact_relationship_name}.Name" if contact_relationship_name else None
    )

    fields = ["Id", DC_DEAL_FIELD, "Is_Guarantor__c", "Name"]
    if contact_name_path:
        fields.append(contact_name_path)

    rows: list[dict[str, Any]] = []
    for group in chunked(opportunity_ids, 150):
        ids_in = ", ".join(soql_quote(item) for item in group)
        trial_fields = list(fields)
        while True:
            soql = (
                "SELECT "
                + ", ".join(trial_fields)
                + " FROM Deal_Contact__c"
                + f" WHERE {DC_DEAL_FIELD} IN ({ids_in})"
                + " AND Is_Guarantor__c = TRUE"
            )
            try:
                result = sf.query_all(soql)
                rows.extend(result.get("records", []))
                break
            except Exception as exc:
                message = str(exc)
                missing_column = re.search(r"No such column '([^']+)'", message)
                bad_relationship = re.search(r"Didn't understand relationship '([^']+)'", message)

                if missing_column and missing_column.group(1) in trial_fields:
                    trial_fields.remove(missing_column.group(1))
                    continue

                if bad_relationship:
                    relbad = bad_relationship.group(1)
                    to_drop = [
                        field
                        for field in trial_fields
                        if field.startswith(relbad + ".") or (("." + relbad + ".") in field)
                    ]
                    if to_drop:
                        for field in to_drop:
                            trial_fields.remove(field)
                        continue
                raise

    df = pd.DataFrame(rows).drop(columns=["attributes"], errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=[DC_DEAL_FIELD, "GuarantorName"])

    if contact_relationship_name and contact_relationship_name in df.columns:
        df["ContactName"] = df[contact_relationship_name].apply(lambda x: (x or {}).get("Name"))
    else:
        df["ContactName"] = None

    df["GuarantorName"] = df["ContactName"]
    missing_mask = df["GuarantorName"].isna() | (df["GuarantorName"].astype(str).str.strip() == "")
    df.loc[missing_mask, "GuarantorName"] = df.loc[missing_mask, "Name"]

    # Drop ID-like values so they never reach the slide.
    df["GuarantorName"] = df["GuarantorName"].apply(clean_guarantor_name)
    df = df[df["GuarantorName"] != ""].copy()
    if df.empty:
        return pd.DataFrame(columns=[DC_DEAL_FIELD, "GuarantorName"])

    return df[[DC_DEAL_FIELD, "GuarantorName"]].copy()



# -------------------------
# Sponsor / guarantor search
# -------------------------
def record_path_value(record: dict, path: str):
    """Reads a dotted SOQL path such as 'Contact__r.Name' out of a query record."""
    value: Any = record
    for part in path.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value



def find_party_fields(sf: Salesforce) -> dict:
    """
    Works out, from the org's own schema, where sponsors and guarantors live.

    Guarantors are Deal_Contact__c rows with Is_Guarantor__c. Sponsors are
    looked for the same way (a checkbox or a role picklist on Deal_Contact__c
    that mentions "sponsor") and also as a sponsor field on the Opportunity
    itself. Only fields that exist and are filterable are returned, so the
    queries built from them cannot fail on a missing column.
    """
    dc_fields = sf.Deal_Contact__c.describe().get("fields", [])
    dc_name_paths = ["Name"]
    dc_sponsor_flags = []
    dc_sponsor_picklists = []
    for field in dc_fields:
        name = field.get("name") or ""
        label = field.get("label") or ""
        mentions_sponsor = "sponsor" in name.lower() or "sponsor" in label.lower()
        field_type = field.get("type")
        if field_type == "reference" and field.get("relationshipName"):
            reference_to = [str(item).lower() for item in (field.get("referenceTo") or [])]
            if "contact" in reference_to or "account" in reference_to:
                dc_name_paths.append(field["relationshipName"] + ".Name")
        elif field_type == "boolean" and mentions_sponsor and field.get("filterable", True):
            dc_sponsor_flags.append(name)
        elif field_type in ("picklist", "multipicklist") and field.get("filterable", True):
            sponsor_values = [
                item.get("value")
                for item in (field.get("picklistValues") or [])
                if "sponsor" in str(item.get("value") or "").lower()
            ]
            if sponsor_values:
                dc_sponsor_picklists.append((name, field_type, sponsor_values))

    opp_sponsor_paths = []
    for field in sf.Opportunity.describe().get("fields", []):
        name = field.get("name") or ""
        label = field.get("label") or ""
        if "sponsor" not in name.lower() and "sponsor" not in label.lower():
            continue
        if not field.get("filterable", True):
            continue
        if field.get("type") == "reference" and field.get("relationshipName"):
            opp_sponsor_paths.append(field["relationshipName"] + ".Name")
        elif field.get("type") in ("string", "picklist", "combobox"):
            opp_sponsor_paths.append(name)

    return {
        "dc_name_paths": dc_name_paths,
        "dc_sponsor_flags": dc_sponsor_flags,
        "dc_sponsor_picklists": dc_sponsor_picklists,
        "opp_sponsor_paths": opp_sponsor_paths,
    }



def pick_matched_name(values, query_text: str) -> str:
    """The first candidate name that contains what was typed, else the first usable one."""
    needle = (query_text or "").strip().casefold()
    cleaned = [clean_guarantor_name(value) for value in values]
    cleaned = [value for value in cleaned if value]
    for value in cleaned:
        if needle and needle in value.casefold():
            return value
    return cleaned[0] if cleaned else ""



def search_deals_by_party(sf: Salesforce, query_text: str) -> pd.DataFrame:
    """
    Every closed deal where a sponsor or guarantor name contains query_text.
    One row per deal, matched name and role, across all accounts.
    """
    columns = ["Party", "Role", "Opportunity Id"]
    query_text = (query_text or "").strip()
    if not query_text:
        return pd.DataFrame(columns=columns)

    fields = find_party_fields(sf)
    like = soql_quote("%" + query_text + "%")
    hits: list[dict[str, Any]] = []

    # Deal contacts flagged as guarantor or sponsor.
    role_clauses = ["Is_Guarantor__c = TRUE"]
    role_clauses += [f"{flag} = TRUE" for flag in fields["dc_sponsor_flags"]]
    for name, field_type, values in fields["dc_sponsor_picklists"]:
        quoted = ", ".join(soql_quote(value) for value in values)
        operator = "INCLUDES" if field_type == "multipicklist" else "IN"
        role_clauses.append(f"{name} {operator} ({quoted})")
    name_clauses = [f"{path} LIKE {like}" for path in fields["dc_name_paths"]]
    dc_select = ["Id", DC_DEAL_FIELD, "Is_Guarantor__c"] + fields["dc_name_paths"]
    dc_select += fields["dc_sponsor_flags"]
    dc_select += [name for name, _type, _values in fields["dc_sponsor_picklists"]]
    dc_rows, _, _ = try_query_drop_missing(
        sf,
        "Deal_Contact__c",
        list(dict.fromkeys(dc_select)),
        "(" + " OR ".join(role_clauses) + ") AND (" + " OR ".join(name_clauses) + ")",
        limit=5000,
    )
    for record in dc_rows:
        party = pick_matched_name(
            [record_path_value(record, path) for path in reversed(fields["dc_name_paths"])],
            query_text,
        )
        if not party or not record.get(DC_DEAL_FIELD):
            continue
        is_sponsor = any(record.get(flag) for flag in fields["dc_sponsor_flags"]) or any(
            "sponsor" in str(record.get(name) or "").lower()
            for name, _type, _values in fields["dc_sponsor_picklists"]
        )
        if record.get("Is_Guarantor__c"):
            hits.append({"Party": party, "Role": "Guarantor", "Opportunity Id": record[DC_DEAL_FIELD]})
        if is_sponsor:
            hits.append({"Party": party, "Role": "Sponsor", "Opportunity Id": record[DC_DEAL_FIELD]})

    # Sponsor fields held directly on the deal.
    if fields["opp_sponsor_paths"]:
        opp_where = (
            "(" + " OR ".join(f"{path} LIKE {like}" for path in fields["opp_sponsor_paths"]) + ")"
            + " AND " + valid_stage_clause()
        )
        opp_rows, _, _ = try_query_drop_missing(
            sf, "Opportunity", ["Id"] + fields["opp_sponsor_paths"], opp_where, limit=2000
        )
        for record in opp_rows:
            party = pick_matched_name(
                [record_path_value(record, path) for path in fields["opp_sponsor_paths"]],
                query_text,
            )
            if party:
                hits.append({"Party": party, "Role": "Sponsor", "Opportunity Id": record["Id"]})

    if not hits:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(hits).drop_duplicates().reset_index(drop=True)



def search_matching_parties(sf: Salesforce, query_text: str):
    """
    Returns (party summary, deal detail) for a sponsor/guarantor name search.
    Only deals in VALID_STAGES are kept, the same as the account search.
    """
    summary_columns = ["Sponsor / Guarantor", "Role", "loans", "Accounts"]
    hits = search_deals_by_party(sf, query_text)
    if hits.empty:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame()

    deal_ids = hits["Opportunity Id"].dropna().astype(str).unique().tolist()
    deal_rows: list[dict[str, Any]] = []
    for group in chunked(deal_ids, 200):
        where_ids = (
            "Id IN (" + ", ".join(soql_quote(item) for item in group) + ") AND " + valid_stage_clause()
        )
        rows, _, _ = try_query_drop_missing(
            sf,
            "Opportunity",
            ["Id", "Name", "Deal_Loan_Number__c", "Account_Name__c", "StageName", "CloseDate"],
            where_ids,
            limit=2000,
        )
        deal_rows.extend(rows)
    if not deal_rows:
        return pd.DataFrame(columns=summary_columns), pd.DataFrame()

    df_deals = pd.DataFrame(deal_rows).drop(columns=["attributes"], errors="ignore")
    detail = hits.merge(df_deals, left_on="Opportunity Id", right_on="Id", how="inner")
    detail = detail.drop(columns=["Id"])

    # One spelling per name, whatever case Salesforce holds it in.
    detail["Party Key"] = detail["Party"].str.strip().str.casefold()
    display_name = detail.groupby("Party Key")["Party"].agg(lambda s: s.mode().iloc[0])
    detail["Party"] = detail["Party Key"].map(display_name)

    summary = (
        detail.groupby("Party", dropna=False)
        .agg(
            Role=("Role", lambda s: " / ".join(sorted(set(s)))),
            loans=("Opportunity Id", "nunique"),
            Accounts=("Account_Name__c", lambda s: ", ".join(sorted({str(x) for x in s if pd.notna(x)}))),
        )
        .reset_index()
        .rename(columns={"Party": "Sponsor / Guarantor"})
        .sort_values(["loans", "Sponsor / Guarantor"], ascending=[False, True])
        .reset_index(drop=True)
    )
    detail = (
        detail.groupby(["Party", "Opportunity Id"], dropna=False)
        .agg(
            Role=("Role", lambda s: " / ".join(sorted(set(s)))),
            Deal=("Name", "first"),
            Loan_Number=("Deal_Loan_Number__c", "first"),
            Account=("Account_Name__c", "first"),
            Stage=("StageName", "first"),
            Close_Date=("CloseDate", "first"),
        )
        .reset_index()
        .rename(columns={"Loan_Number": "Deal Loan Number", "Close_Date": "Close Date"})
        .sort_values(["Party", "Close Date"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return summary, detail



def valid_stage_clause() -> str:
    return "StageName IN (" + ", ".join(soql_quote(stage) for stage in VALID_STAGES) + ")"



def build_term_bridge_for_account(sf: Salesforce, account_name: str):
    where_account = "Account_Name__c = " + soql_quote(account_name) + " AND " + valid_stage_clause()
    return build_term_bridge_for_where(sf, where_account)



def build_term_bridge_for_deal_ids(sf: Salesforce, deal_ids):
    deal_ids = [str(item) for item in deal_ids if str(item).strip()]
    if not deal_ids:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    where_ids = (
        "Id IN (" + ", ".join(soql_quote(item) for item in deal_ids) + ") AND " + valid_stage_clause()
    )
    return build_term_bridge_for_where(sf, where_ids)



def build_term_bridge_for_where(sf: Salesforce, where_clause: str):
    opportunity_fields = [
        "Id",
        "Name",
        "Deal_Loan_Number__c",
        "Account_Name__c",
        "RecordType.Name",
        "RecordType.DeveloperName",
        "StageName",
        "CloseDate",
        "Amount",
        "Current_UPB__c",
        "UW_LTV__c",
        "Rate__c",
        "Current_Svc_Interest_Rate__c",
        "State_Percentages__c",
        "Total_Properties__c",
        "Total_Units__c",
        "Recourse__c",
        "Historical_Ontime_Payments_Percentage__c",
        "Next_Payment_Date__c",
        "Stated_Maturity_Date__c",
        "Original_Line_Maturity_Date__c",
        "Aggregate_Funding__c",
        "Servicer_Commitment_Id__c",
    ]

    rows, _, _ = try_query_drop_missing(
        sf,
        "Opportunity",
        opportunity_fields,
        where_clause,
        limit=2000,
        order_by="CloseDate DESC NULLS LAST",
    )

    df_all = pd.DataFrame(rows).drop(columns=["attributes"], errors="ignore")
    df_all = safe_flatten_recordtype(df_all)
    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df_all["InterestRate_Picked"] = df_all.get("Rate__c")
    blank_rate = df_all["InterestRate_Picked"].isna() | (
        df_all["InterestRate_Picked"].astype(str).str.strip() == ""
    )
    df_all.loc[blank_rate, "InterestRate_Picked"] = df_all.loc[
        blank_rate, "Current_Svc_Interest_Rate__c"
    ]

    df_all["LTV_Dec"] = df_all.get("UW_LTV__c").apply(pct_to_dec) if "UW_LTV__c" in df_all.columns else None
    df_all["Rate_Dec"] = df_all["InterestRate_Picked"].apply(pct_to_dec)
    df_all["OriginationDate_dt"] = df_all.get("CloseDate").apply(parse_date_any)
    df_all["NextPay_dt"] = df_all.get("Next_Payment_Date__c").apply(parse_date_any)
    df_all["Maturity_Picked_raw"] = df_all.get("Stated_Maturity_Date__c")
    missing_maturity = df_all["Maturity_Picked_raw"].isna() | (
        df_all["Maturity_Picked_raw"].astype(str).str.strip() == ""
    )
    df_all.loc[missing_maturity, "Maturity_Picked_raw"] = df_all.loc[
        missing_maturity, "Original_Line_Maturity_Date__c"
    ]
    df_all["Maturity_dt"] = df_all["Maturity_Picked_raw"].apply(parse_date_any)

    df_term_raw, df_bridge_opp = classify_term_bridge(df_all)

    if not df_term_raw.empty:
        term_ids = df_term_raw["Id"].dropna().astype(str).unique().tolist()
        df_contacts = query_deal_contacts_for_guarantors(sf, term_ids)
        if not df_contacts.empty:
            guarantor_map = (
                df_contacts.groupby(DC_DEAL_FIELD)["GuarantorName"]
                .apply(
                    lambda s: ", ".join(
                        pd.unique(
                            [
                                name
                                for name in (clean_guarantor_name(item) for item in s.tolist())
                                if name != ""
                            ]
                        )
                    )
                )
                .reset_index()
                .rename(columns={DC_DEAL_FIELD: "Id", "GuarantorName": "Guarantor"})
            )
            df_term_raw = df_term_raw.merge(guarantor_map, on="Id", how="left")
        else:
            df_term_raw["Guarantor"] = ""
    else:
        df_term_raw["Guarantor"] = ""

    df_term = pd.DataFrame()
    if not df_term_raw.empty:
        df_term["Portfolio"] = df_term_raw.apply(
            lambda row: deal_record_type_display(row, "Term"), axis=1
        )
        df_term["Loan ID"] = df_term_raw["Deal_Loan_Number__c"].apply(loan_id_5)
        df_term["Loan"] = df_term_raw.get("Name", "")
        df_term["Account Name"] = df_term_raw.get("Account_Name__c", "")
        df_term["Guarantor"] = df_term_raw.get("Guarantor", "").fillna("")
        df_term["Origination Date"] = df_term_raw.get("OriginationDate_dt")
        df_term["Maturity Date"] = df_term_raw.get("Maturity_dt")
        df_term["Next Payment Date"] = df_term_raw.get("NextPay_dt")
        df_term["Loan Amount Num"] = pd.to_numeric(df_term_raw.get("Amount"), errors="coerce")
        df_term["Outstanding Balance Num"] = pd.to_numeric(
            df_term_raw.get("Current_UPB__c"), errors="coerce"
        )
        df_term["LTV Dec"] = df_term_raw.get("LTV_Dec")
        df_term["Rate Dec"] = df_term_raw.get("Rate_Dec")
        df_term["State(s)"] = df_term_raw.get("State_Percentages__c").apply(extract_states_only)
        df_term["Total Properties Num"] = (
            pd.to_numeric(df_term_raw.get("Total_Properties__c"), errors="coerce")
            .round(0)
            .astype("Int64")
        )
        df_term["Total Units Num"] = (
            pd.to_numeric(df_term_raw.get("Total_Units__c"), errors="coerce")
            .round(0)
            .astype("Int64")
        )
        df_term["Recourse"] = df_term_raw.get("Recourse__c")
        df_term["Historical Ontime % Dec"] = df_term_raw.get(
            "Historical_Ontime_Payments_Percentage__c"
        ).apply(pct_to_dec)
        df_term = df_term.sort_values(
            ["Origination Date", "Loan ID"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)

    df_bridge = pd.DataFrame()
    bridge_fci_rows = pd.DataFrame()
    if not df_bridge_opp.empty:
        deal_ids = df_bridge_opp["Id"].dropna().astype(str).unique().tolist()
        prop_servicer_roll = query_property_servicer_ids_for_deals(sf, deal_ids)
        prop_servicer_map = {}
        if not prop_servicer_roll.empty:
            prop_servicer_map = dict(
                zip(prop_servicer_roll["Deal__c"], prop_servicer_roll["Property Servicer IDs"])
            )
        advance_fields = ["Id", "Deal__c", "Advance_Num__c", "LOC_Commitment__c", "Wire_Date__c"]
        advance_rows: list[dict[str, Any]] = []
        for group in chunked(deal_ids, 200):
            where_advance = f"Deal__c IN ({', '.join(soql_quote(item) for item in group)})"
            rows_adv, _, _ = try_query_drop_missing(
                sf,
                "Advance__c",
                advance_fields,
                where_advance,
                limit=2000,
                order_by="CreatedDate DESC",
            )
            advance_rows.extend(rows_adv)
        df_adv = pd.DataFrame(advance_rows).drop(columns=["attributes"], errors="ignore")

        if df_adv.empty:
            df_bridge["Opportunity Id"] = df_bridge_opp["Id"]
            df_bridge["Deal Loan Number Raw"] = df_bridge_opp["Deal_Loan_Number__c"]
            df_bridge["Servicer Commitment ID"] = df_bridge_opp.get("Servicer_Commitment_Id__c", "")
            df_bridge["Property Servicer IDs"] = (
                df_bridge["Opportunity Id"].map(prop_servicer_map).fillna("")
            )
            df_bridge["Portfolio"] = df_bridge_opp.apply(
                lambda row: deal_record_type_display(row, "Bridge"), axis=1
            )
            df_bridge["Loan ID"] = df_bridge_opp["Deal_Loan_Number__c"].apply(loan_id_5)
            df_bridge["Loan"] = df_bridge_opp.get("Name", "")
            df_bridge["Account Name"] = df_bridge_opp.get("Account_Name__c", "")
            df_bridge["Commitment Amount Num"] = None
            df_bridge["Origination Date"] = df_bridge_opp.get("OriginationDate_dt")
            df_bridge["Maturity Date"] = df_bridge_opp.get("Maturity_dt")
            df_bridge["Rate Dec"] = df_bridge_opp.get("Rate_Dec")
            df_bridge["LTV Dec"] = df_bridge_opp.get("LTV_Dec")
            df_bridge["Advances Num"] = None
            df_bridge["Total Properties Num"] = None
            df_bridge["State(s)"] = df_bridge_opp.get("State_Percentages__c").apply(extract_states_only)
            df_bridge["Lifetime Funded Num"] = pd.to_numeric(
                df_bridge_opp.get("Aggregate_Funding__c"), errors="coerce"
            )
            df_bridge["Paid Off Assets Num"] = None
            df_bridge["Active Assets Num"] = None
            df_bridge["Outstanding Balance Num"] = pd.to_numeric(
                df_bridge_opp.get("Current_UPB__c"), errors="coerce"
            )
            df_bridge["As-Is/ ARV Num"] = None
            df_bridge["Avg Hold Time Num"] = None
            df_bridge["Avg Disposed Time Num"] = None
        else:
            df_adv["Advance_Num__c"] = pd.to_numeric(df_adv.get("Advance_Num__c"), errors="coerce")
            df_adv["LOC_Commitment__c"] = pd.to_numeric(
                df_adv.get("LOC_Commitment__c"), errors="coerce"
            )
            df_adv["Wire_Date__c_dt"] = pd.to_datetime(df_adv.get("Wire_Date__c"), errors="coerce")

            advance_roll = (
                df_adv.groupby("Deal__c", dropna=False)
                .agg(Commitment=("LOC_Commitment__c", "max"), Advances=("Advance_Num__c", "max"))
                .reset_index()
            )

            property_fields = [
                "Id",
                "Advance__c",
                "Deal__c",
                "Payoff_Received_Date__c",
                "After_Repair_Value__c",
                "Appraised_Value_Amount__c",
            ]
            property_rows: list[dict[str, Any]] = []
            advance_ids = df_adv["Id"].dropna().astype(str).unique().tolist()
            for group in chunked(advance_ids, 200):
                where_property = f"Advance__c IN ({', '.join(soql_quote(item) for item in group)})"
                rows_prop, _, _ = try_query_drop_missing(
                    sf,
                    "Property__c",
                    property_fields,
                    where_property,
                    limit=2000,
                    order_by="CreatedDate DESC",
                )
                property_rows.extend(rows_prop)
            df_prop = pd.DataFrame(property_rows).drop(columns=["attributes"], errors="ignore")
            today_dt = pd.to_datetime(date.today())

            if df_prop.empty:
                property_metrics = pd.DataFrame(
                    columns=[
                        "Deal__c",
                        "Total_Assets",
                        "Paid_Off",
                        "Active",
                        "AsIs_ARV",
                        "Avg_Hold",
                        "Avg_Disposed",
                    ]
                )
            else:
                df_prop["Payoff_dt"] = pd.to_datetime(
                    df_prop.get("Payoff_Received_Date__c"), errors="coerce"
                )
                df_prop["ARV_num"] = pd.to_numeric(
                    df_prop.get("After_Repair_Value__c"), errors="coerce"
                )
                missing_arv = df_prop["ARV_num"].isna()
                df_prop.loc[missing_arv, "ARV_num"] = pd.to_numeric(
                    df_prop.get("Appraised_Value_Amount__c"), errors="coerce"
                )
                df_prop = df_prop.merge(
                    df_adv[["Id", "Deal__c", "Wire_Date__c_dt"]],
                    left_on="Advance__c",
                    right_on="Id",
                    how="left",
                    suffixes=("", "_adv"),
                )
                property_id_col = "Id_x" if "Id_x" in df_prop.columns else "Id"

                earliest_wire = (
                    df_prop.groupby(["Deal__c", property_id_col], dropna=False)["Wire_Date__c_dt"]
                    .min()
                    .reset_index()
                    .rename(columns={property_id_col: "PropertyId", "Wire_Date__c_dt": "EarliestWire"})
                )
                payoff_per_asset = (
                    df_prop.groupby(["Deal__c", property_id_col], dropna=False)["Payoff_dt"]
                    .min()
                    .reset_index()
                    .rename(columns={property_id_col: "PropertyId", "Payoff_dt": "Payoff"})
                )
                arv_per_asset = (
                    df_prop.groupby(["Deal__c", property_id_col], dropna=False)["ARV_num"]
                    .max()
                    .reset_index()
                    .rename(columns={property_id_col: "PropertyId", "ARV_num": "ARV"})
                )
                assets = earliest_wire.merge(
                    payoff_per_asset,
                    on=["Deal__c", "PropertyId"],
                    how="left",
                ).merge(
                    arv_per_asset,
                    on=["Deal__c", "PropertyId"],
                    how="left",
                )
                assets["Is_Active"] = assets["Payoff"].isna()
                assets["Is_PaidOff"] = assets["Payoff"].notna()
                assets["Hold_Days"] = (today_dt - assets["EarliestWire"]).dt.days
                assets.loc[assets["EarliestWire"].isna(), "Hold_Days"] = pd.NA
                assets["Disposed_Days"] = (assets["Payoff"] - assets["EarliestWire"]).dt.days
                assets.loc[
                    assets["Payoff"].isna() | assets["EarliestWire"].isna(),
                    "Disposed_Days",
                ] = pd.NA
                base = (
                    assets.groupby("Deal__c", dropna=False)
                    .agg(
                        Total_Assets=("PropertyId", "nunique"),
                        Paid_Off=("Is_PaidOff", "sum"),
                        Active=("Is_Active", "sum"),
                        AsIs_ARV=("ARV", "sum"),
                    )
                    .reset_index()
                )
                hold = (
                    assets[assets["Is_Active"]]
                    .groupby("Deal__c", dropna=False)
                    .agg(Avg_Hold=("Hold_Days", "mean"))
                    .reset_index()
                )
                disposed = (
                    assets[assets["Is_PaidOff"]]
                    .groupby("Deal__c", dropna=False)
                    .agg(Avg_Disposed=("Disposed_Days", "mean"))
                    .reset_index()
                )
                property_metrics = base.merge(hold, on="Deal__c", how="left").merge(
                    disposed,
                    on="Deal__c",
                    how="left",
                )

            bridge_base = df_bridge_opp.copy()
            bridge_base = bridge_base.merge(
                advance_roll,
                left_on="Id",
                right_on="Deal__c",
                how="left",
            ).drop(columns=["Deal__c"], errors="ignore")
            bridge_base = bridge_base.merge(
                property_metrics,
                left_on="Id",
                right_on="Deal__c",
                how="left",
            ).drop(columns=["Deal__c"], errors="ignore")

            df_bridge["Opportunity Id"] = bridge_base["Id"]
            df_bridge["Deal Loan Number Raw"] = bridge_base["Deal_Loan_Number__c"]
            df_bridge["Servicer Commitment ID"] = bridge_base.get("Servicer_Commitment_Id__c", "")
            df_bridge["Property Servicer IDs"] = (
                df_bridge["Opportunity Id"].map(prop_servicer_map).fillna("")
            )
            df_bridge["Portfolio"] = bridge_base.apply(
                lambda row: deal_record_type_display(row, "Bridge"), axis=1
            )
            df_bridge["Loan ID"] = bridge_base["Deal_Loan_Number__c"].apply(loan_id_5)
            df_bridge["Loan"] = bridge_base.get("Name", "")
            df_bridge["Account Name"] = bridge_base.get("Account_Name__c", "")
            df_bridge["Commitment Amount Num"] = pd.to_numeric(
                bridge_base.get("Commitment"), errors="coerce"
            )
            df_bridge["Origination Date"] = bridge_base.get("OriginationDate_dt")
            df_bridge["Maturity Date"] = bridge_base.get("Maturity_dt")
            df_bridge["Rate Dec"] = bridge_base.get("Rate_Dec")
            df_bridge["LTV Dec"] = bridge_base.get("LTV_Dec")
            df_bridge["Advances Num"] = (
                pd.to_numeric(bridge_base.get("Advances"), errors="coerce").round(0).astype("Int64")
            )
            df_bridge["Total Properties Num"] = (
                pd.to_numeric(bridge_base.get("Total_Assets"), errors="coerce")
                .round(0)
                .astype("Int64")
            )
            df_bridge["State(s)"] = bridge_base.get("State_Percentages__c").apply(extract_states_only)
            df_bridge["Lifetime Funded Num"] = pd.to_numeric(
                bridge_base.get("Aggregate_Funding__c"), errors="coerce"
            )
            df_bridge["Paid Off Assets Num"] = (
                pd.to_numeric(bridge_base.get("Paid_Off"), errors="coerce").round(0).astype("Int64")
            )
            df_bridge["Active Assets Num"] = (
                pd.to_numeric(bridge_base.get("Active"), errors="coerce").round(0).astype("Int64")
            )
            df_bridge["Outstanding Balance Num"] = pd.to_numeric(
                bridge_base.get("Current_UPB__c"), errors="coerce"
            )
            df_bridge["As-Is/ ARV Num"] = pd.to_numeric(
                bridge_base.get("AsIs_ARV"), errors="coerce"
            )
            df_bridge["Avg Hold Time Num"] = (
                pd.to_numeric(bridge_base.get("Avg_Hold"), errors="coerce").round(0).astype("Int64")
            )
            df_bridge["Avg Disposed Time Num"] = (
                pd.to_numeric(bridge_base.get("Avg_Disposed"), errors="coerce")
                .round(0)
                .astype("Int64")
            )

        df_bridge = df_bridge.sort_values(
            ["Origination Date", "Loan ID"],
            ascending=[False, True],
            kind="stable",
        ).reset_index(drop=True)
        bridge_fci_rows = df_bridge.copy()
        df_bridge = df_bridge.drop(columns=FCI_INTERNAL_BRIDGE_COLUMNS, errors="ignore")

    return df_term, df_bridge, bridge_fci_rows


# -------------------------
# FCI Bridge late-payment / late-charge helpers
# -------------------------
def load_fci_token() -> str | None:
    try:
        fci_section = dict(st.secrets.get("fci", {}))
    except Exception:
        fci_section = {}
    token = fci_section.get("token")
    if token:
        return str(token).strip()
    token = st.session_state.get("fci_token")
    return str(token).strip() if token else None



def split_candidate_ids(value):
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except Exception:
        pass
    parts = re.split(r"[,;|\n\r\t]+", str(value))
    out = []
    for part in parts:
        s = part.strip()
        if s and s.lower() not in {"nan", "none", "null", "n/a", "na", "-"}:
            out.append(s)
    return out



def unique_join(values):
    out = []
    seen = set()
    for v in values:
        for s in split_candidate_ids(v):
            if s not in seen:
                seen.add(s)
                out.append(s)
    return ", ".join(out)



def norm_match_key(x) -> str:
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none", "null", "n/a", "na", "-"}:
        return ""
    return re.sub(r"[\s\-]+", "", s).upper()



def to_float_or_none(x):
    if x in ("", None):
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        s = str(x).replace("$", "").replace(",", "").strip()
        if s.lower() in {"n/a", "na", "none", "null", ""}:
            return None
        return float(s)
    except Exception:
        return None



def parse_fci_date(x):
    if x in ("", None):
        return None
    s = str(x).strip()
    if not s or s.lower() in {"n/a", "na", "none", "null", "0"}:
        return None
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()



def is_nonzero_money(x, tolerance: float = 0.01) -> bool:
    v = to_float_or_none(x)
    return v is not None and abs(v) > tolerance



def fci_auth_headers(api_token: str) -> dict:
    token_text = str(api_token or "").strip()
    auth_value = token_text if token_text.lower().startswith("bearer ") else f"Bearer {token_text}"
    return {
        "Authorization": auth_value,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }



def fci_build_get_loan_information_query(loan_account, fields):
    fields_text = (chr(10) + "            ").join(fields)
    loan_account_literal = json.dumps(str(loan_account))
    return f"""
{{
  getLoanInformation
    (
        loanaccount:{loan_account_literal},
        offset:0,
        orderby: "LoanAccount",
        order: "asc"
    )
        {{
            {fields_text}
        }}
}}
"""



def fci_post_graphql(query: str, api_token=None):
    if not api_token:
        raise RuntimeError("Missing FCI token.")

    resp = requests.post(
        FCI_URL,
        headers=fci_auth_headers(api_token),
        json={"query": query, "variables": {}},
        timeout=FCI_TIMEOUT_SECONDS,
    )

    preview = (resp.text or "")[:500]
    try:
        body = resp.json()
    except Exception:
        raise RuntimeError(f"FCI API returned non-JSON response. HTTP {resp.status_code}. Preview: {preview}")

    if resp.status_code >= 400:
        raise RuntimeError(f"FCI API HTTP {resp.status_code}. Preview: {preview}")

    return body



def fci_get_loan_information_by_loanaccount(loan_account, api_token=None):
    """Query FCI by documented loanaccount argument. Deal Loan Number is not used."""
    fields = list(FCI_LOAN_FIELDS)
    last_error = None

    for _ in range(8):
        query = fci_build_get_loan_information_query(loan_account, fields)
        body = fci_post_graphql(query, api_token=api_token)
        errors = body.get("errors") or []

        if not errors:
            rows = (body.get("data") or {}).get("getLoanInformation") or []
            if isinstance(rows, dict):
                rows = [rows]
            return rows

        msg = " | ".join(str(e.get("message", e)) for e in errors)
        last_error = msg

        bad_fields = set(re.findall(r'Cannot query field "([^"]+)"', msg))
        if bad_fields:
            new_fields = [f for f in fields if f not in bad_fields]
            if len(new_fields) != len(fields):
                fields = new_fields
                continue

        raise RuntimeError(f"FCI GraphQL error for loanAccount {loan_account}: {msg}")

    raise RuntimeError(f"FCI GraphQL error for loanAccount {loan_account}: {last_error}")



def query_property_servicer_ids_for_deals(sf: Salesforce, deal_ids):
    """FCI-only lookup. Does not change the original Bridge rollup logic."""
    if not deal_ids:
        return pd.DataFrame(columns=["Deal__c", "Property Servicer IDs"])

    fields = ["Id", "Deal__c", "Servicer_Id__c"]
    rows_all = []
    for group in chunked(deal_ids, 200):
        where_prop = f"Deal__c IN ({', '.join(soql_quote(item) for item in group)})"
        rows, _, _ = try_query_drop_missing(
            sf, "Property__c", fields, where_prop, limit=2000, order_by="CreatedDate DESC"
        )
        rows_all.extend(rows)

    df = pd.DataFrame(rows_all).drop(columns=["attributes"], errors="ignore")
    if df.empty or "Deal__c" not in df.columns or "Servicer_Id__c" not in df.columns:
        return pd.DataFrame(columns=["Deal__c", "Property Servicer IDs"])

    df["Servicer_Id__c"] = df["Servicer_Id__c"].fillna("").astype(str).str.strip()
    df = df[df["Servicer_Id__c"] != ""].copy()
    if df.empty:
        return pd.DataFrame(columns=["Deal__c", "Property Servicer IDs"])

    return (
        df.groupby("Deal__c", dropna=False)["Servicer_Id__c"]
        .apply(lambda s: unique_join(s.tolist()))
        .reset_index()
        .rename(columns={"Servicer_Id__c": "Property Servicer IDs"})
    )



def bridge_row_fci_candidates(row):
    candidates = []
    for col_name in ["Servicer Commitment ID", "Property Servicer IDs"]:
        if col_name in row.index:
            candidates.extend(split_candidate_ids(row.get(col_name)))

    out = []
    seen = set()
    for candidate in candidates:
        key = norm_match_key(candidate)
        if key and key not in seen:
            seen.add(key)
            out.append(candidate)
    return out



def fci_add_alert(alerts, base, severity, issue, field="", fci_value=None, salesforce_value=None, days_past_due=None):
    alerts.append({
        "Severity": severity,
        "Loan": base.get("Loan"),
        "Loan ID": base.get("Loan ID"),
        "Opportunity Id": base.get("Opportunity Id"),
        "Deal Loan Number Raw": base.get("Deal Loan Number Raw"),
        "Servicer Commitment ID": base.get("Servicer Commitment ID"),
        "Property Servicer IDs": base.get("Property Servicer IDs"),
        "Attempted FCI LoanAccount IDs": base.get("Attempted FCI LoanAccount IDs"),
        "FCI Loan Account": base.get("FCI Loan Account"),
        "FCI Match Key": base.get("FCI Match Key"),
        "Issue": issue,
        "Field": field,
        "FCI Value": fci_value,
        "Salesforce Value": salesforce_value,
        "Days Past Due": days_past_due,
        "FCI Status": base.get("FCI Status"),
        "FCI Status Lender": base.get("FCI Status Lender"),
        "FCI Principal Balance": base.get("FCI Principal Balance"),
        "FCI Next Due Date": base.get("FCI Next Due Date"),
        "FCI Paid To Date": base.get("FCI Paid To Date"),
    })



def evaluate_fci_record_for_alerts(match_row: dict):
    """Checks only: (1) late payments, (2) late charge balances."""
    rec = match_row.get("FCI Record") or {}
    alerts = []

    fci_principal = to_float_or_none(rec.get("principalBalance"))
    fci_next_due = parse_fci_date(rec.get("nextDueDate"))
    fci_paid_to = parse_fci_date(rec.get("paidToDate"))
    today = date.today()

    status = str(rec.get("status") or "").strip()
    status_lender = str(rec.get("statusLender") or "").strip()
    status_text = f"{status} {status_lender}".strip().lower()

    base = dict(match_row)
    base.update({
        "FCI Status": status,
        "FCI Status Lender": status_lender,
        "FCI Principal Balance": fci_principal,
        "FCI Next Due Date": fci_next_due,
        "FCI Paid To Date": fci_paid_to,
    })

    if fci_principal is not None and fci_principal > 0:
        if fci_next_due is not None and fci_next_due < today:
            days = (today - fci_next_due).days
            severity = "Critical" if days >= 30 else "Warning"
            fci_add_alert(
                alerts,
                base,
                severity,
                f"Late payment: FCI next due date is {days} day(s) past due.",
                "nextDueDate",
                fci_next_due,
                days_past_due=days,
            )

    late_status_tokens = [
        "delinquent",
        "late",
        "past due",
        "default",
        "non-performing",
        "non performing",
    ]
    for token_text in late_status_tokens:
        if token_text in status_text:
            fci_add_alert(
                alerts,
                base,
                "Warning",
                f"Late-payment status indicator: FCI status contains '{token_text}'.",
                "status/statusLender",
                f"{status} / {status_lender}",
            )
            break

    late_charge_fields = [
        ("unpaidLateCharges", "Unpaid late charges are non-zero."),
        ("deferredLateCharges", "Deferred late charges are non-zero."),
        ("poffAcurredLateCharges", "Payoff accrued late charges are non-zero."),
        ("poffUnpaidLateCharges", "Payoff unpaid late charges are non-zero."),
        ("poffPaidLateCharges", "Payoff paid late charges are non-zero."),
    ]
    for field_name, issue in late_charge_fields:
        val = rec.get(field_name)
        if is_nonzero_money(val):
            fci_add_alert(alerts, base, "Warning", issue, field_name, val)

    return alerts



def check_bridge_loans_against_fci(bridge_fci_rows: pd.DataFrame, api_token=None):
    """Return (matches_df, alerts_df). Matching is only by FCI loanAccount using
    Salesforce Servicer Commitment ID and Property Servicer ID."""
    if bridge_fci_rows is None or bridge_fci_rows.empty:
        return pd.DataFrame(), pd.DataFrame()

    if not api_token:
        return pd.DataFrame(), pd.DataFrame()

    rows = bridge_fci_rows.reset_index(drop=True).copy()
    rows["_fci_candidates"] = rows.apply(bridge_row_fci_candidates, axis=1)
    rows["Attempted FCI LoanAccount IDs"] = rows["_fci_candidates"].apply(lambda vals: ", ".join(vals))

    all_candidates = []
    seen = set()
    for vals in rows["_fci_candidates"].tolist():
        for candidate in vals:
            key = norm_match_key(candidate)
            if key and key not in seen:
                seen.add(key)
                all_candidates.append(candidate)

    candidate_records = {}
    lookup_errors = []
    for candidate in all_candidates:
        try:
            candidate_records[norm_match_key(candidate)] = fci_get_loan_information_by_loanaccount(
                candidate, api_token=api_token
            )
        except Exception as exc:
            candidate_records[norm_match_key(candidate)] = []
            lookup_errors.append(f"{candidate}: {exc}")

    matched_rows = []
    alerts = []

    for _, row in rows.iterrows():
        candidates = row.get("_fci_candidates", []) or []
        matches = []
        for candidate in candidates:
            key = norm_match_key(candidate)
            for rec in candidate_records.get(key, []) or []:
                matches.append((candidate, rec))

        deduped = []
        seen_match = set()
        for candidate, rec in matches:
            rec_key = norm_match_key(rec.get("loanAccount")) or f"{candidate}-{id(rec)}"
            if rec_key in seen_match:
                continue
            seen_match.add(rec_key)
            deduped.append((candidate, rec))

        if not deduped:
            continue

        for candidate, rec in deduped:
            match_row = {
                "Loan": row.get("Loan"),
                "Loan ID": row.get("Loan ID"),
                "Opportunity Id": row.get("Opportunity Id"),
                "Deal Loan Number Raw": row.get("Deal Loan Number Raw"),
                "Servicer Commitment ID": row.get("Servicer Commitment ID"),
                "Property Servicer IDs": row.get("Property Servicer IDs"),
                "Attempted FCI LoanAccount IDs": row.get("Attempted FCI LoanAccount IDs"),
                "Outstanding Balance Num": row.get("Outstanding Balance Num"),
                "FCI Loan Account": rec.get("loanAccount"),
                "FCI Match Key": candidate,
                "FCI Record": rec,
            }
            matched_rows.append(match_row)
            alerts.extend(evaluate_fci_record_for_alerts(match_row))

    matches_preview = []
    for m in matched_rows:
        rec = m.get("FCI Record") or {}
        matches_preview.append({
            "Loan": m.get("Loan"),
            "Loan ID": m.get("Loan ID"),
            "Opportunity Id": m.get("Opportunity Id"),
            "Deal Loan Number Raw": m.get("Deal Loan Number Raw"),
            "Servicer Commitment ID": m.get("Servicer Commitment ID"),
            "Property Servicer IDs": m.get("Property Servicer IDs"),
            "Attempted FCI LoanAccount IDs": m.get("Attempted FCI LoanAccount IDs"),
            "FCI Loan Account": rec.get("loanAccount"),
            "FCI Match Key": m.get("FCI Match Key"),
            "FCI Status": rec.get("status"),
            "FCI Status Lender": rec.get("statusLender"),
            "FCI Principal Balance": to_float_or_none(rec.get("principalBalance")),
            "FCI Next Due Date": parse_fci_date(rec.get("nextDueDate")),
            "FCI Paid To Date": parse_fci_date(rec.get("paidToDate")),
            "FCI Paid Off Date": parse_fci_date(rec.get("paidOffDate")),
            "FCI Late Charge Days": to_float_or_none(rec.get("lateChargesDays")),
            "FCI Late Charge Pct": to_float_or_none(rec.get("lateChargesPct")),
            "FCI Unpaid Late Charges": to_float_or_none(rec.get("unpaidLateCharges")),
            "FCI Deferred Late Charges": to_float_or_none(rec.get("deferredLateCharges")),
            "FCI Payoff Accrued Late Charges": to_float_or_none(rec.get("poffAcurredLateCharges")),
            "FCI Payoff Unpaid Late Charges": to_float_or_none(rec.get("poffUnpaidLateCharges")),
            "FCI Payoff Paid Late Charges": to_float_or_none(rec.get("poffPaidLateCharges")),
        })

    matches_df = pd.DataFrame(matches_preview)
    alerts_df = pd.DataFrame(alerts)

    severity_order = {"Critical": 0, "Warning": 1, "Info": 2}
    if not alerts_df.empty:
        alerts_df["_severity_sort"] = alerts_df["Severity"].map(severity_order).fillna(9)
        alerts_df = (
            alerts_df.sort_values(["_severity_sort", "Loan", "FCI Loan Account", "Issue"], kind="stable")
            .drop(columns=["_severity_sort"], errors="ignore")
            .reset_index(drop=True)
        )

    if lookup_errors:
        st.session_state["fci_lookup_errors"] = lookup_errors
    else:
        st.session_state.pop("fci_lookup_errors", None)

    return matches_df, alerts_df



def write_fci_alerts_sheet(wb, alerts_df: pd.DataFrame, matches_df: pd.DataFrame | None = None):
    if FCI_ALERT_SHEET in wb.sheetnames:
        del wb[FCI_ALERT_SHEET]

    ws = wb.create_sheet(FCI_ALERT_SHEET)
    ws.sheet_view.showGridLines = False

    title_fill = PatternFill("solid", fgColor="1F4E78")
    header_fill = PatternFill("solid", fgColor="D9EAF7")
    critical_fill = PatternFill("solid", fgColor="F4CCCC")
    warning_fill = PatternFill("solid", fgColor="FFF2CC")
    info_fill = PatternFill("solid", fgColor="D9EAD3")

    ws["A1"] = "FCI Bridge Late Payment / Late Charge Alerts"
    ws["A1"].font = Font(bold=True, color="FFFFFF", size=14)
    ws["A1"].fill = title_fill
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=8)

    ws["A2"] = f"Run Date: {datetime.now().strftime('%m/%d/%Y %I:%M %p')}"
    ws["A2"].font = Font(italic=True)

    if alerts_df is None or alerts_df.empty:
        ws["A4"] = "No late payment or late charge items found for the matched FCI loans."
        ws["A4"].font = Font(bold=True)
        start_match_row = 7
    else:
        cols = [
            "Severity",
            "Loan",
            "Loan ID",
            "Deal Loan Number Raw",
            "Servicer Commitment ID",
            "Property Servicer IDs",
            "Attempted FCI LoanAccount IDs",
            "FCI Loan Account",
            "FCI Match Key",
            "Issue",
            "Field",
            "FCI Value",
            "Salesforce Value",
            "Days Past Due",
            "FCI Status",
            "FCI Status Lender",
            "FCI Principal Balance",
            "FCI Next Due Date",
            "FCI Paid To Date",
        ]
        cols = [c for c in cols if c in alerts_df.columns]

        start_row = 4
        for c_idx, col_name in enumerate(cols, start=1):
            cell = ws.cell(start_row, c_idx)
            cell.value = col_name
            cell.font = Font(bold=True)
            cell.fill = header_fill
            cell.alignment = Alignment(wrap_text=True, vertical="top")

        for r_idx, (_, row) in enumerate(alerts_df[cols].iterrows(), start=start_row + 1):
            sev = str(row.get("Severity", ""))
            row_fill = critical_fill if sev == "Critical" else warning_fill if sev == "Warning" else info_fill if sev == "Info" else None
            for c_idx, col_name in enumerate(cols, start=1):
                cell = ws.cell(r_idx, c_idx)
                val = row.get(col_name)
                if isinstance(val, (date, datetime)):
                    cell.value = val
                    cell.number_format = "m/d/yyyy"
                else:
                    cell.value = excel_safe(val)
                cell.alignment = Alignment(wrap_text=True, vertical="top")
                if row_fill and col_name == "Severity":
                    cell.fill = row_fill

        fill_blanks_with_dash(ws, start_row + 1, start_row + len(alerts_df), range(1, len(cols) + 1))

        start_match_row = start_row + len(alerts_df) + 4

    if matches_df is not None and not matches_df.empty:
        ws.cell(start_match_row, 1).value = "FCI Matched Records"
        ws.cell(start_match_row, 1).font = Font(bold=True, size=12)

        match_cols = [
            "Loan",
            "Loan ID",
            "Deal Loan Number Raw",
            "Servicer Commitment ID",
            "Property Servicer IDs",
            "Attempted FCI LoanAccount IDs",
            "FCI Loan Account",
            "FCI Match Key",
            "FCI Status",
            "FCI Status Lender",
            "FCI Principal Balance",
            "FCI Next Due Date",
            "FCI Paid To Date",
            "FCI Paid Off Date",
            "FCI Late Charge Days",
            "FCI Late Charge Pct",
            "FCI Unpaid Late Charges",
            "FCI Deferred Late Charges",
            "FCI Payoff Accrued Late Charges",
            "FCI Payoff Unpaid Late Charges",
            "FCI Payoff Paid Late Charges",
        ]
        match_cols = [c for c in match_cols if c in matches_df.columns]

        header_row = start_match_row + 1
        for c_idx, col_name in enumerate(match_cols, start=1):
            cell = ws.cell(header_row, c_idx)
            cell.value = col_name
            cell.font = Font(bold=True)
            cell.fill = header_fill
            cell.alignment = Alignment(wrap_text=True, vertical="top")

        for r_idx, (_, row) in enumerate(matches_df[match_cols].iterrows(), start=header_row + 1):
            for c_idx, col_name in enumerate(match_cols, start=1):
                cell = ws.cell(r_idx, c_idx)
                val = row.get(col_name)
                if isinstance(val, (date, datetime)):
                    cell.value = val
                    cell.number_format = "m/d/yyyy"
                else:
                    cell.value = excel_safe(val)
                cell.alignment = Alignment(wrap_text=True, vertical="top")

        fill_blanks_with_dash(
            ws, header_row + 1, header_row + len(matches_df), range(1, len(match_cols) + 1)
        )

    width_by_col = {
        "A": 14, "B": 28, "C": 12, "D": 18, "E": 22, "F": 30, "G": 50,
        "H": 18, "I": 18, "J": 60, "K": 22, "L": 24, "M": 30, "N": 16,
        "O": 18, "P": 20, "Q": 18, "R": 18, "S": 18, "T": 18, "U": 18,
    }
    for col_letter, width in width_by_col.items():
        ws.column_dimensions[col_letter].width = width

    ws.freeze_panes = "A5"


# -------------------------
# Occupancy helpers (template-driven, matches the notebook)
# -------------------------
_OCC_RX_1 = re.compile(r"^\s*(?P<year>\d{4})\s*[Qq]\s*(?P<quarter>[1-4])\s*Occ\s*%?\s*$")
_OCC_RX_2 = re.compile(r"^\s*[Qq]\s*(?P<quarter>[1-4])\s*(?P<year>\d{4})\s*Occ\s*%?\s*$")


def parse_occ_header(value: Any):
    if value is None or str(value).strip() == "":
        return None
    txt = re.sub(r"\s+", " ", str(value).strip())
    for rx in (_OCC_RX_1, _OCC_RX_2):
        m = rx.match(txt)
        if m:
            year = int(m.group("year"))
            quarter = int(m.group("quarter"))
            return {
                "header": txt,
                "quarter_key": f"{year} Q{quarter}",
                "year": year,
                "quarter": quarter,
            }
    return None



def get_term_occupancy_headers(ws):
    header_row, _ = find_header_row_and_map(ws, must_have=("portfolio", "loan id"))
    items = []
    for c in range(1, ws.max_column + 1):
        parsed = parse_occ_header(ws.cell(header_row, c).value)
        if parsed:
            parsed["col"] = c
            items.append(parsed)
    return items



def read_template_term_occ_headers(template_bytes: bytes):
    workbook = load_workbook(io.BytesIO(template_bytes), read_only=True, data_only=False)
    try:
        if TERM_SHEET in workbook.sheetnames:
            return get_term_occupancy_headers(workbook[TERM_SHEET])
        return []
    finally:
        workbook.close()



def find_sheet_name_case_insensitive(sheet_names, target_name: str):
    target_norm = norm_hdr(target_name)
    for s in sheet_names:
        if norm_hdr(s) == target_norm:
            return s
    for s in sheet_names:
        if target_norm in norm_hdr(s):
            return s
    return None



def detect_header_row_in_uploaded_sheet(ws, required_headers, scan_rows: int = 20, scan_cols: int = 80):
    required = {norm_hdr(x) for x in required_headers}
    if scan_rows > 0 and scan_cols > 0:
        rows = ws.iter_rows(
            min_row=1,
            max_row=min(ws.max_row or scan_rows, scan_rows),
            max_col=min(ws.max_column or scan_cols, scan_cols),
            values_only=True,
        )
        for r, values in enumerate(rows, start=1):
            if required.issubset({norm_hdr(value) for value in values}):
                return r
    raise ValueError(
        f"Could not find a header row on '{ws.title}' containing {sorted(required_headers)} in the first {scan_rows} rows."
    )



def load_financial_analysis_df_from_bytes(berkadia_bytes: bytes) -> pd.DataFrame:
    with pd.ExcelFile(io.BytesIO(berkadia_bytes), engine="openpyxl") as xls:
        sheet_name = find_sheet_name_case_insensitive(xls.sheet_names, "Financial Analysis")
        if not sheet_name:
            raise ValueError(
                f"Could not find a sheet named 'Financial Analysis' (case-insensitive). Sheets found: {xls.sheet_names}"
            )
        header_row = detect_header_row_in_uploaded_sheet(
            xls.book[sheet_name],
            required_headers=["Investor Loan#", "Freq of Analysis", "Period End Date", "Occupancy %"],
        )
        df = xls.parse(sheet_name=sheet_name, header=header_row - 1)
    df = df.loc[:, ~df.columns.astype(str).str.startswith("Unnamed:")].copy()
    return df



def build_term_occupancy_lookup(berkadia_bytes: bytes, target_quarter_keys=None):
    fa = load_financial_analysis_df_from_bytes(berkadia_bytes)

    required_cols = ["Investor Loan#", "Freq of Analysis", "Period End Date", "Occupancy %"]
    missing_cols = [c for c in required_cols if c not in fa.columns]
    if missing_cols:
        raise ValueError(f"Financial Analysis sheet is missing required columns: {missing_cols}")

    fa = fa.copy()
    fa["Loan ID 5"] = fa["Investor Loan#"].apply(loan_id_5)
    fa = fa[fa["Loan ID 5"] != ""].copy()

    fa["Period End Date_dt"] = pd.to_datetime(fa["Period End Date"], errors="coerce")
    fa = fa[fa["Period End Date_dt"].notna()].copy()

    fa["Quarter Key"] = (
        fa["Period End Date_dt"].dt.year.astype(int).astype(str)
        + " Q"
        + fa["Period End Date_dt"].dt.quarter.astype(int).astype(str)
    )

    if target_quarter_keys:
        target_quarter_keys = list(dict.fromkeys(target_quarter_keys))
        fa = fa[fa["Quarter Key"].isin(target_quarter_keys)].copy()

    fa["Occupancy Dec"] = fa["Occupancy %"].apply(pct_to_dec)
    fa["Occupancy Date_dt"] = pd.to_datetime(fa.get("Occupancy Date"), errors="coerce")
    fa["Freq Norm"] = fa["Freq of Analysis"].fillna("").astype(str).str.strip().str.upper()
    if "Consolidated?" in fa.columns:
        fa["Consolidated Norm"] = fa["Consolidated?"].fillna("").astype(str).str.strip().str.upper()
    else:
        fa["Consolidated Norm"] = ""
    fa["Months Num"] = pd.to_numeric(fa.get("# of Months"), errors="coerce")
    fa["Expected Freq"] = fa["Period End Date_dt"].dt.quarter.map(EXPECTED_FREQ_BY_QTR)
    fa["Expected Months"] = fa["Period End Date_dt"].dt.quarter.map(EXPECTED_MONTHS_BY_QTR)
    fa["Is Canonical Freq"] = fa["Freq Norm"].isin(CANONICAL_FREQS).astype(int)
    fa["Freq Match"] = (fa["Freq Norm"] == fa["Expected Freq"]).astype(int)
    fa["Months Match"] = (fa["Months Num"] == fa["Expected Months"]).astype(int)
    fa["Has Occupancy"] = fa["Occupancy Dec"].notna().astype(int)

    if "Prop Seq#" in fa.columns:
        fa["Prop Seq Key"] = fa["Prop Seq#"].astype(str)
    else:
        fa["Prop Seq Key"] = ""

    selected_rows = []
    issue_rows = []

    grp_cols = ["Loan ID 5", "Quarter Key"]
    for (loan5, quarter_key), grp_all in fa.groupby(grp_cols, dropna=False):
        grp = grp_all[grp_all["Has Occupancy"] == 1].copy()
        if grp.empty:
            continue

        grp_y = grp[grp["Consolidated Norm"] == "Y"].copy()
        if not grp_y.empty:
            pool = grp_y.copy()
            selection_source = "Consolidated"
        else:
            prop_count = (
                grp["Prop Seq Key"]
                .replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})
                .nunique(dropna=True)
            )
            if prop_count > 1:
                issue_rows.append({
                    "Loan ID 5": loan5,
                    "Quarter Key": quarter_key,
                    "Issue": "Skipped multi-property non-consolidated group (no safe way to aggregate occupancy).",
                })
                continue
            pool = grp.copy()
            selection_source = "Single-property non-consolidated"

        pool = pool.sort_values(
            by=[
                "Freq Match",
                "Months Match",
                "Is Canonical Freq",
                "Occupancy Date_dt",
                "Period End Date_dt",
            ],
            ascending=[False, False, False, False, False],
            na_position="last",
            kind="stable",
        )

        chosen = pool.iloc[0].copy()
        chosen["Selection Source"] = selection_source
        selected_rows.append(chosen)

        distinct_occ = pool["Occupancy Dec"].dropna().round(6).nunique()
        if distinct_occ > 1:
            options = pool[[
                "Freq Norm",
                "Months Num",
                "Occupancy Dec",
                "Occupancy Date_dt",
                "Investor Loan#",
            ]].copy()
            issue_rows.append({
                "Loan ID 5": loan5,
                "Quarter Key": quarter_key,
                "Issue": "Multiple occupancy values found; best-ranked row selected.",
                "Selected Freq": chosen.get("Freq Norm"),
                "Selected Months": chosen.get("Months Num"),
                "Selected Occupancy": chosen.get("Occupancy Dec"),
                "Candidate Rows": options.to_dict("records"),
            })

    selected_df = pd.DataFrame(selected_rows)
    issues_df = pd.DataFrame(issue_rows)

    if selected_df.empty:
        return {}, selected_df, issues_df, fa

    lookup = {
        (row["Loan ID 5"], row["Quarter Key"]): row["Occupancy Dec"]
        for _, row in selected_df.iterrows()
    }
    return lookup, selected_df, issues_df, fa


# -------------------------
# Midland occupancy (CAF extract bundle)
# -------------------------
def _caf_decode(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1", errors="replace")



def _caf_find_member(zip_names, token: str):
    """
    Returns the archive member whose base name carries the given extract token.

    Shortest name wins so a token never loses to a longer sibling that happens
    to contain it.
    """
    token = token.lower()
    matches = [
        name
        for name in zip_names
        if not name.endswith("/") and token in Path(name).name.lower()
    ]
    if not matches:
        return None
    return sorted(matches, key=lambda name: (len(Path(name).name), name))[0]



def parse_caf_extract(raw: bytes) -> pd.DataFrame:
    """
    Parses one pipe-delimited McCracken CAF extract into a DataFrame.

    Record types live in the first column: "A" is the file audit header, "C"
    holds the column names and "D" holds the data. Everything is read as text;
    callers coerce the columns they need.
    """
    reader = csv.reader(
        io.StringIO(_caf_decode(raw), newline=""),
        delimiter=CAF_DELIMITER,
        quotechar=CAF_QUOTECHAR,
    )

    columns = None
    records: list[list[str]] = []
    for record in reader:
        if not record:
            continue
        kind = str(record[0]).strip().strip(CAF_QUOTECHAR).upper()
        if kind == CAF_COLUMN_RECORD:
            columns = [str(name).strip() for name in record[1:]]
            continue
        if kind != CAF_DATA_RECORD or columns is None:
            continue
        values = record[1 : len(columns) + 1]
        if len(values) < len(columns):
            values = values + [""] * (len(columns) - len(values))
        records.append(values)

    if columns is None:
        raise ValueError(
            "This CAF extract has no 'C' column-name record, so its columns cannot be read."
        )
    return pd.DataFrame(records, columns=columns)



def read_caf_extract_from_zip(zf: zipfile.ZipFile, token: str, label: str, required: bool = True):
    member = _caf_find_member(zf.namelist(), token)
    if member is None:
        if not required:
            return None
        available = sorted(Path(name).name for name in zf.namelist() if not name.endswith("/"))
        raise ValueError(
            f"Could not find the {label} extract in the CAF .zip (looked for a file name containing "
            f"'{token}'). Files in the archive: {available}"
        )
    with zf.open(member) as handle:
        return parse_caf_extract(handle.read())



def _caf_resolve_loan_number_column(loans: pd.DataFrame) -> str:
    for candidate in CAF_LOAN_NUMBER_CANDIDATE_COLUMNS:
        if candidate in loans.columns:
            return candidate
    raise ValueError(
        "Could not find a loan-number column in the CAF Loan extract. Tried "
        f"{CAF_LOAN_NUMBER_CANDIDATE_COLUMNS}. Set CAF_LOAN_NUMBER_CANDIDATE_COLUMNS at the top of "
        f"amslide.py to one of the available columns: {sorted(loans.columns.tolist())}"
    )



def _caf_investor_loan_keys(alt_ids, known_loan_ids: set) -> pd.DataFrame:
    """
    Pulls {LnID -> loan_id_5(Investor Loan Number)} out of the AltAssetId extract.

    Only loans the Loan extract actually lists are kept, so a stale alt-id row
    cannot invent a loan.
    """
    empty = pd.DataFrame(columns=["_LnID", "_Loan ID 5"])
    if alt_ids is None or alt_ids.empty:
        return empty

    needed = {CAF_ALT_LOAN_COL, CAF_ALT_TYPE_COL, CAF_ALT_VALUE_COL}
    if not needed.issubset(set(alt_ids.columns)):
        return empty

    investor = alt_ids[
        alt_ids[CAF_ALT_TYPE_COL].astype(str).str.strip().str.upper() == CAF_ALT_INVESTOR_LOAN_TYPE
    ]
    if investor.empty:
        return empty

    keys = pd.DataFrame(
        {
            "_LnID": investor[CAF_ALT_LOAN_COL].astype(str).str.strip(),
            "_Loan ID 5": investor[CAF_ALT_VALUE_COL].apply(loan_id_5),
        }
    )
    keys = keys[(keys["_Loan ID 5"] != "") & keys["_LnID"].isin(known_loan_ids)]
    return keys.drop_duplicates()



def _caf_build_loan_key_map(loans: pd.DataFrame, alt_ids) -> pd.DataFrame:
    """
    Maps each CAF LnID to the 5-digit loan key that Salesforce is joined on.

    The Investor Loan Number from the AltAssetId extract is the number
    Salesforce carries, so it is used wherever a loan has one. The Loan
    extract's own number covers the remaining loans.
    """
    loan_number_col = _caf_resolve_loan_number_column(loans)
    if CAF_LOAN_ID_COL not in loans.columns:
        raise ValueError(
            f"The CAF Loan extract has no '{CAF_LOAN_ID_COL}' column, so collateral cannot be tied "
            f"to loans. Available columns: {sorted(loans.columns.tolist())}"
        )

    servicer_keys = pd.DataFrame(
        {
            "_LnID": loans[CAF_LOAN_ID_COL].astype(str).str.strip(),
            "_Loan ID 5": loans[loan_number_col].apply(loan_id_5),
        }
    )
    servicer_keys = servicer_keys[
        (servicer_keys["_LnID"] != "")
        & (servicer_keys["_Loan ID 5"] != "")
        & (
            servicer_keys["_LnID"].apply(lambda value: len(digits_only(value)))
            >= CAF_MIN_LOAN_NUMBER_DIGITS
        )
    ].drop_duplicates()

    if not CAF_PREFER_INVESTOR_LOAN_NUMBER:
        return servicer_keys

    known_loan_ids = set(servicer_keys["_LnID"])
    investor_keys = _caf_investor_loan_keys(alt_ids, known_loan_ids)
    if investor_keys.empty:
        # Falling back here would silently re-key every loan, not just the few
        # that have no investor number, and the servicer's own LnID lines up
        # with Salesforce for almost none of them. Stop instead of filling the
        # slide with occupancy attributed to the wrong loans.
        raise ValueError(
            "The CAF bundle has no usable Investor Loan Numbers, so its loans cannot be matched to "
            f"Salesforce. Expected rows with {CAF_ALT_TYPE_COL}='{CAF_ALT_INVESTOR_LOAN_TYPE}' in the "
            "AltAssetId extract. Upload the complete CAF .zip rather than a pruned copy. If this feed "
            "genuinely has no investor numbers, set CAF_PREFER_INVESTOR_LOAN_NUMBER = False at the top "
            "of amslide.py to key off the Loan extract instead."
        )

    # The Loan extract only covers loans with no Investor Loan Number, unless
    # servicer ids are explicitly allowed as an extra match.
    if CAF_ALSO_MATCH_ON_SERVICER_LOAN_ID:
        fallback = servicer_keys
    else:
        fallback = servicer_keys[~servicer_keys["_LnID"].isin(set(investor_keys["_LnID"]))]

    return pd.concat([investor_keys, fallback], ignore_index=True).drop_duplicates()



def _caf_parse_period_dates(series: pd.Series) -> pd.Series:
    exact = pd.to_datetime(series, errors="coerce", format=CAF_DATE_FORMAT)
    if exact.isna().any():
        loose = pd.to_datetime(series, errors="coerce")
        exact = exact.fillna(loose)
    return exact



def build_midland_occupancy_lookup(caf_zip_bundles, target_quarter_keys=None):
    """
    Builds {(loan_id_5, "YYYY Qn"): occupancy_decimal} from Midland CAF bundles.

    Accepts one bundle or a list of them. A CAF bundle carries only the latest
    rent roll for each property, so a single download fills whichever quarter
    that rent roll happens to fall in and leaves the rest blank. Pass one
    bundle per quarter wanted and their rent rolls are pooled before the
    rollup, which is what fills the template's older quarter columns.

    Occupancy is reported per collateral in the RentRoll extract. Collateral is
    tied to a loan through LoanToProperty, and the loan number comes from the
    Investor Loan Number in the AltAssetId extract, falling back to the Loan
    extract. Per collateral the latest rent roll in each quarter is used,
    then the collaterals of a loan are rolled up by square feet / units
    (occupied divided by total), falling back to a plain average of the
    reported percentages when the totals are missing or zero.
    """
    bundles = caf_zip_bundles
    if isinstance(bundles, (bytes, bytearray)):
        bundles = [bundles]
    bundles = [item for item in bundles if item]
    if not bundles:
        raise ValueError("No Midland CAF bundle was uploaded.")

    rent_roll_parts = []
    loan_to_property_parts = []
    loan_parts = []
    alt_id_parts = []
    for position, bundle_bytes in enumerate(bundles, start=1):
        try:
            archive = zipfile.ZipFile(
                bundle_bytes if isinstance(bundle_bytes, Path) else io.BytesIO(bundle_bytes)
            )
        except zipfile.BadZipFile as exc:
            raise ValueError(
                f"CAF bundle {position} of {len(bundles)} is not a readable .zip archive. Upload "
                "the Midland CAF extract bundles exactly as downloaded."
            ) from exc

        with archive as zf:
            rent_roll_parts.append(
                read_caf_extract_from_zip(zf, CAF_RENTROLL_FILE_TOKEN, "RentRoll")
            )
            loan_to_property_parts.append(
                read_caf_extract_from_zip(zf, CAF_LOAN_TO_PROPERTY_FILE_TOKEN, "LoanToProperty")
            )
            loan_parts.append(read_caf_extract_from_zip(zf, CAF_LOAN_FILE_TOKEN, "Loan"))
            # Required, not optional: the Investor Loan Number is the join key.
            if CAF_PREFER_INVESTOR_LOAN_NUMBER:
                alt_id_parts.append(
                    read_caf_extract_from_zip(zf, CAF_ALT_ASSET_ID_FILE_TOKEN, "AltAssetId")
                )

    rent_roll = pd.concat(rent_roll_parts, ignore_index=True, sort=False)
    loan_to_property = pd.concat(loan_to_property_parts, ignore_index=True, sort=False)
    loans = pd.concat(loan_parts, ignore_index=True, sort=False)
    alt_ids = pd.concat(alt_id_parts, ignore_index=True, sort=False) if alt_id_parts else None

    empty_selected = pd.DataFrame(
        columns=["Loan ID 5", "Quarter Key", "Occupancy Dec", "Collaterals", "Period End", "Method"]
    )
    issue_rows: list[dict[str, Any]] = []

    required_rr = [CAF_RR_COLLATERAL_COL, CAF_RR_PERIOD_DATE_COL, CAF_RR_OCC_PCT_COL]
    missing_rr = [c for c in required_rr if c not in rent_roll.columns]
    if missing_rr:
        raise ValueError(
            f"The CAF RentRoll extract is missing required columns: {missing_rr}. "
            f"Available columns: {sorted(rent_roll.columns.tolist())}"
        )

    missing_l2p = [
        c for c in (CAF_L2P_COLLATERAL_COL, CAF_L2P_LOAN_COL) if c not in loan_to_property.columns
    ]
    if missing_l2p:
        raise ValueError(
            f"The CAF LoanToProperty extract is missing required columns: {missing_l2p}. "
            f"Available columns: {sorted(loan_to_property.columns.tolist())}"
        )

    loan_keys = _caf_build_loan_key_map(loans, alt_ids)

    rr = rent_roll.copy()
    rr["_ColID"] = rr[CAF_RR_COLLATERAL_COL].astype(str).str.strip()
    rr["_Period_dt"] = _caf_parse_period_dates(rr[CAF_RR_PERIOD_DATE_COL])
    rr["_Occ Pct"] = pd.to_numeric(rr[CAF_RR_OCC_PCT_COL], errors="coerce")
    if CAF_RR_OCC_AMOUNT_COL in rr.columns:
        rr["_Occ Amount"] = pd.to_numeric(rr[CAF_RR_OCC_AMOUNT_COL], errors="coerce")
    else:
        rr["_Occ Amount"] = pd.Series([None] * len(rr), index=rr.index, dtype="float64")
    if CAF_RR_TOTAL_AMOUNT_COL in rr.columns:
        rr["_Total Amount"] = pd.to_numeric(rr[CAF_RR_TOTAL_AMOUNT_COL], errors="coerce")
    else:
        rr["_Total Amount"] = pd.Series([None] * len(rr), index=rr.index, dtype="float64")
    if CAF_RR_UOM_COL in rr.columns:
        rr["_UOM"] = rr[CAF_RR_UOM_COL].astype(str).str.strip().str.upper()
    else:
        rr["_UOM"] = ""

    if CAF_RR_EXTRACT_DATE_COL in rr.columns:
        rr["_ExtractDate_dt"] = _caf_parse_period_dates(rr[CAF_RR_EXTRACT_DATE_COL])
    else:
        rr["_ExtractDate_dt"] = pd.NaT

    rent_roll_rows_read = len(rr)
    rr = rr[(rr["_ColID"] != "") & rr["_Period_dt"].notna()].copy()
    has_weights = rr["_Occ Amount"].notna() & rr["_Total Amount"].notna()
    rr = rr[rr["_Occ Pct"].notna() | has_weights].copy()

    if rr.empty:
        return {}, empty_selected, pd.DataFrame(issue_rows), rent_roll_rows_read

    # A rent roll master repeats its occupancy on every tenant line, so collapse
    # to one row per collateral / period / master before anything is summed.
    master_keys = ["_ColID", "_Period_dt"]
    if CAF_RR_MASTER_ID_COL in rr.columns:
        rr["_MasterID"] = rr[CAF_RR_MASTER_ID_COL].astype(str).str.strip()
        master_keys = ["_ColID", "_Period_dt", "_MasterID"]
    # Bundles overlap, so the same rent roll arrives more than once. Keep the
    # one from the newest extract in case the servicer restated it.
    rr = rr.sort_values("_ExtractDate_dt", kind="stable", na_position="first")
    rr = rr.drop_duplicates(subset=master_keys, keep="last").copy()

    rr["Quarter Key"] = (
        rr["_Period_dt"].dt.year.astype(int).astype(str)
        + " Q"
        + rr["_Period_dt"].dt.quarter.astype(int).astype(str)
    )

    if target_quarter_keys:
        wanted = list(dict.fromkeys(target_quarter_keys))
        rr = rr[rr["Quarter Key"].isin(wanted)].copy()
        if rr.empty:
            return {}, empty_selected, pd.DataFrame(issue_rows), rent_roll_rows_read

    # Within a quarter keep the latest rent roll for each collateral.
    rr = rr.sort_values(["_ColID", "Quarter Key", "_Period_dt"], kind="stable")
    rr = rr.drop_duplicates(subset=["_ColID", "Quarter Key"], keep="last").copy()

    l2p = loan_to_property[[CAF_L2P_COLLATERAL_COL, CAF_L2P_LOAN_COL]].copy()
    l2p.columns = ["_ColID", "_LnID"]
    l2p["_ColID"] = l2p["_ColID"].astype(str).str.strip()
    l2p["_LnID"] = l2p["_LnID"].astype(str).str.strip()
    l2p = l2p[(l2p["_ColID"] != "") & (l2p["_LnID"] != "")].drop_duplicates()

    merged = rr.merge(l2p, on="_ColID", how="inner").merge(loan_keys, on="_LnID", how="inner")
    if merged.empty:
        issue_rows.append(
            {
                "Loan ID 5": "",
                "Quarter Key": "",
                "Issue": "No rent roll collateral could be tied to a loan in this CAF bundle.",
            }
        )
        return {}, empty_selected, pd.DataFrame(issue_rows), rent_roll_rows_read

    # One collateral counts once per loan-quarter even if it reaches the loan
    # through more than one LoanToProperty row.
    merged = merged.drop_duplicates(subset=["_Loan ID 5", "Quarter Key", "_ColID"]).copy()

    selected_rows: list[dict[str, Any]] = []
    for (loan5, quarter_key), group in merged.groupby(["_Loan ID 5", "Quarter Key"], dropna=False):
        pool = group
        uoms = {value for value in group["_UOM"].tolist() if value not in ("", "NAN", "NONE")}
        if len(uoms) > 1:
            # Square feet and unit counts cannot be added together, so weight
            # within whichever unit of measure covers the most of the loan.
            totals_by_uom = group.groupby("_UOM")["_Total Amount"].sum(min_count=1)
            totals_by_uom = totals_by_uom[totals_by_uom.index.isin(uoms)].dropna()
            if not totals_by_uom.empty:
                dominant = totals_by_uom.idxmax()
                pool = group[group["_UOM"] == dominant]
                issue_rows.append(
                    {
                        "Loan ID 5": loan5,
                        "Quarter Key": quarter_key,
                        "Issue": (
                            f"Collateral mixes units of measure {sorted(uoms)}; weighted using "
                            f"'{dominant}' only."
                        ),
                    }
                )

        def average_of_percentages(frame):
            percentages = frame["_Occ Pct"].dropna()
            if percentages.empty:
                return None
            return float(percentages.mean()) / 100.0

        # A collateral with no total carries no weight, so its occupied count
        # must not be added either. CAF bundles do contain superseded rows that
        # repeat the occupied figure against a zero total, and counting those
        # would push the loan above 100%.
        carries_weight = pool["_Total Amount"].notna() & (pool["_Total Amount"] > 0)
        weighted = pool[carries_weight]
        unweighted = pool[~carries_weight]
        total_amount = weighted["_Total Amount"].sum(min_count=1)
        occupied_amount = weighted["_Occ Amount"].sum(min_count=1)

        if pd.notna(total_amount) and pd.notna(occupied_amount) and float(total_amount) > 0:
            occupancy_dec = float(occupied_amount) / float(total_amount)
            method = "Weighted by square feet / units"
            dropped = unweighted[unweighted["_Occ Amount"].fillna(0) > 0]
            if not dropped.empty:
                issue_rows.append(
                    {
                        "Loan ID 5": loan5,
                        "Quarter Key": quarter_key,
                        "Issue": (
                            f"Ignored {len(dropped)} collateral row(s) that report occupied space "
                            "against a zero total."
                        ),
                    }
                )
        else:
            occupancy_dec = average_of_percentages(pool)
            if occupancy_dec is None:
                continue
            method = "Average of collateral occupancy percentages"
            issue_rows.append(
                {
                    "Loan ID 5": loan5,
                    "Quarter Key": quarter_key,
                    "Issue": (
                        "No usable square feet / unit totals; used the average of the reported "
                        "occupancy percentages."
                    ),
                }
            )

        if occupancy_dec > 1.0:
            # An impossible rollup means the underlying counts disagree. The
            # servicer's own reported percentage is the safer number to show.
            reported = average_of_percentages(pool)
            if reported is not None and reported <= 1.0:
                issue_rows.append(
                    {
                        "Loan ID 5": loan5,
                        "Quarter Key": quarter_key,
                        "Issue": (
                            f"Weighted rollup came to {occupancy_dec:.1%}, above 100%; used the "
                            f"reported occupancy of {reported:.1%} instead."
                        ),
                    }
                )
                occupancy_dec = reported
                method = "Average of collateral occupancy percentages"
            else:
                issue_rows.append(
                    {
                        "Loan ID 5": loan5,
                        "Quarter Key": quarter_key,
                        "Issue": (
                            f"Rolled-up occupancy is {occupancy_dec:.1%}, above 100%. Check the CAF "
                            "rent roll for this loan."
                        ),
                    }
                )

        selected_rows.append(
            {
                "Loan ID 5": loan5,
                "Quarter Key": quarter_key,
                "Occupancy Dec": occupancy_dec,
                "Collaterals": int(pool["_ColID"].nunique()),
                "Period End": pool["_Period_dt"].max(),
                "Method": method,
            }
        )

    selected_df = pd.DataFrame(selected_rows) if selected_rows else empty_selected
    issues_df = pd.DataFrame(issue_rows)

    lookup = {
        (row["Loan ID 5"], row["Quarter Key"]): row["Occupancy Dec"]
        for _, row in selected_df.iterrows()
        if row["Loan ID 5"] and pd.notna(row["Occupancy Dec"])
    }
    return lookup, selected_df, issues_df, rent_roll_rows_read


@st.cache_data(show_spinner=False, max_entries=4)
def load_midland_occupancy_lookup_cached(
    caf_zip_bundles: tuple[bytes | Path, ...], target_quarter_keys: tuple[str, ...]
):
    return build_midland_occupancy_lookup(list(caf_zip_bundles), list(target_quarter_keys))



def parse_caf_bundle_date(file_name: str):
    """Reads the date out of a CAF_Extracts_MMDDYY_HHMM.zip file name."""
    match = CAF_BUNDLE_NAME_RX.match(Path(file_name).name)
    if not match:
        return None
    month, day, year = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    try:
        return date(2000 + year, month, day)
    except ValueError:
        return None



def unc_target_for_drive(drive: str):
    """
    Returns the \\\\server\\share behind a mapped Windows drive letter.

    Drive letters are per-logon-session, so a letter that works in Explorer can
    be missing from the process running this app. The UNC path behind it
    usually still resolves, which makes it a good fallback.
    """
    drive = str(drive).rstrip("\\/")
    if len(drive) != 2 or not drive.endswith(":"):
        return None
    try:
        import ctypes
        from ctypes import wintypes

        mpr = ctypes.WinDLL("mpr")
    except Exception:
        return None

    size = wintypes.DWORD(2048)
    buffer = ctypes.create_unicode_buffer(size.value)
    try:
        result = mpr.WNetGetConnectionW(ctypes.c_wchar_p(drive), buffer, ctypes.byref(size))
    except Exception:
        return None

    # 0 is success; 1201 means the mapping is known but not connected, and the
    # buffer is filled in either case.
    if result in (0, 1201) and buffer.value:
        return buffer.value
    return None



def unc_equivalent_path(folder: Path):
    """Rewrites X:\\some\\folder onto the share that X: points at."""
    drive = folder.drive
    if not drive or drive.startswith("\\\\"):
        return None
    target = unc_target_for_drive(drive)
    if not target:
        return None
    relative = str(folder)[len(drive):].lstrip("\\/")
    return Path(target) / relative if relative else Path(target)



def describe_unreadable_folder(typed: str, folder: Path, attempted_unc) -> str:
    parts = [f"'{typed}' is not a folder this app can reach."]

    try:
        if folder.is_file():
            parts.append("That path is a file, not a folder. Give the folder that contains it.")
            return " ".join(parts)
    except OSError:
        pass

    drive = folder.drive
    if drive and not drive.startswith("\\\\"):
        mapped = unc_target_for_drive(drive)
        if mapped:
            parts.append(f"{drive} is a mapped network drive pointing at {mapped}.")
            if attempted_unc is not None:
                parts.append(f"That share was tried as '{attempted_unc}' and did not open either.")
            parts.append(
                "Try pasting the full network path instead of the drive letter, and check the "
                "account running this app can open that share."
            )
        else:
            parts.append(
                f"The machine running this app has no {drive} drive. Mapped drive letters belong "
                "to the Windows session that created them, so a letter that works in Explorer is "
                "often missing here. Paste the full network path instead, or upload the files."
            )
    else:
        try:
            parent = folder.parent
            if parent != folder and parent.is_dir():
                parts.append(f"Its parent '{parent}' does open, so check the spelling of the last part.")
        except OSError:
            pass

    return " ".join(parts)



def resolve_servicer_folder(folder_path: str) -> Path:
    typed = str(folder_path).strip().strip('"').strip("'")
    if not typed:
        raise ValueError("Enter a folder path first.")

    folder = Path(typed).expanduser()
    try:
        if folder.is_dir():
            return folder
    except OSError:
        pass

    # A mapped drive letter often is not visible to this process, but the share
    # behind it is.
    attempted_unc = unc_equivalent_path(folder)
    if attempted_unc is not None:
        try:
            if attempted_unc.is_dir():
                return attempted_unc
        except OSError:
            pass

    raise ValueError(describe_unreadable_folder(typed, folder, attempted_unc))



def iter_servicer_folder_files(folder: Path, max_depth: int = SERVICER_FOLDER_SEARCH_DEPTH):
    """
    Yields the files in a folder and its subfolders, down to max_depth.

    Servicers archive older files a couple of levels down, so a plain listing
    of the top folder misses most of the history.
    """
    directories = [(folder, 0)]
    while directories:
        directory, depth = directories.pop()
        try:
            entries = list(directory.iterdir())
        except OSError:
            continue
        for item in entries:
            try:
                if item.is_dir():
                    if depth < max_depth:
                        directories.append((item, depth + 1))
                elif item.is_file():
                    yield item
            except OSError:
                continue



def list_caf_bundles_in_folder(folder_path: str):
    """Returns [(bundle_date, Path)] for every CAF bundle in a folder, oldest first."""
    folder = resolve_servicer_folder(folder_path)

    found = []
    seen_names: set = set()
    for item in iter_servicer_folder_files(folder):
        stamp = parse_caf_bundle_date(item.name)
        if stamp is not None and item.name not in seen_names:
            seen_names.add(item.name)
            found.append((stamp, item))

    if not found:
        raise ValueError(
            f"No files named like 'CAF_Extracts_MMDDYY_HHMM.zip' were found in '{folder_path}'."
        )
    found.sort(key=lambda pair: (pair[0], pair[1].name))
    return found



def quarter_key_end_date(quarter_key: str):
    """'2025 Q3' -> date(2025, 9, 30)."""
    try:
        year_text, quarter_text = str(quarter_key).split(" Q")
        year = int(year_text)
        quarter = int(quarter_text)
        month = quarter * 3
        return date(year, month, calendar.monthrange(year, month)[1])
    except Exception:
        return None



def pick_caf_bundles_for_quarters(bundles, quarter_keys, lag_days: int = CAF_RENT_ROLL_LAG_DAYS):
    """
    Chooses one bundle per template quarter.

    A bundle only holds the latest rent roll for each property, so the one that
    fills a given quarter is the one downloaded about a quarter later. Picks the
    bundle closest to that date and returns {quarter_key: (bundle_date, Path)}.
    """
    chosen: dict = {}
    if not bundles:
        return chosen

    for quarter_key in quarter_keys:
        quarter_end = quarter_key_end_date(quarter_key)
        if quarter_end is None:
            continue
        target = quarter_end + timedelta(days=lag_days)
        chosen[quarter_key] = min(bundles, key=lambda pair: abs((pair[0] - target).days))
    return chosen


def parse_berkadia_tape_date(file_name: str):
    """Reads the date out of a name like CoreVest_Data_Tape_09_19_2026.xlsx."""
    match = BERKADIA_TAPE_NAME_RX.search(Path(file_name).name)
    if not match:
        return None
    month, day, year = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    try:
        return date(year, month, day)
    except ValueError:
        return None



def list_berkadia_tapes_in_folder(folder_path: str, include_subfolders: bool = True):
    """
    Returns [(tape_date, Path)] for the Berkadia tapes in a folder, newest first.

    Older tapes are usually filed away in year subfolders, so one level down is
    searched too. A tape with no date in its name falls back to its file date.
    """
    folder = resolve_servicer_folder(folder_path)
    depth = SERVICER_FOLDER_SEARCH_DEPTH if include_subfolders else 0

    found = []
    seen: set = set()
    for item in iter_servicer_folder_files(folder, depth):
        if item.suffix.lower() not in BERKADIA_TAPE_SUFFIXES:
            continue
        if item.name.startswith("~$") or item.name in seen:
            continue
        stamp = parse_berkadia_tape_date(item.name)
        if stamp is None:
            try:
                stamp = datetime.fromtimestamp(item.stat().st_mtime).date()
            except OSError:
                continue
        seen.add(item.name)
        found.append((stamp, item))

    if not found:
        raise ValueError(f"No Berkadia .xlsx tapes were found in '{folder_path}'.")

    preferred = [
        pair for pair in found if BERKADIA_TAPE_NAME_HINT in pair[1].name.lower()
    ]
    if preferred:
        found = preferred

    # Newest first, with the name as a tie-break so a given folder always
    # resolves the same way.
    found.sort(key=lambda pair: (pair[0], pair[1].name), reverse=True)
    return found



def build_berkadia_occupancy_lookup_multi(tape_bytes_list, target_quarter_keys=None):
    """
    Runs the Berkadia reader over one or more tapes, newest first, and merges.

    Earlier tapes in the list win on any overlap, so pass the newest first.
    """
    tapes = tape_bytes_list
    if isinstance(tapes, (bytes, bytearray)):
        tapes = [tapes]
    tapes = [item for item in tapes if item]
    if not tapes:
        raise ValueError("No Berkadia tape was provided.")

    lookups = []
    selected_parts = []
    issue_parts = []
    rows_read = 0
    for tape_bytes in tapes:
        lookup, selected, issues, fa = build_term_occupancy_lookup(tape_bytes, target_quarter_keys)
        lookups.append(lookup)
        if isinstance(selected, pd.DataFrame) and not selected.empty:
            selected_parts.append(selected)
        if isinstance(issues, pd.DataFrame) and not issues.empty:
            issue_parts.append(issues)
        rows_read += len(fa) if isinstance(fa, pd.DataFrame) else 0

    merged_lookup = merge_occupancy_lookups(*lookups)

    if selected_parts:
        merged_selected = pd.concat(selected_parts, ignore_index=True, sort=False)
        key_cols = [c for c in ("Loan ID 5", "Quarter Key") if c in merged_selected.columns]
        if key_cols:
            merged_selected = merged_selected.drop_duplicates(subset=key_cols, keep="first")
    else:
        merged_selected = pd.DataFrame()

    merged_issues = (
        pd.concat(issue_parts, ignore_index=True, sort=False) if issue_parts else pd.DataFrame()
    )
    if not merged_issues.empty:
        merged_issues = merged_issues.drop_duplicates()

    return merged_lookup, merged_selected, merged_issues, rows_read


@st.cache_data(show_spinner=False, max_entries=4)
def load_berkadia_occupancy_lookup_cached(
    tape_bytes_list: tuple[bytes, ...], target_quarter_keys: tuple[str, ...]
):
    return build_berkadia_occupancy_lookup_multi(list(tape_bytes_list), list(target_quarter_keys))


@st.cache_data(show_spinner=False)
def read_berkadia_tape_files(file_keys: tuple) -> tuple:
    """Reads tapes off disk. Keyed on path plus size so edits bust the cache."""
    return tuple(Path(path).read_bytes() for path, _size in file_keys)


@st.cache_data(show_spinner=False)
def read_caf_bundle_files(file_keys: tuple) -> tuple:
    """Reads bundles off disk. Keyed on path plus size so edits bust the cache."""
    return tuple(Path(path).read_bytes() for path, _size in file_keys)



def merge_occupancy_lookups(*lookups) -> dict:
    """
    Combines occupancy lookups into one. Earlier arguments win on overlap, so
    passing Berkadia first keeps Berkadia's value wherever both servicers
    report the same loan and quarter.
    """
    merged: dict = {}
    for lookup in reversed([item for item in lookups if item]):
        for key, value in lookup.items():
            if value is None:
                continue
            try:
                if pd.isna(value):
                    continue
            except Exception:
                pass
            merged[key] = value
    return merged




def apply_term_occupancy_to_rows(
    term_rows: pd.DataFrame,
    occ_lookup: dict,
    occ_headers,
) -> pd.DataFrame:
    out = term_rows.copy()
    if out.empty or not occ_headers:
        return out

    out["_Loan ID 5"] = out["Loan ID"].apply(loan_id_5)
    for item in occ_headers:
        header = item["header"]
        quarter_key = item["quarter_key"]
        out[header] = out["_Loan ID 5"].apply(
            lambda k: occ_lookup.get((k, quarter_key), "-") if k else "-"
        )

    occ_cols = [item["header"] for item in occ_headers]
    out["Occupancy Matched"] = out[occ_cols].apply(
        lambda row: any(v not in ("-", None) and not (isinstance(v, float) and pd.isna(v)) for v in row),
        axis=1,
    )
    out = out.drop(columns=["_Loan ID 5"], errors="ignore")
    return out



def add_default_term_occupancy_columns(term_rows: pd.DataFrame, occ_headers) -> pd.DataFrame:
    return apply_term_occupancy_to_rows(term_rows, {}, occ_headers)


# -------------------------
# Excel helpers
# -------------------------
def norm_hdr(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    return re.sub(r"\s+", " ", text)



def find_header_row_and_map(ws, must_have=("portfolio", "loan id"), scan_rows: int = 80, scan_cols: int = 200):
    header_row = None
    for row_num in range(1, min(ws.max_row, scan_rows) + 1):
        row_values = [norm_hdr(ws.cell(row_num, col_num).value) for col_num in range(1, min(ws.max_column, scan_cols) + 1)]
        if all(header in row_values for header in must_have):
            header_row = row_num
            break
    if header_row is None:
        raise ValueError(f"Could not find header row on '{ws.title}' containing {must_have}.")

    col_map = {}
    for col_num in range(1, min(ws.max_column, scan_cols) + 1):
        value = ws.cell(header_row, col_num).value
        if value is not None and str(value).strip() != "":
            col_map[norm_hdr(value)] = col_num
    return header_row, col_map



def find_total_row(ws, header_row: int, scan_cols: int = 200):
    start_row = header_row + 1
    for row_num in range(start_row, ws.max_row + 1):
        for col_num in range(1, min(ws.max_column, scan_cols) + 1):
            value = ws.cell(row_num, col_num).value
            if isinstance(value, str) and value.strip().lower() == "total":
                return row_num
    return None



def snapshot_row_style(ws, row_num: int, last_col: int):
    row_height = ws.row_dimensions[row_num].height
    styles = {}
    for col_num in range(1, last_col + 1):
        cell = ws.cell(row_num, col_num)
        styles[col_num] = {
            "_style": copy(cell._style),
            "font": copy(cell.font),
            "border": copy(cell.border),
            "fill": copy(cell.fill),
            "alignment": copy(cell.alignment),
            "protection": copy(cell.protection),
            "number_format": cell.number_format,
        }
    return row_height, styles



def apply_row_style(ws, row_num: int, styles_by_col, row_height, last_col: int) -> None:
    if row_height is not None:
        ws.row_dimensions[row_num].height = row_height
    for col_num in range(1, last_col + 1):
        cell = ws.cell(row_num, col_num)
        style = styles_by_col.get(col_num)
        if not style:
            continue
        cell._style = copy(style["_style"])
        cell.font = copy(style["font"])
        cell.border = copy(style["border"])
        cell.fill = copy(style["fill"])
        cell.alignment = copy(style["alignment"])
        cell.protection = copy(style["protection"])
        cell.number_format = style["number_format"]



def ensure_rows(ws, header_row: int, total_row: int, needed_rows: int, last_col: int):
    if total_row is None:
        raise ValueError(f"Could not find TOTAL row on '{ws.title}'.")

    start_row = header_row + 1
    existing_rows = total_row - start_row
    row_a = start_row
    row_b = start_row + 1 if start_row + 1 < total_row else start_row
    a_height, a_styles = snapshot_row_style(ws, row_a, last_col)
    b_height, b_styles = snapshot_row_style(ws, row_b, last_col)
    # Inserting or deleting rows drags a neighbouring row's height onto the
    # TOTAL row, so keep its own look and put it back afterwards.
    total_height, total_styles = snapshot_row_style(ws, total_row, last_col)

    if needed_rows > existing_rows:
        add_rows = needed_rows - existing_rows
        ws.insert_rows(total_row, amount=add_rows)
        total_row += add_rows
    elif needed_rows < existing_rows:
        remove_rows = existing_rows - needed_rows
        ws.delete_rows(total_row - remove_rows, amount=remove_rows)
        total_row -= remove_rows

    for idx, row_num in enumerate(range(start_row, start_row + needed_rows)):
        use_alternate = idx % 2 == 1
        apply_row_style(
            ws,
            row_num,
            b_styles if use_alternate else a_styles,
            b_height if use_alternate else a_height,
            last_col,
        )

    apply_row_style(ws, total_row, total_styles, total_height, last_col)

    # Deleting rows leaves their heights behind, which shows up as banded
    # empty rows under the TOTAL row.
    for stale_row in [r for r in ws.row_dimensions if r > total_row]:
        del ws.row_dimensions[stale_row]

    return start_row, total_row



def excel_safe(value: Any):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return value.item()
    except Exception:
        pass
    return value



def set_cell(ws, row_num: int, col_num: int, value: Any, number_format: str | None = None) -> None:
    cell = ws.cell(row_num, col_num)
    cell.value = excel_safe(value)
    if number_format:
        cell.number_format = number_format



def round_percent_decimal(value: Any):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return round(float(value) * 100) / 100.0



def set_occupancy_cell(ws, row_num: int, col_num: int, value: Any) -> None:
    if value is None:
        set_cell(ws, row_num, col_num, "-")
        return

    if isinstance(value, str):
        text = value.strip()
        if text == "" or text == "-":
            set_cell(ws, row_num, col_num, "-")
            return
        parsed = pct_to_dec(text)
        if parsed is None:
            set_cell(ws, row_num, col_num, text)
        else:
            set_cell(ws, row_num, col_num, round_percent_decimal(parsed), "0%")
        return

    try:
        if pd.isna(value):
            set_cell(ws, row_num, col_num, "-")
            return
    except Exception:
        pass

    set_cell(ws, row_num, col_num, round_percent_decimal(value), "0%")



def sum_ints(series) -> int:
    values = [value for value in series if value is not None and not pd.isna(value)]
    return int(sum(values)) if values else 0



def sum_money(series) -> float:
    values = [float(value) for value in series if value is not None and not pd.isna(value)]
    return float(sum(values)) if values else 0.0




def fill_blanks_with_dash(ws, first_row, last_row, columns, placeholder: str = BLANK_CELL_PLACEHOLDER) -> None:
    """
    Writes a dash into every empty cell of the given block.

    Only None and whitespace-only strings count as empty, so a real zero, a
    False and an existing dash are all left exactly as they are.
    """
    if first_row is None or last_row is None or last_row < first_row:
        return
    for row_num in range(int(first_row), int(last_row) + 1):
        for col_num in columns:
            cell = ws.cell(row_num, col_num)
            value = cell.value
            if value is None or (isinstance(value, str) and value.strip() == ""):
                cell.value = placeholder



def write_term_sheet(ws, term_rows: pd.DataFrame, guarantor: str = "", occ_headers=None):
    header_row, col_map = find_header_row_and_map(ws, must_have=("portfolio", "loan id"))
    last_col = max(col_map.values())
    total_row = find_total_row(ws, header_row)
    start_row, total_row = ensure_rows(ws, header_row, total_row, needed_rows=len(term_rows), last_col=last_col)

    if occ_headers is None:
        occ_headers = get_term_occupancy_headers(ws)

    def col(name: str):
        return col_map.get(norm_hdr(name))

    total_loan_amount = sum_money(term_rows["Loan Amount Num"].tolist()) if "Loan Amount Num" in term_rows else 0.0
    total_upb = sum_money(term_rows["Outstanding Balance Num"].tolist()) if "Outstanding Balance Num" in term_rows else 0.0
    total_properties = sum_ints(term_rows["Total Properties Num"].tolist()) if "Total Properties Num" in term_rows else 0
    total_units = sum_ints(term_rows["Total Units Num"].tolist()) if "Total Units Num" in term_rows else 0

    for idx, row in term_rows.reset_index(drop=True).iterrows():
        row_num = start_row + idx
        if col("portfolio"):
            set_cell(ws, row_num, col("portfolio"), nonblank_or_fallback(row.get("Portfolio", ""), "Term"))
        if col("loan id"):
            set_cell(ws, row_num, col("loan id"), row.get("Loan ID", ""))
        if col("loan"):
            set_cell(ws, row_num, col("loan"), row.get("Loan", ""))
        if col("account name"):
            set_cell(ws, row_num, col("account name"), row.get("Account Name", ""))
        if col("guarantor"):
            guarantor_value = clean_guarantor_name(row.get("Guarantor", "")) or clean_guarantor_name(
                guarantor
            )
            set_cell(ws, row_num, col("guarantor"), guarantor_value)
        if col("origination date"):
            set_cell(ws, row_num, col("origination date"), row.get("Origination Date", None), "m/d/yyyy")
        if col("loan amount"):
            value = row.get("Loan Amount Num", None)
            value = None if (value is None or pd.isna(value)) else int(round(float(value)))
            set_cell(ws, row_num, col("loan amount"), value, "$#,##0")
        if col("outstanding balance"):
            value = row.get("Outstanding Balance Num", None)
            value = None if (value is None or pd.isna(value)) else int(round(float(value)))
            set_cell(ws, row_num, col("outstanding balance"), value, "$#,##0")
        if col("origination ltv"):
            value = row.get("LTV Dec", None)
            value = None if (value is None or pd.isna(value)) else float(value)
            set_cell(ws, row_num, col("origination ltv"), value, "0%")
        if col("interest rate"):
            value = row.get("Rate Dec", None)
            value = None if (value is None or pd.isna(value)) else float(value)
            set_cell(ws, row_num, col("interest rate"), value, "0.00%")
        if col("state(s)"):
            set_cell(ws, row_num, col("state(s)"), row.get("State(s)", ""))
        if col("total properties"):
            set_cell(ws, row_num, col("total properties"), row.get("Total Properties Num", None), "0")
        if col("total units"):
            set_cell(ws, row_num, col("total units"), row.get("Total Units Num", None), "0")
        if col("recourse"):
            set_cell(ws, row_num, col("recourse"), row.get("Recourse", ""))
        if col("historical ontime payment %"):
            value = row.get("Historical Ontime % Dec", None)
            value = None if (value is None or pd.isna(value)) else float(value)
            set_cell(ws, row_num, col("historical ontime payment %"), value, "0%")
        if col("next payment date"):
            outstanding_balance = row.get("Outstanding Balance Num", None)
            is_paid_off = False
            if outstanding_balance is not None and not pd.isna(outstanding_balance):
                try:
                    is_paid_off = float(outstanding_balance) == 0.0
                except Exception:
                    is_paid_off = False
            if is_paid_off:
                set_cell(ws, row_num, col("next payment date"), "Paid Off")
            else:
                set_cell(ws, row_num, col("next payment date"), row.get("Next Payment Date", None), "m/d/yyyy")
        if col("current loan maturity date"):
            set_cell(ws, row_num, col("current loan maturity date"), row.get("Maturity Date", None), "m/d/yyyy")

        if occ_headers:
            for occ in occ_headers:
                set_occupancy_cell(ws, row_num, occ["col"], row.get(occ["header"], "-"))
        else:
            for key in list(col_map.keys()):
                if "occ" in key:
                    set_cell(ws, row_num, col_map[key], "-")

    if col("loan"):
        set_cell(ws, total_row, col("loan"), int(len(term_rows)))
    if col("loan amount"):
        set_cell(ws, total_row, col("loan amount"), int(round(total_loan_amount)) if total_loan_amount else 0, "$#,##0")
    if col("outstanding balance"):
        set_cell(ws, total_row, col("outstanding balance"), int(round(total_upb)) if total_upb else 0, "$#,##0")
    if col("interest rate"):
        # The template's =AVERAGE(J3:J9) never follows the inserted rows, so it
        # averaged the first seven loans only. Write the mean of every loan.
        rates = [
            float(value)
            for value in (term_rows["Rate Dec"].tolist() if "Rate Dec" in term_rows else [])
            if value is not None and not pd.isna(value)
        ]
        set_cell(ws, total_row, col("interest rate"), sum(rates) / len(rates) if rates else None, "0.00%")
    if col("total properties"):
        set_cell(ws, total_row, col("total properties"), total_properties, "0")
    if col("total units"):
        set_cell(ws, total_row, col("total units"), total_units, "0")

    # Loan rows show a dash rather than a blank. The TOTAL row is left alone:
    # a dash under a column that has no total reads as a missing figure.
    fill_blanks_with_dash(ws, start_row, start_row + len(term_rows) - 1, sorted(set(col_map.values())))



def write_bridge_sheet(ws, bridge_rows: pd.DataFrame):
    header_row, col_map = find_header_row_and_map(ws, must_have=("portfolio", "loan id"))
    last_col = max(col_map.values())
    total_row = find_total_row(ws, header_row)
    start_row, total_row = ensure_rows(ws, header_row, total_row, needed_rows=len(bridge_rows), last_col=last_col)

    def col(name: str):
        return col_map.get(norm_hdr(name))

    total_commitment = sum_money(bridge_rows["Commitment Amount Num"].tolist()) if "Commitment Amount Num" in bridge_rows else 0.0
    total_lifetime = sum_money(bridge_rows["Lifetime Funded Num"].tolist()) if "Lifetime Funded Num" in bridge_rows else 0.0
    total_upb = sum_money(bridge_rows["Outstanding Balance Num"].tolist()) if "Outstanding Balance Num" in bridge_rows else 0.0
    total_properties = sum_ints(bridge_rows["Total Properties Num"].tolist()) if "Total Properties Num" in bridge_rows else 0
    total_paid = sum_ints(bridge_rows["Paid Off Assets Num"].tolist()) if "Paid Off Assets Num" in bridge_rows else 0
    total_active = sum_ints(bridge_rows["Active Assets Num"].tolist()) if "Active Assets Num" in bridge_rows else 0

    for idx, row in bridge_rows.reset_index(drop=True).iterrows():
        row_num = start_row + idx
        if col("portfolio"):
            set_cell(ws, row_num, col("portfolio"), nonblank_or_fallback(row.get("Portfolio", ""), "Bridge"))
        if col("loan id"):
            set_cell(ws, row_num, col("loan id"), row.get("Loan ID", ""))
        if col("loan name"):
            set_cell(ws, row_num, col("loan name"), row.get("Loan", ""))
        elif col("loan"):
            set_cell(ws, row_num, col("loan"), row.get("Loan", ""))
        if col("commitment amount"):
            value = row.get("Commitment Amount Num", None)
            value = None if (value is None or pd.isna(value)) else int(round(float(value)))
            set_cell(ws, row_num, col("commitment amount"), value, "$#,##0")
        if col("line origination date"):
            set_cell(ws, row_num, col("line origination date"), row.get("Origination Date", None), "m/d/yyyy")
        elif col("origination date"):
            set_cell(ws, row_num, col("origination date"), row.get("Origination Date", None), "m/d/yyyy")
        if col("line maturity date"):
            set_cell(ws, row_num, col("line maturity date"), row.get("Maturity Date", None), "m/d/yyyy")
        if col("interest rate"):
            value = row.get("Rate Dec", None)
            value = None if (value is None or pd.isna(value)) else float(value)
            set_cell(ws, row_num, col("interest rate"), value, "0.00%")
        if col("ltv"):
            value = row.get("LTV Dec", None)
            value = None if (value is None or pd.isna(value)) else float(value)
            set_cell(ws, row_num, col("ltv"), value, "0%")
        if col("advances"):
            set_cell(ws, row_num, col("advances"), row.get("Advances Num", None), "0")
        if col("total funded assets"):
            set_cell(ws, row_num, col("total funded assets"), row.get("Total Properties Num", None), "0")
        if col("state(s)"):
            set_cell(ws, row_num, col("state(s)"), row.get("State(s)", ""))
        if col("lifetime funded"):
            value = row.get("Lifetime Funded Num", None)
            value = None if (value is None or pd.isna(value)) else int(round(float(value)))
            set_cell(ws, row_num, col("lifetime funded"), value, "$#,##0")
        if col("paid off assets"):
            set_cell(ws, row_num, col("paid off assets"), row.get("Paid Off Assets Num", None), "0")
        if col("active assets"):
            set_cell(ws, row_num, col("active assets"), row.get("Active Assets Num", None), "0")
        if col("outstanding balance"):
            value = row.get("Outstanding Balance Num", None)
            value = None if (value is None or pd.isna(value)) else int(round(float(value)))
            set_cell(ws, row_num, col("outstanding balance"), value, "$#,##0")
        if col("as-is/ arv"):
            value = row.get("As-Is/ ARV Num", None)
            value = None if (value is None or pd.isna(value)) else int(round(float(value)))
            set_cell(ws, row_num, col("as-is/ arv"), value, "$#,##0")
        if col("avg hold time"):
            set_cell(ws, row_num, col("avg hold time"), row.get("Avg Hold Time Num", None), "0")
        if col("avg disposed time"):
            set_cell(ws, row_num, col("avg disposed time"), row.get("Avg Disposed Time Num", None), "0")

    if col("loan name"):
        set_cell(ws, total_row, col("loan name"), int(len(bridge_rows)))
    elif col("loan"):
        set_cell(ws, total_row, col("loan"), int(len(bridge_rows)))
    if col("commitment amount"):
        set_cell(ws, total_row, col("commitment amount"), int(round(total_commitment)) if total_commitment else 0, "$#,##0")
    if col("lifetime funded"):
        set_cell(ws, total_row, col("lifetime funded"), int(round(total_lifetime)) if total_lifetime else 0, "$#,##0")
    if col("outstanding balance"):
        set_cell(ws, total_row, col("outstanding balance"), int(round(total_upb)) if total_upb else 0, "$#,##0")
    if col("total funded assets"):
        set_cell(ws, total_row, col("total funded assets"), total_properties, "0")
    if col("paid off assets"):
        set_cell(ws, total_row, col("paid off assets"), total_paid, "0")
    if col("active assets"):
        set_cell(ws, total_row, col("active assets"), total_active, "0")

    # Loan rows show a dash rather than a blank. The TOTAL row is left alone:
    # a dash under a column that has no total reads as a missing figure.
    fill_blanks_with_dash(
        ws, start_row, start_row + len(bridge_rows) - 1, sorted(set(col_map.values()))
    )



def sanitize_filename(name: str) -> str:
    text = "" if name is None or pd.isna(name) else str(name).strip()
    text = re.sub(r'[<>:"/\\|?*]', "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:120] if text else "AM"



def resolve_repo_file(file_names) -> Path:
    """First of file_names found next to amslide.py, in the working folder, or in a templates folder there."""
    for name in file_names:
        candidate_paths = [
            Path(__file__).resolve().parent / name,
            Path.cwd() / name,
            Path(__file__).resolve().parent / "templates" / name,
            Path.cwd() / "templates" / name,
        ]
        for candidate_path in candidate_paths:
            if candidate_path.exists():
                return candidate_path

    raise RuntimeError(
        f"Could not find '{file_names[0]}' in the repository. Place it next to amslide.py or inside a templates folder in the repo."
    )



def resolve_repo_template_path() -> Path:
    return resolve_repo_file((DEFAULT_TEMPLATE_NAME,))


def load_template_bytes() -> bytes:
    return resolve_repo_template_path().read_bytes()



def build_workbook_bytes(
    template_bytes: bytes,
    term_rows: pd.DataFrame,
    bridge_rows: pd.DataFrame,
    account_name: str,
    fci_alerts: pd.DataFrame | None = None,
    fci_matches: pd.DataFrame | None = None,
    include_fci: bool = False,
):
    workbook = load_workbook(io.BytesIO(template_bytes))
    if TERM_SHEET in workbook.sheetnames:
        write_term_sheet(workbook[TERM_SHEET], term_rows, guarantor="", occ_headers=None)
    if BRIDGE_SHEET in workbook.sheetnames:
        write_bridge_sheet(workbook[BRIDGE_SHEET], bridge_rows)
    if include_fci:
        write_fci_alerts_sheet(
            workbook,
            fci_alerts if fci_alerts is not None else pd.DataFrame(),
            fci_matches if fci_matches is not None else pd.DataFrame(),
        )

    output = io.BytesIO()
    workbook.save(output)
    filename = f"{sanitize_filename(account_name)} AM Slides.xlsx"
    return output.getvalue(), filename



# -------------------------
# PowerPoint slide decks: reading the finished workbook
# -------------------------
# Workbook headers that name the same column as a slide header.
SLIDE_HEADER_ALIASES = {"loanname": "loan", "lineoriginationdate": "originationdate"}


def slide_header_key(value: Any) -> str:
    """Header text cut to letters, digits and %, so 'As-Is / ARV' matches 'As-Is/ ARV'."""
    key = re.sub(r"[^a-z0-9%]+", "", norm_hdr(value))
    return SLIDE_HEADER_ALIASES.get(key, key)



def format_slide_value(value: Any, number_format: str, blank: str = BLANK_CELL_PLACEHOLDER) -> str:
    """A workbook cell's text as Excel displays it under the cell's number format."""
    if value is None:
        return blank
    if isinstance(value, str):
        text = value.replace("\u00a0", " ").strip()
        # An uncalculated formula has no value to show.
        return blank if text == "" or text.startswith("=") else text
    if isinstance(value, (datetime, date)):
        return f"{value.month}/{value.day}/{value.year}"
    if isinstance(value, bool):
        return "Yes" if value else "No"
    try:
        # Excel works from 15 significant digits, so 0.08274999999999999
        # (the mean of 7.80% and 8.75%) shows as 8.28%, not 8.27%.
        number = Decimal(value) if isinstance(value, int) else Decimal(format(float(value), ".15g"))
    except (TypeError, ValueError, ArithmeticError):
        return str(value)
    if not number.is_finite():
        return blank

    fmt = (number_format or "General").split(";")[0]
    if fmt.strip().lower() in ("general", "@", ""):
        if number == number.to_integral_value():
            return str(int(number))
        return format(number.normalize(), "f")
    is_percent = "%" in fmt
    decimals = re.search(r"\.(0+)", fmt)
    places = len(decimals.group(1)) if decimals else 0
    if is_percent:
        number *= 100
    # Excel rounds half away from zero; Python's own formatting would not.
    rounded = number.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    body = f"{abs(rounded):,.{places}f}" if "," in fmt else f"{abs(rounded):.{places}f}"
    sign = "-" if rounded < 0 else ""
    return f"{sign}{'$' if '$' in fmt else ''}{body}{'%' if is_percent else ''}"



def read_sheet_for_slides(workbook_bytes: bytes, sheet_name: str):
    """
    The finished workbook sheet as (headers, rows, total): headers maps column
    number to header text, and each row maps column number to (value,
    number_format). None when the workbook has no such sheet.
    """
    workbook = load_workbook(io.BytesIO(workbook_bytes))
    if sheet_name not in workbook.sheetnames:
        return None
    ws = workbook[sheet_name]
    header_row, col_map = find_header_row_and_map(ws, must_have=("portfolio", "loan id"))
    total_row = find_total_row(ws, header_row)
    columns = sorted(set(col_map.values()))
    headers = {col: str(ws.cell(header_row, col).value).strip() for col in columns}

    def read_row(row_num):
        return {col: (ws.cell(row_num, col).value, ws.cell(row_num, col).number_format) for col in columns}

    rows = []
    for row_num in range(header_row + 1, total_row or ws.max_row + 1):
        row = read_row(row_num)
        if any(not (v is None or (isinstance(v, str) and not v.strip())) for v, _fmt in row.values()):
            rows.append(row)
    return headers, rows, (read_row(total_row) if total_row else None)



def sheet_records_by_key(sheet) -> list[dict]:
    """Each body row of read_sheet_for_slides() as {slide_header_key: raw value}."""
    headers, rows, _total = sheet
    keys = {col: slide_header_key(text) for col, text in headers.items()}
    return [{keys[col]: value for col, (value, _fmt) in row.items()} for row in rows]



# -------------------------
# PowerPoint slide decks: how PowerPoint wraps table text
# -------------------------
# Arial advance widths in 1/1000 em for ASCII 32-126. Arial is metrically
# identical to Helvetica, so these are the standard Helvetica AFM widths, and
# no font file is needed on the server.
ARIAL_WIDTHS = (
    278, 278, 355, 556, 556, 889, 667, 191, 333, 333, 389, 584, 278, 333, 278, 278,
    556, 556, 556, 556, 556, 556, 556, 556, 556, 556, 278, 278, 584, 584, 584, 556,
    1015, 667, 667, 722, 722, 667, 611, 778, 722, 278, 500, 667, 556, 833, 722, 778,
    667, 778, 722, 667, 611, 722, 667, 944, 667, 667, 611, 278, 278, 278, 469, 556,
    333, 556, 556, 500, 556, 556, 278, 556, 556, 222, 222, 500, 222, 833, 556, 556,
    556, 556, 333, 500, 278, 556, 500, 722, 500, 500, 500, 334, 260, 334, 584,
)
ARIAL_BOLD_WIDTHS = (
    278, 333, 474, 556, 556, 889, 722, 238, 333, 333, 389, 584, 278, 333, 278, 278,
    556, 556, 556, 556, 556, 556, 556, 556, 556, 556, 333, 333, 584, 584, 584, 611,
    975, 722, 722, 722, 722, 667, 611, 778, 722, 278, 556, 722, 611, 833, 722, 778,
    667, 778, 722, 667, 611, 722, 667, 944, 667, 667, 611, 333, 278, 333, 584, 556,
    333, 556, 611, 556, 611, 556, 333, 611, 611, 278, 278, 556, 278, 889, 611, 611,
    611, 611, 389, 556, 333, 611, 556, 778, 556, 556, 500, 389, 280, 389, 584,
)
ARIAL_OTHER_WIDTHS = {
    "\u00a0": 278, "\u2013": 556, "\u2014": 1000, "\u2018": 222, "\u2019": 222,
    "\u201c": 333, "\u201d": 333, "\u2022": 350, "\u2026": 1000, "\u00b0": 400,
}
# PowerPoint lays text out on a 1/8pt grid: each glyph's advance is rounded to
# the nearest eighth of a point (measured in PowerPoint across all of ASCII),
# so at 6pt a digit takes 3.375pt rather than 3.336pt and a space 1.625pt.
# With that rounding this model reproduced PowerPoint's own line breaks for
# all but one of 1,365 test rows. Lines are 1.2 x the font size apart.
PPT_ADVANCE_GRID_PT = 0.125
PPT_LINE_HEIGHT_PER_PT = 1.2


@lru_cache(maxsize=4096)
def _glyph_emu(ch: str, size_pt: float, bold: bool) -> float:
    code = ord(ch)
    widths = ARIAL_BOLD_WIDTHS if bold else ARIAL_WIDTHS
    if 32 <= code <= 126:
        units = widths[code - 32]
    elif ch in ARIAL_OTHER_WIDTHS:
        units = ARIAL_OTHER_WIDTHS[ch]
    else:
        base = unicodedata.normalize("NFKD", ch)[:1]
        units = widths[ord(base) - 32] if base and 32 <= ord(base) <= 126 else 556
    points = units * size_pt / 1000.0
    return round(points / PPT_ADVANCE_GRID_PT) * PPT_ADVANCE_GRID_PT * EMU_PER_PT



def text_width_emu(text: str, size_pt: float, bold: bool) -> float:
    return sum(_glyph_emu(ch, size_pt, bold) for ch in text)



def _break_chunks(word: str) -> list[str]:
    """A word split after each hyphen: PowerPoint may end a line at 'Non-'."""
    return re.findall(r"[^-]+-?|-", word)



@lru_cache(maxsize=65536)
def text_line_count(text: str, width_emu: int, size_pt: float, bold: bool) -> int:
    """Lines PowerPoint needs for text in a table cell whose inner width is width_emu."""
    space = _glyph_emu(" ", size_pt, bold)
    total = 0
    for paragraph in (text or "").split("\n"):
        lines, line_w = 1, 0.0
        for word in paragraph.split():
            for idx, chunk in enumerate(_break_chunks(word)):
                chunk_w = text_width_emu(chunk, size_pt, bold)
                gap = space if (idx == 0 and line_w > 0) else 0.0
                if line_w > 0 and line_w + gap + chunk_w > width_emu:
                    lines += 1
                    line_w, gap = 0.0, 0.0
                if line_w == 0 and chunk_w > width_emu:
                    # Too wide for any line, so PowerPoint breaks it between
                    # characters; the last piece stays on the current line.
                    for ch in chunk:
                        ch_w = _glyph_emu(ch, size_pt, bold)
                        if line_w > 0 and line_w + ch_w > width_emu:
                            lines += 1
                            line_w = 0.0
                        line_w += ch_w
                else:
                    line_w += gap + chunk_w
        total += lines
    return max(total, 1)



def longest_unbreakable_emu(text: str, size_pt: float, bold: bool) -> float:
    widest = 0.0
    for word in (text or "").split():
        for chunk in _break_chunks(word):
            widest = max(widest, text_width_emu(chunk, size_pt, bold))
    return widest



# -------------------------
# PowerPoint slide decks: table rows
# -------------------------
def _tc_text(tc) -> str:
    return "".join(t.text or "" for t in tc.iter(qn("a:t")))



def _set_text_body(tx_body, text: str) -> None:
    """
    Puts text in a text body's first run, so it keeps that run's font, size and
    colour. Assigning .text instead would reset the run to the default style.
    """
    paragraphs = tx_body.findall(qn("a:p"))
    first = paragraphs[0]
    for extra in paragraphs[1:]:
        tx_body.remove(extra)
    runs = first.findall(qn("a:r"))
    for extra in first.findall(qn("a:br")) + first.findall(qn("a:fld")) + runs[1:]:
        first.remove(extra)
    if runs:
        runs[0].find(qn("a:t")).text = text
        return
    run = first.makeelement(qn("a:r"), {})
    end_props = first.find(qn("a:endParaRPr"))
    if end_props is not None:
        run_props = deepcopy(end_props)
        run_props.tag = qn("a:rPr")
        run.append(run_props)
    t = run.makeelement(qn("a:t"), {})
    t.text = text
    run.append(t)
    if end_props is not None:
        end_props.addprevious(run)
    else:
        first.append(run)



def _set_paragraph_text(paragraph, text: str) -> None:
    runs = paragraph.runs
    if runs:
        runs[0].text = text
        for extra in runs[1:]:
            extra._r.getparent().remove(extra._r)
    else:
        paragraph.text = text



def _tr_is_shaded(tr) -> bool:
    for tc in tr.tc_lst:
        tc_pr = tc.find(qn("a:tcPr"))
        if tc_pr is not None and tc_pr.find(qn("a:solidFill")) is not None:
            return True
    return False



def _tr_without_shading(tr):
    """The same row with its cell fills switched off: the white band."""
    plain = deepcopy(tr)
    for tc in plain.tc_lst:
        tc_pr = tc.find(qn("a:tcPr"))
        fill = tc_pr.find(qn("a:solidFill")) if tc_pr is not None else None
        if fill is not None:
            fill.addprevious(tc_pr.makeelement(qn("a:noFill"), {}))
            tc_pr.remove(fill)
    return plain



def _set_side_margins(tr, margin_emu: int) -> None:
    """Gives every cell in the row at least margin_emu on the left and right."""
    for tc in tr.tc_lst:
        tc_pr = tc.find(qn("a:tcPr"))
        if tc_pr is None:
            tc_pr = tc.makeelement(qn("a:tcPr"), {})
            tc.append(tc_pr)
        for name in ("marL", "marR"):
            if int(tc_pr.get(name, "91440")) < margin_emu:
                tc_pr.set(name, str(margin_emu))



def table_row_donors(tbl):
    """
    Copies of the template table's header row, shaded body row, white body row
    and Total row. Every finished deck bands its loan rows shaded, white,
    shaded..., and keeps the template's own grey Total row with its column
    pattern of bold and left-aligned cells, so one donor row would not do. The
    Bridge template has no white row, so one is made from the shaded row.
    """
    rows = list(tbl.tr_lst)
    total_idx = next(
        (i for i in range(len(rows) - 1, 0, -1) if _tc_text(rows[i].tc_lst[0]).strip().lower() == "total"),
        None,
    )
    body = rows[1:total_idx] if total_idx else rows[1:]
    shaded = next((tr for tr in body if _tr_is_shaded(tr)), None)
    plain = next((tr for tr in body if not _tr_is_shaded(tr)), None)
    if shaded is None and plain is None:
        raise ValueError("The slide template's table has no sample loan row to copy.")
    shaded = deepcopy(shaded if shaded is not None else plain)
    plain = deepcopy(plain) if plain is not None else _tr_without_shading(shaded)
    total = deepcopy(rows[total_idx]) if total_idx else deepcopy(shaded)
    return deepcopy(rows[0]), shaded, plain, total



def _tc_font(tc):
    """(size in points, bold) of a table cell's first run."""
    run_props = tc.find(".//" + qn("a:rPr"))
    if run_props is None:
        run_props = tc.find(".//" + qn("a:endParaRPr"))
    if run_props is None:
        return 18.0, False
    return int(run_props.get("sz", "1800")) / 100.0, run_props.get("b") in ("1", "true")



def _tc_margins(tc):
    """(left, right, top, bottom) cell margins in EMU, with DrawingML's defaults."""
    tc_pr = tc.find(qn("a:tcPr"))

    def margin(name, default):
        return int(tc_pr.get(name, default)) if tc_pr is not None else default

    return margin("marL", 91440), margin("marR", 91440), margin("marT", 45720), margin("marB", 45720)



def _row_cells(donor_tr, texts, min_height: int):
    """One table row as (text, size_pt, bold, margin_left, margin_right, vertical_padding, min_height) per cell."""
    cells = []
    for tc, text in zip(donor_tr.tc_lst, texts):
        size, bold = _tc_font(tc)
        mar_l, mar_r, mar_t, mar_b = _tc_margins(tc)
        cells.append((text, size, bold, mar_l, mar_r, mar_t + mar_b, min_height))
    return cells



def _cell_height_emu(cell, width: int) -> float:
    text, size, bold, mar_l, mar_r, pad, _min_height = cell
    lines = text_line_count(text, int(width - mar_l - mar_r), size, bold)
    return lines * size * PPT_LINE_HEIGHT_PER_PT * EMU_PER_PT + pad



def row_height_emu(cells, widths) -> int:
    """The height PowerPoint gives a row: its tallest cell, or the row's stored minimum."""
    tallest = max(_cell_height_emu(cell, width) for cell, width in zip(cells, widths))
    return int(max(cells[0][6], tallest) + 0.5)



def balance_column_widths(widths, grid):
    """
    Re-splits a table's fixed total width between its columns, as the analysts
    do by hand on almost every deck. grid holds every row that will appear
    (header, loans, Total) in _row_cells() form.

    No column ends up narrower than its widest figure or word, so values like
    "$11,127,500" stay on one line. Then, a round at a time, the column that
    saves the most table height for the least extra width is widened, paid for
    by columns that can give width up without growing any row.
    """
    widths = [int(w) for w in widths]
    n_cols, n_rows = len(widths), len(grid)
    if not grid or n_cols == 0:
        return widths
    need = [0] * n_cols
    for row in grid:
        for c, (text, size, bold, mar_l, mar_r, _pad, _min_h) in enumerate(row[:n_cols]):
            fit = longest_unbreakable_emu(text, size, bold) + mar_l + mar_r + COLUMN_FIT_SAFETY_EMU
            need[c] = max(need[c], int(fit) + 1)
    if sum(need) > sum(widths):
        return widths  # cannot fit every value; keep the template's grid

    # 1) Lift every column to its minimum, paid for by the columns with room.
    deficit = sum(max(0, need[c] - widths[c]) for c in range(n_cols))
    widths = [max(widths[c], need[c]) for c in range(n_cols)]
    while deficit > 0:
        slack = [widths[c] - need[c] for c in range(n_cols)]
        total_slack = sum(s for s in slack if s > 0)
        if total_slack <= 0:
            break
        for c in range(n_cols):
            if deficit <= 0:
                break
            if slack[c] > 0:
                take = min(slack[c], deficit, max(1, round(deficit * slack[c] / total_slack)))
                widths[c] -= take
                deficit -= take

    # 2) Trade width for height. Every row's height only falls as a column
    # widens, which is what makes the binary searches below valid.
    floor = [max(cell[6] for cell in row) for row in grid]
    cache: dict = {}

    def column(c, width):
        key = (c, width)
        if key not in cache:
            cache[key] = [_cell_height_emu(grid[r][c], width) for r in range(n_rows)]
        return cache[key]

    def tops_of(cols):
        tops = []
        for r in range(n_rows):
            first, second, owner = 0.0, 0.0, -1
            for c in range(n_cols):
                value = cols[c][r]
                if value > first:
                    first, second, owner = value, first, c
                elif value > second:
                    second = value
            tops.append((first, second, owner))
        return tops

    def height_with(tops, c, col):
        return sum(
            max(floor[r], col[r], tops[r][1] if tops[r][2] == c else tops[r][0]) for r in range(n_rows)
        )

    step = COLUMN_WIDTH_STEP_EMU
    lookahead = 80  # widen a column by up to 40pt to save a line
    for _round in range(COLUMN_WIDTH_MAX_ROUNDS):
        cols = [column(c, widths[c]) for c in range(n_cols)]
        tops = tops_of(cols)
        current = sum(max(floor[r], tops[r][0]) for r in range(n_rows))
        offers = []
        for c in range(n_cols):
            if height_with(tops, c, column(c, widths[c] + lookahead * step)) >= current:
                continue
            low, high = 1, lookahead
            while low < high:
                mid = (low + high) // 2
                if height_with(tops, c, column(c, widths[c] + mid * step)) < current:
                    high = mid
                else:
                    low = mid + 1
            gain = current - height_with(tops, c, column(c, widths[c] + low * step))
            offers.append((gain / low, -c, c, low))

        moved = False
        for _ratio, _tie, receiver, k in sorted(offers, reverse=True):
            trial = list(widths)
            trial[receiver] += k * step
            trial_cols = [column(c, trial[c]) for c in range(n_cols)]
            trial_tops = tops_of(trial_cols)
            base = sum(max(floor[r], trial_tops[r][0]) for r in range(n_rows))
            # Width each other column can give up without growing any row:
            # first what re-wraps none of its own cells, then what only
            # re-wraps cells in rows that something else already makes taller.
            # Taking the first kind first keeps short values such as
            # "Term Loan" on one line.
            tidy, free = {}, {}
            for c in range(n_cols):
                if c == receiver:
                    continue
                limit = max(0, (trial[c] - need[c]) // step)
                own = trial_cols[c]

                def largest_cut(fits):
                    low, high = 0, limit
                    while low < high:
                        mid = (low + high + 1) // 2
                        if fits(column(c, trial[c] - mid * step)):
                            low = mid
                        else:
                            high = mid - 1
                    return low

                loose = largest_cut(lambda col: height_with(trial_tops, c, col) <= base)
                if loose:
                    free[c] = loose
                    tidy[c] = min(loose, largest_cut(lambda col: col == own))
            if sum(free.values()) < k:
                continue
            owed, taken = k, {}
            for pool in (tidy, free):
                for c in sorted(pool, key=lambda d: (-(pool[d] - taken.get(d, 0)), d)):
                    take = min(pool[c] - taken.get(c, 0), owed)
                    if take <= 0:
                        continue
                    trial[c] -= take * step
                    taken[c] = taken.get(c, 0) + take
                    owed -= take
                    if owed == 0:
                        break
                if owed == 0:
                    break
            widths = trial
            moved = True
            break
        if not moved:
            break
    return widths



def paginate_rows(body_heights, header_height: int, total_height: int, first_budget: int, later_budget: int):
    """
    Splits loan rows into slides by rendered height. Every slide repeats the
    header; the Total row closes the last slide and never sits there alone.
    """
    pages, current, used, budget = [], [], header_height, first_budget
    for idx, height in enumerate(body_heights):
        if current and used + height > budget:
            pages.append(current)
            current, used, budget = [], header_height, later_budget
        current.append(idx)
        used += height
    if current and used + total_height > budget and len(current) > 1:
        moved = current.pop()
        pages.append(current)
        current = [moved]
    pages.append(current)

    # Don't leave one or two loans stranded with the Total on a slide of
    # their own: pull rows down from the slide before while they still fit.
    if len(pages) > 1:
        last, before = pages[-1], pages[-2]
        last_used = header_height + total_height + sum(body_heights[i] for i in last)
        while (
            len(last) < SLIDE_MIN_LAST_PAGE_ROWS
            and len(before) > SLIDE_MIN_LAST_PAGE_ROWS
            and last_used + body_heights[before[-1]] <= later_budget
        ):
            last_used += body_heights[before[-1]]
            last.insert(0, before.pop())
    return pages



# -------------------------
# PowerPoint slide decks: slide text
# -------------------------
_NAME_NICKNAME_RX = re.compile(r"\s[\"\u201c'\u2018][^\s\"\u201d'\u2019]+[\"\u201d'\u2019](?=\s)")
_NAME_SUFFIX_RX = re.compile(r",?\s+(?:jr|sr)\.?$", re.IGNORECASE)
_NAME_ROMAN_RX = re.compile(r"^(?:i{1,3}|iv|vi{0,3})$", re.IGNORECASE)
_NAME_ENTITY_TAIL_RX = re.compile(r"^(?:l\.?l\.?c\.?|inc\.?|l\.?p\.?|l\.?l\.?p\.?|ltd\.?|corp\.?|co\.?)$", re.IGNORECASE)
# Pieces after a comma that belong to the name before them: "Smith, Jr.", "Doe, III".
_NAME_SUFFIX_PIECE_RX = re.compile(r"^(?:jr|sr)\.?$|^(?:i{1,3}|iv|vi{0,3})$", re.IGNORECASE)
# Abbreviations that stay in capitals when an ALL-CAPS name is title-cased.
_NAME_KEEP_UPPER_RX = re.compile(r"^(?:llc|lp|llp|lllp|reit|(?:[a-z]\.){2,})$", re.IGNORECASE)


def _title_word(word: str) -> str:
    """'O'NEAL' -> 'O'Neal', 'SMITH-JONES' -> 'Smith-Jones', '(PERSONAL)' -> '(Personal)'."""
    return "-".join(
        "'".join(re.sub(r"[a-z]", lambda m: m.group(0).upper(), piece.lower(), count=1) for piece in part.split("'"))
        for part in word.split("-")
    )



def intro_person_name(raw: Any) -> str:
    """
    A guarantor as the decks' intro line writes them: nicknames in quotes and
    Sr./Jr. dropped, initials written with a period dropped ("Alex W. Severyn"
    -> "Alex Severyn"), and ALL-CAPS names title-cased.
    """
    text = clean_guarantor_name(raw)
    if text.lower() in ("", "-", "n/a", "na", "none"):
        return ""
    text = _NAME_NICKNAME_RX.sub(" ", f" {text} ")
    text = _NAME_SUFFIX_RX.sub("", re.sub(r"\s+", " ", text).strip())
    text = re.sub(r",\s+(?=(?:i{1,3}|iv|vi{0,3})$)", " ", text, flags=re.IGNORECASE)
    parts = text.split(" ")
    if len(parts) > 2:
        parts = [parts[0]] + [p for p in parts[1:-1] if not re.fullmatch(r"[A-Za-z]\.", p)] + [parts[-1]]
    if text.upper() == text and any(ch.isalpha() for ch in text):
        parts = [
            p.upper() if _NAME_ROMAN_RX.match(p) or _NAME_KEEP_UPPER_RX.match(p.rstrip(",")) else _title_word(p)
            for p in parts
        ]
    return " ".join(parts)



def guarantor_intro_names(guarantor_cells) -> list[str]:
    """Distinct guarantors in order of first appearance down the table."""
    names, seen = [], set()
    for cell in guarantor_cells:
        pieces = []
        for piece in re.split(r"[,;]", str(cell or "")):
            # "Omega Holdings, LLC" and "John Smith, Jr." are one guarantor, not two.
            if pieces and (_NAME_ENTITY_TAIL_RX.match(piece.strip()) or _NAME_SUFFIX_PIECE_RX.match(piece.strip())):
                pieces[-1] = f"{pieces[-1]}, {piece.strip()}"
            else:
                pieces.append(piece)
        for piece in pieces:
            name = intro_person_name(piece)
            if not name:
                continue
            tokens = [t for t in name.split() if not _NAME_ROMAN_RX.match(t)] or name.split()
            key = (tokens[0].casefold(), tokens[-1].casefold())
            if key not in seen:
                seen.add(key)
                names.append(name)
    return names



def join_intro_names(names) -> str:
    """'A', 'A and B' (or 'A and B Surname' for a shared surname), 'A, B, and C'."""
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        first, second = names[0].split(), names[1].split()
        if len(first) > 1 and len(second) > 1 and first[-1].casefold() == second[-1].casefold():
            return f"{' '.join(first[:-1])} and {names[1]}"
        return f"{names[0]} and {names[1]}"
    if len(names) > 2:
        return ", ".join(names[:-1]) + ", and " + names[-1]
    return ""



def term_intro_text(guarantor_cells) -> str:
    names = join_intro_names(guarantor_intro_names(guarantor_cells))
    return f"Asset Management review for term loans with {names}:" if names else "Asset Management review for term loans:"



def _number_or_none(value: Any):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return None if pd.isna(value) else float(value)



def bridge_loan_status(record: dict, today: date | None = None) -> str:
    """'current' while a Bridge line has a balance or active assets (or was never drawn and is open), else 'prior'."""
    if (_number_or_none(record.get("outstandingbalance")) or 0) > 0 or (_number_or_none(record.get("activeassets")) or 0) > 0:
        return "current"
    if (_number_or_none(record.get("totalfundedassets")) or 0) > 0 or (_number_or_none(record.get("lifetimefunded")) or 0) > 0:
        return "prior"
    maturity = record.get("linematuritydate")
    if isinstance(maturity, datetime):
        maturity = maturity.date()
    if isinstance(maturity, date) and maturity < (today or date.today()):
        return "prior"
    return "current"



def bridge_intro_text(statuses) -> str:
    count, current = len(statuses), list(statuses).count("current")
    if current and current < count:
        label = "current and prior Bridge loans"
    elif current:
        label = "current Bridge loan" if count == 1 else "current Bridge loans"
    else:
        label = "prior Bridge loan" if count == 1 else "prior Bridge loans"
    return f"Asset Management review for {label}:"



def bridge_performance_text(statuses) -> str:
    count, current = len(statuses), list(statuses).count("current")
    if current and current < count:
        return "Bridge loans have been performing as expected"
    if current:
        return "Bridge loan is performing as expected" if count == 1 else "Bridge loans are performing as expected"
    return "Bridge loan performed as expected" if count == 1 else "Bridge loans performed as expected"



def default_slide_intros(workbook_bytes: bytes) -> dict:
    """The intro line each deck would carry, from the workbook's loan rows."""
    intros = {}
    term = read_sheet_for_slides(workbook_bytes, TERM_SHEET)
    if term and term[1]:
        intros[TERM_SHEET] = term_intro_text(r.get("guarantor") for r in sheet_records_by_key(term))
    bridge = read_sheet_for_slides(workbook_bytes, BRIDGE_SHEET)
    if bridge and bridge[1]:
        intros[BRIDGE_SHEET] = bridge_intro_text([bridge_loan_status(r) for r in sheet_records_by_key(bridge)])
    return intros



def term_loans_paid_late(workbook_bytes: bytes) -> list[str]:
    """Loans with a balance whose on-time payment rate is under 100%, against the commentary's 'no late or DQ payments'."""
    term = read_sheet_for_slides(workbook_bytes, TERM_SHEET)
    if not term:
        return []
    late = []
    for record in sheet_records_by_key(term):
        on_time = _number_or_none(record.get("historicalontimepayment%"))
        balance = _number_or_none(record.get("outstandingbalance"))
        if on_time is not None and on_time < 0.995 and (balance or 0) > 0:
            late.append(f"{record.get('loanid')} ({format_slide_value(on_time, '0%')})")
    return late



# -------------------------
# PowerPoint slide decks: building the decks
# -------------------------
def slide_template_names(kind: str):
    return TERM_PPTX_TEMPLATE_NAMES if kind == TERM_SHEET else BRIDGE_PPTX_TEMPLATE_NAMES



def slide_deck_file_name(kind: str, name: str, made_on: date) -> str:
    """Named the way the AM team files them: 'Omega Realty Group AM Term Slide 10.19.2026.pptx'."""
    return f"{sanitize_filename(name)} AM {kind} Slide {made_on.month}.{made_on.day}.{made_on.year}.pptx"



def _placeholder(slide, idx: int):
    for shape in slide.placeholders:
        if shape.placeholder_format.idx == idx:
            return shape
    return None



def _commentary_shape(slide):
    """The commentary placeholder. Bridge templates also hold an empty text box of the same name."""
    shape = _placeholder(slide, 1)
    return shape if shape is not None and shape.has_text_frame and shape.text_frame.text.strip() else None



def _legend_shape(slide, slide_width: int):
    for shape in slide.placeholders:
        if shape.has_text_frame and shape.text_frame.text.strip().lower().startswith("chart legend"):
            return shape if shape.left < slide_width else None
    return None



def _commentary_height_emu(shape) -> int:
    """Height of the commentary's visible text plus room for COMMENTARY_SPARE_LINES more bullets."""
    inner = max(1, int(shape.width) - 3 * 91440)
    lines = 0
    for paragraph in shape.text_frame.paragraphs:
        text = "".join(run.text for run in paragraph.runs).strip()
        if text:
            lines += text_line_count(text, inner, 12.0, False)
    lines = max(lines, 1) + COMMENTARY_SPARE_LINES
    return COMMENTARY_INSET_EMU + COMMENTARY_FIRST_LINE_EMU + COMMENTARY_NEXT_LINE_EMU * (lines - 1)



def _set_intro_text(shape, text: str) -> int:
    """
    Writes the intro line and grows its box, as PowerPoint's own shape autofit
    would, when the names run past one line. Returns the EMU added, which the
    table below has to move down by.
    """
    _set_text_body(shape.text_frame._txBody, text)
    run_props = shape.text_frame._txBody.find(".//" + qn("a:rPr"))
    size = int(run_props.get("sz")) / 100.0 if run_props is not None and run_props.get("sz") else INTRO_FONT_PT
    inner = max(1, int(shape.width) - 2 * 45720)
    extra_lines = text_line_count(text, inner, size, False) - 1
    if extra_lines <= 0:
        return 0
    grow = int(extra_lines * size * PPT_LINE_HEIGHT_PER_PT * EMU_PER_PT)
    _pin_placeholder_geometry(shape)
    shape.height = int(shape.height) + grow
    return grow



def _pin_placeholder_geometry(shape) -> None:
    """
    Copies a placeholder's position and size from its layout onto the slide.
    A placeholder that inherits them has no xfrm of its own, and setting just
    one dimension would create one with every other value zero.
    """
    left, top, width, height = shape.left, shape.top, shape.width, shape.height
    shape.left, shape.top, shape.width, shape.height = left, top, width, height



def _find_layout(prs, name: str):
    for master in prs.slide_masters:
        for layout in master.slide_layouts:
            if layout.name.strip().lower() == name.lower():
                return layout
    return None



def _add_table_slide(prs, frame_xml, layout):
    """A new slide holding only a copy of the template's table."""
    slide = prs.slides.add_slide(layout)
    for placeholder in list(slide.placeholders):
        placeholder._element.getparent().remove(placeholder._element)
    frame = deepcopy(frame_xml)
    frame.nvGraphicFramePr.cNvPr.id = slide.shapes._next_shape_id
    slide.shapes._spTree.insert_element_before(frame, "p:extLst")
    return next(s for s in slide.shapes if getattr(s, "has_table", False) and s.has_table)



def _fill_table(tbl, rows) -> int:
    """Replaces every row of tbl with rows = [(donor_tr, texts, height_emu)]; returns the table height."""
    for tr in list(tbl.tr_lst):
        tbl.remove(tr)
    ext = tbl.find(qn("a:extLst"))
    total_height = 0
    for donor, texts, height in rows:
        tr = deepcopy(donor)
        tr.set("h", str(int(height)))
        for tc, text in zip(tr.tc_lst, texts):
            _set_text_body(tc.find(qn("a:txBody")), text)
        if ext is not None:
            ext.addprevious(tr)
        else:
            tbl.append(tr)
        total_height += int(height)
    return total_height



def _strip_template_extras(prs) -> None:
    """
    Drops what the template would otherwise pass to every deck: the preview
    picture, which still shows the template's sample rows, PowerPoint's
    co-authoring log of who edited the template, and its list of commenters
    when no slide carries a comment.
    """
    try:
        package_rels = prs.part.package._rels
        for r_id, rel in list(package_rels.items()):
            if rel.reltype == PPTX_RT.THUMBNAIL:
                package_rels.pop(r_id)
        has_comments = any(
            rel.reltype == PPTX_RT.COMMENTS for slide in prs.slides for rel in slide.part.rels.values()
        )
        for r_id, rel in list(prs.part.rels.items()):
            if rel.reltype.endswith(("/changesInfo", "/revisionInfo")) or (
                rel.reltype == PPTX_RT.COMMENT_AUTHORS and not has_comments
            ):
                prs.part.drop_rel(r_id)
    except Exception:
        pass  # only these optional parts are at stake



def _map_slide_columns(slide_headers, wb_headers):
    """
    The workbook column behind each slide column, matched by header name, and
    the header text each slide column should show. Occupancy quarters pair up
    in order and take the workbook's labels, since the template's are stale.
    """
    by_key = {slide_header_key(text): col for col, text in wb_headers.items()}
    wb_occupancy = [col for col, text in wb_headers.items() if parse_occ_header(text)]
    source_cols, labels, occupancy_seen = [], [], 0
    for text in slide_headers:
        if parse_occ_header(text):
            col = wb_occupancy[occupancy_seen] if occupancy_seen < len(wb_occupancy) else None
            occupancy_seen += 1
            source_cols.append(col)
            labels.append(wb_headers[col] if col else text)
        else:
            source_cols.append(by_key.get(slide_header_key(text)))
            labels.append(text)
    return source_cols, labels



def build_slide_deck(kind: str, template_bytes: bytes, workbook_bytes: bytes, title_name: str, intro: str | None = None):
    """
    Fills the Term or Bridge slide template from the finished workbook, so the
    slides show exactly what the Excel sheet shows. Returns the Presentation,
    or None when that sheet has no loans.
    """
    sheet = read_sheet_for_slides(workbook_bytes, kind)
    if sheet is None or not sheet[1]:
        return None
    wb_headers, wb_rows, wb_total = sheet

    prs = Presentation(io.BytesIO(template_bytes))
    slide = prs.slides[0]
    table_shape = next((s for s in slide.shapes if getattr(s, "has_table", False) and s.has_table), None)
    if table_shape is None:
        raise ValueError(f"The {kind} slide template has no table.")
    tbl = table_shape.table._tbl
    header_tr, shaded_tr, plain_tr, total_tr = table_row_donors(tbl)
    for donor in (header_tr, shaded_tr, plain_tr, total_tr):
        _set_side_margins(donor, CELL_SIDE_MARGIN_EMU)
    pristine_frame = deepcopy(table_shape._element)
    records = sheet_records_by_key(sheet)

    source_cols, header_texts = _map_slide_columns([_tc_text(tc) for tc in header_tr.tc_lst], wb_headers)

    def texts_for(row, blank):
        return [format_slide_value(*row[col], blank=blank) if col in row else blank for col in source_cols]

    body_texts = [texts_for(row, BLANK_CELL_PLACEHOLDER) for row in wb_rows]
    total_texts = texts_for(wb_total, SLIDE_TOTAL_BLANK) if wb_total else None
    # Bands run shaded, white, shaded... down the whole list, across slides.
    body_donors = [shaded_tr if i % 2 == 0 else plain_tr for i in range(len(body_texts))]

    # The template's sample rows are taller than one loan needs, so the
    # shorter of them is the floor for every loan row.
    min_body = min(int(shaded_tr.get("h", "0")), int(plain_tr.get("h", "0")))
    grid = [_row_cells(header_tr, header_texts, int(header_tr.get("h", "0")))]
    grid += [_row_cells(d, t, min_body) for d, t in zip(body_donors, body_texts)]
    if total_texts:
        grid.append(_row_cells(total_tr, total_texts, int(total_tr.get("h", "0"))))
    widths = balance_column_widths([int(g.get("w")) for g in tbl.tblGrid.findall(qn("a:gridCol"))], grid)
    for frame_tbl in (tbl, pristine_frame.find(".//" + qn("a:tbl"))):
        for grid_col, width in zip(frame_tbl.tblGrid.findall(qn("a:gridCol")), widths):
            grid_col.set("w", str(width))
    heights = [row_height_emu(cells, widths) for cells in grid]
    header_h, body_h = heights[0], heights[1 : 1 + len(body_texts)]
    total_h = heights[-1] if total_texts else 0

    title = slide.shapes.title
    if title is not None:
        heading = TERM_DECK_TITLE if kind == TERM_SHEET else BRIDGE_DECK_TITLE
        _set_text_body(title.text_frame._txBody, f"{heading} \u2013 {str(title_name or '').strip()}")
    table_top = SLIDE_TABLE_TOP_EMU
    intro_text = (intro or "").strip()
    if not intro_text:
        # A cleared box must not leave the template's sample line behind.
        if kind == TERM_SHEET:
            intro_text = term_intro_text([])
        else:
            intro_text = bridge_intro_text([bridge_loan_status(r) for r in records])
    intro_shape = _placeholder(slide, 24)
    if intro_shape is not None and intro_shape.has_text_frame:
        table_top += _set_intro_text(intro_shape, intro_text)

    # Slide 0 keeps the commentary below the table and, on Term, clear of the
    # chart legend; continuation slides carry the table alone.
    commentary = _commentary_shape(slide)
    legend = _legend_shape(slide, prs.slide_width)
    text_floor = SLIDE_TEXT_FLOOR_EMU
    if legend is not None:
        legend_text_top = legend.top + (legend.height - LEGEND_TEXT_HEIGHT_EMU) // 2
        text_floor = min(text_floor, legend_text_top - LEGEND_CLEARANCE_EMU)
    table_floor = text_floor
    if commentary is not None:
        table_floor -= COMMENTARY_GAP_EMU + _commentary_height_emu(commentary)
    pages = paginate_rows(
        body_h,
        header_h,
        total_h,
        table_floor - table_top,
        CONT_SLIDE_TABLE_FLOOR_EMU - CONT_SLIDE_TABLE_TOP_EMU,
    )

    blank_layout = _find_layout(prs, "Blank") or slide.slide_layout
    for page_no, page in enumerate(pages):
        rows = [(header_tr, header_texts, header_h)]
        rows += [(body_donors[i], body_texts[i], body_h[i]) for i in page]
        if page_no == len(pages) - 1 and total_texts:
            rows.append((total_tr, total_texts, total_h))
        if page_no == 0:
            shape = table_shape
            shape.top = table_top
        else:
            shape = _add_table_slide(prs, pristine_frame, blank_layout)
            shape.top = CONT_SLIDE_TABLE_TOP_EMU
        shape.left = SLIDE_TABLE_LEFT_EMU
        shape.height = _fill_table(shape.table._tbl, rows)
        if page_no == 0 and commentary is not None:
            _pin_placeholder_geometry(commentary)
            commentary.left = SLIDE_TABLE_LEFT_EMU  # bullets line up with the table
            commentary.top = shape.top + shape.height + COMMENTARY_GAP_EMU
            # Stretch the box to the floor so bullets the analyst adds keep full size.
            commentary.height = max(commentary.height, text_floor - commentary.top)

    if kind == BRIDGE_SHEET and commentary is not None:
        statuses = [bridge_loan_status(r) for r in records]
        for paragraph in commentary.text_frame.paragraphs:
            if re.match(r"^\s*bridge loans?\b.*\bas expected\b", paragraph.text, re.IGNORECASE):
                _set_paragraph_text(paragraph, bridge_performance_text(statuses))

    _strip_template_extras(prs)
    return prs



def build_slide_deck_bytes(kind: str, workbook_bytes: bytes, title_name: str, intro: str | None, made_on: date):
    """(pptx bytes, file name) for one deck, or None when the workbook has no loans of that kind."""
    if Presentation is None:
        raise RuntimeError("PowerPoint output needs the python-pptx package (pip install python-pptx).")
    template_bytes = resolve_repo_file(slide_template_names(kind)).read_bytes()
    prs = build_slide_deck(kind, template_bytes, workbook_bytes, title_name, intro)
    if prs is None:
        return None
    output = io.BytesIO()
    prs.save(output)
    return output.getvalue(), slide_deck_file_name(kind, title_name, made_on)



@st.cache_data(show_spinner=False, max_entries=8)
def slide_deck_download(kind: str, workbook_bytes: bytes, title_name: str, intro: str, made_on: date):
    """build_slide_deck_bytes, kept across reruns so typing in the slide text box rebuilds only that deck."""
    return build_slide_deck_bytes(kind, workbook_bytes, title_name, intro, made_on)



def format_preview(df: pd.DataFrame, occ_headers) -> pd.DataFrame:
    preview = df.copy()
    occ_cols = [item["header"] for item in occ_headers] if occ_headers else []
    for column in occ_cols:
        if column in preview.columns:
            preview[column] = preview[column].apply(
                lambda x: x if isinstance(x, str) else ("" if pd.isna(x) else f"{x:.1%}")
            )
    if "Historical Ontime % Dec" in preview.columns:
        preview["Historical Ontime % Dec"] = preview["Historical Ontime % Dec"].apply(
            lambda x: "" if pd.isna(x) else f"{x:.0%}"
        )
    if "Occupancy Matched" in preview.columns:
        preview["Occupancy Matched"] = preview["Occupancy Matched"].map({True: "Yes", False: "No"})
    return preview


# -------------------------
# Streamlit UI
# -------------------------
st.set_page_config(page_title="AM Slides Builder", layout="wide")
# The slide text boxes sit below several st.stop() gates. A run that stops
# before reaching them would otherwise drop what they hold, and the decks
# would lose the analyst's edits; re-assigning the keys keeps them.
for deck_text_key in ("deck_title_name", "deck_term_intro", "deck_bridge_intro"):
    if deck_text_key in st.session_state:
        st.session_state[deck_text_key] = st.session_state[deck_text_key]
st.title("AM Slides Builder")
st.caption(
    "Build the AM Slides workbook and the Term and Bridge PowerPoint slides from Salesforce plus "
    "a Berkadia Financial Analysis export, a Midland CAF extract bundle, or both."
)

oauth_config = None
oauth_setup_error = None
try:
    oauth_config = load_salesforce_oauth_config()
    maybe_finish_salesforce_oauth(oauth_config)
except Exception as exc:
    oauth_setup_error = str(exc)

sf = None
if oauth_setup_error is None:
    try:
        sf = get_salesforce_client_from_session()
    except Exception as exc:
        oauth_setup_error = str(exc)

template_error = None
template_path = None
try:
    template_path = resolve_repo_template_path()
except Exception as exc:
    template_error = str(exc)

with st.sidebar:
    st.header("App status")
    if template_error:
        st.error(template_error)
    elif template_path is not None:
        st.success("Template found in repository")
        st.caption(template_path.name)
    if Presentation is None:
        st.warning("PowerPoint output is off: install python-pptx to get the slide decks.")
    else:
        for deck_kind in (TERM_SHEET, BRIDGE_SHEET):
            try:
                st.caption(f"{deck_kind} slide template: {resolve_repo_file(slide_template_names(deck_kind)).name}")
            except RuntimeError:
                st.warning(f"{deck_kind} slide template missing: add {slide_template_names(deck_kind)[0]} to the repository.")

    st.divider()
    st.header("Salesforce")
    if oauth_setup_error:
        st.error(oauth_setup_error)
    elif sf is None:
        st.info("Not connected")
    else:
        auth_data = st.session_state.get("salesforce_auth", {})
        st.success("Connected")
        if auth_data.get("instance_url"):
            st.caption(auth_data["instance_url"])
        if st.button("Log out of Salesforce", use_container_width=True):
            clear_salesforce_session()
            st.rerun()

if template_error:
    st.error(template_error)
    st.stop()

st.subheader("Step 1: Log in to Salesforce")
if oauth_setup_error:
    st.error(oauth_setup_error)
    st.stop()

if sf is None:
    st.info("Log in first. After you sign in, the app will let you upload the Berkadia file and build the AM slide.")
    st.link_button(
        "Log in to Salesforce",
        build_salesforce_login_url(oauth_config),
        use_container_width=False,
    )
    st.caption(f"Callback URL: {oauth_config['redirect_uri']}")
    st.stop()

st.success("Salesforce login complete.")

# Read the dated occupancy quarter columns the Term template expects.
template_bytes = load_template_bytes()
try:
    occ_headers = read_template_term_occ_headers(template_bytes)
except Exception as exc:
    occ_headers = []
    st.warning(f"Could not read occupancy headers from the template: {exc}")

st.session_state["occ_headers"] = occ_headers
target_quarters = tuple(item["quarter_key"] for item in occ_headers)

st.subheader("Step 2: Upload the servicer occupancy files")
st.caption("The AM template workbook is loaded automatically from the repository. You do not need to upload it here.")
if occ_headers:
    st.caption("Template occupancy quarters: " + ", ".join(target_quarters))
else:
    st.caption("The Term template has no dated occupancy columns (e.g. '2024 Q1 Occ%'), so occupancy will be left blank.")
st.caption(
    "Upload whichever servicers cover this account. Both feed the same quarter columns; where the two "
    "overlap on a loan and quarter, Berkadia is used."
)

berkadia_file = upload_berkadia_workbook()

midland_bundle_bytes = upload_caf_folder(
    target_quarters, parse_caf_bundle_date, pick_caf_bundles_for_quarters
)
berkadia_tape_bytes = [berkadia_file.read_bytes()] if berkadia_file is not None else []

if not berkadia_tape_bytes and not midland_bundle_bytes:
    st.info(
        "Upload the Berkadia workbook, select the Midland folder, or add both to continue."
    )
    st.stop()

berkadia_lookup = {}
occupancy_selected = pd.DataFrame()
occupancy_issues = pd.DataFrame()
fa_row_count = 0
if berkadia_tape_bytes:
    try:
        tape_count = len(berkadia_tape_bytes)
        tape_noun = "tape" if tape_count == 1 else "tapes"
        with st.spinner(f"Reading {tape_count} Berkadia {tape_noun}..."):
            berkadia_lookup, occupancy_selected, occupancy_issues, fa_row_count = (
                load_berkadia_occupancy_lookup_cached(
                    tuple(berkadia_tape_bytes),
                    target_quarters,
                )
            )
        st.success(
            f"{tape_count} Berkadia {tape_noun} loaded. Financial Analysis rows read: "
            f"{fa_row_count}. Selected occupancy rows: {len(occupancy_selected)}."
        )
        if target_quarters:
            berkadia_missing = [
                q for q in target_quarters if not any(key[1] == q for key in berkadia_lookup)
            ]
            if berkadia_missing and berkadia_lookup:
                st.warning(
                    "No Berkadia occupancy for " + ", ".join(berkadia_missing) + "."
                )
    except Exception as exc:
        st.error(f"Berkadia tape: {exc}")
        st.stop()

midland_lookup = {}
midland_selected = pd.DataFrame()
midland_issues = pd.DataFrame()
midland_rows_read = 0
if midland_bundle_bytes:
    try:
        count = len(midland_bundle_bytes)
        noun = "bundle" if count == 1 else "bundles"
        with st.spinner(f"Reading {count} Midland CAF extract {noun}..."):
            midland_lookup, midland_selected, midland_issues, midland_rows_read = (
                load_midland_occupancy_lookup_cached(
                    tuple(midland_bundle_bytes),
                    target_quarters,
                )
            )
        st.success(
            f"{count} Midland CAF {noun} loaded. Rent roll rows read: {midland_rows_read}. "
            f"Loan-quarter occupancy rows: {len(midland_selected)}."
        )
        if not midland_lookup:
            st.warning(
                "The Midland bundles produced no occupancy for the template quarters "
                f"({', '.join(target_quarters) if target_quarters else 'none defined'}). "
                "Their rent rolls may not cover those periods."
            )
        else:
            filled = {key[1] for key in midland_lookup}
            empty_quarters = [q for q in target_quarters if q not in filled]
            if empty_quarters:
                st.warning(
                    "No Midland occupancy for " + ", ".join(empty_quarters) + ". "
                    "Add a CAF bundle downloaded shortly after each of those quarter ends."
                )
    except Exception as exc:
        st.error(f"Midland CAF bundles: {exc}")
        st.stop()

# Berkadia is passed first, so it wins wherever both servicers report the same
# loan and quarter.
occupancy_lookup = merge_occupancy_lookups(berkadia_lookup, midland_lookup)

overlap = sorted(set(berkadia_lookup) & set(midland_lookup))
if overlap:
    st.caption(
        f"{len(overlap)} loan-quarter values are reported by both servicers; the Berkadia value was kept."
    )

st.session_state["occupancy_selected"] = occupancy_selected
st.session_state["occupancy_issues"] = occupancy_issues
st.session_state["midland_selected"] = midland_selected
st.session_state["midland_issues"] = midland_issues

PARTY_SEARCH_MODE = "Sponsor / Guarantor"

st.subheader("Step 3: Search Salesforce and choose an account, sponsor or guarantor")
search_col1, search_col2 = st.columns([1, 2])
with search_col1:
    search_mode = st.selectbox(
        "Search Salesforce by",
        ["Account Name", "Deal Name", "Deal Loan Number", PARTY_SEARCH_MODE],
        help=(
            f"{PARTY_SEARCH_MODE} finds every deal, across all accounts, where the name you type "
            "is a sponsor or a guarantor."
        ),
    )
with search_col2:
    search_text = st.text_input("Search text")

if st.button("Search Salesforce", type="secondary"):
    if not search_text.strip():
        st.error("Enter a search value first.")
    else:
        try:
            if search_mode == PARTY_SEARCH_MODE:
                party_candidates, party_deals = search_matching_parties(sf, search_text)
                st.session_state["party_candidates"] = party_candidates
                st.session_state["party_deals"] = party_deals
                st.session_state["account_candidates"] = pd.DataFrame()
                if party_candidates.empty:
                    st.warning("No sponsor or guarantor by that name was found on any closed deal.")
                else:
                    st.success(f"Found {len(party_candidates)} matching sponsor / guarantor names.")
            else:
                account_candidates = search_matching_accounts(sf, search_mode, search_text)
                st.session_state["account_candidates"] = account_candidates
                st.session_state["party_candidates"] = pd.DataFrame()
                st.session_state["party_deals"] = pd.DataFrame()
                if account_candidates.empty:
                    st.warning("No matching accounts found.")
                else:
                    st.success(f"Found {len(account_candidates)} matching account candidates.")
        except Exception as exc:
            st.error(str(normalize_salesforce_error(exc)))

account_candidates = st.session_state.get("account_candidates", pd.DataFrame())
party_candidates = st.session_state.get("party_candidates", pd.DataFrame())
party_deals = st.session_state.get("party_deals", pd.DataFrame())
selected_account = None
selected_party = None
selected_party_deal_ids: list[str] = []
if isinstance(party_candidates, pd.DataFrame) and not party_candidates.empty:
    st.dataframe(party_candidates, use_container_width=True, hide_index=True)
    selected_party = st.selectbox(
        "Pick the sponsor / guarantor for the AM slide",
        options=party_candidates["Sponsor / Guarantor"].tolist(),
    )
    chosen_deals = party_deals[party_deals["Party"] == selected_party]
    selected_party_deal_ids = chosen_deals["Opportunity Id"].astype(str).unique().tolist()
    with st.expander(f"Deals for {selected_party} ({len(selected_party_deal_ids)})", expanded=False):
        st.dataframe(
            chosen_deals.drop(columns=["Opportunity Id"]),
            use_container_width=True,
            hide_index=True,
        )
elif isinstance(account_candidates, pd.DataFrame) and not account_candidates.empty:
    st.dataframe(account_candidates, use_container_width=True, hide_index=True)
    selected_account = st.selectbox(
        "Pick the account for the AM slide",
        options=account_candidates["Account_Name__c"].tolist(),
    )
else:
    st.info("Search Salesforce to load the account list.")

slide_label = selected_party or selected_account

st.subheader("Step 4: Build and download the AM slide")

fci_token = load_fci_token()
run_fci = st.checkbox(
    "Also check Bridge loans against FCI (late payments / late charges)",
    value=False,
    help="Matches Bridge loans to FCI by Servicer Commitment ID and Property Servicer ID, then flags late payments and late-charge balances on a separate sheet.",
)
if run_fci and not fci_token:
    fci_token_input = st.text_input(
        "FCI API token",
        type="password",
        help="Used only for this session. You can instead set it under [fci] token in Streamlit secrets.",
    )
    if fci_token_input:
        st.session_state["fci_token"] = fci_token_input.strip()
        fci_token = fci_token_input.strip()
if run_fci and not fci_token:
    st.warning("Enter an FCI API token (or add it to secrets) to run the FCI check.")

build_disabled = not bool(slide_label)
if st.button("Build completed AM slide", type="primary", disabled=build_disabled):
    try:
        with st.spinner("Building the AM slide workbook..."):
            if selected_party:
                term_rows, bridge_rows, bridge_fci_rows = build_term_bridge_for_deal_ids(
                    sf, selected_party_deal_ids
                )
            else:
                term_rows, bridge_rows, bridge_fci_rows = build_term_bridge_for_account(
                    sf, selected_account
                )
            if term_rows.empty and bridge_rows.empty:
                raise RuntimeError(f"No term or bridge rows were returned for {slide_label}.")

            term_rows_with_occ = apply_term_occupancy_to_rows(term_rows, occupancy_lookup, occ_headers)

            fci_alerts = pd.DataFrame()
            fci_matches = pd.DataFrame()
            include_fci = bool(
                run_fci
                and fci_token
                and isinstance(bridge_fci_rows, pd.DataFrame)
                and not bridge_fci_rows.empty
            )
            if include_fci:
                with st.spinner("Checking Bridge loans against FCI..."):
                    fci_matches, fci_alerts = check_bridge_loans_against_fci(
                        bridge_fci_rows, api_token=fci_token
                    )

            workbook_bytes, workbook_name = build_workbook_bytes(
                template_bytes,
                term_rows_with_occ,
                bridge_rows,
                slide_label,
                fci_alerts=fci_alerts,
                fci_matches=fci_matches,
                include_fci=include_fci,
            )

        st.session_state["term_preview"] = format_preview(term_rows_with_occ, occ_headers)
        st.session_state["bridge_preview"] = bridge_rows
        st.session_state["workbook_bytes"] = workbook_bytes
        st.session_state["workbook_name"] = workbook_name
        if not term_rows_with_occ.empty and "Occupancy Matched" in term_rows_with_occ.columns:
            st.session_state["match_count"] = int(term_rows_with_occ["Occupancy Matched"].sum())
        else:
            st.session_state["match_count"] = 0
        st.session_state["term_count"] = len(term_rows_with_occ)
        st.session_state["account_loan_ids"] = sorted(
            {
                str(value)
                for value in term_rows_with_occ.get("Loan ID", pd.Series(dtype=str)).tolist()
                if str(value).strip()
            }
        )
        st.session_state["fci_ran"] = include_fci
        st.session_state["fci_alerts"] = fci_alerts
        st.session_state["fci_matches"] = fci_matches

        # Starting text for the slide decks, editable below before download.
        # A problem here must never cost the analyst the Excel workbook.
        try:
            slide_intros = default_slide_intros(workbook_bytes)
            late_term_loans = term_loans_paid_late(workbook_bytes)
        except Exception as exc:
            slide_intros, late_term_loans = {}, []
            st.warning(f"The workbook is ready, but the PowerPoint slides could not be prepared: {exc}")
        st.session_state["deck_kinds"] = [kind for kind in (TERM_SHEET, BRIDGE_SHEET) if kind in slide_intros]
        st.session_state["deck_default_name"] = slide_label
        st.session_state["deck_title_name"] = slide_label
        st.session_state["deck_term_intro"] = slide_intros.get(TERM_SHEET, "")
        st.session_state["deck_bridge_intro"] = slide_intros.get(BRIDGE_SHEET, "")
        st.session_state["late_term_loans"] = late_term_loans

        st.success("The AM slide workbook is ready.")
    except Exception as exc:
        st.error(str(normalize_salesforce_error(exc)))

term_preview = st.session_state.get("term_preview")
bridge_preview = st.session_state.get("bridge_preview")
workbook_bytes = st.session_state.get("workbook_bytes")
workbook_name = st.session_state.get("workbook_name")

if workbook_bytes:
    metric_col1, metric_col2 = st.columns(2)
    with metric_col1:
        st.metric("Term loans", st.session_state.get("term_count", 0))
    with metric_col2:
        st.metric("Term loans with occupancy match", st.session_state.get("match_count", 0))

    st.download_button(
        "Download completed AM slide (Excel)",
        data=workbook_bytes,
        file_name=workbook_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        on_click="ignore",
    )

    deck_kinds = st.session_state.get("deck_kinds") or []
    if deck_kinds and Presentation is None:
        st.info("Install python-pptx (pip install python-pptx) to download the PowerPoint slides as well.")
    elif deck_kinds:
        with st.expander("PowerPoint slide text", expanded=False):
            st.caption(
                "The decks follow the AM templates. Change these before downloading if needed; "
                "the commentary bullets are left as the template's standard wording."
            )
            st.text_input("Name in the slide titles", key="deck_title_name")
            if TERM_SHEET in deck_kinds:
                st.text_area(
                    "Term slide intro",
                    key="deck_term_intro",
                    height=80,
                    help="Built from the Guarantor column. Remove anyone who should not be named.",
                )
            if BRIDGE_SHEET in deck_kinds:
                st.text_input("Bridge slide intro", key="deck_bridge_intro")
        late_loans = st.session_state.get("late_term_loans") or []
        if late_loans and TERM_SHEET in deck_kinds:
            st.warning(
                "The Term slide's commentary says there were no late or DQ payments, but these loans "
                "show on-time payments under 100%: " + ", ".join(late_loans) + ". Adjust that bullet in PowerPoint."
            )
        deck_name = (st.session_state.get("deck_title_name") or "").strip() or st.session_state.get("deck_default_name", "")
        for deck_kind, deck_column in zip(deck_kinds, st.columns(len(deck_kinds))):
            intro_key = "deck_term_intro" if deck_kind == TERM_SHEET else "deck_bridge_intro"
            try:
                with st.spinner(f"Building the {deck_kind} slides..."):
                    deck = slide_deck_download(
                        deck_kind, workbook_bytes, deck_name, st.session_state.get(intro_key) or "", date.today()
                    )
            except Exception as exc:
                deck_column.warning(f"The {deck_kind} slides could not be built: {exc}")
                continue
            if deck:
                deck_column.download_button(
                    f"Download {deck_kind} slides (PowerPoint)",
                    data=deck[0],
                    file_name=deck[1],
                    mime=PPTX_MIME,
                    key=f"download_{deck_kind.lower()}_deck",
                    on_click="ignore",
                )

if isinstance(term_preview, pd.DataFrame) and not term_preview.empty:
    st.subheader("Term preview")
    st.dataframe(term_preview, use_container_width=True, hide_index=True)

if isinstance(bridge_preview, pd.DataFrame) and not bridge_preview.empty:
    st.subheader("Bridge preview")
    st.dataframe(bridge_preview, use_container_width=True, hide_index=True)

# Occupancy notes for the loans on this slide. Everything else the servicers
# reported belongs to other accounts and is only noise here.
account_loan_ids = set(st.session_state.get("account_loan_ids") or [])
scoped_notes = []
for servicer_label, notes_frame in (("Berkadia", occupancy_issues), ("Midland", midland_issues)):
    if not isinstance(notes_frame, pd.DataFrame) or notes_frame.empty:
        continue
    if "Loan ID 5" not in notes_frame.columns:
        continue
    scoped = notes_frame[notes_frame["Loan ID 5"].astype(str).isin(account_loan_ids)].copy()
    if scoped.empty:
        continue
    if "Candidate Rows" in scoped.columns:
        scoped["Candidate Rows"] = scoped["Candidate Rows"].apply(
            lambda x: json.dumps(x, default=str) if isinstance(x, (list, dict)) else x
        )
    scoped.insert(0, "Servicer", servicer_label)
    scoped_notes.append(scoped)

if account_loan_ids:
    st.subheader("Occupancy notes")
    if scoped_notes:
        notes_df = pd.concat(scoped_notes, ignore_index=True, sort=False)
        lead = [c for c in ["Servicer", "Loan ID 5", "Quarter Key", "Issue"] if c in notes_df.columns]
        rest = [c for c in notes_df.columns if c not in lead]
        notes_df = notes_df[lead + rest].sort_values(
            [c for c in ["Loan ID 5", "Quarter Key"] if c in notes_df.columns]
        )
        st.caption(
            f"{len(notes_df)} note(s) on how occupancy was chosen for the loans on this slide."
        )
        st.dataframe(notes_df, use_container_width=True, hide_index=True)
    else:
        st.caption("No occupancy notes for this account: every quarter resolved cleanly.")

if st.session_state.get("fci_ran"):
    st.subheader("FCI Bridge late-payment / late-charge check")
    fci_alerts = st.session_state.get("fci_alerts")
    fci_matches = st.session_state.get("fci_matches")

    matched_count = 0 if not isinstance(fci_matches, pd.DataFrame) else len(fci_matches)
    alert_count = 0 if not isinstance(fci_alerts, pd.DataFrame) else len(fci_alerts)
    metric_a, metric_b = st.columns(2)
    with metric_a:
        st.metric("FCI records matched", matched_count)
    with metric_b:
        st.metric("Alert rows", alert_count)

    lookup_errors = st.session_state.get("fci_lookup_errors") or []
    if lookup_errors:
        with st.expander(f"FCI lookup warnings ({len(lookup_errors)})", expanded=False):
            for err in lookup_errors:
                st.text(err)

    if isinstance(fci_alerts, pd.DataFrame) and not fci_alerts.empty:
        severity_counts = (
            fci_alerts.groupby("Severity", dropna=False).size().reset_index(name="Count")
        )
        st.dataframe(severity_counts, use_container_width=True, hide_index=True)
        st.dataframe(fci_alerts, use_container_width=True, hide_index=True)
    else:
        st.info("No late payment or late charge items found for the matched FCI loans.")

    if isinstance(fci_matches, pd.DataFrame) and not fci_matches.empty:
        with st.expander("FCI matched records", expanded=False):
            st.dataframe(fci_matches, use_container_width=True, hide_index=True)
