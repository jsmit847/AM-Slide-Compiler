# ============================================================
# AM Slides Builder (TERM + BRIDGE) — ONE CELL (Salesforce API -> Excel template)
# FIXES / IMPROVEMENTS INCLUDED:
# - BRIDGE timing logic (avg hold / avg disposed) already corrected
# - EXCEL WRITE FIX: convert pandas <NA>/NaN to None before openpyxl writes
# - TERM FIX: if Outstanding Balance == 0 -> Next Payment Date cell says "Paid Off"
# - UX FIX: removed guarantor input prompt (uses Guarantor column if present)
# - ✅ SAB FIX (REAL): includes "Single Asset Bridge Loan" RecordType
#     * matches on RecordType.Name AND RecordType.DeveloperName (more stable)
#     * exact + contains matching
# ============================================================

# --- Ensure Salesforce client exists as `sf` ---
import truststore
truststore.inject_into_ssl()

import keyring
from simple_salesforce import Salesforce

SERVICE = "salesforce_prod_oauth"  # must match what you used in auth
instance_url = keyring.get_password(SERVICE, "instance_url")
access_token = keyring.get_password(SERVICE, "access_token")

if not instance_url or not access_token:
    raise RuntimeError("Missing instance_url/access_token in keyring. Run your OAuth login cell first.")

sf = Salesforce(instance_url=instance_url, session_id=access_token)
print("✅ sf ready:", instance_url)

import re
from copy import copy
from datetime import date
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from IPython.display import display

pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# -------------------------
# EDIT THIS PATH ON YOUR MACHINE (template workbook)
# -------------------------
TEMPLATE_PATH = r"C:\Users\jonathan.smith\OneDrive - Redwood Trust, Inc\Desktop\Reference AM Templates.xlsx"
TERM_SHEET = "Term"
BRIDGE_SHEET = "Bridge"

VALID_STAGES = ["Closed Won", "Expired", "Matured", "Paid Off", "Sold"]

# -------------------------
# RecordType classification (ROBUST; Name + DeveloperName)
# -------------------------
def norm_rt(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).strip().lower())

# TERM
TERM_RT_EXACT = {"term loan", "dscr"}
TERM_RT_CONTAINS = {"dscr"}

# BRIDGE (✅ includes Single Asset Bridge Loan)
BRIDGE_RT_EXACT = {"acquired bridge loan", "bridge loan", "sab loan", "single asset bridge loan"}
BRIDGE_RT_CONTAINS = {"sab", "single asset bridge"}

# Bridge DeveloperName (more stable than Name)
BRIDGE_DEV_EXACT = {"single_asset_bridge_loan"}
BRIDGE_DEV_CONTAINS = {"single_asset_bridge", "sab"}

# Deal_Contact__c linkage (confirmed)
DC_DEAL_FIELD = "Deal__c"
DC_DEAL_REL   = "Deal__r"

# -------------------------
# Small helpers
# -------------------------
def soql_quote(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"

def digits_only(x) -> str:
    return re.sub(r"\D", "", "" if x is None or pd.isna(x) else str(x))

def last5_strip_prefix(x) -> str:
    d = digits_only(x)
    if d.startswith("4030") or d.startswith("6000"):
        d = d[4:]
    return d[-5:] if len(d) >= 5 else d

def pct_to_dec(x):
    if x in ("", None) or pd.isna(x):
        return None
    s = str(x).strip().replace("%", "")
    try:
        v = float(s)
        return v / 100.0 if v > 1.5 else v
    except Exception:
        return None

def parse_date_any(x):
    if x in ("", None) or pd.isna(x):
        return None
    dt = pd.to_datetime(x, errors="coerce")
    if pd.isna(dt):
        return None
    return dt.date()

def extract_states_only(state_percentages: str) -> str:
    s = "" if state_percentages is None or pd.isna(state_percentages) else str(state_percentages)
    codes = re.findall(r"\b[A-Z]{2}\b", s.upper())
    if not codes:
        return s.strip()
    seen = set()
    out = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return ", ".join(out)

def safe_flatten_recordtype(df: pd.DataFrame) -> pd.DataFrame:
    if "RecordType" in df.columns:
        df["RecordType.Name"] = df["RecordType"].apply(lambda x: (x or {}).get("Name"))
        df["RecordType.DeveloperName"] = df["RecordType"].apply(lambda x: (x or {}).get("DeveloperName"))
        df = df.drop(columns=["RecordType"], errors="ignore")
    return df

def chunked(vals, n=200):
    vals = list(vals)
    for i in range(0, len(vals), n):
        yield vals[i:i+n]

def try_query_drop_missing(sf, obj_name: str, fields, where_clause, limit=2000, order_by=None):
    """Query + auto-drop missing fields / bad relationship paths."""
    fields = list(fields)
    while True:
        soql = f"SELECT {', '.join(fields)} FROM {obj_name} WHERE {where_clause}"
        if order_by:
            soql += f" ORDER BY {order_by}"
        soql += f" LIMIT {int(limit)}"
        try:
            rows = sf.query_all(soql).get("records", [])
            return rows, fields, soql
        except Exception as e:
            msg = str(e)
            m1 = re.search(r"No such column '([^']+)'", msg)
            m2 = re.search(r"Didn't understand relationship '([^']+)'", msg)

            if m1:
                bad = m1.group(1)
                if bad in fields:
                    print(f"[{obj_name}] Dropping missing field and retrying: {bad}")
                    fields.remove(bad)
                    continue

            if m2:
                relbad = m2.group(1)
                drop = [f for f in fields if f.startswith(relbad + ".") or (("." + relbad + ".") in f)]
                if drop:
                    for f in drop:
                        print(f"[{obj_name}] Dropping bad relationship field and retrying: {f}")
                        fields.remove(f)
                    continue
            raise

def build_where_for_search(mode: str, q: str) -> str:
    q = (q or "").strip()
    if mode == "1":
        return "Account_Name__c LIKE " + soql_quote("%" + q + "%")
    elif mode == "2":
        return "Name LIKE " + soql_quote("%" + q + "%")
    else:
        digits = re.sub(r"\D", "", q)
        if digits:
            return "(" + " OR ".join([
                "Deal_Loan_Number__c = " + soql_quote(digits),
                "Deal_Loan_Number__c LIKE " + soql_quote("%" + digits + "%"),
                "Deal_Loan_Number__c LIKE " + soql_quote("%" + q + "%"),
            ]) + ")"
        return "Deal_Loan_Number__c LIKE " + soql_quote("%" + q + "%")

def classify_term_bridge(df_all: pd.DataFrame):
    """
    Robust classification:
      - TERM if RecordType.Name exact/contains
      - BRIDGE if RecordType.Name exact/contains OR RecordType.DeveloperName exact/contains
    Prefer TERM if overlap.
    """
    if "RecordType.Name" not in df_all.columns:
        return df_all.iloc[0:0].copy(), df_all.iloc[0:0].copy()

    rt_name = df_all["RecordType.Name"].apply(norm_rt)

    rt_dev = df_all.get("RecordType.DeveloperName", pd.Series([""] * len(df_all), index=df_all.index))
    rt_dev = rt_dev.fillna("").astype(str).str.strip().str.lower()

    # TERM
    term_exact = rt_name.isin(TERM_RT_EXACT)
    term_contains = pd.Series(False, index=df_all.index)
    for tok in TERM_RT_CONTAINS:
        term_contains = term_contains | rt_name.str.contains(re.escape(tok), na=False)
    is_term = term_exact | term_contains

    # BRIDGE (Name)
    bridge_exact = rt_name.isin(BRIDGE_RT_EXACT)
    bridge_contains = pd.Series(False, index=df_all.index)
    for tok in BRIDGE_RT_CONTAINS:
        bridge_contains = bridge_contains | rt_name.str.contains(re.escape(tok), na=False)

    # BRIDGE (DeveloperName)
    bridge_dev_exact = rt_dev.isin(BRIDGE_DEV_EXACT)
    bridge_dev_contains = pd.Series(False, index=df_all.index)
    for tok in BRIDGE_DEV_CONTAINS:
        bridge_dev_contains = bridge_dev_contains | rt_dev.str.contains(re.escape(tok), na=False)

    is_bridge = bridge_exact | bridge_contains | bridge_dev_exact | bridge_dev_contains

    # Prefer TERM if both match
    is_bridge = is_bridge & (~is_term)

    return df_all[is_term].copy(), df_all[is_bridge].copy()

# -------------------------
# Deal_Contact__c guarantors
# -------------------------
def find_contact_ref_field_on_deal_contact(sf):
    d = sf.Deal_Contact__c.describe()
    for f in d.get("fields", []):
        if f.get("type") == "reference":
            rto = f.get("referenceTo") or []
            if any(str(x).lower() == "contact" for x in rto):
                return f.get("name"), f.get("relationshipName")
    return None, None

def query_deal_contacts_for_guarantors(sf, opp_ids):
    if not opp_ids:
        return pd.DataFrame(columns=[DC_DEAL_FIELD, "GuarantorName"])

    _, contact_relname = find_contact_ref_field_on_deal_contact(sf)
    contact_name_path = f"{contact_relname}.Name" if contact_relname else None

    fields = ["Id", DC_DEAL_FIELD, "Is_Guarantor__c", "Name"]
    if contact_name_path:
        fields.append(contact_name_path)

    out = []
    for ch in chunked(opp_ids, 150):
        ids_in = ", ".join(soql_quote(x) for x in ch)
        f_try = list(fields)
        while True:
            soql = (
                "SELECT " + ", ".join(f_try) +
                " FROM Deal_Contact__c" +
                " WHERE " + DC_DEAL_FIELD + " IN (" + ids_in + ")" +
                " AND Is_Guarantor__c = TRUE"
            )
            try:
                res = sf.query_all(soql)
                out.extend(res.get("records", []))
                break
            except Exception as e:
                msg = str(e)
                m1 = re.search(r"No such column '([^']+)'", msg)
                m2 = re.search(r"Didn't understand relationship '([^']+)'", msg)

                if m1 and m1.group(1) in f_try:
                    bad = m1.group(1)
                    print("Dropping missing Deal_Contact__c field and retrying:", bad)
                    f_try.remove(bad)
                    continue

                if m2:
                    relbad = m2.group(1)
                    drop = [f for f in f_try if f.startswith(relbad + ".") or (("." + relbad + ".") in f)]
                    if drop:
                        for f in drop:
                            print("Dropping bad Deal_Contact__c relationship field and retrying:", f)
                            f_try.remove(f)
                        continue
                raise

    df = pd.DataFrame(out).drop(columns=["attributes"], errors="ignore")
    if df.empty:
        return pd.DataFrame(columns=[DC_DEAL_FIELD, "GuarantorName"])

    if contact_relname and contact_relname in df.columns:
        df["ContactName"] = df[contact_relname].apply(lambda x: (x or {}).get("Name"))
    else:
        df["ContactName"] = None

    df["GuarantorName"] = df["ContactName"]
    m = df["GuarantorName"].isna() | (df["GuarantorName"].astype(str).str.strip() == "")
    df.loc[m, "GuarantorName"] = df.loc[m, "Name"]

    return df[[DC_DEAL_FIELD, "GuarantorName"]].copy()

# -------------------------
# Excel template utilities
# -------------------------
def norm_hdr(x):
    if x is None:
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s

def find_header_row_and_map(ws, must_have=("portfolio", "loan id"), scan_rows=80, scan_cols=200):
    header_row = None
    for r in range(1, min(ws.max_row, scan_rows) + 1):
        row_vals = [norm_hdr(ws.cell(r, c).value) for c in range(1, min(ws.max_column, scan_cols) + 1)]
        if all(h in row_vals for h in must_have):
            header_row = r
            break
    if header_row is None:
        raise ValueError(f"Could not find header row on '{ws.title}' containing {must_have}.")
    col_map = {}
    for c in range(1, min(ws.max_column, scan_cols) + 1):
        h = ws.cell(header_row, c).value
        if h is not None and str(h).strip() != "":
            col_map[norm_hdr(h)] = c
    return header_row, col_map

def find_total_row(ws, header_row, scan_cols=200):
    start = header_row + 1
    for r in range(start, ws.max_row + 1):
        for c in range(1, min(ws.max_column, scan_cols) + 1):
            v = ws.cell(r, c).value
            if isinstance(v, str) and v.strip().lower() == "total":
                return r
    return None

def snapshot_row_style(ws, row, last_col):
    row_height = ws.row_dimensions[row].height
    styles = {}
    for c in range(1, last_col + 1):
        cell = ws.cell(row, c)
        styles[c] = {
            "_style": copy(cell._style),
            "font": copy(cell.font),
            "border": copy(cell.border),
            "fill": copy(cell.fill),
            "alignment": copy(cell.alignment),
            "protection": copy(cell.protection),
            "number_format": cell.number_format,
        }
    return row_height, styles

def apply_row_style(ws, row, styles_by_col, row_height, last_col):
    if row_height is not None:
        ws.row_dimensions[row].height = row_height
    for c in range(1, last_col + 1):
        dst = ws.cell(row, c)
        st = styles_by_col.get(c)
        if not st:
            continue
        dst._style = copy(st["_style"])
        dst.font = copy(st["font"])
        dst.border = copy(st["border"])
        dst.fill = copy(st["fill"])
        dst.alignment = copy(st["alignment"])
        dst.protection = copy(st["protection"])
        dst.number_format = st["number_format"]

def ensure_rows(ws, header_row, total_row, needed_rows, last_col):
    if total_row is None:
        raise ValueError(f"Could not find TOTAL row on '{ws.title}'. Template must include a Total row.")

    start_row = header_row + 1
    existing = total_row - start_row

    row_a = start_row
    row_b = start_row + 1 if start_row + 1 < total_row else start_row
    a_h, a_st = snapshot_row_style(ws, row_a, last_col)
    b_h, b_st = snapshot_row_style(ws, row_b, last_col)

    if needed_rows > existing:
        add = needed_rows - existing
        ws.insert_rows(total_row, amount=add)
        total_row += add
    elif needed_rows < existing:
        remove = existing - needed_rows
        ws.delete_rows(total_row - remove, amount=remove)
        total_row -= remove

    for i, r in enumerate(range(start_row, start_row + needed_rows)):
        use_blue = (i % 2 == 1)
        apply_row_style(ws, r, (b_st if use_blue else a_st), (b_h if use_blue else a_h), last_col)

    return start_row, total_row

def excel_safe(v):
    """Convert pandas missing/scalars into openpyxl-friendly Python types."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    try:
        import numpy as np
        if isinstance(v, (np.generic,)):
            return v.item()
    except Exception:
        pass
    return v

def set_cell(ws, r, c, value, number_format=None):
    cell = ws.cell(r, c)
    cell.value = excel_safe(value)
    if number_format:
        cell.number_format = number_format

def sum_ints(series):
    vals = [v for v in series if v is not None and not pd.isna(v)]
    return int(sum(vals)) if vals else 0

def sum_money(series):
    vals = [float(v) for v in series if v is not None and not pd.isna(v)]
    return float(sum(vals)) if vals else 0.0

# -------------------------
# Write TERM sheet
# -------------------------
def write_term_sheet(ws, term_rows: pd.DataFrame, guarantor: str = ""):
    hdr, cmap = find_header_row_and_map(ws, must_have=("portfolio", "loan id"))
    last_col = max(cmap.values())
    total_row = find_total_row(ws, hdr)
    start_row, total_row = ensure_rows(ws, hdr, total_row, needed_rows=len(term_rows), last_col=last_col)

    def col(name): return cmap.get(norm_hdr(name))

    tot_loan_amt = sum_money(term_rows["Loan Amount Num"].tolist()) if "Loan Amount Num" in term_rows else 0.0
    tot_upb      = sum_money(term_rows["Outstanding Balance Num"].tolist()) if "Outstanding Balance Num" in term_rows else 0.0
    tot_props    = sum_ints(term_rows["Total Properties Num"].tolist()) if "Total Properties Num" in term_rows else 0
    tot_units    = sum_ints(term_rows["Total Units Num"].tolist()) if "Total Units Num" in term_rows else 0

    for i, row in term_rows.reset_index(drop=True).iterrows():
        r = start_row + i

        if col("portfolio"):
            set_cell(ws, r, col("portfolio"), "Term")

        if col("loan id"):
            set_cell(ws, r, col("loan id"), row.get("Loan ID", ""))

        if col("loan"):
            set_cell(ws, r, col("loan"), row.get("Loan", ""))

        if col("account name"):
            set_cell(ws, r, col("account name"), row.get("Account Name", ""))

        if col("guarantor"):
            set_cell(ws, r, col("guarantor"), row.get("Guarantor", "") or guarantor)

        if col("origination date"):
            set_cell(ws, r, col("origination date"), row.get("Origination Date", None), "m/d/yyyy")

        if col("loan amount"):
            v = row.get("Loan Amount Num", None)
            v = None if (v is None or pd.isna(v)) else int(round(float(v)))
            set_cell(ws, r, col("loan amount"), v, '$#,##0')

        if col("outstanding balance"):
            v = row.get("Outstanding Balance Num", None)
            v = None if (v is None or pd.isna(v)) else int(round(float(v)))
            set_cell(ws, r, col("outstanding balance"), v, '$#,##0')

        if col("origination ltv"):
            v = row.get("LTV Dec", None)
            v = None if (v is None or pd.isna(v)) else float(v)
            set_cell(ws, r, col("origination ltv"), v, '0%')

        if col("interest rate"):
            v = row.get("Rate Dec", None)
            v = None if (v is None or pd.isna(v)) else float(v)
            set_cell(ws, r, col("interest rate"), v, '0.00%')

        if col("state(s)"):
            set_cell(ws, r, col("state(s)"), row.get("State(s)", ""))

        if col("total properties"):
            set_cell(ws, r, col("total properties"), row.get("Total Properties Num", None), '0')

        if col("total units"):
            set_cell(ws, r, col("total units"), row.get("Total Units Num", None), '0')

        if col("recourse"):
            set_cell(ws, r, col("recourse"), row.get("Recourse", ""))

        if col("historical ontime payment %"):
            v = row.get("Historical Ontime % Dec", None)
            v = None if (v is None or pd.isna(v)) else float(v)
            set_cell(ws, r, col("historical ontime payment %"), v, '0%')

        if col("next payment date"):
            ob = row.get("Outstanding Balance Num", None)
            is_paid_off = False
            if ob is not None and not pd.isna(ob):
                try:
                    is_paid_off = float(ob) == 0.0
                except Exception:
                    is_paid_off = False

            if is_paid_off:
                set_cell(ws, r, col("next payment date"), "Paid Off")
            else:
                set_cell(ws, r, col("next payment date"), row.get("Next Payment Date", None), "m/d/yyyy")

        if col("current loan maturity date"):
            set_cell(ws, r, col("current loan maturity date"), row.get("Maturity Date", None), "m/d/yyyy")

        for k in list(cmap.keys()):
            if "occ" in k:
                set_cell(ws, r, cmap[k], None)

    if col("loan"):
        set_cell(ws, total_row, col("loan"), int(len(term_rows)))

    if col("loan amount"):
        set_cell(ws, total_row, col("loan amount"), int(round(tot_loan_amt)) if tot_loan_amt else 0, '$#,##0')
    if col("outstanding balance"):
        set_cell(ws, total_row, col("outstanding balance"), int(round(tot_upb)) if tot_upb else 0, '$#,##0')
    if col("total properties"):
        set_cell(ws, total_row, col("total properties"), tot_props, '0')
    if col("total units"):
        set_cell(ws, total_row, col("total units"), tot_units, '0')

# -------------------------
# Write BRIDGE sheet
# -------------------------
def write_bridge_sheet(ws, bridge_rows: pd.DataFrame):
    hdr, cmap = find_header_row_and_map(ws, must_have=("portfolio", "loan id"))
    last_col = max(cmap.values())
    total_row = find_total_row(ws, hdr)
    start_row, total_row = ensure_rows(ws, hdr, total_row, needed_rows=len(bridge_rows), last_col=last_col)

    def col(name): return cmap.get(norm_hdr(name))

    tot_commit = sum_money(bridge_rows["Commitment Amount Num"].tolist()) if "Commitment Amount Num" in bridge_rows else 0.0
    tot_life   = sum_money(bridge_rows["Lifetime Funded Num"].tolist()) if "Lifetime Funded Num" in bridge_rows else 0.0
    tot_upb    = sum_money(bridge_rows["Outstanding Balance Num"].tolist()) if "Outstanding Balance Num" in bridge_rows else 0.0
    tot_props  = sum_ints(bridge_rows["Total Properties Num"].tolist()) if "Total Properties Num" in bridge_rows else 0
    tot_paid   = sum_ints(bridge_rows["Paid Off Assets Num"].tolist()) if "Paid Off Assets Num" in bridge_rows else 0
    tot_active = sum_ints(bridge_rows["Active Assets Num"].tolist()) if "Active Assets Num" in bridge_rows else 0

    for i, row in bridge_rows.reset_index(drop=True).iterrows():
        r = start_row + i

        if col("portfolio"):
            set_cell(ws, r, col("portfolio"), "Bridge")

        if col("loan id"):
            set_cell(ws, r, col("loan id"), row.get("Loan ID", ""))

        if col("loan name"):
            set_cell(ws, r, col("loan name"), row.get("Loan", ""))
        elif col("loan"):
            set_cell(ws, r, col("loan"), row.get("Loan", ""))

        if col("commitment amount"):
            v = row.get("Commitment Amount Num", None)
            v = None if (v is None or pd.isna(v)) else int(round(float(v)))
            set_cell(ws, r, col("commitment amount"), v, '$#,##0')

        if col("line origination date"):
            set_cell(ws, r, col("line origination date"), row.get("Origination Date", None), "m/d/yyyy")
        elif col("origination date"):
            set_cell(ws, r, col("origination date"), row.get("Origination Date", None), "m/d/yyyy")

        if col("line maturity date"):
            set_cell(ws, r, col("line maturity date"), row.get("Maturity Date", None), "m/d/yyyy")

        if col("interest rate"):
            v = row.get("Rate Dec", None)
            v = None if (v is None or pd.isna(v)) else float(v)
            set_cell(ws, r, col("interest rate"), v, '0.00%')

        if col("ltv"):
            v = row.get("LTV Dec", None)
            v = None if (v is None or pd.isna(v)) else float(v)
            set_cell(ws, r, col("ltv"), v, '0%')

        if col("advances"):
            set_cell(ws, r, col("advances"), row.get("Advances Num", None), "0")

        if col("total funded assets"):
            set_cell(ws, r, col("total funded assets"), row.get("Total Properties Num", None), "0")

        if col("state(s)"):
            set_cell(ws, r, col("state(s)"), row.get("State(s)", ""))

        if col("lifetime funded"):
            v = row.get("Lifetime Funded Num", None)
            v = None if (v is None or pd.isna(v)) else int(round(float(v)))
            set_cell(ws, r, col("lifetime funded"), v, '$#,##0')

        if col("paid off assets"):
            set_cell(ws, r, col("paid off assets"), row.get("Paid Off Assets Num", None), "0")

        if col("active assets"):
            set_cell(ws, r, col("active assets"), row.get("Active Assets Num", None), "0")

        if col("outstanding balance"):
            v = row.get("Outstanding Balance Num", None)
            v = None if (v is None or pd.isna(v)) else int(round(float(v)))
            set_cell(ws, r, col("outstanding balance"), v, '$#,##0')

        if col("as-is/ arv"):
            v = row.get("As-Is/ ARV Num", None)
            v = None if (v is None or pd.isna(v)) else int(round(float(v)))
            set_cell(ws, r, col("as-is/ arv"), v, '$#,##0')

        if col("avg hold time"):
            set_cell(ws, r, col("avg hold time"), row.get("Avg Hold Time Num", None), "0")

        if col("avg disposed time"):
            set_cell(ws, r, col("avg disposed time"), row.get("Avg Disposed Time Num", None), "0")

    if col("loan name"):
        set_cell(ws, total_row, col("loan name"), int(len(bridge_rows)))
    elif col("loan"):
        set_cell(ws, total_row, col("loan"), int(len(bridge_rows)))

    if col("commitment amount"):
        set_cell(ws, total_row, col("commitment amount"), int(round(tot_commit)) if tot_commit else 0, '$#,##0')
    if col("lifetime funded"):
        set_cell(ws, total_row, col("lifetime funded"), int(round(tot_life)) if tot_life else 0, '$#,##0')
    if col("outstanding balance"):
        set_cell(ws, total_row, col("outstanding balance"), int(round(tot_upb)) if tot_upb else 0, '$#,##0')
    if col("total funded assets"):
        set_cell(ws, total_row, col("total funded assets"), tot_props, "0")
    if col("paid off assets"):
        set_cell(ws, total_row, col("paid off assets"), tot_paid, "0")
    if col("active assets"):
        set_cell(ws, total_row, col("active assets"), tot_active, "0")

def sanitize_filename(name: str) -> str:
    s = "" if name is None or pd.isna(name) else str(name).strip()
    s = re.sub(r'[<>:"/\\|?*]', "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:120] if s else "AM"

# -------------------------
# Pull + build normalized TERM/BRIDGE rows for Excel writer
# -------------------------
def build_term_bridge_from_salesforce(sf):
    print("Search by:\n  1) Account Name\n  2) Deal Name\n  3) Deal Loan Number")
    mode = (input("> ").strip() or "1")
    q = input("Enter search text: ").strip()

    # ✅ include RecordType.DeveloperName in both preview + main query
    opp_preview_fields = ["Id","Name","Deal_Loan_Number__c","Account_Name__c","RecordType.Name","RecordType.DeveloperName","StageName","CloseDate"]
    where_search = build_where_for_search(mode, q) + " AND StageName IN (" + ", ".join(soql_quote(s) for s in VALID_STAGES) + ")"
    preview_rows, _, _ = try_query_drop_missing(sf, "Opportunity", opp_preview_fields, where_search, limit=2000, order_by="CloseDate DESC NULLS LAST")
    if not preview_rows:
        print("No loans found for that search + stages.")
        return pd.DataFrame(), pd.DataFrame(), ""

    df_prev = pd.DataFrame(preview_rows).drop(columns=["attributes"], errors="ignore")
    df_prev = safe_flatten_recordtype(df_prev)

    acct_counts = (
        df_prev.groupby("Account_Name__c", dropna=False)
               .size()
               .reset_index(name="loans")
               .sort_values(["loans","Account_Name__c"], ascending=[False, True])
               .reset_index(drop=True)
    )
    print("\nAccount candidates from matches (pick the right account):")
    display(acct_counts.head(30))

    pick = input("Pick account index (ENTER = 0): ").strip()
    pick_idx = int(pick) if pick.isdigit() else 0
    acct_name = acct_counts.iloc[pick_idx]["Account_Name__c"]

    opp_fields = [
        "Id","Name","Deal_Loan_Number__c","Account_Name__c","RecordType.Name","RecordType.DeveloperName","StageName",
        "CloseDate","Amount","Current_UPB__c","UW_LTV__c","Rate__c","Current_Svc_Interest_Rate__c",
        "State_Percentages__c","Total_Properties__c","Total_Units__c","Recourse__c",
        "Historical_Ontime_Payments_Percentage__c","Next_Payment_Date__c",
        "Stated_Maturity_Date__c","Original_Line_Maturity_Date__c","Aggregate_Funding__c"
    ]
    where_acct = "Account_Name__c = " + soql_quote(acct_name) + " AND StageName IN (" + ", ".join(soql_quote(s) for s in VALID_STAGES) + ")"
    rows, _, _ = try_query_drop_missing(sf, "Opportunity", opp_fields, where_acct, limit=2000, order_by="CloseDate DESC NULLS LAST")

    df_all = pd.DataFrame(rows).drop(columns=["attributes"], errors="ignore")
    df_all = safe_flatten_recordtype(df_all)

    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame(), acct_name

    df_all["InterestRate_Picked"] = df_all.get("Rate__c")
    mblank = df_all["InterestRate_Picked"].isna() | (df_all["InterestRate_Picked"].astype(str).str.strip() == "")
    df_all.loc[mblank, "InterestRate_Picked"] = df_all.loc[mblank, "Current_Svc_Interest_Rate__c"]

    df_all["LTV_Dec"] = df_all.get("UW_LTV__c").apply(pct_to_dec) if "UW_LTV__c" in df_all.columns else None
    df_all["Rate_Dec"] = df_all["InterestRate_Picked"].apply(pct_to_dec)

    df_all["OriginationDate_dt"] = df_all.get("CloseDate").apply(parse_date_any)
    df_all["NextPay_dt"] = df_all.get("Next_Payment_Date__c").apply(parse_date_any)

    df_all["Maturity_Picked_raw"] = df_all.get("Stated_Maturity_Date__c")
    mb = df_all["Maturity_Picked_raw"].isna() | (df_all["Maturity_Picked_raw"].astype(str).str.strip() == "")
    df_all.loc[mb, "Maturity_Picked_raw"] = df_all.loc[mb, "Original_Line_Maturity_Date__c"]
    df_all["Maturity_dt"] = df_all["Maturity_Picked_raw"].apply(parse_date_any)

    # ✅ classification with Single Asset Bridge Loan support
    df_term_raw, df_bridge_opp = classify_term_bridge(df_all)

    # Term guarantors from Deal_Contact__c
    if not df_term_raw.empty:
        term_ids = df_term_raw["Id"].dropna().astype(str).unique().tolist()
        df_dc = query_deal_contacts_for_guarantors(sf, term_ids)
        if not df_dc.empty:
            gmap = (
                df_dc.groupby(DC_DEAL_FIELD)["GuarantorName"]
                     .apply(lambda s: ", ".join(pd.unique([x for x in s.tolist() if str(x).strip() != ""])))
                     .reset_index()
                     .rename(columns={DC_DEAL_FIELD: "Id", "GuarantorName": "Guarantor"})
            )
            df_term_raw = df_term_raw.merge(gmap, on="Id", how="left")
        else:
            df_term_raw["Guarantor"] = ""
    else:
        df_term_raw["Guarantor"] = ""

    # TERM normalized output
    df_term = pd.DataFrame()
    if not df_term_raw.empty:
        df_term["Loan ID"] = df_term_raw["Deal_Loan_Number__c"].apply(lambda x: str(last5_strip_prefix(x)).zfill(5) if str(last5_strip_prefix(x)).strip() else "")
        df_term["Loan"] = df_term_raw.get("Name", "")
        df_term["Account Name"] = df_term_raw.get("Account_Name__c", "")
        df_term["Guarantor"] = df_term_raw.get("Guarantor", "").fillna("")
        df_term["Origination Date"] = df_term_raw.get("OriginationDate_dt")
        df_term["Maturity Date"] = df_term_raw.get("Maturity_dt")
        df_term["Next Payment Date"] = df_term_raw.get("NextPay_dt")
        df_term["Loan Amount Num"] = pd.to_numeric(df_term_raw.get("Amount"), errors="coerce")
        df_term["Outstanding Balance Num"] = pd.to_numeric(df_term_raw.get("Current_UPB__c"), errors="coerce")
        df_term["LTV Dec"] = df_term_raw.get("LTV_Dec")
        df_term["Rate Dec"] = df_term_raw.get("Rate_Dec")
        df_term["State(s)"] = df_term_raw.get("State_Percentages__c").apply(extract_states_only)
        df_term["Total Properties Num"] = pd.to_numeric(df_term_raw.get("Total_Properties__c"), errors="coerce").round(0).astype("Int64")
        df_term["Total Units Num"] = pd.to_numeric(df_term_raw.get("Total_Units__c"), errors="coerce").round(0).astype("Int64")
        df_term["Recourse"] = df_term_raw.get("Recourse__c")
        df_term["Historical Ontime % Dec"] = df_term_raw.get("Historical_Ontime_Payments_Percentage__c").apply(pct_to_dec)
        df_term = df_term.sort_values(["Origination Date","Loan ID"], ascending=[False, True], kind="stable").reset_index(drop=True)

    # BRIDGE rollups
    df_bridge = pd.DataFrame()
    if not df_bridge_opp.empty:
        deal_ids = df_bridge_opp["Id"].dropna().astype(str).unique().tolist()

        ADV_FIELDS = ["Id","Deal__c","Advance_Num__c","LOC_Commitment__c","Wire_Date__c"]
        adv_rows_all = []
        for ch in chunked(deal_ids, 200):
            where_adv = f"Deal__c IN ({', '.join(soql_quote(x) for x in ch)})"
            rows_adv, _, _ = try_query_drop_missing(sf, "Advance__c", ADV_FIELDS, where_adv, limit=2000, order_by="CreatedDate DESC")
            adv_rows_all.extend(rows_adv)
        df_adv = pd.DataFrame(adv_rows_all).drop(columns=["attributes"], errors="ignore")

        if df_adv.empty:
            df_bridge["Loan ID"] = df_bridge_opp["Deal_Loan_Number__c"].apply(lambda x: str(last5_strip_prefix(x)).zfill(5) if str(last5_strip_prefix(x)).strip() else "")
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
            df_bridge["Lifetime Funded Num"] = pd.to_numeric(df_bridge_opp.get("Aggregate_Funding__c"), errors="coerce")
            df_bridge["Paid Off Assets Num"] = None
            df_bridge["Active Assets Num"] = None
            df_bridge["Outstanding Balance Num"] = pd.to_numeric(df_bridge_opp.get("Current_UPB__c"), errors="coerce")
            df_bridge["As-Is/ ARV Num"] = None
            df_bridge["Avg Hold Time Num"] = None
            df_bridge["Avg Disposed Time Num"] = None
        else:
            df_adv["Advance_Num__c"] = pd.to_numeric(df_adv.get("Advance_Num__c"), errors="coerce")
            df_adv["LOC_Commitment__c"] = pd.to_numeric(df_adv.get("LOC_Commitment__c"), errors="coerce")
            df_adv["Wire_Date__c_dt"] = pd.to_datetime(df_adv.get("Wire_Date__c"), errors="coerce")

            adv_roll = (
                df_adv.groupby("Deal__c", dropna=False)
                      .agg(Commitment=("LOC_Commitment__c","max"), Advances=("Advance_Num__c","max"))
                      .reset_index()
            )

            PROP_FIELDS = [
                "Id","Advance__c","Deal__c","Payoff_Received_Date__c",
                "After_Repair_Value__c","Appraised_Value_Amount__c"
            ]
            prop_rows_all = []
            adv_ids = df_adv["Id"].dropna().astype(str).unique().tolist()
            for ch in chunked(adv_ids, 200):
                where_prop = f"Advance__c IN ({', '.join(soql_quote(x) for x in ch)})"
                rows_prop, _, _ = try_query_drop_missing(sf, "Property__c", PROP_FIELDS, where_prop, limit=2000, order_by="CreatedDate DESC")
                prop_rows_all.extend(rows_prop)
            df_prop = pd.DataFrame(prop_rows_all).drop(columns=["attributes"], errors="ignore")

            today_dt = pd.to_datetime(date.today())

            if df_prop.empty:
                prop_metrics = pd.DataFrame(columns=[
                    "Deal__c","Total_Assets","Paid_Off","Active","AsIs_ARV","Avg_Hold","Avg_Disposed"
                ])
            else:
                df_prop["Payoff_dt"] = pd.to_datetime(df_prop.get("Payoff_Received_Date__c"), errors="coerce")

                df_prop["ARV_num"] = pd.to_numeric(df_prop.get("After_Repair_Value__c"), errors="coerce")
                m = df_prop["ARV_num"].isna()
                df_prop.loc[m, "ARV_num"] = pd.to_numeric(df_prop.get("Appraised_Value_Amount__c"), errors="coerce")

                df_prop = df_prop.merge(
                    df_adv[["Id","Deal__c","Wire_Date__c_dt"]],
                    left_on="Advance__c",
                    right_on="Id",
                    how="left",
                    suffixes=("", "_adv"),
                )

                prop_id_col = "Id_x" if "Id_x" in df_prop.columns else "Id"

                earliest_wire = (
                    df_prop.groupby(["Deal__c", prop_id_col], dropna=False)["Wire_Date__c_dt"]
                           .min()
                           .reset_index()
                           .rename(columns={prop_id_col:"PropertyId", "Wire_Date__c_dt":"EarliestWire"})
                )

                payoff_per_asset = (
                    df_prop.groupby(["Deal__c", prop_id_col], dropna=False)["Payoff_dt"]
                           .min()
                           .reset_index()
                           .rename(columns={prop_id_col:"PropertyId", "Payoff_dt":"Payoff"})
                )

                arv_per_asset = (
                    df_prop.groupby(["Deal__c", prop_id_col], dropna=False)["ARV_num"]
                           .max()
                           .reset_index()
                           .rename(columns={prop_id_col:"PropertyId", "ARV_num":"ARV"})
                )

                assets = earliest_wire.merge(payoff_per_asset, on=["Deal__c","PropertyId"], how="left").merge(arv_per_asset, on=["Deal__c","PropertyId"], how="left")

                assets["Is_Active"] = assets["Payoff"].isna()
                assets["Is_PaidOff"] = assets["Payoff"].notna()

                assets["Hold_Days"] = (today_dt - assets["EarliestWire"]).dt.days
                assets.loc[assets["EarliestWire"].isna(), "Hold_Days"] = pd.NA

                assets["Disposed_Days"] = (assets["Payoff"] - assets["EarliestWire"]).dt.days
                assets.loc[assets["Payoff"].isna() | assets["EarliestWire"].isna(), "Disposed_Days"] = pd.NA

                base = (
                    assets.groupby("Deal__c", dropna=False)
                          .agg(
                              Total_Assets=("PropertyId","nunique"),
                              Paid_Off=("Is_PaidOff","sum"),
                              Active=("Is_Active","sum"),
                              AsIs_ARV=("ARV","sum"),
                          )
                          .reset_index()
                )

                hold = (
                    assets[assets["Is_Active"]]
                    .groupby("Deal__c", dropna=False)
                    .agg(Avg_Hold=("Hold_Days","mean"))
                    .reset_index()
                )

                disp = (
                    assets[assets["Is_PaidOff"]]
                    .groupby("Deal__c", dropna=False)
                    .agg(Avg_Disposed=("Disposed_Days","mean"))
                    .reset_index()
                )

                prop_metrics = base.merge(hold, on="Deal__c", how="left").merge(disp, on="Deal__c", how="left")

            b = df_bridge_opp.copy()
            b = b.merge(adv_roll, left_on="Id", right_on="Deal__c", how="left").drop(columns=["Deal__c"], errors="ignore")
            b = b.merge(prop_metrics, left_on="Id", right_on="Deal__c", how="left").drop(columns=["Deal__c"], errors="ignore")

            df_bridge["Loan ID"] = b["Deal_Loan_Number__c"].apply(lambda x: str(last5_strip_prefix(x)).zfill(5) if str(last5_strip_prefix(x)).strip() else "")
            df_bridge["Loan"] = b.get("Name", "")
            df_bridge["Account Name"] = b.get("Account_Name__c", "")
            df_bridge["Commitment Amount Num"] = pd.to_numeric(b.get("Commitment"), errors="coerce")
            df_bridge["Origination Date"] = b.get("OriginationDate_dt")
            df_bridge["Maturity Date"] = b.get("Maturity_dt")
            df_bridge["Rate Dec"] = b.get("Rate_Dec")
            df_bridge["LTV Dec"] = b.get("LTV_Dec")
            df_bridge["Advances Num"] = pd.to_numeric(b.get("Advances"), errors="coerce").round(0).astype("Int64")
            df_bridge["Total Properties Num"] = pd.to_numeric(b.get("Total_Assets"), errors="coerce").round(0).astype("Int64")
            df_bridge["State(s)"] = b.get("State_Percentages__c").apply(extract_states_only)
            df_bridge["Lifetime Funded Num"] = pd.to_numeric(b.get("Aggregate_Funding__c"), errors="coerce")
            df_bridge["Paid Off Assets Num"] = pd.to_numeric(b.get("Paid_Off"), errors="coerce").round(0).astype("Int64")
            df_bridge["Active Assets Num"] = pd.to_numeric(b.get("Active"), errors="coerce").round(0).astype("Int64")
            df_bridge["Outstanding Balance Num"] = pd.to_numeric(b.get("Current_UPB__c"), errors="coerce")
            df_bridge["As-Is/ ARV Num"] = pd.to_numeric(b.get("AsIs_ARV"), errors="coerce")
            df_bridge["Avg Hold Time Num"] = pd.to_numeric(b.get("Avg_Hold"), errors="coerce").round(0).astype("Int64")
            df_bridge["Avg Disposed Time Num"] = pd.to_numeric(b.get("Avg_Disposed"), errors="coerce").round(0).astype("Int64")

        df_bridge = df_bridge.sort_values(["Origination Date","Loan ID"], ascending=[False, True], kind="stable").reset_index(drop=True)

    return df_term, df_bridge, acct_name

# -------------------------
# MAIN
# -------------------------
def main():
    term_rows, bridge_rows, acct_name = build_term_bridge_from_salesforce(sf)

    if (term_rows is None or term_rows.empty) and (bridge_rows is None or bridge_rows.empty):
        print("\nNo matches found after account selection.")
        return pd.DataFrame(), pd.DataFrame()

    print(f"\nSelected Account: {acct_name}")
    print(f"  Term loans:   {0 if term_rows is None else len(term_rows)}")
    print(f"  Bridge loans: {0 if bridge_rows is None else len(bridge_rows)}")

    print("\n=== TERM (normalized for Excel writer) ===")
    display(term_rows)

    print("\n=== BRIDGE (normalized for Excel writer) ===")
    display(bridge_rows)

    make_slides = input("\nCreate AM Slides workbook now? (y/N): ").strip().lower() == "y"
    if not make_slides:
        print("Cancelled (no workbook created). Returning dataframes.")
        return term_rows, bridge_rows

    wb = load_workbook(TEMPLATE_PATH)

    if TERM_SHEET in wb.sheetnames:
        write_term_sheet(wb[TERM_SHEET], term_rows, guarantor="")
    else:
        print(f"WARNING: Template missing '{TERM_SHEET}' sheet.")

    if BRIDGE_SHEET in wb.sheetnames:
        write_bridge_sheet(wb[BRIDGE_SHEET], bridge_rows)
    else:
        print(f"WARNING: Template missing '{BRIDGE_SHEET}' sheet.")

    out_dir = Path(TEMPLATE_PATH).parent
    out_path = out_dir / f"{sanitize_filename(acct_name)} AM Slides.xlsx"
    wb.save(out_path)

    print(f"\nSaved: {out_path}")
    return term_rows, bridge_rows

term_rows, bridge_rows = main()
term_rows, bridge_rows

# ============================================================
# APPEND-ON CELL (run this RIGHT AFTER your existing AM Slides code)
# Uses existing: sf, bridge_rows, term_rows (no rewrites)
#
# Goal:
# 1) Pull Loan_Modification__c records tied to the bridge loans in bridge_rows
# 2) Create a bridge-level summary of modifications (counts, latest dates, key flags)
# 3) Merge that summary back onto bridge_rows
# 4) Also expose Historical_Ontime_Payments_Percentage__c for bridge loans (if not already in bridge_rows)
# ============================================================

import pandas as pd
from IPython.display import display
import re

pd.set_option("display.max_rows", 5000)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

# -------------------------
# Helpers
# -------------------------
def soql_quote(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"

def chunked(vals, n=200):
    vals = list(vals)
    for i in range(0, len(vals), n):
        yield vals[i:i+n]

def pct_to_dec(x):
    if x in ("", None) or pd.isna(x):
        return None
    s = str(x).strip().replace("%", "")
    try:
        v = float(s)
        return v / 100.0 if v > 1.5 else v
    except Exception:
        return None

# -------------------------
# Guardrails
# -------------------------
if "bridge_rows" not in globals() or bridge_rows is None or bridge_rows.empty:
    raise RuntimeError("bridge_rows is missing/empty. Run your main AM Slides cell first so bridge_rows exists.")

# Your bridge_rows "Loan ID" is 5-digit internal display, not SF Id.
# We need the Opportunity Ids to join to Loan_Modification__c (Deal__c reference -> Opportunity)
# So we re-query Opportunity by Deal_Loan_Number__c suffix match (best-effort) and map to Id.
bridge_loan_ids = (
    bridge_rows.get("Loan ID", pd.Series([], dtype="object"))
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

if not bridge_loan_ids:
    raise RuntimeError("No Loan ID values found in bridge_rows to look up Opportunity IDs.")

# -------------------------
# Step 1: Find the bridge Opportunities (Ids) for these Loan IDs
# -------------------------
opp_fields = [
    "Id",
    "Name",
    "Deal_Loan_Number__c",
    "Account_Name__c",
    "StageName",
    "CloseDate",
    "RecordType.Name",
    "RecordType.DeveloperName",
    "Historical_Ontime_Payments_Percentage__c",  # <-- bridge on-time %
]

opp_rows_all = []

# We can't safely do "IN" on a suffix, so we do batched OR LIKE on the last 5 digits
# (Deal_Loan_Number__c is often longer/prefixed; your code strips prefixes & keeps last 5)
for ch in chunked(bridge_loan_ids, 40):  # keep SOQL length reasonable
    like_clauses = []
    for lid in ch:
        digits = re.sub(r"\D", "", str(lid))
        if not digits:
            continue
        # match anything ending in those digits, or containing those digits
        like_clauses.append(f"Deal_Loan_Number__c LIKE {soql_quote('%' + digits)}")
        like_clauses.append(f"Deal_Loan_Number__c LIKE {soql_quote('%' + digits + '%')}")
    if not like_clauses:
        continue

    where = "(" + " OR ".join(like_clauses) + ")"
    soql = f"SELECT {', '.join(opp_fields)} FROM Opportunity WHERE {where} LIMIT 2000"
    res = sf.query_all(soql).get("records", [])
    opp_rows_all.extend(res)

df_bridge_opps = pd.DataFrame(opp_rows_all).drop(columns=["attributes"], errors="ignore")

if df_bridge_opps.empty:
    print("No Opportunity matches found for bridge_rows Loan IDs (suffix search).")
    df_bridge_opps = pd.DataFrame(columns=opp_fields)

# Normalize RecordType fields if present as nested dict (rare, but safe)
if "RecordType" in df_bridge_opps.columns:
    df_bridge_opps["RecordType.Name"] = df_bridge_opps["RecordType"].apply(lambda x: (x or {}).get("Name"))
    df_bridge_opps["RecordType.DeveloperName"] = df_bridge_opps["RecordType"].apply(lambda x: (x or {}).get("DeveloperName"))
    df_bridge_opps = df_bridge_opps.drop(columns=["RecordType"], errors="ignore")

# Create a join key that matches your bridge_rows "Loan ID" (last 5 digits)
def last5(x):
    d = re.sub(r"\D", "", "" if x is None or pd.isna(x) else str(x))
    return d[-5:] if len(d) >= 5 else d

df_bridge_opps["LoanID_5"] = df_bridge_opps["Deal_Loan_Number__c"].apply(last5).astype(str).str.zfill(5)

# De-dupe: keep the most recent CloseDate per LoanID_5 (or first if missing)
df_bridge_opps["CloseDate_dt"] = pd.to_datetime(df_bridge_opps.get("CloseDate"), errors="coerce")
df_bridge_opps = (
    df_bridge_opps.sort_values(["LoanID_5", "CloseDate_dt"], ascending=[True, False])
                 .drop_duplicates(subset=["LoanID_5"], keep="first")
                 .reset_index(drop=True)
)

# Prepare on-time % as decimal (optional formatting later)
df_bridge_opps["HistOntime_Dec"] = df_bridge_opps.get("Historical_Ontime_Payments_Percentage__c").apply(pct_to_dec)

print("Bridge Opportunity matches found:", len(df_bridge_opps))
display(df_bridge_opps[["LoanID_5","Id","Name","RecordType.Name","RecordType.DeveloperName","Historical_Ontime_Payments_Percentage__c"]].head(50))

# -------------------------
# Step 2: Pull Loan_Modification__c for these Opportunity Ids
# -------------------------
opp_ids = df_bridge_opps["Id"].dropna().astype(str).unique().tolist()
if not opp_ids:
    print("No Opportunity Ids found to query Loan_Modification__c.")
    df_mods = pd.DataFrame()
else:
    # Fields: pick the ones that are most useful for "keeping track of modifications"
    # (you can add more later, but these are the core tracking fields)
    MOD_FIELDS = [
        "Id",
        "Name",
        "Deal__c",                        # link back to Opportunity
        "Mod_Status__c",
        "Modification_Type__c",
        "Loan_Mod_Type__c",
        "Mod_Reporting_Type__c",
        "Remodification__c",
        "Exclude_from_Reporting__c",
        "Include_In_Pipeline__c",
        "Mod_Effective_Date__c",
        "Modification_Finalized_Date__c",
        "Modification_Maturity_Date__c",
        "Updated_Expiration_Date__c",
        "Previous_Maturity_Date__c",
        "Previous_Expiration_Date__c",
        "Updated_LOC_Commitment__c",
        "Previous_LOC_Commitment__c",
        "Updated_Interest_Rate__c",
        "Previous_Interest_Rate__c",
        "Updated_Max_LTV__c",
        "Previous_Max_LTV__c",
        "Comments__c",
        "CreatedDate",
        "LastModifiedDate",
    ]

    mod_rows_all = []
    for ch in chunked(opp_ids, 150):
        ids_in = ", ".join(soql_quote(x) for x in ch)
        where = f"Deal__c IN ({ids_in})"
        soql = f"SELECT {', '.join(MOD_FIELDS)} FROM Loan_Modification__c WHERE {where} ORDER BY Mod_Effective_Date__c DESC NULLS LAST LIMIT 2000"
        res = sf.query_all(soql).get("records", [])
        mod_rows_all.extend(res)

    df_mods = pd.DataFrame(mod_rows_all).drop(columns=["attributes"], errors="ignore")

print("\nLoan_Modification__c rows:", 0 if df_mods is None else len(df_mods))
if df_mods is not None and not df_mods.empty:
    # Parse a few dates for summaries
    for col in ["Mod_Effective_Date__c","Modification_Finalized_Date__c","Modification_Maturity_Date__c","Updated_Expiration_Date__c","CreatedDate","LastModifiedDate"]:
        if col in df_mods.columns:
            df_mods[col + "_dt"] = pd.to_datetime(df_mods[col], errors="coerce")

    display(df_mods.head(100))
else:
    df_mods = pd.DataFrame(columns=["Deal__c"])

# -------------------------
# Step 3: Build a bridge-level modification summary (per Opportunity / per LoanID_5)
# -------------------------
if df_mods.empty:
    df_mod_summary = pd.DataFrame(columns=[
        "Deal__c",
        "Mod_Count",
        "Latest_Mod_Effective_Date",
        "Latest_Mod_Status",
        "Latest_Mod_Type",
        "Any_Remodification",
        "Any_Exclude_From_Reporting",
        "Any_Include_In_Pipeline",
        "Latest_Updated_Maturity",
        "Latest_Updated_Expiration",
        "Latest_Updated_Commitment",
        "Latest_Updated_Interest_Rate",
        "Latest_Comments",
    ])
else:
    # pick "latest" mod per deal using effective date, else created date
    df_mods["_sort_dt"] = df_mods.get("Mod_Effective_Date__c_dt")
    if "_sort_dt" in df_mods.columns:
        df_mods["_sort_dt"] = df_mods["_sort_dt"].fillna(df_mods.get("CreatedDate_dt"))
    else:
        df_mods["_sort_dt"] = df_mods.get("CreatedDate_dt")

    df_mods_sorted = df_mods.sort_values(["Deal__c","_sort_dt"], ascending=[True, False])

    latest = df_mods_sorted.drop_duplicates(subset=["Deal__c"], keep="first").copy()

    df_mod_summary = (
        df_mods.groupby("Deal__c", dropna=False)
               .agg(
                   Mod_Count=("Id", "count"),
                   Any_Remodification=("Remodification__c", lambda s: bool(pd.Series(s).fillna(False).astype(bool).any())),
                   Any_Exclude_From_Reporting=("Exclude_from_Reporting__c", lambda s: bool(pd.Series(s).fillna(False).astype(bool).any())),
                   Any_Include_In_Pipeline=("Include_In_Pipeline__c", lambda s: bool(pd.Series(s).fillna(False).astype(bool).any())),
               )
               .reset_index()
    )

    # attach latest fields
    latest_cols = {
        "Mod_Effective_Date__c": "Latest_Mod_Effective_Date",
        "Mod_Status__c": "Latest_Mod_Status",
        "Modification_Type__c": "Latest_Mod_Type",
        "Modification_Maturity_Date__c": "Latest_Updated_Maturity",
        "Updated_Expiration_Date__c": "Latest_Updated_Expiration",
        "Updated_LOC_Commitment__c": "Latest_Updated_Commitment",
        "Updated_Interest_Rate__c": "Latest_Updated_Interest_Rate",
        "Comments__c": "Latest_Comments",
    }
    tmp = latest[["Deal__c"] + [c for c in latest_cols.keys() if c in latest.columns]].copy()
    tmp = tmp.rename(columns=latest_cols)

    df_mod_summary = df_mod_summary.merge(tmp, on="Deal__c", how="left")

print("\nModification summary (per Opportunity):", len(df_mod_summary))
display(df_mod_summary.head(100))

# -------------------------
# Step 4: Merge mod summary + bridge on-time % onto bridge_rows
# -------------------------
# Map Opportunity Id -> LoanID_5 so we can merge by your bridge_rows Loan ID
df_join = df_bridge_opps[["Id","LoanID_5","HistOntime_Dec","Historical_Ontime_Payments_Percentage__c","RecordType.Name","RecordType.DeveloperName","Name"]].copy()
df_join = df_join.rename(columns={"Id":"Deal__c", "Name":"SF Deal Name"})

df_bridge_enriched = bridge_rows.copy()
df_bridge_enriched["LoanID_5"] = df_bridge_enriched["Loan ID"].astype(str).str.zfill(5)

df_bridge_enriched = df_bridge_enriched.merge(df_join, on="LoanID_5", how="left")
df_bridge_enriched = df_bridge_enriched.merge(df_mod_summary, on="Deal__c", how="left")

# Friendly columns to inspect
cols_show = [
    "Loan ID", "Loan", "Account Name",
    "Historical_Ontime_Payments_Percentage__c", "HistOntime_Dec",
    "Mod_Count", "Latest_Mod_Effective_Date", "Latest_Mod_Status", "Latest_Mod_Type",
    "Any_Remodification", "Any_Exclude_From_Reporting", "Any_Include_In_Pipeline",
    "Latest_Updated_Maturity", "Latest_Updated_Expiration",
    "Latest_Updated_Commitment", "Latest_Updated_Interest_Rate",
    "SF Deal Name", "RecordType.Name", "RecordType.DeveloperName",
]
cols_show = [c for c in cols_show if c in df_bridge_enriched.columns]

print("\nBridge rows enriched with on-time % + modification tracking columns:", len(df_bridge_enriched))
display(df_bridge_enriched[cols_show].sort_values(["Mod_Count","Loan ID"], ascending=[False, True]).head(250))

# Return the enriched df for further use
df_bridge_enriched