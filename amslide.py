from __future__ import annotations

import io
import json
import re
import base64
import hashlib
import secrets
from copy import copy
from datetime import date, datetime
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
    for key in [
        "salesforce_auth",
        "account_candidates",
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
    return df[[DC_DEAL_FIELD, "GuarantorName"]].copy()



def build_term_bridge_for_account(sf: Salesforce, account_name: str):
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

    where_account = (
        "Account_Name__c = "
        + soql_quote(account_name)
        + " AND StageName IN ("
        + ", ".join(soql_quote(stage) for stage in VALID_STAGES)
        + ")"
    )

    rows, _, _ = try_query_drop_missing(
        sf,
        "Opportunity",
        opportunity_fields,
        where_account,
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
                        pd.unique([item for item in s.tolist() if str(item).strip() != ""])
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
    for r in range(1, min(ws.max_row, scan_rows) + 1):
        row_vals = [norm_hdr(ws.cell(r, c).value) for c in range(1, min(ws.max_column, scan_cols) + 1)]
        if required.issubset(set(row_vals)):
            return r
    raise ValueError(
        f"Could not find a header row on '{ws.title}' containing {sorted(required_headers)} in the first {scan_rows} rows."
    )



def load_financial_analysis_df_from_bytes(berkadia_bytes: bytes) -> pd.DataFrame:
    xls = pd.ExcelFile(io.BytesIO(berkadia_bytes), engine="openpyxl")
    sheet_name = find_sheet_name_case_insensitive(xls.sheet_names, "Financial Analysis")
    if not sheet_name:
        raise ValueError(
            f"Could not find a sheet named 'Financial Analysis' (case-insensitive). Sheets found: {xls.sheet_names}"
        )

    workbook = load_workbook(io.BytesIO(berkadia_bytes), read_only=True, data_only=True)
    try:
        ws = workbook[sheet_name]
        header_row = detect_header_row_in_uploaded_sheet(
            ws,
            required_headers=["Investor Loan#", "Freq of Analysis", "Period End Date", "Occupancy %"],
        )
    finally:
        workbook.close()

    df = pd.read_excel(
        io.BytesIO(berkadia_bytes),
        sheet_name=sheet_name,
        header=header_row - 1,
        engine="openpyxl",
    )
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


@st.cache_data(show_spinner=False)
def load_occupancy_lookup_cached(berkadia_bytes: bytes, target_quarter_keys: tuple[str, ...]):
    return build_term_occupancy_lookup(berkadia_bytes, list(target_quarter_keys))



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
            set_cell(ws, row_num, col("guarantor"), row.get("Guarantor", "") or guarantor)
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
    if col("total properties"):
        set_cell(ws, total_row, col("total properties"), total_properties, "0")
    if col("total units"):
        set_cell(ws, total_row, col("total units"), total_units, "0")



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



def sanitize_filename(name: str) -> str:
    text = "" if name is None or pd.isna(name) else str(name).strip()
    text = re.sub(r'[<>:"/\\|?*]', "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:120] if text else "AM"



def resolve_repo_template_path() -> Path:
    candidate_paths = [
        Path(__file__).resolve().parent / DEFAULT_TEMPLATE_NAME,
        Path.cwd() / DEFAULT_TEMPLATE_NAME,
        Path(__file__).resolve().parent / "templates" / DEFAULT_TEMPLATE_NAME,
        Path.cwd() / "templates" / DEFAULT_TEMPLATE_NAME,
    ]
    for candidate_path in candidate_paths:
        if candidate_path.exists():
            return candidate_path

    raise RuntimeError(
        "Could not find 'Reference AM Templates.xlsx' in the repository. Place it next to amslide.py or inside a templates folder in the repo."
    )


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
st.title("AM Slides Builder")
st.caption("Build the AM Slides workbook from Salesforce and a Berkadia Financial Analysis export.")

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

st.subheader("Step 2: Upload the Berkadia servicer file")
st.caption("The AM template workbook is loaded automatically from the repository. You do not need to upload it here.")
if occ_headers:
    st.caption("Template occupancy quarters: " + ", ".join(target_quarters))
else:
    st.caption("The Term template has no dated occupancy columns (e.g. '2024 Q1 Occ%'), so occupancy will be left blank.")

berkadia_file = st.file_uploader(
    "Upload Berkadia servicer file",
    type=["xlsx", "xlsm"],
    key="berkadia_file",
    help="Use the servicer workbook that contains the Financial Analysis sheet.",
)

if berkadia_file is None:
    st.info("Upload the Berkadia servicer file to continue.")
    st.stop()

occupancy_lookup = {}
occupancy_selected = pd.DataFrame()
occupancy_issues = pd.DataFrame()
occupancy_fa = pd.DataFrame()
fa_row_count = 0
try:
    occupancy_lookup, occupancy_selected, occupancy_issues, occupancy_fa = load_occupancy_lookup_cached(
        berkadia_file.getvalue(),
        target_quarters,
    )
    fa_row_count = len(occupancy_fa) if isinstance(occupancy_fa, pd.DataFrame) else int(occupancy_fa)
    st.success(
        f"Berkadia file loaded. Financial Analysis rows read: {fa_row_count}. "
        f"Selected occupancy rows: {len(occupancy_selected)}."
    )
except Exception as exc:
    st.error(str(exc))
    st.stop()

st.session_state["occupancy_selected"] = occupancy_selected
st.session_state["occupancy_issues"] = occupancy_issues

if isinstance(occupancy_selected, pd.DataFrame) and not occupancy_selected.empty:
    with st.expander("Review selected occupancy rows", expanded=False):
        review_cols = [
            "Loan ID 5",
            "Quarter Key",
            "Occupancy Dec",
            "Freq Norm",
            "Months Num",
            "Selection Source",
            "Investor Loan#",
        ]
        review_cols = [c for c in review_cols if c in occupancy_selected.columns]
        review = occupancy_selected[review_cols].copy()
        if "Occupancy Dec" in review.columns:
            review["Occupancy Dec"] = review["Occupancy Dec"].apply(
                lambda x: "" if pd.isna(x) else f"{x:.1%}"
            )
        st.dataframe(
            review.sort_values([c for c in ["Loan ID 5", "Quarter Key"] if c in review.columns]),
            use_container_width=True,
            hide_index=True,
        )

if isinstance(occupancy_issues, pd.DataFrame) and not occupancy_issues.empty:
    with st.expander("Occupancy selection notes", expanded=False):
        issues_display = occupancy_issues.copy()
        if "Candidate Rows" in issues_display.columns:
            issues_display["Candidate Rows"] = issues_display["Candidate Rows"].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, (list, dict)) else x
            )
        st.dataframe(issues_display, use_container_width=True, hide_index=True)

st.subheader("Step 3: Search Salesforce and choose an account")
search_col1, search_col2 = st.columns([1, 2])
with search_col1:
    search_mode = st.selectbox(
        "Search Salesforce by",
        ["Account Name", "Deal Name", "Deal Loan Number"],
    )
with search_col2:
    search_text = st.text_input("Search text")

if st.button("Search Salesforce", type="secondary"):
    if not search_text.strip():
        st.error("Enter a search value first.")
    else:
        try:
            account_candidates = search_matching_accounts(sf, search_mode, search_text)
            st.session_state["account_candidates"] = account_candidates
            if account_candidates.empty:
                st.warning("No matching accounts found.")
            else:
                st.success(f"Found {len(account_candidates)} matching account candidates.")
        except Exception as exc:
            st.error(str(normalize_salesforce_error(exc)))

account_candidates = st.session_state.get("account_candidates", pd.DataFrame())
selected_account = None
if isinstance(account_candidates, pd.DataFrame) and not account_candidates.empty:
    st.dataframe(account_candidates, use_container_width=True, hide_index=True)
    selected_account = st.selectbox(
        "Pick the account for the AM slide",
        options=account_candidates["Account_Name__c"].tolist(),
    )
else:
    st.info("Search Salesforce to load the account list.")

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

build_disabled = not bool(selected_account)
if st.button("Build completed AM slide", type="primary", disabled=build_disabled):
    try:
        with st.spinner("Building the AM slide workbook..."):
            term_rows, bridge_rows, bridge_fci_rows = build_term_bridge_for_account(sf, selected_account)
            if term_rows.empty and bridge_rows.empty:
                raise RuntimeError("No term or bridge rows were returned for the selected account.")

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
                selected_account,
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
        st.session_state["fci_ran"] = include_fci
        st.session_state["fci_alerts"] = fci_alerts
        st.session_state["fci_matches"] = fci_matches

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
    )

if isinstance(term_preview, pd.DataFrame) and not term_preview.empty:
    st.subheader("Term preview")
    st.dataframe(term_preview, use_container_width=True, hide_index=True)

if isinstance(bridge_preview, pd.DataFrame) and not bridge_preview.empty:
    st.subheader("Bridge preview")
    st.dataframe(bridge_preview, use_container_width=True, hide_index=True)

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
