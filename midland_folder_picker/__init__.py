"""Select CAF archives by filename before transferring their contents."""

import json
from pathlib import Path

import streamlit as st
from streamlit.components.v1 import declare_component


STATE_KEY = "_midland_folder_upload"
_picker = declare_component(
    "midland_folder_upload", path=str(Path(__file__).parent / "frontend")
)


def new_upload_state():
    return {"request": None, "data": {}, "complete": False, "last_event": None}


def _basename(name):
    return name.replace("\\", "/").rsplit("/", 1)[-1]


def apply_upload_event(state, event, quarter_keys, parse_date, choose_bundles, max_file_bytes=None):
    """Handle one manifest or selected file; return whether an ACK render is needed."""
    if event is None:
        return False

    if isinstance(event, dict) and event.get("kind") == "clear":
        event_id = event.get("id")
        if not isinstance(event_id, str) or not event_id or state["last_event"] == event_id:
            return False
        state.update(new_upload_state(), last_event=event_id)
        return True

    if isinstance(event, dict) and event.get("kind") == "manifest":
        batch_id = event.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id or state["last_event"] == batch_id:
            return False
        request = {
            "batch_id": batch_id, "files": [], "ack": None, "error": None,
            "folder_name": "Midland", "total_files": 0,
        }
        state.update(request=request, data={}, complete=False, last_event=batch_id)
        try:
            manifest = event.get("files")
            if not isinstance(manifest, list):
                raise ValueError("The selected folder's file list could not be read. Select the folder again.")
            request["total_files"] = len(manifest)
            available = {}
            metadata = {}
            for item in manifest:
                name = item.get("name") if isinstance(item, dict) else None
                size = item.get("size") if isinstance(item, dict) else None
                if not isinstance(name, str) or not name or type(size) is not int or size < 0:
                    raise ValueError("The selected folder's file list could not be read. Select the folder again.")
                metadata.setdefault(name, {"name": name, "size": size})
                stamp = parse_date(name)
                if stamp is not None:
                    available.setdefault(_basename(name), (stamp, name))
            if not quarter_keys:
                raise ValueError("The template has no dated occupancy columns, so there is nothing to pick.")
            if not available:
                raise ValueError("No bundles named like 'CAF_Extracts_MMDDYY_HHMM.zip' were found in the selected folder.")
            bundles = sorted(available.values(), key=lambda pair: (pair[0], _basename(pair[1])))
            picks = choose_bundles(bundles, quarter_keys)
            names = list(dict.fromkeys(name for _stamp, name in picks.values()))
            if not names:
                raise ValueError("No bundle could be matched to a template quarter.")
            selected = [metadata[name] for name in names]
            if max_file_bytes is not None and any(item["size"] > max_file_bytes for item in selected):
                raise ValueError("A selected CAF bundle exceeds the app's transfer limit. Use the disk-folder option below.")
            request["files"] = selected
            request["folder_name"] = names[0].replace("\\", "/").split("/")[0]
        except ValueError as exc:
            request["error"] = str(exc)
        return True

    request = state["request"]
    if request is None or request["error"]:
        return False
    if isinstance(event, dict) and event.get("kind") == "error":
        if event.get("batch_id") != request["batch_id"]:
            return False
        request["error"] = str(event.get("message") or "A selected CAF bundle could not be read. Select the folder again.")
        state.update(data={}, complete=False)
        return True
    if not isinstance(event, (bytes, bytearray)):
        return False

    try:
        if len(event) < 4:
            raise ValueError("The selected CAF bundle was not received completely. Select the folder again.")
        header_size = int.from_bytes(event[:4], "big")
        if not 0 < header_size <= 65536 or len(event) < 4 + header_size:
            raise ValueError("The selected CAF bundle was not received completely. Select the folder again.")
        header = json.loads(event[4:4 + header_size])
        if not isinstance(header, dict):
            raise ValueError("The selected CAF bundle could not be identified. Select the folder again.")
        if header.get("batch_id") != request["batch_id"]:
            return False
        name = header.get("name")
        expected = next((item for item in request["files"] if item["name"] == name), None)
        if expected is None:
            raise ValueError("An unexpected CAF bundle was received. Select the folder again.")
        if name in state["data"]:
            return False
        content = bytes(event[4 + header_size:])
        if len(content) != expected["size"]:
            raise ValueError("The selected CAF bundle was not received completely. Select the folder again.")
        state["data"][name] = content
        request["ack"] = name
        state["complete"] = len(state["data"]) == len(request["files"])
    except (ValueError, UnicodeDecodeError) as exc:
        request["error"] = str(exc)
        state.update(data={}, complete=False)
    return True


def upload_caf_folder(quarter_keys, parse_date, choose_bundles):
    """Show a folder picker and return only the ZIP bytes chosen by the old CAF selector."""
    state = st.session_state.get(STATE_KEY) or new_upload_state()
    event = _picker(request=state["request"], key="midland_folder_picker", default=None)
    limit = int(st.get_option("server.maxMessageSize") * 1_000_000) - 65536
    changed = apply_upload_event(state, event, quarter_keys, parse_date, choose_bundles, limit)
    st.session_state[STATE_KEY] = state
    if changed:
        st.rerun()
    if state["request"] and state["request"]["error"]:
        st.error(state["request"]["error"])
    if state["complete"]:
        return [state["data"][item["name"]] for item in state["request"]["files"]]
    return []
