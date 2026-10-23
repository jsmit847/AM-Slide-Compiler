"""Choose quarterly CAF archives before uploading them in bounded chunks."""

import json
from pathlib import Path

import streamlit as st
from streamlit.components.v1 import declare_component
from streamlit.errors import StreamlitAPIException


STATE_KEY = "_midland_folder_upload"
CHUNK_BYTES = 4 * 1024 * 1024
_picker = declare_component(
    "midland_folder_upload", path=str(Path(__file__).parent / "frontend")
)


def new_upload_state():
    return {
        "request": None, "data": {}, "partial": bytearray(),
        "complete": False, "last_event": None,
    }


def _basename(name):
    return name.replace("\\", "/").rsplit("/", 1)[-1]


def apply_upload_event(
    state, event, quarter_keys, parse_date, choose_bundles,
    max_file_bytes=None, chunk_size=CHUNK_BYTES,
):
    """Process a manifest or one chunk and acknowledge the next expected byte."""
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
            "chunk_size": chunk_size, "complete": False,
        }
        state.update(new_upload_state(), request=request, last_event=batch_id)
        try:
            manifest = event.get("files")
            if not isinstance(manifest, list):
                raise ValueError("The folder's file list could not be read. Choose the folder again.")
            request["total_files"] = len(manifest)
            available, metadata = {}, {}
            for item in manifest:
                name = item.get("name") if isinstance(item, dict) else None
                size = item.get("size") if isinstance(item, dict) else None
                if not isinstance(name, str) or not name or type(size) is not int or size < 0:
                    raise ValueError("The folder's file list could not be read. Choose the folder again.")
                metadata.setdefault(name, {"name": name, "size": size})
                stamp = parse_date(_basename(name))
                if stamp is not None:
                    available.setdefault(_basename(name), (stamp, name))
            if not quarter_keys:
                raise ValueError("The template has no dated occupancy columns, so there is nothing to pick.")
            if not available:
                raise ValueError("No bundles named like 'CAF_Extracts_MMDDYY_HHMM.zip' were found in this folder.")
            bundles = sorted(available.values(), key=lambda pair: (pair[0], _basename(pair[1])))
            picks = choose_bundles(bundles, quarter_keys)
            names = list(dict.fromkeys(name for _stamp, name in picks.values()))
            if not names:
                raise ValueError("No bundle could be matched to a template quarter.")
            selected = [metadata[name] for name in names]
            if any(item["size"] == 0 for item in selected):
                raise ValueError("A selected CAF bundle is empty. Check the files and choose the folder again.")
            if max_file_bytes is not None and any(item["size"] > max_file_bytes for item in selected):
                raise ValueError("A selected CAF bundle exceeds the app's per-file upload limit.")
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
        request["error"] = str(event.get("message") or "The folder upload failed. Choose it again.")
        state.update(data={}, partial=bytearray(), complete=False)
        return True
    if not isinstance(event, (bytes, bytearray)):
        return False
    try:
        if len(event) < 4:
            raise ValueError("A CAF upload chunk was incomplete. Choose the folder again.")
        header_size = int.from_bytes(event[:4], "big")
        if not 0 < header_size <= 65536 or len(event) < 4 + header_size:
            raise ValueError("A CAF upload chunk was incomplete. Choose the folder again.")
        header = json.loads(event[4:4 + header_size])
        if not isinstance(header, dict):
            raise ValueError("A CAF upload chunk could not be identified. Choose the folder again.")
        if header.get("batch_id") != request["batch_id"]:
            return False
        name, offset, total = header.get("name"), header.get("offset"), header.get("total")
        if not isinstance(name, str) or type(offset) is not int or type(total) is not int:
            raise ValueError("A CAF upload chunk could not be identified. Choose the folder again.")
        expected = next((item for item in request["files"] if item["name"] == name), None)
        if expected is None or total != expected["size"]:
            raise ValueError("An unexpected CAF upload chunk was received. Choose the folder again.")
        if name in state["data"]:
            return False
        next_file = next(item for item in request["files"] if item["name"] not in state["data"])
        if name != next_file["name"] or offset < 0 or offset >= total:
            raise ValueError("A CAF upload chunk arrived out of order. Choose the folder again.")
        received = len(state["partial"])
        content = event[4 + header_size:]
        expected_size = min(request["chunk_size"], total - offset)
        if len(content) != expected_size:
            raise ValueError("A CAF upload chunk was incomplete. Choose the folder again.")
        if offset < received:
            return False
        if offset != received:
            raise ValueError("A CAF upload chunk arrived out of order. Choose the folder again.")
        state["partial"].extend(content)
        received += len(content)
        request["ack"] = {"name": name, "offset": received}
        if received == total:
            state["data"][name] = bytes(state["partial"])
            state["partial"] = bytearray()
        state["complete"] = len(state["data"]) == len(request["files"])
        request["complete"] = state["complete"]
    except (ValueError, UnicodeDecodeError) as exc:
        request["error"] = str(exc)
        state.update(data={}, partial=bytearray(), complete=False)
    return True


def upload_caf_folder(quarter_keys, parse_date, choose_bundles):
    """Keep binary widget messages below Cloud limits and return selected ZIPs."""
    state = st.session_state.get(STATE_KEY) or new_upload_state()
    event = _picker(request=state["request"], key="midland_folder_picker", default=None)
    chunk_size = CHUNK_BYTES
    for option in ("server.maxMessageSize", "server.maxWidgetStateSize"):
        try:
            limit = st.get_option(option)
        except (RuntimeError, StreamlitAPIException):
            continue  # Older Streamlit versions have no separate widget-state limit.
        if limit and limit > 0:
            chunk_size = min(chunk_size, max(1024, int(limit * 1_000_000 / 2)))
    max_file_bytes = int(st.get_option("server.maxUploadSize") * 1_000_000)
    changed = apply_upload_event(
        state, event, quarter_keys, parse_date, choose_bundles,
        max_file_bytes=max_file_bytes, chunk_size=chunk_size,
    )
    st.session_state[STATE_KEY] = state
    if changed:
        st.rerun()
    if state["request"] and state["request"]["error"]:
        st.error(state["request"]["error"])
    if state["complete"]:
        return [state["data"][item["name"]] for item in state["request"]["files"]]
    return []
