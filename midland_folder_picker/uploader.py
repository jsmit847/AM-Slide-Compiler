"""Choose quarterly CAF archives before uploading them in bounded chunks."""

import json
import tempfile
from pathlib import Path

import streamlit as st
from streamlit.components.v1 import declare_component
from streamlit.errors import StreamlitAPIException


STATE_KEY = "_midland_folder_upload_v2"
WIDGET_KEY = "midland_folder_picker_v2"
CHUNK_BYTES = 4 * 1024 * 1024
_picker = declare_component(
    "midland_folder_upload", path=str(Path(__file__).parent / "frontend")
)


def new_upload_state():
    return {
        "request": None, "data": {}, "partial": 0, "directory": None,
        "complete": False, "last_event": None, "notify_app": False,
    }


def _release_files(state):
    directory = state.get("directory")
    if directory is not None:
        directory.cleanup()


def clear_caf_folder():
    """Release this session's temporary archives on logout."""
    state = st.session_state.pop(STATE_KEY, None)
    if state:
        _release_files(state)
    st.session_state.pop("_midland_folder_upload", None)


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
        _release_files(state)
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
        _release_files(state)
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
            state["directory"] = tempfile.TemporaryDirectory(
                prefix="am_midland_", ignore_cleanup_errors=True
            )
        except (ValueError, OSError) as exc:
            request["error"] = str(exc)
        return True

    request = state["request"]
    if request is None or request["error"]:
        return False
    if isinstance(event, dict) and event.get("kind") == "error":
        if event.get("batch_id") != request["batch_id"]:
            return False
        request["error"] = str(event.get("message") or "The folder upload failed. Choose it again.")
        _release_files(state)
        state.update(data={}, partial=0, directory=None, complete=False)
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
        received = state["partial"]
        content = event[4 + header_size:]
        expected_size = min(request["chunk_size"], total - offset)
        if len(content) != expected_size:
            raise ValueError("A CAF upload chunk was incomplete. Choose the folder again.")
        if offset < received:
            return False
        if offset != received:
            raise ValueError("A CAF upload chunk arrived out of order. Choose the folder again.")
        directory = state["directory"]
        if directory is None:
            raise ValueError("The upload session expired. Choose the folder again.")
        path = Path(directory.name) / f"{request['files'].index(expected)}.zip"
        if received and (not path.is_file() or path.stat().st_size != received):
            raise ValueError("The upload session expired. Choose the folder again.")
        with path.open("ab" if received else "wb") as handle:
            handle.write(content)
        received += len(content)
        state["partial"] = received
        request["ack"] = {"name": name, "offset": received}
        if received == total:
            state["data"][name] = path
            state["partial"] = 0
        state["complete"] = len(state["data"]) == len(request["files"])
        request["complete"] = state["complete"]
    except (ValueError, UnicodeDecodeError, OSError) as exc:
        request["error"] = str(exc)
        _release_files(state)
        state.update(data={}, partial=0, directory=None, complete=False)
    return True


def _upload_limits():
    chunk_size = CHUNK_BYTES
    for option in ("server.maxMessageSize", "server.maxWidgetStateSize"):
        try:
            limit = st.get_option(option)
        except (RuntimeError, StreamlitAPIException):
            continue  # Older Streamlit versions have no separate widget-state limit.
        if limit and limit > 0:
            chunk_size = min(chunk_size, max(1024, int(limit * 1_000_000 / 2)))
    max_file_bytes = int(st.get_option("server.maxUploadSize") * 1_000_000)
    return chunk_size, max_file_bytes


def _receive_upload(quarter_keys, parse_date, choose_bundles):
    state = st.session_state.get(STATE_KEY) or new_upload_state()
    event = st.session_state.get(WIDGET_KEY)
    chunk_size, max_file_bytes = _upload_limits()
    changed = apply_upload_event(
        state, event, quarter_keys, parse_date, choose_bundles,
        max_file_bytes=max_file_bytes, chunk_size=chunk_size,
    )
    if changed:
        # Ordinary chunks only redraw the fragment. The rest of the app needs
        # new data when a batch starts, finishes, is cleared, or fails.
        kind = event.get("kind") if isinstance(event, dict) else None
        if kind in ("manifest", "clear", "error") or state["complete"] or (
            state["request"] and state["request"]["error"]
        ):
            state["notify_app"] = True
    st.session_state[STATE_KEY] = state


@st.fragment
def _render_folder_picker(quarter_keys, parse_date, choose_bundles):
    state = st.session_state.get(STATE_KEY) or new_upload_state()
    st.session_state[STATE_KEY] = state
    _picker(
        request=state["request"], key=WIDGET_KEY, default=None,
        on_change=lambda: _receive_upload(quarter_keys, parse_date, choose_bundles),
    )
    if state["notify_app"]:
        state["notify_app"] = False
        st.rerun()
    if state["request"] and state["request"]["error"]:
        st.error(state["request"]["error"])


def upload_caf_folder(quarter_keys, parse_date, choose_bundles):
    """Show the folder picker and return selected archive paths once complete."""
    st.session_state.pop("_midland_folder_upload", None)
    _render_folder_picker(quarter_keys, parse_date, choose_bundles)
    state = st.session_state[STATE_KEY]
    if state["complete"]:
        return [state["data"][item["name"]] for item in state["request"]["files"]]
    return []
