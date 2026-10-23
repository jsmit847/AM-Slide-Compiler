"""Select Midland quarterly ZIPs from a browser folder before uploading bytes."""

import base64
import binascii
import tempfile
from pathlib import Path

import streamlit as st
from streamlit.components.v2 import component
from streamlit.errors import StreamlitAPIException


STATE_KEY = "_midland_folder_upload_v1"
WIDGET_KEY = "midland_folder_upload_v1"
CHUNK_BYTES = 4 * 1024 * 1024

_folder_picker = component(
    "midland_folder_handle_upload_v1",
    js=Path(__file__).with_suffix(".js").read_text(encoding="utf-8"),
    css="""
    .midland-folder-picker {font-family:inherit;color:inherit;}
    .midland-folder-picker label {display:block;margin-bottom:.45rem;}
    .midland-folder-picker .controls {padding:1rem;border-radius:.5rem;
      background:var(--st-secondary-background-color,#f0f2f6);}
    .midland-folder-picker button {font:inherit;padding:.45rem .7rem;
      border:1px solid #aaa;border-radius:.4rem;cursor:pointer;}
    .midland-folder-picker button + button {margin-left:.5rem;}
    .midland-folder-picker [role=status] {margin-top:.55rem;overflow-wrap:anywhere;}
    .midland-folder-picker .hint {font-size:.8rem;opacity:.75;margin-top:.4rem;}
    .midland-folder-picker .error {color:var(--st-primary-color,#d62f2f);}
    """,
)


def new_upload_state():
    return {
        "request": None, "files": {}, "directory": None, "partial": 0,
        "complete": False, "bundles": (), "last_event": None, "notify_app": False,
    }


def _release_files(state):
    if state.get("directory") is not None:
        state["directory"].cleanup()
        state["directory"] = None


def clear_midland_folder():
    """Remove this session's temporary selected bundles."""
    state = st.session_state.pop(STATE_KEY, None)
    if state:
        _release_files(state)


def _basename(name):
    return name.replace("\\", "/").rsplit("/", 1)[-1]


def _fail(state, message):
    _release_files(state)
    state.update(files={}, directory=None, partial=0, complete=False, bundles=())
    state["request"]["error"] = str(message)
    state["request"]["complete"] = False


def apply_folder_event(
    state, event, quarter_keys, parse_date, pick_bundles,
    *, chunk_size=CHUNK_BYTES, max_file_bytes=None,
):
    """Apply one manifest, size, or chunk event; acknowledge committed bytes."""
    if event is None or not isinstance(event, dict):
        return False
    kind = event.get("kind")
    if kind == "clear":
        event_id = event.get("id")
        if not isinstance(event_id, str) or not event_id or state["last_event"] == event_id:
            return False
        _release_files(state)
        state.update(new_upload_state(), last_event=event_id)
        return True
    if kind == "manifest":
        batch_id = event.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id or state["last_event"] == batch_id:
            return False
        _release_files(state)
        request = {
            "batch_id": batch_id, "files": [], "picks": [], "ack": None,
            "error": None, "complete": False, "ready": False,
            "chunk_size": chunk_size,
            "total_files": 0,
        }
        state.update(new_upload_state(), request=request, last_event=batch_id)
        try:
            manifest = event.get("files")
            if not isinstance(manifest, list):
                raise ValueError("The folder file list could not be read. Choose the folder again.")
            request["total_files"] = len(manifest)
            available = {}
            for item in manifest:
                name = item.get("name") if isinstance(item, dict) else None
                if not isinstance(name, str) or not name:
                    raise ValueError("The folder file list could not be read. Choose the folder again.")
                stamp = parse_date(_basename(name))
                if stamp is not None:
                    available.setdefault(_basename(name), (stamp, name))
            if not quarter_keys:
                raise ValueError("The template has no dated occupancy columns, so there is nothing to pick.")
            if not available:
                raise ValueError("No bundles named like 'CAF_Extracts_MMDDYY_HHMM.zip' were found in this folder.")
            bundles = sorted(available.values(), key=lambda pair: (pair[0], _basename(pair[1])))
            picks = pick_bundles(bundles, quarter_keys)
            names = list(dict.fromkeys(name for _stamp, name in picks.values()))
            if not names:
                raise ValueError("No bundle could be matched to a template quarter.")
            request["files"] = [{"name": name, "size": None} for name in names]
            request["picks"] = [
                {"Quarter": quarter, "Bundle": _basename(name), "Bundle date": stamp.strftime("%m/%d/%Y")}
                for quarter, (stamp, name) in picks.items()
            ]
        except (ValueError, OSError) as exc:
            _fail(state, exc)
        return True

    request = state["request"]
    if not request or event.get("batch_id") != request["batch_id"] or request["error"]:
        return False
    if kind == "error":
        _fail(state, event.get("message") or "The folder upload failed. Choose the folder again.")
        return True
    if kind == "sizes":
        if request["ready"]:
            return False
        try:
            sizes = event.get("files")
            expected_names = [item["name"] for item in request["files"]]
            if not isinstance(sizes, list) or len(sizes) != len(expected_names):
                raise ValueError("The selected CAF bundles could not be opened. Choose the folder again.")
            for item, name in zip(sizes, expected_names):
                if not isinstance(item, dict) or item.get("name") != name:
                    raise ValueError("The selected CAF bundles changed. Choose the folder again.")
                size = item.get("size")
                if type(size) is not int or size <= 0:
                    raise ValueError("A selected CAF bundle is empty. Choose the folder again.")
                if max_file_bytes is not None and size > max_file_bytes:
                    raise ValueError("A selected CAF bundle exceeds the app's per-file upload limit.")
                request["files"][expected_names.index(name)]["size"] = size
            state["directory"] = tempfile.TemporaryDirectory(
                prefix="am_midland_folder_", ignore_cleanup_errors=True,
            )
            request["ready"] = True
        except (ValueError, OSError) as exc:
            _fail(state, exc)
        return True
    if kind != "chunk":
        return False
    if not request["ready"] or state["complete"]:
        return False
    try:
        name, offset, total = event.get("name"), event.get("offset"), event.get("total")
        if not isinstance(name, str) or type(offset) is not int or type(total) is not int:
            raise ValueError("An upload chunk could not be identified. Choose the folder again.")
        expected = next((item for item in request["files"] if item["name"] == name), None)
        if expected is None or expected["size"] != total:
            raise ValueError("An unexpected file was received. Choose the folder again.")
        if name in state["files"]:
            return False
        next_file = next(item for item in request["files"] if item["name"] not in state["files"])
        if name != next_file["name"] or offset < 0 or offset >= total:
            raise ValueError("An upload chunk arrived out of order. Choose the folder again.")
        if offset < state["partial"]:
            return False
        if offset != state["partial"]:
            raise ValueError("An upload chunk arrived out of order. Choose the folder again.")
        expected_size = min(request["chunk_size"], total - offset)
        encoded = event.get("content")
        if not isinstance(encoded, str) or len(encoded) != 4 * ((expected_size + 2) // 3):
            raise ValueError("An upload chunk was incomplete. Choose the folder again.")
        content = base64.b64decode(encoded, validate=True)
        if len(content) != expected_size:
            raise ValueError("An upload chunk was incomplete. Choose the folder again.")
        directory = state["directory"]
        if directory is None:
            raise ValueError("The upload session expired. Choose the folder again.")
        path = Path(directory.name) / f"{request['files'].index(expected)}.zip"
        if offset and (not path.is_file() or path.stat().st_size != offset):
            raise ValueError("The upload session expired. Choose the folder again.")
        with path.open("ab" if offset else "wb") as handle:
            handle.write(content)
        received = offset + len(content)
        state["partial"] = received
        request["ack"] = {"name": name, "offset": received}
        if received == total:
            state["files"][name] = path
            state["partial"] = 0
        state["complete"] = len(state["files"]) == len(request["files"])
        request["complete"] = state["complete"]
        if state["complete"]:
            state["bundles"] = tuple(
                state["files"][item["name"]].read_bytes() for item in request["files"]
            )
            _release_files(state)
            state["files"] = {}
    except (ValueError, OSError, binascii.Error) as exc:
        _fail(state, exc)
    return True


def _upload_limits():
    chunk_size = CHUNK_BYTES
    for option in ("server.maxMessageSize", "server.maxWidgetStateSize"):
        try:
            limit = st.get_option(option)
        except (RuntimeError, StreamlitAPIException):
            continue
        if limit and limit > 0:
            chunk_size = min(chunk_size, max(1024, int(limit * 1_000_000 / 4)))
    return chunk_size, int(st.get_option("server.maxUploadSize") * 1_000_000)


def _receive_event(quarter_keys, parse_date, pick_bundles):
    state = st.session_state.get(STATE_KEY) or new_upload_state()
    event = st.session_state.get(WIDGET_KEY, {}).get("event")
    chunk_size, max_file_bytes = _upload_limits()
    changed = apply_folder_event(
        state, event, quarter_keys, parse_date, pick_bundles,
        chunk_size=chunk_size, max_file_bytes=max_file_bytes,
    )
    if changed and (
        event.get("kind") in ("clear", "error")
        or state["complete"] or (state["request"] and state["request"]["error"])
    ):
        state["notify_app"] = True
    st.session_state[STATE_KEY] = state


@st.fragment
def _render_folder(quarter_keys, parse_date, pick_bundles):
    state = st.session_state.get(STATE_KEY) or new_upload_state()
    st.session_state[STATE_KEY] = state
    _folder_picker(
        key=WIDGET_KEY, data={"request": state["request"]}, default={"event": None},
        on_event_change=lambda: _receive_event(quarter_keys, parse_date, pick_bundles),
    )
    if state["notify_app"]:
        st.rerun()
    if state["request"] and state["request"]["error"]:
        st.error(state["request"]["error"])
    elif state["request"] and state["request"]["picks"]:
        st.caption(
            f"{state['request']['total_files']} ZIP files in the folder; "
            f"using {len(state['request']['files'])} CAF bundles."
        )
        st.dataframe(state["request"]["picks"], width="stretch", hide_index=True)


def upload_midland_folder(quarter_keys, parse_date, pick_bundles):
    """Choose a whole folder, then return only fully transferred quarterly ZIPs."""
    state = st.session_state.get(STATE_KEY)
    if state:
        state["notify_app"] = False
    _render_folder(quarter_keys, parse_date, pick_bundles)
    state = st.session_state[STATE_KEY]
    if not state["complete"]:
        return []
    return list(state["bundles"])
