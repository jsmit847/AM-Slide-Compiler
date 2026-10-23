"""Upload selected servicer files in bounded chunks, spooling bytes to disk."""

import json
import tempfile
from pathlib import Path

import streamlit as st
from streamlit.components.v1 import declare_component
from streamlit.errors import StreamlitAPIException


STATE_KEY = "_midland_folder_upload_v2"
WIDGET_KEY = "midland_folder_picker_v2"
BERKADIA_STATE_KEY = "_berkadia_workbook_upload_v1"
BERKADIA_WIDGET_KEY = "berkadia_workbook_picker_v1"
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


def _clear_upload(state_key):
    state = st.session_state.pop(state_key, None)
    if state:
        _release_files(state)


def clear_caf_folder():
    """Release this session's temporary Midland archives."""
    _clear_upload(STATE_KEY)
    st.session_state.pop("_midland_folder_upload", None)


def clear_berkadia_workbook():
    """Release this session's temporary Berkadia workbook."""
    _clear_upload(BERKADIA_STATE_KEY)


def clear_servicer_uploads():
    """Release both servicers' temporary files on logout."""
    clear_caf_folder()
    clear_berkadia_workbook()


def _basename(name):
    return name.replace("\\", "/").rsplit("/", 1)[-1]


def _select_caf_files(manifest, quarter_keys, parse_date, choose_bundles):
    available, metadata = {}, {}
    for item in manifest:
        name, size = item["name"], item["size"]
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
    return [metadata[name] for name in names]


def _select_workbook_file(manifest, quarter_keys, parse_date, choose_bundles):
    if len(manifest) != 1:
        raise ValueError("Choose one Berkadia workbook (.xlsx or .xlsm).")
    item = manifest[0]
    if Path(_basename(item["name"])).suffix.lower() not in (".xlsx", ".xlsm"):
        raise ValueError("Choose a Berkadia workbook ending in .xlsx or .xlsm.")
    return [{"name": item["name"], "size": item["size"]}]


CAF_SPEC = {
    "state_key": STATE_KEY, "widget_key": WIDGET_KEY, "mode": "folder",
    "label": "Midland servicer folder", "planner": _select_caf_files,
}
BERKADIA_SPEC = {
    "state_key": BERKADIA_STATE_KEY, "widget_key": BERKADIA_WIDGET_KEY,
    "mode": "file", "label": "Berkadia servicer file", "planner": _select_workbook_file,
}


def apply_upload_event(
    state, event, quarter_keys=(), parse_date=None, choose_bundles=None,
    max_file_bytes=None, chunk_size=CHUNK_BYTES, spec=None,
):
    """Process a manifest or one chunk and acknowledge the next expected byte."""
    spec = CAF_SPEC if spec is None else spec
    is_folder = spec["mode"] == "folder"
    choose_again = "Choose the folder again." if is_folder else "Choose the workbook again."
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
            "folder_name": "Midland" if is_folder else "Berkadia", "total_files": 0,
            "chunk_size": chunk_size, "complete": False,
        }
        _release_files(state)
        state.update(new_upload_state(), request=request, last_event=batch_id)
        try:
            manifest = event.get("files")
            metadata_error = "The selected file list could not be read. " + choose_again
            if not isinstance(manifest, list):
                raise ValueError(metadata_error)
            request["total_files"] = len(manifest)
            for item in manifest:
                name = item.get("name") if isinstance(item, dict) else None
                size = item.get("size") if isinstance(item, dict) else None
                if not isinstance(name, str) or not name or type(size) is not int or size < 0:
                    raise ValueError(metadata_error)
            selected = spec["planner"](manifest, quarter_keys, parse_date, choose_bundles)
            if any(item["size"] == 0 for item in selected):
                description = "A selected CAF bundle" if is_folder else "The selected Berkadia workbook"
                raise ValueError(description + " is empty. " + choose_again)
            if max_file_bytes is not None and any(item["size"] > max_file_bytes for item in selected):
                description = "A selected CAF bundle" if is_folder else "The selected Berkadia workbook"
                raise ValueError(description + " exceeds the app's per-file upload limit.")
            request["files"] = selected
            first_name = selected[0]["name"].replace("\\", "/")
            request["folder_name"] = first_name.split("/")[0] if is_folder else _basename(first_name)
            state["directory"] = tempfile.TemporaryDirectory(
                prefix="am_midland_" if is_folder else "am_berkadia_", ignore_cleanup_errors=True
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
        request["error"] = str(event.get("message") or ("The upload failed. " + choose_again))
        _release_files(state)
        state.update(data={}, partial=0, directory=None, complete=False)
        return True
    if not isinstance(event, (bytes, bytearray)):
        return False
    try:
        if len(event) < 4:
            raise ValueError("An upload chunk was incomplete. " + choose_again)
        header_size = int.from_bytes(event[:4], "big")
        if not 0 < header_size <= 65536 or len(event) < 4 + header_size:
            raise ValueError("An upload chunk was incomplete. " + choose_again)
        header = json.loads(event[4:4 + header_size])
        if not isinstance(header, dict):
            raise ValueError("An upload chunk could not be identified. " + choose_again)
        if header.get("batch_id") != request["batch_id"]:
            return False
        name, offset, total = header.get("name"), header.get("offset"), header.get("total")
        if not isinstance(name, str) or type(offset) is not int or type(total) is not int:
            raise ValueError("An upload chunk could not be identified. " + choose_again)
        expected = next((item for item in request["files"] if item["name"] == name), None)
        if expected is None or total != expected["size"]:
            raise ValueError("An unexpected upload chunk was received. " + choose_again)
        if name in state["data"]:
            return False
        next_file = next(item for item in request["files"] if item["name"] not in state["data"])
        if name != next_file["name"] or offset < 0 or offset >= total:
            raise ValueError("An upload chunk arrived out of order. " + choose_again)
        received = state["partial"]
        content = event[4 + header_size:]
        expected_size = min(request["chunk_size"], total - offset)
        if len(content) != expected_size:
            raise ValueError("An upload chunk was incomplete. " + choose_again)
        if offset < received:
            return False
        if offset != received:
            raise ValueError("An upload chunk arrived out of order. " + choose_again)
        directory = state["directory"]
        if directory is None:
            raise ValueError("The upload session expired. " + choose_again)
        suffix = ".zip" if is_folder else Path(_basename(name)).suffix.lower()
        path = Path(directory.name) / f"{request['files'].index(expected)}{suffix}"
        if received and (not path.is_file() or path.stat().st_size != received):
            raise ValueError("The upload session expired. " + choose_again)
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
            continue
        if limit and limit > 0:
            chunk_size = min(chunk_size, max(1024, int(limit * 1_000_000 / 2)))
    max_file_bytes = int(st.get_option("server.maxUploadSize") * 1_000_000)
    return chunk_size, max_file_bytes


def _receive_upload(spec, quarter_keys, parse_date, choose_bundles):
    state = st.session_state.get(spec["state_key"]) or new_upload_state()
    event = st.session_state.get(spec["widget_key"])
    chunk_size, max_file_bytes = _upload_limits()
    changed = apply_upload_event(
        state, event, quarter_keys, parse_date, choose_bundles,
        max_file_bytes=max_file_bytes, chunk_size=chunk_size, spec=spec,
    )
    if changed:
        # Ordinary chunks only redraw the fragment. Notify the rest of the app
        # when a batch starts, finishes, is cleared, or fails.
        kind = event.get("kind") if isinstance(event, dict) else None
        if kind in ("manifest", "clear", "error") or state["complete"] or (
            state["request"] and state["request"]["error"]
        ):
            state["notify_app"] = True
    st.session_state[spec["state_key"]] = state


@st.fragment
def _render_upload(spec, quarter_keys, parse_date, choose_bundles):
    state = st.session_state.get(spec["state_key"]) or new_upload_state()
    st.session_state[spec["state_key"]] = state
    _picker(
        request=state["request"], mode=spec["mode"], label=spec["label"],
        key=spec["widget_key"], default=None,
        on_change=lambda: _receive_upload(spec, quarter_keys, parse_date, choose_bundles),
    )
    if state["notify_app"]:
        state["notify_app"] = False
        st.rerun()
    if state["request"] and state["request"]["error"]:
        st.error(state["request"]["error"])


def _completed_paths(spec):
    state = st.session_state[spec["state_key"]]
    if state["complete"]:
        return [state["data"][item["name"]] for item in state["request"]["files"]]
    return []


def upload_caf_folder(quarter_keys, parse_date, choose_bundles):
    """Show the folder picker and return selected archive paths once complete."""
    st.session_state.pop("_midland_folder_upload", None)
    _render_upload(CAF_SPEC, quarter_keys, parse_date, choose_bundles)
    return _completed_paths(CAF_SPEC)


def upload_berkadia_workbook():
    """Show the workbook picker and return its path only after upload completes."""
    _render_upload(BERKADIA_SPEC, (), None, None)
    paths = _completed_paths(BERKADIA_SPEC)
    return paths[0] if paths else None
