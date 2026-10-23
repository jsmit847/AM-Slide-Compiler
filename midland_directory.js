export default function(component) {
  const {parentElement, data, setStateValue} = component;
  let state = parentElement._midlandDirectory;
  if (!state) {
    const root = document.createElement("div");
    root.className = "midland-directory-picker";
    root.innerHTML = `<label>Midland servicer folder</label><div class="controls">
      <input type="file" webkitdirectory directory multiple accept=".zip" aria-label="Midland servicer folder">
      <button type="button" disabled>Clear</button>
      <div role="status" aria-live="polite">Choose your Midland folder.</div>
      <div class="hint">The app checks filenames first and uploads only the CAF bundles needed for the template quarters.</div>
    </div>`;
    parentElement.appendChild(root);
    state = {
      root, input: root.querySelector("input"), clear: root.querySelector("button"),
      status: root.querySelector("[role=status]"), files: new Map(), batch: null,
      remoteBatch: null, phase: "idle", reader: null, inFlight: null, timer: null,
      offsets: new Map(), finished: new Set(), clearedValue: false, handledError: null,
      lastList: null, fingerprint: null, lastSelectionAt: 0,
    };
    parentElement._midlandDirectory = state;

    state.stopTimer = () => {
      if (state.timer !== null) clearTimeout(state.timer);
      state.timer = null;
    };
    state.stopReader = () => {
      state.stopTimer();
      state.inFlight = null;
      const reader = state.reader;
      state.reader = null;
      if (reader && reader.readyState === FileReader.LOADING) reader.abort();
    };
    state.show = (message, error = false) => {
      state.status.textContent = message;
      state.status.classList.toggle("error", error);
      state.input.disabled = state.phase === "manifest" || state.phase === "uploading";
      state.clear.disabled = !state.batch && !state.remoteBatch;
    };
    state.fail = message => {
      state.stopReader();
      state.phase = "error";
      state.show(message, true);
      state.send({kind: "error", batch_id: state.batch, message});
    };
    state.wait = message => {
      state.stopTimer();
      state.timer = setTimeout(() => {
        if (state.root.isConnected) state.fail(message);
        else state.stopReader();
      }, 60000);
    };
    state.reset = () => {
      state.stopReader();
      state.files = new Map();
      state.batch = null;
      state.phase = "idle";
      state.offsets = new Map();
      state.finished = new Set();
      state.clearedValue = false;
      state.handledError = null;
      state.lastList = null;
      state.fingerprint = null;
    };
    state.read = (request, item, position) => {
      const file = state.files.get(item.name);
      if (!file || file.size !== item.size) {
        state.fail("The folder selection changed. Choose the folder again.");
        return;
      }
      const offset = state.offsets.get(item.name) || 0;
      const requested = Number.isSafeInteger(request.chunk_size) && request.chunk_size > 0
        ? request.chunk_size : 2 * 1024 * 1024;
      const end = Math.min(file.size, offset + Math.min(requested, 2 * 1024 * 1024));
      const reader = new FileReader();
      const transfer = {batch: state.batch, name: item.name, offset, end};
      state.reader = reader;
      state.inFlight = transfer;
      state.phase = "uploading";
      state.clearedValue = false;
      const prefix = `${file.name} (bundle ${position + 1} of ${request.files.length})`;
      const mb = value => (value / 1048576).toFixed(1);
      const current = () => state.root.isConnected && state.inFlight === transfer && state.batch === transfer.batch;
      const readTimeout = "The file read stopped responding. Check that the file is available, then clear and choose the folder again.";
      state.show(`Reading ${prefix}: ${mb(offset)} / ${mb(file.size)} MB.`);
      state.wait(readTimeout);
      reader.onprogress = event => {
        if (!current()) return;
        state.show(`Reading ${prefix}: ${mb(offset + event.loaded)} / ${mb(file.size)} MB.`);
        state.wait(readTimeout);
      };
      reader.onerror = () => { if (current()) state.fail(`Could not read ${file.name}. Clear and choose the folder again.`); };
      reader.onabort = () => { if (current()) state.fail("The read was interrupted. Clear and choose the folder again."); };
      reader.onload = () => {
        if (!current()) return;
        const result = reader.result;
        const comma = typeof result === "string" ? result.indexOf(",") : -1;
        if (comma < 0) {
          state.fail(`Could not read ${file.name}. Clear and choose the folder again.`);
          return;
        }
        state.show(`Uploading ${prefix}: ${mb(end)} / ${mb(file.size)} MB.`);
        state.send({kind: "chunk", batch_id: state.batch, name: item.name,
          offset, total: file.size, content: result.slice(comma + 1)});
        state.wait("The app did not confirm the upload within 60 seconds. Clear and choose the folder again.");
      };
      try { reader.readAsDataURL(file.slice(offset, end)); }
      catch (error) { state.fail(`Could not read ${file.name}. Clear and choose the folder again.`); }
    };
    state.finish = request => {
      state.stopReader();
      state.phase = "loaded";
      const count = request.files.length;
      state.show(`Loaded ${count} ${count === 1 ? "bundle" : "bundles"}; ${request.total_files} ZIP files checked.`);
      if (!state.clearedValue) {
        state.clearedValue = true;
        state.send(null);
      }
    };
    state.receive = request => {
      const previousRemote = state.remoteBatch;
      state.remoteBatch = request ? request.batch_id : null;
      if (!state.batch && previousRemote !== state.remoteBatch) state.clearedValue = false;
      if (!request) return;
      if (!state.batch || state.batch !== request.batch_id) {
        if (!state.batch) {
          if (request.complete) state.finish(request);
          else state.show("Choose your Midland folder to select its bundles again.");
        }
        return;
      }
      if (request.error) {
        state.stopReader();
        state.phase = "error";
        state.show(String(request.error), true);
        const errorKey = `${state.batch}:${request.error}`;
        if (state.handledError !== errorKey) {
          state.handledError = errorKey;
          state.send(null);
        }
        return;
      }
      if (state.phase === "error" || !Array.isArray(request.files)) return;
      if (state.phase === "manifest") state.stopTimer();
      const ack = request.ack;
      if (ack && typeof ack.name === "string" && Number.isSafeInteger(ack.offset)) {
        const item = request.files.find(file => file.name === ack.name);
        if (!item || ack.offset < 0 || ack.offset > item.size) {
          state.fail("The upload could not be confirmed. Clear and choose the folder again.");
          return;
        }
        const pending = state.inFlight;
        if (pending && pending.name === ack.name && ack.offset >= pending.end) {
          if (ack.offset !== pending.end) {
            state.fail("The upload could not be confirmed. Clear and choose the folder again.");
            return;
          }
          state.stopTimer();
          state.inFlight = null;
          state.reader = null;
        }
        state.offsets.set(ack.name, Math.max(state.offsets.get(ack.name) || 0, ack.offset));
        if (ack.offset === item.size) state.finished.add(ack.name);
      }
      if (request.complete) { state.finish(request); return; }
      if (state.inFlight) return;
      const position = request.files.findIndex(item => !state.finished.has(item.name));
      if (position !== -1) state.read(request, request.files[position], position);
    };
    const id = () => typeof crypto.randomUUID === "function" ? crypto.randomUUID() : `${Date.now()}-${Math.random()}`;
    const select = () => {
      const list = state.input.files;
      if (!list || !list.length) return;
      const selected = Array.from(list);
      const fingerprint = selected.map(file => `${file.webkitRelativePath || file.name}:${file.size}:${file.lastModified}`).join("\n");
      const now = Date.now();
      if (fingerprint === state.fingerprint && (
        now - state.lastSelectionAt < 250 || (list === state.lastList && state.phase !== "error")
      )) return;
      state.reset();
      state.lastList = list;
      state.fingerprint = fingerprint;
      state.lastSelectionAt = now;
      state.batch = id();
      for (const file of selected) {
        if (/\.zip$/i.test(file.name)) state.files.set(file.webkitRelativePath || file.name, file);
      }
      state.phase = "manifest";
      state.show(`Selected folder: checking ${state.files.size} ZIP filenames before uploading.`);
      state.send({kind: "manifest", batch_id: state.batch,
        files: Array.from(state.files, ([name, file]) => ({name, size: file.size}))});
      state.wait("The app did not start the upload within 60 seconds. Clear and choose the folder again.");
    };
    state.input.addEventListener("input", select);
    state.input.addEventListener("change", select);
    state.clear.addEventListener("click", () => {
      state.reset();
      state.remoteBatch = null;
      state.input.value = "";
      state.show("Choose your Midland folder.");
      state.send({kind: "clear", id: id()});
    });
  }
  state.send = value => setStateValue("event", value);
  state.receive((data && data.request) || null);
}
