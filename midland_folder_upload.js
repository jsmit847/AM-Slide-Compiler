export default function(component) {
  const {parentElement, data, setStateValue} = component;
  let state = parentElement._midlandFolderUpload;
  if (!state) {
    const root = document.createElement("div");
    root.className = "midland-folder-picker";
    root.innerHTML = '<label>Midland servicer folder</label><div class="controls">' +
      '<button type="button" class="browse">Browse folders</button>' +
      '<button type="button" class="clear" disabled>Clear</button>' +
      '<div role="status" aria-live="polite">Choose your Midland folder.</div>' +
      '<div class="hint">The app checks filenames first and uploads only the CAF bundles needed for the template quarters.</div>' +
      '</div>';
    parentElement.appendChild(root);
    state = {
      root, browse: root.querySelector(".browse"), clear: root.querySelector(".clear"),
      status: root.querySelector('[role="status"]'), handles: new Map(), files: new Map(),
      batch: null, remoteBatch: null, phase: "idle", reader: null, inFlight: null,
      timer: null, operation: 0, offsets: new Map(), finished: new Set(),
      clearedValue: false, handledError: null,
    };
    parentElement._midlandFolderUpload = state;
    const id = () => typeof crypto.randomUUID === "function"
      ? crypto.randomUUID() : String(Date.now()) + "-" + String(Math.random());
    const mb = value => (value / 1048576).toFixed(1);

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
      state.browse.disabled = ["choosing", "enumerating", "manifest", "sizing", "uploading"].includes(state.phase);
      state.clear.disabled = !state.batch && !state.remoteBatch;
    };
    state.reset = () => {
      state.operation += 1;
      state.stopReader();
      state.handles = new Map();
      state.files = new Map();
      state.batch = null;
      state.phase = "idle";
      state.offsets = new Map();
      state.finished = new Set();
      state.clearedValue = false;
      state.handledError = null;
    };
    state.fail = message => {
      state.operation += 1;
      state.stopReader();
      state.phase = "error";
      state.show(message, true);
      if (state.batch) state.send({kind: "error", batch_id: state.batch, message});
    };
    state.wait = message => {
      state.stopTimer();
      state.timer = setTimeout(() => state.fail(message), 180000);
    };
    state.read = (request, item, position) => {
      const file = state.files.get(item.name);
      if (!file || file.size !== item.size) {
        state.fail("The folder selection changed. Clear and choose the folder again.");
        return;
      }
      const offset = state.offsets.get(item.name) || 0;
      const requested = Number.isSafeInteger(request.chunk_size) && request.chunk_size > 0
        ? request.chunk_size : 4 * 1024 * 1024;
      const end = Math.min(file.size, offset + Math.min(requested, 4 * 1024 * 1024));
      const reader = new FileReader();
      const transfer = {batch: state.batch, name: item.name, offset, end};
      state.reader = reader;
      state.inFlight = transfer;
      state.phase = "uploading";
      const prefix = file.name + " (bundle " + String(position + 1) +
        " of " + String(request.files.length) + ")";
      const current = () => state.inFlight === transfer && state.batch === transfer.batch;
      const readTimeout = "Reading the selected bundle timed out. Clear and choose the folder again.";
      state.show("Reading " + prefix + ": " + mb(offset) + " / " + mb(file.size) + " MB.");
      state.wait(readTimeout);
      reader.onprogress = event => {
        if (!current()) return;
        state.show("Reading " + prefix + ": " + mb(offset + event.loaded) +
          " / " + mb(file.size) + " MB.");
        state.wait(readTimeout);
      };
      reader.onerror = () => {
        if (current()) state.fail("Could not read " + file.name + ". Clear and choose the folder again.");
      };
      reader.onabort = () => {
        if (current()) state.fail("The read was interrupted. Clear and choose the folder again.");
      };
      reader.onload = () => {
        if (!current()) return;
        const result = reader.result;
        const comma = typeof result === "string" ? result.indexOf(",") : -1;
        if (comma < 0) {
          state.fail("Could not read " + file.name + ". Clear and choose the folder again.");
          return;
        }
        state.show("Uploading " + prefix + ": " + mb(end) + " / " + mb(file.size) + " MB.");
        state.send({kind: "chunk", batch_id: state.batch, name: item.name,
          offset, total: file.size, content: result.slice(comma + 1)});
        state.wait("The app did not confirm the upload within 180 seconds. Clear and choose the folder again.");
      };
      try { reader.readAsDataURL(file.slice(offset, end)); }
      catch (error) { state.fail("Could not read " + file.name + ". Clear and choose the folder again."); }
    };
    state.finish = request => {
      state.stopReader();
      state.phase = "loaded";
      const count = request.files.length;
      state.show("Loaded " + count + (count === 1 ? " bundle" : " bundles") +
        "; " + request.total_files + " ZIP filenames checked.");
      if (!state.clearedValue) {
        state.clearedValue = true;
        state.send(null);
      }
    };
    state.openSelected = async request => {
      state.phase = "sizing";
      const token = state.operation;
      const sizes = [];
      const count = request.files.length;
      state.show("Opening " + count + " selected quarterly bundles.");
      state.wait("Opening the selected bundles timed out. Clear and choose the folder again.");
      try {
        for (const item of request.files) {
          if (token !== state.operation) return;
          const handle = state.handles.get(item.name);
          if (!handle) throw new Error("A selected file was not found in the chosen folder.");
          const file = await handle.getFile();
          if (token !== state.operation) return;
          state.files.set(item.name, file);
          sizes.push({name: item.name, size: file.size});
          state.show("Opened " + sizes.length + " of " + count + " selected bundles.");
          state.wait("Opening the selected bundles timed out. Clear and choose the folder again.");
        }
        if (token !== state.operation) return;
        state.send({kind: "sizes", batch_id: state.batch, files: sizes});
        state.wait("The app did not confirm the selected bundles within 180 seconds. Clear and choose the folder again.");
      } catch (error) {
        if (token === state.operation) state.fail("Could not open the selected CAF bundles: " +
          String(error && error.message ? error.message : error) +
          " Clear and choose the folder again.");
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
          else state.show("The folder selection was interrupted. Clear and choose the folder again.", true);
        }
        return;
      }
      if (request.error) {
        state.stopReader();
        state.phase = "error";
        state.show(String(request.error), true);
        const errorKey = state.batch + ":" + String(request.error);
        if (state.handledError !== errorKey) {
          state.handledError = errorKey;
          state.send(null);
        }
        return;
      }
      if (state.phase === "error" || !Array.isArray(request.files)) return;
      if (request.complete) { state.finish(request); return; }
      if (!request.ready) {
        if (state.phase === "manifest") {
          state.stopTimer();
          state.openSelected(request);
        }
        return;
      }
      if (state.phase === "sizing") state.stopTimer();
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
      if (state.inFlight) return;
      const position = request.files.findIndex(item => !state.finished.has(item.name));
      if (position !== -1) state.read(request, request.files[position], position);
    };
    state.browse.addEventListener("click", async () => {
      if (typeof window.showDirectoryPicker !== "function") {
        state.show("This browser does not allow folder access for this site. Open the app in a browser that supports folder selection.", true);
        return;
      }
      state.phase = "choosing";
      state.show("Opening the Midland folder chooser...");
      let directory;
      try {
        directory = await window.showDirectoryPicker({mode: "read"});
      } catch (error) {
        state.phase = state.batch ? "loaded" : "idle";
        if (error && error.name === "AbortError") {
          state.show("Folder selection canceled. Choose your Midland folder to try again.");
        } else if (error && error.name === "NotAllowedError") {
          state.show("The browser denied folder access. Allow this site to read the folder, then try again.", true);
        } else {
          state.show("The browser could not open the folder: " +
            String(error && error.message ? error.message : error), true);
        }
        return;
      }
      state.reset();
      const token = state.operation;
      state.batch = id();
      state.phase = "enumerating";
      state.show("Checking ZIP filenames in " + directory.name + "...");
      const listTimeout = "Checking ZIP filenames timed out. Clear and choose the folder again.";
      state.wait(listTimeout);
      const manifest = [];
      try {
        const walk = async (handle, prefix, depth) => {
          for await (const entry of handle.values()) {
            if (token !== state.operation) return;
            state.wait(listTimeout);
            const name = prefix ? prefix + "/" + entry.name : entry.name;
            if (entry.kind === "directory") {
              if (depth < 2) await walk(entry, name, depth + 1);
            } else if (entry.kind === "file" && /\.zip$/i.test(entry.name)) {
              manifest.push({name});
              state.handles.set(name, entry);
              if (manifest.length % 100 === 0) {
                state.show("Checked " + manifest.length + " ZIP filenames...");
                await new Promise(resolve => setTimeout(resolve, 0));
              }
            }
          }
        };
        await walk(directory, directory.name, 0);
        if (token !== state.operation) return;
        state.phase = "manifest";
        state.show("Checked " + manifest.length + " ZIP filenames; choosing the quarterly bundles.");
        state.send({kind: "manifest", batch_id: state.batch, files: manifest});
        state.wait("The app did not choose the quarterly bundles within 180 seconds. Clear and choose the folder again.");
      } catch (error) {
        if (token === state.operation) state.fail("Could not list ZIP filenames in the folder: " +
          String(error && error.message ? error.message : error) +
          " Clear and choose the folder again.");
      }
    });
    state.clear.addEventListener("click", () => {
      state.reset();
      state.remoteBatch = null;
      state.show("Choose your Midland folder.");
      state.send({kind: "clear", id: id()});
    });
  }
  state.send = value => setStateValue("event", value);
  state.receive((data && data.request) || null);
}
