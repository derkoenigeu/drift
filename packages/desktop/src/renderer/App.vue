<script setup lang="ts">
import { ref, watch, onMounted, onUnmounted } from "vue";
import { rpc } from "./ipc.js";
import UnlockView from "./views/UnlockView.vue";
import ConnectionsView from "./views/ConnectionsView.vue";
import WorkbenchView from "./views/WorkbenchView.vue";

type Tab = "connections" | "workbench";

const unlocked = ref(false);
const tab = ref<Tab>("connections");

const onUnload = () => { rpc.vaultLock(); };

watch(unlocked, (val) => {
  if (val) window.addEventListener("beforeunload", onUnload);
  else window.removeEventListener("beforeunload", onUnload);
});

async function lock() {
  await rpc.vaultLock();
  unlocked.value = false;
}

type UpdateState = "idle" | "downloading" | "ready";
const updateState     = ref<UpdateState>("idle");
const updateVersion   = ref("");
const updatePercent   = ref(0);
const updateDismissed = ref(false);

onMounted(() => {
  const unsubs = [
    rpc.onUpdateAvailable(({ version }) => {
      updateVersion.value = version;
      updateState.value = "downloading";
      updateDismissed.value = false;
    }),
    rpc.onUpdateDownloadProgress(({ percent }) => {
      updatePercent.value = percent;
    }),
    rpc.onUpdateDownloaded(({ version }) => {
      updateVersion.value = version;
      updateState.value = "ready";
    }),
    rpc.onUpdateError(() => {
      updateState.value = "idle";
    }),
  ];
  onUnmounted(() => unsubs.forEach((u) => u()));
});

// Keyboard: ⌘/Ctrl+1 connections, ⌘/Ctrl+2 workbench
function onKeyDown(e: KeyboardEvent) {
  if (!unlocked.value) return;
  if ((e.metaKey || e.ctrlKey) && e.key === "1") { tab.value = "connections"; e.preventDefault(); }
  if ((e.metaKey || e.ctrlKey) && e.key === "2") { tab.value = "workbench"; e.preventDefault(); }
}
window.addEventListener("keydown", onKeyDown);
</script>

<template>
  <UnlockView v-if="!unlocked" @unlocked="unlocked = true" />
  <div v-else class="shell">
    <header class="titlebar">
      <div class="traffic"><span /><span /><span /></div>
      <div class="brand">
        <span class="brandmark" />
        <strong>DB-Mirror</strong>
        <span class="muted ver">v0.4.2</span>
      </div>
      <div class="sep" />
      <nav>
        <button :class="{ on: tab === 'connections' }" @click="tab = 'connections'">
          <svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M6 2v3M10 2v3"/><rect x="4.5" y="5" width="7" height="4" rx="1"/><path d="M8 9v3a2 2 0 0 1-2 2"/></svg>
          Connections
          <span class="kbd" style="margin-left:4px">⌘1</span>
        </button>
        <button :class="{ on: tab === 'workbench' }" @click="tab = 'workbench'">
          <svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M5 2v12M11 2v12M3 4l2-2 2 2M9 12l2 2 2-2"/></svg>
          Workbench
          <span class="kbd" style="margin-left:4px">⌘2</span>
        </button>
      </nav>
      <div class="right">
        <span class="pill dot ok">Vault open</span>
        <button class="btn ghost sm" @click="lock">
          <svg viewBox="0 0 16 16" width="13" height="13" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="7" width="10" height="7" rx="1.2"/><path d="M5 7V5a3 3 0 0 1 6 0v2"/></svg>
          Lock
        </button>
      </div>
    </header>
    <div v-if="updateState !== 'idle' && !updateDismissed" class="update-banner" role="status">
      <template v-if="updateState === 'downloading'">
        <span>Downloading v{{ updateVersion }}…</span>
        <div class="update-progress">
          <div class="update-progress-fill" :style="{ width: updatePercent + '%' }" />
        </div>
        <span class="muted">{{ updatePercent }}%</span>
      </template>
      <template v-else>
        <span><strong>v{{ updateVersion }}</strong> ready to install</span>
        <button class="btn sm primary" @click="rpc.installUpdate()">Restart &amp; Update</button>
      </template>
      <button class="btn ghost sm" @click="updateDismissed = true">✕</button>
    </div>
    <main class="main">
      <ConnectionsView v-if="tab === 'connections'" />
      <WorkbenchView v-else />
    </main>
  </div>
</template>

<style scoped>
.shell { display: flex; flex-direction: column; height: 100vh; }
.main { flex: 1; overflow: hidden; }
.traffic { display: flex; gap: 7px; padding-right: 6px; }
.traffic span { width: 11px; height: 11px; border-radius: 50%; display: inline-block; }
.traffic span:nth-child(1) { background: #ff5f57; }
.traffic span:nth-child(2) { background: #febc2e; }
.traffic span:nth-child(3) { background: #28c840; }
.brand { display: inline-flex; gap: 7px; align-items: center; font: 600 12px/1 var(--font-ui); color: var(--text-dim); }
.ver { font-weight: 400; margin-left: 2px; font-size: 11.5px; }
.sep { width: 1px; align-self: stretch; background: var(--hairline); margin: 0 4px; }
.update-banner {
  display: flex; align-items: center; gap: 10px;
  padding: 5px 14px; flex-shrink: 0;
  background: color-mix(in srgb, var(--accent) 10%, var(--bg));
  border-bottom: 1px solid color-mix(in srgb, var(--accent) 20%, transparent);
  font: 500 12px/1 var(--font-ui); color: var(--text-dim);
}
.update-banner strong { color: var(--accent); }
.update-progress {
  flex: 1; height: 4px; background: var(--hairline);
  border-radius: 2px; overflow: hidden; min-width: 80px;
}
.update-progress-fill {
  height: 100%; background: var(--accent);
  border-radius: 2px; transition: width 0.3s ease;
}
.update-banner .btn { margin-left: auto; }
.update-banner .btn + .btn { margin-left: 0; }
</style>
