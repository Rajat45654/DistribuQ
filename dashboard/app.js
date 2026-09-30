/**
 * DistribuQ Dashboard — Real-time monitoring frontend
 */

(() => {
  'use strict';

  // -------------------------------------------------------------------------
  // State
  // -------------------------------------------------------------------------
  const state = {
    counts: {
      PENDING: 0,
      RUNNING: 0,
      SUCCESS: 0,
      RETRYING: 0,
      FAILED: 0,
      DEAD_LETTER: 0,
    },
    workers: new Map(), // worker_id -> { id, status, jobs_processed, last_heartbeat }
    chartBuckets: [],   // Array of { time: timestamp, success: int, failed: int }
    maxBuckets: 20,     // 20 x 5s = 100 seconds window
    currentBucket: { success: 0, failed: 0 },
    ws: null,
    wsReconnectAttempts: 0,
    maxReconnectDelay: 5000,
  };

  // -------------------------------------------------------------------------
  // DOM References
  // -------------------------------------------------------------------------
  const dom = {
    wsIndicator: document.getElementById('ws-indicator'),
    wsLabel: document.getElementById('ws-label'),
    btnSubmitJob: document.getElementById('btn-submit-job'),
    btnChaos: document.getElementById('btn-chaos'),

    countPending: document.getElementById('count-pending'),
    countRunning: document.getElementById('count-running'),
    countSuccess: document.getElementById('count-success'),
    countRetrying: document.getElementById('count-retrying'),
    countFailed: document.getElementById('count-failed'),
    countDead: document.getElementById('count-dead'),

    barPending: document.getElementById('bar-pending'),
    barRunning: document.getElementById('bar-running'),
    barSuccess: document.getElementById('bar-success'),
    barRetrying: document.getElementById('bar-retrying'),
    barFailed: document.getElementById('bar-failed'),
    barDead: document.getElementById('bar-dead'),

    chartCanvas: document.getElementById('throughput-chart'),
    eventFeed: document.getElementById('event-feed'),
    btnClearFeed: document.getElementById('btn-clear-feed'),

    workerCountBadge: document.getElementById('worker-count-badge'),
    workerList: document.getElementById('worker-list'),

    btnRefreshJobs: document.getElementById('btn-refresh-jobs'),
    jobTableBody: document.getElementById('job-table-body'),

    toastContainer: document.getElementById('toast-container'),
    modalOverlay: document.getElementById('modal-overlay'),
    modalTitle: document.getElementById('modal-title'),
    modalBody: document.getElementById('modal-body'),
    modalClose: document.getElementById('modal-close'),
  };

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------
  function formatTime(isoString) {
    const d = isoString ? new Date(isoString) : new Date();
    return d.toTimeString().split(' ')[0];
  }

  function shortId(uuid) {
    if (!uuid) return '–';
    const s = String(uuid);
    return s.length > 8 ? s.substring(0, 8) + '…' : s;
  }

  function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.innerHTML = `<span>${message}</span>`;
    dom.toastContainer.appendChild(toast);
    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateY(10px)';
      toast.style.transition = 'all 0.3s ease';
      setTimeout(() => toast.remove(), 300);
    }, 4000);
  }

  // -------------------------------------------------------------------------
  // WebSocket Connection
  // -------------------------------------------------------------------------
  function getWsUrl() {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const host = window.location.host || 'localhost:8000';
    return `${proto}//${host}/ws/dashboard`;
  }

  function connectWs() {
    const url = getWsUrl();
    dom.wsIndicator.className = 'ws-indicator';
    dom.wsLabel.textContent = 'Connecting…';

    try {
      const ws = new WebSocket(url);
      state.ws = ws;

      ws.onopen = () => {
        state.wsReconnectAttempts = 0;
        dom.wsIndicator.className = 'ws-indicator connected';
        dom.wsLabel.textContent = 'Live';
        showToast('Connected to DistribuQ live events stream', 'success');
        refreshStats();
        refreshWorkers();
        refreshJobs();
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          handleIncomingEvent(msg);
        } catch (err) {
          console.error('Error parsing WS message:', err);
        }
      };

      ws.onclose = () => {
        dom.wsIndicator.className = 'ws-indicator disconnected';
        dom.wsLabel.textContent = 'Disconnected';
        scheduleReconnect();
      };

      ws.onerror = () => {
        ws.close();
      };
    } catch (e) {
      dom.wsIndicator.className = 'ws-indicator disconnected';
      scheduleReconnect();
    }
  }

  function scheduleReconnect() {
    state.wsReconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(1.5, state.wsReconnectAttempts), state.maxReconnectDelay);
    dom.wsLabel.textContent = `Retrying in ${(delay / 1000).toFixed(0)}s…`;
    setTimeout(connectWs, delay);
  }

  // -------------------------------------------------------------------------
  // Event Dispatcher
  // -------------------------------------------------------------------------
  function handleIncomingEvent(envelope) {
    const { event, timestamp, data } = envelope;

    switch (event) {
      case 'stats.snapshot':
        if (data.counts) {
          updateStatsCards(data.counts);
        }
        if (data.active_workers !== undefined) {
          dom.workerCountBadge.textContent = data.active_workers;
        }
        break;

      case 'job.submitted':
        addEventFeedItem({
          time: timestamp,
          badge: 'SUBMITTED',
          desc: `Job <strong>${shortId(data.job_id)}</strong> (${data.type}) priority: ${data.priority}`,
        });
        state.counts.PENDING = (state.counts.PENDING || 0) + 1;
        renderStats();
        throttledRefreshJobs();
        break;

      case 'job.status_changed': {
        const { job_id, type, status, worker_id } = data;
        addEventFeedItem({
          time: timestamp,
          badge: status,
          desc: `Job <strong>${shortId(job_id)}</strong> (${type}) → <strong>${status}</strong>${worker_id ? ' on ' + shortId(worker_id) : ''}`,
        });

        if (status === 'SUCCESS') {
          state.currentBucket.success += 1;
        } else if (status === 'FAILED' || status === 'DEAD_LETTER') {
          state.currentBucket.failed += 1;
        }

        throttledRefreshJobs();
        refreshStats();
        break;
      }

      case 'worker.heartbeat':
        updateWorkerState(data);
        break;

      case 'worker.dead':
        markWorkerDeadState(data.worker_id);
        addEventFeedItem({
          time: timestamp,
          badge: 'DEAD',
          desc: `Worker <strong>${shortId(data.worker_id)}</strong> marked DEAD`,
        });
        showToast(`Worker ${shortId(data.worker_id)} is dead`, 'warning');
        refreshWorkers();
        break;

      default:
        console.log('Unhandled event:', envelope);
    }
  }

  // -------------------------------------------------------------------------
  // Stats & Progress Bars
  // -------------------------------------------------------------------------
  function updateStatsCards(counts) {
    state.counts = { ...state.counts, ...counts };
    renderStats();
  }

  function renderStats() {
    const { PENDING, RUNNING, SUCCESS, RETRYING, FAILED, DEAD_LETTER } = state.counts;

    dom.countPending.textContent = PENDING ?? 0;
    dom.countRunning.textContent = RUNNING ?? 0;
    dom.countSuccess.textContent = SUCCESS ?? 0;
    dom.countRetrying.textContent = RETRYING ?? 0;
    dom.countFailed.textContent = FAILED ?? 0;
    dom.countDead.textContent = DEAD_LETTER ?? 0;

    const total = Math.max(1, (PENDING || 0) + (RUNNING || 0) + (SUCCESS || 0) + (RETRYING || 0) + (FAILED || 0) + (DEAD_LETTER || 0));

    dom.barPending.style.width = `${((PENDING || 0) / total) * 100}%`;
    dom.barRunning.style.width = `${((RUNNING || 0) / total) * 100}%`;
    dom.barSuccess.style.width = `${((SUCCESS || 0) / total) * 100}%`;
    dom.barRetrying.style.width = `${((RETRYING || 0) / total) * 100}%`;
    dom.barFailed.style.width = `${((FAILED || 0) / total) * 100}%`;
    dom.barDead.style.width = `${((DEAD_LETTER || 0) / total) * 100}%`;
  }

  async function refreshStats() {
    try {
      const res = await fetch('/api/v1/stats');
      if (!res.ok) return;
      const data = await res.json();
      if (data.job_counts) updateStatsCards(data.job_counts);
      if (data.active_workers !== undefined) dom.workerCountBadge.textContent = data.active_workers;
      if (data.workers) renderWorkersList(data.workers);
    } catch (e) {
      console.warn('Failed to fetch stats:', e);
    }
  }

  // -------------------------------------------------------------------------
  // Live Event Feed
  // -------------------------------------------------------------------------
  function addEventFeedItem({ time, badge, desc }) {
    const item = document.createElement('div');
    item.className = 'event-item';
    item.innerHTML = `
      <span class="event-time">${formatTime(time)}</span>
      <span class="event-badge badge-status-${badge}">${badge}</span>
      <span class="event-desc">${desc}</span>
    `;

    dom.eventFeed.insertBefore(item, dom.eventFeed.firstChild);

    // Limit feed to max 60 items
    while (dom.eventFeed.children.length > 60) {
      dom.eventFeed.removeChild(dom.eventFeed.lastChild);
    }
  }

  dom.btnClearFeed.addEventListener('click', () => {
    dom.eventFeed.innerHTML = '';
  });

  // -------------------------------------------------------------------------
  // Workers Management
  // -------------------------------------------------------------------------
  function updateWorkerState(w) {
    state.workers.set(w.worker_id, {
      id: w.worker_id,
      status: w.status,
      jobs_processed: w.jobs_processed,
      last_heartbeat: w.last_heartbeat,
    });
    renderWorkersList(Array.from(state.workers.values()));
  }

  function markWorkerDeadState(workerId) {
    if (state.workers.has(workerId)) {
      const w = state.workers.get(workerId);
      w.status = 'DEAD';
      renderWorkersList(Array.from(state.workers.values()));
    }
  }

  function renderWorkersList(workers) {
    if (!workers || workers.length === 0) {
      dom.workerList.innerHTML = '<div class="empty-state">No workers registered</div>';
      dom.workerCountBadge.textContent = '0';
      return;
    }

    workers.sort((a, b) => (a.status === 'ALIVE' ? -1 : 1));

    let activeCount = 0;
    dom.workerList.innerHTML = workers.map(w => {
      const isAlive = (w.status === 'ALIVE');
      if (isAlive) activeCount++;
      return `
        <div class="worker-card">
          <div class="worker-info">
            <span class="worker-dot ${isAlive ? 'alive' : 'dead'}"></span>
            <span class="worker-id">${shortId(w.id || w.worker_id)}</span>
          </div>
          <div class="worker-meta">
            <span>Jobs: <strong>${w.jobs_processed ?? 0}</strong></span>
            <span>${w.status}</span>
          </div>
        </div>
      `;
    }).join('');

    dom.workerCountBadge.textContent = activeCount;
  }

  async function refreshWorkers() {
    try {
      const res = await fetch('/api/v1/workers');
      if (!res.ok) return;
      const list = await res.json();
      list.forEach(w => state.workers.set(w.id, w));
      renderWorkersList(list);
    } catch (e) {
      console.warn('Failed to fetch workers:', e);
    }
  }

  // -------------------------------------------------------------------------
  // Recent Jobs Table
  // -------------------------------------------------------------------------
  let refreshJobsTimeout = null;
  function throttledRefreshJobs() {
    if (refreshJobsTimeout) return;
    refreshJobsTimeout = setTimeout(() => {
      refreshJobsTimeout = null;
      refreshJobs();
    }, 800);
  }

  async function refreshJobs() {
    try {
      const res = await fetch('/api/v1/jobs?limit=25');
      if (!res.ok) return;
      const jobs = await res.json();
      renderJobTable(jobs);
    } catch (e) {
      console.warn('Failed to fetch jobs:', e);
    }
  }

  function renderJobTable(jobs) {
    if (!jobs || jobs.length === 0) {
      dom.jobTableBody.innerHTML = '<tr><td colspan="6" class="empty-td">No jobs found</td></tr>';
      return;
    }

    dom.jobTableBody.innerHTML = jobs.map(job => {
      const updated = job.updated_at ? formatTime(job.updated_at) : '–';
      return `
        <tr data-job-id="${job.id}">
          <td class="job-id-cell">${shortId(job.id)}</td>
          <td>${job.type}</td>
          <td><span class="event-badge badge-status-${job.status}">${job.status}</span></td>
          <td>${job.priority ?? 0}</td>
          <td>${job.attempts}/${job.max_attempts}</td>
          <td>${updated}</td>
        </tr>
      `;
    }).join('');

    // Attach click handlers to open modal
    dom.jobTableBody.querySelectorAll('tr').forEach(tr => {
      tr.addEventListener('click', () => {
        const jobId = tr.getAttribute('data-job-id');
        if (jobId) openJobDetailModal(jobId);
      });
    });
  }

  dom.btnRefreshJobs.addEventListener('click', refreshJobs);

  // -------------------------------------------------------------------------
  // Job Detail Modal
  // -------------------------------------------------------------------------
  async function openJobDetailModal(jobId) {
    dom.modalTitle.textContent = `Job Details — ${shortId(jobId)}`;
    dom.modalBody.innerHTML = '<div class="empty-state">Loading job details…</div>';
    dom.modalOverlay.removeAttribute('hidden');

    try {
      const res = await fetch(`/api/v1/jobs/${jobId}`);
      if (!res.ok) {
        dom.modalBody.innerHTML = '<div class="empty-state">Job not found</div>';
        return;
      }
      const j = await res.json();

      dom.modalBody.innerHTML = `
        <div class="detail-row">
          <div class="detail-label">Full ID</div>
          <div class="detail-val">${j.id}</div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Type</div>
          <div class="detail-val">${j.type}</div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Status</div>
          <div class="detail-val"><span class="event-badge badge-status-${j.status}">${j.status}</span></div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Priority</div>
          <div class="detail-val">${j.priority}</div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Attempts</div>
          <div class="detail-val">${j.attempts} / ${j.max_attempts}</div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Locked By</div>
          <div class="detail-val">${j.locked_by ? j.locked_by : '–'}</div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Payload</div>
          <div class="detail-val" style="width:100%">
            <pre class="detail-pre">${JSON.stringify(j.payload, null, 2)}</pre>
          </div>
        </div>
        ${j.result ? `
        <div class="detail-row">
          <div class="detail-label">Result</div>
          <div class="detail-val" style="width:100%">
            <pre class="detail-pre">${JSON.stringify(j.result, null, 2)}</pre>
          </div>
        </div>` : ''}
        ${j.error ? `
        <div class="detail-row">
          <div class="detail-label">Error</div>
          <div class="detail-val" style="width:100%">
            <pre class="detail-pre" style="color:var(--accent-rose)">${j.error}</pre>
          </div>
        </div>` : ''}
        <div class="detail-row">
          <div class="detail-label">Created</div>
          <div class="detail-val">${j.created_at || '–'}</div>
        </div>
        <div class="detail-row">
          <div class="detail-label">Updated</div>
          <div class="detail-val">${j.updated_at || '–'}</div>
        </div>
      `;
    } catch (e) {
      dom.modalBody.innerHTML = `<div class="empty-state">Failed to load details: ${e.message}</div>`;
    }
  }

  function closeModal() {
    dom.modalOverlay.setAttribute('hidden', '');
  }

  dom.modalClose.addEventListener('click', closeModal);
  dom.modalOverlay.addEventListener('click', (e) => {
    if (e.target === dom.modalOverlay) closeModal();
  });

  // -------------------------------------------------------------------------
  // Submit Job Dialog
  // -------------------------------------------------------------------------
  dom.btnSubmitJob.addEventListener('click', () => {
    dom.modalTitle.textContent = 'Submit New Job';
    dom.modalBody.innerHTML = `
      <form id="job-submit-form">
        <div class="form-group">
          <label class="form-label" for="inp-type">Job Type</label>
          <select id="inp-type" class="form-select">
            <option value="test_echo">test_echo (Simple Echo)</option>
            <option value="test_slow">test_slow (Sleep 3 seconds)</option>
            <option value="test_fail">test_fail (Always Fails -> Retry/DLQ)</option>
            <option value="test_math">test_math (Math Computation)</option>
          </select>
        </div>
        <div class="form-group">
          <label class="form-label" for="inp-priority">Priority</label>
          <select id="inp-priority" class="form-select">
            <option value="1">High Priority (1)</option>
            <option value="0" selected>Normal Priority (0)</option>
            <option value="-1">Low Priority (-1)</option>
          </select>
        </div>
        <div class="form-group">
          <label class="form-label" for="inp-max-attempts">Max Attempts</label>
          <input type="number" id="inp-max-attempts" class="form-input" value="3" min="1" max="10" />
        </div>
        <div class="form-group">
          <label class="form-label" for="inp-payload">Payload (JSON)</label>
          <textarea id="inp-payload" class="form-textarea" rows="3">{"message": "Hello from DistribuQ UI!"}</textarea>
        </div>
        <div style="display:flex; justify-content:flex-end; gap:0.5rem; margin-top:1rem;">
          <button type="button" class="btn btn-ghost" id="btn-cancel-submit">Cancel</button>
          <button type="submit" class="btn btn-primary" id="btn-confirm-submit">Enqueue Job</button>
        </div>
      </form>
    `;

    dom.modalOverlay.removeAttribute('hidden');

    document.getElementById('btn-cancel-submit').addEventListener('click', closeModal);
    document.getElementById('job-submit-form').addEventListener('submit', async (e) => {
      e.preventDefault();
      const type = document.getElementById('inp-type').value;
      const priority = parseInt(document.getElementById('inp-priority').value, 10);
      const maxAttempts = parseInt(document.getElementById('inp-max-attempts').value, 10);
      let payload = {};
      try {
        payload = JSON.parse(document.getElementById('inp-payload').value);
      } catch (err) {
        showToast('Invalid JSON in payload', 'error');
        return;
      }

      try {
        const res = await fetch('/api/v1/jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            type,
            priority,
            max_attempts: maxAttempts,
            payload,
          }),
        });

        if (!res.ok) {
          const errData = await res.json();
          showToast(`Submit failed: ${errData.detail || res.statusText}`, 'error');
          return;
        }

        const data = await res.json();
        showToast(`Job ${shortId(data.job_id)} enqueued!`, 'success');
        closeModal();
        refreshJobs();
      } catch (err) {
        showToast(`Network error: ${err.message}`, 'error');
      }
    });
  });

  // -------------------------------------------------------------------------
  // Chaos Button
  // -------------------------------------------------------------------------
  dom.btnChaos.addEventListener('click', async () => {
    dom.btnChaos.disabled = true;
    try {
      const res = await fetch('/api/v1/dev/kill-worker', { method: 'POST' });
      if (res.ok) {
        const data = await res.json();
        showToast(`Chaos injected: Killed worker ${shortId(data.killed_worker_id)}!`, 'warning');
      } else if (res.status === 404) {
        showToast('Chaos failed: No alive workers found to kill', 'error');
      } else {
        const err = await res.json();
        showToast(`Chaos failed: ${err.detail || res.statusText}`, 'error');
      }
    } catch (e) {
      showToast(`Chaos request failed: ${e.message}`, 'error');
    } finally {
      setTimeout(() => { dom.btnChaos.disabled = false; }, 1000);
    }
  });

  // -------------------------------------------------------------------------
  // Throughput Chart (HTML5 Canvas)
  // -------------------------------------------------------------------------
  function initChart() {
    const canvas = dom.chartCanvas;
    if (!canvas) return;

    // Initialize with 20 empty 5-second buckets
    for (let i = 0; i < state.maxBuckets; i++) {
      state.chartBuckets.push({ success: 0, failed: 0 });
    }

    // Every 5 seconds, push currentBucket into chartBuckets and reset
    setInterval(() => {
      state.chartBuckets.push({ ...state.currentBucket });
      state.currentBucket = { success: 0, failed: 0 };
      if (state.chartBuckets.length > state.maxBuckets) {
        state.chartBuckets.shift();
      }
      drawChart();
    }, 5000);

    // Initial draw & handle resize
    drawChart();
    window.addEventListener('resize', drawChart);
  }

  function drawChart() {
    const canvas = dom.chartCanvas;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');

    // Handle high DPI
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);

    const width = rect.width;
    const height = rect.height;
    const padding = { top: 15, right: 15, bottom: 25, left: 35 };

    ctx.clearRect(0, 0, width, height);

    // Find max value in buckets
    let maxVal = 5;
    for (const b of state.chartBuckets) {
      if (b.success > maxVal) maxVal = b.success;
      if (b.failed > maxVal) maxVal = b.failed;
    }
    // Round maxVal up to multiple of 5
    maxVal = Math.ceil(maxVal / 5) * 5;

    const plotWidth = width - padding.left - padding.right;
    const plotHeight = height - padding.top - padding.bottom;

    // Draw horizontal grid lines
    ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
    ctx.lineWidth = 1;
    ctx.fillStyle = '#64748b';
    ctx.font = '10px JetBrains Mono, monospace';
    ctx.textAlign = 'right';

    const gridSteps = 4;
    for (let i = 0; i <= gridSteps; i++) {
      const y = padding.top + (plotHeight / gridSteps) * i;
      const val = Math.round(maxVal - (maxVal / gridSteps) * i);
      ctx.beginPath();
      ctx.moveTo(padding.left, y);
      ctx.lineTo(width - padding.right, y);
      ctx.stroke();
      ctx.fillText(val, padding.left - 8, y + 3);
    }

    // Draw series
    const stepX = plotWidth / (state.chartBuckets.length - 1);

    function drawLine(key, strokeColor, fillColor) {
      ctx.beginPath();
      for (let i = 0; i < state.chartBuckets.length; i++) {
        const x = padding.left + i * stepX;
        const val = state.chartBuckets[i][key];
        const y = padding.top + plotHeight - (val / maxVal) * plotHeight;
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.strokeStyle = strokeColor;
      ctx.lineWidth = 2.2;
      ctx.lineJoin = 'round';
      ctx.stroke();

      // Area fill
      ctx.lineTo(padding.left + (state.chartBuckets.length - 1) * stepX, padding.top + plotHeight);
      ctx.lineTo(padding.left, padding.top + plotHeight);
      ctx.closePath();
      ctx.fillStyle = fillColor;
      ctx.fill();
    }

    // Success line (emerald)
    drawLine('success', '#10b981', 'rgba(16, 185, 129, 0.12)');

    // Failed line (rose)
    drawLine('failed', '#f43f5e', 'rgba(244, 63, 94, 0.12)');

    // Points on the most recent bucket
    if (state.chartBuckets.length > 0) {
      const lastIdx = state.chartBuckets.length - 1;
      const lastX = padding.left + lastIdx * stepX;
      const lastSuccessY = padding.top + plotHeight - (state.chartBuckets[lastIdx].success / maxVal) * plotHeight;
      const lastFailedY = padding.top + plotHeight - (state.chartBuckets[lastIdx].failed / maxVal) * plotHeight;

      ctx.fillStyle = '#10b981';
      ctx.beginPath();
      ctx.arc(lastX, lastSuccessY, 3.5, 0, Math.PI * 2);
      ctx.fill();

      if (state.chartBuckets[lastIdx].failed > 0) {
        ctx.fillStyle = '#f43f5e';
        ctx.beginPath();
        ctx.arc(lastX, lastFailedY, 3.5, 0, Math.PI * 2);
        ctx.fill();
      }
    }
  }

  // -------------------------------------------------------------------------
  // Initialize
  // -------------------------------------------------------------------------
  window.addEventListener('DOMContentLoaded', () => {
    initChart();
    connectWs();
    refreshStats();
    refreshWorkers();
    refreshJobs();

    // Polling fallback every 10s to keep UI strictly in sync
    setInterval(() => {
      refreshStats();
      refreshWorkers();
    }, 10000);
  });

})();
