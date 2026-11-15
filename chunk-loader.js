// chunk-loader.js - 解决 Cloudflare Pages 25MB 上限，流式平滑进度与并发池调度
(function() {
  const CHUNKS = {
    'index.wasm': { count: 3, totalSize: 39514754, label: '引擎核心', mime: 'application/wasm' },
    'index.pck': { count: 3, totalSize: 41814704, label: '3D资源包', mime: 'application/octet-stream' }
  };

  const originalFetch = window.fetch;
  const totalBytesAll = 81329458;
  const loadedBytesByPart = {};
  let lastUpdateTs = 0;

  function updateStatus(label, percent, downloadedBytes) {
    const statusNotice = document.getElementById('status-notice');
    const progressBar = document.getElementById('status-progress');
    const progressInner = document.getElementById('status-progress-inner');
    
    if (statusNotice) {
      if (downloadedBytes !== undefined && downloadedBytes !== null) {
        const dlMb = (downloadedBytes / (1024 * 1024)).toFixed(1);
        const totalMb = (totalBytesAll / (1024 * 1024)).toFixed(1);
        statusNotice.textContent = '《算力余烬》下载中 (' + label + ') ' + dlMb + 'MB / ' + totalMb + 'MB (' + percent + '%)';
      } else {
        statusNotice.textContent = label;
      }
    }
    if (progressBar) progressBar.style.display = 'block';
    if (progressInner && percent !== undefined && percent !== null) {
      progressInner.style.width = Math.min(100, Math.max(0, percent)) + '%';
    }
  }

  function notifyProgress(label) {
    const now = Date.now();
    if (now - lastUpdateTs < 80) return;
    lastUpdateTs = now;
    const totalLoaded = Object.values(loadedBytesByPart).reduce((a, b) => a + b, 0);
    const pct = Math.min(99, Math.floor((totalLoaded / totalBytesAll) * 100));
    updateStatus(label, pct, totalLoaded);
  }

  async function fetchChunkWithRetry(url, partId, label, maxRetries = 3) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const resp = await originalFetch(url);
        if (!resp.ok) {
          throw new Error('HTTP ' + resp.status);
        }
        
        if (resp.body && typeof resp.body.getReader === 'function') {
          const reader = resp.body.getReader();
          const chunks = [];
          let partLoaded = 0;
          
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
            partLoaded += value.byteLength;
            loadedBytesByPart[partId] = partLoaded;
            notifyProgress(label);
          }
          
          const combined = new Uint8Array(partLoaded);
          let offset = 0;
          for (const c of chunks) {
            combined.set(c, offset);
            offset += c.byteLength;
          }
          chunks.length = 0;
          return combined.buffer;
        } else {
          const buf = await resp.arrayBuffer();
          loadedBytesByPart[partId] = buf.byteLength;
          notifyProgress(label);
          return buf;
        }
      } catch (err) {
        console.warn('[ChunkLoader] ' + url + ' 尝试第 ' + attempt + ' 次失败:', err.message);
        if (attempt === maxRetries) {
          throw new Error('分片拉取失败 [' + url + ']: ' + err.message);
        }
        await new Promise(r => setTimeout(r, 1000 * attempt));
      }
    }
  }

  async function runConcurrencyPool(tasks, maxConcurrent = 3) {
    const results = new Array(tasks.length);
    let nextIdx = 0;
    
    async function worker() {
      while (nextIdx < tasks.length) {
        const current = nextIdx++;
        results[current] = await tasks[current]();
      }
    }
    
    const workers = Array.from({ length: Math.min(maxConcurrent, tasks.length) }, () => worker());
    await Promise.all(workers);
    return results;
  }

  let chunkQueue = Promise.resolve();

  window.fetch = async function(resource, init) {
    const url = typeof resource === 'string' ? resource : (resource?.url ?? String(resource ?? ''));
    
    let matchedKey = null;
    for (const key of Object.keys(CHUNKS)) {
      if (url.endsWith(key)) {
        matchedKey = key;
        break;
      }
    }

    if (!matchedKey) {
      return originalFetch.apply(this, arguments);
    }

    const info = CHUNKS[matchedKey];
    const baseUrl = url.substring(0, url.lastIndexOf('/') + 1);

    const prevQueue = chunkQueue;
    let resolveCurrentQueue;
    chunkQueue = new Promise(resolve => { resolveCurrentQueue = resolve; });
    await prevQueue;

    try {
      updateStatus('正在连接 ' + info.label + '...', null);

      const tasks = [];
      for (let i = 0; i < info.count; i++) {
        const partIdx = i;
        const partUrl = baseUrl + matchedKey + '.part' + partIdx + (matchedKey === 'index.pck' ? '?v=f5529ee2e52b5938' : '');
        const partId = matchedKey + '#' + partIdx;
        tasks.push(() => fetchChunkWithRetry(partUrl, partId, info.label, 3));
      }

      const buffers = await runConcurrencyPool(tasks, 3);

      const blob = new Blob(buffers, { type: info.mime });
      updateStatus(info.label + ' 已就绪，正在启动引擎...', 100);
      
      return new Response(blob, {
        status: 200,
        statusText: 'OK',
        headers: new Headers({
          'Content-Type': info.mime,
          'Content-Length': String(info.totalSize),
          'Cross-Origin-Embedder-Policy': 'require-corp',
          'Cross-Origin-Resource-Policy': 'same-origin'
        })
      });
    } catch (err) {
      console.error('[ChunkLoader Error]', err);
      updateStatus('分片下载错误: ' + (err.message || err) + '，请刷新重试', null);
      throw err;
    } finally {
      if (typeof resolveCurrentQueue === 'function') {
        resolveCurrentQueue();
      }
    }
  };
})();
