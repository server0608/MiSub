<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue';
import { useRoute } from 'vue-router';
import { fetchVpsPublicSnapshot } from '../lib/api.js';
import VpsMetricChart from '../components/vps/VpsMetricChart.vue';

const route = useRoute();

const loading = ref(true);
const error = ref('');
const nodes = ref([]);
const lastUpdatedAt = ref('');
const selectedGroup = ref('全部');

// v2.1 Interactive Effects
const mouseX = ref(0);
const mouseY = ref(0);
const updateMouse = (e) => {
  mouseX.value = e.clientX;
  mouseY.value = e.clientY;
};

const displayMetrics = ref({
  total: 0,
  online: 0,
  offline: 0,
  sla: 0
});

const animateValues = (target) => {
  const duration = 1000;
  const start = Date.now();
  const initial = { ...displayMetrics.value };
  
  const step = () => {
    const elapsed = Date.now() - start;
    const progress = Math.min(elapsed / duration, 1);
    const ease = 1 - Math.pow(1 - progress, 3); // Ease out cubic
    
    displayMetrics.value.total = Math.floor(initial.total + (target.total - initial.total) * ease);
    displayMetrics.value.online = Math.floor(initial.online + (target.online - initial.online) * ease);
    displayMetrics.value.offline = Math.floor(initial.offline + (target.offline - initial.offline) * ease);
    displayMetrics.value.sla = Math.floor(initial.sla + (target.sla - initial.sla) * ease);
    
    if (progress < 1) requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
};

const groups = computed(() => {
  const g = new Set(['全部']);
  nodes.value.forEach(n => {
    if (n.groupTag) g.add(n.groupTag);
  });
  return Array.from(g);
});

const filteredNodes = computed(() => {
  if (selectedGroup.value === '全部') return nodes.value;
  return nodes.value.filter(n => n.groupTag === selectedGroup.value);
});

const statusSummary = computed(() => {
  const total = nodes.value.length;
  const online = nodes.value.filter((n) => n.status === 'online').length;
  const offline = total - online;
  return { total, online, offline };
});

const onlineRate = computed(() => {
  if (!statusSummary.value.total) return 0;
  return Math.round((statusSummary.value.online / statusSummary.value.total) * 100);
});

const topNodes = computed(() => {
  return [...nodes.value]
    .sort((a, b) => {
      if (a.status !== b.status) return a.status === 'online' ? -1 : 1;
      return (a.name || '').localeCompare(b.name || '');
    })
    .slice(0, 6);
});

const featuredIndex = ref(0);
const featuredNodes = computed(() => {
  return [...nodes.value]
    .sort((a, b) => {
      if (a.status !== b.status) return a.status === 'online' ? -1 : 1;
      return (a.name || '').localeCompare(b.name || '');
    })
    .slice(0, 8);
});

const activeFeatured = computed(() => {
  if (!featuredNodes.value.length) return null;
  const index = featuredIndex.value % featuredNodes.value.length;
  return featuredNodes.value[index];
});

const anomalyNodes = computed(() => {
  return nodes.value.filter((node) => {
    const cpu = node.latest?.cpu?.usage ?? node.latest?.cpuPercent;
    const mem = node.latest?.mem?.usage ?? node.latest?.memPercent;
    const disk = node.latest?.disk?.usage ?? node.latest?.diskPercent;
    return node.status === 'offline'
      || (Number.isFinite(cpu) && cpu >= 85)
      || (Number.isFinite(mem) && mem >= 90)
      || (Number.isFinite(disk) && disk >= 90);
  });
});

const sortedNodes = computed(() => {
  return [...filteredNodes.value].sort((a, b) => {
    if (a.status !== b.status) return a.status === 'online' ? -1 : 1;
    return (a.name || '').localeCompare(b.name || '');
  });
});

const loadSnapshot = async () => {
  loading.value = true;
  error.value = '';
  const token = typeof route.query?.token === 'string' ? route.query.token : '';
  const result = await fetchVpsPublicSnapshot(token);
  if (result.success) {
    nodes.value = result.data?.data || [];
    lastUpdatedAt.value = new Date().toLocaleString();
    
    // Trigger animation
    animateValues({
      total: statusSummary.value.total,
      online: statusSummary.value.online,
      offline: statusSummary.value.offline,
      sla: onlineRate.value
    });
  } else {
    error.value = result.error || '加载失败';
  }
  loading.value = false;
};

let rotateTimer = null;
const startRotation = () => {
  if (rotateTimer) return;
  rotateTimer = setInterval(() => {
    featuredIndex.value += 1;
  }, 6000);
};

const stopRotation = () => {
  if (rotateTimer) {
    clearInterval(rotateTimer);
    rotateTimer = null;
  }
};

const formatPercent = (val) => (val === null || val === undefined ? '--' : `${val}%`);
const formatLoad = (val) => (val === null || val === undefined ? '--' : Number(val).toFixed(2));
const formatTraffic = (traffic) => {
  if (!traffic) return '--';
  const rx = traffic.rx ?? traffic.download ?? traffic.in;
  const tx = traffic.tx ?? traffic.upload ?? traffic.out;
  const format = (value) => {
    if (value === null || value === undefined) return '--';
    const num = Number(value);
    if (isNaN(num)) return '--';
    const gb = num / (1024 * 1024 * 1024);
    return `${gb.toFixed(2)} GB`;
  };
  return `⬇ ${format(rx)} / ⬆ ${format(tx)}`;
};

const formatTotalTraffic = (bytes) => {
  if (bytes === null || bytes === undefined || bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  if (bytes < k) return bytes + ' B';
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
};

const formatUptime = (seconds) => {
  const total = Number(seconds);
  if (!Number.isFinite(total)) return '--';
  const days = Math.floor(total / 86400);
  const hours = Math.floor((total % 86400) / 3600);
  const mins = Math.floor((total % 3600) / 60);
  if (days > 0) return `${days}d ${hours}h`;
  if (hours > 0) return `${hours}h ${mins}m`;
  return `${mins}m`;
};

const averageMetric = (selector) => {
  const values = nodes.value
    .map(selector)
    .filter((val) => Number.isFinite(Number(val)))
    .map(Number);
  if (!values.length) return null;
  const sum = values.reduce((acc, val) => acc + val, 0);
  return Math.round(sum / values.length);
};

const avgCpu = computed(() => averageMetric(node => node.latest?.cpu?.usage ?? node.latest?.cpuPercent));
const avgMem = computed(() => averageMetric(node => node.latest?.mem?.usage ?? node.latest?.memPercent));
const avgDisk = computed(() => averageMetric(node => node.latest?.disk?.usage ?? node.latest?.diskPercent));
const avgLoad = computed(() => {
  const values = nodes.value
    .map(node => node.latest?.load1 ?? node.latest?.load?.load1)
    .filter((val) => Number.isFinite(Number(val)))
    .map(Number);
  if (!values.length) return null;
  const sum = values.reduce((acc, val) => acc + val, 0);
  return (sum / values.length).toFixed(2);
});

const sparklinePoints = (values) => {
  const data = values
    .map((val) => (val === null || val === undefined ? null : Number(val)))
    .filter((val) => Number.isFinite(val));
  if (!data.length) return '';
  const width = 120;
  const height = 32;
  const max = Math.max(...data, 1);
  const min = Math.min(...data, 0);
  const range = max - min || 1;
  const step = data.length > 1 ? width / (data.length - 1) : width;
  return data
    .map((val, idx) => {
      const x = Math.round(idx * step);
      const y = Math.round(height - ((val - min) / range) * height);
      return `${x},${y}`;
    })
    .join(' ');
};

const nodeSparkline = (node) => {
  const cpu = node.latest?.cpu?.usage ?? node.latest?.cpuPercent ?? null;
  const mem = node.latest?.mem?.usage ?? node.latest?.memPercent ?? null;
  const disk = node.latest?.disk?.usage ?? node.latest?.diskPercent ?? null;
  return sparklinePoints([cpu, mem, disk]);
};

const flippedNodes = ref(new Set());
const toggleFlip = (nodeId) => {
  if (flippedNodes.value.has(nodeId)) {
    flippedNodes.value.delete(nodeId);
  } else {
    flippedNodes.value.add(nodeId);
  }
};

const getLatencyColor = (ms) => {
  if (ms === null || ms === undefined) return 'text-[#8a7f70]';
  if (ms < 100) return 'text-emerald-500';
  if (ms < 300) return 'text-sky-500';
  if (ms < 500) return 'text-amber-500';
  return 'text-rose-500';
};

const getLossColor = (loss) => {
  if (loss === null || loss === undefined) return 'text-[#8a7f70]';
  if (loss === 0) return 'text-emerald-500';
  if (loss < 10) return 'text-amber-500';
  return 'text-rose-500';
};

onMounted(() => {
  loadSnapshot();
  startRotation();
  window.addEventListener('mousemove', updateMouse);
});

onUnmounted(() => {
  stopRotation();
  window.removeEventListener('mousemove', updateMouse);
});
</script>

<style scoped>
.vps-card-container {
  perspective: 1200px;
  min-height: 220px;
}

.vps-card-inner {
  position: relative;
  width: 100%;
  height: 100%;
  transition: transform 0.7s cubic-bezier(0.4, 0, 0.2, 1);
  transform-style: preserve-3d;
  cursor: pointer;
}

.vps-card-inner.is-flipped {
  transform: rotateY(180deg);
}

.vps-card-front,
.vps-card-back {
  position: absolute;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  backface-visibility: hidden;
  -webkit-backface-visibility: hidden;
  transform-style: preserve-3d;
}

.vps-card-front {
  z-index: 2;
  transform: translateZ(1px);
}

.vps-card-back {
  transform: rotateY(180deg) translateZ(1px);
}

/* v2.1 Enhanced Visuals */
@keyframes shimmer {
  0% { background-position: -200% 0; }
  100% { background-position: 200% 0; }
}

.glass-premium {
  background: rgba(255, 255, 255, 0.7);
  backdrop-filter: blur(20px);
  border: 1px solid rgba(255, 255, 255, 0.3);
  box-shadow: 0 8px 32px 0 rgba(31, 38, 135, 0.07);
}

.dark .glass-premium {
  background: rgba(15, 23, 42, 0.6);
  border: 1px solid rgba(255, 255, 255, 0.1);
  box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
}

.status-glow-online {
  box-shadow: 0 0 15px rgba(16, 185, 129, 0.2);
  animation: pulse-glow 3s infinite ease-in-out;
}

@keyframes pulse-glow {
  0%, 100% { box-shadow: 0 0 10px rgba(16, 185, 129, 0.1); }
  50% { box-shadow: 0 0 20px rgba(16, 185, 129, 0.3); }
}

.bg-shimmer {
  background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
  background-size: 200% 100%;
  animation: shimmer 2s infinite;
}
</style>

<template>
  <div class="min-h-screen bg-[#f7f6f1] dark:bg-[#0a0d14]">
    <div class="relative overflow-hidden">
      <div class="absolute inset-0">
        <!-- Interactive Glow -->
        <div 
          class="pointer-events-none absolute h-96 w-96 rounded-full bg-primary-500/10 blur-[100px] transition-transform duration-300 ease-out"
          :style="{ transform: `translate(${mouseX - 192}px, ${mouseY - 192}px)` }"
        ></div>
        <div class="absolute -top-24 left-10 h-72 w-72 rounded-full bg-gradient-to-br from-[#0ea5e9]/25 via-[#2dd4bf]/15 to-[#f97316]/20 blur-3xl"></div>
        <div class="absolute top-24 right-10 h-64 w-64 rounded-full bg-gradient-to-br from-[#f97316]/20 via-[#22c55e]/15 to-[#38bdf8]/20 blur-3xl"></div>
        <div class="absolute -bottom-24 left-1/3 h-72 w-72 rounded-full bg-gradient-to-br from-[#f59e0b]/18 via-[#22c55e]/12 to-[#0ea5e9]/16 blur-3xl"></div>
        <div class="absolute inset-0 bg-[radial-gradient(circle_at_top,#ffffff52,transparent_60%)] dark:bg-[radial-gradient(circle_at_top,#1e293b4d,transparent_60%)]"></div>
        <div class="absolute inset-0 opacity-20 dark:opacity-25 bg-[linear-gradient(transparent_23px,rgba(148,163,184,0.22)_24px),linear-gradient(90deg,transparent_23px,rgba(148,163,184,0.22)_24px)] bg-[size:24px_24px]"></div>
        <div class="absolute inset-x-0 bottom-0 h-56 bg-gradient-to-t from-[#f7f6f1] via-[#f7f6f1]/80 dark:from-[#0a0d14] dark:via-[#0a0d14]/70 to-transparent"></div>
      </div>
      <div class="relative max-w-6xl mx-auto px-6 pt-16 pb-12">
        <div class="flex flex-col gap-8 lg:flex-row lg:items-end lg:justify-between">
          <div class="max-w-2xl">
            <div class="inline-flex items-center gap-2 rounded-full border border-[#e7e1d6] bg-white/80 px-3 py-1 text-[11px] uppercase tracking-[0.28em] text-[#8a7f70] dark:border-slate-700/60 dark:bg-slate-900/70 dark:text-slate-300">
              Status Observatory
            </div>
            <h1 class="mt-4 text-4xl md:text-5xl font-semibold text-[#1f1b17] dark:text-slate-100">
              VPS 探针公开视图
            </h1>
            <p class="mt-3 text-sm md:text-base text-[#6a5f54] dark:text-slate-400">
              对外展示节点健康、资源负载与在线率。所有关键指标以清晰、可信的方式汇总呈现。
            </p>
            <div class="mt-6 flex flex-wrap items-center gap-3 text-xs text-[#6a5f54] dark:text-slate-400">
              <span class="inline-flex items-center gap-2 rounded-full border border-[#e7e1d6] bg-white/80 px-3 py-1 dark:border-slate-700/60 dark:bg-slate-900/70">
                更新时间 {{ lastUpdatedAt || '--' }}
              </span>
              <button
                class="inline-flex items-center gap-2 rounded-full border border-[#d9cdbd] bg-[#1f1b17] px-4 py-1 text-white shadow-lg shadow-[#1f1b17]/20 hover:shadow-xl dark:border-slate-700/60 dark:bg-slate-100 dark:text-slate-900 dark:shadow-slate-900/30"
                @click="loadSnapshot"
              >
                刷新数据
              </button>
            </div>
          </div>
          <div class="grid grid-cols-2 gap-4 text-xs">
            <div class="rounded-2xl border border-[#e9e2d6] bg-white/70 backdrop-blur-xl p-4 shadow-[0_14px_30px_-22px_rgba(31,27,23,0.35)] dark:border-slate-800/70 dark:bg-slate-900/60 transition-transform hover:scale-[1.02]">
              <div class="flex items-center justify-between">
                <p class="text-[#8a7f70] dark:text-slate-400">节点总数</p>
                <span class="text-[10px] px-2 py-0.5 rounded-full border border-[#efe6db] bg-[#fdfaf6] text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">ALL</span>
              </div>
              <p class="mt-2 text-2xl font-semibold text-[#1f1b17] dark:text-slate-100 tabular-nums">{{ displayMetrics.total }}</p>
            </div>
            <div class="rounded-2xl border border-[#d1fae5]/80 bg-[#ecfdf3]/80 p-4 shadow-[0_14px_30px_-22px_rgba(5,150,105,0.2)] dark:border-emerald-500/30 dark:bg-emerald-500/10 transition-transform hover:scale-[1.02]">
              <div class="flex items-center justify-between">
                <p class="text-[#087f5b] dark:text-emerald-300">在线节点</p>
                <span class="text-[10px] px-2 py-0.5 rounded-full border border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-500/40 dark:bg-emerald-500/15 dark:text-emerald-300">OK</span>
              </div>
              <p class="mt-2 text-2xl font-semibold text-[#064e3b] dark:text-emerald-200 tabular-nums">{{ displayMetrics.online }}</p>
            </div>
            <div class="rounded-2xl border border-[#fee2e2]/80 bg-[#fef2f2]/80 p-4 shadow-[0_14px_30px_-22px_rgba(244,63,94,0.22)] dark:border-rose-500/30 dark:bg-rose-500/10 transition-transform hover:scale-[1.02]">
              <div class="flex items-center justify-between">
                <p class="text-[#b91c1c] dark:text-rose-300">离线节点</p>
                <span class="text-[10px] px-2 py-0.5 rounded-full border border-rose-200 bg-rose-50 text-rose-700 dark:border-rose-500/40 dark:bg-rose-500/15 dark:text-rose-300">DOWN</span>
              </div>
              <p class="mt-2 text-2xl font-semibold text-[#7f1d1d] dark:text-rose-200 tabular-nums">{{ displayMetrics.offline }}</p>
            </div>
            <div class="rounded-2xl border border-[#e0e7ff]/80 bg-[#eef2ff]/80 p-4 shadow-[0_14px_30px_-22px_rgba(59,130,246,0.22)] dark:border-sky-500/30 dark:bg-sky-500/10 transition-transform hover:scale-[1.02]">
              <div class="flex items-center justify-between">
                <p class="text-[#3730a3] dark:text-sky-300">在线率</p>
                <span class="text-[10px] px-2 py-0.5 rounded-full border border-sky-200 bg-sky-50 text-sky-700 dark:border-sky-500/40 dark:bg-sky-500/15 dark:text-sky-300">SLA</span>
              </div>
              <p class="mt-2 text-2xl font-semibold text-[#312e81] dark:text-sky-200 tabular-nums">{{ displayMetrics.sla }}%</p>
            </div>
          </div>
        </div>
        <div class="mt-8 rounded-[24px] border border-[#e9e2d6] bg-white/70 backdrop-blur-xl p-4 shadow-[0_12px_30px_-20px_rgba(31,27,23,0.35)] dark:border-slate-800/70 dark:bg-slate-900/60">
          <div class="flex flex-wrap items-center justify-between gap-4">
            <div class="flex items-center gap-3">
              <div class="h-2 w-24 rounded-full bg-[#efe6db] dark:bg-slate-800">
                <div class="h-2 rounded-full bg-gradient-to-r from-emerald-500 via-sky-400 to-amber-400" :style="{ width: onlineRate + '%' }"></div>
              </div>
              <span class="text-xs text-[#8a7f70] dark:text-slate-400">在线率趋势</span>
            </div>
            <div class="flex flex-wrap items-center gap-2 text-[11px]">
              <span class="px-2 py-1 rounded-full border border-[#efe6db] bg-[#fdfaf6] text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">CPU ≤ 85%</span>
              <span class="px-2 py-1 rounded-full border border-[#efe6db] bg-[#fdfaf6] text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">内存 ≤ 90%</span>
              <span class="px-2 py-1 rounded-full border border-[#efe6db] bg-[#fdfaf6] text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">响应稳定</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="max-w-6xl mx-auto px-6 pb-16">
      <div v-if="loading" class="py-16 text-center text-sm text-[#8a7f70] dark:text-slate-400">正在加载探针数据...</div>
      <div v-else-if="error" class="py-16 text-center text-sm text-rose-600 dark:text-rose-300">{{ error }}</div>
      <div v-else class="space-y-10">
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div class="rounded-[28px] border border-[#e9e2d6] bg-white/80 backdrop-blur-2xl p-6 shadow-[0_20px_60px_-40px_rgba(31,27,23,0.45)] lg:col-span-2 dark:border-slate-800/70 dark:bg-slate-900/60 dark:shadow-black/50">
            <div class="flex items-center justify-between">
              <h2 class="text-lg font-semibold text-[#1f1b17] dark:text-slate-100">重点节点</h2>
              <span class="text-xs text-[#8a7f70] dark:text-slate-400">按在线优先展示</span>
            </div>
            <div class="mt-5 grid grid-cols-1 md:grid-cols-2 gap-4">
              <div v-for="node in topNodes" :key="node.id" class="vps-card-container">
                <div class="vps-card-inner group" :class="{ 'is-flipped': flippedNodes.has(node.id) }" @click="toggleFlip(node.id)">
                  <!-- Front Side -->
                  <div class="vps-card-front rounded-2xl border border-[#efe6db] bg-[#fdfaf6] p-4 dark:border-slate-800 dark:bg-slate-900">
                    <div class="h-1 w-full rounded-full bg-[#efe6db] dark:bg-slate-800 relative">
                      <div class="absolute inset-0 flex items-center justify-between px-1 opacity-40">
                        <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                        <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                        <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                        <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                      </div>
                      <div
                        class="h-1 rounded-full bg-gradient-to-r"
                        :class="node.status === 'online'
                          ? 'from-emerald-500 via-sky-400 to-amber-400'
                          : 'from-rose-500 via-orange-400 to-amber-300'"
                        :style="{ width: node.status === 'online' ? '100%' : '45%' }"
                      ></div>
                    </div>
                    <div class="flex items-start justify-between mt-2">
                      <div>
                        <div class="flex items-center gap-2">
                          <img 
                            v-if="node.countryCode" 
                            :src="`https://flagcdn.com/w20/${node.countryCode.toLowerCase()}.png`" 
                            class="h-3.5 w-auto rounded-sm opacity-90" 
                            alt=""
                            :title="node.countryCode"
                            @error="(e) => e.target.style.display = 'none'"
                          />
                          <p class="text-sm font-semibold text-[#1f1b17] dark:text-slate-100">{{ node.name || node.id }}</p>
                        </div>
                        <p class="text-xs text-[#8a7f70] dark:text-slate-400">
                          <span v-if="node.tag" class="mr-1">{{ node.tag }} ·</span>
                          {{ node.region || '未知地区' }}
                          <span class="ml-1 opacity-70">| 📊 {{ formatTotalTraffic(node.totalRx + node.totalTx) }}</span>
                        </p>
                        <div class="mt-2 flex flex-wrap items-center gap-2 text-[10px]">
                          <span class="inline-flex items-center gap-1 rounded-full border border-[#efe6db] bg-white/70 px-2 py-0.5 text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                            ⚡ 负载: {{ formatLoad(node.latest?.load1 ?? node.latest?.load?.load1) }}
                          </span>
                        </div>
                      </div>
                      <span class="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px]"
                        :class="node.status === 'online'
                          ? 'border-[#bbf7d0] bg-[#ecfdf3] text-[#0f766e] dark:border-emerald-500/40 dark:bg-emerald-500/15 dark:text-emerald-300'
                          : 'border-[#fecdd3] bg-[#fff1f2] text-[#be123c] dark:border-rose-500/40 dark:bg-rose-500/15 dark:text-rose-300'"
                      >
                        {{ node.status === 'online' ? '在线' : '离线' }}
                      </span>
                    </div>
                    <div class="mt-3 flex items-center justify-between">
                      <svg viewBox="0 0 120 32" class="h-8 w-28">
                        <polyline :points="nodeSparkline(node)" fill="none" stroke="#0ea5e9" stroke-width="2" stroke-linecap="round" />
                      </svg>
                      <span class="text-[10px] text-[#8a7f70] dark:text-slate-400">CPU/MEM/DISK</span>
                    </div>
                    <div class="mt-4 grid grid-cols-2 gap-2 text-[11px] text-[#6a5f54] dark:text-slate-400">
                      <div>CPU {{ formatPercent(node.latest?.cpu?.usage ?? node.latest?.cpuPercent) }}</div>
                      <div>内存 {{ formatPercent(node.latest?.mem?.usage ?? node.latest?.memPercent) }}</div>
                      <div>磁盘 {{ formatPercent(node.latest?.disk?.usage ?? node.latest?.diskPercent) }}</div>
                      <div>流量 {{ formatTraffic(node.latest?.traffic) }}</div>
                    </div>
                    <!-- Traffic Limit Progress -->
                    <div v-if="node.trafficLimitGb > 0" class="mt-4 pt-3 border-t border-[#efe6db]/60 dark:border-slate-800/60">
                      <div class="flex justify-end items-center text-[10px] mb-1 text-emerald-600 dark:text-emerald-400">
                        <span class="font-medium text-[#6a5f54] dark:text-slate-300">限额: {{ node.trafficLimitGb }} GB</span>
                      </div>
                      <div class="h-1 w-full bg-[#efe6db] dark:bg-slate-800 rounded-full overflow-hidden mt-1">
                        <div 
                          class="h-full transition-all duration-500" 
                          :class="((node.totalRx + node.totalTx) / (node.trafficLimitGb * 1024 * 1024 * 1024) * 100) > 95
                            ? 'bg-rose-500' 
                            : (((node.totalRx + node.totalTx) / (node.trafficLimitGb * 1024 * 1024 * 1024) * 100) > 80 ? 'bg-amber-500' : 'bg-gradient-to-r from-blue-400 to-indigo-500')"
                          :style="{ width: Math.min(100, ((node.totalRx + node.totalTx) / (node.trafficLimitGb * 1024 * 1024 * 1024) * 100)) + '%' }"
                        ></div>
                      </div>
                    </div>
                  </div>

                  <!-- Back Side: Network Metrics -->
                  <div class="vps-card-back rounded-2xl border border-[#efe6db] bg-[#fdfaf6] p-4 dark:border-slate-800 dark:bg-slate-900 flex flex-col h-full">
                    <div class="flex items-center justify-between mb-2 border-b border-[#efe6db] pb-1.5 dark:border-slate-800">
                      <h4 class="text-xs font-semibold text-[#1f1b17] dark:text-slate-100 flex items-center gap-1">
                        <span class="text-blue-500 text-[10px]">🌐</span> 网络状态
                      </h4>
                      <span class="text-[9px] text-[#8a7f70] dark:text-slate-400 opacity-70">点击返回</span>
                    </div>
                    
                    <div v-if="node.latest?.network && node.latest.network.length" class="flex-1 overflow-y-auto pr-1">
                      <div class="grid grid-cols-[1fr_65px_45px] gap-2 px-1 mb-1.5 text-[10px] font-bold text-[#8a7f70] dark:text-slate-500 uppercase tracking-wider border-b border-[#efe6db]/50 dark:border-slate-800/50 pb-0.5">
                        <span>监测点</span>
                        <span class="text-right">延迟</span>
                        <span class="text-right">丢包</span>
                      </div>
                      <div class="divide-y divide-[#efe6db]/60 dark:divide-slate-800/60">
                        <div v-for="(check, idx) in node.latest.network" :key="idx" class="grid grid-cols-[1fr_65px_45px] gap-2 py-2 items-baseline hover:bg-black/5 dark:hover:bg-white/5 rounded-sm px-1 transition-colors">
                          <span class="text-[11px] font-medium text-[#2c2721] dark:text-slate-200 truncate" :title="check.name || check.target">
                            {{ check.name || check.target.replace(/^http(s)?:\/\//, '') }}
                          </span>
                          <span class="text-[11px] font-bold text-right tabular-nums" :class="getLatencyColor(check.latencyMs)">
                            {{ check.latencyMs !== null ? Math.round(check.latencyMs) + 'ms' : '--' }}
                          </span>
                          <span class="text-[11px] font-bold text-right tabular-nums" :class="getLossColor(check.lossPercent)">
                            {{ check.lossPercent !== null ? check.lossPercent + '%' : '--' }}
                          </span>
                        </div>
                      </div>
                    </div>
                    <div v-else class="flex flex-col items-center justify-center flex-1 text-center opacity-60">
                      <span class="text-xl mb-1">📡</span>
                      <p class="text-[10px] text-[#8a7f70] dark:text-slate-400">暂无网络监控数据</p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="rounded-[28px] border border-[#e6e0d6] bg-gradient-to-br from-white/90 via-[#fdfaf6]/80 to-[#f4efe7]/80 p-6 shadow-[0_20px_60px_-40px_rgba(31,27,23,0.45)] dark:border-slate-800/70 dark:bg-gradient-to-br dark:from-slate-900/70 dark:via-slate-900/55 dark:to-slate-800/55 dark:shadow-black/50">
            <h2 class="text-lg font-semibold text-[#1f1b17] dark:text-slate-100">资源脉冲</h2>
            <p class="mt-1 text-xs text-[#8a7f70] dark:text-slate-400">汇总最近一次上报的资源占用</p>
            <div class="mt-5 grid grid-cols-2 gap-4">
              <VpsMetricChart title="CPU" unit="%" :points="nodes.map(node => node.latest?.cpu?.usage ?? node.latest?.cpuPercent ?? null)" color="#0ea5e9" :height="80" />
              <VpsMetricChart title="内存" unit="%" :points="nodes.map(node => node.latest?.mem?.usage ?? node.latest?.memPercent ?? null)" color="#f97316" :height="80" />
              <VpsMetricChart title="磁盘" unit="%" :points="nodes.map(node => node.latest?.disk?.usage ?? node.latest?.diskPercent ?? null)" color="#22c55e" :height="80" />
              <VpsMetricChart title="流量" unit="GB" :points="nodes.map(node => {
                const b = node.latest?.traffic?.rx ?? node.latest?.traffic?.download ?? null;
                return b !== null ? Number((b / (1024 * 1024 * 1024)).toFixed(2)) : null;
              })" color="#6366f1" :height="80" :max="10" />
            </div>
            <div class="mt-6 grid grid-cols-2 gap-3 text-xs">
              <div class="rounded-2xl border border-[#efe6db] bg-white/70 px-3 py-3 text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                平均 CPU <span class="font-semibold">{{ formatPercent(avgCpu) }}</span>
              </div>
              <div class="rounded-2xl border border-[#efe6db] bg-white/70 px-3 py-3 text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                平均内存 <span class="font-semibold">{{ formatPercent(avgMem) }}</span>
              </div>
              <div class="rounded-2xl border border-[#efe6db] bg-white/70 px-3 py-3 text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                平均磁盘 <span class="font-semibold">{{ formatPercent(avgDisk) }}</span>
              </div>
              <div class="rounded-2xl border border-[#efe6db] bg-white/70 px-3 py-3 text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                平均负载 <span class="font-semibold">{{ avgLoad || '--' }}</span>
              </div>
            </div>
          </div>
        </div>

        <!-- Featured Carousel -->
        <div class="rounded-[30px] border border-[#e7e1d6] bg-white/80 backdrop-blur-2xl p-6 shadow-[0_20px_60px_-40px_rgba(31,27,23,0.45)] dark:border-slate-800/70 dark:bg-slate-900/60 dark:shadow-black/50">
          <div class="flex items-center justify-between">
            <h2 class="text-lg font-semibold text-[#1f1b17] dark:text-slate-100">重点轮播</h2>
            <span class="text-xs text-[#8a7f70] dark:text-slate-400">每 6 秒切换</span>
          </div>
          <div class="mt-5 grid grid-cols-1 md:grid-cols-[1.2fr_1fr] gap-4">
            <div class="rounded-2xl border border-[#efe6db] bg-[#fdfaf6] p-4 dark:border-slate-800 dark:bg-slate-900/60">
              <div class="flex items-start justify-between mt-3">
                <div>
                    <div class="flex items-center gap-2">
                      <img 
                        v-if="activeFeatured?.countryCode" 
                        :src="`https://flagcdn.com/w20/${activeFeatured.countryCode.toLowerCase()}.png`" 
                        class="h-3.5 w-auto rounded-sm opacity-90" 
                        alt=""
                        :title="activeFeatured.countryCode"
                        @error="(e) => e.target.style.display = 'none'"
                      />
                      <p class="text-sm font-semibold text-[#1f1b17] dark:text-slate-100">{{ activeFeatured?.name || activeFeatured?.id || '--' }}</p>
                    </div>
                  <p class="text-xs text-[#8a7f70] dark:text-slate-400">{{ activeFeatured?.tag || '--' }} · {{ activeFeatured?.region || '未知地区' }}</p>
                </div>
                <span class="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px]"
                  :class="activeFeatured?.status === 'online'
                    ? 'border-[#bbf7d0] bg-[#ecfdf3] text-[#0f766e] dark:border-emerald-500/40 dark:bg-emerald-500/15 dark:text-emerald-300'
                    : 'border-[#fecdd3] bg-[#fff1f2] text-[#be123c] dark:border-rose-500/40 dark:bg-rose-500/15 dark:text-rose-300'"
                >
                  {{ activeFeatured?.status === 'online' ? '在线' : '离线' }}
                </span>
              </div>
              <div class="mt-4 grid grid-cols-2 gap-4 text-[11px] text-[#6a5f54] dark:text-slate-400">
                <div class="flex justify-between"><span>CPU</span> <span class="font-medium">{{ formatPercent(activeFeatured?.latest?.cpu?.usage ?? activeFeatured?.latest?.cpuPercent) }}</span></div>
                <div class="flex justify-between"><span>内存</span> <span class="font-medium">{{ formatPercent(activeFeatured?.latest?.mem?.usage ?? activeFeatured?.latest?.memPercent) }}</span></div>
                <div class="flex justify-between"><span>磁盘</span> <span class="font-medium">{{ formatPercent(activeFeatured?.latest?.disk?.usage ?? activeFeatured?.latest?.diskPercent) }}</span></div>
                <div class="flex justify-between"><span>运行</span> <span class="font-medium">{{ formatUptime(activeFeatured?.latest?.uptimeSec) }}</span></div>
              </div>
            </div>
            
            <div class="rounded-2xl border border-[#efe6db] bg-[#fdfaf6] p-4 dark:border-slate-800 dark:bg-slate-900/60">
              <h3 class="text-sm font-semibold text-[#1f1b17] dark:text-slate-100">运行概览</h3>
              <p class="mt-1 text-xs text-[#8a7f70] dark:text-slate-400">健康状况自检</p>
              <div v-if="anomalyNodes.length" class="mt-3 space-y-2 text-[11px] text-[#6a5f54] dark:text-slate-400">
                <div v-for="node in anomalyNodes.slice(0,5)" :key="node.id" class="flex items-center justify-between">
                  <span>{{ node.name || node.id }}</span>
                  <span class="px-2 py-0.5 rounded-full border"
                    :class="node.status === 'offline'
                      ? 'border-[#fecdd3] bg-[#fff1f2] text-[#be123c] dark:border-rose-500/40 dark:bg-rose-500/15 dark:text-rose-300'
                      : 'border-[#fde68a] bg-[#fffbeb] text-[#b45309] dark:border-amber-500/40 dark:bg-amber-500/15 dark:text-amber-300'"
                  >
                    {{ node.status === 'offline' ? '离线' : '重负载' }}
                  </span>
                </div>
              </div>
              <div v-else class="mt-3 text-[11px] text-[#8a7f70] dark:text-slate-400">所有节点运行良好</div>
            </div>
          </div>
        </div>

        <!-- All Nodes -->
        <div class="rounded-[30px] border border-[#e7e1d6] bg-white/90 p-6 shadow-xl shadow-[#d8cab8]/30 dark:border-slate-800 dark:bg-slate-900/70 dark:shadow-black/40">
          <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
            <h2 class="text-lg font-semibold text-[#1f1b17] dark:text-slate-100">全部节点</h2>
            <!-- Group Tabs -->
            <div v-if="groups.length > 1" class="flex items-center gap-1 overflow-x-auto p-1 bg-[#efe6db]/50 dark:bg-slate-800/50 rounded-xl no-scrollbar">
              <button 
                v-for="group in groups" 
                :key="group"
                @click="selectedGroup = group"
                class="whitespace-nowrap px-4 py-1.5 rounded-lg text-[11px] font-medium transition-all"
                :class="selectedGroup === group 
                  ? 'bg-white text-blue-600 shadow-sm dark:bg-slate-700 dark:text-blue-400' 
                  : 'text-[#8a7f70] hover:text-[#1f1b17] dark:text-slate-400 dark:hover:text-slate-200'"
              >
                {{ group }}
              </button>
            </div>
            <span class="text-xs text-[#8a7f70] dark:text-slate-400">共 {{ filteredNodes.length }} 个节点</span>
          </div>
          <div v-if="anomalyNodes.length" class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
            <div
              v-for="node in anomalyNodes.slice(0, 4)"
              :key="node.id"
              class="rounded-2xl border border-rose-200 bg-rose-50/80 p-4 text-xs text-rose-900 shadow-sm dark:border-rose-500/40 dark:bg-rose-500/10 dark:text-rose-200"
            >
              <div class="flex items-center justify-between">
                <div>
                  <p class="text-sm font-semibold">{{ node.name || node.id }}</p>
                  <p class="text-[11px] opacity-80">{{ node.tag || '--' }} · {{ node.region || '未知地区' }}</p>
                </div>
                <span class="px-2 py-0.5 rounded-full border border-rose-200 bg-white/70 text-[10px] dark:border-rose-500/40 dark:bg-rose-500/20">
                  {{ node.status === 'offline' ? '离线' : '高负载' }}
                </span>
              </div>
              <div class="mt-2 grid grid-cols-2 gap-2 text-[11px]">
                <div>CPU {{ formatPercent(node.latest?.cpu?.usage ?? node.latest?.cpuPercent) }}</div>
                <div>内存 {{ formatPercent(node.latest?.mem?.usage ?? node.latest?.memPercent) }}</div>
                <div>磁盘 {{ formatPercent(node.latest?.disk?.usage ?? node.latest?.diskPercent) }}</div>
                <div>负载 {{ formatLoad(node.latest?.load1 ?? node.latest?.load?.load1) }}</div>
              </div>
            </div>
          </div>
          <div class="mt-5 grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
            <div v-for="node in sortedNodes" :key="node.id" class="vps-card-container">
              <div class="vps-card-inner group text-left" :class="{ 'is-flipped': flippedNodes.has(node.id) }" @click="toggleFlip(node.id)">
                <!-- Front Side -->
                <div class="vps-card-front rounded-2xl border border-[#efe6db] bg-[#fdfaf6] p-4 dark:border-slate-800 dark:bg-slate-900">
                  <div class="h-1 w-full rounded-full bg-[#efe6db] dark:bg-slate-800 relative">
                    <div class="absolute inset-0 flex items-center justify-between px-1 opacity-40">
                      <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                      <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                      <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                      <span class="h-0.5 w-2 bg-white/70 dark:bg-white/20"></span>
                    </div>
                    <div
                      class="h-1 rounded-full bg-gradient-to-r"
                      :class="node.status === 'online'
                        ? 'from-emerald-500 via-sky-400 to-amber-400'
                        : 'from-rose-500 via-orange-400 to-amber-300'"
                      :style="{ width: node.status === 'online' ? '100%' : '45%' }"
                    ></div>
                  </div>
                  <div class="flex items-start justify-between mt-2">
                    <div>
                      <div class="flex items-center gap-2">
                        <img v-if="node.countryCode" :src="`https://flagcdn.com/w20/${node.countryCode.toLowerCase()}.png`" 
                          class="h-3 rounded-sm opacity-90" 
                          alt="" 
                          :title="node.countryCode"
                          @error="(e) => e.target.style.display = 'none'" 
                        />
                        <p class="text-sm font-semibold text-[#1f1b17] dark:text-slate-100">{{ node.name || node.id }}</p>
                      </div>
                      <p class="text-xs text-[#8a7f70] dark:text-slate-400">
                        <span v-if="node.tag" class="mr-1">{{ node.tag }} ·</span>
                        {{ node.region || '未知地区' }}
                        <span class="ml-1 opacity-70">| 📊 {{ formatTotalTraffic(node.totalRx + node.totalTx) }}</span>
                      </p>
                      <div class="mt-2 flex flex-wrap items-center gap-2 text-[10px]">
                        <span class="inline-flex items-center gap-1 rounded-full border border-[#efe6db] bg-white/70 px-2 py-0.5 text-[#6a5f54] dark:border-slate-800 dark:bg-slate-900/60 dark:text-slate-300">
                          ⏱ {{ formatUptime(node.latest?.uptimeSec) }}
                        </span>
                      </div>
                    </div>
                    <span class="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[10px]"
                      :class="node.status === 'online'
                        ? 'border-[#bbf7d0] bg-[#ecfdf3] text-[#0f766e] dark:border-emerald-500/40 dark:bg-emerald-500/15 dark:text-emerald-300'
                        : 'border-[#fecdd3] bg-[#fff1f2] text-[#be123c] dark:border-rose-500/40 dark:bg-rose-500/15 dark:text-rose-300'"
                    >
                      {{ node.status === 'online' ? '在线' : '离线' }}
                    </span>
                  </div>
                  <div class="mt-3 flex items-center justify-between">
                    <svg viewBox="0 0 120 32" class="h-8 w-28">
                      <polyline :points="nodeSparkline(node)" fill="none" stroke="#22c55e" stroke-width="2" stroke-linecap="round" />
                    </svg>
                    <span class="text-[10px] text-[#8a7f70] dark:text-slate-400">CPU/MEM/DISK</span>
                  </div>
                  <div class="mt-4 grid grid-cols-2 gap-2 text-[11px] text-[#6a5f54] dark:text-slate-400">
                    <div>CPU {{ formatPercent(node.latest?.cpu?.usage ?? node.latest?.cpuPercent) }}</div>
                    <div>内存 {{ formatPercent(node.latest?.mem?.usage ?? node.latest?.memPercent) }}</div>
                    <div>磁盘 {{ formatPercent(node.latest?.disk?.usage ?? node.latest?.diskPercent) }}</div>
                    <div>流量 {{ formatTraffic(node.latest?.traffic) }}</div>
                  </div>
                  <!-- Traffic Limit Progress -->
                  <div v-if="node.trafficLimitGb > 0" class="mt-4 pt-3 border-t border-[#efe6db]/60 dark:border-slate-800/60">
                    <div class="flex justify-end items-center text-[10px] mb-1 text-emerald-600 dark:text-emerald-400">
                      <span class="font-medium text-[#6a5f54] dark:text-slate-300">限额: {{ node.trafficLimitGb }} GB</span>
                    </div>
                    <div class="h-1 w-full bg-[#efe6db] dark:bg-slate-800 rounded-full overflow-hidden mt-1">
                      <div 
                        class="h-full transition-all duration-500" 
                        :class="((node.totalRx + node.totalTx) / (node.trafficLimitGb * 1024 * 1024 * 1024) * 100) > 95
                          ? 'bg-rose-500' 
                          : (((node.totalRx + node.totalTx) / (node.trafficLimitGb * 1024 * 1024 * 1024) * 100) > 80 ? 'bg-amber-500' : 'bg-gradient-to-r from-emerald-400 to-sky-500')"
                        :style="{ width: Math.min(100, ((node.totalRx + node.totalTx) / (node.trafficLimitGb * 1024 * 1024 * 1024) * 100)) + '%' }"
                      ></div>
                    </div>
                  </div>
                </div>

                <!-- Back Side: Network Metrics -->
                <div class="vps-card-back rounded-2xl border border-[#efe6db] bg-[#fdfaf6] p-4 dark:border-slate-800 dark:bg-slate-900 flex flex-col h-full">
                  <div class="flex items-center justify-between mb-2 border-b border-[#efe6db] pb-1.5 dark:border-slate-800">
                    <h4 class="text-xs font-semibold text-[#1f1b17] dark:text-slate-100 flex items-center gap-1">
                      <span class="text-blue-500 text-[10px]">🌐</span> 网络状态
                    </h4>
                    <span class="text-[9px] text-[#8a7f70] dark:text-slate-400 opacity-70">点击返回</span>
                  </div>
                  
                  <div v-if="node.latest?.network && node.latest.network.length" class="flex-1 overflow-y-auto pr-1">
                    <div class="grid grid-cols-[1fr_55px_40px] gap-2 px-1 mb-1 text-[9px] font-bold text-[#8a7f70] dark:text-slate-500 uppercase tracking-wider">
                      <span>监测点</span>
                      <span class="text-right">延迟</span>
                      <span class="text-right">丢包</span>
                    </div>
                    <div class="divide-y divide-[#efe6db] dark:divide-slate-800">
                      <div v-for="(check, idx) in node.latest.network" :key="idx" class="grid grid-cols-[1fr_55px_40px] gap-2 py-1.5 items-baseline">
                        <span class="text-[10px] text-[#2c2721] dark:text-slate-200 truncate" :title="check.name || check.target">
                          {{ check.name || check.target.replace(/^http(s)?:\/\//, '') }}
                        </span>
                        <span class="text-[10px] font-bold text-right tabular-nums" :class="getLatencyColor(check.latencyMs)">
                          {{ check.latencyMs !== null ? Math.round(check.latencyMs) + 'ms' : '--' }}
                        </span>
                        <span class="text-[10px] font-bold text-right tabular-nums" :class="getLossColor(check.lossPercent)">
                          {{ check.lossPercent !== null ? check.lossPercent + '%' : '--' }}
                        </span>
                      </div>
                    </div>
                  </div>
                  <div v-else class="flex flex-col items-center justify-center flex-1 text-center opacity-60">
                    <span class="text-xl mb-1">📡</span>
                    <p class="text-[10px] text-[#8a7f70] dark:text-slate-400">暂无网络监控数据</p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <details class="rounded-[30px] border border-[#e7e1d6] bg-white/80 backdrop-blur-2xl p-6 shadow-[0_20px_60px_-40px_rgba(31,27,23,0.45)] dark:border-slate-800/70 dark:bg-slate-900/60 dark:shadow-black/50">
          <summary class="flex cursor-pointer items-center justify-between text-sm font-semibold text-[#1f1b17] dark:text-slate-100">
            <span>节点明细表</span>
            <span class="text-xs text-[#8a7f70] dark:text-slate-400">点击展开</span>
          </summary>
          <div class="mt-4 overflow-x-auto">
            <table class="w-full text-xs">
              <thead class="text-[#8a7f70] dark:text-slate-400">
                <tr class="text-left">
                  <th class="py-2">节点</th>
                  <th class="py-2">地区</th>
                  <th class="py-2">状态</th>
                  <th class="py-2">CPU</th>
                  <th class="py-2">内存</th>
                  <th class="py-2">磁盘</th>
                  <th class="py-2">负载</th>
                  <th class="py-2">运行</th>
                </tr>
              </thead>
              <tbody class="text-[#1f1b17] dark:text-slate-200">
                <tr v-for="node in sortedNodes" :key="node.id" class="border-t border-[#efe6db] dark:border-slate-800">
                  <td class="py-2">{{ node.name || node.id }}</td>
                  <td class="py-2">{{ node.region || '--' }}</td>
                  <td class="py-2">{{ node.status === 'online' ? '在线' : '离线' }}</td>
                  <td class="py-2">{{ formatPercent(node.latest?.cpu?.usage ?? node.latest?.cpuPercent) }}</td>
                  <td class="py-2">{{ formatPercent(node.latest?.mem?.usage ?? node.latest?.memPercent) }}</td>
                  <td class="py-2">{{ formatPercent(node.latest?.disk?.usage ?? node.latest?.diskPercent) }}</td>
                  <td class="py-2">{{ formatLoad(node.latest?.load1 ?? node.latest?.load?.load1) }}</td>
                  <td class="py-2">{{ formatUptime(node.latest?.uptimeSec) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </details>
      </div>
    </div>
  </div>
</template>
