<script setup>
import { computed } from 'vue';
import Switch from '../../../ui/Switch.vue';

const props = defineProps({
  settings: {
    type: Object,
    required: true
  }
});

const vpsMonitorConfig = computed({
  get() {
    return props.settings.vpsMonitor || {
      enabled: true,
      requireSecret: true,
      requireSignature: false,
      signatureClockSkewMinutes: 5,
      offlineThresholdMinutes: 10,
      cpuWarnPercent: 90,
      memWarnPercent: 90,
      diskWarnPercent: 90,
      overloadConfirmCount: 2,
      alertCooldownMinutes: 15,
      alertsEnabled: true,
      notifyOffline: true,
      notifyRecovery: true,
      notifyOverload: true,
      reportRetentionDays: 30,
      cooldownIgnoreRecovery: true,
      networkSampleIntervalMinutes: 5,
      reportIntervalMinutes: 1,
      reportStoreIntervalMinutes: 1,
      networkTargetsLimit: 3,
      publicPageEnabled: false,
      publicPageToken: ''
    };
  },
  set(value) {
    props.settings.vpsMonitor = value;
  }
});

const updateField = (key, value) => {
  vpsMonitorConfig.value = {
    ...vpsMonitorConfig.value,
    [key]: value
  };
};
</script>

<template>
  <div class="bg-white/90 dark:bg-gray-900/70 misub-radius-lg p-6 space-y-5 border border-gray-100/80 dark:border-white/10 shadow-sm transition-shadow duration-300">
    <div>
      <h3 class="text-base font-semibold text-gray-900 dark:text-white flex items-center gap-2">
        <svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5 text-indigo-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 6h16M4 12h16M4 18h16M7 6v12M12 6v12M17 6v12" />
        </svg>
        VPS 探针设置
      </h3>
      <p class="text-xs text-gray-500 dark:text-gray-400 mt-1">
        管理探针上报开关、离线阈值与告警通知规则。
      </p>
      <p class="text-xs text-amber-600 dark:text-amber-400 mt-2">
        使用 VPS 探针前请确保已绑定 D1 数据库（MISUB_DB），并在存储设置中切换为 D1 模式。
      </p>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">启用 VPS 探针</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">关闭后拒绝所有上报请求</div>
        </div>
        <Switch v-model="vpsMonitorConfig.enabled" />
      </div>
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">告警推送</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">发送离线/负载等提醒</div>
        </div>
        <Switch v-model="vpsMonitorConfig.alertsEnabled" />
      </div>
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">公开展示页</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">用于对外展示探针数据</div>
        </div>
        <Switch v-model="vpsMonitorConfig.publicPageEnabled" />
      </div>
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">上报签名校验</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">启用 HMAC 防重放</div>
        </div>
        <Switch v-model="vpsMonitorConfig.requireSignature" />
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">离线阈值（分钟）</label>
        <input
          type="number"
          min="1"
          max="1440"
          :value="vpsMonitorConfig.offlineThresholdMinutes"
          @input="updateField('offlineThresholdMinutes', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">上报间隔（分钟）</label>
        <input
          type="number"
          min="1"
          max="60"
          :value="vpsMonitorConfig.reportIntervalMinutes"
          @input="updateField('reportIntervalMinutes', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">记录间隔（分钟）</label>
        <input
          type="number"
          min="1"
          max="60"
          :value="vpsMonitorConfig.reportStoreIntervalMinutes"
          @input="updateField('reportStoreIntervalMinutes', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">网络采样间隔（分钟）</label>
        <input
          type="number"
          min="1"
          max="60"
          :value="vpsMonitorConfig.networkSampleIntervalMinutes"
          @input="updateField('networkSampleIntervalMinutes', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">公开页 Token（可选）</label>
        <input
          type="text"
          :value="vpsMonitorConfig.publicPageToken"
          @input="updateField('publicPageToken', $event.target.value)"
          placeholder="留空则公开访问"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">告警冷却（分钟）</label>
        <input
          type="number"
          min="1"
          max="1440"
          :value="vpsMonitorConfig.alertCooldownMinutes"
          @input="updateField('alertCooldownMinutes', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">签名时钟偏差（分钟）</label>
        <input
          type="number"
          min="1"
          max="60"
          :value="vpsMonitorConfig.signatureClockSkewMinutes"
          @input="updateField('signatureClockSkewMinutes', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">CPU 告警阈值 %</label>
        <input
          type="number"
          min="1"
          max="100"
          :value="vpsMonitorConfig.cpuWarnPercent"
          @input="updateField('cpuWarnPercent', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">内存告警阈值 %</label>
        <input
          type="number"
          min="1"
          max="100"
          :value="vpsMonitorConfig.memWarnPercent"
          @input="updateField('memWarnPercent', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">磁盘告警阈值 %</label>
        <input
          type="number"
          min="1"
          max="100"
          :value="vpsMonitorConfig.diskWarnPercent"
          @input="updateField('diskWarnPercent', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">负载告警确认次数</label>
        <input
          type="number"
          min="1"
          max="10"
          :value="vpsMonitorConfig.overloadConfirmCount"
          @input="updateField('overloadConfirmCount', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">恢复通知不受冷却</label>
        <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
          <div>
            <div class="text-xs text-gray-500 dark:text-gray-400">恢复在线立即提醒</div>
          </div>
          <Switch v-model="vpsMonitorConfig.cooldownIgnoreRecovery" />
        </div>
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">网络目标上限</label>
        <input
          type="number"
          min="1"
          max="10"
          :value="vpsMonitorConfig.networkTargetsLimit"
          @input="updateField('networkTargetsLimit', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div>
        <label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">历史保留（天）</label>
        <input
          type="number"
          min="1"
          max="180"
          :value="vpsMonitorConfig.reportRetentionDays"
          @input="updateField('reportRetentionDays', Number($event.target.value))"
          class="block w-full px-3 py-2 bg-white/70 dark:bg-gray-900/50 border border-gray-200/80 dark:border-white/10 misub-radius-lg shadow-sm focus:ring-2 focus:ring-indigo-500/40 focus:border-indigo-500 sm:text-sm dark:text-white transition-colors"
        />
      </div>
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">离线通知</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">节点离线时提醒</div>
        </div>
        <Switch v-model="vpsMonitorConfig.notifyOffline" />
      </div>
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">恢复通知</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">节点恢复在线提醒</div>
        </div>
        <Switch v-model="vpsMonitorConfig.notifyRecovery" />
      </div>
      <div class="flex items-center justify-between p-3 bg-white/70 dark:bg-gray-900/50 border border-gray-200/60 dark:border-white/10 misub-radius-lg">
        <div>
          <div class="text-sm font-medium text-gray-900 dark:text-gray-200">负载通知</div>
          <div class="text-xs text-gray-500 dark:text-gray-400">CPU/内存/磁盘超阈值</div>
        </div>
        <Switch v-model="vpsMonitorConfig.notifyOverload" />
      </div>
    </div>
  </div>
</template>
