<script setup>
import Modal from '../forms/Modal.vue';
import { computed, ref, watch } from 'vue';
import VpsMonitorCard from '../settings/sections/ServiceSettings/VpsMonitorCard.vue';
import VpsNetworkTargets from '../vps/VpsNetworkTargets.vue';
import { fetchVpsGlobalNetworkTargets, createVpsGlobalNetworkTarget } from '../../lib/api.js';

const props = defineProps({
  show: {
    type: Boolean,
    default: false
  },
  settings: {
    type: Object,
    required: true
  },
  saving: {
    type: Boolean,
    default: false
  }
});

const emit = defineEmits(['update:show', 'save']);

const globalTargets = ref([]);
const isLoadingTargets = ref(false);

const canEditTargets = computed(() => props.settings?.vpsMonitor?.enabled !== false);

const loadGlobalTargets = async () => {
  if (!props.show) return;
  isLoadingTargets.value = true;
  const result = await fetchVpsGlobalNetworkTargets();
  if (result.success) {
    globalTargets.value = result.data?.data || [];
  }
  isLoadingTargets.value = false;
};

const handleGlobalTargetSave = async (payload) => {
  const result = await createVpsGlobalNetworkTarget(payload);
  if (result.success) {
    globalTargets.value = [result.data?.data, ...globalTargets.value].filter(Boolean);
  }
};

watch(() => props.show, (val) => {
  if (val) loadGlobalTargets();
});

const handleClose = () => {
  emit('update:show', false);
};

const handleSave = () => {
  emit('save');
};
</script>

<template>
  <Modal :show="show" @update:show="handleClose" @confirm="handleSave" confirm-text="保存" cancel-text="关闭" size="4xl" :confirm-disabled="saving">
    <template #title>
      <div class="flex items-center gap-2">
        <span class="text-lg font-bold text-gray-900 dark:text-white">VPS 探针设置</span>
      </div>
    </template>
    <template #body>
      <div class="space-y-6">
        <VpsMonitorCard :settings="settings" />

        <div class="bg-white/80 dark:bg-gray-900/60 border border-gray-100/80 dark:border-white/10 misub-radius-lg p-4">
          <div class="flex items-center justify-between">
            <div>
              <h4 class="text-sm font-semibold text-gray-900 dark:text-white">全局网络监测目标</h4>
              <p class="text-xs text-gray-500 dark:text-gray-400">适用于勾选“应用全局监测目标”的节点</p>
            </div>
            <span class="text-xs text-gray-400" v-if="isLoadingTargets">加载中...</span>
          </div>
          <div class="mt-4">
            <VpsNetworkTargets
              v-if="canEditTargets"
              node-id="global"
              :targets="globalTargets"
              :limit="settings?.vpsMonitor?.networkTargetsLimit || 3"
              @refresh="loadGlobalTargets"
            />
          </div>
        </div>
      </div>
    </template>
  </Modal>
</template>
