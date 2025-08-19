// FILE: src/composables/useSubscriptions.js
import { ref, computed, watch, onMounted, onUnmounted } from 'vue';
import { fetchNodeCount, batchUpdateNodes, fetchSettings } from '../lib/api.js';
import { useToastStore } from '../stores/toast.js';

export function useSubscriptions(initialSubsRef, markDirty) {
  const { showToast } = useToastStore();
  const subscriptions = ref([]);
  const subsCurrentPage = ref(1);
  const subsItemsPerPage = 6;
  const updateInterval = ref(0); // 默认不自动更新
  let updateTimer = null;

  // 获取设置并初始化更新间隔
  async function initializeUpdateInterval() {
    try {
      const settings = await fetchSettings();
      updateInterval.value = settings.updateInterval || 0;
      setupAutoUpdate();
    } catch (error) {
      console.error('Failed to fetch settings:', error);
    }
  }

  // 设置自动更新
  function setupAutoUpdate() {
    // 清除现有定时器
    if (updateTimer) {
      clearInterval(updateTimer);
      updateTimer = null;
    }

    // 如果设置了更新间隔且大于0，则启动定时器
    if (updateInterval.value > 0) {
      updateTimer = setInterval(async () => {
        // 只更新启用的订阅
        const enabledSubs = subscriptions.value.filter(s => s.enabled && s.url.startsWith('http'));
        if (enabledSubs.length > 0) {
          try {
            const result = await batchUpdateNodes(enabledSubs.map(sub => sub.id));
            
            if (result.success) {
              // 更新本地数据
              result.results.forEach(updateResult => {
                if (updateResult.success) {
                  const sub = subscriptions.value.find(s => s.id === updateResult.id);
                  if (sub) {
                    sub.nodeCount = updateResult.nodeCount;
                    // userInfo会在下次数据同步时更新
                  }
                }
              });
              
              const successCount = result.results.filter(r => r.success).length;
              showToast(`自动更新完成！成功更新 ${successCount}/${enabledSubs.length} 个订阅`, 'success');
              markDirty();
            } else {
              console.error('Auto update failed:', result.message);
            }
          } catch (error) {
            console.error('Auto update error:', error);
          }
        }
      }, updateInterval.value * 60 * 1000); // 将分钟转换为毫秒
    }
  }

  // 为每个订阅设置单独的更新定时器
  function setupIndividualUpdateTimers() {
    // 清除所有单独的定时器
    clearIndividualUpdateTimers();

    // 为每个启用的订阅设置单独的定时器
    subscriptions.value.forEach(sub => {
      if (sub.enabled && sub.url.startsWith('http') && sub.updateInterval && sub.updateInterval > 0) {
        const timerId = setInterval(async () => {
          try {
            const result = await batchUpdateNodes([sub.id]);
            
            if (result.success) {
              // 更新本地数据
              result.results.forEach(updateResult => {
                if (updateResult.success) {
                  const subToUpdate = subscriptions.value.find(s => s.id === updateResult.id);
                  if (subToUpdate) {
                    subToUpdate.nodeCount = updateResult.nodeCount;
                    // userInfo会在下次数据同步时更新
                  }
                }
              });
              
              const successCount = result.results.filter(r => r.success).length;
              showToast(`订阅 ${sub.name || ''} 自动更新完成！`, 'success');
              markDirty();
            } else {
              console.error(`Individual auto update failed for ${sub.name}:`, result.message);
            }
          } catch (error) {
            console.error(`Individual auto update error for ${sub.name}:`, error);
          }
        }, sub.updateInterval * 60 * 1000); // 将分钟转换为毫秒

        // 存储定时器ID，以便后续清理
        if (!sub.updateTimers) {
          sub.updateTimers = [];
        }
        sub.updateTimers.push(timerId);
      }
    });
  }

  // 清除所有单独的定时器
  function clearIndividualUpdateTimers() {
    subscriptions.value.forEach(sub => {
      if (sub.updateTimers && sub.updateTimers.length > 0) {
        sub.updateTimers.forEach(timerId => clearInterval(timerId));
        sub.updateTimers = [];
      }
    });
  }

  function initializeSubscriptions(subsData) {
    subscriptions.value = (subsData || []).map(sub => ({
      ...sub,
      id: sub.id || crypto.randomUUID(),
      enabled: sub.enabled ?? true,
      nodeCount: sub.nodeCount || 0,
      isUpdating: false,
      userInfo: sub.userInfo || null,
      exclude: sub.exclude || '', // 新增 exclude 属性
      updateInterval: sub.updateInterval || null, // 新增订阅单独更新时间属性
    }));
    // [最終修正] 移除此處的自動更新迴圈，以防止本地開發伺服器因併發請求過多而崩潰。
    // subscriptions.value.forEach(sub => handleUpdateNodeCount(sub.id, true));
  }

  const enabledSubscriptions = computed(() => subscriptions.value.filter(s => s.enabled));
  
  const totalRemainingTraffic = computed(() => {
    return subscriptions.value.reduce((acc, sub) => {
      if (sub.enabled && sub.userInfo && sub.userInfo.total > 0) {
        const used = (sub.userInfo.upload || 0) + (sub.userInfo.download || 0);
        const remaining = sub.userInfo.total - used;
        return acc + Math.max(0, remaining);
      }
      return acc;
    }, 0);
  });

  const subsTotalPages = computed(() => Math.ceil(subscriptions.value.length / subsItemsPerPage));
  const paginatedSubscriptions = computed(() => {
    const start = (subsCurrentPage.value - 1) * subsItemsPerPage;
    const end = start + subsItemsPerPage;
    return subscriptions.value.slice(start, end);
  });

  function changeSubsPage(page) {
    if (page < 1 || page > subsTotalPages.value) return;
    subsCurrentPage.value = page;
  }

  async function handleUpdateNodeCount(subId, isInitialLoad = false) {
    const subToUpdate = subscriptions.value.find(s => s.id === subId);
    if (!subToUpdate || !subToUpdate.url.startsWith('http')) return;
    
    if (!isInitialLoad) {
        subToUpdate.isUpdating = true;
    }

    try {
      const data = await fetchNodeCount(subToUpdate.url);
      subToUpdate.nodeCount = data.count || 0;
      subToUpdate.userInfo = data.userInfo || null;
      
      if (!isInitialLoad) {
        showToast(`${subToUpdate.name || '订阅'} 更新成功！`, 'success');
        markDirty();
      }
    } catch (error) {
      if (!isInitialLoad) showToast(`${subToUpdate.name || '订阅'} 更新失败`, 'error');
      console.error(`Failed to fetch node count for ${subToUpdate.name}:`, error);
    } finally {
      subToUpdate.isUpdating = false;
    }
  }

  function addSubscription(sub) {
    subscriptions.value.unshift(sub);
    subsCurrentPage.value = 1;
    handleUpdateNodeCount(sub.id); // 新增時自動更新單個
    markDirty();
  }

  function updateSubscription(updatedSub) {
    const index = subscriptions.value.findIndex(s => s.id === updatedSub.id);
    if (index !== -1) {
      if (subscriptions.value[index].url !== updatedSub.url) {
        updatedSub.nodeCount = 0;
        handleUpdateNodeCount(updatedSub.id); // URL 變更時自動更新單個
      }
      subscriptions.value[index] = updatedSub;
      markDirty();
    }
  }

  function deleteSubscription(subId) {
    subscriptions.value = subscriptions.value.filter((s) => s.id !== subId);
    if (paginatedSubscriptions.value.length === 0 && subsCurrentPage.value > 1) {
      subsCurrentPage.value--;
    }
    markDirty();
  }

  function deleteAllSubscriptions() {
    subscriptions.value = [];
    subsCurrentPage.value = 1;
    markDirty();
  }
  
  // {{ AURA-X: Modify - 使用批量更新API优化批量导入. Approval: 寸止(ID:1735459200). }}
  // [优化] 批量導入使用批量更新API，减少KV写入次数
  async function addSubscriptionsFromBulk(subs) {
    subscriptions.value.unshift(...subs);
    markDirty();

    // 过滤出需要更新的订阅（只有http/https链接）
    const subsToUpdate = subs.filter(sub => sub.url && sub.url.startsWith('http'));

    if (subsToUpdate.length > 0) {
      showToast(`正在批量更新 ${subsToUpdate.length} 个订阅...`, 'success');

      try {
        const result = await batchUpdateNodes(subsToUpdate.map(sub => sub.id));

        if (result.success) {
          // 更新本地数据
          result.results.forEach(updateResult => {
            if (updateResult.success) {
              const sub = subscriptions.value.find(s => s.id === updateResult.id);
              if (sub) {
                sub.nodeCount = updateResult.nodeCount;
                // userInfo会在下次数据同步时更新
              }
            }
          });

          const successCount = result.results.filter(r => r.success).length;
          showToast(`批量更新完成！成功更新 ${successCount}/${subsToUpdate.length} 个订阅`, 'success');
          markDirty(); // 标记需要保存
        } else {
          showToast(`批量更新失败: ${result.message}`, 'error');
          // 降级到逐个更新
          showToast('正在降级到逐个更新模式...', 'info');
          for(const sub of subsToUpdate) {
            await handleUpdateNodeCount(sub.id);
          }
        }
      } catch (error) {
        console.error('Batch update failed:', error);
        showToast('批量更新失败，正在降级到逐个更新...', 'error');
        // 降级到逐个更新
        for(const sub of subsToUpdate) {
          await handleUpdateNodeCount(sub.id);
        }
      }
    } else {
      showToast('批量导入完成！', 'success');
    }
  }

  // 更新间隔设置变更处理
  async function handleUpdateIntervalChange(newInterval) {
    updateInterval.value = newInterval;
    setupAutoUpdate();
  }

  // 订阅单独更新间隔设置变更处理
  function handleSubscriptionUpdateIntervalChange(subId, newInterval) {
    const sub = subscriptions.value.find(s => s.id === subId);
    if (sub) {
      sub.updateInterval = newInterval;
      // 重新设置所有单独的定时器
      setupIndividualUpdateTimers();
    }
  }

  // 组件挂载时初始化
  onMounted(() => {
    initializeUpdateInterval();
    // 初始化单独的更新定时器
    setupIndividualUpdateTimers();
  });

  // 组件卸载时清理定时器
  onUnmounted(() => {
    if (updateTimer) {
      clearInterval(updateTimer);
      updateTimer = null;
    }
    // 清除所有单独的定时器
    clearIndividualUpdateTimers();
  });

  watch(initialSubsRef, (newInitialSubs) => {
    initializeSubscriptions(newInitialSubs);
  }, { immediate: true, deep: true });

  return {
    subscriptions,
    subsCurrentPage,
    subsTotalPages,
    paginatedSubscriptions,
    totalRemainingTraffic,
    enabledSubscriptionsCount: computed(() => enabledSubscriptions.value.length),
    changeSubsPage,
    addSubscription,
    updateSubscription,
    deleteSubscription,
    deleteAllSubscriptions,
    addSubscriptionsFromBulk,
    handleUpdateNodeCount,
    updateInterval,
    handleUpdateIntervalChange,
    handleSubscriptionUpdateIntervalChange,
  };
}
