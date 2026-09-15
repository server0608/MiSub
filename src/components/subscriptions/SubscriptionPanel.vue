<script setup>
    import { computed, ref } from 'vue';
    import draggable from 'vuedraggable';
    import Card from '../ui/Card.vue';
    import MoreActionsMenu from '@/components/shared/MoreActionsMenu.vue';
    import PanelPagination from '@/components/shared/PanelPagination.vue';
    import EmptyState from '@/components/ui/EmptyState.vue';
    import { useUIStore } from '@/stores/ui';
    import { useI18n } from '@/i18n/index.js';

    const { layoutMode } = useUIStore();
    const { t } = useI18n();

    const props = defineProps({
        subscriptions: { type: Array, default: () => [] },
        paginatedSubscriptions: Array,
        currentPage: Number,
        totalPages: Number,
        isSorting: Boolean,
        searchable: { type: Boolean, default: false },
        searchQuery: { type: String, default: '' },
        filteredCount: { type: Number, default: undefined },
    });

    const emit = defineEmits([
        'add',
        'delete',
        'changePage',
        'updateNodeCount',
        'edit',
        'toggleSort',
        'markDirty',
        'preview',
        'deleteAll',
        'refreshAll',
        'reorder',
        'import',
        'qrcode',
        'updateSearch',
    ]);

    const searchModel = computed({
        get: () => props.searchQuery,
        set: (value) => emit('updateSearch', value),
    });

    const visibleCount = computed(() => props.filteredCount ?? props.subscriptions.length);

    const draggableSubscriptions = computed({
        get: () => [...props.subscriptions],
        set: (val) => emit('reorder', val),
    });

    const handleDelete = (id) => emit('delete', id);
    const handleEdit = (id) => emit('edit', id);
    const handleUpdate = (id) => emit('updateNodeCount', id);
    const handlePreview = (id) => emit('preview', id);
    const handleQRCode = (id) => emit('qrcode', id);
    const handleAdd = () => emit('add');
    const handleChangePage = (page) => emit('changePage', page);
    const handleToggleSort = () => emit('toggleSort');
    const handleSortEnd = () => emit('markDirty');
    const handleDeleteAll = () => emit('deleteAll');
    const handleRefreshAll = () => emit('refreshAll');
    const handleImport = () => emit('import');

    // === 按站点自动折叠 ===
    // 同一家机场常有多个订阅链接（域名相同、路径 token 不同），逐个平铺会很乱。
    // 这里按 URL 的域名自动聚合，同一站点的订阅源折叠为一组，可展开查看。
    const collapsedGroups = ref(new Set());

    /** 从订阅 URL 提取站点标识（域名）；非 http 链接归入「其他」。 */
    const siteKeyOf = (sub) => {
        try {
            const host = new URL(sub.url).hostname;
            return host ? host.replace(/^www\./, '') : '';
        } catch (e) {
            return '';
        }
    };

    /**
     * 将被分页截断的列表还原为完整列表，再按站点聚合。
     * 注意：分组会绕过分页（组内项目一次性展示），因此这里使用完整列表。
     */
    const groupedSubscriptions = computed(() => {
        const list = props.subscriptions || [];
        const order = [];
        const map = new Map();

        list.forEach((sub) => {
            const key = siteKeyOf(sub) || '';
            if (!map.has(key)) {
                map.set(key, []);
                order.push(key);
            }
            map.get(key).push(sub);
        });

        return order.map((key) => ({
            key: key || '__other__',
            host: key,
            items: map.get(key),
        }));
    });

    /** 只有多条目站点才值得折叠；单条目站点直接平铺，避免多余的展开操作。 */
    const collapsibleGroups = computed(() =>
        groupedSubscriptions.value.filter((g) => g.items.length > 1)
    );

    /** 单条目站点（不折叠，直接平铺展示）。 */
    const ungroupedSubscriptions = computed(() =>
        groupedSubscriptions.value.filter((g) => g.items.length === 1).flatMap((g) => g.items)
    );

    const toggleGroup = (key) => {
        const next = new Set(collapsedGroups.value);
        if (next.has(key)) next.delete(key);
        else next.add(key);
        collapsedGroups.value = next;
    };

    const isGroupCollapsed = (key) => collapsedGroups.value.has(key);

    const isGrouped = computed(() => collapsibleGroups.value.length > 0);

    const collapseAllGroups = () => {
        collapsedGroups.value = new Set(collapsibleGroups.value.map((g) => g.key));
    };

    const expandAllGroups = () => {
        collapsedGroups.value = new Set();
    };
</script>

<template>
    <div>
        <div
            class="mb-4 rounded-xl border border-gray-100/80 bg-white/85 p-4 shadow-sm dark:border-white/10 dark:bg-gray-900/70"
        >
            <div class="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                <div class="min-w-0">
                    <div class="flex items-center gap-3 shrink-0">
                        <h2 class="text-xl font-bold text-gray-900 dark:text-white">
                            {{ t('subscriptions.title') }}
                        </h2>
                        <span
                            class="rounded-full bg-gray-100 px-2.5 py-0.5 text-sm font-semibold text-gray-700 dark:bg-white/10 dark:text-gray-200"
                            >{{ subscriptions.length }}</span
                        >
                    </div>
                    <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
                        {{ t('subscriptions.subtitle') }}
                    </p>
                </div>
                <div
                    class="flex flex-wrap items-center gap-2 sm:w-auto justify-end sm:justify-start"
                >
                    <slot name="actions-prepend"></slot>
                    <button
                        @click="handleImport"
                        class="shrink-0 rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-white/10 dark:bg-white/5 dark:text-gray-200 dark:hover:bg-white/10"
                    >
                        {{ t('actions.bulkImport') }}
                    </button>
                    <button
                        v-if="isGrouped && !isSorting"
                        @click="collapseAllGroups"
                        class="shrink-0 rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-white/10 dark:bg-white/5 dark:text-gray-200 dark:hover:bg-white/10"
                    >
                        {{ t('subscriptions.collapseAll') }}
                    </button>
                    <button
                        v-if="isGrouped && !isSorting"
                        @click="expandAllGroups"
                        class="shrink-0 rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 dark:border-white/10 dark:bg-white/5 dark:text-gray-200 dark:hover:bg-white/10"
                    >
                        {{ t('subscriptions.expandAll') }}
                    </button>
                    <button
                        @click="handleAdd"
                        class="shrink-0 rounded-lg bg-primary-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-primary-700"
                    >
                        {{ t('actions.add') }}
                    </button>
                    <MoreActionsMenu menu-width-class="w-36">
                        <template #menu="{ close }">
                            <button
                                @click="
                                    handleRefreshAll();
                                    close();
                                "
                                class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
                            >
                                {{ t('actions.refreshAll') }}
                            </button>
                            <button
                                @click="
                                    handleToggleSort();
                                    close();
                                "
                                class="w-full text-left px-4 py-2 text-sm text-gray-700 dark:text-gray-200 hover:bg-gray-100 dark:hover:bg-gray-700"
                            >
                                {{ isSorting ? t('actions.finishSort') : t('actions.manualSort') }}
                            </button>
                            <div class="border-t border-gray-200 dark:border-gray-700 my-1"></div>
                            <button
                                @click="
                                    handleDeleteAll();
                                    close();
                                "
                                class="w-full text-left px-4 py-2 text-sm text-red-600 hover:bg-red-500/10"
                            >
                                {{ t('actions.clearAll') }}
                            </button>
                        </template>
                    </MoreActionsMenu>
                </div>
            </div>
            <div v-if="searchable" class="relative mt-4">
                <svg
                    class="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                    aria-hidden="true"
                >
                    <circle cx="11" cy="11" r="7" />
                    <path d="m20 20-3.5-3.5" />
                </svg>
                <input
                    v-model="searchModel"
                    data-testid="subscription-search"
                    type="search"
                    :placeholder="t('subscriptions.listSearchPlaceholder')"
                    :aria-label="t('subscriptions.searchPlaceholder')"
                    :disabled="isSorting"
                    class="w-full rounded-lg border border-gray-200 bg-white py-2.5 pl-10 pr-20 text-sm text-gray-900 outline-none transition focus:border-primary-500 focus:ring-2 focus:ring-primary-500/20 disabled:cursor-not-allowed disabled:opacity-60 dark:border-white/10 dark:bg-white/5 dark:text-white dark:placeholder:text-gray-500"
                />
                <span
                    v-if="searchQuery"
                    class="pointer-events-none absolute right-3 top-1/2 -translate-y-1/2 text-xs text-gray-400"
                >
                    {{ visibleCount }}/{{ subscriptions.length }}
                </span>
            </div>
        </div>
        <div v-if="subscriptions.length > 0">
            <draggable
                v-if="isSorting"
                tag="div"
                class="grid grid-cols-1 md:grid-cols-2 gap-4"
                v-model="draggableSubscriptions"
                item-key="id"
                animation="300"
                @end="handleSortEnd"
            >
                <template #item="{ element: subscription }">
                    <div class="cursor-move">
                        <Card
                            :misub="subscription"
                            @delete="handleDelete(subscription.id)"
                            @change="handleSortEnd"
                            @update="handleUpdate(subscription.id)"
                            @edit="handleEdit(subscription.id)"
                            @preview="handlePreview(subscription.id)"
                            @qrcode="handleQRCode(subscription.id)"
                        />
                    </div>
                </template>
            </draggable>
            <div v-else-if="paginatedSubscriptions.length > 0" class="space-y-4">
                <!-- 按站点分组：同站点的多个订阅源折叠为一组 -->
                <template v-if="isGrouped">
                    <template v-for="group in collapsibleGroups" :key="group.key">
                        <div
                            class="rounded-xl border border-gray-100/80 bg-white/70 shadow-sm dark:border-white/10 dark:bg-gray-900/50"
                        >
                            <button
                                type="button"
                                class="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
                                @click="toggleGroup(group.key)"
                            >
                                <div class="flex min-w-0 items-center gap-2">
                                    <svg
                                        class="h-4 w-4 shrink-0 text-gray-400 transition-transform"
                                        :class="isGroupCollapsed(group.key) ? '-rotate-90' : ''"
                                        viewBox="0 0 20 20"
                                        fill="currentColor"
                                        aria-hidden="true"
                                    >
                                        <path
                                            fill-rule="evenodd"
                                            d="M5.23 7.21a.75.75 0 011.06.02L10 11.17l3.71-3.94a.75.75 0 111.08 1.04l-4.25 4.5a.75.75 0 01-1.08 0l-4.25-4.5a.75.75 0 01.02-1.06z"
                                            clip-rule="evenodd"
                                        />
                                    </svg>
                                    <span
                                        class="truncate font-semibold text-gray-800 dark:text-gray-100"
                                    >
                                        {{ group.host || t('subscriptions.otherSources') }}
                                    </span>
                                    <span
                                        class="shrink-0 rounded-full bg-gray-100 px-2 py-0.5 text-xs font-semibold text-gray-600 dark:bg-white/10 dark:text-gray-300"
                                    >
                                        {{ group.items.length }}
                                    </span>
                                </div>
                                <span class="shrink-0 text-xs text-gray-400">
                                    {{
                                        isGroupCollapsed(group.key)
                                            ? t('subscriptions.expand')
                                            : t('subscriptions.collapse')
                                    }}
                                </span>
                            </button>
                            <div
                                v-show="!isGroupCollapsed(group.key)"
                                class="grid grid-cols-1 gap-4 border-t border-gray-100/80 p-4 md:grid-cols-2 dark:border-white/10"
                            >
                                <div
                                    v-for="(subscription, index) in group.items"
                                    :key="subscription.id"
                                    class="list-item-animation"
                                    :style="{ '--delay-index': index }"
                                >
                                    <Card
                                        :misub="subscription"
                                        @delete="handleDelete(subscription.id)"
                                        @change="handleSortEnd"
                                        @update="handleUpdate(subscription.id)"
                                        @edit="handleEdit(subscription.id)"
                                        @preview="handlePreview(subscription.id)"
                                        @qrcode="handleQRCode(subscription.id)"
                                    />
                                </div>
                            </div>
                        </div>
                    </template>

                    <!-- 单条目站点平铺 -->
                    <div
                        v-if="ungroupedSubscriptions.length > 0"
                        class="grid grid-cols-1 gap-4 md:grid-cols-2"
                    >
                        <div
                            v-for="(subscription, index) in ungroupedSubscriptions"
                            :key="subscription.id"
                            class="list-item-animation"
                            :style="{ '--delay-index': index }"
                        >
                            <Card
                                :misub="subscription"
                                @delete="handleDelete(subscription.id)"
                                @change="handleSortEnd"
                                @update="handleUpdate(subscription.id)"
                                @edit="handleEdit(subscription.id)"
                                @preview="handlePreview(subscription.id)"
                                @qrcode="handleQRCode(subscription.id)"
                            />
                        </div>
                    </div>
                </template>

                <div v-else class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div
                        v-for="(subscription, index) in paginatedSubscriptions"
                        :key="subscription.id"
                        class="list-item-animation"
                        :style="{ '--delay-index': index }"
                    >
                        <Card
                            :misub="subscription"
                            @delete="handleDelete(subscription.id)"
                            @change="handleSortEnd"
                            @update="handleUpdate(subscription.id)"
                            @edit="handleEdit(subscription.id)"
                            @preview="handlePreview(subscription.id)"
                            @qrcode="handleQRCode(subscription.id)"
                        />
                    </div>
                </div>
            </div>
            <div
                v-else
                class="rounded-xl border border-dashed border-gray-300 bg-white/60 px-6 py-12 text-center dark:border-gray-700 dark:bg-gray-900/50"
            >
                <p class="text-sm font-medium text-gray-700 dark:text-gray-200">
                    {{ t('subscriptions.noSearchResults') }}
                </p>
                <button
                    type="button"
                    class="mt-3 text-sm font-medium text-primary-600 hover:text-primary-700 dark:text-primary-400"
                    @click="searchModel = ''"
                >
                    {{ t('actions.clearSearch') }}
                </button>
            </div>
            <PanelPagination
                v-if="totalPages > 1 && !isSorting"
                variant="panel"
                :current-page="currentPage"
                :total-pages="totalPages"
                :total-items="visibleCount"
                :show-total-items="true"
                @change-page="handleChangePage"
            />
        </div>
        <div
            v-else
            class="rounded-xl border border-dashed border-gray-300 bg-white/60 py-6 dark:border-gray-700 dark:bg-gray-900/50"
        >
            <EmptyState
                :title="t('subscriptions.empty')"
                :description="t('subscriptions.emptyDesc')"
                icon="folder"
                :total-count="0"
            />
            <div class="-mt-8 mb-8 flex flex-col items-center gap-3 sm:flex-row sm:justify-center">
                <button
                    data-testid="empty-add-subscription"
                    @click="handleAdd"
                    class="inline-flex items-center justify-center rounded-lg bg-primary-600 px-4 py-2.5 text-sm font-medium text-white transition-colors hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-primary-500/30"
                >
                    {{ t('subscriptions.addEmpty') }}
                </button>
                <button
                    data-testid="empty-import-subscriptions"
                    @click="handleImport"
                    class="inline-flex items-center justify-center rounded-lg border border-gray-200 bg-white px-4 py-2.5 text-sm font-medium text-gray-700 transition-colors hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-primary-500/30 dark:border-white/10 dark:bg-white/5 dark:text-gray-300 dark:hover:bg-white/10"
                >
                    {{ t('actions.bulkImport') }}
                </button>
            </div>
        </div>
    </div>
</template>

<style scoped>
    .cursor-move {
        cursor: move;
    }
</style>
