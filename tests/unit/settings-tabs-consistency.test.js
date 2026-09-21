import { describe, expect, it } from 'vitest';

const readSource = async (path) =>
    await import('node:fs/promises').then(({ readFile }) => readFile(path, 'utf8'));

/** 提取源码中所有 `activeTab === 'xxx'` 的取值（即容器实际渲染的 tab） */
const extractRenderedTabs = (source) =>
    [...source.matchAll(/activeTab\s*===\s*'([a-z-]+)'/g)].map((match) => match[1]);

/** 提取 currentTabLabel 的 switch 中所有 case 值 */
const extractLabelCases = (source) => {
    const block = source.match(/const currentTabLabel = computed\(\(\) => \{[\s\S]*?\n    \}\);/);
    if (!block) return [];
    return [...block[0].matchAll(/case\s+'([a-z-]+)'/g)].map((match) => match[1]);
};

/**
 * 设置界面存在两个容器：SettingsPanel.vue（弹窗）与 SettingsView.vue（独立页面），
 * 二者共用同一个 SettingsSidebar.vue。历史上出现过「侧边栏有 tab、容器却没渲染」
 * 的情况（custom-page 点开是空白；handleReset 未传入导致重置按钮无效）。
 * 这里用静态断言把三者锁在一起，避免同类问题再次发生。
 */
describe('设置页信息架构一致性', () => {
    const containers = [
        { name: 'SettingsPanel.vue（设置弹窗）', path: 'src/components/modals/SettingsPanel.vue' },
        { name: 'SettingsView.vue（设置页）', path: 'src/views/SettingsView.vue' },
    ];

    it('侧边栏声明的每个 tab 都能在两个容器中渲染出来', async () => {
        const sidebar = await readSource('src/components/settings/SettingsSidebar.vue');
        const sidebarTabs = [
            ...sidebar.matchAll(/\{\s*id:\s*'([a-z-]+)',\s*labelKey:\s*'settings\.tabs\./g),
        ].map((match) => match[1]);

        expect(sidebarTabs.length).toBeGreaterThan(0);

        for (const { name, path } of containers) {
            const rendered = new Set(extractRenderedTabs(await readSource(path)));
            const missing = sidebarTabs.filter((id) => !rendered.has(id));
            expect(missing, `${name} 未渲染这些 tab：${missing.join(', ')}`).toEqual([]);
        }
    });

    it('两个容器的 currentTabLabel 覆盖全部 tab，避免标题回退成「设置」', async () => {
        const sidebar = await readSource('src/components/settings/SettingsSidebar.vue');
        const sidebarTabs = [
            ...sidebar.matchAll(/\{\s*id:\s*'([a-z-]+)',\s*labelKey:\s*'settings\.tabs\./g),
        ].map((match) => match[1]);

        for (const { name, path } of containers) {
            const cases = new Set(extractLabelCases(await readSource(path)));
            const missing = sidebarTabs.filter((id) => !cases.has(id));
            expect(missing, `${name} 的 currentTabLabel 缺少分支：${missing.join(', ')}`).toEqual(
                []
            );
        }
    });

    it('两个容器都向 SystemSettings 传入 handleReset，避免重置按钮点了没反应', async () => {
        for (const { name, path } of containers) {
            const source = await readSource(path);
            expect(source, `${name} 未传入 handleReset`).toMatch(/:handleReset="handleReset"/);
        }
    });
});
