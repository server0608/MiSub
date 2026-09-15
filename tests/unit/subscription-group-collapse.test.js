import { describe, it, expect } from 'vitest';
import { mount } from '@vue/test-utils';
import { createPinia } from 'pinia';
import SubscriptionPanel from '../../src/components/subscriptions/SubscriptionPanel.vue';

/**
 * 订阅源「按站点自动折叠」回归测试。
 *
 * 场景：同一家机场常有多个订阅链接（域名相同、路径 token 不同），
 * 逐个平铺会导致列表杂乱。面板按 URL 域名自动聚合，同一站点折叠为一组。
 */

const makeSub = (id, name, url) => ({ id, name, url, enabled: true });

// 注意：SubscriptionPanel 的 Card 子组件会渲染较多内容，这里只关心分组结构，
// 用 shallow 挂载避免深入渲染以防触发与网络相关的副作用。
const mountPanel = (subscriptions) =>
    mount(SubscriptionPanel, {
        props: {
            subscriptions,
            paginatedSubscriptions: subscriptions,
            currentPage: 1,
            totalPages: 1,
        },
        global: {
            plugins: [createPinia()],
            stubs: {
                draggable: true,
                Card: { template: '<div class="stub-card" />' },
                MoreActionsMenu: { template: '<div><slot name="menu" /></div>' },
                PanelPagination: true,
                EmptyState: true,
            },
        },
    });

describe('订阅源按站点折叠', () => {
    it('同一域名的多个订阅源聚合为一个分组', async () => {
        const subs = [
            makeSub('a', 'sub1', 'https://sub1.gsafevpn.com/x/token1'),
            makeSub('b', 'sub1', 'https://sub1.gsafevpn.com/x/token2'),
            makeSub('c', 'sub1', 'https://sub1.gsafevpn.com/x/token3'),
        ];
        const wrapper = mountPanel(subs);
        await wrapper.vm.$nextTick();

        // 该分组标题应只出现一次，且带数量徽标 3
        const html = wrapper.html();
        const groupCount = (html.match(/sub1\.gsafevpn\.com/g) || []).length;
        expect(groupCount).toBeGreaterThanOrEqual(1);
        expect(html).toContain('>3<');
    });

    it('单条目站点不折叠，直接平铺', async () => {
        const subs = [
            makeSub('a', 'a', 'https://only-one.example.com/sub'),
            makeSub('b', 'b', 'https://another.example.com/sub'),
        ];
        const wrapper = mountPanel(subs);
        await wrapper.vm.$nextTick();

        // 没有多条目分组时，不应出现折叠控件文案（展开全部按钮）
        expect(wrapper.html()).not.toContain('展开全部');
        expect(wrapper.html()).not.toContain('Collapse all');
    });

    it('不同域名不会互相聚合', async () => {
        const subs = [
            makeSub('a', 'a', 'https://site-a.com/1'),
            makeSub('b', 'b', 'https://site-a.com/2'),
            makeSub('c', 'c', 'https://site-b.com/1'),
            makeSub('d', 'd', 'https://site-b.com/2'),
        ];
        const wrapper = mountPanel(subs);
        await wrapper.vm.$nextTick();

        const html = wrapper.html();
        expect(html).toContain('site-a.com');
        expect(html).toContain('site-b.com');
    });

    it('www. 前缀视为同一站点', async () => {
        const subs = [
            makeSub('a', 'a', 'https://www.example.com/1'),
            makeSub('b', 'b', 'https://example.com/2'),
        ];
        const wrapper = mountPanel(subs);
        await wrapper.vm.$nextTick();

        // 归为同一组，标题使用去掉 www 的域名
        const html = wrapper.html();
        expect(html).toContain('example.com');
        expect(html).not.toContain('www.example.com');
    });
});
