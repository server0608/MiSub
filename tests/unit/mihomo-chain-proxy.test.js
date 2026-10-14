import { describe, it, expect } from 'vitest';
import yaml from 'js-yaml';
import { renderClashFromIniTemplate } from '../../functions/modules/subscription/template-pipeline.js';

const PROXIES = [
    { name: 'HK-01', type: 'trojan', server: '1.1.1.1', port: 443, password: 'pass' },
    { name: 'JP-01', type: 'trojan', server: '2.2.2.2', port: 443, password: 'pass' },
];

function render(template, options = {}) {
    return yaml.load(
        renderClashFromIniTemplate(template, {
            proxies: PROXIES.map((proxy) => ({ ...proxy })),
            ruleLevel: 'none',
            ...options,
        })
    );
}

function group(name, type, members) {
    return `custom_proxy_group=${name}\`${type}\`${members}`;
}

const CHAIN_TEMPLATE = [
    '[custom]',
    group('🔗 链式落地', 'select', '[]🔗 链式前置'),
    group('🔗 链式前置', 'select', '.*'),
    `custom_chain_proxy=🔗 链式落地\`🔗 链式前置`,
    '',
    '[Rule]',
    'MATCH,🔗 链式落地',
].join('\n');

describe('Mihomo chain proxy', () => {
    it('does not emit dialer-proxy for non-Meta clients', () => {
        const parsed = render(CHAIN_TEMPLATE, { isMeta: false });

        expect(parsed.proxies.map((proxy) => proxy.name)).toEqual(['HK-01', 'JP-01']);
        expect(parsed.proxies.every((proxy) => proxy['dialer-proxy'] === undefined)).toBe(true);

        const landing = parsed['proxy-groups'].find((item) => item.name === '🔗 链式落地');
        expect(landing.proxies).toEqual(['🔗 链式前置']);
    });

    it('expands landing copies with dialer-proxy for Meta clients', () => {
        const parsed = render(CHAIN_TEMPLATE, { isMeta: true });

        const copies = parsed.proxies.filter((proxy) => proxy['dialer-proxy']);
        expect(copies.map((proxy) => proxy.name)).toEqual([
            '🔗 链式落地 - HK-01',
            '🔗 链式落地 - JP-01',
        ]);
        expect(copies.every((proxy) => proxy['dialer-proxy'] === '🔗 链式前置')).toBe(true);
        expect(parsed.proxies.find((proxy) => proxy.name === 'HK-01')['dialer-proxy']).toBe(
            undefined
        );

        const landing = parsed['proxy-groups'].find((item) => item.name === '🔗 链式落地');
        expect(landing.type).toBe('select');
        expect(landing.proxies).toEqual(['🔗 链式落地 - HK-01', '🔗 链式落地 - JP-01']);
    });

    it('keeps the entry group free of landing copies to avoid dialer-proxy loops', () => {
        const parsed = render(CHAIN_TEMPLATE, { isMeta: true });

        const entry = parsed['proxy-groups'].find((item) => item.name === '🔗 链式前置');
        expect(entry.proxies).toEqual(['HK-01', 'JP-01']);
        expect(entry.proxies.some((name) => String(name).startsWith('🔗 链式落地'))).toBe(false);
    });

    it('leaves templates without custom_chain_proxy untouched', () => {
        const parsed = render(
            [
                '[custom]',
                group('节点选择', 'select', '.*'),
                group('入口节点', 'select', '.*'),
                '',
                '[Rule]',
                'MATCH,节点选择',
            ].join('\n'),
            { isMeta: true }
        );

        expect(parsed.proxies.every((proxy) => proxy['dialer-proxy'] === undefined)).toBe(true);
    });

    it('creates missing landing and entry groups automatically', () => {
        const parsed = render(
            ['[custom]', `custom_chain_proxy=🔗 落地\`🔗 入口`, '', '[Rule]', 'MATCH,🔗 落地'].join(
                '\n'
            ),
            { isMeta: true }
        );

        const landing = parsed['proxy-groups'].find((item) => item.name === '🔗 落地');
        expect(landing.proxies).toEqual(['🔗 落地 - HK-01', '🔗 落地 - JP-01']);

        const entry = parsed['proxy-groups'].find((item) => item.name === '🔗 入口');
        expect(entry.proxies).toEqual(['HK-01', 'JP-01']);
    });

    it('supports a custom landing copy prefix', () => {
        const parsed = render(
            [
                '[custom]',
                `custom_chain_proxy=🔗 落地\`🔗 入口\`CHAIN: `,
                '',
                '[Rule]',
                'MATCH,🔗 落地',
            ].join('\n'),
            { isMeta: true }
        );

        const copies = parsed.proxies.filter((proxy) => proxy['dialer-proxy'] === '🔗 入口');
        expect(copies.map((proxy) => proxy.name)).toEqual(['CHAIN: HK-01', 'CHAIN: JP-01']);
    });
});
