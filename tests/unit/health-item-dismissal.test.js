import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import {
    DISMISSED_HEALTH_ITEMS_KEY,
    clearDismissedHealthItems,
    dismissHealthItem,
    filterDismissedHealthItems,
    isHealthItemDismissed,
    readDismissedHealthItemIds,
    restoreHealthItem,
} from '../../src/utils/health-item-dismissal.js';

describe('health-item-dismissal', () => {
    beforeEach(() => {
        localStorage.clear();
    });

    afterEach(() => {
        vi.restoreAllMocks();
        vi.unstubAllGlobals();
    });

    it('starts with nothing dismissed', () => {
        expect(readDismissedHealthItemIds()).toEqual([]);
        expect(isHealthItemDismissed('auto-token')).toBe(false);
    });

    it('remembers a dismissed item id', () => {
        dismissHealthItem('auto-token');

        expect(isHealthItemDismissed('auto-token')).toBe(true);
        expect(isHealthItemDismissed('expired-subscriptions')).toBe(false);
        expect(readDismissedHealthItemIds()).toEqual(['auto-token']);
    });

    it('ignores duplicate dismissals', () => {
        dismissHealthItem('auto-token');
        dismissHealthItem('auto-token');

        expect(readDismissedHealthItemIds()).toEqual(['auto-token']);
    });

    it('restores a single item', () => {
        dismissHealthItem('auto-token');
        dismissHealthItem('low-traffic');
        restoreHealthItem('auto-token');

        expect(readDismissedHealthItemIds()).toEqual(['low-traffic']);
    });

    it('clears every dismissal', () => {
        dismissHealthItem('auto-token');
        dismissHealthItem('low-traffic');
        clearDismissedHealthItems();

        expect(readDismissedHealthItemIds()).toEqual([]);
    });

    it('filters dismissed items out of a list', () => {
        dismissHealthItem('low-traffic');
        const items = [{ id: 'auto-token' }, { id: 'low-traffic' }, { id: 'zero-nodes' }];

        expect(filterDismissedHealthItems(items).map((i) => i.id)).toEqual([
            'auto-token',
            'zero-nodes',
        ]);
    });

    it('ignores blank ids so the store cannot be polluted', () => {
        dismissHealthItem('');
        dismissHealthItem('   ');
        dismissHealthItem(null);

        expect(readDismissedHealthItemIds()).toEqual([]);
    });

    it('trims ids before comparing', () => {
        dismissHealthItem('  auto-token  ');

        expect(isHealthItemDismissed('auto-token')).toBe(true);
    });

    it('survives corrupted storage without throwing', () => {
        localStorage.setItem(DISMISSED_HEALTH_ITEMS_KEY, 'not-json{{');

        expect(readDismissedHealthItemIds()).toEqual([]);
        expect(isHealthItemDismissed('auto-token')).toBe(false);

        // Writing still works after a corrupt read.
        dismissHealthItem('auto-token');
        expect(isHealthItemDismissed('auto-token')).toBe(true);
    });

    it('discards non-array storage payloads', () => {
        localStorage.setItem(DISMISSED_HEALTH_ITEMS_KEY, JSON.stringify({ a: 1 }));

        expect(readDismissedHealthItemIds()).toEqual([]);
    });

    it('discards non-string entries inside the array', () => {
        localStorage.setItem(DISMISSED_HEALTH_ITEMS_KEY, JSON.stringify(['ok', 42, null, '']));

        expect(readDismissedHealthItemIds()).toEqual(['ok']);
    });

    it('caps the stored list so it cannot grow without bound', () => {
        for (let i = 0; i < 150; i += 1) dismissHealthItem(`item-${i}`);

        const ids = readDismissedHealthItemIds();
        expect(ids.length).toBe(100);
        // The newest entries are the ones kept.
        expect(ids[ids.length - 1]).toBe('item-149');
        expect(ids).not.toContain('item-0');
    });

    // --- 写入结果可观测（隐私模式 / 存储被禁用时不能「静默失败」） ---

    it('reports whether a dismissal was actually persisted', () => {
        expect(dismissHealthItem('auto-token')).toBe(true);
        // 已忽略视为已持久化，重复点击不应被当成失败。
        expect(dismissHealthItem('auto-token')).toBe(true);
        // 空 id 从未被持久化。
        expect(dismissHealthItem('')).toBe(false);
    });

    it('reports failure when storage rejects the write', () => {
        vi.stubGlobal('localStorage', {
            getItem: () => null,
            setItem: () => {
                throw new Error('QuotaExceededError');
            },
            removeItem: () => {},
        });

        expect(dismissHealthItem('auto-token')).toBe(false);
        expect(isHealthItemDismissed('auto-token')).toBe(false);
        expect(readDismissedHealthItemIds()).toEqual([]);
    });

    it('reports failure when the dismissal cannot be cleared', () => {
        dismissHealthItem('auto-token');
        const stored = localStorage.getItem(DISMISSED_HEALTH_ITEMS_KEY);

        vi.stubGlobal('localStorage', {
            getItem: () => stored,
            setItem: () => {},
            removeItem: () => {
                throw new Error('SecurityError');
            },
        });

        expect(clearDismissedHealthItems()).toBe(false);
        expect(isHealthItemDismissed('auto-token')).toBe(true);
    });

    it('self-heals a non-array payload on the next write', () => {
        localStorage.setItem(DISMISSED_HEALTH_ITEMS_KEY, JSON.stringify({ a: 1 }));

        expect(dismissHealthItem('auto-token')).toBe(true);
        expect(readDismissedHealthItemIds()).toEqual(['auto-token']);
    });
});
