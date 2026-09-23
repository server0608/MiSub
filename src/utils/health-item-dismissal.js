/**
 * 待处理事项的「忽略」记忆
 *
 * 有些待处理项在当前阶段并不打算处理（例如暂时不想固定主 Token），
 * 但健康检查每次都会重新推导出来，用户无法把它从列表里去掉。
 * 这里把被忽略的条目 id 记在本地，让用户可以主动收起不关心的项。
 *
 * 存储位置：localStorage（纯前端偏好，不进入后端数据，不影响导出/备份）。
 * 与 `domain-name-memory.js` 同属一类：只影响本机展示，不改变订阅数据。
 * 读写与失败判定统一走 `local-preference.js`。
 */

import { readPreference, removePreference, writePreference } from './local-preference.js';

const STORAGE_KEY = 'misub:dismissedHealthItems';
const MAX_ENTRIES = 100;

/** 校验载荷：必须是字符串数组，逐项过滤空值 */
function normalizeIds(parsed) {
    if (!Array.isArray(parsed)) {
        // 被其它工具/旧版本写成对象或字符串时，这里只能退化为「无忽略记录」。
        // 下一次写入会用合法的字符串数组覆盖它，从而自愈。
        console.warn('[HealthItemDismissal] Ignoring non-array storage value');
        return [];
    }
    return parsed.filter((id) => typeof id === 'string' && id);
}

/** 读取已忽略的条目 id 集合（容错：损坏时返回空集合） */
function readAll() {
    return readPreference(STORAGE_KEY, normalizeIds, []);
}

/**
 * 写入已忽略的 id 列表。
 * @returns {boolean} 是否真正落盘（隐私模式 / 存储被禁用 / 静默丢失时为 false）
 */
function writeAll(ids) {
    // 控制体积：超出上限时丢弃最早写入的条目
    return writePreference(STORAGE_KEY, ids.slice(-MAX_ENTRIES));
}

/** 某个待处理项是否已被忽略 */
export function isHealthItemDismissed(id) {
    const key = String(id || '').trim();
    if (!key) return false;
    return readAll().includes(key);
}

/**
 * 读取已忽略 id 的数组副本。
 * 供视图镜像到 ref 使用（localStorage 本身不具备响应性）。
 */
export function readDismissedHealthItemIds() {
    return readAll();
}

/**
 * 忽略一个待处理项（重复忽略无副作用）。
 * @returns {boolean} 该条目现在是否已被持久化忽略
 */
export function dismissHealthItem(id) {
    const key = String(id || '').trim();
    if (!key) return false;
    const ids = readAll();
    if (ids.includes(key)) return true;
    ids.push(key);
    return writeAll(ids);
}

/**
 * 取消忽略（用于「全部恢复」）。
 * @returns {boolean} 是否写入成功
 */
export function restoreHealthItem(id) {
    const key = String(id || '').trim();
    if (!key) return false;
    return writeAll(readAll().filter((entry) => entry !== key));
}

/**
 * 清空全部忽略记录（用于「全部恢复」）。
 * @returns {boolean} 是否清除成功
 */
export function clearDismissedHealthItems() {
    return removePreference(STORAGE_KEY);
}

/**
 * 过滤掉已被忽略的条目。
 * @param {Array<{id: string}>} items
 * @returns {Array} 保留的条目
 */
export function filterDismissedHealthItems(items) {
    if (!Array.isArray(items)) return [];
    const dismissed = new Set(readAll());
    return items.filter((item) => !dismissed.has(item.id));
}

export const DISMISSED_HEALTH_ITEMS_KEY = STORAGE_KEY;
