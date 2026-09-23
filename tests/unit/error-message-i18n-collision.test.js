/**
 * 被当作 Error.message 抛出的 i18n 文案，不能长得像「网络错误」。
 *
 * 背景：本仓库习惯写 `throw new Error(t('some.key'))` —— 把**翻译文案**塞进
 * Error.message。随后 errorHandler 这类兜底逻辑会对 message 做模式匹配。
 * 于是同一个故障在不同语言下会被分到不同分支：
 *
 *   en: 'Failed to fetch nodes' → 含 "failed to fetch" → 被判为网络问题
 *   zh: '获取节点失败'           → 不匹配               → 显示原始消息
 *
 * 用户界面语言不同、提示内容就不同，这是错的。更根本的问题是：
 * 错误消息是给机器看的**数据**，翻译文案是给人看的**界面**，两者不该混用。
 * 短期内不便把 29 处 throw 全部改成携带 code 的对象，所以先用这个守卫兜住：
 * 任何被抛出的文案，在**任一语言**下都不得匹配传输层错误措辞。
 */
import { readFileSync, readdirSync, statSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import { messages } from '../../src/i18n/messages.js';
import { t } from '../../src/i18n/index.js';
import { isNetworkErrorMessage } from '../../src/utils/network-error.js';

const SRC_ROOT = path.resolve(__dirname, '../../src');

function walk(dir, out = []) {
    for (const entry of readdirSync(dir)) {
        const full = path.join(dir, entry);
        if (statSync(full).isDirectory()) {
            walk(full, out);
        } else if (/\.(js|vue)$/.test(entry)) {
            out.push(full);
        }
    }
    return out;
}

/** 去掉块注释与行注释，避免文档里举例的写法被当成真实调用 */
function stripComments(source) {
    return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
}

/** 收集所有 `new Error(... t('key') ...)` 里引用的 key */
function collectThrownMessageKeys() {
    const files = walk(SRC_ROOT);
    const found = [];
    const pattern = /new (?:Error|TypeError)\([^;]*?t\('([A-Za-z0-9_.]+)'/g;

    for (const file of files) {
        const source = stripComments(readFileSync(file, 'utf8'));
        let match;
        while ((match = pattern.exec(source)) !== null) {
            found.push({ key: match[1], file: path.relative(SRC_ROOT, file) });
        }
    }

    return { files: files.length, found };
}

const { files: scannedFiles, found: thrownKeys } = collectThrownMessageKeys();
const uniqueKeys = [...new Set(thrownKeys.map((item) => item.key))];

describe('错误消息文案与网络判定的碰撞守卫', () => {
    it('扫描范围没有静默空转（防止目录改名后守卫失效）', () => {
        expect(scannedFiles).toBeGreaterThan(100);
        expect(uniqueKeys.length).toBeGreaterThan(20);
    });

    it('守卫本身有效：真的能抓出撞车的文案', () => {
        // 这一条保证上面的断言不是因为谓词永远返回 false 而「通过」
        expect(isNetworkErrorMessage('Failed to fetch nodes')).toBe(true);
        expect(isNetworkErrorMessage('Load failed')).toBe(true);
    });

    it('每个被抛出的文案 key 在两种语言下都有定义', () => {
        for (const key of uniqueKeys) {
            for (const locale of ['zh-CN', 'en-US']) {
                const resolved = t(key, {}, locale);
                expect(resolved, `${key} 在 ${locale} 下缺失`).not.toBe(key);
                expect(String(resolved).trim().length).toBeGreaterThan(0);
            }
        }
    });

    it('任何被抛出的文案都不会被判成网络错误', () => {
        const violations = [];

        for (const { key, file } of thrownKeys) {
            for (const locale of Object.keys(messages)) {
                const resolved = t(key, {}, locale);
                if (isNetworkErrorMessage(resolved)) {
                    violations.push(`${locale} ${key} = "${resolved}"  (${file})`);
                }
            }
        }

        expect(
            violations,
            `以下错误文案会被误判成网络问题，请改写措辞：\n${violations.join('\n')}`
        ).toEqual([]);
    });
});
