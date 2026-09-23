/**
 * 接线守卫：裸 localStorage 访问必须收敛到 src/utils/local-preference.js。
 *
 * 为什么需要这个守卫：
 * 1. 浏览器禁用站点数据（隐私模式 / 站点权限被关）时，光是**读取** localStorage
 *    就会抛 SecurityError。若在 store / composable / setup 阶段裸读，整个应用
 *    直接白屏起不来。
 * 2. 写入侧更隐蔽：Safari 无痕模式与部分隐私扩展下 setItem 不抛错也不落盘，
 *    只靠 try/catch 无法识别，必须回读校验。
 *
 * 这两件事只在共享助手里统一处理，所以这里静态扫描全部源码，禁止绕过。
 */
import { readFileSync, readdirSync } from 'node:fs';
import path from 'node:path';
import { describe, expect, it } from 'vitest';

const SRC_ROOT = path.resolve(process.cwd(), 'src');
const HELPER = path.resolve(SRC_ROOT, 'utils/local-preference.js');
const BARE_ACCESS = /localStorage\s*\.\s*(getItem|setItem|removeItem|clear)\s*\(/;

function collectSourceFiles(dir) {
    const entries = readdirSync(dir, { withFileTypes: true });
    const files = [];
    for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
            files.push(...collectSourceFiles(fullPath));
        } else if (/\.(js|vue)$/.test(entry.name)) {
            files.push(fullPath);
        }
    }
    return files;
}

/** 去掉注释与字符串里的「假阳性」（如文档注释中举例 localStorage.getItem） */
function stripComments(source) {
    return source.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:])\/\/[^\n]*/g, '$1');
}

describe('localStorage 访问接线守卫', () => {
    const sourceFiles = collectSourceFiles(SRC_ROOT);

    it('扫描范围覆盖到源码目录（防止误删目录后守卫静默失效）', () => {
        expect(sourceFiles.length).toBeGreaterThan(100);
    });

    it('除共享助手外，源码里不存在裸 localStorage 读写', () => {
        const offenders = [];
        for (const file of sourceFiles) {
            if (file === HELPER) continue;
            const code = stripComments(readFileSync(file, 'utf-8'));
            if (BARE_ACCESS.test(code)) {
                offenders.push(path.relative(process.cwd(), file));
            }
        }
        expect(offenders).toEqual([]);
    });

    it('共享助手自身提供完整的读写/删除能力', () => {
        const helperSource = readFileSync(HELPER, 'utf-8');
        for (const fn of [
            'canUseLocalStorage',
            'readPreference',
            'writePreference',
            'readRawPreference',
            'writeRawPreference',
            'removePreference',
        ]) {
            expect(helperSource).toContain(`export function ${fn}`);
        }
    });

    it('写入路径必须回读校验，才能识别「不抛错也不落盘」的静默丢失', () => {
        const helperSource = readFileSync(HELPER, 'utf-8');
        const writeBody = helperSource.slice(
            helperSource.indexOf('export function writePreference')
        );
        expect(writeBody).toMatch(/localStorage\.getItem\(key\)\s*===\s*serialized/);
    });
});
