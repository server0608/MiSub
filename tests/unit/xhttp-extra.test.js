import { describe, it, expect, vi } from 'vitest';
import { parseXhttpExtra, serializeXhttpExtra } from '../../functions/utils/xhttp-extra.js';
import { urlToClashProxy } from '../../functions/utils/url-to-clash.js';
import { convertClashProxyToUrl } from '../../functions/utils/clash-to-url.js';

// packet-up 模式下 extra 可用的完整字段集；同时覆盖下行非安全字段。
const FULL_EXTRA = {
    headers: {
        Host: 'extra.example.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    },
    xPaddingBytes: '100-1000',
    xPaddingObfsMode: true,
    xPaddingPlacement: 'queryInHeader',
    xPaddingMethod: 'tokenish',
    xPaddingKey: 'x',
    xPaddingHeader: 'Origin',
    sessionIDPlacement: 'query',
    sessionIDKey: 's',
    seqPlacement: 'query',
    seqKey: 'q',
    uplinkDataPlacement: 'header',
    uplinkDataKey: 'X-Data',
    sessionIDTable: 'Base62',
    sessionIDLength: '12-20',
    uplinkChunkSize: '2000-3000',
    uplinkHTTPMethod: 'GET',
    noGRPCHeader: false,
    scMaxEachPostBytes: '4000-8000',
    scMinPostsIntervalMs: '10-50',
    xmux: {
        maxConcurrency: 0,
        maxConnections: 0,
        cMaxReuseTimes: 0,
        hMaxRequestTimes: 0,
        hMaxReusableSecs: 0,
        hKeepAlivePeriod: 0,
    },
    downloadSettings: {
        address: 'cf2.example.com',
        port: 443,
        xhttpSettings: {
            path: '/dl',
            host: 'cf3.example.com',
            extra: { xmux: { maxConcurrency: 1 } },
        },
    },
};

const PARSED_OPTS = parseXhttpExtra(FULL_EXTRA);

describe('parseXhttpExtra 正向映射', () => {
    it('应完整映射 packet-up 的 extra 与非安全下载字段', () => {
        const opts = parseXhttpExtra(FULL_EXTRA);

        expect(opts).toEqual({
            headers: {
                Host: 'extra.example.com',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            },
            'x-padding-bytes': '100-1000',
            'x-padding-obfs-mode': true,
            'x-padding-placement': 'queryInHeader',
            'x-padding-method': 'tokenish',
            'x-padding-key': 'x',
            'x-padding-header': 'Origin',
            'session-placement': 'query',
            'session-key': 's',
            'seq-placement': 'query',
            'seq-key': 'q',
            'uplink-data-placement': 'header',
            'uplink-data-key': 'X-Data',
            'session-table': 'Base62',
            'session-length': '12-20',
            'uplink-chunk-size': '2000-3000',
            'uplink-http-method': 'GET',
            'sc-max-each-post-bytes': '4000-8000',
            'sc-min-posts-interval-ms': '10-50',
            'reuse-settings': {
                'max-connections': '0',
                'max-concurrency': '0',
                'c-max-reuse-times': '0',
                'h-max-request-times': '0',
                'h-max-reusable-secs': '0',
                'h-keep-alive-period': 0,
            },
            'download-settings': {
                server: 'cf2.example.com',
                port: 443,
                path: '/dl',
                host: 'cf3.example.com',
                'reuse-settings': { 'max-concurrency': '1' },
            },
        });
    });
});

describe('serializeXhttpExtra 反向序列化', () => {
    it('应把 mihomo xhttp-opts 逆向为 extra，并剔除 Host 头', () => {
        const extra = serializeXhttpExtra({
            path: '/up',
            host: 'up.example.com',
            mode: 'packet-up',
            ...PARSED_OPTS,
        });

        expect(extra).toEqual({
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            },
            xPaddingBytes: '100-1000',
            xPaddingObfsMode: true,
            xPaddingPlacement: 'queryInHeader',
            xPaddingMethod: 'tokenish',
            xPaddingKey: 'x',
            xPaddingHeader: 'Origin',
            sessionIDPlacement: 'query',
            sessionIDKey: 's',
            seqPlacement: 'query',
            seqKey: 'q',
            uplinkDataPlacement: 'header',
            uplinkDataKey: 'X-Data',
            sessionIDTable: 'Base62',
            sessionIDLength: '12-20',
            uplinkChunkSize: '2000-3000',
            uplinkHTTPMethod: 'GET',
            scMaxEachPostBytes: '4000-8000',
            scMinPostsIntervalMs: '10-50',
            xmux: {
                maxConnections: '0',
                maxConcurrency: '0',
                cMaxReuseTimes: '0',
                hMaxRequestTimes: '0',
                hMaxReusableSecs: '0',
                hKeepAlivePeriod: 0,
            },
            downloadSettings: {
                address: 'cf2.example.com',
                port: 443,
                xhttpSettings: {
                    path: '/dl',
                    host: 'cf3.example.com',
                    extra: { xmux: { maxConcurrency: '1' } },
                },
            },
        });
    });

    it('不含 extra 字段时返回 null', () => {
        expect(serializeXhttpExtra({ path: '/x', host: 'h', mode: 'auto' })).toBeNull();
        expect(serializeXhttpExtra(null)).toBeNull();
    });
});

const EXTRA_PARAM = encodeURIComponent(JSON.stringify(FULL_EXTRA));

const XHTTP_URL =
    'vless://uuid@1.2.3.4:443?encryption=none&security=none&type=xhttp' +
    `&path=%2Fup&host=url.example.com&mode=packet-up&extra=${EXTRA_PARAM}#node`;

describe('转换器接入', () => {
    it('parseVlessUrl 应合并 extra，并让 URL 的 Host 优先于 extra.headers.Host', () => {
        const proxy = urlToClashProxy(XHTTP_URL);
        const opts = proxy['xhttp-opts'];

        expect(opts.path).toBe('/up');
        expect(opts.host).toBe('url.example.com');
        expect(opts.headers).toEqual({
            Host: 'url.example.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        });
        expect(opts['x-padding-bytes']).toBe('100-1000');
        expect(opts['reuse-settings']['h-max-request-times']).toBe('0');
        expect(opts['download-settings']).toEqual({
            server: 'cf2.example.com',
            port: 443,
            path: '/dl',
            host: 'cf3.example.com',
            'reuse-settings': { 'max-concurrency': '1' },
        });
    });

    it('extra 为非法 JSON 时仍按基础字段导入', () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const url = 'vless://uuid@1.2.3.4:443?type=xhttp&path=%2Fup&extra=%7Bnot-json';
        const proxy = urlToClashProxy(url);

        expect(proxy['xhttp-opts']).toEqual({ path: '/up' });
        expect(warn).toHaveBeenCalled();
        warn.mockRestore();
    });

    it('convertClashProxyToUrl 应输出 path/host/mode 与 extra', () => {
        const url = convertClashProxyToUrl({
            name: 'node',
            type: 'vless',
            server: '1.2.3.4',
            port: 443,
            uuid: 'uuid',
            network: 'xhttp',
            'xhttp-opts': {
                path: '/up',
                host: 'up.example.com',
                mode: 'packet-up',
                'x-padding-bytes': '100-1000',
                'session-placement': 'query',
                'reuse-settings': { 'max-concurrency': '1' },
            },
        });

        expect(url).toContain('type=xhttp');
        expect(url).toContain('path=%2Fup');
        expect(url).toContain('host=up.example.com');
        expect(url).toContain('mode=packet-up');

        const match = url.match(/(?:\?|&)extra=([^&#]*)/);
        expect(match).not.toBeNull();
        const extra = JSON.parse(decodeURIComponent(match[1]));
        expect(extra.xPaddingBytes).toBe('100-1000');
        expect(extra.sessionIDPlacement).toBe('query');
        expect(extra.xmux).toEqual({ maxConcurrency: '1' });
    });
});

describe('downloadSettings 安全块映射', () => {
    it('security 决定读取 tlsSettings 还是 realitySettings', () => {
        const tlsOpts = parseXhttpExtra({
            downloadSettings: {
                address: 'dl.example.com',
                port: 443,
                security: 'tls',
                tlsSettings: {
                    serverName: 'tls.example.com',
                    fingerprint: 'firefox',
                    alpn: ['h2'],
                    allowInsecure: true,
                },
                realitySettings: {
                    serverName: 'reality.example.com',
                    fingerprint: 'safari',
                    publicKey: 'rpk',
                    shortId: 'rsid',
                },
                xhttpSettings: { path: '/dl' },
            },
        });
        expect(tlsOpts['download-settings']).toEqual({
            server: 'dl.example.com',
            port: 443,
            tls: true,
            servername: 'tls.example.com',
            'client-fingerprint': 'firefox',
            alpn: ['h2'],
            'skip-cert-verify': true,
            path: '/dl',
        });

        const realityOpts = parseXhttpExtra({
            downloadSettings: {
                address: 'dl.example.com',
                port: 443,
                security: 'reality',
                tlsSettings: { serverName: 'tls.example.com', fingerprint: 'firefox' },
                realitySettings: {
                    serverName: 'reality.example.com',
                    fingerprint: 'safari',
                    publicKey: 'rpk',
                    shortId: 'rsid',
                },
            },
        });
        expect(realityOpts['download-settings']).toEqual({
            server: 'dl.example.com',
            port: 443,
            tls: true,
            servername: 'reality.example.com',
            'client-fingerprint': 'safari',
            'reality-opts': { 'public-key': 'rpk', 'short-id': 'rsid' },
        });
    });
});
