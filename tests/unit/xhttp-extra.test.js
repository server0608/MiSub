import { describe, it, expect } from 'vitest';
import { parseXhttpExtra, serializeXhttpExtra } from '../../functions/utils/xhttp-extra.js';

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
