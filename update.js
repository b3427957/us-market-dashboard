const fs = require('fs');
const path = require('path');
const fetch = require('node-fetch');
const WebSocket = require('ws');
const { parse } = require('csv-parse/sync');
const { google } = require('googleapis');

const TOKEN_PATH = path.join(__dirname, 'token.json');
const CREDENTIALS_PATH = path.join(__dirname, 'credentials.json');
const DATA_PATH = path.join(__dirname, 'data.json');

const FRED_KEY = process.env.FRED_API_KEY || '';

async function authorize() {
    if (!fs.existsSync(CREDENTIALS_PATH) || !fs.existsSync(TOKEN_PATH)) return null;
    try {
        const credentials = JSON.parse(fs.readFileSync(CREDENTIALS_PATH));
        const { client_secret, client_id } = credentials.installed || credentials.web;
        const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, 'urn:ietf:wg:oauth:2.0:oob');
        oAuth2Client.setCredentials(JSON.parse(fs.readFileSync(TOKEN_PATH)));
        return oAuth2Client;
    } catch (e) {
        return null;
    }
}

async function createGoogleTask(auth, taskTitle) {
    if (!auth) return;
    const service = google.tasks({ version: 'v1', auth });
    try {
        const lists = await service.tasklists.list({ maxResults: 10 });
        await service.tasks.insert({
            tasklist: lists.data.items[0].id,
            requestBody: { title: taskTitle, due: new Date().toISOString() }
        });
        console.log(`📌 已建立提醒：【${taskTitle}】`);
    } catch (e) {
        console.error('建立 Google Tasks 失敗:', e.message);
    }
}

// 採用 FRED 官方 API 讀取
async function fetchFredSeries(id, count = 60) {
    if (FRED_KEY) {
        const apiUrl = `https://api.stlouisfed.org/fred/series/observations?series_id=${id}&api_key=${FRED_KEY}&file_type=json&sort_order=desc&limit=${count}`;
        const res = await fetch(apiUrl, { timeout: 10000 });
        if (!res.ok) throw new Error(`FRED API 錯誤: ${res.statusText}`);
        const data = await res.json();
        return (data.observations || [])
            .map(r => ({ date: r.date, value: parseFloat(r.value) }))
            .filter(r => r.date && !isNaN(r.value))
            .reverse()
            .map(r => ({ date: r.date, value: Number(r.value.toFixed(2)) }));
    }

    const url = `https://fred.stlouisfed.org/graph/fredgraph.csv?id=${id}`;
    const res = await fetch(url, {
        headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)' },
        timeout: 10000
    });
    const text = await res.text();
    const records = parse(text, { columns: true, skip_empty_lines: true, bom: true });
    return records
        .map(r => ({ date: String(r.observation_date || r.DATE || Object.values(r)[0]).trim(), value: parseFloat(r[id]) }))
        .filter(r => r.date && !isNaN(r.value))
        .slice(-count)
        .map(r => ({ date: r.date, value: Number(r.value.toFixed(2)) }));
}

function fetchTVHistoryBars(symbol, count = 60) {
    return new Promise((resolve) => {
        const ws = new WebSocket('wss://data.tradingview.com/socket.io/websocket', {
            headers: { 'Origin': 'https://www.tradingview.com' }
        });
        const session = 'cs_' + Math.random().toString(36).substring(2, 12);
        let done = false;
        const bars = [];
        const send = (m, p) => {
            const str = JSON.stringify({ m, p });
            ws.send(`~m~${str.length}~m~${str}`);
        };
        const finish = (res) => {
            if (!done) {
                done = true;
                clearTimeout(timer);
                try { ws.close(); } catch (e) {}
                resolve(res);
            }
        };
        const timer = setTimeout(() => finish(bars), 8000);

        ws.on('open', () => {
            send('set_auth_token', ['unauthorized_user_token']);
            send('chart_create_session', [session, '']);
            send('resolve_symbol', [session, 'sds_sym_1', `={"symbol":"${symbol}","adjustment":"splits"}`]);
            send('create_series', [session, 'sds_1', 's1', 'sds_sym_1', '1D', count + 10, '']);
        });

        ws.on('message', (data) => {
            const raw = data.toString();
            const hb = raw.match(/~m~\d+~m~(~h~\d+)/);
            if (hb) ws.send(`~m~${hb[1].length}~m~${hb[1]}`);
            const parts = raw.split(/~m~\d+~m~/).filter(Boolean);
            for (const part of parts) {
                if (part.startsWith('{')) {
                    try {
                        const msg = JSON.parse(part);
                        if (msg.m === 'timescale_update' && msg.p?.[1]?.sds_1?.s) {
                            msg.p[1].sds_1.s.forEach(item => {
                                const d = new Date(item.v[0] * 1000).toISOString().split('T')[0];
                                bars.push({ date: d, value: Number(item.v[4].toFixed(2)) });
                            });
                            if (bars.length > 0) finish(bars.slice(-count));
                        }
                    } catch (e) {}
                }
            }
        });
        ws.on('error', () => finish([]));
    });
}

const soxSymbols = [
  'NVDA', 'AMD', 'AVGO', 'TXN', 'QCOM', 'INTC', 'AMAT', 'LRCX', 'MU', 'KLAC',
  'SNPS', 'CDNS', 'NXPI', 'MRVL', 'ADI', 'ASML', 'TSM', 'STM', 'ON', 'MCHP',
  'MPWR', 'SWKS', 'TER', 'WDC', 'QRVO', 'RMBS', 'LSCC', 'WOLF', 'ENTG', 'ONTO'
];

async function getSoxMMTHHistory(count = 60) {
    console.log("正在透過 TradingView 計算費半 30 檔成分股的歷史 MMTH (SXTH)...");
    
    let dates = [];
    const stockBarsMap = {};

    for (const symbol of soxSymbols) {
        try {
            const bars = await fetchTVHistoryBars(symbol, 350);
            if (bars && bars.length > 0) {
                stockBarsMap[symbol] = bars;
                if (dates.length === 0) {
                    dates = bars.map(b => b.date);
                }
            }
        } catch (e) {}
    }

    if (dates.length === 0) return { latestVal: 0, historyArr: [] };

    const validSymbols = Object.keys(stockBarsMap);
    const fullHistory = [];
    const targetDates = dates.slice(-count);

    for (let i = 0; i < targetDates.length; i++) {
        const currentDate = targetDates[i];
        let above200MA = 0;
        let counted = 0;

        for (const sym of validSymbols) {
            const bars = stockBarsMap[sym];
            const barIndex = bars.findIndex(b => b.date === currentDate);
            if (barIndex >= 200) {
                const currentPrice = bars[barIndex].value;
                const slice200 = bars.slice(barIndex - 199, barIndex + 1);
                const sum = slice200.reduce((acc, b) => acc + b.value, 0);
                const ma200 = sum / 200;

                counted++;
                if (currentPrice > ma200) {
                    above200MA++;
                }
            }
        }

        const pct = counted > 0 ? Number(((above200MA / counted) * 100).toFixed(2)) : 0;
        fullHistory.push({ date: currentDate, value: pct });
    }

    const latestVal = fullHistory.length > 0 ? fullHistory[fullHistory.length - 1].value : 0;
    console.log(`🔥 計算完成！今日最新 SXTH 費半廣度：${latestVal}%`);
    return { latestVal, historyArr: fullHistory.map(h => h.value) };
}

async function main() {
    console.log("正在下載 FRED 與 TradingView 過去 60 個交易日的真實歷史數據...");
    try {
        const [hyOas, cccOas, us10y, vix, tvMMTH, tvS5TH, tvNDTH, soxResult] = await Promise.all([
            fetchFredSeries("BAMLH0A0HYM2", 60),
            fetchFredSeries("BAMLH0A3HYC", 60),
            fetchFredSeries("DGS10", 60),
            fetchFredSeries("VIXCLS", 60),
            fetchTVHistoryBars("INDEX:MMTH", 60),
            fetchTVHistoryBars("INDEX:S5TH", 60),
            fetchTVHistoryBars("INDEX:NDTH", 60),
            getSoxMMTHHistory(60)
        ]);

        const soxMMTHValue = soxResult.latestVal;
        const soxHistoryArr = soxResult.historyArr;

        console.log(`📊 市場廣度真實歷史筆數 -> MMTH: ${tvMMTH.length}, S5TH: ${tvS5TH.length}, NDTH: ${tvNDTH.length}, SXTH: ${soxHistoryArr.length}`);

        const s5Map = new Map(tvS5TH.map(r => [r.date, r.value]));
        const ndMap = new Map(tvNDTH.map(r => [r.date, r.value]));
        const breadthDates = [], mmthArr = [], s5thArr = [], ndthArr = [];

        tvMMTH.forEach(item => {
            if (s5Map.has(item.date) && ndMap.has(item.date)) {
                breadthDates.push(item.date);
                mmthArr.push(item.value);
                s5thArr.push(s5Map.get(item.date));
                ndthArr.push(ndMap.get(item.date));
            }
        });

        const crossAvg = mmthArr.map((m, i) => Number(((m + s5thArr[i] + ndthArr[i]) / 3).toFixed(2)));
        const ndxDiv = mmthArr.map((m, i) => Number((ndthArr[i] - (m + s5thArr[i]) / 2).toFixed(2)));

        const calcFredKpi = (arr) => {
            const l = arr[arr.length - 1] || { date: "-", value: 0 };
            const p = arr[arr.length - 2] || l;
            return { date: l.date, value: l.value, change: Number((l.value - p.value).toFixed(2)) };
        };

        const calcBreadthKpi = (valArr, dateArr) => {
            const len = valArr.length;
            const val = len > 0 ? valArr[len - 1] : 0;
            const prev = len > 1 ? valArr[len - 2] : val;
            return { date: len > 0 ? dateArr[len - 1] : "-", value: val, change: Number((val - prev).toFixed(2)) };
        };

        const kpis = {
            MMTH: calcBreadthKpi(mmthArr, breadthDates),
            S5TH: calcBreadthKpi(s5thArr, breadthDates),
            NDTH: calcBreadthKpi(ndthArr, breadthDates),
            SXTH: { date: breadthDates[breadthDates.length - 1] || "-", value: soxMMTHValue, change: soxHistoryArr.length > 1 ? Number((soxHistoryArr[soxHistoryArr.length - 1] - soxHistoryArr[soxHistoryArr.length - 2]).toFixed(2)) : 0 },
            HY_OAS: calcFredKpi(hyOas),
            CCC_OAS: calcFredKpi(cccOas),
            US10Y: calcFredKpi(us10y),
            VIX: calcFredKpi(vix)
        };

        let alerts = [];
        if (kpis.VIX.value > 20) alerts.push(`🚨 VIX 超過 20（目前：${kpis.VIX.value}）`);
        if (kpis.HY_OAS.value > 4.5) alerts.push(`⚠️ 高收益債利差擴大（目前：${kpis.HY_OAS.value}%）`);
        if (kpis.US10Y.value > 5.0) alerts.push(`🚨 美國 10 年期公債殖利率突破 5% 警戒線（目前：${kpis.US10Y.value}%）`);
        
        if (soxMMTHValue < 60) {
            alerts.push(`⚠️ 費半 MMTH 跌破 60 警戒線（目前值：${soxMMTHValue}%）`);
        }

        if (alerts.length > 0) {
            const auth = await authorize();
            for (const msg of alerts) await createGoogleTask(auth, msg);
        }

        const creditMap = new Map(cccOas.map(r => [r.date, r.value]));
        const creditDates = hyOas.map(r => r.date);
        const hyValues = hyOas.map(r => r.value);
        const fallbackCcc = cccOas.length > 0 ? cccOas[cccOas.length - 1].value : 0;
        const cccValues = hyOas.map(r => creditMap.has(r.date) ? creditMap.get(r.date) : fallbackCcc);

        const latestSnapshotDate = breadthDates[breadthDates.length - 1] || vix[vix.length - 1].date;

        const finalOutput = {
            updatedAt: new Date().toLocaleString('zh-TW', { timeZone: 'Asia/Taipei' }),
            snapshotDate: latestSnapshotDate,
            count: breadthDates.length || vix.length,
            riskRulesStatus: alerts.length > 0 ? `觸發 ${alerts.length} 項警報` : "尚未啟用",
            kpis,
            series: {
                breadth: { 
                    date: breadthDates, 
                    MMTH: mmthArr, 
                    S5TH: s5thArr, 
                    NDTH: ndthArr, 
                    SXTH: soxHistoryArr.length > 0 ? soxHistoryArr : mmthArr,
                    cross_avg: crossAvg, 
                    ndx_div: ndxDiv 
                },
                credit: { date: creditDates, HY_OAS: hyValues, CCC_OAS: cccValues },
                us10y: { date: us10y.map(r => r.date), value: us10y.map(r => r.value) },
                vix: { date: vix.map(r => r.date), value: vix.map(r => r.value) }
            }
        };

        fs.writeFileSync(DATA_PATH, JSON.stringify(finalOutput, null, 2), 'utf8');
        console.log(`✅ 資料更新成功！快照日期: ${latestSnapshotDate}，共 ${finalOutput.count} 個交易日。`);
    } catch (err) {
        console.error("更新失敗:", err);
        process.exit(1);
    }
}

main();
