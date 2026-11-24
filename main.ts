// Pump.fun Bot Sprint — "Patient Momentum + Probe Guardrail" (original design).
// Trades paper SOL on a replay of pump.fun bonding-curve data.
// Deterministic: no clock, no environment, no Math.random. Bigint money math only.
// Iteration is over insertion-ordered Maps fed by the batch stream.

import { runBot, LAMPORTS_PER_SOL } from './vendor/makefun-bot.ts';
import type { Batch, BotState, OrderInput } from './vendor/makefun-bot.ts';

const SOL = LAMPORTS_PER_SOL;
const FIRST_BUY = (SOL * 2n) / 100n; // 0.02 SOL
const ADD_BUY = (SOL * 15n) / 1000n; // 0.015 SOL
const MAX_STAKE = SOL / 20n; // 0.05 SOL
const MIN_BUY_CAP = SOL / 100n; // 0.01 SOL
const PROBE_BUY = (SOL * 12n) / 1000n; // 0.012 SOL

const ENTRY_MIN_AGE = 30;
const WINDOW_SLOTS = 120;
const TAPE_KEEP_SLOTS = 150;
const MIN_BUYS = 5;
const MIN_BUYERS = 3;
const MIN_INFLOW = SOL; // 1.0 SOL
const REAL_MIN = SOL; // 1 SOL
const REAL_MAX = SOL * 60n; // 60 SOL
const PROBE_REAL_MIN = SOL * 5n; // 5 SOL

const MAX_HOLD_SLOTS = 300;
const FADE_WINDOW_SLOTS = 60;
const FADE_THRESHOLD = -(SOL * 2n) / 10n; // -0.2 SOL
const BIG_SELL = SOL; // 1 SOL
const PROBE_HOLD_SLOTS = 30;
const MAIN_BUY_QUIET_SLOTS = 50;
const PROBE_MAX_OPEN = 3;
const PROBE_ROUND_TRIP_TARGET = 25;

interface TapeTrade {
  slot: number;
  isBuy: boolean;
  sol: bigint;
  user: string;
  vq: bigint; // virtual quote reserves after the trade
  vt: bigint; // virtual token reserves after the trade
}

interface CurveTape {
  createdSlot: number | null;
  firstSeenSlot: number;
  trades: TapeTrade[];
}

interface PosInfo {
  firstBuySlot: number;
  lastBuyPriceNum: bigint;
  lastBuyPriceDen: bigint;
  adds: number;
  peakNum: bigint;
  peakDen: bigint;
  trailingArmed: boolean;
  isProbe: boolean;
  lastCost: bigint;
}

// Compare fractions aNum/aDen vs bNum/bDen. Returns -1, 0, 1.
function cmpFrac(aNum: bigint, aDen: bigint, bNum: bigint, bDen: bigint): number {
  const l = aNum * bDen;
  const r = bNum * aDen;
  return l < r ? -1 : l > r ? 1 : 0;
}

const tapes = new Map<string, CurveTape>();
// Mint -> slot of its most recent trade (insertion-ordered activity set).
const active = new Map<string, number>();
const entered = new Set<string>();
const probed = new Set<string>();
const roundTrips = new Set<string>();
const posInfo = new Map<string, PosInfo>();
let lastMainBuySlot = -1000000;

function tapeFor(mint: string, slot: number): CurveTape {
  let t = tapes.get(mint);
  if (t === undefined) {
    t = { createdSlot: null, firstSeenSlot: slot, trades: [] };
    tapes.set(mint, t);
  }
  return t;
}

interface WindowStats {
  buyCount: number;
  buyers: Set<string>;
  netInflow: bigint;
  startNum: bigint;
  startDen: bigint;
  highNum: bigint;
  highDen: bigint;
  tradeCount: number;
}

function windowStats(mint: string, slot: number, windowSlots: number): WindowStats | null {
  const t = tapes.get(mint);
  if (t === undefined) return null;
  const from = slot - windowSlots;
  let st: WindowStats | null = null;
  for (const tr of t.trades) {
    if (tr.slot < from) continue;
    if (st === null) {
      st = {
        buyCount: 0,
        buyers: new Set<string>(),
        netInflow: 0n,
        startNum: tr.vq,
        startDen: tr.vt,
        highNum: tr.vq,
        highDen: tr.vt,
        tradeCount: 0,
      };
    }
    st.tradeCount++;
    if (tr.isBuy) {
      st.buyCount++;
      st.buyers.add(tr.user);
      st.netInflow += tr.sol;
    } else {
      st.netInflow -= tr.sol;
    }
    if (cmpFrac(tr.vq, tr.vt, st.highNum, st.highDen) > 0) {
      st.highNum = tr.vq;
      st.highDen = tr.vt;
    }
  }
  return st;
}

function hasBigSell(mint: string): boolean {
  const t = tapes.get(mint);
  if (t === undefined) return false;
  for (const tr of t.trades) {
    if (!tr.isBuy && tr.sol >= BIG_SELL) return true;
  }
  return false;
}

// Current position value as a fraction (tokens * vq / vt), or null when unpriced.
function positionValue(state: BotState, mint: string, tokens: bigint): { num: bigint; den: bigint } | null {
  const c = state.curves.get(mint);
  if (c === undefined || c.virtual_token_reserves <= 0n) return null;
  return { num: tokens * c.virtual_quote_reserves, den: c.virtual_token_reserves };
}

function currentPrice(state: BotState, mint: string): { num: bigint; den: bigint } | null {
  const c = state.curves.get(mint);
  if (c === undefined || c.virtual_token_reserves <= 0n) return null;
  return { num: c.virtual_quote_reserves, den: c.virtual_token_reserves };
}

await runBot({
  onBatch(state: BotState, batch: Batch) {
    const slot = batch.slot;
    const orders: OrderInput[] = [];
    const txFee = state.txFeeLamports();
    const maxOrders = state.hello.caps.max_orders_per_batch;
    let reserved = 0n; // lamports committed by orders planned in this reply (quote + tx fees)
    const pendingSell = new Set<string>();
    const pendingBuy = new Set<string>();
    for (const p of state.pending.values()) {
      if (p.order.side === 'sell') pendingSell.add(p.order.mint);
      else pendingBuy.add(p.order.mint);
    }

    const canAffordBuy = (quote: bigint): boolean =>
      orders.length < maxOrders && state.freeCashLamports() - reserved - txFee - quote >= 0n;

    // ---- ingest this batch's events into the per-curve tapes ----
    for (const e of batch.events) {
      if (e.event === 'CreateEvent') {
        const t = tapeFor(e.mint, slot);
        if (t.createdSlot === null) t.createdSlot = e.slot;
      } else if (e.event === 'TradeEvent') {
        const t = tapeFor(e.mint, slot);
        t.trades.push({
          slot: e.slot,
          isBuy: e.is_buy,
          sol: BigInt(e.sol_amount),
          user: e.user,
          vq: BigInt(e.virtual_quote_reserves),
          vt: BigInt(e.virtual_token_reserves),
        });
        // Keep only the last 150 slots of tape per curve.
        const keepFrom = e.slot - TAPE_KEEP_SLOTS;
        while (t.trades.length > 0 && t.trades[0].slot < keepFrom) t.trades.shift();
        active.set(e.mint, e.slot);
      }
    }

    // ---- sync tracked positions with the account ----
    for (const mint of posInfo.keys()) {
      if (!state.positions.has(mint)) {
        const info = posInfo.get(mint)!;
        if (info.lastCost >= MIN_BUY_CAP) roundTrips.add(mint);
        posInfo.delete(mint);
      }
    }
    for (const [mint, holding] of state.positions) {
      let info = posInfo.get(mint);
      if (info === undefined) {
        const price = currentPrice(state, mint);
        const value = positionValue(state, mint, holding.token_amount);
        info = {
          firstBuySlot: slot,
          lastBuyPriceNum: price !== null ? price.num : 0n,
          lastBuyPriceDen: price !== null ? price.den : 1n,
          adds: 0,
          peakNum: value !== null ? value.num : 0n,
          peakDen: value !== null ? value.den : 1n,
          trailingArmed: false,
          isProbe: probed.has(mint),
          lastCost: holding.cost_lamports,
        };
        posInfo.set(mint, info);
      } else {
        info.lastCost = holding.cost_lamports;
        const value = positionValue(state, mint, holding.token_amount);
        if (value !== null && cmpFrac(value.num, value.den, info.peakNum, info.peakDen) > 0) {
          info.peakNum = value.num;
          info.peakDen = value.den;
        }
      }
    }

    // ---- exits: sell the whole position on the first trigger ----
    for (const [mint, holding] of state.positions) {
      if (pendingSell.has(mint)) continue;
      const info = posInfo.get(mint);
      if (info === undefined) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const value = positionValue(state, mint, holding.token_amount);
      if (value === null) continue;
      const cost = holding.cost_lamports;
      let exit = false;
      // Stop loss: value below 93% of cost for main positions (tighter: cut
      // losers fast); probes keep the spec's -12% stop (88% of cost).
      const stopNum = info.isProbe ? 88n : 93n;
      if (cmpFrac(value.num, value.den, (cost * stopNum) / 100n, 1n) < 0) exit = true;
      // Trailing stop: arm at 115% of cost, sell on an 8% fall from peak value.
      if (!exit && cmpFrac(value.num, value.den, (cost * 115n) / 100n, 1n) >= 0) info.trailingArmed = true;
      if (!exit && info.trailingArmed && cmpFrac(value.num * 100n, value.den, info.peakNum * 92n, info.peakDen) < 0) exit = true;
      // Max hold: 600 slots since the first buy.
      if (!exit && slot - info.firstBuySlot >= MAX_HOLD_SLOTS) exit = true;
      // Fade: net SOL over the last 60 slots below -0.3 SOL.
      if (!exit) {
        const fade = windowStats(mint, slot, FADE_WINDOW_SLOTS);
        if (fade !== null && fade.netInflow < FADE_THRESHOLD) exit = true;
      }
      // Big sell: any single recorded sell of at least 2 SOL.
      if (!exit && hasBigSell(mint)) exit = true;
      // Probe time exit: sell a probe whole after 30 slots held.
      if (!exit && info.isProbe && slot - info.firstBuySlot >= PROBE_HOLD_SLOTS) exit = true;
      if (exit && orders.length < maxOrders) {
        orders.push({ side: 'sell', mint, token_amount: holding.token_amount, max_slippage_bps: 10000 });
        reserved += txFee;
        pendingSell.add(mint);
      }
    }

    // ---- pyramid adds: up to 2 adds of 0.015 SOL into strength ----
    for (const [mint, holding] of state.positions) {
      if (orders.length >= maxOrders) break;
      if (pendingSell.has(mint) || pendingBuy.has(mint)) continue;
      const info = posInfo.get(mint);
      if (info === undefined || info.isProbe || info.adds >= 2) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const price = currentPrice(state, mint);
      if (price === null) continue;
      // Only add while the price is at least 2% above the last buy's price.
      if (cmpFrac(price.num * 100n, price.den, info.lastBuyPriceNum * 102n, info.lastBuyPriceDen) < 0) continue;
      const win = windowStats(mint, slot, WINDOW_SLOTS);
      if (win === null || win.netInflow <= 0n) continue;
      const cap = state.maxBuyLamports(mint);
      if (cap < MIN_BUY_CAP) continue;
      const size = ADD_BUY < cap ? ADD_BUY : cap;
      if (holding.cost_lamports + size > MAX_STAKE) continue;
      if (!canAffordBuy(size)) continue;
      orders.push({ side: 'buy', mint, quote_lamports: size, max_slippage_bps: 500 });
      reserved += txFee + size;
      info.adds++;
      info.lastBuyPriceNum = price.num;
      info.lastBuyPriceDen = price.den;
      pendingBuy.add(mint);
    }

    // ---- main-signal entries ----
    for (const mint of active.keys()) {
      if (orders.length >= maxOrders) break;
      const lastTrade = active.get(mint)!;
      if (slot - lastTrade > TAPE_KEEP_SLOTS) {
        active.delete(mint);
        continue;
      }
      if (entered.has(mint) || probed.has(mint) || pendingBuy.has(mint)) continue;
      if (state.positions.has(mint)) continue;
      const curve = state.curves.get(mint);
      if (curve === undefined || curve.complete) continue;
      const t = tapes.get(mint);
      if (t === undefined) continue;
      const created = t.createdSlot !== null ? t.createdSlot : t.firstSeenSlot;
      if (slot - created < ENTRY_MIN_AGE) continue;
      if (curve.real_quote_reserves < REAL_MIN || curve.real_quote_reserves > REAL_MAX) continue;
      const win = windowStats(mint, slot, WINDOW_SLOTS);
      if (win === null) continue;
      if (win.buyCount < MIN_BUYS || win.buyers.size < MIN_BUYERS) continue;
      if (win.netInflow < MIN_INFLOW) continue;
      const price = currentPrice(state, mint);
      if (price === null) continue;
      // Up at least 5% vs the window start, and no more than 30%.
      if (cmpFrac(price.num * 100n, price.den, win.startNum * 105n, win.startDen) < 0) continue;
      if (cmpFrac(price.num * 100n, price.den, win.startNum * 130n, win.startDen) > 0) continue;
      // At least 90% of the window high.
      if (cmpFrac(price.num * 100n, price.den, win.highNum * 90n, win.highDen) < 0) continue;
      if (state.committedPositionCount() >= state.hello.caps.max_open_positions) continue;
      const cap = state.maxBuyLamports(mint);
      if (cap < MIN_BUY_CAP) continue;
      const size = FIRST_BUY < cap ? FIRST_BUY : cap;
      if (!canAffordBuy(size)) continue;
      orders.push({ side: 'buy', mint, quote_lamports: size, max_slippage_bps: 500 });
      reserved += txFee + size;
      entered.add(mint);
      lastMainBuySlot = slot;
      pendingBuy.add(mint);
    }

    // ---- probe trades (guardrail insurance): one small probe per batch at most ----
    if (
      roundTrips.size + state.committedPositionCount() < PROBE_ROUND_TRIP_TARGET &&
      state.committedPositionCount() < PROBE_MAX_OPEN &&
      slot - lastMainBuySlot >= MAIN_BUY_QUIET_SLOTS &&
      orders.length < maxOrders
    ) {
      let bestMint: string | null = null;
      let bestReal = -1n;
      for (const mint of active.keys()) {
        if (entered.has(mint) || probed.has(mint) || pendingBuy.has(mint)) continue;
        if (state.positions.has(mint)) continue;
        const curve = state.curves.get(mint);
        if (curve === undefined || curve.complete) continue;
        if (curve.real_quote_reserves < PROBE_REAL_MIN) continue;
        const lastTrade = active.get(mint)!;
        if (slot - lastTrade > 30) continue;
        if (curve.real_quote_reserves > bestReal) {
          bestReal = curve.real_quote_reserves;
          bestMint = mint;
        }
      }
      if (bestMint !== null) {
        const cap = state.maxBuyLamports(bestMint);
        if (cap >= MIN_BUY_CAP) {
          const size = PROBE_BUY < cap ? PROBE_BUY : cap;
          if (canAffordBuy(size)) {
            orders.push({ side: 'buy', mint: bestMint, quote_lamports: size, max_slippage_bps: 500 });
            reserved += txFee + size;
            probed.add(bestMint);
          }
        }
      }
    }

    return orders;
  },
});
