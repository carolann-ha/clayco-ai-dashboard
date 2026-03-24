/**
 * ╔══════════════════════════════════════════════════════════════════════╗
 * ║         CLAYCO AI DASHBOARD — BACKEND SERVER                         ║
 * ╠══════════════════════════════════════════════════════════════════════╣
 * ║                                                                      ║
 * ║  SETUP (one-time):                                                   ║
 * ║    npm install                                                       ║
 * ║                                                                      ║
 * ║  ENV — create a .env file here (see .env.example):                  ║
 * ║    SLACK_BOT_TOKEN                  xoxb-...                        ║
 * ║    SLACK_CHANNEL_IDS                C123,C456                       ║
 * ║    ANTHROPIC_API_KEY                sk-ant-...                      ║
 * ║    PROJECT_INVENTORY_SHEET_URL      https://docs.google.com/...     ║
 * ║    GOOGLE_SERVICE_ACCOUNT_KEY_PATH  ./service-account-key.json      ║
 * ║    GOOGLE_DOC_1_ID                  1ABCdef...                      ║
 * ║    GOOGLE_DOC_2_ID                  1XYZabc...                      ║
 * ║                                                                      ║
 * ║  IMPORTANT: share your Sheet and both Docs with the service          ║
 * ║  account email (found in the key JSON as "client_email").            ║
 * ║  Grant Viewer access to each file.                                   ║
 * ║                                                                      ║
 * ║  RUN:                                                                ║
 * ║    node server.js                                                    ║
 * ║                                                                      ║
 * ║  TEST:                                                               ║
 * ║    curl -X POST http://localhost:3001/api/sync                       ║
 * ║    curl http://localhost:3001/api/inventory                          ║
 * ║    curl http://localhost:3001/api/dashboard/projects                 ║
 * ╚══════════════════════════════════════════════════════════════════════╝
 */

'use strict';
require('dotenv').config();

const fs    = require('fs');
const https = require('https');
const http  = require('http');

const express = require('express');
const cors    = require('cors');
const { WebClient } = require('@slack/web-api');
const Anthropic     = require('@anthropic-ai/sdk');

// googleapis loaded lazily so the server still starts if not yet installed
let googleapis = null;
try {
  googleapis = require('googleapis').google;
} catch {
  console.warn('[startup] "googleapis" not installed. Run: npm install\n  Google Docs + Sheets API will be unavailable until then.');
}

const app  = express();
const PORT = Number(process.env.PORT) || 3001;
app.use(cors());
app.use(express.json());

const slack = new WebClient(process.env.SLACK_BOT_TOKEN);
const ai    = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });


// ═══════════════════════════════════════════════════════════════════
// CONSTANTS
// ═══════════════════════════════════════════════════════════════════

// Canonical lifecycle stages
const CANONICAL_STAGES = ['Scoping', 'Building', 'Live', 'Enablement'];

// Canonical health/status values
const CANONICAL_STATUSES = ['LIVE', 'IN PROGRESS', 'FEEDBACK', 'BLOCKED BY INFRA/SECURITY'];

// Maps canonical status → UI health badge
const STATUS_TO_HEALTH = {
  'LIVE':                       'on-track',
  'IN PROGRESS':                'on-track',
  'FEEDBACK':                   'at-risk',
  'BLOCKED BY INFRA/SECURITY':  'blocked',
};

// Severity ranking for conflict resolution (higher = more urgent)
const HEALTH_SEVERITY = { 'blocked': 3, 'at-risk': 2, 'on-track': 1, 'planning': 0 };

// Explicit blocker keywords — if any match, classify as blocked regardless of LLM status
const BLOCKER_KEYWORDS = [
  'waiting on access', 'waiting on admin', 'pending access', 'access not granted',
  'security review', 'pending security', 'infra issue', 'infrastructure',
  'platform limitation', 'product limitation', 'enterprise plan', 'enterprise limit',
  'blocked by openai', 'blocked by google', 'blocked by microsoft',
  'it approval', 'it review', 'it provisioning', 'okta', 'sso integration',
  'admin approval', 'container access', 'repo access', 'api access', 'provisioning',
  'legal review', 'contract signatory', 'pending legal', 'legal hold',
  'pending it', 'waiting on it', 'hr approval', 'pending hr',
  'no response', 'no eta', 'unanswered', '0 response',
];

// Stage lifecycle priority — used to enforce forward-only advancement
const STAGE_PRIORITY = { scoping: 0, building: 1, live: 2, enablement: 3 };

// Evidence keywords per stage — checked highest-to-lowest so overlapping text resolves correctly
const STAGE_EVIDENCE_KEYWORDS = {
  enablement: [
    'broad rollout', 'org-wide', 'enterprise rollout', 'org wide', 'company-wide',
    'firmwide', 'onboarding at scale', 'enablement program', 'usage tracking',
    'adoption tracking', 'licenses provisioned', 'training rollout', 'scaling adoption',
    'rolling out to', 'full rollout', 'all employees', 'entire org', 'department-wide',
    'adoption program', 'change management', 'organizational rollout', 'training at scale',
  ],
  live: [
    // Strong explicit live phrases (also checked by isStrongLiveEvidence)
    'live and in use', 'live in production', 'live on ', ' is live', 'are live', 'now live',
    'went live', 'has gone live', 'currently live', 'in daily use', 'in active use',
    'pilot underway', 'pilot rollout', 'pilot with ', 'rollout underway',
    'used on real projects', 'deployed and in use', 'in use',
    // Additional live signals
    'in production', 'active use', 'being used', 'in use on real', 'active users',
    'real workflow', 'real output', 'in use by ', 'processing real', 'handling real',
    'has launched', 'launched', 'production use', 'running in production',
    'pilot phase', 'pilot is running', 'pilot active', 'pilot to ', 'in pilot',
    'users are interacting', 'producing output', 'production environment',
    'processing incidents', 'real users', 'processing reports', 'handling requests in',
    // Slack-style live phrases
    'now using', 'users onboarded', 'pilot started', 'active usage', 'deployed',
    'live on projects', 'in use on projects', 'using it on', 'using in',
  ],
  building: [
    // Only CLEARLY pre-launch development work. Do NOT include terms that could describe
    // an already-live solution being refined, maintained, or iterated on.
    'in development', 'under development', 'actively building', 'development underway',
    'build in progress', 'implementation underway', 'being built', 'qa phase',
    'qa underway', 'testing before launch', 'pre-launch', 'integration underway',
    'currently being built', 'model training', 'initial build',
    'architecture complete', 'ready to test', 'pending deployment',
    'pre-production', 'first deployment', 'not yet deployed', 'not yet launched',
    'first launch', 'preparing to launch', 'preparing for launch', 'before launch',
    'before go-live', 'pre-pilot',
  ],
  scoping: [
    'scoping', 'requirements gathering', 'early exploration', 'evaluating approach',
    'defining workflow', 'solution design', 'initial stakeholder', 'discovery phase',
    'vendor evaluation', 'proof of concept', 'planning phase', 'feasibility',
    'evaluating vendors', 'kickoff complete', 'initial research',
    'exploring options', 'assessing feasibility', 'vendor shortlist', 'shortlisted',
    'early concept', 'defining use case', 'concepting', 'early design',
  ],
};

// ── STRONG LIVE PHRASES ─────────────────────────────────────────────────────
// Explicit phrase list used for pre-LLM hardcoded live detection.
// If ANY phrase matches text from the sheet, a doc, or Slack, it is unambiguous Live evidence.
// Checked by isStrongLiveEvidence() before any LLM interpretation.
const STRONG_LIVE_PHRASES = [
  'live and in use', 'live in production', 'live on ', ' is live', 'are live',
  'now live', 'went live', 'has gone live', 'currently live',
  'in use', 'in active use', 'active use', 'being used',
  'pilot underway', 'pilot rollout', 'pilot with ', 'rollout underway',
  'used on real projects', 'deployed and in use', 'launched',
  'in daily use', 'real users', 'active users', 'production use',
  'running in production', 'now using', 'users onboarded', 'pilot started',
  'active usage', 'live on projects', 'in use on projects',
];

/**
 * Returns { found: boolean, snippet: string|null }
 * Checks text against STRONG_LIVE_PHRASES before any LLM runs.
 * Use this as a hard pre-LLM gate on sheet text, doc text, and Slack text.
 */
function isStrongLiveEvidence(text) {
  if (!text) return { found: false, snippet: null };
  const t = text.toLowerCase();
  for (const phrase of STRONG_LIVE_PHRASES) {
    if (t.includes(phrase)) return { found: true, snippet: phrase };
  }
  return { found: false, snippet: null };
}

// Confidence thresholds
const DOC_CONFIDENCE_THRESHOLD   = 0.45;
const SLACK_STATUS_THRESHOLD     = 0.75;
const SLACK_BLOCKER_THRESHOLD    = 0.40;

// Google Sheets column name candidates (case-insensitive, first match wins)
// 'stageInfo' catches combined "Status / Stage Info" columns common in Clayco sheets
const COL_CANDIDATES = {
  name:       ['project name', 'project', 'name', 'title', 'initiative', 'work stream', 'solution'],
  owner:      ['owner', 'lead', 'dri', 'assignee', 'responsible', 'point of contact', 'poc'],
  department: ['department', 'dept', 'team', 'group', 'org', 'bu', 'business unit'],
  stage:      ['stage', 'lifecycle', 'delivery stage', 'current stage', 'phase',
               'status / stage info', 'stage info', 'stage details', 'current state'],
  status:     ['status', 'health', 'state', 'current status', 'rag'],
  milestone:  ['next milestone', 'milestone', 'next step', 'target date', 'due date', 'delivery'],
  priority:   ['priority', 'importance', 'tier'],
  notes:      ['notes', 'description', 'summary', 'comments', 'details', 'status / stage info'],
};


// ═══════════════════════════════════════════════════════════════════
// IN-MEMORY STATE
// ═══════════════════════════════════════════════════════════════════

let STATE = {
  projects:      [],
  blockers:      [],
  signals:       [],
  summary: {
    attention: ['Click Refresh to load live data.'],
    positive:  [],
    horizon:   [],
  },
  lastSync:      null,
  syncStatus:    'idle',

  // Caches — survive failed fetches on next sync
  lastInventory:  [],
  lastDocUpdates: [],

  // Tracks project names mentioned in docs/Slack that don't match the inventory
  // Key = slug of name, value = { name, count, lastSeen }
  unmatchedMentions: {},

  // Per-project live evidence debug data — populated each sync, read by /api/debug/live
  liveDebug: [],
};


// ═══════════════════════════════════════════════════════════════════
// UTILITIES
// ═══════════════════════════════════════════════════════════════════

function timeSince(ms) {
  const diff  = Date.now() - ms;
  const mins  = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days  = Math.floor(diff / 86400000);
  if (mins  <  2) return 'Just now';
  if (mins  < 60) return `${mins}m ago`;
  if (hours < 24) return `${hours}h ago`;
  if (days  ===1) return 'Yesterday';
  return new Date(ms).toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

function toSlug(str) {
  return str.toLowerCase().replace(/[^\w\s-]/g, '').trim()
    .replace(/[\s_]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
}

function toHealth(canonicalStatus) {
  return STATUS_TO_HEALTH[canonicalStatus] || 'on-track';
}

function toCanonicalStatus(raw) {
  if (!raw) return null;
  const s = raw.toUpperCase().trim();
  if (s === 'LIVE')                                     return 'LIVE';
  if (s === 'IN PROGRESS' || s.includes('PROGRESS') || s.includes('ACTIVE') || s.includes('BUILD')) return 'IN PROGRESS';
  if (s === 'FEEDBACK'    || s.includes('FEEDBACK')  || s.includes('REVIEW')) return 'FEEDBACK';
  if (s.includes('BLOCKED') || s.includes('BLOCK'))    return 'BLOCKED BY INFRA/SECURITY';
  if (s.includes('TRACK') || s.includes('GREEN') || s.includes('GOOD'))   return 'IN PROGRESS';
  if (s.includes('RISK')  || s.includes('AMBER') || s.includes('YELLOW')) return 'FEEDBACK';
  if (s.includes('PLAN'))                               return null;
  return null;
}

/**
 * Infer the initial lifecycle stage for a project at inventory load time.
 * Uses explicit phrase matching on the raw sheet stage/status text first,
 * then falls back to canonical status with lower confidence.
 * Returns { stage, confidence, reason }.
 *
 * Phrase priority: Enablement → Live → Building → Scoping
 * IMPORTANT: if stage resolves to Live or higher, initializeProjectMap()
 * will set hasEverBeenLive = true so the forward-only engine preserves it.
 */
function inferInitialStage(canonicalStatus, rawStage) {
  // 1. Sheet stage / "Status / Stage Info" column — highest confidence
  if (rawStage && rawStage.trim()) {
    const s = rawStage.toLowerCase().trim();

    // ── Enablement patterns (check first — superset of live) ────────
    if (/\benabl\w*|\badopt\w*|org.wide rollout|enterprise rollout|org rollout|company.wide|firmwide|scaling adoption|broad rollout|training at scale/.test(s))
      return { stage: 'enablement', confidence: 0.92, reason: `Sheet: "${rawStage}"` };

    // ── Live patterns — explicit phrases + single-word live signal ───
    if (/live and in use|live in production|live on |live with |now live|went live|has gone live|currently live|in production|in active use|pilot rollout|pilot underway|pilot with |pilot is|rollout underway|deployed and in use|launched and in use/.test(s))
      return { stage: 'live', confidence: 0.92, reason: `Sheet: "${rawStage}"` };
    if (/\blive\b|\bpilot\b|\blaunch\w*|\bdeploy\w*|\bproduct\w*/.test(s))
      return { stage: 'live', confidence: 0.90, reason: `Sheet: "${rawStage}"` };

    // ── Building patterns ────────────────────────────────────────────
    if (/build complete|deployment in progress|active development|initial build|in development|under development|being built|build in progress|implementation underway|in progress|actively building/.test(s))
      return { stage: 'building', confidence: 0.88, reason: `Sheet: "${rawStage}"` };
    if (/\bbuild\w*|\bdev\w*|\bimpl\w*|\bprogress\b/.test(s))
      return { stage: 'building', confidence: 0.82, reason: `Sheet: "${rawStage}"` };

    // ── Scoping patterns ─────────────────────────────────────────────
    if (/\bscop\w*|\bplan\w*|\bdiscov\w*|\bresearch\b|\bconcept\w*|\bexplor\w*|\bassess\w*|\bfeasib\w*|\bevaluat\w*/.test(s))
      return { stage: 'scoping', confidence: 0.85, reason: `Sheet: "${rawStage}"` };

    // Fall through to keyword scanner for long freeform text
    const flags = extractEvidenceFlags(rawStage);
    if (flags.enablement) return { stage: 'enablement', confidence: 0.85, reason: `Sheet evidence: "${flags.snippets[0] || rawStage}"` };
    if (flags.live)       return { stage: 'live',       confidence: 0.85, reason: `Sheet evidence: "${flags.snippets[0] || rawStage}"` };
    if (flags.building)   return { stage: 'building',   confidence: 0.80, reason: `Sheet evidence: "${flags.snippets[0] || rawStage}"` };

    return { stage: 'scoping', confidence: 0.30, reason: `Sheet stage unrecognised: "${rawStage}" — defaulting to Scoping` };
  }

  // 2. Fall back to canonical status (lower confidence — status ≠ stage)
  if (!canonicalStatus) return { stage: 'scoping', confidence: 0.20, reason: 'No stage data — defaulting to Scoping' };
  switch (canonicalStatus) {
    case 'LIVE':                      return { stage: 'live',       confidence: 0.70, reason: 'Inferred from LIVE status' };
    case 'IN PROGRESS':               return { stage: 'building',   confidence: 0.50, reason: 'Inferred from IN PROGRESS status' };
    case 'FEEDBACK':                  return { stage: 'enablement', confidence: 0.50, reason: 'Inferred from FEEDBACK status' };
    case 'BLOCKED BY INFRA/SECURITY': return { stage: 'building',   confidence: 0.40, reason: 'Inferred — blocked before launch, assumed Building' };
    default:                          return { stage: 'scoping',    confidence: 0.20, reason: 'Default stage — no data' };
  }
}

/**
 * Classifies which department is causing a blocker based on keywords in the blocker text.
 * Returns the responsible department name (or 'Unassigned').
 */
function inferBlockingDepartment(blockerText) {
  if (!blockerText) return 'Unassigned';
  const t = blockerText.toLowerCase();

  if (/\bokta\b|sso|okta|saml|idp|\bprovisioning\b|container access|repo access|api access|enterprise plan|\bit\b.*approv|\bit\b.*review|pending it|waiting on it|infra|data access|admin request|access request|procore admin|bim access/.test(t))
    return 'IT';
  if (/security review|compliance|soc\b|data security|pen test|penetration|vulnerability|sec review/.test(t))
    return 'Security';
  if (/legal review|contract|signator|legal hold|counsel|attorney|liability|legal sign/.test(t))
    return 'Legal';
  if (/procurement team|vendor database|supplier|procurement bandwidth/.test(t))
    return 'Procurement';
  if (/\bhr\b|human resources|hiring|headcount/.test(t))
    return 'HR';
  if (/\bfinance\b|budget approval|cost approval/.test(t))
    return 'Finance';
  if (/openai|anthropic|microsoft|google workspace|third.party vendor|platform limit|saas/.test(t))
    return 'Vendor / Platform';
  if (/exec|leadership|sign.off|executive approval|c-suite/.test(t))
    return 'Executive';
  if (/data.*not available|data.*missing|missing data|historical data|data request/.test(t))
    return 'IT';

  return 'Unassigned';
}

/**
 * Infers the project department from update text when the sheet says 'Unassigned'.
 * Returns a department string or null if no signal found.
 * NEVER exposes individual owner names — only used to infer group.
 */
function inferDepartmentFromContext(text) {
  if (!text) return null;
  const t = text.toLowerCase();
  if (/\bpreconstruction\b|estimat\w+|bid prep|\brfp\b|\brfi\b|\bsubmittal\b|takeoff|bid\s+doc/.test(t)) return 'Preconstruction';
  if (/\bsafety\b|\bosha\b|incident report|near.miss|field safety/.test(t))                               return 'Safety';
  if (/\bbim\b|\brevit\b|navisworks|clash detection|model coordin/.test(t))                                return 'Technology';
  if (/\boperations\b|field ops|superintendent|field team|project execution/.test(t))                     return 'Operations';
  if (/\bhr\b|human resources|recruit\w+|\bonboard\w+|\bhiring\b|headcount/.test(t))                      return 'HR';
  if (/\blegal\b|\bcontract\w*\b|counsel|attorney|liability\b/.test(t))                                   return 'Legal';
  if (/\bprocurement\b|\bvendor\b|\bsupplier\b|\bpurchas\w+/.test(t))                                     return 'Procurement';
  if (/\bfinance\b|accounting|\bbudget\b|cost approv/.test(t))                                            return 'Finance';
  if (/\bexecutive\b|senior leadership|c.suite|\bceo\b|\bcoo\b|\bcfo\b/.test(t))                          return 'Executive';
  if (/\bit\b|infra\w*|infrastructure|systems admin|devops|provisioning/.test(t))                         return 'IT';
  return null;
}

/**
 * Tracks project names mentioned in docs/Slack that don't match the inventory.
 * Sends a Slack alert to PM_ALERT_CHANNEL_ID when a name is seen ≥2 times.
 * This helps detect projects that should be added to the inventory sheet.
 */
async function trackUnmatchedProject(name, source, syncMs) {
  const key = toSlug(name || '');
  if (!key || key.length < 3) return;
  if (!STATE.unmatchedMentions[key]) {
    STATE.unmatchedMentions[key] = { name, count: 0, lastSeen: null };
  }
  STATE.unmatchedMentions[key].count++;
  STATE.unmatchedMentions[key].lastSeen = syncMs;

  const { count } = STATE.unmatchedMentions[key];
  if (count >= 2 && process.env.PM_ALERT_CHANNEL_ID) {
    try {
      await slack.chat.postMessage({
        channel: process.env.PM_ALERT_CHANNEL_ID,
        text: `⚠️ *Unmatched AI project mention:* "${name}" (seen ${count}× in ${source}). ` +
              `Is this a new project that should be added to the inventory sheet?`,
      });
    } catch (err) {
      console.warn(`  [unmatched-alert] Could not post to PM_ALERT_CHANNEL_ID: ${err.message}`);
    }
  }
}

/**
 * Determines if blocker text contains explicit infra/security/tooling dependency language.
 * Used as a hard override — if this returns true, status = blocked regardless of LLM confidence.
 */
function isExplicitBlocker(text) {
  if (!text) return false;
  const t = text.toLowerCase();
  return BLOCKER_KEYWORDS.some(kw => t.includes(kw));
}

/**
 * ── LAYER 1: EVIDENCE EXTRACTION ─────────────────────────────────────────
 * Scans text for stage evidence keywords across ALL four stages simultaneously.
 * Returns a flag set — NOT a single stage classification.
 * This deliberately avoids picking a "winner"; the deterministic engine does that.
 *
 * Key difference from the old classifyStageFromText():
 *  - Old: returned the HIGHEST matching stage (caused demotion when live text was absent)
 *  - New: returns ALL matching flags; deterministic engine + durable history flags decide stage
 */
/**
 * LAYER 1 — keyword-based evidence scanner.
 * Checks ALL four stage keyword arrays and returns a flag set.
 * Uses the canonical { live, enablement, building, scoping, blocked } shape
 * that matches proj.evidence and the LLM extraction output.
 */
function extractEvidenceFlags(text) {
  if (!text) return { live: false, enablement: false, building: false, scoping: false, blocked: false, snippets: [] };
  const t = text.toLowerCase();
  let enablement = false, live = false, building = false, scoping = false;
  const snippets = [];

  for (const kw of STAGE_EVIDENCE_KEYWORDS.enablement) {
    if (t.includes(kw)) { enablement = true; snippets.push(kw); }
  }
  for (const kw of STAGE_EVIDENCE_KEYWORDS.live) {
    if (t.includes(kw)) { live = true; snippets.push(kw); }
  }
  // Hard gate: if isStrongLiveEvidence fires, live = true regardless of keyword matches above
  const strongLive = isStrongLiveEvidence(text);
  if (strongLive.found) { live = true; if (strongLive.snippet) snippets.push(strongLive.snippet); }

  for (const kw of STAGE_EVIDENCE_KEYWORDS.building) {
    if (t.includes(kw)) { building = true; snippets.push(kw); }
  }
  for (const kw of STAGE_EVIDENCE_KEYWORDS.scoping) {
    if (t.includes(kw)) { scoping = true; snippets.push(kw); }
  }
  // Keyword scanner never detects blocked — that comes from isExplicitBlocker()
  const blocked = isExplicitBlocker(text);

  return {
    live,
    enablement,
    building,
    scoping,
    blocked,
    snippets: [...new Set(snippets)].slice(0, 5),
  };
}

/**
 * ── LAYER 1 (accumulation): Merge evidence flags into a project's running evidence tally.
 * Called once per source per project. Stage is NOT determined here.
 * determineStage() is called once after ALL sources have been accumulated.
 *
 * @param {string} source — 'doc' | 'slack'
 */
function accumulateEvidence(proj, flags, source, evidenceText, syncMs) {
  // OR all flags into the project's running evidence tally
  proj.evidence.live       = proj.evidence.live       || !!(flags.live);
  proj.evidence.enablement = proj.evidence.enablement || !!(flags.enablement);
  proj.evidence.building   = proj.evidence.building   || !!(flags.building);
  proj.evidence.scoping    = proj.evidence.scoping    || !!(flags.scoping);
  proj.evidence.blocked    = proj.evidence.blocked    || !!(flags.blocked);

  // Track per-source live evidence for debug endpoint + logging
  if (flags.live) {
    if (source === 'doc')   proj._liveEvidenceFromDocs  = true;
    if (source === 'slack') proj._liveEvidenceFromSlack = true;
  }

  // Track provenance: timestamp + which sources have contributed
  const hadMeaningful = flags.live || flags.enablement || flags.building || flags.blocked;
  if (hadMeaningful) {
    proj.lastEvidenceTimestamp = syncMs;
    if (proj.stageSource === 'sheet') {
      proj.stageSource = source;
    } else if (proj.stageSource !== source && proj.stageSource !== 'combined') {
      proj.stageSource = 'combined';
    }
  }

  // Collect evidence snippets for the debug panel
  const snippets = (flags.snippets || []);
  if (evidenceText) {
    const excerpt = evidenceText.slice(0, 140).trim();
    if (!proj.stageEvidence.includes(excerpt)) proj.stageEvidence.push(excerpt);
  }
  snippets.forEach(s => { if (!proj.stageEvidence.includes(s)) proj.stageEvidence.push(s); });
}


/**
 * ── LAYER 2 (determination): Deterministic stage engine.
 * Called ONCE per project AFTER all evidence has been accumulated.
 * Uses durable history flags + per-sync evidence accumulator.
 * Returns the resolved stage string (lowercase). Never moves backward.
 *
 * Priority order (explicit per spec):
 *   1. hasEverBeenEnabled  → Enablement (locked forever)
 *   2. hasEverBeenLive     → Live (or Enablement if new enablement evidence)
 *   3. evidence.enablement → Enablement (first time)
 *   4. evidence.live       → Live (first time; pilot counts as Live)
 *   5. evidence.building   → Building (only from Scoping; never demotes Live)
 *   6. default             → Scoping (or current stage if already higher)
 */
function determineStage(proj, syncMs) {
  const ev = proj.evidence || {};

  // ── Rule 1: hasEverBeenEnabled → always Enablement ──────────────────
  if (proj.hasEverBeenEnabled) {
    proj.stageConfidence = 0.97;
    proj.stageReason     = 'Durable: reached Enablement — stage locked forward';
    return 'enablement';
  }

  // ── Rule 2: hasEverBeenLive → at minimum Live ────────────────────────
  // Can still advance to Enablement if enablement evidence found this sync
  if (proj.hasEverBeenLive) {
    if (ev.enablement) {
      proj.hasEverBeenEnabled        = true;
      proj.firstEnablementEvidenceAt = proj.firstEnablementEvidenceAt || syncMs;
      proj.stageConfidence           = 0.93;
      proj.stageReason               = `Enablement evidence on Live project (${proj.stageSource})`;
      return 'enablement';
    }
    proj.stageConfidence = 0.92;
    proj.stageReason     = 'Durable: project was Live — cannot regress to Building or Scoping';
    return 'live';
  }

  // ── Rule 3: Enablement evidence (first time project reaches Enablement) ──
  if (ev.enablement) {
    proj.hasEverBeenEnabled        = true;
    proj.hasEverBeenLive           = true;
    proj.firstLiveEvidenceAt       = proj.firstLiveEvidenceAt       || syncMs;
    proj.firstEnablementEvidenceAt = proj.firstEnablementEvidenceAt || syncMs;
    proj.stageConfidence           = 0.90;
    proj.stageReason               = `Enablement evidence detected (${proj.stageSource})`;
    return 'enablement';
  }

  // ── Rule 4: Live evidence (pilot = Live, deployed = Live, in-use = Live) ──
  if (ev.live) {
    proj.hasEverBeenLive     = true;
    proj.firstLiveEvidenceAt = proj.firstLiveEvidenceAt || syncMs;
    proj.stageConfidence     = 0.88;
    proj.stageReason         = `Live evidence detected (${proj.stageSource})`;
    return 'live';
  }

  // ── Rule 5: Building evidence — only advance from Scoping ────────────
  // A project that was already Live stays Live (guarded by Rules 1-4 above).
  if (ev.building) {
    if (STAGE_PRIORITY[proj.stage] < STAGE_PRIORITY.building) {
      proj.stageConfidence = 0.75;
      proj.stageReason     = `Building evidence detected (${proj.stageSource})`;
      return 'building';
    }
    // Already at building or higher from sheet — keep current stage
    return proj.stage;
  }

  // ── Rule 6: No meaningful evidence — fall back to sheet baseline ─────
  // Keep whatever the sheet said. If sheet had nothing, default to Scoping.
  // Confidence stays at sheet-baseline level (≤0.60).
  return proj.stage || 'scoping';
}

function detectColumns(headers) {
  const cols = {};
  for (const [field, candidates] of Object.entries(COL_CANDIDATES)) {
    cols[field] = headers.find(h => candidates.includes(h.toLowerCase().trim())) || null;
  }
  return cols;
}

// Match an AI-returned project name back to the inventory
function matchToInventory(extractedName, inventory) {
  if (!extractedName) return null;
  const n = extractedName.trim();
  return (
    inventory.find(p => p.name === n) ||
    inventory.find(p => p.name.toLowerCase() === n.toLowerCase()) ||
    inventory.find(p => p.id === toSlug(n)) ||
    inventory.find(p => p.name.toLowerCase().includes(n.toLowerCase()) ||
                        n.toLowerCase().includes(p.name.toLowerCase())) ||
    null
  );
}

// Simple concurrency limiter
async function withConcurrency(fns, limit = 3) {
  const results = [];
  const pool    = [...fns];
  async function run() {
    while (pool.length) {
      const fn = pool.shift();
      try { results.push(await fn()); }
      catch { results.push(null); }
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, fns.length) }, () => run()));
  return results;
}

// Safe JSON parse — strips markdown fences the model may add
function safeParseJSON(text) {
  try {
    const clean = text.trim()
      .replace(/^```(?:json)?\n?/, '')
      .replace(/\n?```$/, '')
      .trim();
    return JSON.parse(clean);
  } catch {
    return null;
  }
}


// ═══════════════════════════════════════════════════════════════════
// HTTP HELPER (no extra dep, follows redirects — needed for CSV)
// ═══════════════════════════════════════════════════════════════════

function httpGet(url, redirectsLeft = 8) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, { timeout: 12000 }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        if (redirectsLeft === 0) return reject(new Error('Too many redirects'));
        return resolve(httpGet(res.headers.location, redirectsLeft - 1));
      }
      if (res.statusCode !== 200) return reject(new Error(`HTTP ${res.statusCode} from ${url}`));
      let body = '';
      res.setEncoding('utf8');
      res.on('data', c => { body += c; });
      res.on('end', () => resolve(body));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('Timed out: ' + url)); });
  });
}


// ═══════════════════════════════════════════════════════════════════
// CSV PARSER
// ═══════════════════════════════════════════════════════════════════

function parseCSV(text) {
  const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n').filter(l => l.trim());
  if (lines.length < 2) return [];

  function parseLine(line) {
    const fields = [];
    let i = 0;
    while (i <= line.length) {
      if (i === line.length) { fields.push(''); break; }
      if (line[i] === '"') {
        i++;
        let field = '';
        while (i < line.length) {
          if (line[i] === '"' && line[i + 1] === '"') { field += '"'; i += 2; }
          else if (line[i] === '"') { i++; break; }
          else { field += line[i++]; }
        }
        fields.push(field.trim());
        if (line[i] === ',') i++;
      } else {
        const comma = line.indexOf(',', i);
        if (comma === -1) { fields.push(line.slice(i).trim()); break; }
        fields.push(line.slice(i, comma).trim());
        i = comma + 1;
      }
    }
    return fields;
  }

  const headers = parseLine(lines[0]).map(h => h.toLowerCase().trim());
  return lines.slice(1).map(line => {
    const vals = parseLine(line);
    const row  = {};
    headers.forEach((h, i) => { row[h] = (vals[i] || '').trim(); });
    return row;
  }).filter(row => Object.values(row).some(v => v.length > 0));
}

function toCSVExportUrl(rawUrl) {
  const url = rawUrl.trim();
  if (url.includes('export?format=csv') || url.includes('pub?output=csv')) return url;
  const idMatch = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
  if (!idMatch) throw new Error(`Cannot parse spreadsheet ID from: "${url}"`);
  const gidMatch = url.match(/[#&?]gid=(\d+)/);
  return `https://docs.google.com/spreadsheets/d/${idMatch[1]}/export?format=csv${gidMatch ? '&gid=' + gidMatch[1] : ''}`;
}

function rowsToInventory(rows) {
  if (!rows.length) return [];
  const cols = detectColumns(Object.keys(rows[0]));
  if (!cols.name) return [];
  return rows.map(row => {
    const name = row[cols.name]?.trim();
    if (!name) return null;
    return {
      id:         toSlug(name),
      name,
      owner:      cols.owner      ? (row[cols.owner]      || '') : '',
      department: cols.department ? (row[cols.department]  || '') : '',
      stage:      cols.stage      ? (row[cols.stage]       || '') : '',
      status:     cols.status     ? (row[cols.status]      || '') : '',
      milestone:  cols.milestone  ? (row[cols.milestone]   || '') : '',
      priority:   cols.priority   ? (row[cols.priority]    || '') : '',
      notes:      cols.notes      ? (row[cols.notes]       || '') : '',
    };
  }).filter(Boolean);
}


// ═══════════════════════════════════════════════════════════════════
// GOOGLE AUTH  (service account — no browser OAuth)
// ═══════════════════════════════════════════════════════════════════

async function getGoogleAuth() {
  if (!googleapis) {
    console.warn('[auth] googleapis not available.');
    return null;
  }

  let credentials = null;

  if (process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH) {
    try {
      const raw = fs.readFileSync(process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH, 'utf8');
      credentials = JSON.parse(raw);
    } catch (err) {
      console.error('[auth] Could not read GOOGLE_SERVICE_ACCOUNT_KEY_PATH:', err.message);
    }
  }

  if (!credentials && process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
    try {
      credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
    } catch (err) {
      console.error('[auth] Could not parse GOOGLE_SERVICE_ACCOUNT_JSON:', err.message);
    }
  }

  if (!credentials) {
    console.warn('[auth] No service account credentials found. Set GOOGLE_SERVICE_ACCOUNT_KEY_PATH in .env');
    return null;
  }

  const auth = new googleapis.auth.GoogleAuth({
    credentials,
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets.readonly',
      'https://www.googleapis.com/auth/documents.readonly',
    ],
  });

  return auth;
}


// ═══════════════════════════════════════════════════════════════════
// LOAD PROJECT INVENTORY  (Sheet → service account API → CSV fallback)
// ═══════════════════════════════════════════════════════════════════

async function loadProjectInventory(auth) {
  if (auth && process.env.PROJECT_INVENTORY_SHEET_URL) {
    try {
      return await loadInventoryViaAPI(auth);
    } catch (err) {
      console.warn(`  [inventory] Sheets API failed (${err.message}), trying CSV fallback…`);
    }
  }

  if (process.env.PROJECT_INVENTORY_SHEET_URL) {
    try {
      return await loadInventoryViaCSV();
    } catch (err) {
      console.warn(`  [inventory] CSV fetch failed: ${err.message}`);
    }
  }

  if (STATE.lastInventory.length) {
    console.warn('  [inventory] Using cached inventory from last sync.');
    return STATE.lastInventory;
  }

  console.warn('  [inventory] Using mock inventory fallback.');
  return MOCK_INVENTORY;
}

async function loadInventoryViaAPI(auth) {
  const sheets = googleapis.sheets({ version: 'v4', auth });
  const url    = process.env.PROJECT_INVENTORY_SHEET_URL;

  const idMatch = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9_-]+)/);
  if (!idMatch) throw new Error('Cannot parse spreadsheet ID from URL');
  const spreadsheetId = idMatch[1];

  let range = 'A:Z';
  const gidMatch = url.match(/[#&?]gid=(\d+)/);
  if (gidMatch) {
    const meta  = await sheets.spreadsheets.get({ spreadsheetId });
    const sheet = meta.data.sheets?.find(s => s.properties.sheetId === parseInt(gidMatch[1]));
    if (sheet) range = `'${sheet.properties.title}'!A:Z`;
  }

  const resp = await sheets.spreadsheets.values.get({ spreadsheetId, range });
  const vals = resp.data.values || [];
  if (vals.length < 2) return [];

  const headers = vals[0].map(h => h.toString().toLowerCase().trim());
  const rows    = vals.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => { obj[h] = (row[i] || '').toString().trim(); });
    return obj;
  }).filter(row => Object.values(row).some(v => v.length > 0));

  const inventory = rowsToInventory(rows);
  console.log(`  [inventory] Loaded ${inventory.length} projects via Sheets API.`);
  return inventory;
}

async function loadInventoryViaCSV() {
  const csvUrl = toCSVExportUrl(process.env.PROJECT_INVENTORY_SHEET_URL);
  const text   = await httpGet(csvUrl);
  const rows   = parseCSV(text);
  const inventory = rowsToInventory(rows);
  console.log(`  [inventory] Loaded ${inventory.length} projects via CSV.`);
  return inventory;
}

// Last-resort mock (only if sheet + cache both unavailable)
const MOCK_INVENTORY = [
  { id: 'ai-cost-estimating-engine',      name: 'AI Cost Estimating Engine',              owner: '', department: 'Preconstruction', stage: '', status: '', milestone: '', priority: '', notes: '' },
  { id: 'document-intelligence',          name: 'Document Intelligence (RFI / Submittal)', owner: '', department: 'Project Controls', stage: '', status: '', milestone: '', priority: '', notes: '' },
  { id: 'schedule-risk-predictor',        name: 'Schedule Risk Predictor',                owner: '', department: 'Project Controls', stage: '', status: '', milestone: '', priority: '', notes: '' },
  { id: 'safety-incident-classifier',     name: 'Safety Incident Classifier',             owner: '', department: 'Safety', stage: 'Live', status: 'LIVE', milestone: '', priority: '', notes: '' },
  { id: 'bim-data-extraction-pipeline',   name: 'BIM Data Extraction Pipeline',           owner: '', department: 'Technology', stage: '', status: 'BLOCKED BY INFRA/SECURITY', milestone: '', priority: '', notes: '' },
  { id: 'executive-reporting-automation', name: 'Executive Reporting Automation',         owner: '', department: 'Executive', stage: '', status: '', milestone: '', priority: '', notes: '' },
  { id: 'contract-signatory-ai',          name: 'Contract Signatory AI',                  owner: '', department: 'Legal', stage: '', status: 'BLOCKED BY INFRA/SECURITY', milestone: '', priority: '', notes: '' },
  { id: 'rfp-assistant',                  name: 'RFP Assistant',                          owner: '', department: 'Preconstruction', stage: '', status: 'BLOCKED BY INFRA/SECURITY', milestone: '', priority: '', notes: '' },
  { id: 'prime-contract-intelligence',    name: 'Prime Contract Intelligence',            owner: '', department: 'Preconstruction', stage: '', status: 'BLOCKED BY INFRA/SECURITY', milestone: '', priority: '', notes: '' },
];


// ═══════════════════════════════════════════════════════════════════
// GOOGLE DOCS — STRUCTURED EXTRACTION
// ═══════════════════════════════════════════════════════════════════

/**
 * Extracts structured content from a Google Doc body.
 * Returns:
 *   sections — array of { heading: string|null, text: string }
 *   tables   — array of string[][] (each table as rows of cell-text arrays)
 *
 * Tables are preserved as grids for project-status table detection (DOC 2 / Weekly Wrap).
 * Table cell text is ALSO appended to the sections stream for fallback coverage.
 */
function extractDocStructured(docBody) {
  const sections = [];   // [{ heading, text }]
  const tables   = [];   // [ [[cell…], [cell…]], … ]

  let currentHeading = null;
  let currentLines   = [];

  function flush() {
    const text = [currentHeading, ...currentLines].filter(Boolean).join('\n').trim();
    if (text.length >= 20) sections.push({ heading: currentHeading, text });
    currentHeading = null;
    currentLines   = [];
  }

  function textFromElements(elements) {
    return (elements || [])
      .map(el => el.textRun?.content || '')
      .join('')
      .replace(/\n$/, '')
      .trim();
  }

  function processContent(content, inTable = false) {
    for (const block of (content || [])) {
      if (block.paragraph) {
        const style = block.paragraph.paragraphStyle?.namedStyleType || 'NORMAL_TEXT';
        const text  = textFromElements(block.paragraph.elements);
        if (!text) continue;

        if (!inTable && (style === 'TITLE' || style.startsWith('HEADING_'))) {
          flush();
          currentHeading = text;
        } else {
          currentLines.push(text);
        }
      } else if (block.table) {
        // ── Structured table extraction ──────────────────────────────
        const tableGrid = [];
        for (const tableRow of (block.table.tableRows || [])) {
          const cells = (tableRow.tableCells || []).map(cell =>
            (cell.content || [])
              .flatMap(b => b.paragraph ? [textFromElements(b.paragraph.elements)] : [])
              .filter(Boolean)
              .join(' ')
              .trim()
          );
          if (cells.some(c => c.length > 0)) tableGrid.push(cells);
        }
        if (tableGrid.length >= 2) tables.push(tableGrid);

        // ── Also feed table text into sections stream (fallback) ─────
        for (const tableRow of (block.table.tableRows || [])) {
          for (const cell of (tableRow.tableCells || [])) {
            processContent(cell.content, true);
          }
        }
      }
    }
  }

  processContent(docBody?.content);
  flush();
  return { sections, tables };
}

async function loadGoogleDocStructured(auth, docId) {
  if (!googleapis || !auth) throw new Error('googleapis or auth not available');
  const docs = googleapis.docs({ version: 'v1', auth });
  const resp = await docs.documents.get({ documentId: docId });
  const doc  = resp.data;
  const { sections, tables } = extractDocStructured(doc.body);
  console.log(`    [doc:${docId.slice(0, 8)}…] "${doc.title}" — ${sections.length} sections, ${tables.length} table(s) extracted`);
  return { title: doc.title, sections, tables };
}


// ═══════════════════════════════════════════════════════════════════
// LLM: GOOGLE DOC EXTRACTION — TABLE MODE (DOC 2 / Weekly Wrap)
// ═══════════════════════════════════════════════════════════════════

/**
 * Batch-extracts project updates from a structured table (DOC 2 — Weekly Wrap).
 * Sends the ENTIRE table in ONE LLM call. Returns array of project update objects.
 * This is the PRIMARY, high-signal extraction path.
 *
 * Expected table structure (flexible — columns detected by LLM):
 *   Row 0 (header): "Project" | "Stage/Status" | "Notes" | ...
 *   Row N (data):   "AI Cost Estimating" | "Live" | "Deployed to 5 projects" | ...
 */
async function extractProjectsFromDocTable(tables, inventory, docId) {
  const results = [];
  const projectList = inventory.map(p => `- ${p.name}`).join('\n');

  for (const tableGrid of tables) {
    if (tableGrid.length < 2) continue;

    // Build readable table representation
    const tableText = tableGrid.map((row, i) =>
      `Row ${i === 0 ? '(header)' : i}: ${row.filter(Boolean).join(' | ')}`
    ).join('\n');

    // Skip tables that clearly aren't project status tables
    // (< 2 columns or too short to be meaningful)
    if (tableGrid[0].length < 2 || tableText.length < 80) continue;

    const prompt =
`You are extracting project status evidence from a table in a weekly AI program recap.
Stage is determined by code — return ONLY evidence flags, never a final stage.

Known projects:
${projectList}

Table:
${tableText}

For each data row (not the header), if it matches a known project, extract evidence.

Evidence flag definitions:
- live: project is deployed, in use, in production, pilot running with real users (PILOT = LIVE)
  A project being refined, maintained, or iterated post-launch is STILL live.
- enablement: already live AND scaling org-wide — broad rollout, company-wide adoption
- building: EXPLICITLY pre-launch — in development, not yet deployed, pending first launch
  Do NOT set if project is live and being refined.
- scoping: early concept, requirements, or vendor evaluation phase
- blocked: explicitly waiting on IT/security/legal/access/provisioning

Return JSON array only — empty array if no rows match known projects:
[
  {
    "project": "exact name from known projects list",
    "evidence": { "live": boolean, "enablement": boolean, "building": boolean, "scoping": boolean, "blocked": boolean },
    "evidenceText": "key phrase ≤25 words",
    "confidence": number
  }
]`;

    try {
      const resp = await ai.messages.create({
        model: 'claude-haiku-4-5-20251001', max_tokens: 900,
        messages: [{ role: 'user', content: prompt }],
      });
      const arr = safeParseJSON(resp.content[0].text);
      if (!Array.isArray(arr)) continue;

      for (const obj of arr) {
        if (!obj?.project) continue;
        const matched = matchToInventory(obj.project, inventory);
        if (!matched) {
          console.log(`    [doc:table] No inventory match for "${obj.project}" — skipped.`);
          await trackUnmatchedProject(obj.project, 'google_doc', Date.now());
          continue;
        }
        const ev = obj.evidence || {};
        const hardBlocked = isExplicitBlocker(obj.evidenceText || '');
        results.push({
          project:     matched.name,
          projectId:   matched.id,
          evidence: {
            live:       !!(ev.live),
            enablement: !!(ev.enablement),
            building:   !!(ev.building),
            scoping:    !!(ev.scoping),
            blocked:    !!(ev.blocked) || hardBlocked,
          },
          evidenceText: obj.evidenceText || '',
          confidence:   typeof obj.confidence === 'number' ? obj.confidence : 0.65,
          source:       'google_doc',
          sourceDocId:  docId,
        });
      }
    } catch (err) {
      console.warn(`    [doc:table] Extraction error: ${err.message}`);
    }
  }

  return results;
}


// ═══════════════════════════════════════════════════════════════════
// LLM: GOOGLE DOC EXTRACTION — SECTION MODE (DOC 1 / Weekly Kickoff)
// ═══════════════════════════════════════════════════════════════════

/**
 * Extracts project updates from the N most recent date-labelled sections.
 * Sends them BATCHED in ONE or TWO LLM calls — NOT one per section.
 * Sections are identified by date-like headings (Week of…, March 18…, etc.)
 *
 * maxSections controls how many recent date blocks to include.
 * Default = 2 (current week + prior week for continuity).
 */
async function extractFromRecentSections(sections, inventory, docId, maxSections = 2) {
  const results = [];
  if (!sections.length) return results;

  const DATE_HEADING_RE = /week\s+of|week\s+\d+|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\.?\s+\d{1,2}|\d{1,2}[\/\-]\d{1,2}/i;

  // Find sections with date-like headings; take the most recent N
  const dateBlocks = sections.filter(s => s.heading && DATE_HEADING_RE.test(s.heading));
  const toProcess  = dateBlocks.length > 0
    ? dateBlocks.slice(-maxSections)
    : sections.slice(-maxSections);  // fallback: last N sections if no date headings

  const combinedText = toProcess
    .map(b => `=== ${b.heading || 'Section'} ===\n${b.text}`)
    .join('\n\n')
    .slice(0, 5000);  // hard cap to avoid token overflow

  if (!combinedText.trim()) return results;

  const projectList = inventory.map(p => `- ${p.name}`).join('\n');

  const prompt =
`You are extracting project evidence from a weekly AI program kickoff document.
Stage is determined by code — return ONLY evidence flags, never a final stage.

Known projects:
${projectList}

Document excerpt (most recent ${toProcess.length} section(s)):
"""
${combinedText}
"""

For each known project mentioned, extract evidence flags.
Evidence definitions:
- live: deployed, in production, in use, pilot running — PILOT = LIVE
- enablement: live AND scaling org-wide — broad rollout, adoption programs
- building: explicitly pre-launch — in development, not yet launched
- scoping: early concept or requirements phase
- blocked: waiting on IT/security/legal/access/provisioning

Return JSON array — only include projects from the known list:
[
  {
    "project": "exact name from known projects",
    "evidence": { "live": boolean, "enablement": boolean, "building": boolean, "scoping": boolean, "blocked": boolean },
    "evidenceText": "key phrase ≤25 words",
    "confidence": number
  }
]
Return [] if no known projects are clearly mentioned.`;

  try {
    const resp = await ai.messages.create({
      model: 'claude-haiku-4-5-20251001', max_tokens: 1000,
      messages: [{ role: 'user', content: prompt }],
    });
    const arr = safeParseJSON(resp.content[0].text);
    if (!Array.isArray(arr)) return results;

    for (const obj of arr) {
      if (!obj?.project) continue;
      const matched = matchToInventory(obj.project, inventory);
      if (!matched) {
        console.log(`    [doc:sections] No inventory match for "${obj.project}" — skipped.`);
        await trackUnmatchedProject(obj.project, 'google_doc', Date.now());
        continue;
      }
      const ev = obj.evidence || {};
      const hardBlocked = isExplicitBlocker(obj.evidenceText || '');
      results.push({
        project:     matched.name,
        projectId:   matched.id,
        evidence: {
          live:       !!(ev.live),
          enablement: !!(ev.enablement),
          building:   !!(ev.building),
          scoping:    !!(ev.scoping),
          blocked:    !!(ev.blocked) || hardBlocked,
        },
        evidenceText: obj.evidenceText || '',
        confidence:   typeof obj.confidence === 'number' ? obj.confidence : 0.55,
        source:       'google_doc',
        sourceDocId:  docId,
      });
    }
  } catch (err) {
    console.warn(`    [doc:sections] Extraction error: ${err.message}`);
  }

  return results;
}


// ═══════════════════════════════════════════════════════════════════
// LOAD GOOGLE DOC UPDATES  (smart routing: table vs sections)
// ═══════════════════════════════════════════════════════════════════

/**
 * Loads and extracts project updates from both Google Docs.
 *
 * DOC 2 (GOOGLE_DOC_2_ID) = Weekly Wrap / AI Solutions Status table
 *   → TABLE MODE: batch-extract all project rows in ONE LLM call
 *   → This is the PRIMARY structured source
 *
 * DOC 1 (GOOGLE_DOC_1_ID) = Weekly Kickoff
 *   → SECTION MODE: find latest 1-2 date sections, extract in ONE LLM call
 *   → Provides directional context (objectives, risks, dependencies)
 *
 * Both docs are processed with at most 2 LLM calls each — NOT one per section.
 */
async function loadGoogleDocsUpdates(auth, inventory) {
  const docConfigs = [
    { id: process.env.GOOGLE_DOC_1_ID, role: 'kickoff' },  // Section-based: weekly kickoff / objectives
    { id: process.env.GOOGLE_DOC_2_ID, role: 'wrap'    },  // Table-based:   weekly wrap / status table (PRIMARY)
  ].filter(d => d.id);

  if (!docConfigs.length) {
    console.log('  [docs] No GOOGLE_DOC_1_ID / GOOGLE_DOC_2_ID configured — skipping.');
    return STATE.lastDocUpdates;
  }

  const allUpdates = [];

  for (const { id: docId, role } of docConfigs) {
    try {
      const { title, sections, tables } = await loadGoogleDocStructured(auth, docId);
      let updates = [];

      if (role === 'wrap' && tables.length > 0) {
        // ── TABLE MODE (DOC 2 — Weekly Wrap, AI Solutions Status table) ──
        console.log(`  [docs:${docId.slice(0, 8)}…] "${title}" table mode — ${tables.length} table(s)`);
        updates = await extractProjectsFromDocTable(tables, inventory, docId);
      } else {
        // ── SECTION MODE (DOC 1 — or DOC 2 fallback if no tables found) ──
        console.log(`  [docs:${docId.slice(0, 8)}…] "${title}" section mode — ${sections.length} sections (latest ≤2)`);
        updates = await extractFromRecentSections(sections, inventory, docId, 2);
      }

      console.log(`  [docs:${docId.slice(0, 8)}…] → ${updates.length} project update(s) extracted`);
      allUpdates.push(...updates);
    } catch (err) {
      console.error(`  [docs] Error on doc ${docId}: ${err.message}`);
    }
  }

  if (allUpdates.length > 0) {
    STATE.lastDocUpdates = allUpdates;
  } else if (STATE.lastDocUpdates.length) {
    console.warn('  [docs] No new updates extracted — using cached doc updates.');
    return STATE.lastDocUpdates;
  }

  return allUpdates;
}


// ═══════════════════════════════════════════════════════════════════
// APPLY DOC UPDATES  (high-confidence source)
// ═══════════════════════════════════════════════════════════════════

function applyDocUpdatesToProjects(projectMap, docUpdates, syncMs) {
  for (const upd of docUpdates) {
    if (!upd || !upd.projectId || upd.confidence < DOC_CONFIDENCE_THRESHOLD) continue;

    const proj = projectMap.get(upd.projectId);
    if (!proj) continue;

    const ev = upd.evidence || {};

    // ── Status (health) — derived from evidence.blocked + keyword check ─
    const isBlocked = ev.blocked || isExplicitBlocker(upd.evidenceText || '');
    if (isBlocked && HEALTH_SEVERITY['blocked'] > (HEALTH_SEVERITY[proj.health] ?? -1)) {
      proj.canonicalStatus  = 'BLOCKED BY INFRA/SECURITY';
      proj.health           = 'blocked';
      proj.statusConfidence = upd.confidence;
      proj.statusReason     = 'Explicit blocker identified in weekly recap';
    } else if (ev.live && !isBlocked) {
      // Live + no blocker → on-track (docs only; Slack doesn't update status)
      if ((HEALTH_SEVERITY['on-track'] ?? 1) >= (HEALTH_SEVERITY[proj.health] ?? -1)) {
        proj.canonicalStatus  = 'LIVE';
        proj.health           = 'on-track';
        proj.statusConfidence = upd.confidence;
        proj.statusReason     = `Live signal from doc (${(upd.confidence * 100).toFixed(0)}% conf)`;
      }
    }

    // ── Stage evidence accumulation (Layer 1) ─────────────────────────
    // Merge LLM evidence flags with keyword scan of evidenceText — union wins
    const kwFlags = extractEvidenceFlags(upd.evidenceText || '');
    const merged = {
      live:       ev.live       || kwFlags.live,
      enablement: ev.enablement || kwFlags.enablement,
      building:   ev.building   || kwFlags.building,
      scoping:    ev.scoping    || kwFlags.scoping,
      blocked:    ev.blocked    || kwFlags.blocked,
      snippets:   kwFlags.snippets,
    };
    // Note: determineStage() is called after ALL sources are accumulated
    accumulateEvidence(proj, merged, 'doc', upd.evidenceText, syncMs);

    // ── Department inference — update from Unassigned if context reveals dept ──
    if (!proj.department || proj.department === 'Unassigned') {
      const inferredDept = inferDepartmentFromContext(upd.evidenceText || '');
      if (inferredDept) proj.department = inferredDept;
    }

    // ── Display text (UI: statusSummary + topBlocker) ─────────────────
    const displayText = upd.evidenceText || '';
    if (displayText && (!proj.statusSummary || upd.confidence >= 0.7)) {
      proj.statusSummary = displayText;
    }
    if (isBlocked && displayText && !proj.topBlocker) {
      proj.topBlocker         = displayText;
      proj.blockerAge         = 'Recent';
      proj.blockingDepartment = inferBlockingDepartment(displayText);
      proj.blockerReason      = displayText;
      proj.blockerSource      = 'weekly_recap';
    }

    // ── Developments feed ─────────────────────────────────────────────
    if (displayText) {
      proj.sourceEvidence.push({ source: 'google_doc', docId: upd.sourceDocId, text: displayText, confidence: upd.confidence });
      if (!proj.signals.includes(displayText)) proj.signals.push(displayText);
      proj.recentDevelopments.push({
        text:   displayText,
        type:   isBlocked ? 'blocker' : (ev.live ? 'milestone' : 'update'),
        source: 'google_doc',
      });
    }

    proj.lastMeaningfulUpdateMs = syncMs;
    proj.lastUpdated            = timeSince(syncMs);
    proj.leadershipAttention    = proj.health === 'blocked' || proj.health === 'at-risk';
  }
}


// ═══════════════════════════════════════════════════════════════════
// LLM: SLACK MESSAGE EXTRACTION
// ═══════════════════════════════════════════════════════════════════

async function extractFromSlackMessage(messageText, inventory) {
  const projectList = inventory.map(p => `- ${p.name}`).join('\n');

  const prompt =
`You are extracting evidence flags from a Slack message for an AI program stage classification engine.
Stage is determined by code — your job is ONLY to detect what is explicitly stated in the message.
Slack is a lower-confidence source than structured docs. Be conservative.

Known projects:
${projectList}

Your task:
1. Match to ONE project from the list (or null if no clear match)
2. Set evidence flags based ONLY on explicit statements — not implied or inferred
3. Return the key phrase (≤20 words) that best supports your flags
4. Set confidence 0.0–1.0 (Slack is noisy — rarely exceed 0.80)

Evidence flag definitions:
- evidence.live: solution is explicitly in real use, launched, in production, or pilot running with real users
  Examples: "went live", "users are in it now", "pilot running", "live on 5 projects"
  Maintenance, bug fixes, and iteration on a live product are still Live — do not set building for these.
- evidence.enablement: solution already live AND scaling — org-wide rollout, adoption programs, all employees
- evidence.building: explicitly pre-launch — build in progress, not yet launched, pending first deployment
  Do NOT set if project is live and being refined.
- evidence.scoping: early concept or requirements phase only
- evidence.blocked: waiting on IT/security/legal/access, provisioning, or infra — explicitly stated

Return JSON only:
{
  "project": string | null,
  "evidence": {
    "live": boolean,
    "enablement": boolean,
    "building": boolean,
    "scoping": boolean,
    "blocked": boolean
  },
  "evidenceText": string,
  "confidence": number
}

Slack message:
"""
${messageText}
"""`;

  try {
    const resp = await ai.messages.create({
      model: 'claude-haiku-4-5-20251001', max_tokens: 200,
      messages: [{ role: 'user', content: prompt }],
    });
    const obj = safeParseJSON(resp.content[0].text);
    if (!obj) return null;

    const ev = obj.evidence || {};
    const hardBlocked = isExplicitBlocker(obj.evidenceText || '');

    return {
      project:  obj.project || null,
      evidence: {
        live:       !!(ev.live),
        enablement: !!(ev.enablement),
        building:   false,           // Slack building signals too noisy — suppressed; only docs can advance to Building
        scoping:    false,           // Slack never moves a project to Scoping
        blocked:    !!(ev.blocked) || hardBlocked,
      },
      evidenceText: obj.evidenceText || '',
      confidence:   typeof obj.confidence === 'number' ? obj.confidence : 0.4,
      source:       'slack',
    };
  } catch (err) {
    console.warn(`    [slack extract] Skipped: ${err.message}`);
    return null;
  }
}

async function loadSlackSignals(inventory) {
  const channelIds = (process.env.SLACK_CHANNEL_IDS || '')
    .split(',').map(s => s.trim()).filter(Boolean);

  if (!channelIds.length) {
    console.log('  [slack] No SLACK_CHANNEL_IDS configured — skipping.');
    return [];
  }

  const allSignals = [];

  // 14-day lookback — Slack can be noisy; older messages are less actionable
  const slackOldest = String(Math.floor((Date.now() - 14 * 24 * 60 * 60 * 1000) / 1000));

  for (const channelId of channelIds) {
    try {
      const infoRes = await slack.conversations.info({ channel: channelId });
      console.log(`  [slack] #${infoRes.channel.name} (${channelId}) — 14-day window`);

      const histRes  = await slack.conversations.history({ channel: channelId, limit: 50, oldest: slackOldest });
      const messages = (histRes.messages || [])
        .filter(m => m.type === 'message' && !m.subtype && m.text && m.text.trim().length > 15)
        .slice(0, 50);

      if (!messages.length) continue;

      const extractions = await withConcurrency(
        messages.map(msg => () => extractFromSlackMessage(msg.text, inventory)),
        3
      );

      for (const ext of extractions) {
        if (!ext || !ext.project) continue;
        const matched = matchToInventory(ext.project, inventory);
        if (!matched) {
          console.log(`    [slack] No inventory match for "${ext.project}" — skipped.`);
          await trackUnmatchedProject(ext.project, 'slack', Date.now());
          continue;
        }
        allSignals.push({ ...ext, project: matched.name, projectId: matched.id });
      }

    } catch (err) {
      console.error(`  [slack] Error on channel ${channelId}: ${err.message}`);
    }
  }

  return allSignals;
}


// ═══════════════════════════════════════════════════════════════════
// APPLY SLACK SIGNALS  (lower-confidence source)
// ═══════════════════════════════════════════════════════════════════

function applySlackSignalsToProjects(projectMap, slackSignals, syncMs) {
  for (const sig of slackSignals) {
    if (!sig || !sig.projectId || sig.confidence < SLACK_BLOCKER_THRESHOLD) continue;

    const proj = projectMap.get(sig.projectId);
    if (!proj) continue;

    const ev = sig.evidence || {};
    const displayText = sig.evidenceText || '';

    // ── Status: blocked only from Slack (explicit keyword required) ───
    const isBlocked = ev.blocked || isExplicitBlocker(displayText);
    if (isBlocked && sig.confidence >= SLACK_BLOCKER_THRESHOLD) {
      if (HEALTH_SEVERITY['blocked'] > (HEALTH_SEVERITY[proj.health] ?? -1)) {
        proj.canonicalStatus  = 'BLOCKED BY INFRA/SECURITY';
        proj.health           = 'blocked';
        proj.statusConfidence = sig.confidence;
        proj.statusReason     = 'Explicit blocker identified in team communications';
      }
      if (displayText && !proj.topBlocker) {
        proj.topBlocker         = displayText;
        proj.blockerAge         = 'Recent';
        proj.blockingDepartment = inferBlockingDepartment(displayText);
        proj.blockerReason      = displayText;
        proj.blockerSource      = 'slack';
      }
    }

    // ── Department inference — fill Unassigned from Slack context ────
    if (!proj.department || proj.department === 'Unassigned') {
      const inferredDept = inferDepartmentFromContext(displayText);
      if (inferredDept) proj.department = inferredDept;
    }

    // ── Display text — fill gaps only ────────────────────────────────
    if (displayText && !proj.statusSummary) {
      proj.statusSummary = displayText;
    }

    // ── Stage evidence accumulation (conservative for Slack) ─────────
    // Slack never contributes building or scoping flags — too noisy.
    // Only live / enablement / blocked from Slack.
    if (sig.confidence >= SLACK_STATUS_THRESHOLD) {
      const kwFlags = extractEvidenceFlags(displayText);
      const conservativeFlags = {
        live:       ev.live       || kwFlags.live,
        enablement: ev.enablement || kwFlags.enablement,
        building:   false,   // Slack never advances to Building
        scoping:    false,   // Slack never advances to Scoping
        blocked:    ev.blocked || kwFlags.blocked,
        snippets:   kwFlags.snippets,
      };
      accumulateEvidence(proj, conservativeFlags, 'slack', displayText, syncMs);
    }

    // ── Developments feed ─────────────────────────────────────────────
    if (displayText) {
      proj.sourceEvidence.push({ source: 'slack', text: displayText, confidence: sig.confidence });
      if (!proj.signals.includes(displayText)) proj.signals.push(displayText);
      proj.recentDevelopments.push({
        text:   displayText,
        type:   isBlocked ? 'blocker' : (ev.live ? 'milestone' : 'update'),
        source: 'slack',
      });
    }

    proj.lastMeaningfulUpdateMs = syncMs;
    proj.lastUpdated            = timeSince(syncMs);
    proj.leadershipAttention    = proj.health === 'blocked' || proj.health === 'at-risk';
  }
}


// ═══════════════════════════════════════════════════════════════════
// INITIALIZE PROJECT MAP FROM INVENTORY
// ═══════════════════════════════════════════════════════════════════

function initializeProjectMap(inventory, syncMs) {
  const map = new Map();
  for (const inv of inventory) {
    const canonical = toCanonicalStatus(inv.status);
    const health    = canonical ? toHealth(canonical) : 'on-track';

    // Derive initial stage using forward-only inferrer
    const { stage: initialStage, confidence: stageConf, reason: stageReason } =
      inferInitialStage(canonical, inv.stage || '');

    // If sheet explicitly marks as blocked, pre-populate blocker
    const isSheetBlocked = health === 'blocked';
    const blockingDept   = isSheetBlocked ? inferBlockingDepartment(inv.notes || '') : null;

    // Detect strong live evidence directly from the raw sheet text (pre-LLM gate)
    const sheetLiveCheck = isStrongLiveEvidence(inv.stage || '');
    const sheetHasLive   = sheetLiveCheck.found || STAGE_PRIORITY[initialStage] >= STAGE_PRIORITY.live;

    map.set(inv.id, {
      // Identity
      id:               inv.id,
      name:             inv.name,
      owner:            inv.owner      || 'Unassigned',
      department:       inv.department || 'Unassigned',
      // ── Stage (lifecycle) — forward-only, durable ──────────────────
      stage:                     initialStage,
      // Sheet confidence is CAPPED — it is a baseline, not a final verdict.
      // Docs + Slack with explicit evidence will override and raise confidence.
      stageConfidence:           Math.min(stageConf, 0.60),
      stageReason,
      stageEvidence:             [],
      // Source provenance: 'sheet' until docs or Slack evidence advances it
      stageSource:               'sheet',
      // Immutable snapshot of what the sheet originally said — preserved even after overrides
      baselineStageFromSheet:    initialStage,
      // Timestamp of the last doc or Slack evidence that touched stage (null = sheet only)
      lastEvidenceTimestamp:     null,
      // Per-sync evidence accumulator — ORed by accumulateEvidence() from each source.
      // Reset to false each sync; determineStage() uses this + durable flags.
      evidence: { live: false, enablement: false, building: false, scoping: false, blocked: false },
      hasEverBeenLive:           sheetHasLive,
      hasEverBeenEnabled:        STAGE_PRIORITY[initialStage] >= STAGE_PRIORITY.enablement,
      // ── Debug fields (internal — stripped before sending to frontend) ──
      _sheetRawStage:         inv.stage || '',
      _liveEvidenceFromSheet: sheetHasLive,
      _liveEvidenceFromDocs:  false,
      _liveEvidenceFromSlack: false,
      firstLiveEvidenceAt:       STAGE_PRIORITY[initialStage] >= STAGE_PRIORITY.live        ? syncMs : null,
      firstEnablementEvidenceAt: STAGE_PRIORITY[initialStage] >= STAGE_PRIORITY.enablement  ? syncMs : null,
      // ── Status (health) — dynamic, independent of stage ───────────
      health,
      canonicalStatus:  canonical,
      baselineStatus:   canonical,
      statusReason:     canonical ? 'Set in project inventory' : null,
      statusConfidence: canonical ? 1.0 : 0,
      statusSummary:    inv.notes || null,
      // Blockers
      topBlocker:          isSheetBlocked ? (inv.notes || 'Blocked — see project details') : null,
      blockerAge:          isSheetBlocked ? '—' : null,
      blockingDepartment:  blockingDept,
      blockerReason:       isSheetBlocked ? (inv.notes || null) : null,
      blockerSource:       isSheetBlocked ? 'sheet' : null,
      // Timeline
      lastMeaningfulUpdateMs: syncMs,
      lastUpdated:            new Date(syncMs).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      nextMilestone:          inv.milestone || 'TBD',
      // Leadership
      leadershipAttention:    health === 'blocked' || health === 'at-risk',
      // Evidence (internal — stripped before sending to frontend)
      sourceEvidence:    [],
      // Signals + developments (shown in detail panel and developments feed)
      signals:           [],
      recentDevelopments: [],
    });
  }
  return map;
}


// ═══════════════════════════════════════════════════════════════════
// EXECUTIVE SUMMARY GENERATION
// ═══════════════════════════════════════════════════════════════════

async function generateExecutiveSummary(projects) {
  const active = projects.filter(p => p.canonicalStatus || p.signals.length > 0);
  if (!active.length) return STATE.summary;

  const context = active.map(p => {
    const stageLabel = p.stage ? p.stage.charAt(0).toUpperCase() + p.stage.slice(1) : 'Unknown';
    return `${p.name} [Stage: ${stageLabel}, Status: ${p.canonicalStatus || p.health}]: ${p.statusSummary || '—'}` +
      (p.topBlocker ? `  BLOCKED: ${p.topBlocker}` : '') +
      (p.blockingDepartment ? ` (blocking dept: ${p.blockingDepartment})` : '');
  }).join('\n');

  const prompt =
`You are writing an executive briefing for Clayco's AI program dashboard. The audience is the CEO and senior leadership.

Rules:
- Plain business language only. No technical terms, no jargon.
- Each bullet must be one sentence, max 20 words.
- Do not explain mechanics. Focus on business impact and decisions.
- Only include items with real signal. Skip noise.

Return JSON only with exactly these 3 array fields (2–3 bullets each, never more):
{
  "attention": ["string", "string"],
  "positive":  ["string", "string"],
  "horizon":   ["string", "string"]
}

Field definitions:
- attention: Blockers or risks needing leadership decision or escalation right now
- positive:  Meaningful progress — launches, milestones, validated results
- horizon:   What could change trajectory in the next 2–4 weeks

Project context:
${context}`;

  try {
    const resp  = await ai.messages.create({
      model: 'claude-haiku-4-5-20251001', max_tokens: 500,
      messages: [{ role: 'user', content: prompt }],
    });
    const obj = safeParseJSON(resp.content[0].text);
    if (!obj) return STATE.summary;
    // Normalise: if model returned strings instead of arrays, wrap them
    const norm = (v) => Array.isArray(v) ? v.slice(0, 3) : (v ? [v] : []);
    return { attention: norm(obj.attention), positive: norm(obj.positive), horizon: norm(obj.horizon) };
  } catch (err) {
    console.warn(`  [summary] Failed: ${err.message}`);
    return STATE.summary;
  }
}


// ═══════════════════════════════════════════════════════════════════
// MAIN SYNC  (Sheet → Docs → Slack → Merge → Summary → Commit)
// ═══════════════════════════════════════════════════════════════════

async function runFullSync() {
  console.log('\n[sync] ════════════ Starting full sync ════════════');
  STATE.syncStatus = 'syncing';
  const syncMs = Date.now();

  // ── 1. Google Auth ────────────────────────────────────────────────
  const auth = await getGoogleAuth();

  // ── 2. Load canonical project inventory ──────────────────────────
  console.log('[sync] Step 1 — Project inventory');
  let inventory;
  try {
    inventory = await loadProjectInventory(auth);
    if (inventory.length) {
      STATE.lastInventory = inventory;
    } else {
      inventory = STATE.lastInventory.length ? STATE.lastInventory : MOCK_INVENTORY;
      console.warn('  [inventory] Empty result — using fallback.');
    }
  } catch (err) {
    console.error('  [inventory] Fatal error:', err.message);
    inventory = STATE.lastInventory.length ? STATE.lastInventory : MOCK_INVENTORY;
  }
  console.log(`  [inventory] ${inventory.length} projects loaded.`);

  // ── 3. Initialise project map ─────────────────────────────────────
  const projectMap = initializeProjectMap(inventory, syncMs);

  // ── 4. Google Docs (high-confidence) ─────────────────────────────
  console.log('[sync] Step 2 — Google Docs (high-confidence)');
  let docUpdates = [];
  try {
    docUpdates = await loadGoogleDocsUpdates(auth, inventory);
    console.log(`  [docs] ${docUpdates.length} updates extracted.`);
    applyDocUpdatesToProjects(projectMap, docUpdates, syncMs);
  } catch (err) {
    console.error('  [docs] Error — continuing without doc updates:', err.message);
  }

  // ── 5. Slack signals (lower-confidence) ──────────────────────────
  console.log('[sync] Step 3 — Slack signals (lower-confidence)');
  let slackSignals = [];
  try {
    slackSignals = await loadSlackSignals(inventory);
    console.log(`  [slack] ${slackSignals.length} signals extracted.`);
    applySlackSignalsToProjects(projectMap, slackSignals, syncMs);
  } catch (err) {
    console.error('  [slack] Error — continuing without Slack signals:', err.message);
  }

  // ── 6. DETERMINISTIC STAGE RESOLUTION ─────────────────────────────
  // All evidence has been accumulated. Now run determineStage() once per
  // project. This is the ONLY place stage is decided. AI never sets stage.
  console.log('[sync] Step 4 — Deterministic stage resolution');
  for (const proj of projectMap.values()) {
    // Capture hasEverBeenLive BEFORE determineStage so we can log the delta
    const hadEverBeenLive    = proj.hasEverBeenLive;
    const hadEverBeenEnabled = proj.hasEverBeenEnabled;

    const finalStage = determineStage(proj, syncMs);
    proj.stage = finalStage;

    // Projects with no doc/slack signal keep sheet baseline confidence (≤0.60)
    if (!proj.lastEvidenceTimestamp) {
      proj.stageConfidence = Math.min(proj.stageConfidence || 0.5, 0.60);
    }

    const ev = proj.evidence;
    console.log(
      `  [live-debug] "${proj.name}"` +
      `\n    sheetRaw="${proj._sheetRawStage}" → baselineStage=${proj.baselineStageFromSheet}` +
      `\n    liveEvidence: sheet=${proj._liveEvidenceFromSheet} | docs=${proj._liveEvidenceFromDocs} | slack=${proj._liveEvidenceFromSlack}` +
      `\n    hasEverBeenLive(pre)=${hadEverBeenLive} hasEverBeenEnabled(pre)=${hadEverBeenEnabled}` +
      `\n    accumulated: live=${ev.live} enablement=${ev.enablement} building=${ev.building} blocked=${ev.blocked}` +
      `\n    → finalStage=${finalStage} | source=${proj.stageSource} | reason="${proj.stageReason}"` +
      `\n    snippets: [${(proj.stageEvidence || []).slice(0, 3).map(s => `"${s.slice(0,50)}"`).join(' | ')}]`
    );
  }

  // ── 7. Finalise projects ──────────────────────────────────────────
  const liveDebug = [];
  const projects  = Array.from(projectMap.values()).map(p => {
    // Capture debug snapshot before stripping internal fields
    liveDebug.push({
      name:                  p.name,
      sheetRawStage:         p._sheetRawStage         || '',
      baselineStageFromSheet: p.baselineStageFromSheet,
      liveEvidenceFromSheet: p._liveEvidenceFromSheet  || false,
      liveEvidenceFromDocs:  p._liveEvidenceFromDocs   || false,
      liveEvidenceFromSlack: p._liveEvidenceFromSlack  || false,
      hasEverBeenLive:       p.hasEverBeenLive,
      hasEverBeenEnabled:    p.hasEverBeenEnabled,
      finalStage:            p.stage,
      stageReason:           p.stageReason,
      stageSource:           p.stageSource,
      topLiveSnippets:       (p.stageEvidence || []).slice(0, 5),
    });

    // Strip internal-only fields before sending to frontend
    const {
      sourceEvidence, explicitStage,
      _sheetRawStage, _liveEvidenceFromSheet, _liveEvidenceFromDocs, _liveEvidenceFromSlack,
      ...pub
    } = p;
    return {
      ...pub,
      signals:            p.signals.slice(0, 5),
      recentDevelopments: p.recentDevelopments.slice(0, 4),
      stageEvidence:      (p.stageEvidence || []).slice(0, 4),
    };
  });

  // ── 8. Build developments feed (CEO-facing, filtered for signal value) ──
  // Only include: blockers, meaningful milestone progress, material risks
  // Skip: operational admin, minor status confirmations, low-value chatter
  const CEOfeedSignals = [];
  for (const p of projects) {
    // Always include blockers
    if (p.health === 'blocked' && p.topBlocker) {
      CEOfeedSignals.push({
        project: p.name,
        type: 'blocker',
        time: timeSince(p.lastMeaningfulUpdateMs),
        text: p.topBlocker,
      });
      continue;
    }
    // Include at-risk with meaningful summary
    if (p.health === 'at-risk' && p.statusSummary) {
      CEOfeedSignals.push({
        project: p.name,
        type: 'risk',
        time: timeSince(p.lastMeaningfulUpdateMs),
        text: p.statusSummary,
      });
      continue;
    }
    // Include live/on-track projects only if they have signal (milestone/progress)
    if ((p.health === 'on-track' || p.stage === 'live') && p.signals.length > 0) {
      CEOfeedSignals.push({
        project: p.name,
        type: p.stage === 'live' ? 'milestone' : 'update',
        time: timeSince(p.lastMeaningfulUpdateMs),
        text: p.signals[0],
      });
    }
  }

  // ── 9. Build blockers list ────────────────────────────────────────
  const blockers = projects
    .filter(p => p.topBlocker)
    .map(p => ({
      projectId:          p.id,
      project:            p.name,
      department:         p.department,
      blockingDepartment: p.blockingDepartment || inferBlockingDepartment(p.topBlocker),
      blocker:            p.topBlocker,
      blockerReason:      p.blockerReason || p.topBlocker,
      age:                p.blockerAge || '—',
      severity:           p.health === 'blocked' ? 'high' : 'medium',
      source:             p.blockerSource || 'unknown',
    }));

  // ── 10. Executive summary ─────────────────────────────────────────
  console.log('[sync] Step 5 — Executive summary');
  const summary = await generateExecutiveSummary(projects);

  // ── 11. Commit state ──────────────────────────────────────────────
  STATE.projects   = projects;
  STATE.blockers   = blockers;
  STATE.signals    = CEOfeedSignals.slice(0, 9);
  STATE.summary    = summary;
  STATE.lastSync   = syncMs;
  STATE.syncStatus = 'idle';
  STATE.liveDebug  = liveDebug;

  const result = {
    projects:    projects.length,
    withUpdates: projects.filter(p => p.signals.length > 0).length,
    blockers:    blockers.length,
    blocked:     projects.filter(p => p.health === 'blocked').length,
    docUpdates:  docUpdates.length,
    slackSignals: slackSignals.length,
    syncedAt:    new Date(syncMs).toISOString(),
  };

  console.log(`[sync] ✓ Complete — ${result.projects} projects (${result.blocked} blocked, ${result.withUpdates} with signals), ${result.blockers} blockers`);
  return result;
}


// ═══════════════════════════════════════════════════════════════════
// API ROUTES
// ═══════════════════════════════════════════════════════════════════

// POST /api/sync  —  unified sync: Sheet → Docs → Slack → rebuild state
app.post('/api/sync', async (req, res) => {
  try {
    const result = await runFullSync();
    res.json({ ok: true, ...result });
  } catch (err) {
    console.error('[/api/sync]', err.message);
    STATE.syncStatus = 'idle';
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/sync  —  non-blocking browser-friendly trigger.
// Returns immediately with { ok: true, status: 'syncing' }.
// Poll GET /api/dashboard/overview and watch syncStatus → 'idle' to know when done.
app.get('/api/sync', (req, res) => {
  if (STATE.syncStatus === 'syncing') {
    return res.json({ ok: true, status: 'syncing', message: 'Sync already in progress. Poll /api/dashboard/overview for status.' });
  }
  // Fire and forget — do NOT await
  runFullSync().catch(err => {
    console.error('[/api/sync GET background]', err.message);
    STATE.syncStatus = 'idle';
  });
  res.json({ ok: true, status: 'syncing', message: 'Sync started. Poll /api/dashboard/overview until syncStatus is idle.' });
});

// POST /api/slack/sync  —  kept for backward compatibility
app.post('/api/slack/sync', async (req, res) => {
  try {
    const result = await runFullSync();
    res.json({ ok: true, ...result });
  } catch (err) {
    STATE.syncStatus = 'idle';
    res.status(500).json({ ok: false, error: err.message });
  }
});

// GET /api/dashboard/overview
app.get('/api/dashboard/overview', (req, res) => {
  const p = STATE.projects;
  res.json({
    summary: STATE.summary,
    metrics: {
      total:       p.length,
      scoping:     p.filter(x => x.stage === 'scoping').length,
      building:    p.filter(x => x.stage === 'building').length,
      live:        p.filter(x => x.stage === 'live').length,
      enablement:  p.filter(x => x.stage === 'enablement').length,
      onTrack:     p.filter(x => x.health === 'on-track').length,
      atRisk:      p.filter(x => x.health === 'at-risk').length,
      blocked:     p.filter(x => x.health === 'blocked').length,
    },
    lastSync:   STATE.lastSync,
    syncStatus: STATE.syncStatus,
  });
});

app.get('/api/dashboard/projects',     (req, res) => res.json(STATE.projects));
app.get('/api/dashboard/projects/:id', (req, res) => {
  const p = STATE.projects.find(x => x.id === req.params.id);
  if (!p) return res.status(404).json({ error: 'Not found' });
  res.json(p);
});
app.get('/api/dashboard/blockers',     (req, res) => res.json(STATE.blockers));
app.get('/api/dashboard/signals',      (req, res) => res.json(STATE.signals));
app.get('/api/dashboard/summary',      (req, res) => res.json(STATE.summary));

// GET /api/debug/live  —  per-project live evidence trace (debug only)
// Shows exactly why each project was or was not classified as Live.
app.get('/api/debug/live', (req, res) => {
  res.json(STATE.liveDebug || []);
});

app.get('/api/inventory', (req, res) => res.json({
  count:    STATE.lastInventory.length,
  source:   process.env.PROJECT_INVENTORY_SHEET_URL || '(none — using mock)',
  projects: STATE.lastInventory.map(p => ({ id: p.id, name: p.name, department: p.department, stage: p.stage, status: p.status })),
}));

app.get('/health', (req, res) => res.json({
  ok:        true,
  lastSync:  STATE.lastSync ? new Date(STATE.lastSync).toISOString() : null,
  projects:  STATE.projects.length,
  blocked:   STATE.projects.filter(p => p.health === 'blocked').length,
  inventory: STATE.lastInventory.length,
}));


// ═══════════════════════════════════════════════════════════════════
// START
// ═══════════════════════════════════════════════════════════════════

app.listen(PORT, () => {
  console.log(`
╔══════════════════════════════════════════════════════════╗
║        Clayco AI Dashboard — Server Ready                ║
╠══════════════════════════════════════════════════════════╣
║  http://localhost:${PORT}                                    ║
║                                                          ║
║  POST /api/sync           full sync (Sheet+Docs+Slack)  ║
║  GET  /api/inventory      inspect loaded projects        ║
║  GET  /api/dashboard/*    read dashboard state           ║
║  GET  /health             liveness check                 ║
╚══════════════════════════════════════════════════════════╝
`);

  const checks = {
    'SLACK_BOT_TOKEN':             process.env.SLACK_BOT_TOKEN,
    'SLACK_CHANNEL_IDS':           process.env.SLACK_CHANNEL_IDS,
    'PM_ALERT_CHANNEL_ID':         process.env.PM_ALERT_CHANNEL_ID,
    'ANTHROPIC_API_KEY':           process.env.ANTHROPIC_API_KEY,
    'PROJECT_INVENTORY_SHEET_URL': process.env.PROJECT_INVENTORY_SHEET_URL,
    'GOOGLE_SERVICE_ACCOUNT':      process.env.GOOGLE_SERVICE_ACCOUNT_KEY_PATH || process.env.GOOGLE_SERVICE_ACCOUNT_JSON,
    'GOOGLE_DOC_1_ID':             process.env.GOOGLE_DOC_1_ID,
    'GOOGLE_DOC_2_ID':             process.env.GOOGLE_DOC_2_ID,
  };

  console.log('Configuration check:');
  for (const [key, val] of Object.entries(checks)) {
    const status = val ? '✓' : '✗ (not set)';
    console.log(`  ${status}  ${key}`);
  }
  console.log('');
});
