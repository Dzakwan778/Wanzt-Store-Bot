import express from "express";
import path from "path";
import fs from "fs";
import crypto from "crypto";
import { createServer as createViteServer } from "vite";
import makeWASocket, { useMultiFileAuthState, DisconnectReason, proto, generateWAMessageFromContent } from "@whiskeysockets/baileys";
import pino from "pino";
import QRCode from "qrcode";
import { isFirebaseEnabled, syncFromFirestore, syncToFirestore } from "./src/firebase-db.js";

const app = express();
const PORT = 3000;

function getFileMimeType(fileName: string, defaultType: string): string {
  const ext = path.extname(fileName).toLowerCase();
  if (ext === ".jpg" || ext === ".jpeg") return "image/jpeg";
  if (ext === ".png") return "image/png";
  if (ext === ".webp") return "image/webp";
  if (ext === ".gif") return "image/gif";
  if (ext === ".mp4") return "video/mp4";
  return defaultType;
}

app.use(express.json({ limit: "15mb" }));
app.use(express.urlencoded({ limit: "15mb", extended: true }));

// Set up paths
const DB_PATH = path.join(process.cwd(), "src", "database.json");
const DB_PRODUCTS_PATH = path.join(process.cwd(), "src", "db_products.json");
const DB_CATEGORIES_PATH = path.join(process.cwd(), "src", "db_categories.json");
const DB_SETTINGS_PATH = path.join(process.cwd(), "src", "db_settings.json");
const DB_COMMANDS_PATH = path.join(process.cwd(), "src", "db_commands.json");
const DB_TRANSACTIONS_PATH = path.join(process.cwd(), "src", "db_transactions.json");
const DB_ACTIVE_TRANSACTIONS_PATH = path.join(process.cwd(), "src", "db_active_transactions.json");
const DB_SCHEDULED_BROADCASTS_PATH = path.join(process.cwd(), "src", "db_scheduled_broadcasts.json");
const AUTH_DIR = path.join(process.cwd(), "baileys_auth_info");

interface MessageLog {
  id: string;
  from: string;
  senderName: string;
  message: string;
  timestamp: string;
  type: "incoming" | "outgoing" | "system";
  status?: string;
}

const lidCache = new Map<string, string>();

function resolveLidToPn(jid: string): string {
  if (!jid) return jid;
  if (jid.endsWith("@lid")) {
    const rawLid = jid.split("@")[0] || "";
    const cleanLid = rawLid.split(":")[0];
    
    // Check in-memory cache first
    if (lidCache.has(cleanLid)) {
      return `${lidCache.get(cleanLid)}@s.whatsapp.net`;
    }
    
    try {
      const authDir = path.join(process.cwd(), "baileys_auth_info");
      if (fs.existsSync(authDir)) {
        const files = fs.readdirSync(authDir);
        for (const file of files) {
          if (file.startsWith("lid-mapping-") && file.endsWith(".json")) {
            const filePath = path.join(authDir, file);
            const content = fs.readFileSync(filePath, "utf-8").trim().replace(/['"]/g, "");
            if (content === cleanLid) {
              const pn = file.replace("lid-mapping-", "").replace(".json", "");
              // Write to cache
              lidCache.set(cleanLid, pn);
              return `${pn}@s.whatsapp.net`;
            }
          }
        }
      }
    } catch (e) {
      console.error("Error resolving LID to PN:", e);
    }
  }
  return jid;
}

function jidNormalizedUser(jid: string | null | undefined): string {
  if (!jid) return "";
  const parts = jid.split("@");
  if (parts.length < 2) return jid;
  const user = parts[0].split(":")[0];
  const server = parts[1];
  return `${user}@${server}`;
}

function isSelfBot(jid: string | null | undefined, sockInstance?: any): boolean {
  if (!jid) return false;
  
  const cleanJid = jidNormalizedUser(jid);
  if (!cleanJid) return false;

  let botPn = "";
  let botLid = "";
  try {
    const credsPath = path.join(process.cwd(), "baileys_auth_info", "creds.json");
    if (fs.existsSync(credsPath)) {
      const creds = JSON.parse(fs.readFileSync(credsPath, "utf-8"));
      if (creds.me) {
        if (creds.me.id) botPn = jidNormalizedUser(creds.me.id);
        if (creds.me.lid) botLid = jidNormalizedUser(creds.me.lid);
      }
    }
  } catch (err) {
    // Ignore error
  }

  const activeSock = sockInstance || (typeof botManager !== "undefined" ? botManager?.sock : null);
  if (activeSock?.user) {
    const user = activeSock.user;
    if (user.id && !botPn) botPn = jidNormalizedUser(user.id);
    if (user.lid && !botLid) botLid = jidNormalizedUser(user.id.includes("@lid") ? user.id : user.lid);
    if (user.id && user.id.includes("@lid")) {
      botLid = jidNormalizedUser(user.id);
    }
  }

  if (botPn && cleanJid === botPn) return true;
  if (botLid && cleanJid === botLid) return true;

  const jidNode = cleanJid.split("@")[0];
  const botPnNode = botPn ? botPn.split("@")[0] : "";
  const botLidNode = botLid ? botLid.split("@")[0] : "";

  if (botPnNode && jidNode === botPnNode) return true;
  if (botLidNode && jidNode === botLidNode) return true;

  const resolvedCandidate = resolveLidToPn(cleanJid);
  const resolvedCandidateNormalized = jidNormalizedUser(resolvedCandidate);
  if (botPn && resolvedCandidateNormalized === botPn) return true;

  return false;
}

// Bot Manager State
class WhatsAppBotManager {
  status: "disconnected" | "connecting" | "connected" = "disconnected";
  qrCode: string = "";
  pairingCode: string = "";
  phoneNumber: string = "";
  pushName: string = "";
  logs: MessageLog[] = [];
  sock: any = null;
  error: string = "";
  reconnectAttempts: number = 0;
  maxReconnectAttempts: number = 5;
  presencesMap: Record<string, Set<string>> = {};
  spamTracker: Record<string, { lastMessage: string; count: number; cooldownUntil: number; warned: boolean }> = {};
  cachedDb: any = null;
  private isSyncingFirebase: boolean = false;
  isManualDisconnect: boolean = false;

  constructor() {
    this.startScheduler();
  }

  startScheduler() {
    console.log("[Scheduler] Scheduled Broadcast checker started (Interval: 15 seconds)");
    setInterval(async () => {
      try {
        const db = this.getDb();
        if (!db || !db.scheduledBroadcasts || db.scheduledBroadcasts.length === 0) {
          return;
        }

        const now = new Date();
        const pendingBroadcasts = db.scheduledBroadcasts.filter(
          (b: any) => b.status === "pending" && new Date(b.scheduledTime) <= now
        );

        if (pendingBroadcasts.length === 0) {
          return;
        }

        console.log(`[Scheduler] Found ${pendingBroadcasts.length} broadcast(s) ready to send!`);

        for (const b of pendingBroadcasts) {
          b.status = "processing";
          this.saveDb(db);

          const delayMs = (db.settings?.broadcastDelay || 3) * 1000;
          console.log(`[Scheduler] Processing broadcast ID: ${b.id}`);

          const manualTargets = db.settings?.manualBroadcastTargets || [];
          const whitelistedGroups = db.settings?.whitelistedGroups || [];
          const excluded = db.settings?.excludedBroadcastPhones || [];

          // Find matching targets
          const matchedTargetsMap = new Map<string, { phone: string; name: string }>();

          if (b.targetPhones && Array.isArray(b.targetPhones) && b.targetPhones.length > 0) {
            for (const tgt of b.targetPhones) {
              const cleanPhone = tgt.phone.replace(/[^0-9]/g, "");
              if (excluded.includes(cleanPhone)) continue;
              matchedTargetsMap.set(tgt.phone, { phone: tgt.phone, name: tgt.name });
            }
          } else if (b.targetCategories && Array.isArray(b.targetCategories)) {
            for (const cat of b.targetCategories) {
              if (cat === "group") {
                for (const gJid of whitelistedGroups) {
                  const manualGroup = manualTargets.find((t: any) => t.phone === gJid);
                  const gName = manualGroup ? manualGroup.name : `Grup Whitelist (${gJid.substring(0, 5)})`;
                  matchedTargetsMap.set(gJid, { phone: gJid, name: gName });
                }
                const groupManuals = manualTargets.filter((t: any) => t.category === "group");
                for (const gm of groupManuals) {
                  matchedTargetsMap.set(gm.phone, { phone: gm.phone, name: gm.name });
                }
              } else {
                const matchingManuals = manualTargets.filter(
                  (t: any) => (t.category || "customer").toLowerCase() === cat.toLowerCase()
                );
                for (const m of matchingManuals) {
                  const cleanPhone = m.phone.replace(/[^0-9]/g, "");
                  if (excluded.includes(cleanPhone)) continue;
                  matchedTargetsMap.set(m.phone, { phone: m.phone, name: m.name });
                }
              }
            }
          }

          const targetList = Array.from(matchedTargetsMap.values());
          console.log(`[Scheduler] Broadcast ${b.id} has ${targetList.length} total targets.`);

          if (targetList.length === 0) {
            b.status = "sent";
            b.sentAt = new Date().toISOString();
            b.sendLogs = [];
            this.saveDb(db);
            console.log(`[Scheduler] Broadcast ${b.id} completed with 0 targets.`);
            continue;
          }

          const sendLogs: Array<{ phone: string; name: string; status: 'success' | 'failed'; error?: string; time: string }> = [];
          let successCount = 0;
          let failedCount = 0;

          if (!this.sock) {
            console.log(`[Scheduler] Bot is not connected! Marking broadcast ${b.id} as failed.`);
            b.status = "failed";
            b.sentAt = new Date().toISOString();
            this.saveDb(db);
            continue;
          }

          for (let i = 0; i < targetList.length; i++) {
            const target = targetList[i];
            const personalizedMessage = b.message
              .replace(/{nama}/gi, target.name)
              .replace(/{toko}/gi, db.settings?.storeName || "Wanzz Store");

            const timeNow = new Date().toLocaleTimeString("id-ID") + " WIB";

            try {
              let jid = target.phone.trim();
              if (!jid.endsWith("@s.whatsapp.net") && !jid.endsWith("@g.us")) {
                let clean = jid.replace(/[^0-9]/g, "");
                if (clean.length > 14) {
                  const resolved = resolveLidToPn(`${clean}@lid`);
                  jid = resolved.endsWith("@lid") ? `${clean}@s.whatsapp.net` : resolved;
                } else {
                  if (clean.startsWith("0")) {
                     clean = "62" + clean.slice(1);
                  } else if (clean.startsWith("8")) {
                     clean = "62" + clean;
                  }
                  jid = `${clean}@s.whatsapp.net`;
                }
              }

              console.log(`[Scheduler] Sending to ${target.name} (${jid})...`);

              if (b.mediaUrl && (b.mediaType === "image" || b.mediaType === "video")) {
                const isBase64 = b.mediaUrl.startsWith("data:");
                if (isBase64) {
                  const matched = b.mediaUrl.match(/^data:([a-zA-Z0-9-\/]+);base64,(.+)$/) || b.mediaUrl.match(/^data:image\/([a-zA-Z0-9+-\/]+);base64,(.+)$/);
                  if (matched) {
                    let mime = matched[1];
                    if (!mime.includes("/")) {
                      mime = b.mediaType === "image" ? `image/${mime}` : `video/${mime}`;
                    }
                    const base64Content = matched[2];
                    const buffer = Buffer.from(base64Content, "base64");
                    await this.sock.sendMessage(jid, {
                      [b.mediaType]: buffer,
                      caption: personalizedMessage,
                      mimetype: mime
                    });
                  } else {
                    await this.sock.sendMessage(jid, {
                      [b.mediaType]: { url: b.mediaUrl },
                      caption: personalizedMessage
                    });
                  }
                } else {
                  // Check local files first
                  const folder = b.mediaType === "image" ? "src/images" : "src/video";
                  const filePath = path.join(process.cwd(), folder, b.mediaUrl);
                  if (fs.existsSync(filePath)) {
                    const buffer = fs.readFileSync(filePath);
                    await this.sock.sendMessage(jid, {
                      [b.mediaType]: buffer,
                      caption: personalizedMessage,
                      mimetype: getFileMimeType(b.mediaUrl, b.mediaType === "image" ? "image/png" : "video/mp4")
                    });
                  } else {
                    await this.sock.sendMessage(jid, {
                      [b.mediaType]: { url: b.mediaUrl },
                      caption: personalizedMessage
                    });
                  }
                }
              } else {
                await this.sock.sendMessage(jid, { text: personalizedMessage });
              }

              successCount++;
              sendLogs.push({ phone: target.phone, name: target.name, status: "success", time: timeNow });

              this.addLog({
                from: jid,
                senderName: this.pushName || "WANZZ BOT",
                message: personalizedMessage,
                type: "outgoing",
                status: `Scheduled Broadcast ${b.id} Sent`
              });

            } catch (err: any) {
              console.error(`[Scheduler] Failed sending to ${target.name}:`, err);
              failedCount++;
              sendLogs.push({ phone: target.phone, name: target.name, status: "failed", error: err.message || "Gagal Kirim", time: timeNow });
            }

            if (i < targetList.length - 1) {
              await new Promise(resolve => setTimeout(resolve, delayMs));
            }
          }

          b.status = "sent";
          b.sentAt = new Date().toISOString();
          b.sendLogs = sendLogs;
          this.saveDb(db);
          console.log(`[Scheduler] Broadcast ${b.id} completed. Sent: ${successCount}, Failed: ${failedCount}`);
        }
      } catch (err) {
        console.error("[Scheduler] Error checking/processing scheduled broadcasts:", err);
      }
    }, 15000);
  }

  addLog(log: Omit<MessageLog, "id" | "timestamp">) {
    const fullLog: MessageLog = {
      id: Math.random().toString(36).substring(2, 9),
      timestamp: new Date().toISOString(),
      ...log,
    };
    this.logs.unshift(fullLog);
    if (this.logs.length > 200) {
      this.logs.pop();
    }
  }

  getDb() {
    const defaultCommands = [
      {
        id: "menu",
        trigger: "menu",
        response: `╭ 🌙 {storeName} MENU 🌙
┃ ✦ Welcome to {storeName}
┃ ✦ Fast Response • Trusted • Cheap
┃ ✦ Premium Apps, Streaming & More
╰━━━━━━━━━━━━━━━━━━━━━╯

📂 MENU

• /List
• /Payment
• /Owner
• /Tutor (produk)
• COMING SOON

━━━━━━━━━━━━━━━━━━
📌 Ketik nama produk untuk lihat detail
📌 Contoh: NETFLIX / CANVA / CHATGPT
━━━━━━━━━━━━━━━━━━`,
        mediaUrl: "",
        mediaType: "none",
        description: "Menampilkan pesan utama / menu utama bot"
      },
      {
        id: "list",
        trigger: "list",
        response: `╭ 🌙 {storeName} LIST 🌙 
┃ ✦ Welcome to {storeName}
┃ ✦ Fast Response • Trusted • Cheap
┃ ✦ Premium Apps, Streaming & More
╰━━━━━━━━━━━━━━━━━━━━━╯

📂 *𝗖𝗔𝗧𝗔𝗟𝗢𝗚 𝗣𝗥𝗢𝗗𝗨𝗞*

{catalog}
━━━━━━━━━━━━━━━━━━
📌 *Ketik nama produk untuk lihat detail*
📌 *Contoh: NETFLIX / CANVA / CHATGPT*
━━━━━━━━━━━━━━━━━━`,
        mediaUrl: "",
        mediaType: "none",
        description: "Menampilkan daftar katalog produk yang terbagi per kategori"
      },
      {
        id: "payment",
        trigger: "payment",
        response: `╭━━━〔 💳 PEMBAYARAN 〕━━━╮

📱 DANA
08123456789

📱 OVO
08123456789

📱 GOPAY
08123456789

🌍 PAYPAL
Coming Soon

✅ QRIS tersedia
Silakan scan QRIS di atas.

📸 Setelah transfer,
kirim bukti pembayaran ke owner.

╰━━━━━━━━━━━━━━━━━━╯`,
        mediaUrl: "",
        mediaType: "none",
        description: "Menampilkan informasi pembayaran dan QRIS"
      },
      {
        id: "owner",
        trigger: "owner",
        response: `╭━━━〔 👤 OWNER CONTACT 〕━━━╮

Berikut adalah nomor kontak Owner Resmi *{storeName}*:

📱 *WhatsApp:* +{ownerNumber}
🔗 *Link Chat:* https://wa.me/{ownerNumber}

Silakan hubungi owner untuk keperluan bisnis, keluhan transaksi, atau mendaftar kemitraan/reseller.

╰━━━━━━━━━━━━━━━━━━━━╯`,
        mediaUrl: "",
        mediaType: "none",
        description: "Menampilkan informasi kontak/WhatsApp owner"
      }
    ];

    if (this.cachedDb) {
      // Return cached in-memory DB instantly (fast & quota friendly!)
      return this.cachedDb;
    }

    // 1. Try to migrate from legacy single database.json if it exists
    if (fs.existsSync(DB_PATH)) {
      try {
        console.log("[Migration] Found legacy single database.json. Migrating to split files...");
        const oldData = JSON.parse(fs.readFileSync(DB_PATH, "utf-8"));
        
        if (oldData.products) fs.writeFileSync(DB_PRODUCTS_PATH, JSON.stringify(oldData.products, null, 2), "utf-8");
        if (oldData.categories) fs.writeFileSync(DB_CATEGORIES_PATH, JSON.stringify(oldData.categories, null, 2), "utf-8");
        if (oldData.settings) fs.writeFileSync(DB_SETTINGS_PATH, JSON.stringify(oldData.settings, null, 2), "utf-8");
        if (oldData.commands) fs.writeFileSync(DB_COMMANDS_PATH, JSON.stringify(oldData.commands, null, 2), "utf-8");
        if (oldData.transactions) fs.writeFileSync(DB_TRANSACTIONS_PATH, JSON.stringify(oldData.transactions, null, 2), "utf-8");
        if (oldData.activeTransactions) fs.writeFileSync(DB_ACTIVE_TRANSACTIONS_PATH, JSON.stringify(oldData.activeTransactions, null, 2), "utf-8");
        if (oldData.scheduledBroadcasts) fs.writeFileSync(DB_SCHEDULED_BROADCASTS_PATH, JSON.stringify(oldData.scheduledBroadcasts, null, 2), "utf-8");
        
        // Rename legacy database.json to backup so we don't migrate next time
        fs.renameSync(DB_PATH, DB_PATH + ".bak");
        console.log("[Migration] Legacy database.json migrated successfully to split files!");
      } catch (e) {
        console.error("[Migration] Error during legacy database migration:", e);
      }
    }

    // 2. Load from split database files
    const db: any = {
      categories: [],
      products: [],
      settings: {},
      commands: defaultCommands,
      transactions: [],
      activeTransactions: [],
      scheduledBroadcasts: []
    };

    try {
      if (fs.existsSync(DB_CATEGORIES_PATH)) {
        db.categories = JSON.parse(fs.readFileSync(DB_CATEGORIES_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading categories database:", e); }

    try {
      if (fs.existsSync(DB_PRODUCTS_PATH)) {
        db.products = JSON.parse(fs.readFileSync(DB_PRODUCTS_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading products database:", e); }

    try {
      if (fs.existsSync(DB_SETTINGS_PATH)) {
        db.settings = JSON.parse(fs.readFileSync(DB_SETTINGS_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading settings database:", e); }

    try {
      if (fs.existsSync(DB_COMMANDS_PATH)) {
        db.commands = JSON.parse(fs.readFileSync(DB_COMMANDS_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading commands database:", e); }

    try {
      if (fs.existsSync(DB_TRANSACTIONS_PATH)) {
        db.transactions = JSON.parse(fs.readFileSync(DB_TRANSACTIONS_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading transactions database:", e); }

    try {
      if (fs.existsSync(DB_ACTIVE_TRANSACTIONS_PATH)) {
        db.activeTransactions = JSON.parse(fs.readFileSync(DB_ACTIVE_TRANSACTIONS_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading activeTransactions database:", e); }

    try {
      if (fs.existsSync(DB_SCHEDULED_BROADCASTS_PATH)) {
        db.scheduledBroadcasts = JSON.parse(fs.readFileSync(DB_SCHEDULED_BROADCASTS_PATH, "utf-8"));
      }
    } catch (e) { console.error("Error reading scheduledBroadcasts database:", e); }

    // 3. Ensure defaults & backfills
    let updated = false;
    if (!db.commands || db.commands.length === 0) {
      db.commands = defaultCommands;
      updated = true;
    }
    if (!db.transactions) {
      db.transactions = [];
      updated = true;
    }
    if (!db.activeTransactions) {
      db.activeTransactions = [];
      updated = true;
    }
    if (!db.scheduledBroadcasts) {
      db.scheduledBroadcasts = [];
      updated = true;
    }
    if (db.products && Array.isArray(db.products)) {
      db.products.forEach((p: any) => {
        if (p.stock === undefined) {
          p.stock = 10;
          updated = true;
        }
        if (p.price === undefined) {
          p.price = 0;
          updated = true;
        }
      });
    }

    this.cachedDb = db;
    if (updated) {
      this.saveDb(this.cachedDb);
    }

    return this.cachedDb;
  }

  async asyncFirebaseLoad() {
    if (!isFirebaseEnabled() || this.isSyncingFirebase) {
      return;
    }
    this.isSyncingFirebase = true;
    try {
      console.log("[Firebase] Connecting to Firebase Cloud Firestore...");
      const cloudData = await syncFromFirestore();
      if (cloudData) {
        // Last-Write-Wins timestamp-based sync logic to prevent stale rollbacks
        const localTimeStr = this.cachedDb?.settings?.lastUpdated || "";
        const cloudTimeStr = cloudData.settings?.lastUpdated || "";

        const localTime = localTimeStr ? new Date(localTimeStr).getTime() : 0;
        const cloudTime = cloudTimeStr ? new Date(cloudTimeStr).getTime() : 0;

        console.log(`[Firebase] Timestamp Compare -> Local: ${localTimeStr || "None"} (${localTime}) vs Cloud: ${cloudTimeStr || "None"} (${cloudTime})`);

        if (cloudTime > localTime) {
          console.log("[Firebase] Cloud Firestore database is newer. Syncing cloud data to local cache.");
          this.cachedDb = cloudData;
          this.saveDb(cloudData);
        } else if (localTime > cloudTime) {
          console.log("[Firebase] Local cache database is newer than Cloud Firestore. Seeding newer local data to Cloud Firestore.");
          await syncToFirestore(this.cachedDb);
        } else {
          console.log("[Firebase] Local cache and Cloud Firestore are perfectly in sync.");
        }
      }
    } catch (e) {
      console.error("[Firebase] Error during background synchronization:", e);
    } finally {
      this.isSyncingFirebase = false;
    }
  }

  saveDb(data: any) {
    try {
      if (!data) return false;

      // Ensure settings exists and set automated timestamp to prevent stale rollbacks
      if (!data.settings) {
        data.settings = {};
      }
      data.settings.lastUpdated = new Date().toISOString();

      const oldDb = this.cachedDb || {};
      this.cachedDb = data;

      // Unpack and save each partition as a separate JSON database file
      // Check if data changed before writing, or write as split files
      if (data.categories !== undefined && JSON.stringify(data.categories) !== JSON.stringify(oldDb.categories)) {
        fs.writeFileSync(DB_CATEGORIES_PATH, JSON.stringify(data.categories, null, 2), "utf-8");
      }
      if (data.products !== undefined && JSON.stringify(data.products) !== JSON.stringify(oldDb.products)) {
        fs.writeFileSync(DB_PRODUCTS_PATH, JSON.stringify(data.products, null, 2), "utf-8");
      }
      if (data.settings !== undefined && JSON.stringify(data.settings) !== JSON.stringify(oldDb.settings)) {
        fs.writeFileSync(DB_SETTINGS_PATH, JSON.stringify(data.settings, null, 2), "utf-8");
      }
      if (data.commands !== undefined && JSON.stringify(data.commands) !== JSON.stringify(oldDb.commands)) {
        fs.writeFileSync(DB_COMMANDS_PATH, JSON.stringify(data.commands, null, 2), "utf-8");
      }
      if (data.transactions !== undefined && JSON.stringify(data.transactions) !== JSON.stringify(oldDb.transactions)) {
        fs.writeFileSync(DB_TRANSACTIONS_PATH, JSON.stringify(data.transactions, null, 2), "utf-8");
      }
      if (data.activeTransactions !== undefined && JSON.stringify(data.activeTransactions) !== JSON.stringify(oldDb.activeTransactions)) {
        fs.writeFileSync(DB_ACTIVE_TRANSACTIONS_PATH, JSON.stringify(data.activeTransactions, null, 2), "utf-8");
      }
      if (data.scheduledBroadcasts !== undefined && JSON.stringify(data.scheduledBroadcasts) !== JSON.stringify(oldDb.scheduledBroadcasts)) {
        fs.writeFileSync(DB_SCHEDULED_BROADCASTS_PATH, JSON.stringify(data.scheduledBroadcasts, null, 2), "utf-8");
      }

      // If oldDb is empty (first load save / fallback), write everything
      if (Object.keys(oldDb).length === 0) {
        fs.writeFileSync(DB_CATEGORIES_PATH, JSON.stringify(data.categories || [], null, 2), "utf-8");
        fs.writeFileSync(DB_PRODUCTS_PATH, JSON.stringify(data.products || [], null, 2), "utf-8");
        fs.writeFileSync(DB_SETTINGS_PATH, JSON.stringify(data.settings || {}, null, 2), "utf-8");
        fs.writeFileSync(DB_COMMANDS_PATH, JSON.stringify(data.commands || [], null, 2), "utf-8");
        fs.writeFileSync(DB_TRANSACTIONS_PATH, JSON.stringify(data.transactions || [], null, 2), "utf-8");
        fs.writeFileSync(DB_ACTIVE_TRANSACTIONS_PATH, JSON.stringify(data.activeTransactions || [], null, 2), "utf-8");
        fs.writeFileSync(DB_SCHEDULED_BROADCASTS_PATH, JSON.stringify(data.scheduledBroadcasts || [], null, 2), "utf-8");
      }

      return true;
    } catch (e) {
      console.error("Error writing split databases:", e);
      return false;
    }
  }

  // Auto clean stale auth keys if needed
  async disconnect() {
    this.isManualDisconnect = true;
    this.status = "disconnected";
    this.qrCode = "";
    this.pairingCode = "";
    this.phoneNumber = "";
    this.pushName = "";
    this.error = "";
    this.reconnectAttempts = 0;

    if (this.sock) {
      try {
        this.sock.logout();
        this.sock.end(undefined);
      } catch (err) {
        console.error("Error during socket logout:", err);
      }
      this.sock = null;
    }

    try {
      // Clean auth session directory
      if (fs.existsSync(AUTH_DIR)) {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        console.log("Auth directory wiped successfully");
      }
    } catch (e) {
      console.error("Error deleting auth dir:", e);
    }

    this.addLog({
      from: "System",
      senderName: "System",
      message: "WhatsApp session disconnected & logged out.",
      type: "system",
      status: "Disconnected",
    });
  }

  async initialize(pairingPhone?: string) {
    this.isManualDisconnect = false;
    // If already connecting or connected, don't re-init unless explicit disconnect
    if (this.status === "connected" && !pairingPhone) {
      return;
    }

    this.status = "connecting";
    this.qrCode = "";
    this.pairingCode = "";
    this.error = "";

    // Clear any existing socket event listeners and connection cleanly before re-initializing
    if (this.sock) {
      try {
        console.log("Terminating existing socket connection before re-initializing...");
        if (this.sock.ev) {
          const ev = this.sock.ev;
          if (typeof ev.removeAllListeners === "function") {
            ev.removeAllListeners("connection.update");
            ev.removeAllListeners("creds.update");
            ev.removeAllListeners("messages.upsert");
            ev.removeAllListeners("presence.update");
          }
        }
        if (typeof this.sock.end === "function") {
          this.sock.end(undefined);
        }
      } catch (err) {
        console.error("Failed to end old socket during re-init:", err);
      }
      this.sock = null;
    }

    this.addLog({
      from: "System",
      senderName: "System",
      message: pairingPhone 
        ? `Menghubungkan via Pairing Code ke nomor ${pairingPhone}...` 
        : "Menghubungkan bot & memuat credentials...",
      type: "system",
      status: "Connecting",
    });

    try {
      const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);

      this.sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: "silent" }) as any,
        browser: ["Ubuntu", "Chrome", "110.0.0"],
      });

      // Credential Update Event
      this.sock.ev.on("creds.update", saveCreds);

      // Connection Update Event
      this.sock.ev.on("connection.update", async (update: any) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
          try {
            // Generate QR data URL for react
            this.qrCode = await QRCode.toDataURL(qr);
            this.status = "disconnected"; // QR available, waiting for scan
            this.error = "";
          } catch (err) {
            console.error("Failed to generate QR Code image:", err);
            this.qrCode = qr; // Fallback to raw string
          }
        }

        if (connection === "connecting") {
          this.status = "connecting";
        }

        if (connection === "open") {
          this.status = "connected";
          this.qrCode = "";
          this.pairingCode = "";
          this.reconnectAttempts = 0;
          this.error = "";
          
          // Capture user details
          const userJid = this.sock.user?.id;
          this.phoneNumber = userJid ? userJid.split(":")[0] : "";
          this.pushName = this.sock.user?.name || "Wanzz Store Bot";

          this.addLog({
            from: "System",
            senderName: "System",
            message: `Bot berhasil terhubung! Login sebagai: ${this.pushName} (${this.phoneNumber})`,
            type: "system",
            status: "Connected",
          });
        }

        if (connection === "close") {
          const hasError = lastDisconnect?.error !== undefined;
          const errCode = (lastDisconnect?.error as any)?.output?.statusCode || (lastDisconnect?.error as any)?.statusCode;
          const errorMessage = lastDisconnect?.error?.message || (hasError ? String(lastDisconnect.error) : "Normal Disconnect / Connection closed by client");
          
          const isLoggedOut = errCode === DisconnectReason.loggedOut || 
                             errorMessage.includes("Unauthorized") || 
                             errorMessage.includes("unauthorized") ||
                             errCode === 401 ||
                             errCode === 403;

          const isQrTimeout = errorMessage.includes("QR refs attempts ended") || 
                              errorMessage.includes("attempts ended");

          const isStreamError = errorMessage.includes("Stream Errored") || 
                                errorMessage.includes("restart required") ||
                                errorMessage.includes("515") ||
                                errCode === 515 ||
                                (lastDisconnect?.error as any)?.data?.tag === "stream:error" ||
                                (lastDisconnect?.error as any)?.data?.attrs?.code === "515";

          const shouldReconnect = hasError && !this.isManualDisconnect && !isLoggedOut && !isQrTimeout && errCode !== DisconnectReason.badSession;
          
          console.log(`Connection closed (code: ${errCode}). Status details: ${isStreamError ? 'Stream transition' : errorMessage}`);
          
          this.qrCode = "";
          this.pairingCode = "";

          // CRITICAL: Clean up current socket and listeners immediately to prevent duplicate events or memory leaks
          if (this.sock) {
            try {
              if (this.sock.ev) {
                if (typeof this.sock.ev.removeAllListeners === "function") {
                  this.sock.ev.removeAllListeners("connection.update");
                  this.sock.ev.removeAllListeners("creds.update");
                  this.sock.ev.removeAllListeners("messages.upsert");
                  this.sock.ev.removeAllListeners("presence.update");
                }
              }
              if (typeof this.sock.end === "function") {
                this.sock.end(undefined);
              }
            } catch (e) {
              console.error("Error cleaning up closed socket in handler:", e);
            }
            this.sock = null;
          }

          if (isQrTimeout) {
            this.status = "disconnected";
            this.error = "Batas waktu QR Code terlampaui (QR refs attempts ended). Silakan muat ulang halaman atau klik tombol hubungkan kembali.";
            this.addLog({
              from: "System",
              senderName: "System",
              message: "Batas waktu pemindaian QR Code dari WhatsApp telah berakhir. Hubungkan kembali manual.",
              type: "system",
              status: "QR Timeout",
            });
          } else if (shouldReconnect) {
            if (isStreamError) {
              // Stream errors (code 515) are transient websocket drops. We handle them by resetting reconnectAttempts
              // to ensure infinite auto-recovery.
              this.reconnectAttempts = 0;
            } else {
              this.reconnectAttempts++;
            }

            if (isStreamError || this.reconnectAttempts <= this.maxReconnectAttempts) {
              this.status = "connecting";
              
              let reconnectReasonMessage = errorMessage;
              if (isStreamError) {
                reconnectReasonMessage = "Stream Errored / Restart Required (Code 515)";
              }
              
              const attemptsText = isStreamError ? "Otomatis" : `${this.reconnectAttempts}/${this.maxReconnectAttempts}`;
              this.error = `Koneksi Bermasalah: ${reconnectReasonMessage} (Mencoba menghubungkan ulang [${attemptsText}])`;
              
              this.addLog({
                from: "System",
                senderName: "System",
                message: `Mencoba menghubungkan kembali ([${attemptsText}]) karena: ${reconnectReasonMessage}`,
                type: "system",
                status: "Reconnecting",
              });

              setTimeout(() => this.initialize(), isStreamError ? 2000 : 5000); // Retry reconnect in 2s for stream errors
            } else {
              this.status = "disconnected";
              this.error = `Gagal Terhubung: ${errorMessage}. Batas percobaan hubungkan ulang terlampaui.`;
              
              this.addLog({
                from: "System",
                senderName: "System",
                message: `Gagal menyambung otomatis: ${errorMessage}. Silakan klik tombol 'Hubungkan' secara manual.`,
                type: "system",
                status: "Failed",
              });
            }
          } else {
            this.status = "disconnected";
            
            if (isLoggedOut) {
              this.error = "Terputus: Sesi tidak sah atau telah keluar (Unauthorized / Logged Out).";
            } else if (this.isManualDisconnect) {
              this.error = "Sesi ditutup secara manual oleh Pengguna.";
            } else {
              this.error = `Koneksi Terputus: ${errorMessage}.`;
            }

            // Clean credentials to allow fresh start next time only if manually disconnected or logged out
            if (this.isManualDisconnect || isLoggedOut) {
              if (fs.existsSync(AUTH_DIR)) {
                try {
                  fs.rmSync(AUTH_DIR, { recursive: true, force: true });
                } catch (e) {
                  console.error("Failed to clean authorization directory:", e);
                }
              }
            }

            const cleanMsg = this.isManualDisconnect 
              ? "Koneksi WhatsApp dihentikan secara manual." 
              : `Koneksi WhatsApp dihentikan: ${errorMessage}.`;

            this.addLog({
              from: "System",
              senderName: "System",
              message: cleanMsg,
              type: "system",
              status: this.isManualDisconnect ? "Disconnected" : "Session Reset",
            });
          }
        }
      });

      // Messages Upsert (Incoming chat listener)
      this.sock.ev.on("messages.upsert", async (m: any) => {
        if (m.type !== "notify") return;
        for (const msg of m.messages) {
          // Normalize LID JIDs to real Phone Numbers if present
          if (msg.key) {
            if (msg.key.remoteJid && msg.key.remoteJid.endsWith("@lid")) {
              msg.key.remoteJid = resolveLidToPn(msg.key.remoteJid);
            }
            if (msg.key.participant && msg.key.participant.endsWith("@lid")) {
              msg.key.participant = resolveLidToPn(msg.key.participant);
            }
          }
          if (msg.participant && msg.participant.endsWith("@lid")) {
            msg.participant = resolveLidToPn(msg.participant);
          }

          // Guard criteria: skip groups, self-messages (temporarily allowed based on user request)
          if (msg.message) {
            const from = msg.key.remoteJid;
            if (!from) continue;

            // Track sender as active/online in this group if it is a group message
            if (from.endsWith("@g.us") && msg.key.participant) {
              const participantJid = msg.key.participant;
              if (!this.presencesMap) {
                this.presencesMap = {};
              }
              if (!this.presencesMap[from]) {
                this.presencesMap[from] = new Set();
              }
              this.presencesMap[from].add(participantJid);
            }

            // Extract content
            let body = msg.message.conversation || 
                       msg.message.extendedTextMessage?.text || 
                       msg.message.imageMessage?.caption || "";

            if (!body && msg.message) {
              if (msg.message.buttonsResponseMessage) {
                body = msg.message.buttonsResponseMessage.selectedButtonId || "";
              } else if (msg.message.listResponseMessage) {
                body = msg.message.listResponseMessage.singleSelectReply?.selectedRowId || "";
              } else if (msg.message.templateButtonReplyMessage) {
                body = msg.message.templateButtonReplyMessage.selectedId || "";
              } else if (msg.message.interactiveResponseMessage) {
                try {
                  const interMsg = msg.message.interactiveResponseMessage;
                  if (interMsg.nativeFlowResponseMessage?.paramsJson) {
                    body = JSON.parse(interMsg.nativeFlowResponseMessage.paramsJson).id || "";
                  }
                } catch (e) {
                  console.error("Error parsing interactiveResponseMessage:", e);
                }
              }
            }
            
            const senderName = msg.pushName || "Pelanggan";

            if (body) {
              await this.handleIncomingMessage(from, body, senderName, false, msg);
            }
          }
        }
      });

      // Presence Update (Track online members of groups)
      this.sock.ev.on("presence.update", (json: any) => {
        try {
          const from = json.id; // group JID or personal user JID
          if (!from) return;
          const presences = json.presences;
          if (!presences) return;

          for (const [memberId, presenceData] of Object.entries(presences)) {
            const data = presenceData as any;
            const isOnline = data?.lastKnownPresence === "composing" || 
                             data?.lastKnownPresence === "available" || 
                             data?.lastKnownPresence === "recording";
            
            const groupJid = from.endsWith("@g.us") ? from : "global";
            if (!this.presencesMap) {
              this.presencesMap = {};
            }
            if (!this.presencesMap[groupJid]) {
              this.presencesMap[groupJid] = new Set();
            }

            if (isOnline) {
              this.presencesMap[groupJid].add(memberId);
            } else {
              if (data?.lastKnownPresence === "unavailable") {
                this.presencesMap[groupJid].delete(memberId);
              }
            }
          }
        } catch (err) {
          console.error("Error handling presence.update event:", err);
        }
      });

      // Request pairing code if phone number is provided
      if (pairingPhone && !this.sock.authState?.creds?.registered) {
        // Delay slightly for socket setup
        setTimeout(async () => {
          try {
            const cleanedPhone = pairingPhone.replace(/[^0-9]/g, "");
            console.log(`Requesting pairing code for ${cleanedPhone}...`);
            const pCode = await this.sock.requestPairingCode(cleanedPhone);
            this.pairingCode = pCode;
            this.status = "connecting"; // Set structure to connecting
            this.addLog({
              from: "System",
              senderName: "System",
              message: `Pairing Code Berhasil Dibuat: ${pCode}. Masukkan kode ini ke WhatsApp Anda.`,
              type: "system",
              status: "Pairing Code Generated",
            });
          } catch (err: any) {
            console.error("Failed to generate pairing code:", err);
            this.error = "Gagal membuat pairing code. Pastikan format nomor benar (gunakan kode negara, cth: 6281xx).";
            this.status = "disconnected";
            this.addLog({
              from: "System",
              senderName: "System",
              message: `Gagal membuat Pairing Code: ${err.message || err}`,
              type: "system",
              status: "Pairing Failed",
            });
          }
        }, 2000);
      }

    } catch (err: any) {
      console.error("Error creating Baileys socket:", err);
      this.status = "disconnected";
      this.error = err.message || String(err);
    }
  }

  // Command & Product response processor
  async handleIncomingMessage(
    from: string, 
    messageText: string, 
    senderName: string, 
    isSimulation: boolean = false,
    rawMsg?: any
  ): Promise<{ response: string; command: string; status: string; hasImage: boolean; mediaUrl?: string; mediaType?: string; buttons?: Array<{ id: string; text: string }>; isSingleSelect?: boolean; buttonTitle?: string }> {
    
    const db = this.getDb();
    const settings = db.settings;
    const text = messageText.trim();
    const lowerText = text.toLowerCase();

    let responseText = "";
    let statusText = "Unhandled";
    let matchedCommand = "none";
    let hasImage = false;
    let mediaUrlToSend = "";
    let mediaTypeToSend = "";

    // 1. Log incoming message
    if (!isSimulation) {
      this.addLog({
        from,
        senderName,
        message: text,
        type: "incoming",
        status: "Received",
      });
    }

    // 1b. Spam Protection
    const now = Date.now();
    if (!this.spamTracker) {
      this.spamTracker = {};
    }
    if (!this.spamTracker[from]) {
      this.spamTracker[from] = { lastMessage: "", count: 0, cooldownUntil: 0, warned: false };
    }

    const tracker = this.spamTracker[from];

    // Check if user is currently on cooldown
    if (tracker.cooldownUntil > now) {
      const remainingMinutes = Math.ceil((tracker.cooldownUntil - now) / 60000);
      const blockedResponse = `🛡️ *SISTEM ANTI-SPAM*\n\nMaaf, Anda dideteksi melakukan spamming. Bot dinonaktifkan sementara untuk Anda.\nSilakan coba lagi dalam *${remainingMinutes} menit*.`;
      return {
        response: blockedResponse,
        command: "spam_blocked",
        status: "Blocked",
        hasImage: false
      };
    }

    // Track consecutive duplicate messages
    if (tracker.lastMessage === text) {
      tracker.count += 1;
    } else {
      tracker.lastMessage = text;
      tracker.count = 1;
    }

    // Trigger cooldown if count reaches 5
    if (tracker.count >= 5) {
      tracker.cooldownUntil = now + 5 * 60 * 1000; // 5 minutes block
      tracker.count = 0; // reset
      tracker.warned = true;

      const cooldownWarning = `⚠️ *SISTEM ANTI-SPAM*\n\nAnda mengirimkan pesan yang sama sebanyak 5 kali berturut-turut.\n*Bot ditangguhkan sementara selama 5 menit* untuk menjaga performa server.\nSilakan coba lagi beberapa saat lagi!`;

      // Send the WhatsApp notification immediately if live connected
      if (!isSimulation && this.sock && this.status === "connected") {
        try {
          await this.sock.sendMessage(from, { text: cooldownWarning });
        } catch (err) {
          console.error("Failed to send cooldown warning to WA:", err);
        }
      }

      return {
        response: cooldownWarning,
        command: "spam_triggered",
        status: "Spam Blocked",
        hasImage: false
      };
    }

    // Helper to format templates safely
    const format = (template: string | undefined | null, vars: Record<string, any>) => {
      if (!template) return "";
      let res = template;
      for (const [k, v] of Object.entries(vars)) {
        const val = v !== undefined && v !== null ? String(v) : "";
        res = res.replace(new RegExp(`{${k}}`, "g"), val);
      }
      return res;
    };

    // Check for required prefixes: /, !, or . for non-catalog commands
    const hasPrefix = text.startsWith("/") || text.startsWith("!") || text.startsWith(".");
    const commandText = hasPrefix ? text.substring(1).trim() : "";
    const lowerCommandText = commandText.toLowerCase();

    // 2. Identify Command / Triggers (must be prefixed with /, !, or .)
    const isHideTagCommand = hasPrefix && (lowerCommandText === "h" || lowerCommandText.startsWith("h ") || lowerCommandText.startsWith("h\n"));
    const isGreeting = hasPrefix && ["halo", "p", "hi", "hello", "hei", "start", "bot", "bantuan", "help"].includes(lowerCommandText);
    const isMenuTrigger = hasPrefix && lowerCommandText === "menu";
    const isOwnerTrigger = hasPrefix && lowerCommandText === "owner";
    const isPaymentTrigger = hasPrefix && ["payment", "qris", "bayar"].includes(lowerCommandText);
    const isIdGrupTrigger = hasPrefix && lowerCommandText === "idgrup";
    const isKickTrigger = hasPrefix && (lowerCommandText === "kick" || lowerCommandText.startsWith("kick "));
    const isAddTrigger = hasPrefix && (lowerCommandText === "add" || lowerCommandText.startsWith("add "));
    const isBcAddTrigger = hasPrefix && (lowerCommandText === "bcadd" || lowerCommandText.startsWith("bcadd "));
    const isCloseTrigger = hasPrefix && lowerCommandText === "close";
    const isOpenTrigger = hasPrefix && lowerCommandText === "open";
    const isOnlineTrigger = hasPrefix && lowerCommandText === "online";
    const isInfoCommand = hasPrefix && lowerCommandText.startsWith("info");
    const isProsesTrigger = hasPrefix && lowerCommandText === "proses";
    const isSelesaiTrigger = hasPrefix && lowerCommandText === "selesai";
    const isGagalTrigger = hasPrefix && lowerCommandText === "gagal";
    const isOrderTrigger = false;

    // Group products & compile catalog layout for potential list or dynamic catalog template variables
    let catalogText = "";
    for (const cat of db.categories) {
      const catProducts = db.products.filter((p: any) => p.category === cat.id);
      if (catProducts.length > 0) {
        catalogText += `\n🎬 *${cat.name.toUpperCase()}*\n`;
        catProducts.forEach((p: any) => {
          catalogText += `┊ ${p.name.toUpperCase()}\n`;
        });
      }
    }

    // Try to find matching command in commands db first (e.g. .list, /list, /menu, !bayar triggers)
    const matchedCommandObj = hasPrefix ? (db.commands || []).find((c: any) => {
      const triggers = c.trigger.split(",").map((t: string) => t.trim().toLowerCase());
      return triggers.includes(lowerCommandText);
    }) : null;

    const commandVars = {
      storeName: settings.storeName || "WANZZ STORE",
      ownerNumber: settings.ownerNumber || "6285712439395",
      catalog: catalogText,
      name: senderName,
    };

    // 2b. Compute Sender Roles early (Owner and Admin status)
    let isSenderOwner = false;
    let isSenderAdmin = false;

    if (isSimulation) {
      const lowerSimSender = senderName.toLowerCase();
      isSenderOwner = lowerSimSender.includes("owner");
      isSenderAdmin = lowerSimSender.includes("admin");
    } else {
      const rawOwnerPhone = String(settings.ownerNumber || "6285712439395");
      const cleanOwnerPhone = rawOwnerPhone.replace(/[^0-9]/g, "");

      let senderPhone = "";
      const isGroup = from.endsWith("@g.us");
      if (isGroup) {
        senderPhone = rawMsg?.key?.participant ? rawMsg.key.participant.split("@")[0] : "";
      } else {
        senderPhone = from.split("@")[0] || "";
      }
      const cleanSenderPhone = String(senderPhone).replace(/[^0-9]/g, "");
      isSenderOwner = cleanSenderPhone === cleanOwnerPhone;

      if (isGroup && !isSenderOwner && this.sock) {
        try {
          const metadata = await this.sock.groupMetadata(from);
          const senderJid = rawMsg?.key?.participant || "";
          const senderParticipant = metadata.participants.find((p: any) => p.id === senderJid);
          isSenderAdmin = senderParticipant?.admin !== undefined && senderParticipant?.admin !== null;
        } catch (err) {
          console.error("Error checking sender admin status in validation:", err);
        }
      }
    }

    // Identify if the command trigger is recognized
    const isRecognizedCommand = isGreeting || isMenuTrigger || isOwnerTrigger || isPaymentTrigger || isIdGrupTrigger || isKickTrigger || isAddTrigger || isBcAddTrigger || isCloseTrigger || isOpenTrigger || isOnlineTrigger || isInfoCommand || isProsesTrigger || isSelesaiTrigger || isGagalTrigger || isHideTagCommand || lowerCommandText === "list" || !!matchedCommandObj;

    // Define command categories
    const isOwnerOnlyCommand = isIdGrupTrigger || isBcAddTrigger;
    const isAdminOnlyCommand = isKickTrigger || isAddTrigger || isCloseTrigger || isOpenTrigger || isOnlineTrigger || isHideTagCommand || isProsesTrigger || isSelesaiTrigger || isGagalTrigger;

    // --- MANDATORY ACCESS & WHITELIST VALIDATIONS ---
    const isGroup = from.endsWith("@g.us");

    if (isRecognizedCommand) {
      // Langkah 1 & 2: Whitelist Validation for Group Messages
      if (isGroup) {
        const whitelistedGroups = settings.whitelistedGroups || ["120363425916568709@g.us"];
        const isTargetGroup = whitelistedGroups.includes(from);

        if (!isTargetGroup) {
          // Pengecualian: /idgrup tetap boleh diproses apabila pengirim adalah owner.
          if (isIdGrupTrigger) {
            if (!isSenderOwner) {
              // Jika pengirim bukan owner: /idgrup ditolak dengan Owner Only Command Message
              matchedCommand = "owner_restricted";
              statusText = "Owner Restricted Command";
              responseText = format(settings.ownerRestrictedTemplate || "Command ini hanya dapat diakses oleh Owner bot!", {
                storeName: settings.storeName || "WANZZ STORE"
              });

              // Send the WhatsApp notification immediately if live connected
              if (!isSimulation && responseText && this.sock && this.status === "connected") {
                try {
                  await this.sock.sendMessage(from, { text: responseText }, { quoted: rawMsg });
                  this.addLog({
                    from,
                    senderName: this.pushName,
                    message: responseText,
                    type: "outgoing",
                    status: statusText,
                  });
                } catch (err) {
                  console.error("Failed to send /idgrup restriction message:", err);
                }
              }

              return {
                response: responseText,
                command: matchedCommand,
                status: statusText,
                hasImage: false
              };
            }
            // If they are owner, let it fall through to execution of /idgrup normally!
          } else {
            // Abaikan seluruh command secara total, bot harus sepenuhnya diam
            return {
              response: "",
              command: "none",
              status: "Ignored (Non-Whitelisted Group)",
              hasImage: false
            };
          }
        }
      }

      // Langkah 3 & 4: Access validations depending on Category
      if (isOwnerOnlyCommand) {
        if (!isSenderOwner) {
          matchedCommand = "owner_restricted";
          statusText = "Owner Restricted Command";
          responseText = format(settings.ownerRestrictedTemplate || "Command ini hanya dapat diakses oleh Owner bot!", {
            storeName: settings.storeName || "WANZZ STORE"
          });

          // Send rejection response
          if (!isSimulation && responseText && this.sock && this.status === "connected") {
            try {
              await this.sock.sendMessage(from, { text: responseText }, { quoted: rawMsg });
              this.addLog({
                from,
                senderName: this.pushName,
                message: responseText,
                type: "outgoing",
                status: statusText,
              });
            } catch (err) {
              console.error("Failed to send owner restriction message:", err);
            }
          }

          return {
            response: responseText,
            command: matchedCommand,
            status: statusText,
            hasImage: false
          };
        }
      }

      if (isAdminOnlyCommand) {
        if (!isGroup) {
          // Admin Only commands run outside group (e.g. Private Chat)
          // For /kick, /add, /close, /open, /online: let them roll down to their existing sub-handlers which throw outside-group rejections
          // For /proses, /selesai, /gagal, /h: enforce prompt's rejection
          if (isProsesTrigger || isSelesaiTrigger || isGagalTrigger || isHideTagCommand) {
            matchedCommand = "admin_outside_group";
            statusText = "Admin Private Chat Ignored";
            responseText = format(settings.kickOutsideGroupTemplate || "❌ Command ini hanya dapat digunakan di dalam grup!", {
              storeName: settings.storeName || "WANZZ STORE"
            });

            if (!isSimulation && responseText && this.sock && this.status === "connected") {
              try {
                await this.sock.sendMessage(from, { text: responseText }, { quoted: rawMsg });
                this.addLog({
                  from,
                  senderName: this.pushName,
                  message: responseText,
                  type: "outgoing",
                  status: statusText,
                });
              } catch (err) {
                console.error("Failed to send admin-only outside group warning:", err);
              }
            }

            return {
              response: responseText,
              command: matchedCommand,
              status: statusText,
              hasImage: false
            };
          }
        } else {
          // Group Admin Only Validations: must be Owner or Admin
          if (!isSenderOwner && !isSenderAdmin) {
            matchedCommand = "admin_restricted";
            statusText = "Admin Restricted Command";
            responseText = format(settings.adminRestrictedTemplate || "Command ini hanya dapat diakses oleh Admin grup dan Owner bot!", {
              storeName: settings.storeName || "WANZZ STORE"
            });

            // Send rejection response
            if (!isSimulation && responseText && this.sock && this.status === "connected") {
              try {
                await this.sock.sendMessage(from, { text: responseText }, { quoted: rawMsg });
                this.addLog({
                  from,
                  senderName: this.pushName,
                  message: responseText,
                  type: "outgoing",
                  status: statusText,
                });
              } catch (err) {
                console.error("Failed to send admin restricted warning:", err);
              }
            }

            return {
              response: responseText,
              command: matchedCommand,
              status: statusText,
              hasImage: false
            };
          }
        }
      }

      if (isPaymentTrigger && !isGroup) {
        // Group Only check for Payment commands (/payment, /bayar, /qris)
        matchedCommand = "payment_restricted";
        statusText = "Payment Group Only Ignore";
        responseText = format(settings.paymentGroupOnlyTemplate || "⚠️ Command ini hanya dapat digunakan di dalam grup!", {
          storeName: settings.storeName || "WANZZ STORE"
        });

        // Send payment outside warning response
        if (!isSimulation && responseText && this.sock && this.status === "connected") {
          try {
            await this.sock.sendMessage(from, { text: responseText }, { quoted: rawMsg });
            this.addLog({
              from,
              senderName: this.pushName,
              message: responseText,
              type: "outgoing",
              status: statusText,
            });
          } catch (err) {
            console.error("Failed to send payment outside group warning:", err);
          }
        }

        return {
          response: responseText,
          command: matchedCommand,
          status: statusText,
          hasImage: false
        };
      }
    }

    if (matchedCommandObj) {
      const isGroup = from.endsWith("@g.us");
      if (matchedCommandObj.isGroupOnly && !isGroup) {
        matchedCommand = "group_restricted";
        statusText = "Group Only Intercept";
        responseText = "⚠️ Maaf, command ini diatur khusus agar hanya dapat digunakan di dalam grup WhatsApp saja!";
        hasImage = false;
        mediaUrlToSend = "";
        mediaTypeToSend = "none";
      } else {
        matchedCommand = lowerCommandText;
        statusText = `Sent ${matchedCommandObj.trigger.split(",")[0].trim()}`;
        
        let responseTemplate = matchedCommandObj.response;
        let commandMediaUrl = matchedCommandObj.mediaUrl || "";
        let commandMediaType = matchedCommandObj.mediaType || "none";
        
        if (matchedCommandObj.id === "owner" && settings.ownerTemplate) {
          responseTemplate = settings.ownerTemplate;
          commandMediaUrl = settings.ownerImageUrl || "";
          commandMediaType = settings.ownerImageUrl ? "image" : "none";
        }
        
        responseText = format(responseTemplate, commandVars);
        hasImage = commandMediaType === "image";
        mediaUrlToSend = commandMediaUrl;
        mediaTypeToSend = commandMediaType;
      }
    } else if (isGreeting) {
      matchedCommand = "greeting";
      statusText = "Sent Welcome";
      responseText = format(settings.welcomeTemplate, {
        name: senderName,
        storeName: settings.storeName,
      });
      if (settings.welcomeImageUrl) {
        hasImage = true;
        mediaUrlToSend = settings.welcomeImageUrl;
        mediaTypeToSend = "image";
      }
    } else if (isHideTagCommand) {
      const isGroup = from.endsWith("@g.us");
      if (isGroup) {
        matchedCommand = "hidetag";
        statusText = "Sent HideTag Message";
        let msg = "";
        if (commandText.substring(1).startsWith("\n") || commandText.substring(1).startsWith(" ")) {
          msg = commandText.substring(2).trim();
        } else {
          msg = commandText.substring(1).trim();
        }
        responseText = msg;
      } else {
        matchedCommand = "hidetag_restricted";
        statusText = "HideTag Personal Ignore";
        responseText = "";
      }
    } else if (isMenuTrigger) {
      matchedCommand = "menu";
      statusText = "Sent Menu";
      hasImage = settings.sendMenuWithImage;

      responseText = format(settings.menuTemplate, {
        storeName: settings.storeName,
        catalog: catalogText,
      });
      if (hasImage) {
        mediaUrlToSend = settings.menuImageUrl || "";
        mediaTypeToSend = "image";
      }
    } else if (isOwnerTrigger) {
      matchedCommand = "owner";
      statusText = "Sent Owner Contact";
      responseText = format(settings.ownerTemplate || "Hubungi owner kami di nomor: {ownerNumber}", {
        storeName: settings.storeName,
        ownerNumber: settings.ownerNumber || "628123456789",
      });
      if (settings.ownerImageUrl) {
        hasImage = true;
        mediaUrlToSend = settings.ownerImageUrl;
        mediaTypeToSend = "image";
      }
    } else if (isPaymentTrigger) {
      matchedCommand = "payment";
      statusText = "Sent Payment Info";
      hasImage = !!settings.paymentQrisUrl;
      responseText = format(settings.paymentTemplate || "Konfigurasi pembayaran belum diset.", {
        storeName: settings.storeName,
      });
      if (hasImage) {
        mediaUrlToSend = settings.paymentQrisUrl || "";
        mediaTypeToSend = "image";
      }
    } else if (isIdGrupTrigger) {
      matchedCommand = "idgrup";
      statusText = "Sent Group ID";
      const isGroup = from.endsWith("@g.us");
      if (isGroup) {
        responseText = format(settings.idGroupSuccessTemplate || "ID Grup: {groupId}", {
          groupId: from,
          storeName: settings.storeName || "WANZZ STORE"
        });
      } else {
        responseText = format(settings.idGroupPrivateTemplate || "di luar grup", {
          storeName: settings.storeName || "WANZZ STORE"
        });
      }
    } else if (isKickTrigger) {
      const isGroup = from.endsWith("@g.us");
      if (!isGroup) {
        responseText = format(settings.kickOutsideGroupTemplate || "❌ Command ini hanya dapat digunakan di dalam grup!", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "kick_outside_group";
        statusText = "Kick Private Chat Ignored";
      } else {
        let targetJid: string | null = null;
        let targetMentionOrNum = "";

        if (isSimulation) {
          const kickArg = commandText.substring(4).trim();
          if (kickArg.startsWith("@")) {
            targetMentionOrNum = kickArg;
            const digits = kickArg.replace(/[^0-9]/g, "");
            targetJid = digits ? `${digits}@s.whatsapp.net` : "628123456789@s.whatsapp.net";
          } else if (kickArg) {
            let digits = kickArg.replace(/[^0-9]/g, "");
            if (digits.startsWith("0")) {
              digits = "62" + digits.substring(1);
            }
            targetMentionOrNum = `@${digits}`;
            targetJid = digits ? `${digits}@s.whatsapp.net` : null;
          } else {
            targetMentionOrNum = "@628123456789";
            targetJid = "628123456789@s.whatsapp.net";
          }
        } else {
          const contextInfo = rawMsg?.message?.extendedTextMessage?.contextInfo;
          const quotedParticipant = contextInfo?.participant;
          const mentionedJids = contextInfo?.mentionedJid || [];
          
          if (quotedParticipant) {
            targetJid = quotedParticipant;
            const phone = quotedParticipant.split("@")[0];
            targetMentionOrNum = `@${phone}`;
          } else if (mentionedJids.length > 0) {
            targetJid = mentionedJids[0];
            const phone = targetJid.split("@")[0];
            targetMentionOrNum = `@${phone}`;
          } else {
            const kickArg = commandText.substring(4).trim();
            let digits = kickArg.replace(/[^0-9]/g, "");
            if (digits.startsWith("0")) {
              digits = "62" + digits.substring(1);
            }
            if (digits) {
              targetJid = `${digits}@s.whatsapp.net`;
              targetMentionOrNum = `@${digits}`;
            }
          }
        }

        if (!targetJid) {
          responseText = format(settings.kickEmptyTemplate || `⚠️ *Format Kick Salah*\n\nSilakan tag/menyebut member, reply chat member, atau masukkan nomor member yang ingin dikeluarkan.\n\nContoh:\n*!kick @628123456789*\n*!kick 08123456789*`, {
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "kick_empty";
          statusText = "Sent Kick Usage Help";
        } else {
          // Identify bot and owner JIDs/numbers
          const rawOwnerPhone = settings.ownerNumber || "6285712439395";
          const cleanOwnerPhone = rawOwnerPhone.replace(/[^0-9]/g, "");
          const ownerId = `${cleanOwnerPhone}@s.whatsapp.net`;

          const isTargetBot = isSelfBot(targetJid, this.sock);

          const isTargetOwner = (targetJid === ownerId) || (isSimulation && (targetMentionOrNum.toLowerCase().includes("owner") || targetJid?.includes("owner")));

          if (isTargetBot) {
            responseText = format(settings.kickBotSelfTemplate || "Tidak Dapat Menggunakan Fitur Tersebut Ke Nomer Bot!", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "kick_bot_self";
            statusText = "Kick Bot Attempt Blocked";
          } else if (isTargetOwner) {
            if (isSenderOwner) {
              responseText = format(settings.kickOwnerSelfTemplate || "Tidak dapat mengeluarkan Owner (Mencoba mengeluarkan diri sendiri).", {
                storeName: settings.storeName || "WANZZ STORE"
              });
              matchedCommand = "kick_owner_self";
              statusText = "Owner Self Kick Prevented";
            } else {
              // Admin tries to kick Owner -> Demote admin!
              if (isSimulation) {
                responseText = format(settings.kickOwnerDemoteTemplate || "⚠️ Gagal mengeluarkan Owner! Jabatan Anda sebagai Admin telah dicabut (diturunkan menjadi anggota biasa) karena mencoba mengeluarkan Owner.", {
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "kick_owner_demote";
                statusText = "Simulated Demotion of Sender";
              } else {
                try {
                  const senderJid = rawMsg?.key?.participant || "";
                  if (senderJid) {
                    await this.sock.groupParticipantsUpdate(from, [senderJid], "demote");
                  }
                  responseText = format(settings.kickOwnerDemoteTemplate || "⚠️ Gagal mengeluarkan Owner! Jabatan Anda sebagai Admin telah dicabut (diturunkan menjadi anggota biasa) karena mencoba mengeluarkan Owner.", {
                    storeName: settings.storeName || "WANZZ STORE"
                  });
                  matchedCommand = "kick_owner_demote";
                  statusText = "Admin Demoted for Kicking Owner";
                } catch (err) {
                  console.error("Failed to demote admin trying to kick owner:", err);
                  responseText = format(settings.kickOwnerDemoteTemplate || "⚠️ Gagal mengeluarkan Owner! Jabatan Anda sebagai Admin telah dicabut (diturunkan menjadi anggota biasa) karena mencoba mengeluarkan Owner.", {
                    storeName: settings.storeName || "WANZZ STORE"
                  });
                  matchedCommand = "kick_owner_demote_error";
                  statusText = "Admin Demotion Error";
                }
              }
            }
          } else if (isSimulation) {
            const lowerArg = commandText.toLowerCase();
            if (lowerArg.includes("noadmin") || lowerArg.includes("bukanadmin")) {
              responseText = format(settings.kickBotNotAdminTemplate || `[Bot bukan admin] {targetNumber} Tidak Dapat Di Keluarkan Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              statusText = "Simulated Kick Non-Admin";
              matchedCommand = "kick_bot_not_admin";
            } else if (lowerArg.includes("gagal") || lowerArg.includes("admin") || lowerArg.includes("owner")) {
              responseText = format(settings.kickTargetIsAdminTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Keluarkan!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              statusText = "Simulated Kick Fail";
              matchedCommand = "kick_target_is_admin";
            } else {
              responseText = format(settings.kickSuccessTemplate || `[Berhasil] {targetNumber} Berhasil Di Keluarkan!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              statusText = "Simulated Kick Success";
              matchedCommand = "kick_success";
            }
          } else {
            try {
              const metadata = await this.sock.groupMetadata(from);
              
              // Debug logging to help identify bot and participants
              console.log("[KickCommand] Checking admin. Bot raw ID:", this.sock.user?.id);
              console.log("[KickCommand] Group participants:", metadata.participants.map((p: any) => ({ id: p.id, admin: p.admin })));

              const botParticipant = metadata.participants.find((p: any) => isSelfBot(p.id, this.sock));
              console.log("[KickCommand] Found botParticipant object:", botParticipant);
              
              const targetParticipant = metadata.participants.find((p: any) => p.id === targetJid);
              
              const isBotAdmin = botParticipant?.admin !== undefined && botParticipant?.admin !== null;
              const isTargetAdmin = targetParticipant?.admin !== undefined && targetParticipant?.admin !== null;

              if (!isBotAdmin) {
                responseText = format(settings.kickBotNotAdminTemplate || `[Bot bukan admin] {targetNumber} Tidak Dapat Di Keluarkan Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!`, {
                  targetNumber: targetMentionOrNum,
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "kick_bot_not_admin";
                statusText = "Kick Failed: Bot Not Admin";
              } else if (isTargetAdmin) {
                responseText = format(settings.kickTargetIsAdminTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Keluarkan!`, {
                  targetNumber: targetMentionOrNum,
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "kick_target_is_admin";
                statusText = "Kick Failed: Target is Admin";
              } else {
                try {
                  await this.sock.groupParticipantsUpdate(from, [targetJid], "remove");
                  responseText = format(settings.kickSuccessTemplate || `[Berhasil] {targetNumber} Berhasil Di Keluarkan!`, {
                    targetNumber: targetMentionOrNum,
                    storeName: settings.storeName || "WANZZ STORE"
                  });
                  matchedCommand = "kick_success";
                  statusText = `Kick Successful: ${targetJid}`;
                } catch (err) {
                  console.error("Baileys groupParticipantsUpdate remove failed:", err);
                  responseText = format(settings.kickFailedTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Keluarkan!`, {
                    targetNumber: targetMentionOrNum,
                    storeName: settings.storeName || "WANZZ STORE"
                  });
                  matchedCommand = "kick_failed";
                  statusText = `Kick Failed: ${targetJid}`;
                }
              }
            } catch (err) {
              console.error("Failed to fetch group metadata or perform kick:", err);
              responseText = format(settings.kickFailedTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Keluarkan!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              matchedCommand = "kick_error";
              statusText = `Kick Error: ${err}`;
            }
          }
        }
      }
    } else if (isAddTrigger) {
      const isGroup = from.endsWith("@g.us");
      if (!isGroup) {
        responseText = format(settings.addOutsideGroupTemplate || "❌ Command ini hanya dapat digunakan di dalam grup!", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "add_outside_group";
        statusText = "Add Private Chat Ignored";
      } else {
        let targetJid: string | null = null;
        let targetMentionOrNum = "";

        const addArg = commandText.substring(3).trim();
        let digits = addArg.replace(/[^0-9]/g, "");
        if (digits.startsWith("0")) {
          digits = "62" + digits.substring(1);
        }
        if (digits) {
          targetJid = `${digits}@s.whatsapp.net`;
          targetMentionOrNum = `@${digits}`;
        }

        if (!targetJid) {
          responseText = format(settings.addEmptyTemplate || `⚠️ *Format Add Salah*\n\nSilakan masukkan nomor member yang ingin ditambahkan.\n\nContoh:\n*!add 08123456789* atau */add 628123456789*`, {
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "add_empty";
          statusText = "Sent Add Usage Help";
        } else {
          if (isSimulation) {
            const lowerArg = commandText.toLowerCase();
            if (lowerArg.includes("noadmin") || lowerArg.includes("bukanadmin")) {
              responseText = format(settings.addBotNotAdminTemplate || `[Bot bukan admin] {targetNumber} Tidak Dapat Di Tambahkan Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              statusText = "Simulated Add Non-Admin";
              matchedCommand = "add_bot_not_admin";
            } else if (lowerArg.includes("gagal") || lowerArg.includes("admin") || lowerArg.includes("owner")) {
              responseText = format(settings.addFailedTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Tambahkan!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              statusText = "Simulated Add Fail";
              matchedCommand = "add_fail";
            } else {
              responseText = format(settings.addSuccessTemplate || `[Berhasil] {targetNumber} Berhasil Di Tambahkan!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              statusText = "Simulated Add Success";
              matchedCommand = "add_success";
            }
          } else {
            try {
              const metadata = await this.sock.groupMetadata(from);
              
              // Debug logging to help identify bot and participants
              console.log("[AddCommand] Checking admin. Bot raw ID:", this.sock.user?.id);
              console.log("[AddCommand] Group participants:", metadata.participants.map((p: any) => ({ id: p.id, admin: p.admin })));

              const botParticipant = metadata.participants.find((p: any) => isSelfBot(p.id, this.sock));
              console.log("[AddCommand] Found botParticipant object:", botParticipant);
              
              const isBotAdmin = botParticipant?.admin !== undefined && botParticipant?.admin !== null;

              if (!isBotAdmin) {
                responseText = format(settings.addBotNotAdminTemplate || `[Bot bukan admin] {targetNumber} Tidak Dapat Di Tambahkan Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!`, {
                  targetNumber: targetMentionOrNum,
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "add_bot_not_admin";
                statusText = "Add Failed: Bot Not Admin";
              } else {
                try {
                  await this.sock.groupParticipantsUpdate(from, [targetJid], "add");
                  responseText = format(settings.addSuccessTemplate || `[Berhasil] {targetNumber} Berhasil Di Tambahkan!`, {
                    targetNumber: targetMentionOrNum,
                    storeName: settings.storeName || "WANZZ STORE"
                  });
                  matchedCommand = "add_success";
                  statusText = `Add Successful: ${targetJid}`;
                } catch (err) {
                  console.error("Baileys groupParticipantsUpdate add failed:", err);
                  responseText = format(settings.addFailedTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Tambahkan!`, {
                    targetNumber: targetMentionOrNum,
                    storeName: settings.storeName || "WANZZ STORE"
                  });
                  matchedCommand = "add_failed";
                  statusText = `Add Failed: ${targetJid}`;
                }
              }
            } catch (err) {
              console.error("Failed to fetch group metadata or perform add:", err);
              responseText = format(settings.addFailedTemplate || `[Gagal] {targetNumber} Tidak Dapat Di Tambahkan!`, {
                targetNumber: targetMentionOrNum,
                storeName: settings.storeName || "WANZZ STORE"
              });
              matchedCommand = "add_error";
              statusText = `Add Error: ${err}`;
            }
          }
        }
      }
    } else if (isBcAddTrigger) {
      const isGroup = from.endsWith("@g.us");
      const subArg = commandText.substring(5).trim();
      let targetPhone = "";
      let targetName = "";
      let targetCategory = "customer"; // customer, supplier, reseller, group

      if (!subArg) {
        if (isGroup) {
          targetPhone = from;
          targetName = senderName || "Grup Whitelist";
          targetCategory = "group";
        } else {
          responseText = format(settings.bcaddEmptyTemplate || `⚠️ *Format Tambah Target Salah*\n\nSilakan masukkan nomor WhatsApp atau ID grup dan nama target.\n\nContoh:\n*!bcadd 628123456789 Budi reseller*\n*!bcadd 120363024888877123@g.us Info_Grup group*`, {
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "bcadd_empty";
          statusText = "Sent BcAdd Help";
        }
      } else {
        const spaceIdx = subArg.indexOf(" ");
        let rawTarget = "";
        let restStr = "";
        if (spaceIdx === -1) {
          rawTarget = subArg;
        } else {
          rawTarget = subArg.substring(0, spaceIdx).trim();
          restStr = subArg.substring(spaceIdx + 1).trim();
        }

        if (rawTarget.endsWith("@g.us") || rawTarget.includes("-") || rawTarget.length > 15) {
          targetPhone = rawTarget;
          targetCategory = "group";
          targetName = restStr || "Grup Chat";
        } else if (isGroup && isNaN(Number(rawTarget.replace(/[^0-9]/g, ""))) && !rawTarget.startsWith("0") && !rawTarget.startsWith("8") && !rawTarget.startsWith("62")) {
          targetPhone = from;
          targetCategory = "group";
          targetName = subArg;
        } else {
          let digits = rawTarget.replace(/[^0-9]/g, "");
          if (digits.startsWith("0")) {
            digits = "62" + digits.substring(1);
          } else if (digits.startsWith("8")) {
            digits = "62" + digits;
          }
          targetPhone = digits;
          targetName = restStr || ("Target Manual " + digits.slice(-4));
        }

        if (targetName) {
          const nameWords = targetName.split(/\s+/);
          if (nameWords.length > 1) {
            const lastWord = nameWords[nameWords.length - 1].toLowerCase();
            if (["customer", "supplier", "reseller", "group"].includes(lastWord)) {
              targetCategory = lastWord;
              targetName = nameWords.slice(0, nameWords.length - 1).join(" ");
            }
          }
        }
      }

      if (targetPhone) {
        const wGroups = settings.whitelistedGroups || [];
        const isGroupTgt = targetPhone.endsWith("@g.us") || targetCategory === "group";
        if (isGroupTgt && !wGroups.includes(targetPhone)) {
          responseText = format(settings.bcaddNotWhitelistedTemplate || `❌ *Gagal Tambah Target*\n\nGrup dengan JID *{targetPhone}* tidak terdaftar di Whitelist. Pastikan grup tersebut dimasukkan ke daftar Whitelist terlebih dahulu di Dashboard.`, {
            targetPhone: targetPhone,
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "bcadd_not_whitelisted";
          statusText = "BcAdd Group JID Not Whitelisted";
        } else {
          const currentTargets = settings.manualBroadcastTargets || [];
          const existingIdx = currentTargets.findIndex((t: any) => t.phone.replace(/[^0-9]/g, "") === targetPhone.replace(/[^0-9]/g, ""));
          
          const newTarget = {
            name: targetName,
            phone: targetPhone,
            category: targetCategory
          };

          if (existingIdx !== -1) {
            currentTargets[existingIdx] = newTarget;
            responseText = format(settings.bcaddSuccessTemplate || `✅ *Target di-Update*\n\nBerhasil memperbarui data target broadcast:\n👤 *Nama:* {targetName}\n📱 *No/JID:* {targetPhone}\n🏷️ *Kategori:* {targetCategory}`, {
              targetName: targetName,
              targetPhone: targetPhone,
              targetCategory: targetCategory.toUpperCase(),
              storeName: settings.storeName || "WANZZ STORE"
            });
          } else {
            currentTargets.push(newTarget);
            responseText = format(settings.bcaddSuccessTemplate || `✅ *Target Ditambahkan*\n\nBerhasil menambahkan target broadcast baru:\n������ *Nama:* {targetName}\n📱 *No/JID:* {targetPhone}\n🏷️ *Kategori:* {targetCategory}`, {
              targetName: targetName,
              targetPhone: targetPhone,
              targetCategory: targetCategory.toUpperCase(),
              storeName: settings.storeName || "WANZZ STORE"
            });
          }

          db.settings.manualBroadcastTargets = currentTargets;
          this.saveDb(db);
          matchedCommand = "bcadd_success";
          statusText = `BcAdd Success: ${targetPhone} (${targetCategory})`;
        }
      }
    } else if (isCloseTrigger) {
      const isGroup = from.endsWith("@g.us");
      if (!isGroup) {
        responseText = format(settings.closeOutsideGroupTemplate || "Command hanya berlaku di grup!", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "group_close_private";
        statusText = "Close Private Chat Ignored";
      } else {
        if (isSimulation) {
          const lowerArg = commandText.toLowerCase();
          if (lowerArg.includes("noadmin") || lowerArg.includes("bukanadmin")) {
            responseText = format(settings.closeBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            statusText = "Simulated Close Non-Admin";
            matchedCommand = "close_bot_not_admin";
          } else if (lowerArg.includes("gagal")) {
            responseText = format(settings.closeBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            statusText = "Simulated Close Fail";
            matchedCommand = "close_fail";
          } else {
            responseText = format(settings.closeSuccessTemplate || "Grup di tutup, grup akan di buka kembali segera.", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            statusText = "Simulated Close Success";
            matchedCommand = "close_success";
          }
        } else {
          try {
            const metadata = await this.sock.groupMetadata(from);
            
            // Debug logging to help identify bot and participants
            console.log("[CloseCommand] Checking admin. Bot raw ID:", this.sock.user?.id);
            console.log("[CloseCommand] Group participants:", metadata.participants.map((p: any) => ({ id: p.id, admin: p.admin })));

            const botParticipant = metadata.participants.find((p: any) => isSelfBot(p.id, this.sock));
            console.log("[CloseCommand] Found botParticipant object:", botParticipant);
            
            const isBotAdmin = botParticipant?.admin !== undefined && botParticipant?.admin !== null;

            if (!isBotAdmin) {
              responseText = format(settings.closeBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
                storeName: settings.storeName || "WANZZ STORE"
              });
              matchedCommand = "close_bot_not_admin";
              statusText = "Close Failed: Bot Not Admin";
            } else {
              try {
                await this.sock.groupSettingUpdate(from, "announcement");
                responseText = format(settings.closeSuccessTemplate || "Grup di tutup, grup akan di buka kembali segera.", {
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "close_success";
                statusText = "Close Group Success";
              } catch (err) {
                console.error("Baileys groupSettingUpdate close failed:", err);
                responseText = format(settings.closeBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "close_failed";
                statusText = "Close Group Failed";
              }
            }
          } catch (err) {
            console.error("Failed to fetch group metadata or perform close:", err);
            responseText = format(settings.closeBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "close_error";
            statusText = "Close Group Error";
          }
        }
      }
    } else if (isOpenTrigger) {
      const isGroup = from.endsWith("@g.us");
      if (!isGroup) {
        responseText = format(settings.openOutsideGroupTemplate || "Command hanya berlaku di grup!", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "group_open_private";
        statusText = "Open Private Chat Ignored";
      } else {
        const idTime = new Date().toLocaleString("en-US", { timeZone: "Asia/Jakarta" });
        const hour = new Date(idTime).getHours();
        let timeGreeting = "malam";
        if (hour >= 4 && hour < 10) {
          timeGreeting = "pagi";
        } else if (hour >= 10 && hour < 15) {
          timeGreeting = "siang";
        } else if (hour >= 15 && hour < 18) {
          timeGreeting = "sore";
        }

        const openResponseMsg = format(settings.openSuccessTemplate || "Selamat {timeGreeting}, grup telah di buka kembali, silahkan bertransaksi dengan bijak", {
          timeGreeting: timeGreeting,
          storeName: settings.storeName || "WANZZ STORE"
        });

        if (isSimulation) {
          const lowerArg = commandText.toLowerCase();
          if (lowerArg.includes("noadmin") || lowerArg.includes("bukanadmin")) {
            responseText = format(settings.openBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            statusText = "Simulated Open Non-Admin";
            matchedCommand = "open_bot_not_admin";
          } else {
            responseText = openResponseMsg;
            statusText = "Simulated Open Success";
            matchedCommand = "open_success";
          }
        } else {
          try {
            const metadata = await this.sock.groupMetadata(from);
            
            // Debug logging to help identify bot and participants
            console.log("[OpenCommand] Checking admin. Bot raw ID:", this.sock.user?.id);
            console.log("[OpenCommand] Group participants:", metadata.participants.map((p: any) => ({ id: p.id, admin: p.admin })));

            const botParticipant = metadata.participants.find((p: any) => isSelfBot(p.id, this.sock));
            console.log("[OpenCommand] Found botParticipant object:", botParticipant);
            
            const isBotAdmin = botParticipant?.admin !== undefined && botParticipant?.admin !== null;

            if (!isBotAdmin) {
              responseText = format(settings.openBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
                storeName: settings.storeName || "WANZZ STORE"
              });
              matchedCommand = "open_bot_not_admin";
              statusText = "Open Failed: Bot Not Admin";
            } else {
              try {
                await this.sock.groupSettingUpdate(from, "not_announcement");
                responseText = openResponseMsg;
                matchedCommand = "open_success";
                statusText = "Open Group Success";
              } catch (err) {
                console.error("Baileys groupSettingUpdate open failed:", err);
                responseText = format(settings.openBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
                  storeName: settings.storeName || "WANZZ STORE"
                });
                matchedCommand = "open_failed";
                statusText = "Open Group Failed";
              }
            }
          } catch (err) {
            console.error("Failed to fetch group metadata or perform open:", err);
            responseText = format(settings.openBotNotAdminTemplate || "Tidak Dapat Menutup/Membuka Grup Dikarenakan Bot Bukan Admin Grup. Jadikan Bot Sebagai Admin Grup!", {
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "open_error";
            statusText = "Open Group Error";
          }
        }
      }
    } else if (isOnlineTrigger) {
      const isGroup = from.endsWith("@g.us");
      if (!isGroup) {
        responseText = format(settings.onlineOutsideGroupTemplate || "Command hanya berlaku di grup!", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "group_online_private";
        statusText = "Online Private Chat Ignored";
      } else {
        matchedCommand = "online";
        statusText = "Sent Group Online Status";
        if (isSimulation) {
          responseText = `🟢 *ANGGOTA ONLINE (SIMULATION)*\n\nTerdapat *3* anggota yang sedang online:\n1. @628123456789 (Ahmad)\n2. @628987654321 (Budi)\n3. @628555555555 (Siti)\n\n_Silakan bertransaksi dengan aman!_`;
        } else {
          try {
            const onlineSet = this.presencesMap[from];
            if (!onlineSet || onlineSet.size === 0) {
              const cleanSenderName = senderName || "Pelanggan";
              const cleanSenderPhone = from.split("@")[0];
              responseText = format(settings.onlineEmptyTemplate || `🟢 *ANGGOTA ONLINE*\n\nTerdapat *1* anggota yang terdeteksi aktif/online saat ini:\n1. @{senderPhone} ({senderName})\n\n_Catatan: Anggota lain akan terdeteksi online saat mereka mengirim pesan atau aktif mengetik._`, {
                senderPhone: cleanSenderPhone,
                senderName: cleanSenderName,
                storeName: settings.storeName || "WANZZ STORE"
              });
            } else {
              const membersList = Array.from(onlineSet);
              let listStr = "";
              membersList.forEach((mJid, idx) => {
                const phone = mJid.split("@")[0];
                listStr += `${idx + 1}. @${phone}\n`;
              });
              responseText = format(settings.onlineSuccessTemplate || `🟢 *ANGGOTA ONLINE*\n\nTerdapat *{onlineCount}* anggota yang aktif/online baru-baru ini:\n\n{listStr}\n_Silakan bertransaksi dengan aman!_`, {
                onlineCount: String(membersList.length),
                listStr: listStr,
                storeName: settings.storeName || "WANZZ STORE"
              });
            }
          } catch (err) {
            console.error("Failed to compile online state:", err);
            responseText = format(settings.onlineFailedTemplate || "⚠️ Gagal mengambil daftar anggota online.", {
              storeName: settings.storeName || "WANZZ STORE"
            });
          }
        }
      }
    } else if (isProsesTrigger) {
      let quotedParticipant: string | null = null;
      if (isSimulation) {
        const lastIncoming = this.logs.find(
          (l) => l.type === "incoming" && 
                 !l.senderName.toLowerCase().includes("owner") && 
                 !l.senderName.toLowerCase().includes("admin")
        );
        if (lastIncoming) {
          quotedParticipant = lastIncoming.from.includes("@") ? lastIncoming.from : `${lastIncoming.from}@s.whatsapp.net`;
        } else {
          quotedParticipant = "628123456789@s.whatsapp.net";
        }
      } else {
        const contextInfo = rawMsg?.message?.extendedTextMessage?.contextInfo;
        quotedParticipant = contextInfo?.participant || null;
        if (!quotedParticipant && contextInfo && !from.endsWith("@g.us")) {
          quotedParticipant = from;
        }
      }

      if (quotedParticipant) {
        quotedParticipant = resolveLidToPn(quotedParticipant);
      }

      if (!quotedParticipant) {
        responseText = format(settings.prosesNoReplyTemplate || "❌ Reply pesan customer terlebih dahulu.", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "proses_error_no_reply";
        statusText = "Proses Ignored: No Reply";
      } else {
        const customerNumber = quotedParticipant.split("@")[0].split(":")[0].replace(/[^0-9]/g, "");
        let senderPhone = "";
        if (isSimulation) {
          senderPhone = "6285712439395";
        } else {
          let senderJid = "";
          if (from.endsWith("@g.us")) {
            senderJid = rawMsg?.key?.participant || rawMsg?.participant || "";
          } else {
            senderJid = from;
          }
          if (senderJid) {
            senderJid = resolveLidToPn(senderJid);
          }
          senderPhone = senderJid.split("@")[0] || "";
        }
        const adminNumber = senderPhone.split(":")[0].replace(/[^0-9]/g, "");

        if (!db.activeTransactions) {
          db.activeTransactions = [];
        }

        const existingTx = db.activeTransactions.find((t: any) => t.customerNumber === customerNumber && t.status === "proses");
        if (existingTx) {
          responseText = format(settings.prosesExistingTemplate || "❌ Customer ini masih memiliki transaksi yang sedang diproses.", {
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "proses_error_existing";
          statusText = "Proses Ignored: Existing Active Transaction";
        } else {
          db.activeTransactions.push({
            customerNumber,
            adminNumber,
            createdAt: new Date().toISOString(),
            status: "proses"
          });
          this.saveDb(db);

          const nowJakarta = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Jakarta" }));
          const tanggalStr = nowJakarta.toLocaleDateString("id-ID", { day: "numeric", month: "long", year: "numeric" });
          const waktuStr = nowJakarta.toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit", second: "2-digit" }) + " WIB";

          const defaultProses = `🚀 *TRANSAKSI SEDANG PROSES* 🚀\n\n*Detail Order*\n\n👤 *Pemesan*: @{customerNumber}\n\n🗓️ *Tanggal*: {tanggal}\n\n⏰ *Waktu*: {waktu}\n\n👤 *Admin*: @{adminNumber}\n\nPesanan Anda sedang dalam proses. Mohon menunggu notifikasi selanjutnya.`;
          responseText = format(settings.prosesTemplate || defaultProses, {
            customerNumber,
            adminNumber,
            tanggal: tanggalStr,
            waktu: waktuStr,
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "proses";
          statusText = "Transaction Set to Proses";
        }
      }
    } else if (isSelesaiTrigger) {
      let quotedParticipant: string | null = null;
      if (isSimulation) {
        const lastIncoming = this.logs.find(
          (l) => l.type === "incoming" && 
                 !l.senderName.toLowerCase().includes("owner") && 
                 !l.senderName.toLowerCase().includes("admin")
        );
        if (lastIncoming) {
          quotedParticipant = lastIncoming.from.includes("@") ? lastIncoming.from : `${lastIncoming.from}@s.whatsapp.net`;
        } else {
          quotedParticipant = "628123456789@s.whatsapp.net";
        }
      } else {
        const contextInfo = rawMsg?.message?.extendedTextMessage?.contextInfo;
        quotedParticipant = contextInfo?.participant || null;
        if (!quotedParticipant && contextInfo && !from.endsWith("@g.us")) {
          quotedParticipant = from;
        }
      }

      if (quotedParticipant) {
        quotedParticipant = resolveLidToPn(quotedParticipant);
      }

      if (!quotedParticipant) {
        responseText = format(settings.selesaiNoReplyTemplate || "❌ Reply pesan customer terlebih dahulu.", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "selesai_error_no_reply";
        statusText = "Selesai Ignored: No Reply";
      } else {
        const customerNumber = quotedParticipant.split("@")[0].split(":")[0].replace(/[^0-9]/g, "");
        if (!db.activeTransactions) {
          db.activeTransactions = [];
        }

        const existingTxIndex = db.activeTransactions.findIndex((t: any) => t.customerNumber === customerNumber && t.status === "proses");
        if (existingTxIndex === -1) {
          responseText = format(settings.selesaiNoTxTemplate || "❌ Tidak ditemukan transaksi aktif untuk customer ini.", {
            customerName: customerNumber,
            customerNumber,
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "selesai_error_no_tx";
          statusText = "Selesai Ignored: No Active Tx";
        } else {
          const tx = db.activeTransactions[existingTxIndex];
          let senderPhone = "";
          if (isSimulation) {
            senderPhone = "6285712439395";
          } else {
            let senderJid = "";
            if (from.endsWith("@g.us")) {
              senderJid = rawMsg?.key?.participant || rawMsg?.participant || "";
            } else {
              senderJid = from;
            }
            if (senderJid) {
              senderJid = resolveLidToPn(senderJid);
            }
            senderPhone = senderJid.split("@")[0] || "";
          }
          const adminNumber = senderPhone.split(":")[0].replace(/[^0-9]/g, "");

          if (tx.adminNumber !== adminNumber) {
            responseText = format(settings.selesaiForbiddenTemplate || "❌ Hanya admin yang memulai proses transaksi ini yang dapat mengubah status transaksi.", {
              customerNumber,
              adminNumber,
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "selesai_error_forbidden";
            statusText = "Selesai Ignored: Admin Mismatch";
          } else {
            db.activeTransactions.splice(existingTxIndex, 1);
            this.saveDb(db);

            const nowJakarta = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Jakarta" }));
            const tanggalStr = nowJakarta.toLocaleDateString("id-ID", { day: "numeric", month: "long", year: "numeric" });
            const waktuStr = nowJakarta.toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit", second: "2-digit" }) + " WIB";

            const defaultSelesai = `✅ *TRANSAKSI BERHASIL* ✅\n\n*Detail Order*\n\n👤 *Pemesan*: @{customerNumber}\n\n🗓️ *Tanggal*: {tanggal}\n\n⏰ *Waktu*: {waktu}\n\n👤 *Admin*: @{adminNumber}\n\nPesanan Anda telah berhasil diproses.\n\nTerima kasih telah berbelanja di {storeName}.`;
            responseText = format(settings.selesaiTemplate || defaultSelesai, {
              customerNumber,
              adminNumber,
              tanggal: tanggalStr,
              waktu: waktuStr,
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "selesai";
            statusText = "Transaction Finished Successfully";
          }
        }
      }
    } else if (isGagalTrigger) {
      let quotedParticipant: string | null = null;
      if (isSimulation) {
        const lastIncoming = this.logs.find(
          (l) => l.type === "incoming" && 
                 !l.senderName.toLowerCase().includes("owner") && 
                 !l.senderName.toLowerCase().includes("admin")
        );
        if (lastIncoming) {
          quotedParticipant = lastIncoming.from.includes("@") ? lastIncoming.from : `${lastIncoming.from}@s.whatsapp.net`;
        } else {
          quotedParticipant = "628123456789@s.whatsapp.net";
        }
      } else {
        const contextInfo = rawMsg?.message?.extendedTextMessage?.contextInfo;
        quotedParticipant = contextInfo?.participant || null;
        if (!quotedParticipant && contextInfo && !from.endsWith("@g.us")) {
          quotedParticipant = from;
        }
      }

      if (quotedParticipant) {
        quotedParticipant = resolveLidToPn(quotedParticipant);
      }

      if (!quotedParticipant) {
        responseText = format(settings.gagalNoReplyTemplate || "❌ Reply pesan customer terlebih dahulu.", {
          storeName: settings.storeName || "WANZZ STORE"
        });
        matchedCommand = "gagal_error_no_reply";
        statusText = "Gagal Ignored: No Reply";
      } else {
        const customerNumber = quotedParticipant.split("@")[0].split(":")[0].replace(/[^0-9]/g, "");
        if (!db.activeTransactions) {
          db.activeTransactions = [];
        }

        const existingTxIndex = db.activeTransactions.findIndex((t: any) => t.customerNumber === customerNumber && t.status === "proses");
        if (existingTxIndex === -1) {
          responseText = format(settings.gagalNoTxTemplate || "❌ Tidak ditemukan transaksi aktif untuk customer ini.", {
            customerName: customerNumber,
            customerNumber,
            storeName: settings.storeName || "WANZZ STORE"
          });
          matchedCommand = "gagal_error_no_tx";
          statusText = "Gagal Ignored: No Active Tx";
        } else {
          const tx = db.activeTransactions[existingTxIndex];
          let senderPhone = "";
          if (isSimulation) {
            senderPhone = "6285712439395";
          } else {
            let senderJid = "";
            if (from.endsWith("@g.us")) {
              senderJid = rawMsg?.key?.participant || rawMsg?.participant || "";
            } else {
              senderJid = from;
            }
            if (senderJid) {
              senderJid = resolveLidToPn(senderJid);
            }
            senderPhone = senderJid.split("@")[0] || "";
          }
          const adminNumber = senderPhone.split(":")[0].replace(/[^0-9]/g, "");

          if (tx.adminNumber !== adminNumber) {
            responseText = format(settings.gagalForbiddenTemplate || "❌ Hanya admin yang memulai proses transaksi ini yang dapat mengubah status transaksi.", {
              customerNumber,
              adminNumber,
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "gagal_error_forbidden";
            statusText = "Gagal Ignored: Admin Mismatch";
          } else {
            db.activeTransactions.splice(existingTxIndex, 1);
            this.saveDb(db);

            const nowJakarta = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Jakarta" }));
            const tanggalStr = nowJakarta.toLocaleDateString("id-ID", { day: "numeric", month: "long", year: "numeric" });
            const waktuStr = nowJakarta.toLocaleTimeString("id-ID", { hour: "2-digit", minute: "2-digit", second: "2-digit" }) + " WIB";

            const defaultGagal = `❌ *TRANSAKSI DIBATALKAN* ❌\n\n*Detail Order*\n\n👤 *Pemesan*: @{customerNumber}\n\n🗓️ *Tanggal*: {tanggal}\n\n⏰ *Waktu*: {waktu}\n\n👤 *Admin*: @{adminNumber}\n\nMohon hubungi admin untuk informasi lebih lanjut.`;
            responseText = format(settings.gagalTemplate || defaultGagal, {
              customerNumber,
              adminNumber,
              tanggal: tanggalStr,
              waktu: waktuStr,
              storeName: settings.storeName || "WANZZ STORE"
            });
            matchedCommand = "gagal";
            statusText = "Transaction Set to Gagal";
          }
        }
      }
    } else if (isOrderTrigger) {
      let orderProductKey = "";
      if (lowerCommandText.startsWith("order")) {
        orderProductKey = commandText.substring(5).trim().toLowerCase();
      } else if (lowerCommandText.startsWith("beli")) {
        orderProductKey = commandText.substring(4).trim().toLowerCase();
      }

      if (!orderProductKey) {
        matchedCommand = "order_empty";
        statusText = "Sent Order Help";
        responseText = `⚠️ *Format Pemesanan Salah*\n\nSilakan masukkan nama produk yang ingin Anda beli.\n\nContoh:\n*!order NETFLIX* atau */beli YT PREMIUM*`;
      } else {
        let matchedProduct: any = null;
        let matchedVariant: any = null;

        // 1. Try to find if orderProductKey directly matches a variant's ID, Name, or Alternative Commands (case-insensitive) for exact matches
        for (const p of db.products) {
          if (p.variants && p.variants.length > 0) {
            const v = p.variants.find((v: any) => {
              const idL = v.id.toLowerCase();
              const nameL = v.name.toLowerCase();
              const parentL = p.name.toLowerCase();
              const combinedL = `${parentL} ${nameL}`;
              const combinedL2 = `${parentL} - ${nameL}`;
              const matchAlt = v.alternativeCommands && Array.isArray(v.alternativeCommands)
                ? v.alternativeCommands.some((cmd: string) => cmd.trim().toLowerCase() === orderProductKey.trim().toLowerCase())
                : false;
              return idL === orderProductKey || 
                     nameL === orderProductKey || 
                     combinedL === orderProductKey || 
                     combinedL2 === orderProductKey ||
                     matchAlt;
            });
            if (v) {
              matchedProduct = p;
              matchedVariant = v;
              break;
            }
          }
        }

        // 2. Clear & resilient token-based fuzzy matching
        if (!matchedProduct) {
          const normalizeString = (str: string) => {
            return str
              .toLowerCase()
              .replace(/[^a-z0-9]/g, " ")
              .replace(/\b3bu\b/g, " 3 bulan buyer ")
              .replace(/\b1bu\b/g, " 1 bulan buyer ")
              .replace(/\b3b\b|\b3bln\b|\b3bul\b/g, " 3 bulan ")
              .replace(/\b1b\b|\b1bln\b|\b1bul\b/g, " 1 bulan ")
              .replace(/\b2b\b|\b2bln\b|\b2bul\b/g, " 2 bulan ")
              .replace(/\byt\b/g, " youtube ")
              .replace(/\bnf\b/g, " netflix ")
              .replace(/\bsp\b|\bspot\b/g, " spotify ")
              .replace(/\bdis\b/g, " disney ")
              .replace(/\bml\b/g, " mobile legends ")
              .replace(/\bff\b/g, " free fire ")
              .replace(/\bprem\b/g, " premium ")
              .replace(/\bfam\b/g, " famplan ")
              .replace(/\bind\b/g, " indplan ")
              .replace(/\bse\b|\bsel\b/g, " seller ")
              .replace(/\bby\b|\bbuy\b/g, " buyer ");
          };

          const userNormalized = normalizeString(orderProductKey);
          const userTokens = userNormalized.split(/\s+/).filter(Boolean);

          if (userTokens.length > 0) {
            let bestVariant: any = null;
            let bestProduct: any = null;
            let maxMatchPct = 0;

            for (const p of db.products) {
              if (p.variants && p.variants.length > 0) {
                for (const v of p.variants) {
                  const variantNormalized = normalizeString(`${p.id} ${p.name} ${v.id} ${v.name}`);
                  
                  let matchCount = 0;
                  for (const token of userTokens) {
                    if (variantNormalized.includes(token)) {
                      matchCount++;
                    }
                  }
                  
                  const matchPct = matchCount / userTokens.length;
                  // Require at least 2 tokens matched if user typed multiple tokens to prevent single letter matches
                  const minRequiredMatches = userTokens.length >= 2 ? 2 : 1;
                  if (matchPct > maxMatchPct && matchCount >= minRequiredMatches) {
                    maxMatchPct = matchPct;
                    bestVariant = v;
                    bestProduct = p;
                  }
                }
              }
            }

            if (maxMatchPct >= 0.8) {
              matchedProduct = bestProduct;
              matchedVariant = bestVariant;
            }
          }
        }

        // 3. If no variant matched, match top-level product by Name or ID
        if (!matchedProduct) {
          matchedProduct = db.products.find((p: any) => {
            const nameLower = p.name.toLowerCase();
            const idLower = p.id.toLowerCase();
            return nameLower === orderProductKey || idLower === orderProductKey || (nameLower.includes(orderProductKey) && orderProductKey.length > 2);
          });

          // If matched a parent product and it has variants, we require a variant selection
          if (matchedProduct && matchedProduct.variants && matchedProduct.variants.length > 0) {
            matchedCommand = "order_need_variant";
            statusText = `Order Variant Needed: ${matchedProduct.name}`;
            responseText = `harap pilih varian, ketik ${matchedProduct.name} untuk melihat varian!`;
            matchedProduct = null; // Prevent proceeding with generic booking
          }
        }

        if (!matchedProduct) {
          if (matchedCommand !== "order_need_variant") {
            matchedCommand = "order_not_found";
            statusText = `Order Not Found: ${orderProductKey}`;
            responseText = ""; // Silent response per user request
          }
        } else {
          const isUnknown = matchedProduct.stockType === "UNKNOWN";
          let stock = 10;
          if (matchedVariant) {
            stock = matchedVariant.stock !== undefined ? matchedVariant.stock : (matchedProduct.stock !== undefined ? matchedProduct.stock : 10);
          } else {
            stock = matchedProduct.stock !== undefined ? matchedProduct.stock : 10;
          }

          if (!isUnknown && stock <= 0) {
            const displayName = matchedVariant ? `${matchedProduct.name} (${matchedVariant.name})` : matchedProduct.name;
            matchedCommand = "order_out_of_stock";
            statusText = `Order Out of Stock: ${displayName}`;
            responseText = `⚠️ *Stok Habis*\n\nMohon maaf Kak, untuk saat ini stok produk *${displayName}* sedang kosong.\n\nSilakan tanyakan kepada admin (+${settings.ownerNumber || "6285712439395"}) kapan produk ini restock. Terimakasih! 🙏`;
          } else {
            // No automated stock deduction or database saving in db.transactions!
            // Format ORD-DDMMYYYY-HHMM-RANDOM to embed date & time
            const now = new Date();
            const targetTimezoneOffset = 7; // WIB (UTC+7)
            const utcTime = now.getTime() + (now.getTimezoneOffset() * 60000);
            const jakartaTime = new Date(utcTime + (3600000 * targetTimezoneOffset));

            const dayStr = String(jakartaTime.getDate()).padStart(2, '0');
            const monthStr = String(jakartaTime.getMonth() + 1).padStart(2, '0');
            const yearStr = String(jakartaTime.getFullYear());
            const hourStr = String(jakartaTime.getHours()).padStart(2, '0');
            const minuteStr = String(jakartaTime.getMinutes()).padStart(2, '0');
            const randStr = Math.floor(1000 + Math.random() * 9000);

            // Place verification random code at the beginning
            const orderId = `ORD-${randStr}-${dayStr}${monthStr}${yearStr}-${hourStr}${minuteStr}`;

            // Save the transaction to database
            let participantJid = rawMsg?.key?.participant || rawMsg?.participant || "";
            if (!participantJid || participantJid.endsWith("@g.us")) {
              if (from.endsWith("@g.us")) {
                participantJid = settings.ownerNumber ? `${settings.ownerNumber.replace(/[^0-9]/g, "")}@s.whatsapp.net` : "6285712439395@s.whatsapp.net";
              } else {
                participantJid = from;
              }
            }
            const buyerPhoneNum = participantJid.split("@")[0] || "";

            const resolvedPrice = matchedVariant ? (matchedVariant.price || 0) : (matchedProduct.price || 0);
            const resolvedOriginalPrice = matchedVariant ? (matchedVariant.originalPrice || matchedVariant.price || 0) : (matchedProduct.price || 0);
            const resolvedName = matchedVariant ? `${matchedProduct.name} - ${matchedVariant.name}` : matchedProduct.name;
            const resolvedProdId = matchedVariant ? matchedVariant.id : matchedProduct.id;

            const newTx = {
              id: orderId,
              customerName: senderName || "Pelanggan Terdaftar",
              customerPhone: buyerPhoneNum,
              productId: resolvedProdId,
              productName: resolvedName,
              quantity: 1,
              totalPrice: resolvedPrice,
              originalPrice: resolvedOriginalPrice,
              sellingPrice: resolvedPrice,
              paymentMethod: "QRIS",
              buyerPhone: buyerPhoneNum,
              status: "Pending",
              timestamp: now.toISOString()
            };

            db.transactions = [newTx, ...(db.transactions || [])];
            this.saveDb(db);

            matchedCommand = "order_success";
            statusText = `Order Successful: ${orderId}`;
            
            const defaultOrderSuccessTemplate = "╭━━━〔 🛒 ORDER BERHASIL 〕━━━╮\n■ *No. Order:* #{orderId}\n■ *Produk:* {productName}\n■ *Harga:* {price}\n■ *Status:* Menunggu Pembayaran\n■ *Nama:* {name}\n━━━━━━━━━━━━━━━━━━\n\n{paymentTemplate}\n\n━━━━━━━━━━━━━━━━━━\n📌 *Kirim bukti transfer ke WhatsApp Owner (+{ownerNumber}) dan sebutkan No Order Anda untuk verifikasi instan!*\n╰━━━━━━━━━━━━━━━━━━━━━━╯";
            const currentTemplate = settings.orderSuccessTemplate || defaultOrderSuccessTemplate;
            
            responseText = currentTemplate
              .replace(/{orderId}/g, orderId)
              .replace(/{productName}/g, resolvedName)
              .replace(/{price}/g, `Rp${resolvedPrice.toLocaleString("id-ID")}`)
              .replace(/{name}/g, senderName)
              .replace(/{paymentTemplate}/g, settings.paymentTemplate || "Konfigurasi pembayaran belum diset.")
              .replace(/{ownerNumber}/g, settings.ownerNumber || "6285712439395");

            if (settings.orderSuccessImageUrl) {
              hasImage = true;
              mediaUrlToSend = settings.orderSuccessImageUrl;
              mediaTypeToSend = "image";
            }
          }
        }
      }
    } else {
      // Check if user specifically searched inside info command e.g., "/info DISNEY"
      let productSearchKey = "";
      if (isInfoCommand) {
        productSearchKey = commandText.substring(4).trim().toLowerCase();
      } else {
        // Direct matching e.g., check if the text is exactly a product name/ID (case-insensitive)
        productSearchKey = lowerText;
      }

      if (isInfoCommand && !productSearchKey) {
        matchedCommand = "info_empty";
        statusText = "Sent Info Help (Empty)";
        responseText = (settings.infoEmptyTemplate || "⚠️ *Format Informasi Salah*\n\nSilakan masukkan nama produk yang ingin Anda cari informasi detailnya.\n\nContoh:\n*!info NETFLIX* atau */info YT PREMIUM*")
          .replace(/{storeName}/g, settings.storeName || "WANZZ STORE");
      } else {
        // Exact or partial name matching in products
        const matchedProduct = db.products.find((p: any) => {
          const nameLower = p.name.toLowerCase();
          const idLower = p.id.toLowerCase();
          return nameLower === productSearchKey || idLower === productSearchKey || nameLower.includes(productSearchKey) && productSearchKey.length > 2;
        });

        if (matchedProduct) {
          matchedCommand = `info:${matchedProduct.id}`;
          statusText = `Sent Info ${matchedProduct.name}`;
          
          // Find category name
          const catObj = db.categories.find((c: any) => c.id === matchedProduct.category);
          const catName = catObj ? catObj.name : "Other Services";

          // Increment product search statistic
          matchedProduct.searchCount = (matchedProduct.searchCount || 0) + 1;
          this.saveDb(db);

          let detailsText = "";
          if (matchedProduct.variants && matchedProduct.variants.length > 0) {
            detailsText += `PILIHAN / VARIAN LAYANAN:`;
            
            const categoriesList = matchedProduct.variantCategories || [];
            const usedCategories = Array.from(new Set(
              matchedProduct.variants
                .map((v: any) => v.variantCategory)
                .filter((cat: any) => cat)
            )) as string[];

            const allCategories = [...categoriesList];
            usedCategories.forEach(cat => {
              if (!allCategories.includes(cat)) {
                allCategories.push(cat);
              }
            });

            const grouped: Record<string, any[]> = {};
            allCategories.forEach(cat => {
              grouped[cat] = [];
            });
            const uncategorized: any[] = [];

            matchedProduct.variants.forEach((v: any) => {
              if (v.variantCategory) {
                if (!grouped[v.variantCategory]) {
                  grouped[v.variantCategory] = [];
                }
                grouped[v.variantCategory].push(v);
              } else {
                uncategorized.push(v);
              }
            });

            // Print categorized categories
            allCategories.forEach(cat => {
              const list = grouped[cat] || [];
              if (list.length > 0) {
                detailsText += `\n${cat.toUpperCase()}\n`;
                list.forEach((v: any) => {
                  const subName = v.name;
                  const displayName = subName.toLowerCase().includes(matchedProduct.name.toLowerCase())
                    ? subName
                    : `${matchedProduct.name} ${subName}`;
                  detailsText += `📍\n└ ${displayName} : Rp${v.price.toLocaleString("id-ID")}\n`;
                });
              }
            });

            // Print any uncategorized variants
            if (uncategorized.length > 0) {
              detailsText += `\nLAINNYA\n`;
              uncategorized.forEach((v: any) => {
                const subName = v.name;
                const displayName = subName.toLowerCase().includes(matchedProduct.name.toLowerCase())
                  ? subName
                  : `${matchedProduct.name} ${subName}`;
                detailsText += `📍\n└ ${displayName} : Rp${v.price.toLocaleString("id-ID")}\n`;
              });
            }
          }

          if (matchedProduct.details) {
            if (detailsText) {
              detailsText += `\n\n${matchedProduct.details}`;
            } else {
              detailsText = matchedProduct.details;
            }
          }

          const priceStr = matchedProduct.variants && matchedProduct.variants.length > 0
            ? `Mulai Rp${Math.min(...matchedProduct.variants.map((v: any) => v.price || 0)).toLocaleString("id-ID")}`
            : (matchedProduct.price ? `Rp${matchedProduct.price.toLocaleString("id-ID")}` : "Free / Hubungi Admin");

          const stockStr = matchedProduct.stockType === "UNKNOWN"
            ? "Tersedia (Ready)"
            : (matchedProduct.stock !== undefined ? matchedProduct.stock : 10) > 0 
              ? `${matchedProduct.stock !== undefined ? matchedProduct.stock : 10} unit` 
              : "Habis";

          const templateToUse = (settings.infoTemplate || "").replace(/\/menu/g, "/list");
          responseText = format(templateToUse, {
            productName: matchedProduct.name,
            categoryName: catName,
            details: detailsText,
            storeName: settings.storeName,
            price: priceStr,
            stock: stockStr,
          });

          const infoImage = matchedProduct.image || settings.infoImageUrl;
          if (infoImage) {
            hasImage = true;
            mediaUrlToSend = infoImage;
            mediaTypeToSend = "image";
          }
        } else if (isInfoCommand) {
          const originalText = commandText.substring(4).trim();
          matchedCommand = "info_not_found";
          statusText = `Info Not Found: ${originalText}`;
          responseText = (settings.infoNotFoundTemplate || "⚠️ *Produk Tidak Ditemukan*\n\nMohon maaf Kak, produk *{productName}* tidak tersedia di katalog kami.\n\nKetik */list* untuk melihat daftar produk yang tersedia! 🙏")
            .replace(/{productName}/g, originalText)
            .replace(/{storeName}/g, settings.storeName || "WANZZ STORE");
        }
      }
    }

    if (!responseText && matchedCommand === "none") {
      // Per user request, if command is not available or product name is not available, the bot remains silent.
      matchedCommand = "none";
      statusText = "Ignored (No Match)";
      responseText = "";
    }

    // Whitelist and category-based permissions are already pre-evaluated.

    // 3. Dispatch response through WhatsApp (if connected and NOT simulation)
    if (!isSimulation && responseText && this.sock && this.status === "connected") {
      try {
        const options = rawMsg ? { quoted: rawMsg } : undefined;

        const sendWithImageCheck = async (imageUrl: string, captionText: string) => {
          if (imageUrl.startsWith("data:")) {
            const matched = imageUrl.match(/^data:([^;]+);base64,(.+)$/) || imageUrl.match(/^data:image\/([a-zA-Z0-9+-\/]+);base64,(.+)$/);
            if (matched) {
              let mime = matched[1];
              if (!mime.includes("/")) {
                mime = `image/${mime}`;
              }
              const base64Content = matched[2];
              const buffer = Buffer.from(base64Content, "base64");
              await this.sock.sendMessage(from, {
                image: buffer,
                caption: captionText,
                mimetype: mime
              }, options);
              return true;
            }
          } else if (imageUrl) {
            // Check local files first
            const filePath = path.join(process.cwd(), "src/images", imageUrl);
            if (fs.existsSync(filePath)) {
              const buffer = fs.readFileSync(filePath);
              const determinedMime = getFileMimeType(imageUrl, "image/png");
              await this.sock.sendMessage(from, {
                image: buffer,
                caption: captionText,
                mimetype: determinedMime
              }, options);
              return true;
            } else {
              await this.sock.sendMessage(from, {
                image: { url: imageUrl },
                caption: captionText,
              }, options);
              return true;
            }
          }
          return false;
        };

        // Try finding matching command in custom commands list to inspect if it contains photo or video
        const customCommandObj = (db.commands || []).find((c: any) => {
          const triggers = c.trigger.split(",").map((t: string) => t.trim().toLowerCase());
          return triggers.includes(matchedCommand);
        });
        if (customCommandObj && customCommandObj.mediaType && customCommandObj.mediaType !== "none" && customCommandObj.mediaUrl) {
          const mediaUrl = customCommandObj.mediaUrl;
          const mediaType = customCommandObj.mediaType;
          const isBase64 = mediaUrl.startsWith("data:");
          let mediaBuffer: Buffer | null = null;
          let mime = "";

          if (isBase64) {
            const matched = mediaUrl.match(/^data:([a-zA-Z0-9-\/]+);base64,(.+)$/);
            if (matched) {
              mime = matched[1];
              if (!mime.includes("/")) {
                mime = mediaType === "image" ? `image/${mime}` : `video/${mime}`;
              }
              mediaBuffer = Buffer.from(matched[2], "base64");
            }
          } else {
            // Check local files first
            const folder = mediaType === "image" ? "src/images" : "src/video";
            const filePath = path.join(process.cwd(), folder, mediaUrl);
            if (fs.existsSync(filePath)) {
              mediaBuffer = fs.readFileSync(filePath);
              mime = getFileMimeType(mediaUrl, mediaType === "image" ? "image/png" : "video/mp4");
            }
          }

          if (mediaType === "image") {
            if (mediaBuffer) {
              await this.sock.sendMessage(from, {
                image: mediaBuffer,
                caption: responseText,
                mimetype: mime || "image/png"
              }, options);
            } else {
              await this.sock.sendMessage(from, {
                image: { url: mediaUrl },
                caption: responseText,
              }, options);
            }
          } else if (mediaType === "video") {
            if (mediaBuffer) {
              await this.sock.sendMessage(from, {
                video: mediaBuffer,
                caption: responseText,
                mimetype: mime || "video/mp4"
              }, options);
            } else {
              await this.sock.sendMessage(from, {
                video: { url: mediaUrl },
                caption: responseText,
              }, options);
            }
          }
        } else if (matchedCommand === "menu" && settings.sendMenuWithImage && settings.menuImageUrl) {
          // Send image with menu caption
          const sent = await sendWithImageCheck(settings.menuImageUrl, responseText);
          if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
        } else if (matchedCommand === "payment" && settings.paymentQrisUrl) {
          const sent = await sendWithImageCheck(settings.paymentQrisUrl, responseText);
          if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
        } else if (matchedCommand === "greeting" && settings.welcomeImageUrl) {
          const sent = await sendWithImageCheck(settings.welcomeImageUrl, responseText);
          if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
        } else if (matchedCommand === "owner" && settings.ownerImageUrl) {
          const sent = await sendWithImageCheck(settings.ownerImageUrl, responseText);
          if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
        } else if (matchedCommand === "order_success" && settings.orderSuccessImageUrl) {
          const sent = await sendWithImageCheck(settings.orderSuccessImageUrl, responseText);
          if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
        } else if (matchedCommand === "fallback" && settings.fallbackImageUrl) {
          const sent = await sendWithImageCheck(settings.fallbackImageUrl, responseText);
          if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
        } else if (matchedCommand && matchedCommand.startsWith("info:")) {
          const prodId = matchedCommand.split(":")[1];
          const productObj = db.products.find((p: any) => p.id === prodId);
          const infoImage = (productObj && productObj.image) || settings.infoImageUrl;
          if (infoImage) {
            const sent = await sendWithImageCheck(infoImage, responseText);
            if (!sent) await this.sock.sendMessage(from, { text: responseText }, options);
          } else {
            await this.sock.sendMessage(from, { text: responseText }, options);
          }
        } else if (matchedCommand === "hidetag") {
          let mentions: string[] = [];
          try {
            const groupMetadata = await this.sock.groupMetadata(from);
            if (groupMetadata && groupMetadata.participants) {
              mentions = groupMetadata.participants.map((p: any) => p.id);
            }
          } catch (err) {
            console.error("Error fetching group participants for hidetag:", err);
          }
          await this.sock.sendMessage(from, { text: responseText, mentions }, options);

        } else if (matchedCommand.startsWith("kick") || matchedCommand.startsWith("add") || ["proses", "selesai", "gagal"].includes(matchedCommand)) {
          const mentions: string[] = [];
          const matches = responseText.match(/@\d+/g);
          if (matches) {
            for (const match of matches) {
              const phone = match.substring(1);
              mentions.push(`${phone}@s.whatsapp.net`);
            }
          }
          await this.sock.sendMessage(from, { text: responseText, mentions }, options);
        } else {
          // General text message
          await this.sock.sendMessage(from, { text: responseText }, options);
        }

        // Add to log
        this.addLog({
          from,
          senderName: this.pushName,
          message: responseText,
          type: "outgoing",
          status: statusText,
        });

      } catch (err) {
        console.error("Error sending WhatsApp message:", err);
        // Fallback send text if media fails
        try {
          const options = rawMsg ? { quoted: rawMsg } : undefined;
          await this.sock.sendMessage(from, { text: responseText }, options);
          this.addLog({
            from,
            senderName: this.pushName,
            message: responseText,
            type: "outgoing",
            status: `${statusText} (Text Fallback)`,
          });
        } catch (subErr) {
          console.error("Even text fallback failed:", subErr);
        }
      }
    }

    // Return metrics for UI simulation
    return {
      response: responseText,
      command: matchedCommand,
      status: statusText,
      hasImage,
      mediaUrl: mediaUrlToSend,
      mediaType: mediaTypeToSend,
    };
  }
}

const botManager = new WhatsAppBotManager();

// On server start, auto boot connection if authorization file credentials exist
if (fs.existsSync(path.join(AUTH_DIR, "creds.json"))) {
  console.log("Found existing credentials in", AUTH_DIR, "reconnecting automatically...");
  botManager.initialize();
}

// ==========================================
//    ROLE-BASED ACCESS CONTROL SYSTEM (RBAC)
// ==========================================

// Secure Hashing with unique or randomized salt
function hashPassword(password: string, salt = crypto.randomBytes(16).toString("hex")): string {
  const hash = crypto.createHash("sha256").update(password + salt).digest("hex");
  return `${salt}:${hash}`;
}

function verifyPassword(password: string, stored: string): boolean {
  if (!stored || !stored.includes(":")) return false;
  const [salt, hash] = stored.split(":");
  const newHash = crypto.createHash("sha256").update(password + salt).digest("hex");
  return newHash === hash;
}

interface SessionInfo {
  userId: string;
  username: string;
  role: "OWNER" | "ADMIN";
  permissions: string[];
  expiresAt: number;
}
const activeSessions = new Map<string, SessionInfo>();

// Activity Logging Helper
function logActivity(db: any, username: string, role: string, action: string, details: string) {
  if (!db.settings) db.settings = {};
  if (!db.settings.activityLogs) db.settings.activityLogs = [];
  
  const newLog = {
    id: "LOG-" + Math.random().toString(36).substring(2, 9).toUpperCase(),
    username,
    role,
    action, // "Login" | "Logout" | "Edit Produk" | "Hapus Data" | "Broadcast" | "Ubah Permission"
    details,
    timestamp: new Date().toISOString()
  };
  
  db.settings.activityLogs.unshift(newLog);
  if (db.settings.activityLogs.length > 500) {
    db.settings.activityLogs.pop();
  }
}

// Seeder inside DB for default Owner
function getAdminsList(db: any) {
  if (!db.settings) db.settings = {};
  if (!db.settings.admins || !Array.isArray(db.settings.admins) || db.settings.admins.length === 0) {
    const salt = crypto.randomBytes(16).toString("hex");
    const hash = crypto.createHash("sha256").update("owner123" + salt).digest("hex");
    const defaultOwnerPasswordHash = `${salt}:${hash}`;
    db.settings.admins = [
      {
        id: "OWNER-01",
        username: "wanma2",
        passwordHash: defaultOwnerPasswordHash,
        role: "OWNER",
        isActive: true,
        permissions: [
          "view_products",
          "manage_products",
          "view_transactions",
          "add_transactions",
          "manage_broadcast",
          "manage_broadcast_targets",
          "view_logs",
          "export_data",
          "manage_backup"
        ],
        lastLogin: ""
      }
    ];
    botManager.saveDb(db);
  } else {
    // If the database has already been initialized, ensure the OWNER's username is updated to wanma2
    const owner = db.settings.admins.find((a: any) => a.role === "OWNER");
    if (owner && owner.username !== "wanma2") {
      owner.username = "wanma2";
      botManager.saveDb(db);
    }
  }
  return db.settings.admins;
}

// Helper to look up active session
function getSessionToken(req: any) {
  return req.headers["x-session-token"] || req.query.token;
}

function getSessionUser(req: any, db: any) {
  const token = getSessionToken(req);
  if (!token) return null;
  const session = activeSessions.get(token);
  if (!session) return null;
  if (Date.now() > session.expiresAt) {
    activeSessions.delete(token);
    return null;
  }
  const admins = getAdminsList(db);
  const foundAdmin = admins.find((a: any) => a.id === session.userId);
  if (!foundAdmin || !foundAdmin.isActive) {
    activeSessions.delete(token);
    return null;
  }
  return foundAdmin;
}

// Global API Interceptor for Session verification
app.use("/api", (req, res, next) => {
  if (req.path === "/auth/login" || req.path === "/auth/me" || req.path === "/status") {
    return next();
  }
  
  const db = botManager.getDb();
  const user = getSessionUser(req, db);
  if (!user) {
    return res.status(401).json({ error: "Sesi Anda telah kedaluwarsa atau belum terautentikasi. Silakan login." });
  }
  
  (req as any).user = user;
  next();
});

// ---------------- AUTHENTICATION & MANAGEMENT ENDPOINTS ----------------

app.post("/api/auth/login", (req, res) => {
  const { username, password } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: "Username dan password wajib diisi" });
  }

  const db = botManager.getDb();
  const admins = getAdminsList(db);
  const foundAdmin = admins.find((a: any) => a.username.toLowerCase() === username.trim().toLowerCase());

  if (!foundAdmin) {
    return res.status(401).json({ error: "Username atau password salah" });
  }

  if (!foundAdmin.isActive) {
    return res.status(403).json({ error: "Akun Anda telah dinonaktifkan oleh Owner" });
  }

  const matches = verifyPassword(password, foundAdmin.passwordHash);
  if (!matches) {
    return res.status(401).json({ error: "Username atau password salah" });
  }

  // Update last login
  foundAdmin.lastLogin = new Date().toISOString();
  botManager.saveDb(db);

  // Generate sessionId token
  const token = crypto.randomBytes(32).toString("hex");
  activeSessions.set(token, {
    userId: foundAdmin.id,
    username: foundAdmin.username,
    role: foundAdmin.role,
    permissions: foundAdmin.role === "OWNER" ? [
      "view_products",
      "manage_products",
      "view_transactions",
      "add_transactions",
      "manage_broadcast",
      "manage_broadcast_targets",
      "view_logs",
      "export_data",
      "manage_backup"
    ] : (foundAdmin.permissions || []),
    expiresAt: Date.now() + 24 * 60 * 60 * 1000 // 24 Hours duration
  });

  logActivity(db, foundAdmin.username, foundAdmin.role, "Login", "Admin berhasil masuk ke sistem");

  res.json({
    success: true,
    token,
    user: {
      id: foundAdmin.id,
      username: foundAdmin.username,
      role: foundAdmin.role,
      permissions: foundAdmin.role === "OWNER" ? [
        "view_products",
        "manage_products",
        "view_transactions",
        "add_transactions",
        "manage_broadcast",
        "manage_broadcast_targets",
        "view_logs",
        "export_data",
        "manage_backup"
      ] : (foundAdmin.permissions || []),
      lastLogin: foundAdmin.lastLogin
    }
  });
});

app.post("/api/auth/logout", (req, res) => {
  const token = getSessionToken(req);
  if (token) {
    const session = activeSessions.get(token);
    if (session) {
      const db = botManager.getDb();
      logActivity(db, session.username, session.role, "Logout", "Admin berhasil keluar dari sistem");
      activeSessions.delete(token);
    }
  }
  res.json({ success: true });
});

app.get("/api/auth/me", (req, res) => {
  const db = botManager.getDb();
  const user = getSessionUser(req, db);
  if (!user) {
    return res.status(401).json({ error: "Tidak ada session aktif." });
  }
  res.json({
    success: true,
    user: {
      id: user.id,
      username: user.username,
      role: user.role,
      permissions: user.role === "OWNER" ? [
        "view_products",
        "manage_products",
        "view_transactions",
        "add_transactions",
        "manage_broadcast",
        "manage_broadcast_targets",
        "view_logs",
        "export_data",
        "manage_backup"
      ] : (user.permissions || []),
      lastLogin: user.lastLogin
    }
  });
});

app.get("/api/auth/admins", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Akses Ditolak. Hanya Owner yang diperbolehkan melihat list Admin." });
  }

  const db = botManager.getDb();
  const admins = getAdminsList(db).map((a: any) => {
    const { passwordHash, ...safe } = a;
    return safe;
  });

  res.json(admins);
});

app.post("/api/auth/admins", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Akses Ditolak. Hanya Owner yang diperbolehkan membuat admin." });
  }

  const { username, password, role, permissions, isActive } = req.body;
  if (!username || !password) {
    return res.status(400).json({ error: "Username dan password wajib diisi." });
  }

  const cleanUsername = username.trim().toLowerCase();
  if (cleanUsername.length < 3) {
    return res.status(400).json({ error: "Username minimal 3 karakter." });
  }

  if (password.length < 5) {
    return res.status(400).json({ error: "Password minimal 5 karakter." });
  }

  const db = botManager.getDb();
  const admins = getAdminsList(db);
  if (admins.some((a: any) => a.username.toLowerCase() === cleanUsername)) {
    return res.status(400).json({ error: "Username sudah digunakan oleh admin lain." });
  }

  const newAdmin = {
    id: "ADM-" + Math.random().toString(36).substring(2, 9).toUpperCase(),
    username: cleanUsername,
    passwordHash: hashPassword(password),
    role: role || "ADMIN",
    isActive: isActive !== undefined ? isActive : true,
    permissions: permissions || [],
    lastLogin: ""
  };

  admins.push(newAdmin);
  botManager.saveDb(db);

  logActivity(db, user.username, user.role, "Ubah Permission", `Owner menambahkan admin baru: '${cleanUsername}'`);

  const { passwordHash, ...safe } = newAdmin;
  res.status(201).json(safe);
});

app.put("/api/auth/admins/:id", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Akses Ditolak. Hanya Owner yang diperbolehkan merubah profil & izin admin." });
  }

  const adminId = req.params.id;
  const db = botManager.getDb();
  const admins = getAdminsList(db);
  const found = admins.find((a: any) => a.id === adminId);

  if (!found) {
    return res.status(404).json({ error: "Admin tidak ditemukan." });
  }

  const { username, password, isActive, permissions } = req.body;

  // Protect Owner: "Tidak dapat didelete, dinonaktifkan, diturunkan role"
  if (found.role === "OWNER") {
    if (isActive === false) {
      return res.status(400).json({ error: "Proteksi Owner: Akun Owner utama tidak dapat dinonaktifkan." });
    }
  }

  if (username) {
    const cleanUsername = username.trim().toLowerCase();
    if (cleanUsername.length < 3) {
      return res.status(400).json({ error: "Username minimal 3 karakter." });
    }
    if (admins.some((a: any) => a.username.toLowerCase() === cleanUsername && a.id !== adminId)) {
      return res.status(400).json({ error: "Username sudah digunakan oleh akun lain." });
    }
    found.username = cleanUsername;
  }

  if (password) {
    if (password.length < 5) {
      return res.status(400).json({ error: "Password minimal 5 karakter." });
    }
    found.passwordHash = hashPassword(password);
  }

  if (isActive !== undefined) {
    found.isActive = isActive;
  }

  if (permissions !== undefined && found.role !== "OWNER") {
    found.permissions = permissions;
  }

  botManager.saveDb(db);

  logActivity(db, user.username, user.role, "Ubah Permission", `Owner memperbarui status/permissions admin: '${found.username}'`);

  const { passwordHash, ...safe } = found;
  res.json(safe);
});

app.delete("/api/auth/admins/:id", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Akses Ditolak. Hanya Owner yang dapat menghapus admin." });
  }

  const adminId = req.params.id;
  const db = botManager.getDb();
  const admins = getAdminsList(db);
  const idx = admins.findIndex((a: any) => a.id === adminId);

  if (idx === -1) {
    return res.status(404).json({ error: "Admin tidak ditemukan." });
  }

  const found = admins[idx];
  if (found.role === "OWNER") {
    return res.status(400).json({ error: "Proteksi Owner: Akun Owner utama tidak dapat dihapus." });
  }

  admins.splice(idx, 1);

  // Kick out sessions belonging to this deleted user
  for (const [token, sess] of activeSessions.entries()) {
    if (sess.userId === adminId) {
      activeSessions.delete(token);
    }
  }

  botManager.saveDb(db);

  logActivity(db, user.username, user.role, "Hapus Data", `Owner menghapus akun admin: '${found.username}'`);

  res.json({ success: true });
});

app.post("/api/auth/logout-all", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Forbidden. Hanya Owner yang dapat mereset seluruh sesi." });
  }

  const currentToken = getSessionToken(req);
  for (const [token, sess] of activeSessions.entries()) {
    if (token !== currentToken) {
      activeSessions.delete(token);
    }
  }

  const db = botManager.getDb();
  logActivity(db, user.username, user.role, "Logout", "Owner memutus semua sesi aktif pengguna lain secara masal");

  res.json({ success: true, message: "Semua sesi pengguna lainnya berhasil diputus." });
});

app.get("/api/auth/activity-logs", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("view_logs")) {
    return res.status(403).json({ error: "Akses ditolak. Tidak memiliki izin 'view_logs'." });
  }
  const db = botManager.getDb();
  res.json(db.settings?.activityLogs || []);
});

// ---------------- API ENDPOINTS ----------------

// Get local media files inside src/images or src/video
app.get("/api/local-media", (req, res) => {
  try {
    const imagesDir = path.join(process.cwd(), "src/images");
    const videoDir = path.join(process.cwd(), "src/video");

    // Ensure they exist
    if (!fs.existsSync(imagesDir)) fs.mkdirSync(imagesDir, { recursive: true });
    if (!fs.existsSync(videoDir)) fs.mkdirSync(videoDir, { recursive: true });

    const images = fs.readdirSync(imagesDir).filter(file => {
      const stats = fs.statSync(path.join(imagesDir, file));
      return stats.isFile() && !file.startsWith(".");
    });

    const videos = fs.readdirSync(videoDir).filter(file => {
      const stats = fs.statSync(path.join(videoDir, file));
      return stats.isFile() && !file.startsWith(".");
    });

    res.json({ images, videos });
  } catch (err: any) {
    console.error("Gagal membaca folder media lokal:", err);
    res.status(500).json({ error: "Gagal membaca folder media lokal: " + err.message });
  }
});

// Get Bot Status
app.get("/api/status", (req, res) => {
  res.json({
    status: botManager.status,
    qrCode: botManager.qrCode,
    pairingCode: botManager.pairingCode,
    phoneNumber: botManager.phoneNumber,
    pushName: botManager.pushName,
    error: botManager.error,
    isFirebaseEnabled: isFirebaseEnabled(),
  });
});

// Trigger connection (QR mode or Pairing code mode)
app.post("/api/connect", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Akses Ditolak. Hanya Owner yang diperbolehkan menghubungkan bot." });
  }
  const { phoneNumber } = req.body;
  botManager.initialize(phoneNumber);
  res.json({ success: true, message: "Koneksi bot berhasil diinisialisasi" });
});

// Disconnect bot
app.post("/api/disconnect", async (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER") {
    return res.status(403).json({ error: "Akses Ditolak. Hanya Owner yang diperbolehkan memutus bot." });
  }
  await botManager.disconnect();
  res.json({ success: true, message: "WhatsApp ditiadakan & session di-reset" });
});

// Fetch Database
app.get("/api/database", (req, res) => {
  res.json(botManager.getDb());
});

// Save Database changes
app.post("/api/database", (req, res) => {
  const user = (req as any).user;
  const db = botManager.getDb();
  
  const oldDb = JSON.parse(JSON.stringify(db)); // clone existing DB
  const incomingDb = req.body;

  // Detect and secure different changes
  const oldTx = oldDb.transactions || [];
  const incomingTx = incomingDb.transactions || [];
  const txDeleted = incomingTx.length < oldTx.length;
  const txModified = JSON.stringify(oldTx) !== JSON.stringify(incomingTx);

  const productsChanged = JSON.stringify(oldDb.products) !== JSON.stringify(incomingDb.products) || 
                          JSON.stringify(oldDb.categories) !== JSON.stringify(incomingDb.categories);
  const settingsChanged = JSON.stringify(oldDb.settings) !== JSON.stringify(incomingDb.settings);
  const commandsChanged = JSON.stringify(oldDb.commands) !== JSON.stringify(incomingDb.commands);

  let changeType = "";
  let actionDetails = "";

  if (txDeleted) {
    if (user.role !== "OWNER") {
      return res.status(403).json({ success: false, error: "Akses Ditolak. Hanya Owner yang dapat menghapus data transaksi." });
    }
    changeType = "Hapus Data";
    actionDetails = "Menghapus data transaksi";
  } else if (txModified) {
    const isAddingTxOnly = incomingTx.length > oldTx.length;
    if (user.role !== "OWNER" && !user.permissions.includes("add_transactions") && !user.permissions.includes("manage_products")) {
      return res.status(403).json({ success: false, error: "Akses Ditolak. Anda tidak memiliki izin 'add_transactions' atau 'manage_products'." });
    }
    changeType = isAddingTxOnly ? "Add Transaksi" : "Edit Transaksi";
    actionDetails = isAddingTxOnly ? "Menambahkan data transaksi baru" : "Mengubah data transaksi";
  }

  if (productsChanged) {
    // If stock level changed because of transaction addition/modification, allow it (transaction inputs decrease/increase stock)
    const isStockUpdateByTx = txModified && (user.permissions.includes("add_transactions") || user.permissions.includes("manage_products"));
    if (user.role !== "OWNER" && !user.permissions.includes("manage_products") && !isStockUpdateByTx) {
      return res.status(403).json({ success: false, error: "Akses Ditolak. Anda tidak memiliki izin 'manage_products' untuk mengelola produk." });
    }
    if (!changeType) {
      changeType = "Edit Produk";
      actionDetails = "Mengubah data produk/kategori";
    }
  }

  if (settingsChanged) {
    if (user.role !== "OWNER" && !user.permissions.includes("manage_broadcast")) {
      return res.status(403).json({ success: false, error: "Akses Ditolak. Pengaturan sistem hanya dapat diubah oleh Owner atau Admin dengan izin 'manage_broadcast'." });
    }
    
    // Prevent non-owners from editing sensitive properties of settings (like admin list!)
    if (user.role !== "OWNER") {
      // Keep old admin list and settings that are not broadcast-related
      incomingDb.settings.admins = oldDb.settings.admins || [];
    }
    if (!changeType) {
      changeType = "Edit Pengaturan";
      actionDetails = "Mengubah konfigurasi pengaturan toko / template pesan";
    }
  }

  if (commandsChanged) {
    if (user.role !== "OWNER") {
      return res.status(403).json({ success: false, error: "Akses Ditolak. Hanya Owner yang diperbolehkan mengubah perintah bot." });
    }
    changeType = "Ubah Command";
    actionDetails = "Mengubah trigger command bot WhatsApp";
  }

  // Pre-log the activity directly into incomingDb settings block to avoid destructive overwrites
  if (changeType) {
    logActivity(incomingDb, user.username, user.role, changeType, actionDetails);
  }

  const success = botManager.saveDb(incomingDb);
  if (success) {
    res.json({ success: true, message: "Database berhasil diperbarui dan disimpan" });
  } else {
    res.status(500).json({ success: false, error: "Gagal menyimpan database" });
  }
});

// Get Message Logs
app.get("/api/logs", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("view_logs")) {
    return res.status(403).json({ error: "Akses Ditolak. Anda tidak memiliki izin 'view_logs'." });
  }
  res.json(botManager.logs);
});

// Send Message (manual/broadcast)
app.post("/api/send-message", async (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("manage_broadcast")) {
    return res.status(403).json({ error: "Akses Ditolak. Anda tidak memiliki izin 'manage_broadcast' untuk mengirim pesan." });
  }

  const { to, text, mediaUrl, mediaType } = req.body;
  if (!to || !text) {
    return res.status(400).json({ error: "Nomor tujuan (to) dan isi pesan (text) wajib diisi" });
  }

  // Ensure bot is initialized and sock exists
  if (!botManager.sock) {
    return res.status(400).json({ error: "WhatsApp belum terhubung. Silakan hubungkan bot terlebih dahulu di tab status." });
  }

  try {
    const db = botManager.getDb();
    logActivity(db, user.username, user.role, "Broadcast", `Mengirimkan pesan manual ke: ${to}`);
    botManager.saveDb(db);
    let jid = to.trim();
    if (!jid.endsWith("@s.whatsapp.net") && !jid.endsWith("@g.us")) {
      let clean = jid.replace(/[^0-9]/g, "");
      if (clean.length > 14) {
        const resolved = resolveLidToPn(`${clean}@lid`);
        jid = resolved.endsWith("@lid") ? `${clean}@s.whatsapp.net` : resolved;
      } else {
        if (clean.startsWith("0")) {
          clean = "62" + clean.slice(1);
        } else if (clean.startsWith("8")) {
          clean = "62" + clean;
        }
        jid = `${clean}@s.whatsapp.net`;
      }
    }

    if (mediaUrl && (mediaType === "image" || mediaType === "video")) {
      const isBase64 = mediaUrl.startsWith("data:");
      if (isBase64) {
        const matched = mediaUrl.match(/^data:([a-zA-Z0-9-\/]+);base64,(.+)$/) || mediaUrl.match(/^data:image\/([a-zA-Z0-9+-\/]+);base64,(.+)$/);
        if (matched) {
          let mime = matched[1];
          if (!mime.includes("/")) {
            mime = mediaType === "image" ? `image/${mime}` : `video/${mime}`;
          }
          const base64Content = matched[2];
          const buffer = Buffer.from(base64Content, "base64");
          await botManager.sock.sendMessage(jid, {
            [mediaType]: buffer,
            caption: text,
            mimetype: mime
          });
        } else {
          await botManager.sock.sendMessage(jid, {
            [mediaType]: { url: mediaUrl },
            caption: text
          });
        }
      } else {
        // Check local files first
        const folder = mediaType === "image" ? "src/images" : "src/video";
        const filePath = path.join(process.cwd(), folder, mediaUrl);
        if (fs.existsSync(filePath)) {
          const buffer = fs.readFileSync(filePath);
          await botManager.sock.sendMessage(jid, {
            [mediaType]: buffer,
            caption: text,
            mimetype: getFileMimeType(mediaUrl, mediaType === "image" ? "image/png" : "video/mp4")
          });
        } else {
          await botManager.sock.sendMessage(jid, {
            [mediaType]: { url: mediaUrl },
            caption: text
          });
        }
      }
    } else {
      await botManager.sock.sendMessage(jid, { text });
    }

    botManager.addLog({
      from: "Broadcast manual",
      senderName: botManager.pushName || "WANZZ BOT",
      message: `${text}${mediaUrl ? `\n[Media: ${mediaUrl}]` : ""}`,
      type: "outgoing",
      status: "Berhasil",
    });

    res.json({ success: true, message: "Pesan berhasil dikirim" });
  } catch (err: any) {
    console.error("Gagal mengirim pesan manual:", err);
    res.status(500).json({ error: err.message || "Gagal mengirim pesan" });
  }
});

// Clear Logs
app.post("/api/logs/clear", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("view_logs")) {
    return res.status(403).json({ error: "Akses Ditolak. Anda tidak memiliki izin 'view_logs'." });
  }
  botManager.logs = [];
  const db = botManager.getDb();
  logActivity(db, user.username, user.role, "Hapus Data", "Admin menghapus semua riwayat logs transaksi WhatsApp");
  botManager.saveDb(db);
  res.json({ success: true });
});

// Simulate receiving/answering a command to test without real phone link
app.post("/api/simulate", async (req, res) => {
  const { message, senderName, isGroup, customFrom } = req.body;
  if (!message) {
    return res.status(400).json({ error: "Pesan tidak boleh kosong" });
  }

  const name = senderName || "Pelanggan Simulasi";
  let from = isGroup ? "120363024847291040@g.us" : "simulated-user@s.whatsapp.net";
  if (customFrom) {
    from = customFrom;
  }

  // Process simulated behavior
  const result = await botManager.handleIncomingMessage(from, message, name, true);

  // Storing simulated exchange inside in-memory logs for presentation UI
  if (result.response) {
    botManager.addLog({
      from: "Simulasi (" + name + ")",
      senderName: name,
      message: message,
      type: "incoming",
      status: "Simulated",
    });

    botManager.addLog({
      from: "Simulasi Bot",
      senderName: botManager.pushName || "WANZZ BOT",
      message: result.response,
      type: "outgoing",
      status: result.status,
    });
  }

  res.json({
    success: true,
    ...result,
  });
});

// Manual Backup to Firestore Route
app.post("/api/backup-now", async (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("manage_backup")) {
    return res.status(403).json({ success: false, error: "Akses Ditolak. Anda tidak memiliki izin 'manage_backup' untuk melakukan backup." });
  }

  if (!isFirebaseEnabled()) {
    return res.status(400).json({ success: false, error: "Firebase Firestore belum dikonfigurasi pada environment ini" });
  }
  try {
    const dbData = botManager.getDb();
    await syncToFirestore(dbData);
    logActivity(dbData, user.username, user.role, "Backup", "Admin melakukan backup manual ke Cloud Firestore");
    botManager.saveDb(dbData);
    res.json({ success: true, message: "Semua data berhasil dibackup ke Cloud Firestore!" });
  } catch (e: any) {
    console.error("[Manual Backup Error]:", e);
    res.status(500).json({ success: false, error: e.message || "Gagal melakukan backup manual ke Firestore" });
  }
});

// Get scheduled broadcasts list
app.get("/api/scheduled-broadcasts", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("manage_broadcast")) {
    return res.status(403).json({ error: "Akses Ditolak. Anda tidak memiliki izin 'manage_broadcast'." });
  }
  const db = botManager.getDb();
  res.json(db.scheduledBroadcasts || []);
});

// Create/Update scheduled broadcast
app.post("/api/scheduled-broadcasts", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("manage_broadcast")) {
    return res.status(403).json({ error: "Akses Ditolak. Anda tidak memiliki izin 'manage_broadcast'." });
  }

  const db = botManager.getDb();
  const broadcast = req.body;
  if (!broadcast.id) {
    broadcast.id = "SCH-" + Math.random().toString(36).substring(2, 9).toUpperCase();
  }
  if (!db.scheduledBroadcasts) {
    db.scheduledBroadcasts = [];
  }

  const idx = db.scheduledBroadcasts.findIndex((b: any) => b.id === broadcast.id);
  if (idx !== -1) {
    db.scheduledBroadcasts[idx] = { ...db.scheduledBroadcasts[idx], ...broadcast };
  } else {
    db.scheduledBroadcasts.push(broadcast);
  }
  
  logActivity(db, user.username, user.role, "Broadcast", `Penjadwalan broadcast dibuat/diubah ID: ${broadcast.id}`);
  botManager.saveDb(db);
  res.json({ success: true, broadcast });
});

// Delete/Cancel scheduled broadcast
app.delete("/api/scheduled-broadcasts/:id", (req, res) => {
  const user = (req as any).user;
  if (user.role !== "OWNER" && !user.permissions.includes("manage_broadcast")) {
    return res.status(403).json({ error: "Akses Ditolak. Anda tidak memiliki izin 'manage_broadcast'." });
  }

  const db = botManager.getDb();
  const id = req.params.id;
  if (db.scheduledBroadcasts) {
    db.scheduledBroadcasts = db.scheduledBroadcasts.filter((b: any) => b.id !== id);
    logActivity(db, user.username, user.role, "Broadcast", `Membatalkan/menghapus penjadwalan broadcast ID: ${id}`);
    botManager.saveDb(db);
  }
  res.json({ success: true });
});

// Vite & Static file hosting for dev / production
async function startServer() {
  app.use("/src/images", express.static(path.join(process.cwd(), "src/images")));
  app.use("/src/video", express.static(path.join(process.cwd(), "src/video")));

  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server WhatsApp Bot Wanzz Store berjalan di http://localhost:${PORT}`);
  });
}

startServer();
