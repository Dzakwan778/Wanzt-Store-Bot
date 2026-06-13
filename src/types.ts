/**
 * Types definition for the WhatsApp Bot Manager.
 */

export interface ProductVariant {
  id: string;             // Kode unik opsi varian (Cth: NETFLIX_1M)
  name: string;           // Nama opsi varian (Cth: Netflix 1 Bulan 1 User)
  price: number;          // Harga Jual (Rp)
  originalPrice?: number;  // Harga Modal (Rp)
  stockType?: 'UNKNOWN' | 'numeric'; // Tipe stok khusus varian
  stock?: number;         // Stok khusus varian (jika tipe stok numerik)
  variantCategory?: string; // Kategori varian (Cth: FAMPLAN, INDPLAN)
  alternativeCommands?: string[]; // Command alternatif (Cth: ["netprem", "net prem privat"])
}

export interface Product {
  id: string;
  name: string;
  category: string;
  details: string;
  image?: string; // Optional image URL or base64 for product
  searchCount?: number;
  variants?: ProductVariant[]; // Daftar pilihan/opsi layanan sub-produk
  variantCategories?: string[]; // Daftar kategori variasi per produk (Cth: ["FAMPLAN", "INDPLAN"])
}

export interface Transaction {
  id: string;
  customerName?: string;
  customerPhone?: string;
  productId: string;
  productName: string;
  quantity: number;
  totalPrice: number;
  originalPrice: number; // Harga asli (Rp)
  sellingPrice: number;  // Harga jual (Rp)
  paymentMethod: string; // QRIS, Dana, Gopay, etc.
  buyerPhone: string;    // Nomer pembeli
  status: 'Pending' | 'Success' | 'Failed';
  timestamp: string; // ISO String
}

export interface Category {
  id: string;
  name: string;
  icon?: string;
}

export interface BotSettings {
  storeName: string;
  menuTemplate: string;
  listTemplate?: string;
  infoTemplate: string;
  fallbackTemplate: string;
  welcomeTemplate: string;
  sendMenuWithImage: boolean;
  menuImageUrl: string;
  ownerNumber: string;
  ownerTemplate: string;
  paymentTemplate?: string;
  paymentQrisUrl?: string; // QRIS photo: base64 data url or local path
  orderSuccessTemplate?: string;
  prosesTemplate?: string;
  selesaiTemplate?: string;
  gagalTemplate?: string;
  whitelistedGroups?: string[]; // List of whitelisted group IDs
  excludedBroadcastPhones?: string[]; // List of clean phone numbers excluded from broadcast lists
  manualBroadcastTargets?: Array<{ name: string; phone: string; category?: string }>;
  welcomeImageUrl?: string;
  infoImageUrl?: string;
  fallbackImageUrl?: string;
  ownerImageUrl?: string;
  orderSuccessImageUrl?: string;
  infoEmptyTemplate?: string;
  infoNotFoundTemplate?: string;
  
  // Custom templates for command outputs
  ownerRestrictedTemplate?: string;
  adminRestrictedTemplate?: string;
  idGroupSuccessTemplate?: string;
  idGroupPrivateTemplate?: string;
  kickOutsideGroupTemplate?: string;
  kickEmptyTemplate?: string;
  kickBotSelfTemplate?: string;
  kickOwnerSelfTemplate?: string;
  kickOwnerDemoteTemplate?: string;
  kickBotNotAdminTemplate?: string;
  kickTargetIsAdminTemplate?: string;
  kickSuccessTemplate?: string;
  kickFailedTemplate?: string;
  addOutsideGroupTemplate?: string;
  addEmptyTemplate?: string;
  addBotNotAdminTemplate?: string;
  addSuccessTemplate?: string;
  addFailedTemplate?: string;
  bcaddEmptyTemplate?: string;
  bcaddNotWhitelistedTemplate?: string;
  bcaddSuccessTemplate?: string;
  closeOutsideGroupTemplate?: string;
  closeBotNotAdminTemplate?: string;
  closeSuccessTemplate?: string;
  openOutsideGroupTemplate?: string;
  openBotNotAdminTemplate?: string;
  openSuccessTemplate?: string;
  onlineOutsideGroupTemplate?: string;
  onlineEmptyTemplate?: string;
  onlineSuccessTemplate?: string;
  prosesNoReplyTemplate?: string;
  prosesExistingTemplate?: string;
  selesaiNoReplyTemplate?: string;
  selesaiNoTxTemplate?: string;
  selesaiForbiddenTemplate?: string;
  gagalNoReplyTemplate?: string;
  gagalNoTxTemplate?: string;
  gagalForbiddenTemplate?: string;
  paymentGroupOnlyTemplate?: string;
}

export interface ScheduledBroadcast {
  id: string;
  message: string;
  mediaUrl?: string;
  mediaType?: "none" | "image" | "video";
  targetCategories: string[]; // e.g. ["customer", "supplier", "reseller", "group"]
  scheduledTime: string; // ISO string format
  status: "pending" | "sent" | "failed" | "processing";
  sendLogs?: Array<{ phone: string; name: string; status: "success" | "failed"; error?: string; time: string }>;
  sentAt?: string;
}

export interface MessageLog {
  id: string;
  from: string;
  senderName: string;
  message: string;
  timestamp: string; // ISO string or format
  type: 'incoming' | 'outgoing' | 'system';
  status?: string; // e.g. "Sent menu", "Sent info", "Unhandled"
}

export interface ConnectionStatus {
  status: 'disconnected' | 'connecting' | 'connected';
  qrCode?: string; // QR code text or data URL
  pairingCode?: string; // Pairing code format (e.g. ABCD-EFGH)
  phoneNumber?: string; // Logged-in phone number
  pushName?: string; // Logged-in push name
  isFirebaseEnabled?: boolean; // Reflects if Firebase cloud connection is enabled
}

export interface BotCommand {
  id: string;
  trigger: string;
  response: string;
  mediaUrl?: string;
  mediaType: "none" | "image" | "video";
  description?: string;
}
