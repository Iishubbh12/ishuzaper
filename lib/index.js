"use strict";
var __createBinding, __exportStar, __importDefault;
const chalk = require("chalk");
console.log(chalk.bold.blueBright("╔══════════════════════════════════════════╗"));
console.log(chalk.bold.blueBright("║") + chalk.bold.cyanBright("    🦄 THANK YOU FOR USING ISHU-ZAPER 🦄   ") + chalk.bold.blueBright("║"));
console.log(chalk.bold.blueBright("╠══════════════════════════════════════════╣"));
console.log(chalk.bold.blueBright("║  ") + chalk.cyanBright("📱 Telegram : ") + chalk.greenBright.bold("@Ishu_Zaper"));
console.log(chalk.bold.blueBright("║  ") + chalk.cyanBright("▶️  YouTube  : ") + chalk.redBright.bold("@Mr_ishuvaa"));
console.log(chalk.bold.blueBright("║  ") + chalk.cyanBright("💬 WhatsApp : ") + chalk.greenBright.bold("+94788848044"));
console.log(chalk.bold.blueBright("║  ") + chalk.cyanBright("🎵 TikTok   : ") + chalk.yellowBright.bold("@mr_ishuvaa"));
console.log(chalk.bold.blueBright("╚══════════════════════════════════════════╝\n"));
__createBinding = this && this.__createBinding || (Object.create ? function(o, m, k, k2) {
    var desc;
    if (k2 === void 0) k2 = k;
    desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) desc = {
        enumerable: true,
        get: function() {
            return m[k]
        }
    };
    Object.defineProperty(o, k2, desc)
} : function(o, m, k, k2) {
    if (k2 === void 0) k2 = k;
    o[k2] = m[k]
});
__exportStar = this && this.__exportStar || function(m, exports) {
    var p;
    for (p in m)
        if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports, p)) __createBinding(exports, m, p)
};
__importDefault = this && this.__importDefault || function(mod) {
    return mod && mod.__esModule ? mod : {
        default: mod
    }
};
Object.defineProperty(exports, "__esModule", {
    value: true
});
exports.makeWASocket = void 0;
const Socket_1 = __importDefault(require("./Socket"));
exports.makeWASocket = Socket_1.default;
__exportStar(require("../WAProto"), exports);
__exportStar(require("./Utils"), exports);
__exportStar(require("./Types"), exports);
__exportStar(require("./Store"), exports);
__exportStar(require("./Defaults"), exports);
__exportStar(require("./WABinary"), exports);
__exportStar(require("./WAM"), exports);
__exportStar(require("./WAUSync"), exports);
exports.default = Socket_1.default;