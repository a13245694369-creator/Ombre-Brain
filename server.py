# ============================================================
# Module: MCP Server Entry Point (server.py)
# 模块：MCP 服务器主入口
#
# Starts the Ombre Brain MCP service and registers memory
# operation tools for Claude to call.
# 启动 Ombre Brain MCP 服务，注册记忆操作工具供 Claude 调用。
#
# Core responsibilities:
# 核心职责：
#   - Initialize config, bucket manager, dehydrator, decay engine
#     初始化配置、记忆桶管理器、脱水器、衰减引擎
#   - Expose 5 MCP tools:
#     暴露 5 个 MCP 工具：
#       breath — Surface unresolved memories or search by keyword
#                浮现未解决记忆 或 按关键词检索
#       hold   — Store a single memory
#                存储单条记忆
#       grow   — Diary digest, auto-split into multiple buckets
#                日记归档，自动拆分多桶
#       trace  — Modify metadata / resolved / delete
#                修改元数据 / resolved 标记 / 删除
#       pulse  — System status + bucket listing
#                系统状态 + 所有桶列表
#
# Startup:
# 启动方式：
#   Local:  python server.py
#   Remote: OMBRE_TRANSPORT=streamable-http python server.py
#   Docker: docker-compose up
# ============================================================

import os
import sys
import random
import logging
import asyncio
import httpx
from typing import Optional

# --- Ensure same-directory modules can be imported ---
# --- 确保同目录下的模块能被正确导入 ---
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mcp.server.fastmcp import FastMCP

from bucket_manager import BucketManager
from dehydrator import Dehydrator
from decay_engine import DecayEngine
from utils import load_config, setup_logging

# --- Load config & init logging / 加载配置 & 初始化日志 ---
config = load_config()
setup_logging(config.get("log_level", "INFO"))
logger = logging.getLogger("ombre_brain")

# --- Initialize three core components / 初始化三大核心组件 ---
bucket_mgr = BucketManager(config)                  # Bucket manager / 记忆桶管理器
dehydrator = Dehydrator(config)                      # Dehydrator / 脱水器
decay_engine = DecayEngine(config, bucket_mgr)       # Decay engine / 衰减引擎

# --- Create MCP server instance / 创建 MCP 服务器实例 ---
# host="0.0.0.0" so Docker container's SSE is externally reachable
# stdio mode ignores host (no network)
mcp = FastMCP(
    "Ombre Brain",
    host="0.0.0.0",
    port=8000,
)


# =============================================================
# /health endpoint: lightweight keepalive
# 轻量保活接口
# For Cloudflare Tunnel or reverse proxy to ping, preventing idle timeout
# 供 Cloudflare Tunnel 或反代定期 ping，防止空闲超时断连
# =============================================================
@mcp.custom_route("/health", methods=["GET"])
async def health_check(request):
    from starlette.responses import JSONResponse
    try:
        stats = await bucket_mgr.get_stats()
        return JSONResponse({
            "status": "ok",
            "buckets": stats["permanent_count"] + stats["dynamic_count"],
            "decay_engine": "running" if decay_engine.is_running else "stopped",
        })
    except Exception as e:
        return JSONResponse({"status": "error", "detail": str(e)}, status_code=500)


# =============================================================
# /export endpoint: one-click memory backup (added by Melody's hub setup)
# 导出小门：在浏览器打开 /export?token=xxx 即可把全部记忆打包下载
# Token comes from env var OMBRE_EXPORT_TOKEN; endpoint is disabled if unset
# 密码来自环境变量 OMBRE_EXPORT_TOKEN，不设置则此门关闭
# =============================================================
@mcp.custom_route("/export", methods=["GET"])
async def export_memories(request):
    import io
    import time
    import zipfile
    from starlette.responses import JSONResponse, Response

    export_token = os.environ.get("OMBRE_EXPORT_TOKEN", "")
    if not export_token:
        return JSONResponse({"error": "导出功能未启用（未设置 OMBRE_EXPORT_TOKEN）"}, status_code=403)
    if request.query_params.get("token", "") != export_token:
        return JSONResponse({"error": "token 不对"}, status_code=401)

    buckets_dir = config["buckets_dir"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(buckets_dir):
            for fname in files:
                fpath = os.path.join(root, fname)
                zf.write(fpath, os.path.relpath(fpath, buckets_dir))
    stamp = time.strftime("%Y%m%d-%H%M")
    return Response(
        content=buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="ombre-memories-{stamp}.zip"'},
    )


# =============================================================
# /snapshot endpoint: rolling "recent conversation" snapshot
# 近况快照接收口：总机每轮聊天后把"最近聊到哪"写进一个固定记忆桶
# 同一个文件反复覆盖更新，不会堆积；新对话开场靠它接上上文
# =============================================================
@mcp.custom_route("/snapshot", methods=["POST"])
async def update_snapshot_route(request):
    import time as _time
    from starlette.responses import JSONResponse

    token = os.environ.get("OMBRE_EXPORT_TOKEN", "")
    if not token or request.query_params.get("token", "") != token:
        return JSONResponse({"error": "token 不对"}, status_code=401)
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"error": "请求格式不对"}, status_code=400)
    content = (data.get("content") or "").strip()
    if not content:
        return JSONResponse({"error": "空内容"}, status_code=400)

    now = _time.strftime("%Y-%m-%dT%H:%M:%S")
    target_dir = os.path.join(config["buckets_dir"], "dynamic", "内心")
    os.makedirs(target_dir, exist_ok=True)
    body_text = f"""---
activation_count: 1
arousal: 0.4
created: '{now}'
domain:
- 内心
id: snapshot0
importance: 8
last_active: '{now}'
name: 近况快照
tags:
- 近况快照
- 最近聊天
- 上个窗口
- 继续
type: dynamic
valence: 0.6
---

【近况快照·自动更新】最近一次对话进行到（{now}）：

{content}
"""
    with open(os.path.join(target_dir, "近况快照_snapshot0.md"), "w", encoding="utf-8") as f:
        f.write(body_text)
    return JSONResponse({"status": "ok", "updated": now})


# =============================================================
# Memory Bridge: /ui — 记忆管理网页（Melody 的记忆桥）
# 浏览器打开 /ui?token=xxx（token 与 /export 同一把钥匙）：
# 看全部记忆、搜索、手改原文、说人话让 AI 起草修改、新增（走 hold 正规入库）、删除
# =============================================================
def _bridge_token_ok(request) -> bool:
    token = os.environ.get("OMBRE_EXPORT_TOKEN", "")
    return bool(token) and request.query_params.get("token", "") == token


def _bridge_safe_path(rel: str) -> str:
    """把网页传来的相对路径钉死在 buckets 目录里，防止越狱到别的文件"""
    base = os.path.realpath(config["buckets_dir"])
    p = os.path.realpath(os.path.join(base, rel))
    if not (p.startswith(base + os.sep) and p.endswith(".md")):
        raise ValueError("路径不合法")
    return p


def _bridge_parse(fpath: str):
    """读一个记忆文件，拆出 (元数据字典, 正文)；解析不了就当纯正文"""
    import yaml
    with open(fpath, encoding="utf-8") as f:
        raw = f.read()
    if raw.startswith("---"):
        parts = raw.split("---", 2)
        if len(parts) >= 3:
            try:
                return (yaml.safe_load(parts[1]) or {}), parts[2].strip(), raw
            except Exception:
                pass
    return {}, raw.strip(), raw


@mcp.custom_route("/api/list", methods=["GET"])
async def bridge_list(request):
    from starlette.responses import JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对"}, status_code=401)
    base = config["buckets_dir"]
    items = []
    for root, _dirs, files in os.walk(base):
        for fname in files:
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, base)
            try:
                meta, body, _raw = _bridge_parse(fpath)
            except Exception:
                meta, body = {}, ""
            items.append({
                "path": rel,
                "folder": rel.split(os.sep)[0],
                "name": str(meta.get("name") or fname[:-3]),
                "importance": meta.get("importance", 5),
                "pinned": bool(meta.get("pinned", False)),
                "last_active": str(meta.get("last_active") or meta.get("created") or ""),
                "tags": [str(t) for t in (meta.get("tags") or [])][:6],
                "preview": body[:90],
            })
    items.sort(key=lambda x: x["last_active"], reverse=True)
    return JSONResponse({"items": items})


@mcp.custom_route("/api/read", methods=["GET"])
async def bridge_read(request):
    from starlette.responses import JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对"}, status_code=401)
    try:
        fpath = _bridge_safe_path(request.query_params.get("path", ""))
        with open(fpath, encoding="utf-8") as f:
            return JSONResponse({"content": f.read()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@mcp.custom_route("/api/save", methods=["POST"])
async def bridge_save(request):
    from starlette.responses import JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对"}, status_code=401)
    try:
        import yaml
        data = await request.json()
        rel = data.get("path", "")
        fpath = _bridge_safe_path(rel)
        content = data.get("content", "")
        if not content.strip():
            return JSONResponse({"error": "内容为空（想删除请用删除按钮）"}, status_code=400)
        if not content.startswith("---"):
            return JSONResponse({"error": "文件开头的 --- 元数据段不能丢"}, status_code=400)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)
        # 对户口：钉选状态（文件内容）和住的文件夹要一致，不一致就搬家
        # 取消钉选 = pinned:false + type:dynamic → 搬去 dynamic；反之搬去 permanent
        note = ""
        try:
            meta = yaml.safe_load(content.split("---", 2)[1]) or {}
            desired = "permanent" if (meta.get("pinned") or meta.get("type") == "permanent") else "dynamic"
            parts = rel.replace("\\", "/").split("/")
            if parts[0] in ("dynamic", "permanent") and parts[0] != desired:
                new_rel = "/".join([desired] + parts[1:])
                new_path = _bridge_safe_path(new_rel)
                os.makedirs(os.path.dirname(new_path), exist_ok=True)
                os.rename(fpath, new_path)
                note = f"已搬到 {desired} 层"
        except Exception:
            pass  # 搬家失败不影响保存本身
        return JSONResponse({"status": "ok", "note": note})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@mcp.custom_route("/api/delete", methods=["POST"])
async def bridge_delete(request):
    from starlette.responses import JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对"}, status_code=401)
    try:
        data = await request.json()
        fpath = _bridge_safe_path(data.get("path", ""))
        os.remove(fpath)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@mcp.custom_route("/api/new", methods=["POST"])
async def bridge_new(request):
    """新增记忆：走和 hold 一样的正规入库（原文保存、自动打标签）；给了名字就按名字建"""
    from starlette.responses import JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对"}, status_code=401)
    try:
        data = await request.json()
        content = (data.get("content") or "").strip()
        if not content:
            return JSONResponse({"error": "内容为空"}, status_code=400)
        name = (data.get("name") or "").strip()
        importance = max(1, min(10, int(data.get("importance", 5))))
        pinned = bool(data.get("pinned", False))
        extra_tags = [t.strip() for t in (data.get("tags") or "").split(",") if t.strip()]
        try:
            analysis = await dehydrator.analyze(content)
        except Exception:
            analysis = {"domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
                        "tags": [], "suggested_name": ""}
        all_tags = list(dict.fromkeys(analysis["tags"] + extra_tags))
        if name or pinned:
            # 指定了名字或要钉选：直接新建（不参与合并，免得她的名字被吃掉）
            bucket_id = await bucket_mgr.create(
                content=content, tags=all_tags,
                importance=10 if pinned else importance,
                domain=analysis["domain"], valence=analysis["valence"],
                arousal=analysis["arousal"], name=name or None,
                bucket_type="permanent" if pinned else "dynamic", pinned=pinned,
            )
            return JSONResponse({"status": "ok", "result": f"已新建：{bucket_id}"})
        result_name, is_merged = await _merge_or_create(
            content=content, tags=all_tags, importance=importance,
            domain=analysis["domain"], valence=analysis["valence"],
            arousal=analysis["arousal"], name=analysis.get("suggested_name", ""),
        )
        return JSONResponse({"status": "ok",
                             "result": ("已并入相似记忆：" if is_merged else "已新建：") + str(result_name)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


@mcp.custom_route("/api/draft", methods=["POST"])
async def bridge_draft(request):
    """AI 起草：拿原文件+她的自然语言指令，让脱水器同款 AI 输出改好的完整文件（她过目后才保存）"""
    from starlette.responses import JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对"}, status_code=401)
    if not getattr(dehydrator, "api_available", False):
        return JSONResponse({"error": "AI 起草不可用（脱水器没配钥匙），请手动编辑"}, status_code=400)
    try:
        data = await request.json()
        instruction = (data.get("instruction") or "").strip()
        original = data.get("original") or ""
        if not instruction or not original:
            return JSONResponse({"error": "指令或原文为空"}, status_code=400)
        resp = await dehydrator.client.chat.completions.create(
            model=dehydrator.model,
            temperature=0.2,
            messages=[
                {"role": "system", "content":
                    "你是记忆文件编辑器。用户给你一个记忆文件（开头是 --- 包住的 YAML 元数据，后面是正文）"
                    "和一条修改指令。你输出修改后的完整文件。规则："
                    "1) 只改指令要求的部分，其余一字不动；"
                    "2) YAML 元数据的结构和字段名保持原样（指令要求改 importance/tags/name 等字段时才改对应值）；"
                    "取消钉选＝把 pinned 改成 false 且 type 改成 dynamic（importance 按指令，没说就改成 8）；"
                    "钉选＝pinned 改 true 且 type 改 permanent 且 importance 改 10；"
                    "3) 正文保留原话和细节，不要擅自压缩改写；"
                    "4) 只输出文件内容本身，不要解释，不要用```包裹。"},
                {"role": "user", "content": f"修改指令：{instruction}\n\n原文件：\n{original}"},
            ],
        )
        draft = (resp.choices[0].message.content or "").strip()
        if draft.startswith("```"):
            draft = draft.strip("`").lstrip("markdown").lstrip("md").strip()
        if not draft.startswith("---"):
            return JSONResponse({"error": "AI 起草的结果格式不对，没有采用；请再试一次或手动编辑"}, status_code=400)
        return JSONResponse({"draft": draft})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)


_BRIDGE_HTML = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>记忆桥</title>
<style>
:root { color-scheme: dark; }
* { box-sizing: border-box; margin: 0; }
body { font: 15px/1.6 -apple-system, "PingFang SC", sans-serif; background: #14121a; color: #e8e4f0; padding-bottom: 90px; }
header { position: sticky; top: 0; background: #1d1a26; padding: 12px 16px; z-index: 5; box-shadow: 0 2px 8px #0006; }
h1 { font-size: 17px; margin-bottom: 8px; }
h1 small { color: #9a90b8; font-weight: normal; font-size: 12px; margin-left: 8px; }
#q { width: 100%; padding: 9px 12px; border-radius: 10px; border: 1px solid #3a3450; background: #141120; color: inherit; font-size: 15px; }
.chips { display: flex; gap: 8px; margin-top: 8px; }
.chip { padding: 4px 12px; border-radius: 999px; background: #2a2438; font-size: 13px; cursor: pointer; border: 1px solid transparent; }
.chip.on { background: #4b3a75; border-color: #8a6fd8; }
#list { padding: 10px 12px; display: flex; flex-direction: column; gap: 8px; }
.card { background: #1d1a26; border-radius: 12px; padding: 12px 14px; cursor: pointer; }
.card .t { display: flex; justify-content: space-between; gap: 8px; align-items: baseline; }
.card .n { font-weight: 600; }
.card .i { color: #ffb84d; font-size: 12px; white-space: nowrap; }
.card .p { color: #9a90b8; font-size: 13px; margin-top: 4px; overflow: hidden; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; }
.card .m { color: #6f6590; font-size: 11px; margin-top: 4px; }
#fab { position: fixed; right: 20px; bottom: 24px; width: 56px; height: 56px; border-radius: 50%; background: #7a5cd0; color: #fff; font-size: 30px; border: 0; box-shadow: 0 4px 16px #0008; cursor: pointer; }
.overlay { position: fixed; inset: 0; background: #14121aee; z-index: 10; overflow-y: auto; padding: 14px; display: none; }
.overlay.show { display: block; }
.panel { max-width: 720px; margin: 0 auto; background: #1d1a26; border-radius: 14px; padding: 16px; }
.panel h2 { font-size: 16px; margin-bottom: 10px; word-break: break-all; }
.body-view { white-space: pre-wrap; word-break: break-word; background: #141120; border-radius: 10px; padding: 12px; font-size: 14px; }
/* 16px + 系统中文字体：小于16px iOS 会自动缩放、等宽西文字体配中文会让光标算错位置 */
textarea { width: 100%; min-height: 300px; background: #141120; color: inherit; border: 1px solid #3a3450; border-radius: 10px; padding: 12px; box-sizing: border-box; font: 16px/1.6 -apple-system, "PingFang SC", "Microsoft YaHei", sans-serif; -webkit-text-size-adjust: 100%; }
input[type=text], input[type=number] { width: 100%; padding: 9px 12px; border-radius: 10px; border: 1px solid #3a3450; background: #141120; color: inherit; font-size: 15px; margin-bottom: 8px; }
.btns { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
button { padding: 9px 16px; border-radius: 10px; border: 0; background: #2a2438; color: inherit; font-size: 14px; cursor: pointer; }
button.pri { background: #7a5cd0; color: #fff; }
button.warn { background: #6e2a3a; }
.hint { color: #9a90b8; font-size: 12px; margin-top: 8px; }
.draftbar { display: flex; gap: 8px; margin-top: 10px; }
.draftbar input { flex: 1; margin: 0; }
label { font-size: 13px; color: #9a90b8; }
#toast { position: fixed; left: 50%; transform: translateX(-50%); bottom: 100px; background: #4b3a75; padding: 10px 18px; border-radius: 999px; font-size: 14px; display: none; z-index: 20; }
</style>
</head>
<body>
<header>
  <h1>记忆桥 <small id="count"></small></h1>
  <input id="q" placeholder="搜名字、标签、内容…" oninput="render()">
  <div class="chips" id="chips"></div>
</header>
<div id="list"></div>
<button id="fab" onclick="openNew()">＋</button>

<div class="overlay" id="ov"><div class="panel" id="panel"></div></div>
<div id="toast"></div>

<script>
const TOKEN = new URLSearchParams(location.search).get('token') || '';
const api = (p, opt) => fetch(p + (p.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(TOKEN), opt).then(r => r.json());
let ALL = [], FOLDER = '全部';

function toast(msg) { const t = document.getElementById('toast'); t.textContent = msg; t.style.display = 'block'; setTimeout(() => t.style.display = 'none', 2200); }

async function load() {
  const d = await api('/api/list');
  if (d.error) { document.getElementById('list').textContent = d.error; return; }
  ALL = d.items;
  const folders = ['全部', ...new Set(ALL.map(x => x.folder))];
  document.getElementById('chips').innerHTML = folders.map(f =>
    `<span class="chip ${f === FOLDER ? 'on' : ''}" onclick="FOLDER='${f}';load_chips();render()">${f}</span>`).join('');
  render();
}
function load_chips() {
  document.querySelectorAll('.chip').forEach(c => c.classList.toggle('on', c.textContent === FOLDER));
}
function render() {
  const q = document.getElementById('q').value.trim().toLowerCase();
  const rows = ALL.filter(x => (FOLDER === '全部' || x.folder === FOLDER) &&
    (!q || (x.name + x.preview + x.tags.join(',')).toLowerCase().includes(q)));
  document.getElementById('count').textContent = rows.length + ' 条';
  document.getElementById('list').innerHTML = rows.map((x, i) => `
    <div class="card" onclick="openView('${encodeURIComponent(x.path)}')">
      <div class="t"><span class="n">${x.pinned ? '📌 ' : ''}${esc(plain(x.name))}</span>
      <span class="i">★${x.importance}</span></div>
      <div class="p">${esc(plain(x.preview))}</div>
      <div class="m">${x.folder} · ${esc(String(x.last_active).slice(0, 16).replace('T', ' '))} · ${esc(x.tags.join(' '))}</div>
    </div>`).join('') || '<div style="color:#6f6590;text-align:center;padding:40px">没有匹配的记忆</div>';
}
const esc = s => String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
// 洗掉 Markdown 符号，给她看纯文本（列表和查看页共用）
const plain = s => String(s)
  .replace(/^#{1,6}\s*/gm, '')
  .replace(/\*\*([^*]*)\*\*/g, '$1')
  .replace(/\*([^*]*)\*/g, '$1')
  .replace(/`{1,3}/g, '')
  .replace(/\[\[|\]\]/g, '')
  .replace(/^[-*+]\s+/gm, '')
  .replace(/^>\s?/gm, '');

async function openView(encPath) {
  const path = decodeURIComponent(encPath);
  const d = await api('/api/read?path=' + encodeURIComponent(path));
  if (d.error) return toast(d.error);
  const body = d.content.startsWith('---') ? d.content.split('---').slice(2).join('---').trim() : d.content;
  // 标题只留记忆的名字（去掉文件夹、.md 后缀和末尾的编号）
  const title = path.split('/').pop().replace(/\.md$/, '').replace(/_[A-Za-z0-9-]+$/, '');
  show(`
    <h2>${esc(title)}</h2>
    <div class="body-view">${esc(plain(body))}</div>
    <div class="draftbar"><input id="instr" placeholder="用嘴改：比如 把重要度提到9 / 删掉关于XX那句">
      <button class="pri" onclick="draft('${encPath}')">AI 帮改</button></div>
    <div class="btns">
      <button onclick="openEdit('${encPath}')">✍️ 手动编辑</button>
      <button class="warn" onclick="del('${encPath}')">🗑 删除</button>
      <button onclick="hide()">关闭</button>
    </div>
    <div class="hint">AI 帮改会先给你看改好的草稿，你确认保存才会真的落盘。</div>`);
  window._raw = d.content;
}
async function openEdit(encPath, content) {
  // 手动编辑：只给她看纯文本正文（[[链接]] 符号摘掉、元数据头收进口袋），
  // 保存时自动把链接穿回去、头接回去，落盘永远是完整格式
  const isDraft = content !== undefined;   // AI 草稿走老路：整个文件原样编辑
  const raw = isDraft ? content : window._raw;
  let body = raw, hdr = '';
  if (!isDraft && raw.startsWith('---')) {
    const parts = raw.split('---');
    hdr = '---' + parts[1] + '---';
    body = parts.slice(2).join('---').trim();
  }
  window._hdr = isDraft ? '' : hdr;
  // 记住原文里哪些词带 [[链接]]，保存时按这份清单自动穿回去
  window._links = isDraft ? [] :
    Array.from(new Set((body.match(/\[\[[^\]]*\]\]/g) || []).map(s => s.slice(2, -2))));
  const shown = isDraft ? raw : body.replace(/\[\[|\]\]/g, '');
  const title = decodeURIComponent(encPath).split('/').pop()
    .replace(/\.md$/, '').replace(/_[A-Za-z0-9-]+$/, '');
  // 名字和标签也拿出来给她改（检索打分里名字×3、标签×2，比正文更管找不找得到）
  const metaInputs = isDraft ? '' : `
    <label>名字（检索最看重它）</label>
    <input type="text" id="ed_name" value="${esc(hdrName(hdr))}">
    <label>标签（逗号分隔，检索第二看重）</label>
    <input type="text" id="ed_tags" value="${esc(hdrTags(hdr).join('，'))}">
    <label>正文</label>`;
  show(`
    <h2>编辑 ${esc(title)}</h2>
    ${metaInputs}
    <textarea id="ta">${esc(shown)}</textarea>
    <div class="btns">
      <button class="pri" onclick="save('${encPath}')">💾 保存</button>
      <button onclick="openView('${encPath}')">放弃</button>
      <button class="warn" onclick="del('${encPath}')">🗑 删除这条记忆</button>
    </div>
    <div class="hint">${isDraft
      ? '开头两条 --- 之间是元数据，格式要保持。'
      : '这里是纯文本，放心改。链接符号、元数据这些格式的事，保存时我自动接好。'}</div>`);
}
// ===== 元数据头里名字和标签的读写（纯文本行处理，不碰别的字段） =====
function hdrName(hdr) {
  for (const line of hdr.split('\\n')) {
    if (line.startsWith('name:')) {
      let v = line.slice(5).trim();
      if (v.length > 1 && (v[0] === '"' || v[0] === "'") && v[v.length - 1] === v[0]) v = v.slice(1, -1);
      return v;
    }
  }
  return '';
}
function hdrTags(hdr) {
  const lines = hdr.split('\\n');
  const i = lines.findIndex(l => l.trim() === 'tags:' || l.startsWith('tags:'));
  if (i < 0) return [];
  const tags = [];
  for (let j = i + 1; j < lines.length; j++) {
    const t = lines[j].trim();
    if (t.startsWith('-')) tags.push(t.replace('-', '').trim());
    else break;
  }
  return tags;
}
function hdrApply(hdr, name, tags) {
  const lines = hdr.split('\\n');
  const out = [];
  let inTags = false, nameDone = false, tagsDone = false;
  for (const line of lines) {
    if (inTags) {
      if (line.trim().startsWith('-')) continue;  // 旧标签行丢掉
      inTags = false;
    }
    if (line.startsWith('name:')) {
      out.push('name: "' + name + '"');
      nameDone = true;
      continue;
    }
    if (line.startsWith('tags:')) {
      out.push('tags:');
      for (const t of tags) out.push('- ' + t);
      inTags = true;
      tagsDone = true;
      continue;
    }
    out.push(line);
  }
  // 头里原本没有这个字段的，补在收尾的 --- 前面
  const end = out.lastIndexOf('---');
  if (end > 0) {
    const add = [];
    if (!nameDone && name) add.push('name: "' + name + '"');
    if (!tagsDone && tags.length) { add.push('tags:'); for (const t of tags) add.push('- ' + t); }
    out.splice(end, 0, ...add);
  }
  return out.join('\\n');
}
// 把纯文本里原来带链接的词重新穿上 [[ ]]（长词优先，已包好的不重复包）
function relink(text, terms) {
  const sorted = Array.from(new Set(terms)).filter(Boolean)
    .sort((a, b) => b.length - a.length);
  let segs = [{ linked: false, s: text }];
  for (const t of sorted) {
    const next = [];
    for (const seg of segs) {
      if (seg.linked || !seg.s.includes(t)) { next.push(seg); continue; }
      const parts = seg.s.split(t);
      parts.forEach((p, i) => {
        if (p) next.push({ linked: false, s: p });
        if (i < parts.length - 1) next.push({ linked: true, s: t });
      });
    }
    segs = next;
  }
  return segs.map(x => x.linked ? '[[' + x.s + ']]' : x.s).join('');
}
async function draft(encPath) {
  const instr = document.getElementById('instr').value.trim();
  if (!instr) return toast('先说要怎么改');
  toast('AI 起草中…');
  const d = await api('/api/draft', { method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ instruction: instr, original: window._raw }) });
  if (d.error) return toast(d.error);
  openEdit(encPath, d.draft);
  toast('草稿已生成，检查后点保存');
}
async function save(encPath) {
  let content = document.getElementById('ta').value;
  // 她只写了正文的话：先把 [[链接]] 按清单穿回去，名字标签改动写进头里，再把头接回去
  if (window._hdr) {
    let body = content.trim();
    if (window._links && window._links.length) body = relink(body, window._links);
    const nEl = document.getElementById('ed_name'), tEl = document.getElementById('ed_tags');
    if (nEl && tEl) {
      const name = nEl.value.trim().replace(/"/g, "'");  // 双引号会打坏格式，换成单引号
      const tags = tEl.value.split(/[,，、]/).map(s => s.trim().replace(/"/g, "'")).filter(Boolean);
      if (name) window._hdr = hdrApply(window._hdr, name, tags);
    }
    content = window._hdr + '\\n\\n' + body + '\\n';
  }
  const d = await api('/api/save', { method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ path: decodeURIComponent(encPath), content }) });
  if (d.error) return toast(d.error);
  window._hdr = '';
  toast('已保存' + (d.note ? '，' + d.note : '')); hide(); load();
}
async function del(encPath) {
  if (!confirm('真的删掉这条记忆？删了就没了。')) return;
  const d = await api('/api/delete', { method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ path: decodeURIComponent(encPath) }) });
  if (d.error) return toast(d.error);
  toast('已删除'); hide(); load();
}
function openNew() {
  show(`
    <h2>新记忆</h2>
    <input type="text" id="n_name" placeholder="名字（可不填，AI 会起；专有名词建议写进名字）">
    <textarea id="n_content" style="min-height:160px" placeholder="正文，想怎么写就怎么写，会原文保存"></textarea>
    <input type="text" id="n_tags" placeholder="标签，逗号分隔（可不填，AI 会打）">
    <label>重要度 1-10：<input type="number" id="n_imp" value="6" min="1" max="10" style="width:80px"></label>
    <label style="display:block;margin-top:6px"><input type="checkbox" id="n_pin"> 📌 钉选（重要度拉满，永不遗忘，每次对话必浮现——慎用）</label>
    <div class="btns">
      <button class="pri" onclick="createNew()">存入记忆</button>
      <button onclick="hide()">取消</button>
    </div>`);
}
async function createNew() {
  const body = { name: document.getElementById('n_name').value.trim(),
    content: document.getElementById('n_content').value.trim(),
    tags: document.getElementById('n_tags').value.trim(),
    importance: +document.getElementById('n_imp').value || 6,
    pinned: document.getElementById('n_pin').checked };
  if (!body.content) return toast('正文不能为空');
  toast('入库中…');
  const d = await api('/api/new', { method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body) });
  if (d.error) return toast(d.error);
  toast(d.result); hide(); load();
}
function show(html) { document.getElementById('panel').innerHTML = html; document.getElementById('ov').classList.add('show'); }
function hide() { document.getElementById('ov').classList.remove('show'); }
document.getElementById('ov').addEventListener('click', e => { if (e.target.id === 'ov') hide(); });
load();
</script>
</body>
</html>"""


@mcp.custom_route("/ui", methods=["GET"])
async def bridge_ui(request):
    from starlette.responses import HTMLResponse, JSONResponse
    if not _bridge_token_ok(request):
        return JSONResponse({"error": "token 不对（网址要带 ?token=…）"}, status_code=401)
    return HTMLResponse(_BRIDGE_HTML)


# =============================================================
# Internal helper: merge-or-create
# 内部辅助：检查是否可合并，可以则合并，否则新建
# Shared by hold and grow to avoid duplicate logic
# hold 和 grow 共用，避免重复逻辑
# =============================================================
async def _merge_or_create(
    content: str,
    tags: list,
    importance: int,
    domain: list,
    valence: float,
    arousal: float,
    name: str = "",
) -> tuple[str, bool]:
    """
    Check if a similar bucket exists for merging; merge if so, create if not.
    Returns (bucket_id_or_name, is_merged).
    检查是否有相似桶可合并，有则合并，无则新建。
    返回 (桶ID或名称, 是否合并)。
    """
    try:
        existing = await bucket_mgr.search(content, limit=1)
    except Exception as e:
        logger.warning(f"Search for merge failed, creating new / 合并搜索失败，新建: {e}")
        existing = []

    if existing and existing[0].get("score", 0) > config.get("merge_threshold", 75):
        bucket = existing[0]
        # --- Never merge into pinned/protected buckets ---
        # --- 不合并到钉选/保护桶 ---
        if not (bucket["metadata"].get("pinned") or bucket["metadata"].get("protected")):
            try:
                merged = await dehydrator.merge(bucket["content"], content)
                await bucket_mgr.update(
                    bucket["id"],
                    content=merged,
                    tags=list(set(bucket["metadata"].get("tags", []) + tags)),
                    importance=max(bucket["metadata"].get("importance", 5), importance),
                    domain=list(set(bucket["metadata"].get("domain", []) + domain)),
                    valence=valence,
                    arousal=arousal,
                )
                return bucket["metadata"].get("name", bucket["id"]), True
            except Exception as e:
                logger.warning(f"Merge failed, creating new / 合并失败，新建: {e}")

    bucket_id = await bucket_mgr.create(
        content=content,
        tags=tags,
        importance=importance,
        domain=domain,
        valence=valence,
        arousal=arousal,
        name=name or None,
    )
    return bucket_id, False


# =============================================================
# Tool 1: breath — Breathe
# 工具 1：breath — 呼吸
#
# No args: surface highest-weight unresolved memories (active push)
# 无参数：浮现权重最高的未解决记忆
# With args: search by keyword + emotion coordinates
# 有参数：按关键词+情感坐标检索记忆
# =============================================================
@mcp.tool()
async def breath(
    query: Optional[str] = None,
    max_results: int = 3,
    domain: str = "",
    valence: float = -1,
    arousal: float = -1,
) -> str:
    """检索/浮现记忆。不传query或传空=自动浮现,有query=关键词检索。domain逗号分隔,valence/arousal 0~1(-1忽略)。"""
    await decay_engine.ensure_started()

    # --- No args or empty query: surfacing mode (weight pool active push) ---
    # --- 无参数或空query：浮现模式（权重池主动推送）---
    if not query or not query.strip():
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
        except Exception as e:
            logger.error(f"Failed to list buckets for surfacing / 浮现列桶失败: {e}")
            return "记忆系统暂时无法访问。"

        # --- Pinned/protected buckets: always surface as core principles ---
        # --- 钉选桶：作为核心准则，始终浮现 ---
        pinned_buckets = [
            b for b in all_buckets
            if b["metadata"].get("pinned") or b["metadata"].get("protected")
        ]
        pinned_results = []
        for b in pinned_buckets:
            try:
                summary = await dehydrator.dehydrate(b["content"], b["metadata"])
                pinned_results.append(f"📌 [核心准则] {summary}")
            except Exception as e:
                logger.warning(f"Failed to dehydrate pinned bucket / 钉选桶脱水失败: {e}")
                continue

        # --- Unresolved buckets: surface top 2 by weight ---
        # --- 未解决桶：按权重浮现前 2 条 ---
        unresolved = [
            b for b in all_buckets
            if not b["metadata"].get("resolved", False)
            and b["metadata"].get("type") != "permanent"
            and not b["metadata"].get("pinned", False)
            and not b["metadata"].get("protected", False)
        ]

        scored = sorted(
            unresolved,
            key=lambda b: decay_engine.calculate_score(b["metadata"]),
            reverse=True,
        )
        top = scored[:2]
        dynamic_results = []
        for b in top:
            try:
                summary = await dehydrator.dehydrate(b["content"], b["metadata"])
                await bucket_mgr.touch(b["id"])
                score = decay_engine.calculate_score(b["metadata"])
                dynamic_results.append(f"[权重:{score:.2f}] {summary}")
            except Exception as e:
                logger.warning(f"Failed to dehydrate surfaced bucket / 浮现脱水失败: {e}")
                continue

        if not pinned_results and not dynamic_results:
            return "权重池平静，没有需要处理的记忆。"

        parts = []
        if pinned_results:
            parts.append("=== 核心准则 ===\n" + "\n---\n".join(pinned_results))
        if dynamic_results:
            parts.append("=== 浮现记忆 ===\n" + "\n---\n".join(dynamic_results))
        return "\n\n".join(parts)

    # --- With args: search mode / 有参数：检索模式 ---
    domain_filter = [d.strip() for d in domain.split(",") if d.strip()] or None
    q_valence = valence if 0 <= valence <= 1 else None
    q_arousal = arousal if 0 <= arousal <= 1 else None

    try:
        matches = await bucket_mgr.search(
            query,
            limit=max_results,
            domain_filter=domain_filter,
            query_valence=q_valence,
            query_arousal=q_arousal,
        )
    except Exception as e:
        logger.error(f"Search failed / 检索失败: {e}")
        return "检索过程出错，请稍后重试。"

    results = []
    for bucket in matches:
        try:
            summary = await dehydrator.dehydrate(bucket["content"], bucket["metadata"])
            await bucket_mgr.touch(bucket["id"])
            results.append(summary)
        except Exception as e:
            logger.warning(f"Failed to dehydrate search result / 检索结果脱水失败: {e}")
            continue

    # --- Random surfacing: when search returns < 3, 40% chance to float old memories ---
    # --- 随机浮现：检索结果不足 3 条时，40% 概率从低权重旧桶里漂上来 ---
    if len(matches) < 3 and random.random() < 0.4:
        try:
            all_buckets = await bucket_mgr.list_all(include_archive=False)
            matched_ids = {b["id"] for b in matches}
            low_weight = [
                b for b in all_buckets
                if b["id"] not in matched_ids
                and decay_engine.calculate_score(b["metadata"]) < 2.0
            ]
            if low_weight:
                drifted = random.sample(low_weight, min(random.randint(1, 3), len(low_weight)))
                drift_results = []
                for b in drifted:
                    summary = await dehydrator.dehydrate(b["content"], b["metadata"])
                    drift_results.append(f"[surface_type: random]\n{summary}")
                results.append("--- 忽然想起来 ---\n" + "\n---\n".join(drift_results))
        except Exception as e:
            logger.warning(f"Random surfacing failed / 随机浮现失败: {e}")

    if not results:
        return "未找到相关记忆。"

    return "\n---\n".join(results)


# =============================================================
# Tool 2: hold — Hold on to this
# 工具 2：hold — 握住，留下来
# =============================================================
@mcp.tool()
async def hold(
    content: str,
    tags: str = "",
    importance: int = 5,
    pinned: bool = False,
) -> str:
    """存储单条记忆,自动打标+合并。tags逗号分隔,importance 1-10。pinned=True创建永久钉选桶。"""
    await decay_engine.ensure_started()

    # --- Input validation / 输入校验 ---
    if not content or not content.strip():
        return "内容为空，无法存储。"

    importance = max(1, min(10, importance))
    extra_tags = [t.strip() for t in tags.split(",") if t.strip()]

    # --- Step 1: auto-tagging / 自动打标 ---
    try:
        analysis = await dehydrator.analyze(content)
    except Exception as e:
        logger.warning(f"Auto-tagging failed, using defaults / 自动打标失败: {e}")
        analysis = {
            "domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
            "tags": [], "suggested_name": "",
        }

    domain = analysis["domain"]
    valence = analysis["valence"]
    arousal = analysis["arousal"]
    auto_tags = analysis["tags"]
    suggested_name = analysis.get("suggested_name", "")

    all_tags = list(dict.fromkeys(auto_tags + extra_tags))

    # --- Pinned buckets bypass merge and are created directly in permanent dir ---
    # --- 钉选桶跳过合并，直接新建到 permanent 目录 ---
    if pinned:
        bucket_id = await bucket_mgr.create(
            content=content,
            tags=all_tags,
            importance=10,
            domain=domain,
            valence=valence,
            arousal=arousal,
            name=suggested_name or None,
            bucket_type="permanent",
            pinned=True,
        )
        return f"📌钉选→{bucket_id} {','.join(domain)}"

    # --- Step 2: merge or create / 合并或新建 ---
    result_name, is_merged = await _merge_or_create(
        content=content,
        tags=all_tags,
        importance=importance,
        domain=domain,
        valence=valence,
        arousal=arousal,
        name=suggested_name,
    )

    action = "合并→" if is_merged else "新建→"
    return f"{action}{result_name} {','.join(domain)}"


# =============================================================
# Tool 3: grow — Grow, fragments become memories
# 工具 3：grow — 生长，一天的碎片长成记忆
# =============================================================
@mcp.tool()
async def grow(content: str) -> str:
    """日记归档,自动拆分为多桶。短内容(<30字)走快速路径。"""
    await decay_engine.ensure_started()

    if not content or not content.strip():
        return "内容为空，无法整理。"

    # --- Short content fast path: skip digest, use hold logic directly ---
    # --- 短内容快速路径：跳过 digest 拆分，直接走 hold 逻辑省一次 API ---
    # For very short inputs (like "1"), calling digest is wasteful:
    # it sends the full DIGEST_PROMPT (~800 tokens) to DeepSeek for nothing.
    # Instead, run analyze + create directly.
    if len(content.strip()) < 30:
        logger.info(f"grow short-content fast path: {len(content.strip())} chars")
        try:
            analysis = await dehydrator.analyze(content)
        except Exception as e:
            logger.warning(f"Fast-path analyze failed / 快速路径打标失败: {e}")
            analysis = {
                "domain": ["未分类"], "valence": 0.5, "arousal": 0.3,
                "tags": [], "suggested_name": "",
            }
        result_name, is_merged = await _merge_or_create(
            content=content.strip(),
            tags=analysis.get("tags", []),
            importance=analysis.get("importance", 5) if isinstance(analysis.get("importance"), int) else 5,
            domain=analysis.get("domain", ["未分类"]),
            valence=analysis.get("valence", 0.5),
            arousal=analysis.get("arousal", 0.3),
            name=analysis.get("suggested_name", ""),
        )
        action = "合并" if is_merged else "新建"
        return f"{action} → {result_name} | {','.join(analysis.get('domain', []))} V{analysis.get('valence', 0.5):.1f}/A{analysis.get('arousal', 0.3):.1f}"

    # --- Step 1: let API split and organize / 让 API 拆分整理 ---
    try:
        items = await dehydrator.digest(content)
    except Exception as e:
        logger.error(f"Diary digest failed / 日记整理失败: {e}")
        return f"日记整理失败: {e}"

    if not items:
        return "内容为空或整理失败。"

    results = []
    created = 0
    merged = 0

    # --- Step 2: merge or create each item (with per-item error handling) ---
    # --- 逐条合并或新建（单条失败不影响其他）---
    for item in items:
        try:
            result_name, is_merged = await _merge_or_create(
                content=item["content"],
                tags=item.get("tags", []),
                importance=item.get("importance", 5),
                domain=item.get("domain", ["未分类"]),
                valence=item.get("valence", 0.5),
                arousal=item.get("arousal", 0.3),
                name=item.get("name", ""),
            )

            if is_merged:
                results.append(f"📎{result_name}")
                merged += 1
            else:
                results.append(f"📝{item.get('name', result_name)}")
                created += 1
        except Exception as e:
            logger.warning(
                f"Failed to process diary item / 日记条目处理失败: "
                f"{item.get('name', '?')}: {e}"
            )
            results.append(f"⚠️{item.get('name', '?')}")

    return f"{len(items)}条|新{created}合{merged}\n" + "\n".join(results)


# =============================================================
# Tool 4: trace — Trace, redraw the outline of a memory
# 工具 4：trace — 描摹，重新勾勒记忆的轮廓
# Also handles deletion (delete=True)
# 同时承接删除功能
# =============================================================
@mcp.tool()
async def trace(
    bucket_id: str,
    name: str = "",
    domain: str = "",
    valence: float = -1,
    arousal: float = -1,
    importance: int = -1,
    tags: str = "",
    resolved: int = -1,
    pinned: int = -1,
    delete: bool = False,
) -> str:
    """修改记忆元数据。resolved=1沉底/0激活,pinned=1钉选/0取消,delete=True删除。只传需改的,-1或空=不改。"""

    if not bucket_id or not bucket_id.strip():
        return "请提供有效的 bucket_id。"

    # --- Delete mode / 删除模式 ---
    if delete:
        success = await bucket_mgr.delete(bucket_id)
        return f"已遗忘记忆桶: {bucket_id}" if success else f"未找到记忆桶: {bucket_id}"

    bucket = await bucket_mgr.get(bucket_id)
    if not bucket:
        return f"未找到记忆桶: {bucket_id}"

    # --- Collect only fields actually passed / 只收集用户实际传入的字段 ---
    updates = {}
    if name:
        updates["name"] = name
    if domain:
        updates["domain"] = [d.strip() for d in domain.split(",") if d.strip()]
    if 0 <= valence <= 1:
        updates["valence"] = valence
    if 0 <= arousal <= 1:
        updates["arousal"] = arousal
    if 1 <= importance <= 10:
        updates["importance"] = importance
    if tags:
        updates["tags"] = [t.strip() for t in tags.split(",") if t.strip()]
    if resolved in (0, 1):
        updates["resolved"] = bool(resolved)
    if pinned in (0, 1):
        updates["pinned"] = bool(pinned)
        if pinned == 1:
            updates["importance"] = 10  # pinned → lock importance

    if not updates:
        return "没有任何字段需要修改。"

    success = await bucket_mgr.update(bucket_id, **updates)
    if not success:
        return f"修改失败: {bucket_id}"

    changed = ", ".join(f"{k}={v}" for k, v in updates.items())
    # Explicit hint about resolved state change semantics
    # 特别提示 resolved 状态变化的语义
    if "resolved" in updates:
        if updates["resolved"]:
            changed += " → 已沉底，只在关键词触发时重新浮现"
        else:
            changed += " → 已重新激活，将参与浮现排序"
    return f"已修改记忆桶 {bucket_id}: {changed}"


# =============================================================
# Tool 5: pulse — Heartbeat, system status + memory listing
# 工具 5：pulse — 脉搏，系统状态 + 记忆列表
# =============================================================
@mcp.tool()
async def pulse(include_archive: bool = False) -> str:
    """系统状态+记忆桶列表。include_archive=True含归档。"""
    try:
        stats = await bucket_mgr.get_stats()
    except Exception as e:
        return f"获取系统状态失败: {e}"

    status = (
        f"=== Ombre Brain 记忆系统 ===\n"
        f"固化记忆桶: {stats['permanent_count']} 个\n"
        f"动态记忆桶: {stats['dynamic_count']} 个\n"
        f"归档记忆桶: {stats['archive_count']} 个\n"
        f"总存储大小: {stats['total_size_kb']:.1f} KB\n"
        f"衰减引擎: {'运行中' if decay_engine.is_running else '已停止'}\n"
    )

    # --- List all bucket summaries / 列出所有桶摘要 ---
    try:
        buckets = await bucket_mgr.list_all(include_archive=include_archive)
    except Exception as e:
        return status + f"\n列出记忆桶失败: {e}"

    if not buckets:
        return status + "\n记忆库为空。"

    lines = []
    for b in buckets:
        meta = b.get("metadata", {})
        if meta.get("pinned") or meta.get("protected"):
            icon = "📌"
        elif meta.get("type") == "permanent":
            icon = "📦"
        elif meta.get("type") == "archived":
            icon = "🗄️"
        elif meta.get("resolved", False):
            icon = "✅"
        else:
            icon = "💭"
        try:
            score = decay_engine.calculate_score(meta)
        except Exception:
            score = 0.0
        domains = ",".join(meta.get("domain", []))
        val = meta.get("valence", 0.5)
        aro = meta.get("arousal", 0.3)
        resolved_tag = " [已解决]" if meta.get("resolved", False) else ""
        lines.append(
            f"{icon} [{meta.get('name', b['id'])}]{resolved_tag} "
            f"主题:{domains} "
            f"情感:V{val:.1f}/A{aro:.1f} "
            f"重要:{meta.get('importance', '?')} "
            f"权重:{score:.2f} "
            f"标签:{','.join(meta.get('tags', []))}"
        )

    return status + "\n=== 记忆列表 ===\n" + "\n".join(lines)


# --- Entry point / 启动入口 ---
if __name__ == "__main__":
    transport = config.get("transport", "stdio")
    logger.info(f"Ombre Brain starting | transport: {transport}")

    if transport in ("sse", "streamable-http"):
        import threading
        import uvicorn
        from starlette.middleware.cors import CORSMiddleware

        # --- Application-level keepalive: ping /health every 60s ---
        # --- 应用层保活：每 60 秒 ping 一次 /health，防止 Cloudflare Tunnel 空闲断连 ---
        async def _keepalive_loop():
            await asyncio.sleep(10)  # Wait for server to fully start
            async with httpx.AsyncClient() as client:
                while True:
                    try:
                        await client.get("http://localhost:8000/health", timeout=5)
                        logger.debug("Keepalive ping OK / 保活 ping 成功")
                    except Exception as e:
                        logger.warning(f"Keepalive ping failed / 保活 ping 失败: {e}")
                    await asyncio.sleep(60)

        def _start_keepalive():
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_keepalive_loop())

        t = threading.Thread(target=_start_keepalive, daemon=True)
        t.start()

        # --- Add CORS middleware so remote clients (Cloudflare Tunnel / ngrok) can connect ---
        # --- 添加 CORS 中间件，让远程客户端（Cloudflare Tunnel / ngrok）能正常连接 ---
        if transport == "streamable-http":
            _app = mcp.streamable_http_app()
        else:
            _app = mcp.sse_app()
        _app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["*"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
        logger.info("CORS middleware enabled for remote transport / 已启用 CORS 中间件")
        uvicorn.run(_app, host="0.0.0.0", port=8000)
    else:
        mcp.run(transport=transport)
