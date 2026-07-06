# ============================================================
# Melody 的中枢后端（总机）—— 记忆版
#
# 职责：所有客户端（Kelivo / Telegram / Open WebUI …）都打给我，
#       我先去 Ombre Brain 唤起相关记忆，塞给 Claude，再转接。
#       Claude 觉得值得记住的事，会通过 hold 工具存回记忆仓库。
# 对外：OpenAI 兼容接口（/v1/chat/completions）
# 对内：ofox 中转站的 Claude + Ombre Brain（MCP 协议）
# ============================================================
import asyncio
import json
import os
import time
import uuid

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

# ===== 配置：全部从环境变量读取，代码里不放任何密钥 =====
UPSTREAM_BASE_URL = os.environ.get("UPSTREAM_BASE_URL", "https://api.ofox.ai/v1").rstrip("/")
UPSTREAM_API_KEY = os.environ.get("UPSTREAM_API_KEY", "")
DEFAULT_MODEL = os.environ.get("DEFAULT_MODEL", "anthropic/claude-sonnet-4.6")
HUB_API_KEY = os.environ.get("HUB_API_KEY", "")
# 记忆仓库（Ombre Brain）的 MCP 地址
OMBRE_MCP_URL = os.environ.get("OMBRE_MCP_URL", "https://melody-ombre.zeabur.app/mcp")

app = FastAPI(title="Melody Hub")


# ============================================================
# 第一部分：Ombre Brain 的"电话线"（MCP 客户端）
# ============================================================
class OmbreClient:
    """负责和记忆仓库通话：先握手拿到会话号，之后用会话号调用工具"""

    def __init__(self, url: str):
        self.url = url
        self.session_id = ""
        self._lock = asyncio.Lock()
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }

    @staticmethod
    def _parse(resp: httpx.Response):
        """回复可能是纯 JSON 或 SSE 两种包装，拆开取出内容"""
        if "text/event-stream" in resp.headers.get("content-type", ""):
            for line in resp.text.splitlines():
                if line.startswith("data:"):
                    return json.loads(line[5:].strip())
            return None
        if resp.text.strip():
            return resp.json()
        return None

    async def _handshake(self, client: httpx.AsyncClient):
        """MCP 握手：initialize -> 记下会话号 -> 发 initialized 通知"""
        r = await client.post(self.url, headers=self._headers, json={
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "melody-hub", "version": "1.0"},
            },
        })
        r.raise_for_status()
        self.session_id = r.headers.get("mcp-session-id", "")
        h = dict(self._headers)
        if self.session_id:
            h["mcp-session-id"] = self.session_id
        await client.post(self.url, headers=h, json={
            "jsonrpc": "2.0", "method": "notifications/initialized", "params": {},
        })

    async def call(self, name: str, arguments: dict) -> str:
        """调用记忆仓库的一个工具（breath / hold / …），返回文字结果。
        会话过期会自动重新握手一次；彻底失败则返回空字符串——
        记忆临时联系不上时，聊天本身不能受影响。
        """
        try:
            async with self._lock:
                async with httpx.AsyncClient(timeout=30) as client:
                    if not self.session_id:
                        await self._handshake(client)
                    for attempt in range(2):
                        h = dict(self._headers)
                        if self.session_id:
                            h["mcp-session-id"] = self.session_id
                        r = await client.post(self.url, headers=h, json={
                            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                            "params": {"name": name, "arguments": arguments},
                        })
                        if r.status_code in (400, 404) and attempt == 0:
                            # 会话号过期（比如仓库重启过），重新握手再试一次
                            await self._handshake(client)
                            continue
                        data = self._parse(r)
                        if not data:
                            return ""
                        content = data.get("result", {}).get("content", [])
                        return "\n".join(
                            c.get("text", "") for c in content if c.get("type") == "text"
                        )
        except Exception:
            return ""
        return ""


ombre = OmbreClient(OMBRE_MCP_URL)


# ============================================================
# 第二部分：记忆的注入与存储
# ============================================================

# 告诉 Claude 它拥有记忆（刻意写短，因为每轮对话都要重复发送，省 token）
MEMORY_GUIDE = (
    "你有长期记忆(Ombre Brain)。【浮现的记忆】是自动唤起的过往，自然运用不必复述。"
    "对话中出现重要事件、情绪时刻、约定、心愿、未解决的事时，主动用 hold 存下来，"
    "不必等用户要求；日常寒暄不存。"
)
# 记忆管理模式的补充说明（只在用户聊到记忆管理时出现）
MANAGE_GUIDE = (
    "本轮可用记忆管理工具：breath 按关键词主动检索更多记忆(自动浮现不够时用)；"
    "pulse 盘点所有记忆桶；trace 按 bucket_id 修改/标记解决/删除"
    "(id 从 pulse 或浮现记忆中获取)；grow 批量消化长文本自动拆成多条记忆(适合导入旧记忆)。"
    "删除等破坏性操作前先向用户确认。"
)

# 工具说明书（OpenAI tools 格式，转发给 Claude；描述从简以省 token）
HOLD_TOOL = {
    "type": "function",
    "function": {
        "name": "hold",
        "description": "存一条长期记忆。重要事件/情绪/未解决/用户要求记住的内容才存。",
        "parameters": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "要记住的内容，完整句子"},
                "tags": {"type": "string", "description": "逗号分隔标签，可空"},
                "importance": {"type": "integer", "description": "1-10，默认5"},
                "pinned": {"type": "boolean", "description": "永久钉选，几乎不用"},
            },
            "required": ["content"],
        },
    },
}
PULSE_TOOL = {
    "type": "function",
    "function": {
        "name": "pulse",
        "description": "盘点记忆系统：状态+所有记忆桶列表(含bucket_id)。",
        "parameters": {
            "type": "object",
            "properties": {
                "include_archive": {"type": "boolean", "description": "是否含已归档，默认false"},
            },
        },
    },
}
TRACE_TOOL = {
    "type": "function",
    "function": {
        "name": "trace",
        "description": "修改或删除一条记忆。只传要改的字段。",
        "parameters": {
            "type": "object",
            "properties": {
                "bucket_id": {"type": "string", "description": "记忆桶id，来自pulse或浮现记忆"},
                "name": {"type": "string", "description": "改名"},
                "tags": {"type": "string", "description": "改标签"},
                "importance": {"type": "integer", "description": "改重要性1-10"},
                "resolved": {"type": "integer", "description": "1=标记已解决沉底,0=重新激活"},
                "pinned": {"type": "integer", "description": "1=钉选,0=取消钉选"},
                "delete": {"type": "boolean", "description": "true=删除此记忆"},
            },
            "required": ["bucket_id"],
        },
    },
}
GROW_TOOL = {
    "type": "function",
    "function": {
        "name": "grow",
        "description": "批量消化一段长文本(日记/旧记忆导入)，自动拆分成多条记忆。",
        "parameters": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "要消化的完整文本"},
            },
            "required": ["content"],
        },
    },
}
BREATH_TOOL = {
    "type": "function",
    "function": {
        "name": "breath",
        "description": "按关键词主动检索长期记忆，自动浮现的内容不够时使用。",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "检索关键词"},
                "max_results": {"type": "integer", "description": "最多返回条数，默认3"},
            },
            "required": ["query"],
        },
    },
}

# 用户的话里出现这些词，才给 Claude 装备管理工具（平时不带，省 token）
MANAGE_KEYWORDS = (
    "记忆", "记得", "记不记得", "想起", "回忆", "盘点", "忘掉", "忘记", "删",
    "改一下", "更新一下", "修改", "导入", "整理", "钉选", "已解决", "日记",
)


def last_user_text(messages: list) -> str:
    """从对话记录里找出用户最后说的那句话（兼容纯文字和图文混合两种格式）"""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                return " ".join(
                    p.get("text", "") for p in content if p.get("type") == "text"
                )
    return ""


def is_new_conversation(messages: list) -> bool:
    """对话里还没有任何 AI 回复 = 新会话刚开始"""
    return not any(m.get("role") == "assistant" for m in messages)


async def recall_memories(messages: list) -> str:
    """回忆环节：按用户的话检索相关记忆；新会话另加主动浮现。

    检索结果永远排在前面——它和当前这句话最相关，绝不能被截断；
    主动浮现（钉选/未解决的高权重记忆）排后面，超长就牺牲它的尾巴。
    """
    query = last_user_text(messages)[:200]
    new_conv = is_new_conversation(messages)
    q_task = ombre.call("breath", {"query": query, "max_results": 3}) if query else None
    # 新对话专门取一次近况快照，让它能接上"上个窗口聊到哪了"
    snap_task = ombre.call("breath", {"query": "近况快照", "max_results": 1}) if new_conv else None
    surf_task = ombre.call("breath", {}) if new_conv else None
    tasks = [t for t in (q_task, snap_task, surf_task) if t]
    if not tasks:
        return ""
    results = list(await asyncio.gather(*tasks))
    q_text = results.pop(0) if q_task else ""
    snap_text = results.pop(0) if snap_task else ""
    surf_text = results.pop(0) if surf_task else ""

    parts = []
    if q_text and "没有" not in q_text[:20]:
        parts.append(q_text[:1500])  # 检索命中：优先、给足空间
    if snap_text and "近况快照" in snap_text[:60] and snap_text not in q_text:
        parts.append(snap_text[:800])  # 近况快照：新对话续弦用
    if surf_text and "没有" not in surf_text[:20] and surf_text != q_text:
        parts.append(surf_text[:1400])  # 主动浮现：排后，超长砍尾
    return "\n\n".join(parts)


def inject_memory(body: dict, memories: str, manage: bool):
    """把使用说明和浮现的记忆，作为 system 消息垫在对话最前面"""
    text = MEMORY_GUIDE
    if manage:
        text += "\n" + MANAGE_GUIDE
    if memories:
        text += "\n\n【浮现的记忆】\n" + memories
    body["messages"] = [{"role": "system", "content": text}] + body["messages"]


def wants_manage(messages: list) -> bool:
    """用户这句话是否聊到了记忆管理？是的话才装备管理工具"""
    text = last_user_text(messages)
    return any(k in text for k in MANAGE_KEYWORDS)


def add_memory_tools(body: dict, manage: bool):
    """把记忆工具挂到请求上（客户端自带工具时合并，不覆盖）。
    平时只挂 hold；聊到记忆管理时才挂全套，日常对话省 token。
    """
    extra = [HOLD_TOOL] + ([BREATH_TOOL, PULSE_TOOL, TRACE_TOOL, GROW_TOOL] if manage else [])
    tools = body.get("tools") or []
    existing = {t.get("function", {}).get("name") for t in tools}
    tools = tools + [t for t in extra if t["function"]["name"] not in existing]
    body["tools"] = tools


# 允许 Claude 调用的记忆工具清单
ALLOWED_TOOLS = {"hold", "breath", "pulse", "trace", "grow"}
# 这几个工具是"写操作"，执行成功后要给用户看回执
RECEIPT_ICONS = {"hold": "📝", "grow": "📝", "trace": "🔧"}


def sanitize_tool_calls(tool_calls: list):
    """无参数的工具调用 arguments 会是空字符串，转发回中转站前补成 {}，否则它不认"""
    for tc in tool_calls:
        fn = tc.get("function", {})
        if not fn.get("arguments"):
            fn["arguments"] = "{}"


async def execute_hold_calls(tool_calls: list):
    """执行 Claude 发起的记忆工具调用。
    返回 (工具结果消息列表, 回执列表)——回执给用户看，证明记忆动作真的发生了。
    """
    results, receipts = [], []
    for tc in tool_calls:
        name = tc.get("function", {}).get("name", "")
        try:
            args = json.loads(tc.get("function", {}).get("arguments") or "{}")
        except json.JSONDecodeError:
            args = {}
        if name in ALLOWED_TOOLS:
            out = await ombre.call(name, args) or "记忆仓库暂时联系不上"
        else:
            out = "未知工具"
        if name in RECEIPT_ICONS and "联系不上" not in out:
            receipts.append(f"{RECEIPT_ICONS[name]} {out.splitlines()[0][:60]}")
        results.append({
            "role": "tool",
            "tool_call_id": tc.get("id", ""),
            "content": out,
        })
    return results, receipts


# ===== 近况快照：每轮聊天后，悄悄记下"最近聊到哪了" =====
SNAPSHOT_TOKEN = os.environ.get("OMBRE_EXPORT_TOKEN", "")
SNAPSHOT_URL = OMBRE_MCP_URL.rsplit("/mcp", 1)[0] + "/snapshot"


def _msg_text(m: dict) -> str:
    """取出一条消息的纯文字（兼容图文混合格式）"""
    c = m.get("content") or ""
    if isinstance(c, list):
        c = " ".join(p.get("text", "") for p in c if p.get("type") == "text")
    return c


async def update_snapshot(orig_messages: list, reply: str):
    """把最近几句对话写进仓库的固定快照桶（后台静默执行，失败不影响聊天）"""
    if not SNAPSHOT_TOKEN:
        return
    try:
        lines = []
        for m in orig_messages[-4:]:
            if m.get("role") == "system":
                continue
            who = "用户" if m.get("role") == "user" else "我"
            t = _msg_text(m).strip()
            if t:
                lines.append(f"{who}：{t[:200]}")
        if reply.strip():
            lines.append(f"我：{reply.strip()[:300]}")
        if not lines:
            return
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(
                SNAPSHOT_URL,
                params={"token": SNAPSHOT_TOKEN},
                json={"content": "\n".join(lines)},
            )
    except Exception:
        pass  # 快照失败无声跳过，绝不拖累聊天


# ============================================================
# 第三部分：对外接口
# ============================================================
def check_auth(request: Request):
    """查验客户端出示的总机密码"""
    if not HUB_API_KEY:
        return
    auth = request.headers.get("authorization", "")
    if auth != f"Bearer {HUB_API_KEY}":
        raise HTTPException(status_code=401, detail="API key 不对或没带")


@app.get("/health")
async def health():
    return {"status": "ok", "role": "melody-hub", "memory": OMBRE_MCP_URL, "model": DEFAULT_MODEL}


@app.get("/v1/models")
async def list_models(request: Request):
    """模型清单：直接去上游拿最新的，新模型自动出现"""
    check_auth(request)
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                f"{UPSTREAM_BASE_URL}/models",
                headers={"Authorization": f"Bearer {UPSTREAM_API_KEY}"},
            )
        if resp.status_code == 200:
            return JSONResponse(content=resp.json())
    except Exception:
        pass
    return {
        "object": "list",
        "data": [{"id": DEFAULT_MODEL, "object": "model", "owned_by": "melody-hub"}],
    }


UPSTREAM_HEADERS = None  # 运行时组装，见下


def upstream_headers():
    return {
        "Authorization": f"Bearer {UPSTREAM_API_KEY}",
        "Content-Type": "application/json",
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """聊天主入口：回忆 -> 转接 Claude -> （Claude 想记东西就替它存）"""
    check_auth(request)
    body = await request.json()
    body.setdefault("model", DEFAULT_MODEL)

    # 1) 回忆环节：唤起相关记忆并垫进对话
    messages = body.get("messages", [])
    orig_messages = list(messages)  # 留一份没被加工过的原始对话，写近况快照用
    manage = wants_manage(messages)  # 用户聊到记忆管理了吗
    memories = await recall_memories(messages)
    inject_memory(body, memories, manage)
    # 2) 给 Claude 挂上记忆工具（平时轻装，管理时全套）
    add_memory_tools(body, manage)

    url = f"{UPSTREAM_BASE_URL}/chat/completions"
    if body.get("stream"):
        return StreamingResponse(
            stream_with_tools(url, body, orig_messages), media_type="text/event-stream"
        )
    return await complete_with_tools(url, body, orig_messages)


async def complete_with_tools(url: str, body: dict, orig_messages: list):
    """非流式：最多允许 Claude 连续记 3 轮，然后把最终回答传回"""
    receipts = []
    async with httpx.AsyncClient(timeout=300) as client:
        for _ in range(3):
            resp = await client.post(url, json=body, headers=upstream_headers())
            if resp.status_code != 200:
                return JSONResponse(status_code=resp.status_code, content=resp.json())
            data = resp.json()
            choice = (data.get("choices") or [{}])[0]
            msg = choice.get("message", {})
            tool_calls = msg.get("tool_calls") or []
            if not tool_calls:
                # 说完了：附上记忆回执，后台更新近况快照，然后交卷
                reply = msg.get("content") or ""
                if receipts:
                    msg["content"] = reply + "\n\n" + "\n".join(receipts)
                asyncio.create_task(update_snapshot(orig_messages, reply))
                return JSONResponse(content=data)
            # Claude 想操作记忆：执行工具，把结果接回对话，让它继续说完
            # （content 为 null 时改成空字符串，部分中转站不接受 null）
            msg["content"] = msg.get("content") or ""
            sanitize_tool_calls(tool_calls)
            tool_msgs, new_receipts = await execute_hold_calls(tool_calls)
            receipts += new_receipts
            body["messages"] = body["messages"] + [msg] + tool_msgs
        return JSONResponse(content=data)


async def stream_with_tools(url: str, body: dict, orig_messages: list):
    """流式：一边把 Claude 说的字转给客户端，一边悄悄拦下它的记忆动作。

    Claude 中途操作记忆时转播会分成多段，段与段的消息编号不同——
    严格的客户端（如 Kelivo）只认第一段的编号，会把后面的丢掉导致空回复。
    所以这里把所有转发内容统一改写成同一个编号，整场转播看起来就是一条消息。
    """
    stream_id = f"chatcmpl-{uuid.uuid4().hex[:24]}"
    created = int(time.time())
    model = body.get("model", DEFAULT_MODEL)
    receipts = []    # 本次回复中真实发生的记忆动作凭证
    all_content = [] # 整场回复的全文（写近况快照用）

    def wrap(delta: dict, finish_reason=None) -> bytes:
        """把内容装进统一编号的标准信封再发出"""
        out = {
            "id": stream_id, "object": "chat.completion.chunk",
            "created": created, "model": model,
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
        }
        return f"data: {json.dumps(out, ensure_ascii=False)}\n\n".encode()

    # 开场先发一次"角色声明"（整场只发这一次）
    yield wrap({"role": "assistant"})

    async with httpx.AsyncClient(timeout=300) as client:
        for _round in range(3):
            tool_calls = {}     # 按序号累积工具调用的碎片
            finish_by_tools = False
            content_parts = []  # Claude 在调工具前说的话（要记入对话历史）

            async with client.stream("POST", url, json=body, headers=upstream_headers()) as resp:
                if resp.status_code != 200:
                    # 出错时把错误说成"人话"发给客户端，至少不再是空气泡
                    err = (await resp.aread()).decode(errors="replace")[:300]
                    yield wrap({"content": f"（总机转接出错 {resp.status_code}：{err}）"}, "stop")
                    yield b"data: [DONE]\n\n"
                    return
                async for line in resp.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    payload = line[5:].strip()
                    if payload == "[DONE]":
                        break
                    try:
                        chunk = json.loads(payload)
                    except json.JSONDecodeError:
                        continue
                    choice = (chunk.get("choices") or [{}])[0]
                    delta = choice.get("delta", {})

                    # 累积工具调用碎片（这些不转发给客户端）
                    for tc in delta.get("tool_calls") or []:
                        idx = tc.get("index", 0)
                        slot = tool_calls.setdefault(idx, {
                            "id": "", "type": "function",
                            "function": {"name": "", "arguments": ""},
                        })
                        if tc.get("id"):
                            slot["id"] = tc["id"]
                        fn = tc.get("function", {})
                        if fn.get("name"):
                            slot["function"]["name"] = fn["name"]
                        if fn.get("arguments"):
                            slot["function"]["arguments"] += fn["arguments"]

                    if choice.get("finish_reason") == "tool_calls":
                        finish_by_tools = True
                        break

                    # 有内容才转发；角色声明等杂项一律不重复发
                    if delta.get("content"):
                        content_parts.append(delta["content"])
                        all_content.append(delta["content"])
                        yield wrap({"content": delta["content"]})
                    elif choice.get("finish_reason"):
                        # 正式收尾前，把记忆回执附在末尾给用户看
                        if receipts:
                            yield wrap({"content": "\n\n" + "\n".join(receipts)})
                        yield wrap({}, choice["finish_reason"])

            if not finish_by_tools:
                asyncio.create_task(update_snapshot(orig_messages, "".join(all_content)))
                yield b"data: [DONE]\n\n"
                return

            # Claude 想操作记忆：执行工具，把结果接回对话，再开下一轮转播
            calls = list(tool_calls.values())
            sanitize_tool_calls(calls)
            assistant_msg = {
                "role": "assistant",
                "content": "".join(content_parts),  # 空则留空字符串，中转站不认 null
                "tool_calls": calls,
            }
            tool_msgs, new_receipts = await execute_hold_calls(calls)
            receipts += new_receipts
            body["messages"] = body["messages"] + [assistant_msg] + tool_msgs

        # 3 轮还没说完（极少见），礼貌收尾
        asyncio.create_task(update_snapshot(orig_messages, "".join(all_content)))
        yield wrap({}, "stop")
        yield b"data: [DONE]\n\n"
