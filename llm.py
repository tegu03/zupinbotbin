"""DeepSeek (OpenAI-compatible) calls. Fails CLOSED: error apa pun -> objek aman
yang memaksa NO-TRADE di governor."""
import json
import re
import httpx
from config import CONFIG
from prompts import MSE_SYSTEM, PTE_SYSTEM


def _extract_json(s):
    if not isinstance(s, str):
        return s
    t = re.sub(r"^```(?:json)?", "", s.strip())
    t = re.sub(r"```$", "", t.strip()).strip()
    a, b = t.find("{"), t.rfind("}")
    if a != -1 and b != -1:
        t = t[a:b + 1]
    return json.loads(t)


async def _chat(system, user):
    body = {
        "model": CONFIG.model,
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
        "stream": False,
    }
    if CONFIG.thinking:
        body["thinking"] = {"type": "enabled"}
        body["reasoning_effort"] = "high"
    headers = {"Authorization": f"Bearer {CONFIG.deepseek_api_key}", "Content-Type": "application/json"}
    async with httpx.AsyncClient() as c:
        r = await c.post(f"{CONFIG.deepseek_base_url}/chat/completions", json=body, headers=headers, timeout=180)
        r.raise_for_status()
        data = r.json()
    return data["choices"][0]["message"]["content"]


async def classify_regime(snapshot):
    try:
        return _extract_json(await _chat(MSE_SYSTEM, json.dumps(snapshot)))
    except Exception as e:
        return {"regime": "chop", "confidence_pct": 0, "pte_layer1_input": "chop", "parse_error": str(e)}


async def analyze_trade(snapshot, mse):
    try:
        payload = json.dumps({"snapshot": snapshot, "mse_regime": mse})
        return _extract_json(await _chat(PTE_SYSTEM, payload))
    except Exception as e:
        return {"signal": "no_trade", "confidence_pct": 0, "abstain_reason": f"PTE error: {e}"}
