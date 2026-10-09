"""
Frontier models on Databricks (Foundation Model APIs) for the QGIS plugin.

Pure Python (no QGIS imports) so it can be tested on its own. It lists the
workspace's image-capable chat models, ranks a sensible default, builds the
"explain this map" request, and streams the answer from the model's serving
endpoint using the plugin's existing Databricks sign-in: no API keys and no
extra packages.

    POST https://<workspace>/serving-endpoints/<model>/invocations   (OpenAI-style chat)
"""
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request

# ---------------------------------------------------------------------------
# Prompt presets
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You are an expert GIS analyst. You are given a screenshot of a QGIS map and "
    "structured facts about it (area, scale, layers and what each layer's style encodes). "
    "Base every statement on what is visible in the image and on the facts; use the layer "
    "and field names from the facts. If something can't be determined from the map, say so "
    "briefly instead of guessing. Answer in concise Markdown."
)

PRESETS = [
    ("explain", "Explain this map",
     "Explain this map for a decision-maker. Cover: what area and data it shows; the main "
     "spatial patterns (clusters, gradients, hotspots, gaps); notable outliers; and three "
     "concise insights or questions worth investigating. Use short Markdown sections."),
    ("report", "Summary for a report",
     "Write one polished paragraph of 120-180 words summarising this map for a report: what "
     "it shows, the key pattern, and why it matters. Plain prose, no headings or bullet points."),
    ("patterns", "Patterns and outliers",
     "Identify the spatial patterns, clusters and outliers in this map, ranked by how strong "
     "they look. For each, say where it is and what in the data supports it. Finish with "
     "caveats about how the map is classified or styled."),
    ("caption", "Title, caption and alt text",
     "Write (1) a short map title, (2) a one-sentence caption, and (3) alt text for "
     "accessibility of at most 125 characters. Use those three labelled lines only."),
    ("next_steps", "Next analysis steps",
     "Suggest three to five next analysis steps that would deepen understanding of this map, "
     "in QGIS or in Databricks SQL (for example spatial functions such as ST_INTERSECTS, "
     "ST_BUFFER or H3). Say briefly why each step helps."),
]
PRESET_IDS = [p[0] for p in PRESETS]


def preset_prompt(preset_id):
    for pid, _label, prompt in PRESETS:
        if pid == preset_id:
            return prompt
    return PRESETS[0][2]


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

# Family preference for the default model: strong vision + fast first.
_FAMILY_ORDER = ["claude-sonnet", "gpt-5", "gpt-6", "gemini", "claude-opus", "claude-haiku",
                 "llama-4", "grok", "qwen", "kimi"]


def _version_key(name):
    """Version numbers padded to a fixed length, so sonnet-5-5 (5, 5, 0) ranks above
    sonnet-5 (5, 0, 0) above sonnet-4-6 (4, 6, 0)."""
    nums = [int(n) for n in re.findall(r"\d+", name)][:3]
    return tuple(nums + [0] * (3 - len(nums)))


def rank_models(models):
    """Sort model dicts ({name, display_name}) best default first."""
    def key(m):
        name = m["name"].lower()
        fam = next((i for i, f in enumerate(_FAMILY_ORDER) if f in name), len(_FAMILY_ORDER))
        variant = 1 if any(v in name for v in ("-mini", "-nano", "-lite", "-pro", "-luna", "-terra")) else 0
        return (fam, variant, tuple(-v for v in _version_key(name)), name)
    return sorted(models, key=key)


def vision_models(endpoints):
    """Image-capable chat models from a /api/2.0/serving-endpoints listing."""
    out = []
    for ep in endpoints:
        caps = ep.get("capabilities") or {}
        if ep.get("task") != "llm/v1/chat" or not caps.get("image_input"):
            continue
        if (ep.get("state") or {}).get("ready", "READY") not in ("READY", None):
            continue
        entities = (ep.get("config") or {}).get("served_entities") or [{}]
        display = (entities[0].get("foundation_model") or {}).get("display_name") or ep["name"]
        out.append({"name": ep["name"], "display_name": display})
    return rank_models(out)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

class ModelError(RuntimeError):
    """A model call failed with a message suitable for the user."""


def https_url(hostname, path):
    url = f"https://{(hostname or '').strip()}{path}"
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc or "@" in parsed.netloc:
        raise ModelError(f"Refusing non-https workspace URL: {url}")
    return url


def _friendly_error(code, body):
    text = body or ""
    try:
        text = json.loads(body).get("message", body)
    except (ValueError, AttributeError):
        pass
    if "Image input is not supported" in text:
        return "This model can't read images. Choose another model in Options."
    if code in (401, 403):
        return ("Databricks rejected the request (%s). Sign in again, or check you can use "
                "this model's serving endpoint." % code)
    if code == 404:
        return "That model isn't available on this workspace. Choose another model in Options."
    if code == 429 or "REQUEST_LIMIT_EXCEEDED" in text:
        return "The workspace is busy (rate limited). Wait a moment and try again."
    return f"The model request failed (HTTP {code}): {text[:300]}"


def list_models(hostname, token, timeout=60):
    """Image-capable chat models on the workspace, best default first."""
    req = urllib.request.Request(https_url(hostname, "/api/2.0/serving-endpoints"),
                                 headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310 - https only (https_url)
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise ModelError(_friendly_error(exc.code, exc.read().decode("utf-8", "replace")))
    return vision_models(data.get("endpoints") or [])


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

# Largest image (raw bytes) we send. The strictest provider limit is Claude's 5 MB
# on the base64 string, and base64 adds a third, so this keeps a safe margin.
MAX_IMAGE_BYTES = 3_600_000


def image_mime(image_b64):
    """MIME type of a base64 image: JPEG data starts with "/9j/", otherwise PNG."""
    return "image/jpeg" if image_b64.startswith("/9j/") else "image/png"


def build_messages(prompt, facts_text, image_b64, history=None, follow_up=None):
    """OpenAI-style messages: system, then the map (image + facts + prompt);
    follow-ups continue the same conversation."""
    if history:
        messages = list(history)
        if follow_up:
            messages.append({"role": "user", "content": follow_up})
        return messages
    content = [{"type": "text", "text": f"{prompt}\n\nMap facts:\n{facts_text}"}]
    if image_b64:
        content.append({"type": "image_url",
                        "image_url": {"url": f"data:{image_mime(image_b64)};base64,{image_b64}"}})
    return [{"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content}]


def _text_of(content):
    """Message/delta content as text: a string, or a list of parts (Gemini)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type", "text") == "text")
    return str(content)


def _merge_usage(old, new):
    if not new:
        return old
    clean = {k: v for k, v in new.items() if isinstance(v, (int, float))}
    return {**(old or {}), **clean} if clean else old


def stream_chat(hostname, token, model, messages, max_tokens=1200, on_delta=None,
                should_stop=None, timeout=180, retries=3):
    """Stream a chat completion; calls on_delta(text) per chunk.
    Returns (full_text, usage dict or None)."""
    url = https_url(hostname, f"/serving-endpoints/{urllib.parse.quote(model)}/invocations")
    body = {"messages": messages, "max_tokens": int(max_tokens), "stream": True,
            "stream_options": {"include_usage": True}}
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, json.dumps(body).encode("utf-8"),
                                     {"Authorization": f"Bearer {token}",
                                      "Content-Type": "application/json",
                                      "Accept": "text/event-stream"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310 - https only (https_url)
                return _read_stream(resp, on_delta, should_stop)
        except urllib.error.HTTPError as exc:
            err = exc.read().decode("utf-8", "replace")
            if exc.code == 400 and "stream_options" in err and "stream_options" in body:
                body.pop("stream_options")        # some models (e.g. Gemini) don't accept it
                continue
            busy = exc.code == 429 or "REQUEST_LIMIT_EXCEEDED" in err
            if busy and attempt < retries:
                time.sleep(3 * (attempt + 1))
                continue
            raise ModelError(_friendly_error(exc.code, err))
        except urllib.error.URLError as exc:
            raise ModelError(f"Couldn't reach the workspace: {exc.reason}")
    raise ModelError("The model request failed after several attempts.")


def _read_stream(resp, on_delta, should_stop):
    text, usage = [], None
    for raw in resp:
        if should_stop and should_stop():
            break
        line = raw.decode("utf-8", "replace").strip()
        if not line.startswith("data:"):
            continue
        payload = line[5:].strip()
        if payload == "[DONE]":
            break
        try:
            chunk = json.loads(payload)
        except ValueError:
            continue
        usage = _merge_usage(usage, chunk.get("usage"))
        for choice in chunk.get("choices") or []:
            piece = _text_of((choice.get("delta") or {}).get("content")) \
                or _text_of((choice.get("message") or {}).get("content"))
            if piece:
                text.append(piece)
                if on_delta:
                    on_delta(piece)
    return "".join(text), usage
