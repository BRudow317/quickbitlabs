from __future__ import annotations

import os, re, sys, logging
from pathlib import Path


logger = logging.getLogger(__name__)

_BRACED = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PLAIN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
_POS = re.compile(r"\$([0-9]+)")

def _interpolate_once(s, lookup_var, positional_lookup):
        def repl_pos(m):
            n = m.group(1); val = positional_lookup(n); return val if val is not None else m.group(0)
        def repl_braced(m):
            name = m.group(1); val = lookup_var(name); return val if val is not None else m.group(0)
        def repl_plain(m):
            name = m.group(1); val = lookup_var(name); return val if val is not None else m.group(0)
        s = _POS.sub(repl_pos, s)
        s = _BRACED.sub(repl_braced, s)
        s = _PLAIN.sub(repl_plain, s)
        return s

def parse_config_file(config_path:str|Path="", env:str|Path="", allow_os_env=True, max_depth=2, strict_mode:bool=True)->dict[str, str]:
    raw = {}
    #TODO: consider searching for an env file if the file name is included but not the path, or if the path is included but not the file name.
    if config_path=="": return raw
    if not config_path == "" and not Path(config_path).is_file(): raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as f:
        for _, line in enumerate(f, 0):
            line = line.strip()
            if not line or line.startswith('#') or line.startswith('!') or '=' not in line:
                continue
            key, _, val = line.partition('=')
            raw[key.strip()] = val.strip().strip('"').strip("'")
    special_scope = {'env': env, 'ENV': env}
    positional = {'1': env} if env is not None else {}
    os_env = os.environ if allow_os_env else {}
    resolved_cache = {}
    resolving_stack = set()
    def lookup_var(name):
        if name in resolved_cache: return resolved_cache[name]
        if name in raw: return resolve_key(name)
        if name in special_scope: return special_scope[name]
        if allow_os_env and name in os_env: return os_env[name]
        return None
    def positional_lookup(n): return positional.get(n)
    def resolve_key(key):
        if key in resolved_cache: return resolved_cache[key]
        if key in resolving_stack:
            msg = f"Cycle detected while resolving '{key}'"
            if strict_mode: 
                logger.error(msg)
            logger.debug(msg); return raw[key]
        resolving_stack.add(key)
        try:
            val = raw[key]
            depth = 0
            while depth < max_depth:
                depth += 1
                before = val
                val = _interpolate_once(val, lookup_var, positional_lookup)
                if val in raw and val != key:
                    val = resolve_key(val)
                if val == before:
                    break
            else:
                msg = f"Max depth exceeded for '{key}'"
                if strict_mode: logger.error(msg)
                logger.debug(msg)
            resolved_cache[key] = val
            return val
        finally:
            resolving_stack.remove(key)
    return_val = {k: resolve_key(k) for k in raw.keys()}
    logger.debug(f"Config keys resolved: {list(return_val.keys())}")
    return return_val