#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv_translator_v2_resume_cache.py
================================

Objectifs (V2):
- PLUS RAPIDE: déduplication globale par "pool de textes uniques", batch adaptatif, moins d'I/O.
- MOINS CHER: cache persistant SQLite (réutilisation inter-runs), prompt compact, moins de retries.
- PLUS STABLE: reprise robuste (progress + cache + sorties atomiques), parsing JSON plus robuste, QA "restes allemands".

Garanties métier conservées:
1) Lire CSV (sep auto, encoding)
2) Colonnes protégées jamais traduites (+never-translate)
3) Traduction multi-langues
4) Dédup (mieux: dédup globale sur tous les textes)
5) Checkpoint/reprise
6) Remplir Richtig_Text*
7) Vérification + retry ciblé
8) Sortie CSV par langue + protokoll CSV

Usage:
python csv_translator_v2_resume_cache.py \
  --pdf EH_260216.csv \
  --prompt "Megaprompt.docx" \
  --encoding utf-8-sig \
  --langage AR TR RU UK PL EN RO \
  --model gpt-5-mini \
  --batch-size 200 \
  --cache-db translations.sqlite \
  --checkpoint-every 5

Notes:
- Cache SQLite clé: SHA256(lang|model|rules_hash|text_normalized)
- La reprise fonctionne même si l'arrêt survient:
  - pendant la construction du pool
  - pendant la traduction (chunks)
  - pendant l'application au dataframe
  - pendant les retries QA
"""

import os
import re
import json
import time
import argparse
import hashlib
import sqlite3
from datetime import datetime
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd
import ftfy
from docx import Document
from openai import OpenAI


# -------------------------
# Config & Defaults
# -------------------------
DEFAULT_LANGS = ["AR", "TR", "RU", "ES", "PL", "EN", "RO"]
DEFAULT_MODEL = "gpt-5-mini"

# Colonnes "protégées" par défaut (jamais traduites)
DEFAULT_PROTECT_COLS = (
    "lfdNr,FrageNr,BerufNr,Beruf,LFNr,AbschnNr,Nr,Richtig1,Richtig2,Schwierigkeit,Sprache,"
    "Richtig_Text1,Richtig1_Text,Richtig_Text2,Abschlussprüfung Teil 1,Abschlussprüfung Teil 2,"
    "Lehrjahr,Zwischenprüfung,Abschlussprüfung"
)

# Protokoll (identique à ton programme)
PROTOKOLL_COLUMNS = [
    "Datum", "Datei", "Sprache", "Modell", "Temperatur", "Batchgroesse",
    "Parallelisierung", "Anzahl_API_Calls", "Gesamt_Input_Tokens",
    "Gesamt_Output_Tokens", "Laufzeit", "Anzahl_Retries",
    "Gesamtzeilen", "Deutsch_Reste_automatisch", "Verdachtsfaelle_gesamt",
    "Manuell_korrigiert", "Neu_uebersetzt", "Endgueltige_Deutsch_Reste",
    "Technische_Auffaelligkeiten", "Anpassungen_am_Prompt",
    "Anpassungen_an_Pipeline", "Empfehlung_naechster_Lauf",
]


# -------------------------
# Utils: I/O atomique & progress
# -------------------------
def atomic_write_text(path: str, text: str, encoding: str = "utf-8") -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding=encoding) as f:
        f.write(text)
    os.replace(tmp, path)


def atomic_write_json(path: str, obj: Any) -> None:
    atomic_write_text(path, json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def atomic_write_csv(df: pd.DataFrame, path: str, sep: str, encoding: str) -> None:
    tmp = path + ".tmp"
    df.to_csv(tmp, sep=sep, index=False, encoding=encoding)
    os.replace(tmp, path)


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# -------------------------
# API key loader
# -------------------------
def _load_api_key() -> str:
    key = os.environ.get("OPENAI_API_KEY", "")
    if key:
        return key
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("OPENAI_API_KEY="):
                    return line.split("=", 1)[1].strip()
    return ""


# -------------------------
# DOCX prompt rules + hash
# -------------------------
def read_docx_text(path: str) -> str:
    doc = Document(path)
    parts = []
    for p in doc.paragraphs:
        t = (p.text or "").strip()
        if t:
            parts.append(t)
    text = "\n".join(parts).strip()
    if not text:
        raise ValueError(f"Prompt-DOCX est vide ou illisible: {path}")
    return text


def rules_hash(rules_text: str) -> str:
    return hashlib.sha256(rules_text.encode("utf-8")).hexdigest()[:16]


# -------------------------
# CSV sep guess
# -------------------------
def guess_sep(path: str, user_sep: Optional[str]) -> str:
    if user_sep:
        return user_sep
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    return ";" if head.count(";") >= head.count(",") else ","


# -------------------------
# JSON extraction (robuste)
# -------------------------
def safe_json_extract(text: str) -> str:
    t = (text or "").strip()
    # enlever fences
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
        t = t.strip()

    # si ce n'est pas directement du JSON, tenter d'extraire le plus gros bloc [...] ou {...}
    if not t:
        return t

    # priorité à un array
    m = re.search(r"\[[\s\S]*\]", t)
    if m:
        return m.group(0).strip()

    m2 = re.search(r"\{[\s\S]*\}", t)
    if m2:
        return m2.group(0).strip()

    return t.strip()


# -------------------------
# Détection colonnes texte
# -------------------------
def detect_text_columns(df: pd.DataFrame, never_translate: set[str]) -> List[str]:
    cols = []
    for c in df.columns:
        if c in never_translate:
            continue
        if pd.api.types.is_string_dtype(df[c]):
            cols.append(c)
    return cols


def parse_column_order(s: Optional[str]) -> Optional[List[str]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(";") if p.strip()]
    if len(parts) <= 1 and "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts or None


def reorder_df_columns(df: pd.DataFrame, ordered_cols: List[str]) -> pd.DataFrame:
    existing_ordered = [c for c in ordered_cols if c in df.columns]
    return df[existing_ordered]


# -------------------------
# Filtres "non traduisibles" (amélioré)
# -------------------------
def _is_false_positive(val: str) -> bool:
    v = (val or "").strip()
    if not v:
        return True
    if len(v) <= 2:  # A/B/C/D/E etc.
        return True
    if re.fullmatch(r"[\d\s.,/\-+%°]+", v):
        return True
    if re.fullmatch(r"[\d\s.,]+\s*[€$£¥₹%°²³]+", v):
        return True
    if len(v) <= 5:
        # petite abréviation / marque
        return True
    if re.fullmatch(r"[A-Za-z0-9_\-./]+", v) and " " not in v:
        return True
    if re.match(r"https?://|www\.|[^@\s]+@[^@\s]+\.", v):
        return True
    if re.fullmatch(r"[\W_]+", v):
        return True
    if re.fullmatch(r"[A-Z0-9\s./\-&]+", v) and len(v) <= 12:
        return True
    return False


def normalize_text(s: str) -> str:
    # normalisation stable pour dédup + cache
    s = ftfy.fix_text(s or "")
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


# -------------------------
# Heuristique "reste allemand" (réduit les faux retries)
# -------------------------
GERMAN_MARKERS = [
    r"\bder\b", r"\bdie\b", r"\bdas\b", r"\bund\b", r"\bnicht\b", r"\bmit\b",
    r"\bein\b", r"\beine\b", r"\bwerden\b", r"\bist\b", r"\bsind\b", r"\bbei\b",
    r"\bauf\b", r"\baus\b", r"\bwie\b", r"\bwelche\b", r"\bwelcher\b", r"\bwelches\b",
]
GERMAN_CHAR = re.compile(r"[äöüÄÖÜß]")
GERMAN_BIGRAMS = ["sch", "ung", "keit", "lich", "über", "für", "zum", "zur"]


def german_score(text: str) -> float:
    t = (text or "").lower()
    if not t:
        return 0.0
    score = 0.0
    if GERMAN_CHAR.search(text or ""):
        score += 2.0
    for pat in GERMAN_MARKERS:
        if re.search(pat, t):
            score += 1.0
    for bg in GERMAN_BIGRAMS:
        if bg in t:
            score += 0.5
    # normaliser légèrement par longueur
    return score / max(1.0, min(20.0, len(t) / 20.0))


def likely_target_script(lang: str, text: str) -> bool:
    # aide à éviter retries inutiles pour RU/AR etc.
    t = text or ""
    if not t.strip():
        return True
    if lang.upper() in {"AR"}:
        return bool(re.search(r"[\u0600-\u06FF]", t))
    if lang.upper() in {"RU", "UK"}:
        return bool(re.search(r"[\u0400-\u04FF]", t))
    # latin languages: accept
    return True


# -------------------------
# SQLite cache
# -------------------------
def cache_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS translations (
            k TEXT PRIMARY KEY,
            lang TEXT NOT NULL,
            model TEXT NOT NULL,
            rules_hash TEXT NOT NULL,
            text_norm TEXT NOT NULL,
            translation TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lang_model_rules ON translations(lang, model, rules_hash)")
    conn.commit()
    return conn


def cache_key(lang: str, model: str, rules_h: str, text_norm: str) -> str:
    raw = f"{lang}|{model}|{rules_h}|{text_norm}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def cache_get_many(conn: sqlite3.Connection, keys: List[str]) -> Dict[str, str]:
    if not keys:
        return {}
    out: Dict[str, str] = {}
    # chunk to avoid sqlite limits
    CH = 500
    for i in range(0, len(keys), CH):
        chunk = keys[i:i+CH]
        q = "SELECT k, translation FROM translations WHERE k IN (%s)" % ",".join(["?"] * len(chunk))
        for k, tr in conn.execute(q, chunk).fetchall():
            out[k] = tr
    return out


def cache_put_many(conn: sqlite3.Connection, rows: List[Tuple[str, str, str, str, str, str]]) -> None:
    # rows: (k, lang, model, rules_hash, text_norm, translation)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany("""
        INSERT INTO translations(k, lang, model, rules_hash, text_norm, translation, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(k) DO UPDATE SET
          translation=excluded.translation,
          updated_at=excluded.updated_at
    """, [(k, lang, model, rules_h, tnorm, tr, now) for (k, lang, model, rules_h, tnorm, tr) in rows])
    conn.commit()


# -------------------------
# OpenAI call (client réutilisé) + backoff
# -------------------------
def create_chat_completion(client: OpenAI, model: str, messages: list, temperature: Optional[float] = None):
    kwargs = {"model": model, "messages": messages}
    if temperature is not None:
        try:
            kwargs["temperature"] = temperature
            return client.chat.completions.create(**kwargs)
        except Exception as e:
            msg = str(e)
            if "temperature" in msg and ("unsupported" in msg or "Only the default (1)" in msg):
                print(f"[INFO] temperature={temperature} non supportée par {model} — fallback sans temperature")
                kwargs.pop("temperature", None)
                return client.chat.completions.create(**kwargs)
            raise
    return client.chat.completions.create(**kwargs)


def translate_text_chunk(
    client: OpenAI,
    api_key: str,
    model: str,
    lang: str,
    rules_compact: str,
    items: List[Dict[str, Any]],
    temperature: Optional[float],
    max_retries: int = 10,
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    items: [{"id": <int>, "text": <str>}]
    returns: [{"id": <int>, "translation": <str>}]
    """
    # system prompt très compact pour réduire tokens
    instructions = (
        f"{rules_compact.strip()}\n\n"
        f"TARGET_LANGUAGE: {lang}\n"
        "Return ONLY valid JSON: an array of objects with keys: id, translation.\n"
        "Do not add any extra text."
    )

    payload = items

    for attempt in range(1, max_retries + 1):
        try:
            resp = create_chat_completion(
                client=client,
                model=model,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": instructions},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
            )
            usage = {"prompt_tokens": 0, "completion_tokens": 0, "api_calls": 1}
            if hasattr(resp, "usage") and resp.usage:
                usage["prompt_tokens"] = getattr(resp.usage, "prompt_tokens", 0) or 0
                usage["completion_tokens"] = getattr(resp.usage, "completion_tokens", 0) or 0

            out_text = safe_json_extract(resp.choices[0].message.content or "")
            data = json.loads(out_text)

            if not isinstance(data, list):
                raise ValueError("API: réponse n'est pas un JSON array")
            # sanity check
            if len(data) != len(payload):
                raise ValueError(f"API: taille mismatch: attendu {len(payload)}, reçu {len(data)}")

            return data, usage

        except Exception as e:
            if attempt == max_retries:
                raise
            wait = min(2 ** attempt, 60)
            print(f"[{lang}] API error (attempt {attempt}/{max_retries}): {type(e).__name__}: {e} — wait {wait}s")
            time.sleep(wait)

    raise RuntimeError("Unreachable")


# -------------------------
# Fix Richtig columns
# -------------------------
def fix_richtig_columns(df: pd.DataFrame) -> int:
    if "Richtig1" not in df.columns:
        return 0
    answer_letters = [c for c in ["A", "B", "C", "D", "E"] if c in df.columns]
    fixed = 0
    for idx in df.index:
        val = str(df.at[idx, "Richtig1"]).strip()
        if val.upper() in ["A", "B", "C", "D", "E", ""]:
            continue
        for letter in answer_letters:
            cell = str(df.at[idx, letter]).strip()
            if cell and cell == val:
                df.at[idx, "Richtig1"] = letter
                fixed += 1
                break
    if fixed:
        print(f"[fix_richtig_columns] {fixed} lignes corrigées (Richtig1 texte -> lettre).")
    return fixed


def fill_richtig_text_vectorized(df_out: pd.DataFrame) -> None:
    """
    Remplit Richtig_Text1 / Richtig1_Text / Richtig_Text2 sans apply(axis=1).
    """
    import numpy as np

    letters = ["A", "B", "C", "D", "E"]
    existing = [c for c in letters if c in df_out.columns]

    def pick_answer(richtig_col: str) -> pd.Series:
        if richtig_col not in df_out.columns or not existing:
            return pd.Series([""] * len(df_out), index=df_out.index)
        r = df_out[richtig_col].astype(str).str.strip().str.upper()
        choices = []
        conds = []
        for L in existing:
            conds.append(r == L)
            choices.append(df_out[L].astype(str))
        return pd.Series(np.select(conds, choices, default=""), index=df_out.index)

    if "Richtig1" in df_out.columns:
        picked = pick_answer("Richtig1")
        if "Richtig_Text1" in df_out.columns:
            df_out["Richtig_Text1"] = picked
        if "Richtig1_Text" in df_out.columns:
            df_out["Richtig1_Text"] = picked

    if "Richtig2" in df_out.columns and "Richtig_Text2" in df_out.columns:
        df_out["Richtig_Text2"] = pick_answer("Richtig2")


# -------------------------
# Pool build & apply translations
# -------------------------
def build_translation_pool(
    df: pd.DataFrame,
    text_cols: List[str],
) -> Tuple[List[str], Dict[str, str]]:
    """
    Collecte tous les textes traduisibles (non vides, non faux-positifs),
    normalise, et renvoie:
      - unique_texts: liste ordonnée des textes uniques normalisés
      - raw_to_norm: mapping raw_original -> normalized (pour appliquer stablement)
    """
    unique_set = set()
    unique_texts: List[str] = []
    raw_to_norm: Dict[str, str] = {}

    for col in text_cols:
        if col not in df.columns:
            continue
        # itérer en python pur (suffisant pour 7000 lignes)
        for v in df[col].astype(str).tolist():
            v = v or ""
            if not v.strip():
                continue
            if _is_false_positive(v):
                continue
            n = normalize_text(v)
            if not n:
                continue
            raw_to_norm[v] = n
            if n not in unique_set:
                unique_set.add(n)
                unique_texts.append(n)

    return unique_texts, raw_to_norm


def apply_pool_to_df(
    df_out: pd.DataFrame,
    text_cols: List[str],
    lang: str,
    model: str,
    rules_h: str,
    conn: sqlite3.Connection,
) -> None:
    """
    Applique les traductions depuis le cache SQLite à df_out.
    """
    for col in text_cols:
        if col not in df_out.columns:
            continue

        s = df_out[col].astype(str)

        # Normaliser en série (vectorisé)
        s_norm = s.map(lambda x: normalize_text(x) if (x and str(x).strip() and not _is_false_positive(str(x))) else "")

        keys = s_norm.map(lambda x: cache_key(lang, model, rules_h, x) if x else "").tolist()
        # récupérer du cache en une fois
        k_nonempty = [k for k in keys if k]
        k_to_tr = cache_get_many(conn, list(dict.fromkeys(k_nonempty)))  # preserve unique

        # Remplacer là où possible
        def replace_val(orig: str, norm: str) -> str:
            if not norm:
                return orig
            k = cache_key(lang, model, rules_h, norm)
            tr = k_to_tr.get(k, "")
            return ftfy.fix_text(tr) if tr else orig

        df_out[col] = [replace_val(o, n) for o, n in zip(s.tolist(), s_norm.tolist())]


# -------------------------
# QA: suspects & retries (pool ciblé)
# -------------------------
def find_suspect_texts(
    df_in: pd.DataFrame,
    df_out: pd.DataFrame,
    text_cols: List[str],
    lang: str,
    max_samples_print: int = 20,
) -> List[Tuple[int, str, str, str]]:
    """
    Retourne une liste de cellules suspectes:
      (row_idx, col, original, output)
    Critère: output semble encore allemand (score) OU
            output == original ET original semble allemand.
    (On évite "identique => suspect" brut.)
    """
    suspects: List[Tuple[int, str, str, str]] = []
    total_cells = 0

    for idx in range(len(df_in)):
        for col in text_cols:
            if col not in df_in.columns or col not in df_out.columns:
                continue
            v_in = str(df_in.at[idx, col] or "").strip()
            v_out = str(df_out.at[idx, col] or "").strip()
            if not v_in:
                continue
            if _is_false_positive(v_in):
                continue
            total_cells += 1

            in_score = german_score(v_in)
            out_score = german_score(v_out)

            # suspect si sortie encore allemande
            if out_score >= 1.2 and len(v_out) >= 6:
                suspects.append((idx, col, v_in, v_out))
                continue

            # suspect si inchangé ET entrée probablement allemande ET la langue cible attend un script différent
            if v_in == v_out and in_score >= 1.2:
                # pour RU/AR/UK, si la sortie ne contient pas le script attendu, c'est très suspect
                if not likely_target_script(lang, v_out):
                    suspects.append((idx, col, v_in, v_out))

    print(f"\n[{lang}] QA: cellules analysées (non vides & traduisibles): {total_cells}")
    print(f"[{lang}] QA: suspects trouvés: {len(suspects)}")
    for (r, c, vin, vout) in suspects[:max_samples_print]:
        print(f"[{lang}]   - L{r} '{c}': IN='{vin[:70]}' | OUT='{vout[:70]}'")
    if len(suspects) > max_samples_print:
        print(f"[{lang}]   ... +{len(suspects)-max_samples_print} autres")
    return suspects


# -------------------------
# Protokoll writer (identique)
# -------------------------
def write_protokoll(outdir: str, in_csv_basename: str, stats: Dict[str, Any]) -> None:
    proto_path = os.path.join(outdir, f"protokoll_{in_csv_basename}.csv")
    row = {col: "" for col in PROTOKOLL_COLUMNS}
    row["Datum"] = stats.get("datum", "")
    row["Datei"] = stats.get("datei", "")
    row["Sprache"] = stats.get("sprache", "")
    row["Modell"] = stats.get("modell", "")
    row["Temperatur"] = stats.get("temperatur", "")
    row["Batchgroesse"] = stats.get("batchgroesse", "")
    row["Parallelisierung"] = stats.get("parallelisierung", "Nein")
    row["Anzahl_API_Calls"] = stats.get("api_calls", 0)
    row["Gesamt_Input_Tokens"] = stats.get("input_tokens", 0)
    row["Gesamt_Output_Tokens"] = stats.get("output_tokens", 0)
    row["Laufzeit"] = stats.get("laufzeit", "")
    row["Anzahl_Retries"] = stats.get("retries", 0)
    row["Gesamtzeilen"] = stats.get("gesamtzeilen", 0)
    row["Deutsch_Reste_automatisch"] = stats.get("deutsch_reste_auto", 0)
    row["Verdachtsfaelle_gesamt"] = stats.get("verdachtsfaelle_gesamt", 0)
    row["Manuell_korrigiert"] = ""
    row["Neu_uebersetzt"] = stats.get("neu_uebersetzt", 0)
    row["Endgueltige_Deutsch_Reste"] = stats.get("endgueltige_deutsch_reste", 0)

    new_row = pd.DataFrame([row], columns=PROTOKOLL_COLUMNS)

    if os.path.exists(proto_path):
        df_proto = pd.read_csv(proto_path, sep=";", dtype=str, keep_default_na=False, encoding="utf-8-sig")
        mask = df_proto["Sprache"].str.upper() == str(stats.get("sprache", "")).upper()
        if mask.any():
            old_row = df_proto.loc[mask].iloc[0]
            for col in ["Manuell_korrigiert", "Technische_Auffaelligkeiten",
                        "Anpassungen_am_Prompt", "Anpassungen_an_Pipeline",
                        "Empfehlung_naechster_Lauf"]:
                if col in old_row and str(old_row[col]).strip():
                    new_row.at[new_row.index[0], col] = old_row[col]
            df_proto = df_proto[~mask]
        df_proto = pd.concat([df_proto, new_row], ignore_index=True)
    else:
        df_proto = new_row

    df_proto.to_csv(proto_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"[{stats.get('sprache','?')}] Protokoll écrit: {proto_path}")


# -------------------------
# Rules compacting (pour tokens)
# -------------------------
def compact_rules(rules_text: str) -> str:
    """
    IMPORTANT: ici on fait une version courte et stable.
    Idéalement: tu écris toi-même une version "résumé" (le mieux et le moins cher).
    Cette fonction fait un fallback simple: garder seulement les lignes non vides,
    limiter à N caractères.
    """
    lines = [ln.strip() for ln in rules_text.splitlines() if ln.strip()]
    short = "\n".join(lines)
    # limiter (évite de renvoyer un roman à chaque call)
    LIMIT = 2500  # ajuste selon tes règles
    if len(short) > LIMIT:
        short = short[:LIMIT] + "\n[TRUNCATED_RULES]"
    # ajouter des consignes invariantes
    short += (
        "\n\nConstraints:\n"
        "- Keep meaning faithful.\n"
        "- Do not invent content.\n"
        "- Preserve letters A/B/C/D/E and formatting as much as possible.\n"
        "- Keep numbers, units, codes unchanged.\n"
    )
    return short


# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser(description="CSV translator V2: pool+cache+resume (fast, stable, cheaper)")
    ap.add_argument("--pdf", required=True, help="Input CSV (param named --pdf for compatibility)")
    ap.add_argument("--prompt", required=True, help="DOCX file with rules")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--temperature", type=float, default=None)
    ap.add_argument("--outdir", default="out_translated_v2", help="Output folder")
    ap.add_argument("--langage", nargs="*", default=None, help="Target languages")
    ap.add_argument("--batch-size", type=int, default=200, help="Max items per API call (pool chunk)")
    ap.add_argument("--sep", default=None)
    ap.add_argument("--encoding", default="utf-8")
    ap.add_argument("--never-translate", default="")
    ap.add_argument("--protect-cols", default=DEFAULT_PROTECT_COLS)
    ap.add_argument("--column-order", default=None)
    ap.add_argument("--pruefung", action="store_true")
    ap.add_argument("--cache-db", default="translations.sqlite", help="SQLite cache db path")
    ap.add_argument("--checkpoint-every", type=int, default=5, help="Write CSV checkpoint every N chunks (>=1)")
    args = ap.parse_args()

    in_csv = args.pdf
    in_csv_basename = os.path.splitext(os.path.basename(in_csv))[0]
    outdir = args.outdir
    os.makedirs(outdir, exist_ok=True)

    api_key = _load_api_key() or os.environ.get("OPENAI_API_KEY", "")
    if not api_key:
        raise SystemExit("Please set OPENAI_API_KEY in env or .env")

    sep = guess_sep(in_csv, args.sep)
    rules_text = read_docx_text(args.prompt)
    rules_h = rules_hash(rules_text)
    rules_compact = compact_rules(rules_text)

    langs = [x.upper() for x in args.langage] if args.langage else DEFAULT_LANGS
    model = args.model
    temperature = args.temperature
    column_order = parse_column_order(args.column_order)

    never_translate = set([c.strip() for c in args.protect_cols.split(",") if c.strip()])
    if args.never_translate.strip():
        never_translate |= set([c.strip() for c in args.never_translate.split(",") if c.strip()])

    df = pd.read_csv(in_csv, sep=sep, dtype=str, keep_default_na=False, encoding=args.encoding)
    df.columns = [c.strip() for c in df.columns]

    if args.pruefung:
        rename_map = {}
        if "Zwischenprüfung" in df.columns and "Abschlussprüfung Teil 1" not in df.columns:
            rename_map["Zwischenprüfung"] = "Abschlussprüfung Teil 1"
        if "Abschlussprüfung" in df.columns and "Abschlussprüfung Teil 2" not in df.columns:
            rename_map["Abschlussprüfung"] = "Abschlussprüfung Teil 2"
        if rename_map:
            df = df.rename(columns=rename_map)
            print(f"Columns renamed (--pruefung): {rename_map}")

    fix_richtig_columns(df)

    text_cols = detect_text_columns(df, never_translate)
    if not text_cols:
        raise SystemExit("No translatable text columns found. Check protect/never-translate settings.")

    total_rows = len(df)
    print(f"\n{'='*72}")
    print(f"Rows: {total_rows} | Langs: {len(langs)} | Model: {model} | Temp: {temperature if temperature is not None else 'default'}")
    print(f"Sep: '{sep}' | Encoding: {args.encoding} | rules_hash: {rules_h}")
    print(f"Protected cols: {len(never_translate)} | Translatable cols: {len(text_cols)}")
    print(f"Output: {outdir} | Cache DB: {args.cache_db}")
    print(f"{'='*72}\n")

    # init cache + client once
    conn = cache_connect(os.path.join(outdir, args.cache_db) if not os.path.isabs(args.cache_db) else args.cache_db)
    client = OpenAI(api_key=api_key)

    def do_lang(lang: str) -> Dict[str, Any]:
        out_path = os.path.join(outdir, f"{in_csv_basename}_{lang}.csv")
        progress_path = os.path.join(outdir, f".progress_{in_csv_basename}_{lang}.json")
        pool_path = os.path.join(outdir, f".pool_{in_csv_basename}_{lang}.json")  # stores unique pool list + metadata

        stats: Dict[str, Any] = {
            "datum": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "datei": in_csv_basename,
            "sprache": lang,
            "modell": model,
            "temperatur": temperature if temperature is not None else "default",
            "batchgroesse": args.batch_size,
            "parallelisierung": "Non",
            "api_calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "laufzeit": "",
            "retries": 0,
            "gesamtzeilen": total_rows,
            "deutsch_reste_auto": 0,
            "verdachtsfaelle_gesamt": 0,
            "neu_uebersetzt": 0,
            "endgueltige_deutsch_reste": 0,
        }

        progress = load_json(progress_path, default={
            "stage": "init",
            "lang": lang,
            "rules_hash": rules_h,
            "model": model,
            "temperature": temperature,
            "chunks_done": 0,
            "qa_round": 0,
            "done": False,
            "last_update": "",
        })

        # if already done and file exists: quick QA rerun possible, but we'll just exit
        if progress.get("done") and os.path.exists(out_path):
            print(f"[{lang}] Already done -> {out_path}")
            return stats

        t_start = time.time()

        # Prepare output df (load existing if exists to continue applying, else fresh copy)
        if os.path.exists(out_path) and progress.get("stage") in {"applied", "qa"}:
            df_out = pd.read_csv(out_path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
            # restore protected cols from input
            for c in never_translate:
                if c in df.columns and c in df_out.columns:
                    df_out[c] = df[c].values
            print(f"[{lang}] Loaded partial output for resume: {out_path}")
        else:
            df_out = df.copy()

        # Always set language column
        if "Sprache" in df_out.columns:
            df_out["Sprache"] = lang.lower()

        # -------- Stage 1: Build pool (resume-safe)
        if progress.get("stage") in {"init", "pool"}:
            if os.path.exists(pool_path) and progress.get("stage") == "pool":
                pool_obj = load_json(pool_path, default=None)
                if pool_obj and pool_obj.get("rules_hash") == rules_h and pool_obj.get("lang") == lang:
                    unique_texts = pool_obj["unique_texts"]
                    print(f"[{lang}] Pool loaded ({len(unique_texts)} uniques) from checkpoint.")
                else:
                    unique_texts, _ = build_translation_pool(df, text_cols)
            else:
                unique_texts, _ = build_translation_pool(df, text_cols)
                atomic_write_json(pool_path, {
                    "lang": lang,
                    "rules_hash": rules_h,
                    "model": model,
                    "created_at": datetime.now().isoformat(),
                    "unique_texts": unique_texts,
                })
                print(f"[{lang}] Pool built and saved: {len(unique_texts)} unique texts")

            progress["stage"] = "translate"
            progress["chunks_done"] = 0
            progress["last_update"] = datetime.now().isoformat()
            atomic_write_json(progress_path, progress)

        # reload pool
        pool_obj = load_json(pool_path, default=None)
        if not pool_obj:
            raise RuntimeError(f"[{lang}] Missing pool checkpoint: {pool_path}")
        unique_texts = pool_obj["unique_texts"]
        n_pool = len(unique_texts)

        # -------- Stage 2: Translate missing pool texts (resume-safe)
        if progress.get("stage") == "translate":
            # Determine missing via cache
            keys = [cache_key(lang, model, rules_h, t) for t in unique_texts]
            existing = cache_get_many(conn, keys)
            missing_idx = [i for i, k in enumerate(keys) if k not in existing]

            print(f"[{lang}] Pool size: {n_pool} | Cache hits: {n_pool - len(missing_idx)} | Missing: {len(missing_idx)}")

            # Resume chunk translation by skipping already-in-cache each run
            # chunk building: limit count by args.batch_size and also by total chars to avoid giant payloads
            max_items = max(1, int(args.batch_size))
            max_chars = 18000  # safety budget

            chunks_done = 0
            chunk_no = 0

            # Translate missing in stable order
            i = 0
            while i < len(missing_idx):
                chunk_no += 1
                chunk_items = []
                chunk_chars = 0
                while i < len(missing_idx) and len(chunk_items) < max_items:
                    idx = missing_idx[i]
                    text_norm = unique_texts[idx]
                    # stop if char budget exceeded (and at least 1 item)
                    if chunk_items and (chunk_chars + len(text_norm) > max_chars):
                        break
                    chunk_items.append({"id": idx, "text": text_norm})
                    chunk_chars += len(text_norm)
                    i += 1

                if not chunk_items:
                    # fallback: force one item
                    idx = missing_idx[i]
                    chunk_items = [{"id": idx, "text": unique_texts[idx]}]
                    i += 1

                # Call API
                data, usage = translate_text_chunk(
                    client=client,
                    api_key=api_key,
                    model=model,
                    lang=lang,
                    rules_compact=rules_compact,
                    items=chunk_items,
                    temperature=temperature,
                )
                stats["api_calls"] += usage["api_calls"]
                stats["input_tokens"] += usage["prompt_tokens"]
                stats["output_tokens"] += usage["completion_tokens"]

                # Write to cache
                to_put = []
                for obj in data:
                    idx = int(obj["id"])
                    tr = str(obj.get("translation", "") or "").strip()
                    src = unique_texts[idx]
                    # final normalize output a bit
                    tr = ftfy.fix_text(tr)
                    if not tr:
                        tr = src
                    k = cache_key(lang, model, rules_h, src)
                    to_put.append((k, lang, model, rules_h, src, tr))
                cache_put_many(conn, to_put)

                chunks_done += 1
                progress["chunks_done"] = int(progress.get("chunks_done", 0)) + 1
                progress["last_update"] = datetime.now().isoformat()
                atomic_write_json(progress_path, progress)

                # Periodic output checkpoint optional (not needed yet, but helpful for long runs)
                if chunks_done % max(1, args.checkpoint_every) == 0:
                    print(f"[{lang}] Translate checkpoint: {chunks_done} chunk(s) done. (cache saved)")

            progress["stage"] = "apply"
            progress["last_update"] = datetime.now().isoformat()
            atomic_write_json(progress_path, progress)

        # -------- Stage 3: Apply translations to DF and save output (resume-safe)
        if progress.get("stage") == "apply":
            apply_pool_to_df(df_out, text_cols, lang, model, rules_h, conn)

            # Fill Richtig_Text* locally
            fill_richtig_text_vectorized(df_out)

            # Apply column order if any
            df_to_write = reorder_df_columns(df_out, column_order) if column_order else df_out
            atomic_write_csv(df_to_write, out_path, sep=sep, encoding="utf-8-sig")

            progress["stage"] = "qa"
            progress["qa_round"] = 0
            progress["last_update"] = datetime.now().isoformat()
            atomic_write_json(progress_path, progress)

            print(f"[{lang}] Applied translations + wrote output checkpoint: {out_path}")

        # -------- Stage 4: QA + targeted retries (resume-safe)
        max_retry_rounds = 4
        qa_round = int(progress.get("qa_round", 0))

        if progress.get("stage") == "qa":
            # Load freshest output for QA to avoid drift
            df_out = pd.read_csv(out_path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
            # restore protected cols from input
            for c in never_translate:
                if c in df.columns and c in df_out.columns:
                    df_out[c] = df[c].values

            # initial suspects stats
            suspects = find_suspect_texts(df, df_out, text_cols, lang)
            if qa_round == 0:
                stats["deutsch_reste_auto"] = len(suspects)

            while qa_round < max_retry_rounds and suspects:
                qa_round += 1
                stats["retries"] += 1
                stats["verdachtsfaelle_gesamt"] += len(suspects)

                # Build suspect unique text pool (use output text if it still looks german; otherwise input)
                suspect_texts = []
                for (_r, _c, vin, vout) in suspects:
                    candidate = vout if german_score(vout) >= 1.0 else vin
                    if not candidate.strip() or _is_false_positive(candidate):
                        continue
                    suspect_texts.append(normalize_text(candidate))

                suspect_texts = list(dict.fromkeys([t for t in suspect_texts if t]))  # unique preserve
                if not suspect_texts:
                    break

                # translate only those not corrected / missing (cache-aware)
                s_keys = [cache_key(lang, model, rules_h, t) for t in suspect_texts]
                existing = cache_get_many(conn, s_keys)
                missing = [i for i, k in enumerate(s_keys) if k not in existing]

                print(f"[{lang}] QA round {qa_round}: suspect uniques {len(suspect_texts)} | missing {len(missing)}")

                max_items = max(1, min(args.batch_size, 120))
                max_chars = 14000

                i = 0
                while i < len(missing):
                    chunk = []
                    ch_chars = 0
                    while i < len(missing) and len(chunk) < max_items:
                        si = missing[i]
                        txt = suspect_texts[si]
                        if chunk and (ch_chars + len(txt) > max_chars):
                            break
                        chunk.append({"id": si, "text": txt})
                        ch_chars += len(txt)
                        i += 1
                    if not chunk:
                        si = missing[i]
                        chunk = [{"id": si, "text": suspect_texts[si]}]
                        i += 1

                    data, usage = translate_text_chunk(
                        client=client,
                        api_key=api_key,
                        model=model,
                        lang=lang,
                        rules_compact=rules_compact,
                        items=chunk,
                        temperature=temperature,
                    )
                    stats["api_calls"] += usage["api_calls"]
                    stats["input_tokens"] += usage["prompt_tokens"]
                    stats["output_tokens"] += usage["completion_tokens"]

                    to_put = []
                    for obj in data:
                        si = int(obj["id"])
                        src = suspect_texts[si]
                        tr = ftfy.fix_text(str(obj.get("translation", "") or "").strip())
                        if not tr:
                            tr = src
                        k = cache_key(lang, model, rules_h, src)
                        to_put.append((k, lang, model, rules_h, src, tr))
                    cache_put_many(conn, to_put)

                # Re-apply translations (fast local) + rewrite checkpoint
                apply_pool_to_df(df_out, text_cols, lang, model, rules_h, conn)
                fill_richtig_text_vectorized(df_out)
                df_to_write = reorder_df_columns(df_out, column_order) if column_order else df_out
                atomic_write_csv(df_to_write, out_path, sep=sep, encoding="utf-8-sig")

                stats["neu_uebersetzt"] += len(suspects)

                # Save progress after each QA round
                progress["qa_round"] = qa_round
                progress["last_update"] = datetime.now().isoformat()
                atomic_write_json(progress_path, progress)

                # Next QA
                suspects = find_suspect_texts(df, df_out, text_cols, lang)

            # final suspects
            final_suspects = find_suspect_texts(df, df_out, text_cols, lang)
            stats["endgueltige_deutsch_reste"] = len(final_suspects)

            progress["done"] = True
            progress["stage"] = "done"
            progress["last_update"] = datetime.now().isoformat()
            atomic_write_json(progress_path, progress)

        elapsed_total = time.time() - t_start
        stats["laufzeit"] = f"{int(elapsed_total//60)}m{int(elapsed_total%60):02d}s"
        print(f"[{lang}] DONE in {stats['laufzeit']} -> {out_path}")
        return stats

    # Run langs sequentially (safe + stable). Tu peux paralléliser plus tard si besoin.
    for lang in langs:
        try:
            stats = do_lang(lang)
            write_protokoll(outdir, in_csv_basename, stats)
        except Exception as e:
            print(f"[{lang}] ERROR: {e} (resume will continue next run)")

    print("All done.")


if __name__ == "__main__":
    main()

    