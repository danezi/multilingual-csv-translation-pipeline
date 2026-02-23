#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Uebersetzungsprogramm mit Backoff-Retry-Logik
==============================================
Uebersetzt CSV-Dateien (z.B. Pruefungsfragen) in mehrere Zielsprachen
mithilfe der OpenAI-API. Unterstuetzt Fortschritts-Wiederaufnahme,
Dedup-Optimierung, Richtig_Text-Berechnung und Vollstaendigkeitspruefung.

Ausfuehrung:
    python uebersetzer_programm_backoff.py \
        --pdf EH_260216.csv \
        --prompt "Megaprompt_26_02_06_MFA_Uebersetzen.docx" \
        --encoding utf-8-sig \
        --langage AR TR RU UK PL EN RO

Ablauf:
    1. CSV einlesen und Spalten erkennen (Dedup vs. Batch)
    2. Spalte "Sprache" auf Zielsprache setzen
    3. Dedup-Spalten uebersetzen (Spalten mit wenigen einzigartigen Werten)
    4. Batch-Uebersetzung aller uebrigen Textspalten (mit Fortschrittsspeicherung)
    5. Richtig_Text-Spalten aus der richtigen Antwort (A/B/C/D/E) befuellen
    6. Vollstaendigkeitspruefung + automatischer Retry bei fehlenden Uebersetzungen
    7. Endgueltige CSV-Speicherung
"""

import os
import re
import json
import time
import argparse
from typing import List, Dict, Any, Tuple

import pandas as pd
import ftfy
from docx import Document
from openai import OpenAI


# ── Standardmaessige Zielsprachen ──
DEFAULT_LANGS = ["AR", "TR", "RU", "UK", "PL", "EN", "RO"]

# API-Schluessel wird in dieser Reihenfolge gesucht:
# 1. Umgebungsvariable OPENAI_API_KEY
# 2. Datei .env im Projektverzeichnis (Format: OPENAI_API_KEY=sk-...)
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

OPENAI_API_KEY = _load_api_key()

# ── Standardmodell ──
DEFAULT_MODEL = "gpt-5-mini"

# Spalten mit weniger als diesem Schwellenwert an einzigartigen Werten
# werden per Dedup uebersetzt (schneller, da nur einmal pro Wert)
DEDUP_THRESHOLD = 200


def create_chat_completion(client: OpenAI, model: str, messages: list, temperature: float | None = None):
    """
    Erstellt einen Chat-Completion-Aufruf an die OpenAI-API.

    Manche Modelle (z.B. gpt-5-mini) unterstuetzen temperature=0 nicht und akzeptieren
    nur den Standardwert (1). In diesem Fall wird automatisch ohne temperature-Parameter
    ein Fallback durchgefuehrt.
    """
    kwargs = {"model": model, "messages": messages}

    if temperature is not None:
        try:
            kwargs["temperature"] = temperature
            return client.chat.completions.create(**kwargs)
        except Exception as e:
            msg = str(e)
            if "temperature" in msg and ("unsupported" in msg or "Only the default (1)" in msg):
                print(f"[INFO] temperature={temperature} nicht unterstuetzt von {model} — Fallback ohne temperature")
                kwargs.pop("temperature", None)
                return client.chat.completions.create(**kwargs)
            raise

    return client.chat.completions.create(**kwargs)


def read_docx_text(path: str) -> str:
    """Liest den Text aus einer DOCX-Datei (Megaprompt mit Uebersetzungsregeln)."""
    doc = Document(path)
    parts = []
    for p in doc.paragraphs:
        t = (p.text or "").strip()
        if t:
            parts.append(t)
    text = "\n".join(parts).strip()
    if not text:
        raise ValueError(f"Prompt-DOCX ist leer oder nicht lesbar: {path}")
    return text


def guess_sep(path: str, user_sep: str | None) -> str:
    """Erkennt automatisch den CSV-Trenner (';' oder ','), falls nicht angegeben."""
    if user_sep:
        return user_sep
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(4096)
    return ";" if head.count(";") >= head.count(",") else ","


def safe_json_extract(text: str) -> str:
    """Entfernt Markdown-Codeblock-Markierungen (```json ... ```) aus der API-Antwort."""
    t = (text or "").strip()
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z]*\s*", "", t)
        t = re.sub(r"\s*```$", "", t)
    return t.strip()


def detect_text_columns(df: pd.DataFrame, never_translate: set[str]) -> List[str]:
    """Erkennt automatisch alle Textspalten, die uebersetzt werden sollen (ausser geschuetzte Spalten)."""
    cols = []
    for c in df.columns:
        if c in never_translate:
            continue
        if pd.api.types.is_string_dtype(df[c]):
            cols.append(c)
    return cols


def parse_column_order(s: str | None) -> List[str] | None:
    """Parst die gewuenschte Spaltenreihenfolge aus dem CLI-Argument (';' oder ',' getrennt)."""
    if not s:
        return None
    parts = [p.strip() for p in s.split(";") if p.strip()]
    if len(parts) <= 1 and "," in s:
        parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts or None


def reorder_df_columns(df: pd.DataFrame, ordered_cols: List[str]) -> pd.DataFrame:
    """Ordnet die DataFrame-Spalten in die gewuenschte Reihenfolge um."""
    existing_ordered = [c for c in ordered_cols if c in df.columns]
    remaining = [c for c in df.columns if c not in existing_ordered]
    return df[existing_ordered + remaining]


def translate_batch(
    api_key: str,
    model: str,
    prompt_rules: str,
    lang: str,
    batch_rows: List[Tuple[int, pd.Series]],
    text_cols: List[str],
    temperature: float | None = None,
) -> List[Dict[str, Any]]:
    """
    Uebersetzt einen Batch von Zeilen ueber die OpenAI-API.

    Erstellt ein JSON-Payload mit allen Textfeldern der Zeilen und sendet es
    an die API. Wiederholungsversuche mit exponentiellem Backoff bei Fehlern
    (max. 10 Versuche).

    Args:
        batch_rows: Liste von (Zeilenindex, pandas-Series)-Tupeln
        text_cols: Spalten, die uebersetzt werden sollen

    Returns:
        Liste von Dictionaries mit {"row_index": int, "fields": {...}}
    """
    payload = []
    for idx, row in batch_rows:
        fields = {}
        for col in text_cols:
            v = row.get(col, "")
            if v is None:
                continue
            if not isinstance(v, str):
                v = str(v)
            if v.strip():  # Leere Felder werden nicht gesendet
                fields[col] = v
        payload.append({"row_index": int(idx), "fields": fields})

    instructions = (
        prompt_rules.strip()
        + "\n\n"
        + f"ZIELSPRACHE: {lang}\n"
        + "Gib ausschließlich gültiges JSON im gleichen Schema zurück "
          "(Liste aus Objekten mit row_index und fields)."
    )

    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            client = OpenAI(api_key=api_key)
            resp = create_chat_completion(
                client=client,
                model=model,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": instructions},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
            )
            break
        except Exception as e:
            if attempt == max_retries:
                raise
            wait = min(2 ** attempt, 60)
            print(f"[{lang}] Fehler (Versuch {attempt}/{max_retries}): {type(e).__name__}: {e} — warte {wait}s...")
            if hasattr(e, "__cause__") and e.__cause__:
                print(f"        Cause: {type(e.__cause__).__name__}: {e.__cause__}")
            time.sleep(wait)

    out_text = safe_json_extract(resp.choices[0].message.content or "")
    data = json.loads(out_text)

    if not isinstance(data, list):
        raise ValueError("API-Antwort ist kein JSON-Array.")
    if len(data) != len(payload):
        raise ValueError(f"Batch-Groesse stimmt nicht: erwartet {len(payload)}, erhalten {len(data)}")

    return data


def apply_translations(df_out: pd.DataFrame, translated: List[Dict[str, Any]], text_cols: List[str]) -> None:
    """Wendet die uebersetzten Felder auf den Ausgabe-DataFrame an (mit ftfy-Textkorrektur)."""
    for item in translated:
        idx = item["row_index"]
        fields = item["fields"]
        for col in text_cols:
            if col in fields:
                df_out.at[idx, col] = ftfy.fix_text(fields[col]) if isinstance(fields[col], str) else fields[col]


def fix_richtig_columns(df: pd.DataFrame) -> int:
    """
    Korrigiert Zeilen, in denen Richtig1 den vollen Antworttext statt eines
    Buchstabens (A/B/C/D/E) enthaelt.

    Fuer jede betroffene Zeile wird geprueft, welche Antwortspalte (A-E) den
    gleichen Text wie Richtig1 enthaelt. Wird eine Uebereinstimmung gefunden,
    wird Richtig1 durch den Buchstaben ersetzt.

    Returns:
        Anzahl der korrigierten Zeilen.
    """
    if "Richtig1" not in df.columns:
        return 0

    answer_letters = [c for c in ["A", "B", "C", "D", "E"] if c in df.columns]
    fixed = 0

    for idx in df.index:
        val = str(df.at[idx, "Richtig1"]).strip()
        if val.upper() in ["A", "B", "C", "D", "E", ""]:
            continue  # Bereits ein Buchstabe oder leer — nichts zu tun

        # Suche passende Antwortspalte
        for letter in answer_letters:
            cell = str(df.at[idx, letter]).strip()
            if cell and cell == val:
                df.at[idx, "Richtig1"] = letter
                fixed += 1
                break

    if fixed:
        print(f"  [fix_richtig_columns] {fixed} Zeilen korrigiert: Richtig1 enthielt Text statt Buchstabe.")
    return fixed


def fill_richtig_text(df_out: pd.DataFrame) -> None:
    """
    Befuellt die Richtig_Text-Spalten anhand der richtigen Antwort.

    Wenn z.B. Richtig1 = "B", wird Richtig_Text1 mit dem Inhalt der Spalte B
    dieser Zeile befuellt. Funktioniert fuer Richtig1 -> Richtig_Text1/Richtig1_Text
    und Richtig2 -> Richtig_Text2.
    """
    def get_correct(row, richtig_col):
        ans = str(row.get(richtig_col, "")).strip().upper()
        return row.get(ans, "") if ans in ["A", "B", "C", "D", "E"] else ""

    if "Richtig1" in df_out.columns:
        for rt_col in ["Richtig_Text1", "Richtig1_Text"]:
            if rt_col in df_out.columns:
                df_out[rt_col] = df_out.apply(lambda r: get_correct(r, "Richtig1"), axis=1)

    if "Richtig2" in df_out.columns:
        if "Richtig_Text2" in df_out.columns:
            df_out["Richtig_Text2"] = df_out.apply(lambda r: get_correct(r, "Richtig2"), axis=1)


def _is_false_positive(val: str) -> bool:
    """
    Prueft, ob ein Wert ein falsch-positiver Treffer ist:
    Text, der nach der Uebersetzung naturgemaess gleich bleibt.

    Returns:
        True wenn der Wert kein echtes Uebersetzungsproblem darstellt.
    """
    v = val.strip()
    # Leer
    if not v:
        return True
    # Einzelbuchstabe (MC-Antworten: A, B, C, D, E)
    if len(v) <= 2:
        return True
    # Reine Zahlen (Ganzzahlen, Dezimalzahlen, mit Leerzeichen/Trennzeichen)
    if re.fullmatch(r"[\d\s.,/\-+%°]+", v):
        return True
    # Zahlen mit Waehrung/Einheit (z.B. "10 €", "1.000 €", "150 $", "25 kg")
    if re.fullmatch(r"[\d\s.,]+\s*[€$£¥₹%°²³]+", v):
        return True
    # Kurze Markennamen / Abkuerzungen (z.B. "C&A", "H&M", "ALG I", "BWL")
    if len(v) <= 5:
        return True
    # Codes/Kennungen (alphanumerisch ohne Leerzeichen, z.B. "MFA-001", "EH_26")
    if re.fullmatch(r"[A-Za-z0-9_\-./]+", v) and " " not in v:
        return True
    # URLs oder E-Mail-Adressen
    if re.match(r"https?://|www\.|[^@\s]+@[^@\s]+\.", v):
        return True
    # Nur Satzzeichen/Symbole
    if re.fullmatch(r"[\W_]+", v):
        return True
    # Grossbuchstaben-Abkuerzungen mit Leerzeichen (z.B. "ALG II", "BGB AT")
    if re.fullmatch(r"[A-Z0-9\s./\-&]+", v) and len(v) <= 10:
        return True
    return False


def verify_translation_completeness(
    df_input: pd.DataFrame,
    df_output: pd.DataFrame,
    text_cols: List[str],
    lang: str,
) -> List[Tuple[int, str, str]]:
    """
    Prueft, ob alle Zeilen der Eingabe-CSV korrekt uebersetzt wurden.

    Vergleicht Eingabe- und Ausgabezellen. Zellen, die identisch geblieben sind,
    werden als verdaechtig markiert, es sei denn, sie sind falsch-positive Treffer
    (Zahlen, Abkuerzungen, Codes usw.).

    Returns:
        Liste der verdaechtigen Zellen als (Zeilenindex, Spaltenname, Originalwert).
        Leere Liste = alles in Ordnung.
    """
    if len(df_output) != len(df_input):
        print(f"[{lang}] FEHLER PRUEFUNG: Ausgabe-CSV hat {len(df_output)} Zeilen "
              f"statt {len(df_input)} erwarteten Zeilen.")
        return []

    total_cells = 0
    translated_cells = 0
    skipped_cells = 0
    untranslated: List[Tuple[int, str, str]] = []

    for idx in range(len(df_input)):
        for col in text_cols:
            val_in = str(df_input.at[idx, col]).strip() if col in df_input.columns else ""
            val_out = str(df_output.at[idx, col]).strip() if col in df_output.columns else ""
            if not val_in:
                continue
            total_cells += 1
            if val_in != val_out:
                translated_cells += 1
            elif _is_false_positive(val_in):
                skipped_cells += 1
                translated_cells += 1
            else:
                untranslated.append((idx, col, val_in))

    pct = (translated_cells / total_cells * 100) if total_cells else 100

    print(f"\n[{lang}] === PRUEFBERICHT ===")
    print(f"[{lang}]   Gesamtzeilen       : {len(df_input)}")
    print(f"[{lang}]   Textzellen         : {total_cells}")
    print(f"[{lang}]   Uebersetzt         : {translated_cells - skipped_cells}")
    print(f"[{lang}]   Ignoriert (falsch+): {skipped_cells}")
    print(f"[{lang}]   Verdaechtig        : {len(untranslated)}")
    print(f"[{lang}]   Vollstaendigkeit   : {pct:.1f}%")

    if untranslated:
        print(f"[{lang}]   ACHTUNG: {len(untranslated)} Zelle(n) moeglicherweise nicht uebersetzt:")
        for row_idx, col, val in untranslated[:20]:
            print(f"[{lang}]       Zeile {row_idx}, Spalte '{col}': \"{val[:80]}\"")
        if len(untranslated) > 20:
            print(f"[{lang}]       ... und {len(untranslated) - 20} weitere")
    else:
        print(f"[{lang}]   ERGEBNIS: OK")
    print(f"[{lang}] ====================\n")

    return untranslated


def translate_unique_values(
    api_key: str,
    model: str,
    prompt_rules: str,
    lang: str,
    col_name: str,
    values: List[str],
    batch_size: int = 50,
    temperature: float | None = None,
) -> Dict[str, str]:
    """
    Uebersetzt nur die einzigartigen Werte einer Spalte und gibt ein Mapping zurueck.

    Optimierung: Spalten mit wenigen einzigartigen Werten (z.B. "LF", "Abschnitt")
    werden nur einmal uebersetzt statt fuer jede Zeile einzeln.
    """
    mapping: Dict[str, str] = {}

    for start in range(0, len(values), batch_size):
        batch_vals = values[start : start + batch_size]
        payload = [{"row_index": i, "fields": {col_name: v}} for i, v in enumerate(batch_vals)]

        instructions = (
            prompt_rules.strip()
            + "\n\n"
            + f"ZIELSPRACHE: {lang}\n"
            + "Gib ausschließlich gültiges JSON im gleichen Schema zurück "
              "(Liste aus Objekten mit row_index und fields)."
        )

        max_retries = 10
        for attempt in range(1, max_retries + 1):
            try:
                client = OpenAI(api_key=api_key)
                resp = create_chat_completion(
                    client=client,
                    model=model,
                    temperature=temperature,
                    messages=[
                        {"role": "system", "content": instructions},
                        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    ],
                )
                break
            except Exception as e:
                if attempt == max_retries:
                    raise
                wait = min(2 ** attempt, 60)
                print(f"[{lang}] Dedup-Fehler (Versuch {attempt}/{max_retries}): {type(e).__name__}: {e} — warte {wait}s...")
                if hasattr(e, "__cause__") and e.__cause__:
                    print(f"        Cause: {type(e.__cause__).__name__}: {e.__cause__}")
                time.sleep(wait)

        data = json.loads(safe_json_extract(resp.choices[0].message.content or ""))

        if isinstance(data, list):
            for item in data:
                idx = item.get("row_index", -1)
                fields = item.get("fields", {})
                if col_name in fields and 0 <= idx < len(batch_vals):
                    val = fields[col_name]
                    mapping[batch_vals[idx]] = ftfy.fix_text(val) if isinstance(val, str) else val

    return mapping


def main():
    """Hauptfunktion: CLI-Argumente parsen und Uebersetzung fuer alle Zielsprachen starten."""
    ap = argparse.ArgumentParser(
        description="Uebersetzt eine CSV-Datei anhand von Regeln (Prompt-DOCX) und erzeugt Ausgaben pro Sprache."
    )
    ap.add_argument("--pdf", required=True, help="Eingabedatei (CSV). Der Parameter heisst absichtlich --pdf.")
    ap.add_argument("--prompt", required=True, help="DOCX-Word-Datei mit dem Megaprompt/Uebersetzungsregeln.")
    ap.add_argument("--model", default=DEFAULT_MODEL, help="OpenAI-Modell, z.B.: gpt-5-mini, gpt-4o-mini")
    ap.add_argument("--temperature", type=float, default=None, help="Temperature fuer die API (z.B. 0, 0.3, 1). Ohne Angabe wird der Standardwert des Modells verwendet.")

    ap.add_argument("--outdir", default="out_translated_backofff", help="Ausgabeordner")

    ap.add_argument("--langage", nargs="*", default=None, help="Zielsprachen, z.B.: --langage EN oder --langage EN TR AR")
    ap.add_argument("--batch-size", type=int, default=100, help="Zeilen pro Batch (10-200)")
    ap.add_argument("--sep", default=None, help="CSV-Trenner, z.B. ';' oder ',' (automatisch wenn leer)")
    ap.add_argument("--encoding", default="utf-8", help="Eingabe-Encoding, z.B. utf-8 oder utf-8-sig")

    ap.add_argument(
        "--never-translate",
        default="",
        help="Spalten (kommagetrennt), die NIE uebersetzt werden sollen.",
    )
    ap.add_argument(
        "--dedup-cols",
        default="LF,Abschnitt",
        help="Spalten, die IMMER per Dedup (einzigartige Werte) uebersetzt werden, unabhaengig vom Schwellenwert.",
    )
    ap.add_argument(
        "--protect-cols",
        default="lfdNr,FrageNr,BerufNr,Beruf,LFNr,AbschnNr,Nr,Richtig1,Richtig2,Schwierigkeit,Sprache,Richtig_Text1,Richtig1_Text,Richtig_Text2,Abschlussprüfung Teil 1,Abschlussprüfung Teil 2,Lehrjahr,Zwischenprüfung,Abschlussprüfung",
        help="Standardmaessig geschuetzte Spalten (IDs/Loesungen/Kopien), die nicht uebersetzt werden.",
    )

    ap.add_argument(
        "--column-order",
        default=None,
        help='Spaltenreihenfolge in der Ausgabe (durch ; getrennt). Beispiel: --column-order "A;B;C"',
    )

    ap.add_argument(
        "--pruefung",
        action="store_true",
        help="Benennt Zwischenpruefung->Abschlusspruefung Teil 1 und Abschlusspruefung->Abschlusspruefung Teil 2 um.",
    )

    args = ap.parse_args()

    in_csv = args.pdf
    in_csv_basename = os.path.splitext(os.path.basename(in_csv))[0]
    prompt_docx = args.prompt
    outdir = args.outdir
    model = args.model
    temperature = args.temperature
    langs = [x.upper() for x in args.langage] if args.langage else DEFAULT_LANGS
    column_order = parse_column_order(args.column_order)

    os.makedirs(outdir, exist_ok=True)

    api_key = OPENAI_API_KEY or os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("Bitte OPENAI_API_KEY als Umgebungsvariable setzen (empfohlen) oder im Skript hinterlegen.")

    sep = guess_sep(in_csv, args.sep)
    prompt_rules = read_docx_text(prompt_docx)

    never_translate = set([c.strip() for c in args.protect_cols.split(",") if c.strip()])
    if args.never_translate.strip():
        never_translate |= set([c.strip() for c in args.never_translate.split(",") if c.strip()])

    df = pd.read_csv(in_csv, sep=sep, dtype=str, keep_default_na=False, encoding=args.encoding)

    # Spaltennamen normalisieren (fuehrende/nachfolgende Leerzeichen entfernen)
    df.columns = [c.strip() for c in df.columns]

    # --pruefung: Pruefungsspalten umbenennen
    if args.pruefung:
        rename_map = {}
        if "Zwischenprüfung" in df.columns and "Abschlussprüfung Teil 1" not in df.columns:
            rename_map["Zwischenprüfung"] = "Abschlussprüfung Teil 1"
        if "Abschlussprüfung" in df.columns and "Abschlussprüfung Teil 2" not in df.columns:
            rename_map["Abschlussprüfung"] = "Abschlussprüfung Teil 2"
        if rename_map:
            df = df.rename(columns=rename_map)
            print(f"Spalten umbenannt (--pruefung): {rename_map}")

    # Richtig1 korrigieren: Volltext -> Buchstabe (A/B/C/D/E)
    fix_richtig_columns(df)

    forced_dedup = set(c.strip() for c in args.dedup_cols.split(",") if c.strip())
    text_cols = detect_text_columns(df, never_translate)

    # Erzwungene Dedup-Spalten: auch hinzufuegen wenn in protect-Liste
    for col in forced_dedup:
        if col in df.columns and col not in text_cols:
            text_cols.append(col)

    if not text_cols:
        raise SystemExit("Keine Textspalten zum Uebersetzen gefunden. Spaltennamen / protect-Listen pruefen.")

    # Aufteilen: Dedup-Spalten vs. Batch-Spalten
    dedup_cols = []
    batch_cols = []
    for col in text_cols:
        if col in forced_dedup:
            dedup_cols.append(col)
        else:
            unique_count = df[col][df[col].astype(str).str.strip() != ""].nunique()
            if 0 < unique_count <= DEDUP_THRESHOLD:
                dedup_cols.append(col)
            else:
                batch_cols.append(col)

    if dedup_cols:
        print(f"Dedup-Spalten (1x uebersetzen): {dedup_cols}")
    if batch_cols:
        print(f"Batch-Spalten (pro Zeile): {batch_cols}")

    total_rows = len(df)
    total_batches = (total_rows + args.batch_size - 1) // args.batch_size
    print(f"\n{'='*60}")
    print(f"  {total_rows} Zeilen | {total_batches} Batches/Sprache | {len(langs)} Sprachen")
    print(f"  Modell: {model} | Temperature: {temperature if temperature is not None else 'Modell-Standard'}")
    print(f"  Dedup-Spalten: {dedup_cols or '(keine)'}")
    print(f"  Batch-Spalten: {batch_cols}")
    print(f"  Ausgabeordner: {outdir}")
    print(f"{'='*60}\n")

    def do_lang(lang: str):
        """Fuehrt die Uebersetzung fuer eine einzelne Zielsprache durch."""
        df_out = df.copy()
        items = list(df.iterrows())
        out_path = os.path.join(outdir, f"{in_csv_basename}_{lang}.csv")
        progress_path = os.path.join(outdir, f".progress_{in_csv_basename}_{lang}.json")

        # 1) Fortschritt laden / Wiederaufnahme
        done_up_to = 0
        already_done = False
        if os.path.exists(progress_path):
            with open(progress_path, "r", encoding="utf-8") as f:
                progress = json.load(f)
            if progress.get("done"):
                already_done = True
                if os.path.exists(out_path):
                    df_out = pd.read_csv(out_path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
                    # Geschuetzte Spalten aus korrigiertem Input wiederherstellen
                    for col in never_translate:
                        if col in df.columns and col in df_out.columns:
                            df_out[col] = df[col].values
                    print(f"[{lang}] Bereits abgeschlossen — starte Pruefung + ggf. Retry ({out_path})")
                    done_up_to = total_rows
                else:
                    already_done = False
            if not already_done:
                done_up_to = progress.get("done_up_to", 0)
                if os.path.exists(out_path):
                    df_out = pd.read_csv(out_path, sep=sep, dtype=str, keep_default_na=False, encoding="utf-8-sig")
                    # Geschuetzte Spalten aus korrigiertem Input wiederherstellen
                    for col in never_translate:
                        if col in df.columns and col in df_out.columns:
                            df_out[col] = df[col].values
                    pct_resume = done_up_to / total_rows * 100 if total_rows else 0
                    print(f"[{lang}] Wiederaufnahme bei Zeile {done_up_to}/{total_rows} ({pct_resume:.1f}%)")

        t_start = time.time()

        # 2) Spalte "Sprache" auf Zielsprache setzen (immer, auch bei Wiederaufnahme)
        if "Sprache" in df_out.columns:
            df_out["Sprache"] = lang.lower()

        if not already_done:
            # 3) Dedup-Spalten uebersetzen (einzigartige Werte nur 1x uebersetzen)
            for col in dedup_cols:
                unique_vals = [v for v in df[col].unique() if isinstance(v, str) and v.strip()]
                if not unique_vals:
                    continue
                mapping = translate_unique_values(api_key, model, prompt_rules, lang, col, unique_vals, temperature=temperature)
                df_out[col] = df_out[col].map(mapping).fillna(df_out[col])
                print(f"[{lang}] Dedup '{col}': {len(unique_vals)} Werte uebersetzt (statt {total_rows}x)")

            # 4) Batch-Uebersetzung (zeilenweise mit Fortschrittsspeicherung)
            for start in range(0, len(items), args.batch_size):
                batch = items[start : start + args.batch_size]
                end = start + len(batch) - 1
                done_now = start + len(batch)
                pct = done_now / total_rows * 100 if total_rows else 100

                if done_now <= done_up_to:
                    continue

                translated = translate_batch(api_key, model, prompt_rules, lang, batch, batch_cols, temperature=temperature)
                apply_translations(df_out, translated, batch_cols)

                # Zwischenspeicherung (mit Spaltenreihenfolge falls angegeben)
                for _ in range(10):
                    try:
                        df_to_write = reorder_df_columns(df_out, column_order) if column_order else df_out
                        df_to_write.to_csv(out_path, sep=sep, index=False, encoding="utf-8-sig")
                        break
                    except PermissionError:
                        print(f"[{lang}] ACHTUNG: {out_path} ist gesperrt (Excel offen?) — warte 5s...")
                        time.sleep(5)

                with open(progress_path, "w", encoding="utf-8") as f:
                    json.dump({"done_up_to": done_now}, f)

                elapsed = time.time() - t_start
                rows_since_start = done_now - done_up_to
                rows_remaining = total_rows - done_now
                if rows_since_start > 0:
                    secs_per_row = elapsed / rows_since_start
                    eta_secs = rows_remaining * secs_per_row
                    eta_min = int(eta_secs // 60)
                    eta_sec = int(eta_secs % 60)
                    eta_str = f"~{eta_min}m{eta_sec:02d}s"
                else:
                    eta_str = "?"

                print(f"[{lang}] {pct:5.1f}% | Ligne {done_now}/{total_rows} | Batch {start}..{end} OK | ETA: {eta_str}")

        # 5) Richtig_Text-Spalten befuellen (vor der Pruefung)
        fill_richtig_text(df_out)

        # 6) Vollstaendigkeitspruefung + Retry bei nicht uebersetzten Zellen (max. 2 Versuche)
        max_retry = 4
        for retry_round in range(max_retry):
            suspect = verify_translation_completeness(df, df_out, text_cols, lang)
            if not suspect:
                break

            print(f"[{lang}] RETRY {retry_round + 1}/{max_retry}: Neuuebersetzung von {len(suspect)} verdaechtigen Zelle(n)...")

            # Verdaechtige Zellen nach Zeilenindex gruppieren
            rows_to_retry: Dict[int, List[str]] = {}
            for idx, col, _ in suspect:
                rows_to_retry.setdefault(idx, []).append(col)

            # Mini-Batches fuer die Neuuebersetzung erstellen
            retry_indices = sorted(rows_to_retry.keys())
            for rb_start in range(0, len(retry_indices), args.batch_size):
                rb_chunk = retry_indices[rb_start : rb_start + args.batch_size]
                retry_batch = [(idx, df.iloc[idx]) for idx in rb_chunk]
                # Zu uebersetzende Spalten = Vereinigung der verdaechtigen Spalten dieses Batches
                retry_cols = list({col for idx in rb_chunk for col in rows_to_retry[idx]})

                translated = translate_batch(api_key, model, prompt_rules, lang, retry_batch, retry_cols, temperature=temperature)
                apply_translations(df_out, translated, retry_cols)
                print(f"[{lang}] RETRY batch {rb_start}..{rb_start + len(rb_chunk) - 1} OK ({len(rb_chunk)} lignes)")

            # Richtig_Text nach jedem Retry neu befuellen (A/B/C/D-Antworten koennten sich geaendert haben)
            fill_richtig_text(df_out)

        # 7) Endgueltige Speicherung (mit Spaltenreihenfolge falls angegeben)
        df_to_write = reorder_df_columns(df_out, column_order) if column_order else df_out
        df_to_write.to_csv(out_path, sep=sep, index=False, encoding="utf-8-sig")

        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump({"done": True, "done_up_to": total_rows}, f)

        elapsed_total = time.time() - t_start
        print(f"[{lang}] 100.0% | FERTIG in {int(elapsed_total//60)}m{int(elapsed_total%60):02d}s | {out_path}")

    # Alle Zielsprachen nacheinander verarbeiten
    for lang in langs:
        try:
            do_lang(lang)
        except Exception as e:
            print(f"[{lang}] FEHLER: {e} — beim Neustart wird das Skript fortgesetzt.")
    print("Abgeschlossen.")


if __name__ == "__main__":
    main()
