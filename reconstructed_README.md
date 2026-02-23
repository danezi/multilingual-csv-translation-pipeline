# CSV-Übersetzungsprogramm

Übersetzt CSV-Dateien (z.B. Prüfungsfragen) automatisch in mehrere Zielsprachen mithilfe der OpenAI-API.

Entwickelt für **BeeZubi Lernwelt GmbH** zur mehrsprachigen Bereitstellung von Prüfungsfragen.

## Features

- **Batch-Übersetzung** -- Zeilen werden in konfigurierbaren Batches (Standard: 100) an die API gesendet
- **Dedup-Optimierung** -- Spalten mit wenigen einzigartigen Werten werden nur 1x übersetzt
- **Fortschritts-Wiederaufnahme** -- Bei Abbruch wird genau dort fortgesetzt, wo aufgehört wurde
- **Vollständigkeitsprüfung** -- Eingabe vs. Ausgabe zellweiser Vergleich mit automatischem Retry
- **Falsch-Positiv-Erkennung** -- Zahlen, Codes, Abkürzungen werden bei der Prüfung ignoriert
- **Geschützte Spalten** -- IDs, Lösungen und Metadaten werden nie übersetzt
- **Richtig_Text-Berechnung** -- Richtige Antwort (A–E) wird automatisch nachgeschlagen und befüllt
- **Exponentieller Backoff** -- Bis zu 10 Wiederholungsversuche bei API-Fehlern

## Voraussetzungen

- Python 3.10+
- OpenAI API-Key

## Installation

```bash
pip install -r requirements.txt
```

### API-Key setzen

```powershell
# PowerShell
$env:OPENAI_API_KEY="sk-..."
```

```bash
# Linux / macOS
export OPENAI_API_KEY="sk-..."
```

## Verwendung

```bash
python uebersetzer_programm_backoff.py ^
    --pdf EH_260216.csv ^
    --prompt "Megaprompt_26_02_06_MFA_Uebersetzen.docx" ^
    --encoding utf-8-sig ^
    --langage AR TR RU UK PL EN RO ^
    --pruefung
```

## Parameter

| Parameter | Standard | Beschreibung |
|-----------|----------|-------------|
| `--pdf` | *(Pflicht)* | Eingabe-CSV-Datei |
| `--prompt` | *(Pflicht)* | DOCX-Datei mit Übersetzungsregeln (Megaprompt) |
| `--model` | `gpt-5-mini` | OpenAI-Modell |
| `--temperature` | Modell-Std. | Temperature für die API (0 = deterministisch) |
| `--outdir` | `out_translated_backofff` | Ausgabeordner |
| `--langage` | AR TR RU UK PL EN RO | Zielsprachen (Leerzeichen-getrennt) |
| `--batch-size` | 100 | Zeilen pro API-Batch (10–200) |
| `--sep` | *(auto)* | CSV-Trenner (`;` oder `,`) |
| `--encoding` | `utf-8` | Eingabe-Encoding |
| `--protect-cols` | *(siehe unten)* | Spalten, die nie übersetzt werden |
| `--never-translate` | *(leer)* | Zusätzliche geschützte Spalten |
| `--dedup-cols` | `LF,Abschnitt` | Spalten, die per Dedup übersetzt werden |
| `--column-order` | *(keine)* | Spaltenreihenfolge in der Ausgabe |
| `--pruefung` | *(Flag)* | Benennt Prüfungsspalten um |

### Standard-geschützte Spalten

`lfdNr, FrageNr, BerufNr, Beruf, LFNr, AbschnNr, Nr, Richtig1, Richtig2, Schwierigkeit, Sprache, Richtig_Text1, Richtig1_Text, Richtig_Text2, Abschlussprüfung Teil 1, Abschlussprüfung Teil 2, Lehrjahr`

## Ablauf

1. **CSV einlesen** -- Datei laden, Spalten normalisieren, Richtig1 korrigieren (Volltext -> Buchstabe)
2. **Spalten klassifizieren** -- Geschützt / Dedup / Batch
3. **Sprache setzen** -- Spalte `Sprache` auf Zielsprachcode (`ar`, `tr`, `ru`...)
4. **Dedup-Übersetzung** -- Spalten mit wenigen einzigartigen Werten 1x übersetzen, per Mapping anwenden
5. **Batch-Übersetzung** -- Restliche Spalten in Batches an die OpenAI-API, nach jedem Batch zwischenspeichern
6. **Richtig_Text befüllen** -- Richtige Antwort (A–E) nachschlagen und in Richtig_Text-Spalten eintragen
7. **Prüfung + Retry** -- Eingabe vs. Ausgabe vergleichen, falsch-positive ignorieren, verdächtige Zellen bis zu 4x neu übersetzen
8. **Speichern** -- CSV final speichern, Fortschritt auf `done` setzen

## Ausgabe

Pro Zielsprache wird eine Datei `<Eingabename>_<SPRACHE>.csv` im Ausgabeordner erzeugt (z.B. `EH_260216_AR.csv`).

Eine Fortschrittsdatei `.progress_<Name>_<SPRACHE>.json` ermöglicht die Wiederaufnahme bei Abbruch.

## Modell und API

- Standardmodell: `gpt-5-mini` (konfigurierbar über `--model`)
- Kommunikation über die OpenAI Chat Completions API
- Bei API-Fehlern: bis zu 10 Wiederholungsversuche mit exponentiellem Backoff (2^n Sekunden, max. 60s)
- Automatischer Fallback ohne `temperature` falls das Modell den Parameter nicht unterstützt

## Projektstruktur

```
.
├── uebersetzer_programm_backoff.py   # Hauptprogramm
├── requirements.txt                   # Python-Abhängigkeiten
├── dokumentation_uebersetzer.tex      # LaTeX-Dokumentation
├── README.md                          # Diese Datei
└── out_translated_backofff/           # Ausgabeordner (generiert)
    ├── EH_260216_AR.csv
    ├── EH_260216_TR.csv
    ├── .progress_EH_260216_AR.json
    └── ...
```

## Lizenz

Intern -- BeeZubi Lernwelt GmbH
