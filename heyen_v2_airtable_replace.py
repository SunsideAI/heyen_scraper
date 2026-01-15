#!/usr/bin/env python3
"""
Scraper für https://www.heyen-immobilien.de/kaufangebote/
Extrahiert Immobilienangebote und synct mit Airtable

Basierend auf streil-immo Scraper v1.7
"""

import os
import re
import sys
import csv
import json
import time
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("[ERROR] Fehlende Module. Bitte installieren:")
    print("  pip install requests beautifulsoup4 lxml")
    sys.exit(1)

# ===========================================================================
# KONFIGURATION
# ===========================================================================

BASE = "https://www.heyen-immobilien.de"
# Liste von URLs zum Scrapen
LIST_URLS = [
    f"{BASE}/kaufangebote/",
    f"{BASE}/mietangebote/",
]

# Airtable
AIRTABLE_TOKEN = os.getenv("AIRTABLE_TOKEN", "")
AIRTABLE_BASE = os.getenv("AIRTABLE_BASE", "")
AIRTABLE_TABLE_ID = os.getenv("AIRTABLE_TABLE_ID", "")

# OpenAI für Kurzbeschreibung
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Rate Limiting
REQUEST_DELAY = 1.5

# ===========================================================================
# REGEX PATTERNS
# ===========================================================================

RE_PLZ_ORT = re.compile(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß\-\s/]+)")
RE_PRICE = re.compile(r"([\d.,]+)\s*€")

# ===========================================================================
# STOPWORDS
# ===========================================================================

STOP_STRINGS = [
    "Cookie", "Datenschutz", "Impressum", "Sie haben Fragen",
    "kontakt@", "Tel:", "Fax:", "E-Mail:", "www.", "http",
    "© ", "JavaScript", "Alle Rechte", "Rufen Sie uns an",
    "Kontaktieren Sie mich", "RICHARD HEYEN", "Telefon:",
    "Mobil:", "Anschrift:"
]

# ===========================================================================
# HELPER FUNCTIONS
# ===========================================================================

def _norm(s: str) -> str:
    """Normalisiere String"""
    if not s:
        return ""
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _clean_desc_lines(lines: List[str]) -> List[str]:
    """Bereinige Beschreibungszeilen"""
    cleaned = []
    seen = set()
    
    for line in lines:
        line = _norm(line)
        if not line or len(line) < 10:
            continue
        
        # Filtere Stopwords
        if any(stop in line for stop in STOP_STRINGS):
            continue
        
        # Dedupliziere
        line_lower = line.lower()
        if line_lower in seen:
            continue
        seen.add(line_lower)
        cleaned.append(line)
    
    return cleaned

def soup_get(url: str, delay: float = REQUEST_DELAY) -> BeautifulSoup:
    """Hole HTML und parse mit BeautifulSoup"""
    time.sleep(delay)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

# ===========================================================================
# AIRTABLE FUNCTIONS
# ===========================================================================

def airtable_table_segment() -> str:
    """Gibt base/table Segment für Airtable API zurück"""
    if not AIRTABLE_BASE or not AIRTABLE_TABLE_ID:
        return ""
    return f"{AIRTABLE_BASE}/{AIRTABLE_TABLE_ID}"

def airtable_headers() -> dict:
    """Airtable API Headers"""
    return {
        "Authorization": f"Bearer {AIRTABLE_TOKEN}",
        "Content-Type": "application/json"
    }

def airtable_list_all() -> tuple:
    """Liste alle Records aus Airtable"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    all_records = []
    offset = None
    
    while True:
        params = {"pageSize": 100}
        if offset:
            params["offset"] = offset
        
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        all_records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
        time.sleep(0.2)
    
    ids = [rec["id"] for rec in all_records]
    fields = [rec.get("fields", {}) for rec in all_records]
    return ids, fields

def airtable_existing_fields() -> set:
    """Ermittle existierende Felder"""
    _, all_fields = airtable_list_all()
    if not all_fields:
        print("[DEBUG] No existing records in Airtable to determine fields")
        return set()
    
    fields = set(all_fields[0].keys())
    print(f"[DEBUG] Existing Airtable fields: {fields}")
    return fields

def airtable_batch_create(records: List[dict]):
    """Erstelle Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": [{"fields": r} for r in batch]}
        
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        
        if not r.ok:
            print(f"[DEBUG] Airtable API Error: {r.status_code}")
            print(f"[DEBUG] Response: {r.text[:500]}")
        
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_update(updates: List[dict]):
    """Update Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        payload = {"records": batch}
        r = requests.patch(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def airtable_batch_delete(record_ids: List[str]):
    """Lösche Records in Batches"""
    url = f"https://api.airtable.com/v0/{airtable_table_segment()}"
    headers = airtable_headers()
    
    for i in range(0, len(record_ids), 10):
        batch = record_ids[i:i+10]
        params = {"records[]": batch}
        r = requests.delete(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        time.sleep(0.2)

def sanitize_record_for_airtable(record: dict, allowed_fields: set) -> dict:
    """Bereinige Record für Airtable"""
    # Felder die immer erlaubt sind (auch wenn sie in bestehenden Records leer sind)
    ALWAYS_ALLOWED = {"Kurzbeschreibung"}
    
    # Wenn keine allowed_fields gesetzt sind (z.B. erste Records), akzeptiere alles
    if not allowed_fields:
        return record
    
    # Kombiniere allowed_fields mit ALWAYS_ALLOWED
    all_allowed = allowed_fields | ALWAYS_ALLOWED
    
    sanitized = {k: v for k, v in record.items() if k in all_allowed}
    return sanitized

# ===========================================================================
# VALIDIERUNG - Leere Records filtern
# ===========================================================================

def is_valid_record(record: dict) -> bool:
    """
    Prüft ob ein Record gültig ist (nicht leer).
    Ein Record ist ungültig wenn:
    - Kein Titel vorhanden
    - Keine URL/Webseite vorhanden
    - Weniger als 3 ausgefüllte Felder
    """
    # Pflichtfelder
    titel = (record.get("Titel") or "").strip()
    webseite = (record.get("Webseite") or "").strip()
    
    if not titel or not webseite:
        return False
    
    # Zähle ausgefüllte Felder (ohne leere Strings)
    filled_fields = 0
    for key, value in record.items():
        if value is not None:
            if isinstance(value, str) and value.strip():
                filled_fields += 1
            elif isinstance(value, (int, float)) and value > 0:
                filled_fields += 1
    
    # Mindestens 3 ausgefüllte Felder (Titel, Webseite, + 1 weiteres)
    return filled_fields >= 3


def filter_valid_records(records: list) -> list:
    """Filtert ungültige/leere Records heraus"""
    valid = []
    invalid_count = 0
    
    for record in records:
        if is_valid_record(record):
            valid.append(record)
        else:
            invalid_count += 1
            print(f"[FILTER] Ungültiger Record übersprungen: {record.get('Titel', 'KEIN TITEL')[:50]}")
    
    if invalid_count > 0:
        print(f"[FILTER] {invalid_count} ungültige Records herausgefiltert")
    
    return valid


def cleanup_empty_airtable_records():
    """
    Löscht leere/ungültige Records aus Airtable.
    Wird am Ende des Scrapers aufgerufen.
    """
    if not (AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment()):
        return
    
    print("[CLEANUP] Prüfe Airtable auf leere Records...")
    
    try:
        all_ids, all_fields = airtable_list_all()
        
        to_delete = []
        for rec_id, fields in zip(all_ids, all_fields):
            if not is_valid_record(fields):
                to_delete.append(rec_id)
                print(f"[CLEANUP] Leerer Record gefunden: {fields.get('Titel', 'KEIN TITEL')[:40]}")
        
        if to_delete:
            print(f"[CLEANUP] Lösche {len(to_delete)} leere Records...")
            airtable_batch_delete(to_delete)
            print(f"[CLEANUP] ✅ {len(to_delete)} leere Records gelöscht")
        else:
            print("[CLEANUP] ✅ Keine leeren Records gefunden")
            
    except Exception as e:
        print(f"[CLEANUP] Fehler: {e}")

# ===========================================================================
# GPT KURZBESCHREIBUNG - NEUE VERSION
# ===========================================================================

# Cache für existierende Kurzbeschreibungen (wird beim Start gefüllt)
KURZBESCHREIBUNG_CACHE = {}  # {objektnummer: kurzbeschreibung}

def load_kurzbeschreibung_cache():
    """Lädt existierende Kurzbeschreibungen aus Airtable in den Cache"""
    global KURZBESCHREIBUNG_CACHE
    
    if not (AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment()):
        print("[CACHE] Airtable nicht konfiguriert - Cache leer")
        return
    
    try:
        all_ids, all_fields = airtable_list_all()
        for fields in all_fields:
            obj_nr = fields.get("Objektnummer", "").strip()
            kurzbeschreibung = fields.get("Kurzbeschreibung", "").strip()
            if obj_nr and kurzbeschreibung:
                KURZBESCHREIBUNG_CACHE[obj_nr] = kurzbeschreibung
        
        print(f"[CACHE] {len(KURZBESCHREIBUNG_CACHE)} Kurzbeschreibungen aus Airtable geladen")
    except Exception as e:
        print(f"[CACHE] Fehler beim Laden: {e}")

def get_cached_kurzbeschreibung(objektnummer: str) -> str:
    """Holt Kurzbeschreibung aus Cache wenn vorhanden"""
    return KURZBESCHREIBUNG_CACHE.get(objektnummer, "")

# Erlaubte Felder (Whitelist - streng) - NEUE VERSION ohne Schlafzimmer/Kategorie
KURZBESCHREIBUNG_FIELDS = [
    "Objekttyp",
    "Baujahr",
    "Wohnfläche",
    "Grundstück",
    "Zimmer",
    "Preis",
    "Standort",
    "Energieeffizienz",
    "Besonderheiten"
]

def normalize_kurzbeschreibung(gpt_output: str, scraped_data: dict) -> str:
    """
    Normalisiert die GPT-Ausgabe.
    NEUE VERSION: Keine Platzhalter, nur vorhandene Felder.
    """
    # Parse GPT Output in Dictionary
    parsed = {}
    for line in gpt_output.strip().split("\n"):
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            # Nur nicht-leere Werte ohne Platzhalter
            if value and value not in ["-", "—", "k. A.", "unbekannt", "nicht angegeben", ""]:
                parsed[key] = value
    
    # Mapping von Scrape-Feldern zu Kurzbeschreibung-Feldern
    scrape_mapping = {
        "Zimmer": "zimmer",
        "Wohnfläche": "wohnflaeche", 
        "Grundstück": "grundstueck",
        "Baujahr": "baujahr",
        "Preis": "preis",
        "Standort": "standort",
    }
    
    # Fülle fehlende Felder NUR aus Scrape-Daten wenn vorhanden
    for field, scrape_key in scrape_mapping.items():
        if field not in parsed or not parsed[field]:
            scrape_value = scraped_data.get(scrape_key, "")
            if scrape_value and str(scrape_value).strip():
                # Formatiere Preis
                if field == "Preis":
                    try:
                        preis_num = float(str(scrape_value).replace(".", "").replace(",", ".").replace("€", "").strip())
                        parsed[field] = f"{int(preis_num)} €"
                    except:
                        pass
                # Formatiere Wohnfläche
                elif field == "Wohnfläche":
                    val = str(scrape_value).replace("ca.", "").replace("m²", "").strip()
                    if val:
                        parsed[field] = f"{val} m²"
                # Formatiere Grundstück
                elif field == "Grundstück":
                    val = str(scrape_value).replace("ca.", "").replace("m²", "").strip()
                    if val:
                        parsed[field] = f"{val} m²"
                else:
                    parsed[field] = str(scrape_value)
    
    # Baue Ausgabe NUR mit vorhandenen Feldern (keine leeren Zeilen!)
    output_lines = []
    for field in KURZBESCHREIBUNG_FIELDS:
        value = parsed.get(field, "")
        if value and value.strip():
            output_lines.append(f"{field}: {value}")
    
    return "\n".join(output_lines)

def generate_kurzbeschreibung(beschreibung: str, titel: str, kategorie: str, preis: str, ort: str,
                               zimmer: str = "", wohnflaeche: str = "", grundstueck: str = "", baujahr: str = "",
                               objektnummer: str = "") -> str:
    """
    Generiert eine strukturierte Kurzbeschreibung mit GPT.
    NEUE VERSION: Strenger Prompt, nur objektive Fakten, keine Platzhalter.
    """
    
    # CACHE CHECK: Wenn bereits vorhanden, nicht neu generieren!
    if objektnummer:
        cached = get_cached_kurzbeschreibung(objektnummer)
        if cached:
            print(f"[CACHE] Kurzbeschreibung aus Cache verwendet für {objektnummer[:30]}...")
            return cached
    
    # Scrape-Daten für Fallback sammeln
    scraped_data = {
        "kategorie": kategorie,
        "preis": preis,
        "standort": ort,
        "zimmer": zimmer,
        "wohnflaeche": wohnflaeche,
        "grundstueck": grundstueck,
        "baujahr": baujahr,
    }
    
    if not OPENAI_API_KEY:
        print("[WARN] OPENAI_API_KEY nicht gesetzt - erstelle Kurzbeschreibung aus Scrape-Daten")
        return normalize_kurzbeschreibung("", scraped_data)
    
    # NEUER STRENGER PROMPT
    prompt = f"""# Rolle
Du bist ein präziser Immobilien-Datenanalyst und Parser. Deine Aufgabe ist es, aus unstrukturierten Immobilienanzeigen ausschließlich objektive, explizit genannte Fakten zu extrahieren und streng strukturiert auszugeben. Du arbeitest regelbasiert, deterministisch und formatgenau. Kreative Ergänzungen sind untersagt.

# Aufgabe
1. Analysiere die bereitgestellte Immobilienanzeige vollständig.
2. Extrahiere nur eindeutig genannte, objektive Fakten.
3. Gib die strukturierte Kurzbeschreibung exakt im vorgegebenen Zeilenformat aus.
4. Lasse jedes Feld vollständig weg, zu dem keine eindeutige Angabe vorliegt.

# Eingabedaten
TITEL: {titel}
KATEGORIE: {kategorie}
PREIS: {preis if preis else 'nicht angegeben'}
STANDORT: {ort if ort else 'nicht angegeben'}
BESCHREIBUNG: {beschreibung[:3000]}

# Erlaubte Felder (Whitelist – verbindlich)
Es dürfen ausschließlich die folgenden Felder verwendet werden. Jedes andere Feld ist strikt verboten.

Objekttyp
Baujahr
Wohnfläche
Grundstück
Zimmer
Preis
Standort
Energieeffizienz
Besonderheiten

# Ausgabeformat (verbindlich)
Die Ausgabe muss exakt diesem Muster folgen. Jede Eigenschaft steht in einer eigenen Zeile. Keine Leerzeilen, keine zusätzlichen Texte, keine Markdown-Formatierung.

Objekttyp: [Einfamilienhaus | Mehrfamilienhaus | Eigentumswohnung | Baugrundstück | Reihenhaus | Doppelhaushälfte | Sonstiges]
Baujahr: [Jahr]
Wohnfläche: [Zahl in m²]
Grundstück: [Zahl in m²]
Zimmer: [Anzahl]
Preis: [Zahl in €]
Standort: [Ort oder PLZ Ort]
Energieeffizienz: [Klasse]
Besonderheiten: [kommaseparierte Liste]

# Strikte Regeln (bindend)
• Es ist strengstens untersagt, eigene Felder zu erfinden.
• Felder wie „Schlafzimmer", „Kategorie", „Etage", „Ausstattung", „Kauf/Miete" oder ähnliche sind ausnahmslos verboten.
• Es dürfen keine Platzhalter verwendet werden (z. B. „-", „—", „k. A.", „unbekannt").
• Wenn ein Feld nicht eindeutig ermittelbar ist, darf die gesamte Zeile nicht ausgegeben werden.
• Die Reihenfolge der Zeilen muss exakt der Vorgabe entsprechen.
• Es darf niemals mehr als ein Feld pro Zeile stehen.
• Verwende ausschließlich arabische Ziffern.
• Einheiten exakt wie folgt anhängen:
  – Wohnfläche und Grundstück: m²
  – Preis: €
• Keine Interpretationen, keine Schätzungen, keine Ableitungen.
• Im Zweifel gilt: lieber weniger Felder ausgeben, niemals mehr.

# Ziel
Die Ausgabe wird automatisiert weiterverarbeitet (z. B. Airtable, Voiceflow, Such- und Filterlogiken). Jede Abweichung vom Format gilt als Fehler."""

    try:
        headers = {
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": "Du bist ein regelbasierter Datenparser. Halte dich strikt an die Vorgaben. Keine Kreativität, keine Ergänzungen."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 400,
            "temperature": 0.0  # Deterministisch
        }
        
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        result = response.json()
        gpt_output = result["choices"][0]["message"]["content"].strip()
        
        # Normalisiere
        kurzbeschreibung = normalize_kurzbeschreibung(gpt_output, scraped_data)
        
        print(f"[GPT] Kurzbeschreibung generiert ({len(kurzbeschreibung)} Zeichen)")
        return kurzbeschreibung
        
    except Exception as e:
        print(f"[ERROR] GPT Kurzbeschreibung fehlgeschlagen: {e}")
        return normalize_kurzbeschreibung("", scraped_data)

# ===========================================================================
# EXTRACTION FUNCTIONS
# ===========================================================================

def extract_price(page_text: str) -> str:
    """Extrahiere Preis aus dem Seitentext"""
    # Suche nach verschiedenen Preis-Patterns
    patterns = [
        r"Kaufpreis(?:vorstellung)?[:\s]+(?:de[rs]\s+)?(?:Eigentümer(?:s|in)?[:\s]+)?€?\s*([\d.]+),?-?\s*€",
        r"Kaufpreis(?:vorstellung)?[:\s]+(?:de[rs]\s+)?(?:Eigentümer(?:s|in)?[:\s]+)?€?\s*([\d.]+(?:,\d+)?)\s*€",
        r"[-•]?\s*Kaltmiete[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
        r"[-•]?\s*Warmmiete[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
        r"[-•]?\s*Miete[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
        r"[-•]?\s*Preis[:\s]+€?\s*([\d.]+(?:,\d+)?)\s*€",
    ]
    
    for pattern in patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            preis_str = m.group(1)
            preis_clean = preis_str.replace(".", "")
            
            if "," in preis_clean:
                preis_clean = preis_clean.replace(",", ".")
            
            try:
                preis_num = float(preis_clean)
                if preis_num > 100:
                    result = f"€{int(preis_num):,}".replace(",", ".")
                    return result
            except:
                continue
    
    return ""

def parse_price_to_number(preis_str: str) -> Optional[float]:
    """Konvertiere Preis-String zu Nummer für Airtable"""
    if not preis_str:
        return None
    
    clean = preis_str.replace("€", "").strip()
    clean = clean.replace(".", "").replace(",", ".")
    
    try:
        return float(clean)
    except:
        return None

def extract_plz_ort(text: str, title: str = "") -> str:
    """Extrahiere PLZ und Ort aus Text"""
    matches = list(RE_PLZ_ORT.finditer(text))
    
    if matches:
        m = matches[0]
        plz = m.group(1)
        ort = m.group(2).strip()
        ort = re.split(r'\s*[-–/]\s*', ort)[0].strip()
        ort = re.sub(r'\s+(angeboten|von|der|die|das|GmbH|Immobilien).*$', '', ort, flags=re.IGNORECASE).strip()
        ort = re.sub(r"\s+", " ", ort).strip()
        
        if len(ort.split()) > 2:
            ort = " ".join(ort.split()[:2])
        
        return f"{plz} {ort}"
    
    return ""

def extract_objektnummer(url: str) -> str:
    """Extrahiere Objektnummer aus URL"""
    parts = url.rstrip("/").split("/")
    if len(parts) > 0:
        slug = parts[-1]
        return slug
    return ""

def extract_additional_data(page_text: str) -> dict:
    """Extrahiere zusätzliche Daten für die Kurzbeschreibung"""
    data = {
        "zimmer": "",
        "wohnflaeche": "",
        "grundstueck": "",
        "baujahr": ""
    }
    
    # Zimmer extrahieren
    zimmer_patterns = [
        r"(\d+)\s*Zimmer",
        r"Zimmer[:\s]+(\d+)",
        r"(\d+)-Zimmer",
    ]
    for pattern in zimmer_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["zimmer"] = m.group(1)
            break
    
    # Wohnfläche extrahieren
    wohnflaeche_patterns = [
        r"(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*Wohnfläche",
        r"Wohnfläche[:\s]+(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²",
        r"(\d+(?:[.,]\d+)?)\s*m²\s*Wohnfl",
    ]
    for pattern in wohnflaeche_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["wohnflaeche"] = m.group(1).replace(",", ".")
            break
    
    # Grundstück extrahieren
    grundstueck_patterns = [
        r"(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²\s*Grundstück",
        r"Grundstück[:\s]+(?:ca\.\s*)?(\d+(?:[.,]\d+)?)\s*m²",
        r"(\d+(?:[.,]\d+)?)\s*m²\s*(?:großes?\s+)?Grundstück",
    ]
    for pattern in grundstueck_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            data["grundstueck"] = m.group(1).replace(",", ".")
            break
    
    # Baujahr extrahieren
    baujahr_patterns = [
        r"Baujahr[:\s]+(\d{4})",
        r"aus\s+(?:dem\s+)?(?:Baujahr\s+)?(\d{4})",
        r"(\d{4})\s+(?:erbaut|gebaut)",
    ]
    for pattern in baujahr_patterns:
        m = re.search(pattern, page_text, re.IGNORECASE)
        if m:
            jahr = int(m.group(1))
            if 1800 <= jahr <= 2030:
                data["baujahr"] = str(jahr)
                break
    
    return data

def extract_description(soup: BeautifulSoup, title: str, page_text: str) -> str:
    """Extrahiere strukturierte Beschreibung"""
    lines = []
    
    if title:
        lines.append(f"=== {title.upper()} ===")
    
    eckdaten_match = re.search(r"Die Eckdaten:\s*(.+?)(?=\n[A-Z][a-z]+:|$)", page_text, re.DOTALL | re.IGNORECASE)
    if eckdaten_match:
        eckdaten_text = eckdaten_match.group(1).strip()
        eckdaten_lines = [line.strip() for line in eckdaten_text.split("\n") if line.strip()]
        eckdaten_lines = [line.lstrip("-•").strip() for line in eckdaten_lines]
        eckdaten_lines = [line for line in eckdaten_lines if len(line) > 10]
        
        if eckdaten_lines:
            lines.append("\n=== ECKDATEN ===")
            for line in eckdaten_lines[:20]:
                lines.append(f"• {line}")
    
    sections = [
        ("Energieausweis", r"Der Energieausweis:\s*(.+?)(?=\n[A-Z][a-z]+:|$)"),
        ("Objektbeschreibung", r"(?:Objektbeschreibung|Beschreibung):\s*(.+?)(?=\n[A-Z][a-z]+:|$)"),
    ]
    
    for section_name, pattern in sections:
        m = re.search(pattern, page_text, re.DOTALL | re.IGNORECASE)
        if m:
            section_text = m.group(1).strip()
            section_lines = [line.strip() for line in section_text.split("\n") if line.strip()]
            section_lines = [line.lstrip("-•").strip() for line in section_lines]
            section_lines = [line for line in section_lines if len(line) > 5]
            
            if section_lines:
                lines.append(f"\n=== {section_name.upper()} ===")
                lines.extend(section_lines[:15])
    
    cleaned_lines = _clean_desc_lines(lines)
    
    if cleaned_lines:
        return "\n\n".join(cleaned_lines)[:12000]
    
    desc_lines = []
    for p in soup.find_all("p"):
        text = _norm(p.get_text(" ", strip=True))
        if text and len(text) > 50:
            if not any(skip in text for skip in STOP_STRINGS):
                desc_lines.append(text)
    
    desc_lines = _clean_desc_lines(desc_lines)
    if desc_lines:
        return "\n\n".join(desc_lines[:10])[:12000]
    
    return ""

def extract_kategorie(page_text: str, title: str, url: str) -> str:
    """Bestimme Kategorie (Kaufen/Mieten)"""
    if "/kaufangebote/" in url:
        return "Kaufen"
    if "/mietangebote/" in url:
        return "Mieten"
    
    text = (title + " " + page_text).lower()
    if any(keyword in text for keyword in ["zur miete", "zu vermieten", "mietangebot", "miete monatlich"]):
        return "Mieten"
    
    return "Kaufen"

def extract_objekttyp(page_text: str, title: str) -> str:
    """Extrahiere Objekttyp"""
    text = title + " " + page_text
    
    objekttypen = {
        "Wohnhaus": [r"\bWohnhaus\b", r"\bEinfamilienhaus\b", r"\bEFH\b"],
        "Eigentumswohnung": [r"\bEigentumswohnung\b", r"\bWohnung\b", r"\bETW\b"],
        "Baugrundstück": [r"\bBaugrundstück\b", r"\bGrundstück\b"],
        "Wohnanlage": [r"\bWohnanlage\b", r"\bMehrfamilienhaus\b", r"\bMFH\b"],
    }
    
    for typ, patterns in objekttypen.items():
        for pattern in patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return typ
    
    return "Wohnhaus"

# ===========================================================================
# SCRAPING FUNCTIONS
# ===========================================================================

def collect_detail_links() -> List[str]:
    """Sammle alle Detailseiten-Links von allen Angebotsseiten"""
    all_links = []
    
    BLACKLIST = [
        "/finanzierung/",
        "/diskrete-kaufangebote/",
        "/diskrete-mietangebote/",
    ]
    
    for list_url in LIST_URLS:
        print(f"[LIST] Hole {list_url}")
        try:
            soup = soup_get(list_url)
            
            for a in soup.find_all("a", href=True):
                href = a["href"]
                
                if ("/kaufangebote/" in href or "/mietangebote/" in href) and href.count("/") >= 3:
                    if href.strip("/") in ["kaufangebote", "mietangebote"]:
                        continue
                    
                    if any(blacklisted in href for blacklisted in BLACKLIST):
                        continue
                    
                    full_url = urljoin(BASE, href)
                    if full_url not in all_links and full_url not in LIST_URLS:
                        all_links.append(full_url)
        except Exception as e:
            print(f"[ERROR] Fehler beim Holen von {list_url}: {e}")
            continue
    
    print(f"[LIST] Gefunden: {len(all_links)} Immobilien gesamt")
    return all_links

def parse_detail(detail_url: str) -> dict:
    """Parse Detailseite"""
    soup = soup_get(detail_url)
    page_text = soup.get_text("\n", strip=True)
    
    title = ""
    for tag in soup.find_all(["h1", "h2"]):
        text = _norm(tag.get_text(strip=True))
        if text and len(text) > 10 and text not in ["Aktuelles Kaufangebot", "Aktuelles Mietangebot"]:
            title = text
            break
    
    if not title or title in ["Aktuelles Kaufangebot", "Aktuelles Mietangebot"]:
        h_tags = soup.find_all(["h1", "h2", "h3"])
        for i, tag in enumerate(h_tags):
            text = _norm(tag.get_text(strip=True))
            if text in ["Aktuelles Kaufangebot", "Aktuelles Mietangebot"] and i + 1 < len(h_tags):
                next_text = _norm(h_tags[i + 1].get_text(strip=True))
                if next_text and len(next_text) > 10:
                    title = next_text
                    break
    
    if not title or len(title) < 10:
        patterns = [
            r"((?:Wohnhaus|Eigentumswohnung|Baugrundstück|Wohnanlage|Apartment|Maisonette-Wohnung)\s+in\s+[A-Z][\w\s/-]+)",
            r"((?:Stilvolle|Charmante|Luxuriös|Modern)\s+\d+-Zimmer-Wohnung\s+in\s+[A-Z][\w\s/-]+)",
        ]
        for pattern in patterns:
            m = re.search(pattern, page_text)
            if m:
                title = m.group(1).strip()
                break
    
    objektnummer = extract_objektnummer(detail_url)
    preis = extract_price(page_text)
    ort = extract_plz_ort(page_text, title)
    
    image_url = ""
    for img in soup.find_all("img"):
        src = img.get("src", "")
        if src and ("/wp-content/uploads/" in src or "go-x" in src):
            if any(skip in src.lower() for skip in ["logo", "icon", "favicon"]):
                continue
            if "2b42354c-5e2d-4fab-acae-4280e6ed4089" in src:
                continue
            alt = img.get("alt", "").lower()
            if "logo" in alt:
                continue
            image_url = src if src.startswith("http") else urljoin(BASE, src)
            break
    
    kategorie = extract_kategorie(page_text, title, detail_url)
    objekttyp = extract_objekttyp(page_text, title)
    description = extract_description(soup, title, page_text)
    additional_data = extract_additional_data(page_text)
    
    kurzbeschreibung = generate_kurzbeschreibung(
        beschreibung=description,
        titel=title,
        kategorie=kategorie,
        preis=preis,
        ort=ort,
        zimmer=additional_data["zimmer"],
        wohnflaeche=additional_data["wohnflaeche"],
        grundstueck=additional_data["grundstueck"],
        baujahr=additional_data["baujahr"],
        objektnummer=objektnummer
    )
    
    return {
        "Titel": title,
        "URL": detail_url,
        "Beschreibung": description,
        "Kurzbeschreibung": kurzbeschreibung,
        "Objektnummer": objektnummer,
        "Kategorie": kategorie,
        "Objekttyp": objekttyp,
        "Preis": preis,
        "Ort": ort,
        "Bild_URL": image_url,
    }

def make_record(row: dict) -> dict:
    """Erstelle Airtable-Record"""
    preis_value = parse_price_to_number(row["Preis"])
    
    record = {
        "Titel": row["Titel"],
        "Kategorie": row["Kategorie"],
        "Webseite": row["URL"],
        "Objektnummer": row["Objektnummer"],
        "Beschreibung": row["Beschreibung"],
        "Bild": row["Bild_URL"],
        "Standort": row["Ort"],
    }
    
    if row.get("Kurzbeschreibung"):
        record["Kurzbeschreibung"] = row["Kurzbeschreibung"]
    
    if preis_value is not None:
        record["Preis"] = preis_value
    
    return record

def unique_key(fields: dict) -> str:
    """Eindeutiger Key für Record"""
    obj = (fields.get("Objektnummer") or "").strip()
    if obj:
        return f"obj:{obj}"
    url = (fields.get("Webseite") or "").strip()
    if url:
        return f"url:{url}"
    return f"hash:{hash(json.dumps(fields, sort_keys=True))}"

# ===========================================================================
# MAIN
# ===========================================================================

def run():
    """Hauptfunktion"""
    print("[HEYEN] Starte Scraper für heyen-immobilien.de")
    
    # OPTIMIERUNG: Lade existierende Kurzbeschreibungen aus Airtable
    print("[INIT] Lade Kurzbeschreibungen-Cache aus Airtable...")
    load_kurzbeschreibung_cache()
    
    # Sammle Links
    detail_links = collect_detail_links()
    
    if not detail_links:
        print("[WARN] Keine Links gefunden!")
        return
    
    # Scrape Details
    all_rows = []
    for i, url in enumerate(detail_links, 1):
        try:
            print(f"\n[SCRAPE] {i}/{len(detail_links)} | {url}")
            row = parse_detail(url)
            record = make_record(row)
            
            preis_display = record.get('Preis', 'N/A')
            print(f"  → {record['Kategorie']:8} | {record['Titel'][:60]} | {record.get('Standort', 'N/A')} | Preis: {preis_display}")
            
            all_rows.append(record)
        except Exception as e:
            print(f"[ERROR] Fehler bei {url}: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    if not all_rows:
        print("[WARN] Keine Datensätze gefunden.")
        return
    
    # NEUE VALIDIERUNG: Leere Records filtern
    print(f"\n[VALIDATE] Prüfe {len(all_rows)} Records auf Gültigkeit...")
    all_rows = filter_valid_records(all_rows)
    
    if not all_rows:
        print("[WARN] Keine gültigen Datensätze nach Filterung.")
        return
    
    # Speichere CSV
    csv_file = "heyen_immobilien.csv"
    cols = ["Titel", "Kategorie", "Webseite", "Objektnummer", "Objekttyp", "Beschreibung", "Kurzbeschreibung", "Bild", "Preis", "Standort"]
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction='ignore')
        w.writeheader()
        w.writerows(all_rows)
    print(f"\n[CSV] Gespeichert: {csv_file} ({len(all_rows)} Zeilen)")
    
    # Airtable Sync
    if AIRTABLE_TOKEN and AIRTABLE_BASE and airtable_table_segment():
        print("\n[AIRTABLE] Starte Synchronisation...")
        
        allowed = airtable_existing_fields()
        all_ids, all_fields = airtable_list_all()
        
        existing = {}
        for rec_id, f in zip(all_ids, all_fields):
            k = unique_key(f)
            existing[k] = (rec_id, f)
        
        desired = {}
        for r in all_rows:
            k = unique_key(r)
            if k in desired:
                if len(r.get("Beschreibung", "")) > len(desired[k].get("Beschreibung", "")):
                    desired[k] = sanitize_record_for_airtable(r, allowed)
            else:
                desired[k] = sanitize_record_for_airtable(r, allowed)
        
        to_create, to_update, keep = [], [], set()
        for k, fields in desired.items():
            if k in existing:
                rec_id, old = existing[k]
                diff = {fld: val for fld, val in fields.items() if old.get(fld) != val}
                if diff:
                    to_update.append({"id": rec_id, "fields": diff})
                keep.add(k)
            else:
                to_create.append(fields)
        
        to_delete_ids = [rec_id for k, (rec_id, _) in existing.items() if k not in keep]
        
        print(f"\n[SYNC] Gesamt → create: {len(to_create)}, update: {len(to_update)}, delete: {len(to_delete_ids)}")
        
        if to_create:
            print(f"[Airtable] Erstelle {len(to_create)} neue Records...")
            airtable_batch_create(to_create)
        if to_update:
            print(f"[Airtable] Aktualisiere {len(to_update)} Records...")
            airtable_batch_update(to_update)
        if to_delete_ids:
            print(f"[Airtable] Lösche {len(to_delete_ids)} Records...")
            airtable_batch_delete(to_delete_ids)
        
        # NEUER CLEANUP: Lösche leere Records aus Airtable
        cleanup_empty_airtable_records()
        
        print("[Airtable] Synchronisation abgeschlossen.\n")
    else:
        print("[Airtable] ENV nicht gesetzt – Upload übersprungen.")

if __name__ == "__main__":
    run()
